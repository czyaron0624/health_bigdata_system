# -*- coding: utf-8 -*-
"""
异常检测任务。

功能:
1. 优先使用 prediction_results 中的预测值与实际值对比
2. 缺少预测重叠时，使用近三年历史中位数作为期望值
3. 将结果写入 anomaly_detection

执行:
    python spark_job/anomaly_detection.py
"""

from collections import defaultdict
from statistics import median

import mysql.connector


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}

METRIC_LABELS = {
    "doctor_count": "执业(助理)医师数",
    "nurse_count": "注册护士数",
    "bed_count": "实有床位数",
    "outpatient_visits": "总诊疗人次数",
    "discharge_count": "出院人数",
    "outpatient_cost": "门诊病人次均医药费用",
    "discharge_cost": "出院病人人均医药费用",
}

WARNING_THRESHOLD = 20.0
CRITICAL_THRESHOLD = 50.0
HISTORY_WINDOW = 3


def fetch_actual_rows(cursor):
    cursor.execute(
        """
        SELECT
            region,
            year,
            doctor_count,
            nurse_count,
            bed_count,
            outpatient_visits,
            discharge_count,
            outpatient_cost,
            discharge_cost
        FROM ocr_metrics_yearly
        ORDER BY region, year
        """
    )

    rows = []
    for record in cursor.fetchall():
        row = {
            "region": record[0],
            "year": int(record[1]),
        }
        for index, metric_key in enumerate(METRIC_LABELS.keys(), start=2):
            value = record[index]
            row[metric_key] = float(value) if value is not None else None
        rows.append(row)
    return rows


def fetch_prediction_map(cursor):
    cursor.execute(
        """
        SELECT
            region,
            metric_key,
            predict_year,
            predict_value,
            confidence_lower,
            confidence_upper,
            model_type,
            model_accuracy,
            training_data_range
        FROM prediction_results
        """
    )

    prediction_map = {}
    for row in cursor.fetchall():
        prediction_map[(row[0], row[1], int(row[2]))] = {
            "expected_value": float(row[3]) if row[3] is not None else None,
            "confidence_lower": float(row[4]) if row[4] is not None else None,
            "confidence_upper": float(row[5]) if row[5] is not None else None,
            "model_type": row[6],
            "model_accuracy": float(row[7]) if row[7] is not None else None,
            "training_data_range": row[8],
        }
    return prediction_map


def build_history_index(actual_rows):
    history_index = defaultdict(list)
    for row in actual_rows:
        region = row["region"]
        year = row["year"]
        for metric_key in METRIC_LABELS:
            value = row.get(metric_key)
            if value is None or value <= 0:
                continue
            history_index[(region, metric_key)].append((year, float(value)))

    for key in history_index:
        history_index[key] = sorted(history_index[key], key=lambda item: item[0])
    return history_index


def historical_expected(history_points, current_year):
    previous_values = [value for year, value in history_points if year < current_year and value > 0]
    if len(previous_values) < 3:
        return None
    baseline_values = previous_values[-HISTORY_WINDOW:]
    return float(median(baseline_values))


def classify_anomaly(deviation_rate):
    absolute_rate = abs(deviation_rate)
    if absolute_rate >= CRITICAL_THRESHOLD:
        return "critical"
    if absolute_rate >= WARNING_THRESHOLD:
        return "warning"
    return "normal"


def format_rate(value):
    return round(float(value), 2)


def build_description(region, year, metric_key, actual_value, expected_value, deviation_rate, baseline_type, baseline_meta):
    metric_name = METRIC_LABELS[metric_key]
    direction = "高于" if deviation_rate >= 0 else "低于"
    description = (
        f"{region}{year}年{metric_name}实际值{format_rate(actual_value)}，"
        f"{direction}{baseline_type}期望值{format_rate(expected_value)}，"
        f"偏离{format_rate(abs(deviation_rate))}%"
    )

    if baseline_meta.get("training_data_range"):
        description += f"，训练区间 {baseline_meta['training_data_range']}"
    if baseline_meta.get("model_accuracy") is not None:
        description += f"，模型拟合度 {round(float(baseline_meta['model_accuracy']), 4)}"
    if baseline_meta.get("confidence_lower") is not None and baseline_meta.get("confidence_upper") is not None:
        description += (
            f"，预测区间[{format_rate(baseline_meta['confidence_lower'])}, "
            f"{format_rate(baseline_meta['confidence_upper'])}]"
        )

    return description


def build_anomaly_rows(actual_rows, prediction_map):
    history_index = build_history_index(actual_rows)
    results = []

    for row in actual_rows:
        region = row["region"]
        year = row["year"]

        for metric_key in METRIC_LABELS:
            actual_value = row.get(metric_key)
            if actual_value is None or actual_value <= 0:
                continue

            prediction = prediction_map.get((region, metric_key, year))
            baseline_type = None
            baseline_meta = {}
            expected_value = None

            if prediction and prediction.get("expected_value") and prediction["expected_value"] > 0:
                expected_value = float(prediction["expected_value"])
                baseline_type = "预测"
                baseline_meta = prediction
            else:
                expected_value = historical_expected(history_index.get((region, metric_key), []), year)
                baseline_type = "历史基线"
                baseline_meta = {}

            if expected_value is None or expected_value <= 0:
                continue

            deviation_rate = ((float(actual_value) - expected_value) / expected_value) * 100
            anomaly_level = classify_anomaly(deviation_rate)

            results.append(
                {
                    "region": region,
                    "metric_key": metric_key,
                    "year": year,
                    "actual_value": round(float(actual_value), 2),
                    "expected_value": round(float(expected_value), 2),
                    "deviation_rate": round(float(deviation_rate), 4),
                    "anomaly_level": anomaly_level,
                    "description": build_description(
                        region,
                        year,
                        metric_key,
                        actual_value,
                        expected_value,
                        deviation_rate,
                        baseline_type,
                        baseline_meta,
                    ),
                }
            )

    return results


def write_rows(cursor, rows):
    cursor.execute("TRUNCATE TABLE anomaly_detection")
    insert_sql = """
        INSERT INTO anomaly_detection (
            region,
            metric_key,
            year,
            actual_value,
            expected_value,
            deviation_rate,
            anomaly_level,
            description
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (
            row["region"],
            row["metric_key"],
            row["year"],
            row["actual_value"],
            row["expected_value"],
            row["deviation_rate"],
            row["anomaly_level"],
            row["description"],
        )
        for row in rows
    ]
    cursor.executemany(insert_sql, values)


def summarize_levels(rows):
    summary = {"normal": 0, "warning": 0, "critical": 0}
    for row in rows:
        summary[row["anomaly_level"]] = summary.get(row["anomaly_level"], 0) + 1
    return summary


def main():
    print("=" * 60)
    print("异常检测任务")
    print("=" * 60)

    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        actual_rows = fetch_actual_rows(cursor)
        prediction_map = fetch_prediction_map(cursor)
        anomaly_rows = build_anomaly_rows(actual_rows, prediction_map)

        if not anomaly_rows:
            print("No anomaly rows generated")
            return

        write_rows(cursor, anomaly_rows)
        conn.commit()

        summary = summarize_levels(anomaly_rows)
        print(f"anomaly_detection updated: {len(anomaly_rows)} rows")
        print(
            "levels => "
            f"normal: {summary.get('normal', 0)}, "
            f"warning: {summary.get('warning', 0)}, "
            f"critical: {summary.get('critical', 0)}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
