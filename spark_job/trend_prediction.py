# -*- coding: utf-8 -*-
"""
趋势预测任务。
功能: 基于 ocr_metrics_yearly 的历史数据，预测未来 3 年关键指标。
执行: python spark_job/trend_prediction.py
"""

from statistics import median
import math

import mysql.connector


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}

METRICS_TO_PREDICT = [
    ("doctor_count", "执业(助理)医师数"),
    ("nurse_count", "注册护士数"),
    ("bed_count", "实有床位数"),
    ("outpatient_visits", "总诊疗人次数"),
    ("discharge_count", "出院人数"),
    ("outpatient_cost", "门诊病人次均医药费用"),
    ("discharge_cost", "出院病人人均医药费用"),
]

FORECAST_YEARS = 3
MIN_POINTS = 3


def fetch_yearly_metrics(cursor):
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
    columns = [
        "region",
        "year",
        "doctor_count",
        "nurse_count",
        "bed_count",
        "outpatient_visits",
        "discharge_count",
        "outpatient_cost",
        "discharge_cost",
    ]
    rows = []
    for record in cursor.fetchall():
        row = {}
        for idx, key in enumerate(columns):
            row[key] = record[idx]
        rows.append(row)
    return rows


def group_by_region(rows):
    grouped = {}
    for row in rows:
        grouped.setdefault(row["region"], []).append(row)
    for region in grouped:
        grouped[region] = sorted(grouped[region], key=lambda item: item["year"])
    return grouped


def build_series(region_rows, metric_key):
    points = []
    for row in region_rows:
        value = row.get(metric_key)
        if value is None:
            continue
        numeric_value = float(value)
        if numeric_value <= 0:
            continue
        points.append((int(row["year"]), numeric_value))
    return points


def drop_partial_latest_point(points):
    if len(points) < 4:
        return points, None

    latest_year, latest_value = points[-1]
    previous_values = [value for _, value in points[-4:-1] if value > 0]
    if len(previous_values) < 2:
        return points, None

    previous_median = median(previous_values)
    if previous_median <= 0:
        return points, None

    if latest_value / previous_median < 0.6:
        return points[:-1], latest_year
    return points, None


def drop_extreme_outliers(points):
    if len(points) < 5:
        return points, []

    values = [value for _, value in points if value > 0]
    if len(values) < 5:
        return points, []

    baseline = median(values)
    if baseline <= 0:
        return points, []

    kept = []
    dropped = []
    for year, value in points:
        ratio = value / baseline
        if ratio > 3.0 or ratio < 0.2:
            dropped.append(year)
            continue
        kept.append((year, value))

    if len(kept) < MIN_POINTS:
        return points, []
    return kept, dropped


def linear_regression(points):
    n = len(points)
    x_values = [point[0] for point in points]
    y_values = [point[1] for point in points]

    mean_x = sum(x_values) / n
    mean_y = sum(y_values) / n

    denominator = sum((x - mean_x) ** 2 for x in x_values)
    if denominator == 0:
        slope = 0.0
    else:
        slope = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values)) / denominator
    intercept = mean_y - slope * mean_x

    predictions = [intercept + slope * x for x in x_values]
    residuals = [actual - predicted for actual, predicted in zip(y_values, predictions)]
    residual_std = math.sqrt(sum(residual ** 2 for residual in residuals) / max(1, n - 2))

    ss_total = sum((y - mean_y) ** 2 for y in y_values)
    ss_res = sum((actual - predicted) ** 2 for actual, predicted in zip(y_values, predictions))
    if ss_total <= 0:
        r2 = 1.0
    else:
        r2 = max(0.0, min(1.0, 1 - (ss_res / ss_total)))

    return {
        "slope": slope,
        "intercept": intercept,
        "residual_std": residual_std,
        "r2": r2,
    }


def predict_points(region, metric_key, metric_name, points):
    cleaned_points, dropped_year = drop_partial_latest_point(points)
    cleaned_points, dropped_outlier_years = drop_extreme_outliers(cleaned_points)
    if len(cleaned_points) < MIN_POINTS:
        return []

    model = linear_regression(cleaned_points)
    last_year = cleaned_points[-1][0]
    training_years = [year for year, _ in cleaned_points]
    training_range = f"{min(training_years)}-{max(training_years)}"
    if dropped_year is not None:
        training_range += f" (excluded {dropped_year})"
    if dropped_outlier_years:
        outlier_text = ",".join(str(year) for year in sorted(dropped_outlier_years))
        training_range += f" (outliers {outlier_text})"

    predictions = []
    for step in range(1, FORECAST_YEARS + 1):
        predict_year = last_year + step
        predict_value = model["intercept"] + model["slope"] * predict_year
        predict_value = max(0.0, predict_value)

        margin = max(
            abs(predict_value) * 0.05,
            1.96 * model["residual_std"] * (1 + 0.15 * (step - 1)),
        )
        confidence_lower = max(0.0, predict_value - margin)
        confidence_upper = max(confidence_lower, predict_value + margin)

        predictions.append(
            {
                "region": region,
                "metric_key": metric_key,
                "metric_name": metric_name,
                "predict_year": predict_year,
                "predict_value": round(predict_value, 2),
                "confidence_lower": round(confidence_lower, 2),
                "confidence_upper": round(confidence_upper, 2),
                "model_type": "linear_regression",
                "model_accuracy": round(model["r2"], 4),
                "training_data_range": training_range,
            }
        )
    return predictions


def write_predictions(cursor, rows):
    cursor.execute("TRUNCATE TABLE prediction_results")
    insert_sql = """
        INSERT INTO prediction_results (
            region,
            metric_key,
            metric_name,
            predict_year,
            predict_value,
            confidence_lower,
            confidence_upper,
            model_type,
            model_accuracy,
            training_data_range
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (
            row["region"],
            row["metric_key"],
            row["metric_name"],
            row["predict_year"],
            row["predict_value"],
            row["confidence_lower"],
            row["confidence_upper"],
            row["model_type"],
            row["model_accuracy"],
            row["training_data_range"],
        )
        for row in rows
    ]
    cursor.executemany(insert_sql, values)


def main():
    print("=" * 60)
    print("趋势预测任务")
    print("=" * 60)

    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        yearly_rows = fetch_yearly_metrics(cursor)
        grouped = group_by_region(yearly_rows)

        all_predictions = []
        for region, region_rows in grouped.items():
            for metric_key, metric_name in METRICS_TO_PREDICT:
                points = build_series(region_rows, metric_key)
                predictions = predict_points(region, metric_key, metric_name, points)
                all_predictions.extend(predictions)

        if not all_predictions:
            print("No prediction rows generated")
            return

        write_predictions(cursor, all_predictions)
        conn.commit()
        print(f"prediction_results updated: {len(all_predictions)} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
