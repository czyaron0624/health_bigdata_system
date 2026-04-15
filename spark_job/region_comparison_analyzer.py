# -*- coding: utf-8 -*-
"""
跨地区医疗资源对比分析器。
功能: 结合年度指标、人口和机构数据，生成 region_comparison。
执行: python spark_job/region_comparison_analyzer.py
"""

from collections import defaultdict

import mysql.connector


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}


def normalize_score(values_by_key, reverse=False):
    valid_values = [value for value in values_by_key.values() if value is not None]
    if not valid_values:
        return {key: 50.0 for key in values_by_key}

    min_value = min(valid_values)
    max_value = max(valid_values)
    if max_value == min_value:
        return {key: 50.0 for key in values_by_key}

    scores = {}
    for key, value in values_by_key.items():
        if value is None:
            scores[key] = 0.0
            continue
        normalized = (float(value) - min_value) / (max_value - min_value) * 100
        if reverse:
            normalized = 100 - normalized
        scores[key] = round(normalized, 2)
    return scores


def safe_per_10k(value, population):
    if not population:
        return None
    return float(value) / float(population) * 10000


def safe_ratio(value, divisor):
    if not divisor:
        return None
    return float(value) / float(divisor)


def fetch_population_map(cursor):
    cursor.execute(
        """
        SELECT region, SUM(population_count) AS population_total
        FROM population_info
        WHERE age_group IS NULL
        GROUP BY region
        """
    )
    return {region: float(population_total or 0) for region, population_total in cursor.fetchall()}


def fetch_institution_map(cursor):
    cursor.execute(
        """
        SELECT region, year, institution_count, top_hospital_count
        FROM institution_yearly_summary
        """
    )
    yearly_summary_map = {
        (region, int(year)): {
            "institution_count": int(institution_count or 0),
            "top_hospital_count": int(top_hospital_count or 0),
        }
        for region, year, institution_count, top_hospital_count in cursor.fetchall()
    }

    cursor.execute(
        """
        SELECT
            region,
            COUNT(*) AS institution_count,
            SUM(CASE WHEN level LIKE '%三甲%' THEN 1 ELSE 0 END) AS top_hospital_count
        FROM medical_institution
        GROUP BY region
        """
    )
    return {
        region: {
            "institution_count": int(institution_count or 0),
            "top_hospital_count": int(top_hospital_count or 0),
        }
        for region, institution_count, top_hospital_count in cursor.fetchall()
    }, yearly_summary_map


def fetch_yearly_rows(cursor):
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
            avg_stay_days
        FROM ocr_metrics_yearly
        ORDER BY year, region
        """
    )
    rows = []
    for row in cursor.fetchall():
        rows.append(
            {
                "region": row[0],
                "analysis_year": int(row[1]),
                "doctor_count": float(row[2] or 0),
                "nurse_count": float(row[3] or 0),
                "bed_count": float(row[4] or 0),
                "outpatient_visits": float(row[5] or 0),
                "discharge_count": float(row[6] or 0),
                "avg_stay_days": float(row[7]) if row[7] is not None else None,
            }
        )
    return rows


def build_rows(yearly_rows, population_map, institution_map):
    base_rows = []
    for row in yearly_rows:
        region = row["region"]
        population = population_map.get(region)
        region_institution_map = institution_map[0]
        yearly_institution_map = institution_map[1]
        institution_stats = region_institution_map.get(region, {})
        yearly_stats = yearly_institution_map.get((region, row["analysis_year"]), {})
        institution_count = yearly_stats.get("institution_count", institution_stats.get("institution_count", 0))
        top_hospital_count = yearly_stats.get("top_hospital_count", institution_stats.get("top_hospital_count", 0))

        base_rows.append(
            {
                "region": region,
                "analysis_year": row["analysis_year"],
                "institution_count": institution_count,
                "top_hospital_count": top_hospital_count,
                "doctors_per_10k": safe_per_10k(row["doctor_count"], population),
                "nurses_per_10k": safe_per_10k(row["nurse_count"], population),
                "beds_per_10k": safe_per_10k(row["bed_count"], population),
                "avg_outpatient_per_doctor": safe_ratio(row["outpatient_visits"], row["doctor_count"] * 365 if row["doctor_count"] else 0),
                "bed_turnover_rate": safe_ratio(row["discharge_count"], row["bed_count"]),
                "avg_stay_days": row["avg_stay_days"],
            }
        )
    return base_rows


def attach_scores(base_rows):
    grouped = defaultdict(list)
    for row in base_rows:
        grouped[row["analysis_year"]].append(row)

    scored_rows = []
    for analysis_year, rows in grouped.items():
        doctors_scores = normalize_score({row["region"]: row["doctors_per_10k"] for row in rows})
        nurses_scores = normalize_score({row["region"]: row["nurses_per_10k"] for row in rows})
        beds_scores = normalize_score({row["region"]: row["beds_per_10k"] for row in rows})
        outpatient_scores = normalize_score({row["region"]: row["avg_outpatient_per_doctor"] for row in rows})
        turnover_scores = normalize_score({row["region"]: row["bed_turnover_rate"] for row in rows})
        stay_scores = normalize_score({row["region"]: row["avg_stay_days"] for row in rows}, reverse=True)

        for row in rows:
            region = row["region"]
            scored_rows.append(
                {
                    "region": region,
                    "analysis_year": row["analysis_year"],
                    "institution_count": row["institution_count"],
                    "top_hospital_count": row["top_hospital_count"],
                    "doctors_per_10k": round(float(row["doctors_per_10k"] or 0), 2),
                    "nurses_per_10k": round(float(row["nurses_per_10k"] or 0), 2),
                    "beds_per_10k": round(float(row["beds_per_10k"] or 0), 2),
                    "avg_outpatient_per_doctor": round(float(row["avg_outpatient_per_doctor"] or 0), 2),
                    "bed_turnover_rate": round(float(row["bed_turnover_rate"] or 0), 2),
                    "resource_score": round((doctors_scores[region] + nurses_scores[region] + beds_scores[region]) / 3, 2),
                    "service_score": round((outpatient_scores[region] + turnover_scores[region] + stay_scores[region]) / 3, 2),
                }
            )
    return scored_rows


def write_rows(cursor, rows):
    cursor.execute("TRUNCATE TABLE region_comparison")
    insert_sql = """
        INSERT INTO region_comparison (
            region,
            analysis_year,
            institution_count,
            top_hospital_count,
            doctors_per_10k,
            nurses_per_10k,
            beds_per_10k,
            avg_outpatient_per_doctor,
            bed_turnover_rate,
            resource_score,
            service_score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = [
        (
            row["region"],
            row["analysis_year"],
            row["institution_count"],
            row["top_hospital_count"],
            row["doctors_per_10k"],
            row["nurses_per_10k"],
            row["beds_per_10k"],
            row["avg_outpatient_per_doctor"],
            row["bed_turnover_rate"],
            row["resource_score"],
            row["service_score"],
        )
        for row in rows
    ]
    cursor.executemany(insert_sql, values)


def main():
    print("=" * 60)
    print("跨地区医疗资源对比分析器")
    print("=" * 60)

    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        population_map = fetch_population_map(cursor)
        institution_map = fetch_institution_map(cursor)
        yearly_rows = fetch_yearly_rows(cursor)
        base_rows = build_rows(yearly_rows, population_map, institution_map)
        scored_rows = attach_scores(base_rows)

        if not scored_rows:
            print("No region comparison rows generated")
            return

        write_rows(cursor, scored_rows)
        conn.commit()
        cursor.close()
        print("region_comparison updated")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
