# -*- coding: utf-8 -*-
"""
六大模块与分析功能 API。
"""

from collections import defaultdict

from flask import jsonify, request
import mysql.connector


DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}

SCOPE_TO_REGION = {
    "guangxi": "广西",
    "national": "国家",
}

SCOPE_LABELS = {
    "all": "全部来源",
    "guangxi": "省级卫健委（广西）",
    "national": "国家卫健委",
}

RAW_REGION_SQL = """
CASE
    WHEN source_table = 'guangxi_news' THEN '广西'
    WHEN source_table = 'national_news' THEN '国家'
    ELSE source_table
END
"""


def init_analysis_api(app, mysql_instance):
    """初始化分析 API。"""
    app.add_url_rule("/api/analysis/population", "get_population_analysis", get_population_analysis, methods=["GET"])
    app.add_url_rule("/api/analysis/institutions", "get_institutions_analysis", get_institutions_analysis, methods=["GET"])
    app.add_url_rule("/api/analysis/personnel", "get_personnel_analysis", get_personnel_analysis, methods=["GET"])
    app.add_url_rule("/api/analysis/beds", "get_beds_analysis", get_beds_analysis, methods=["GET"])
    app.add_url_rule("/api/analysis/services", "get_services_analysis", get_services_analysis, methods=["GET"])
    app.add_url_rule("/api/analysis/costs", "get_costs_analysis", get_costs_analysis, methods=["GET"])
    app.add_url_rule("/api/metrics/yearly", "get_yearly_metrics", get_yearly_metrics, methods=["GET"])
    app.add_url_rule("/api/analysis/region-comparison", "get_region_comparison", get_region_comparison, methods=["GET"])
    app.add_url_rule("/api/prediction/results", "get_prediction_results", get_prediction_results, methods=["GET"])
    app.add_url_rule("/api/anomaly/alerts", "get_anomaly_alerts", get_anomaly_alerts, methods=["GET"])


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def success_response(data, meta=None, status=200):
    payload = {"success": True, "data": data}
    if meta is not None:
        payload["meta"] = meta
    return jsonify(payload), status


def error_response(message, error_code="INTERNAL_ERROR", status=500):
    return jsonify({"success": False, "error": message, "error_code": error_code}), status


def query_all(sql, params=None):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or [])
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        conn.close()


def query_one(sql, params=None):
    conn = get_db_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql, params or [])
        row = cursor.fetchone()
        cursor.close()
        return row
    finally:
        conn.close()


def table_has_rows(table_name):
    try:
        row = query_one(f"SELECT COUNT(*) AS total FROM `{table_name}`")
        return bool(row and row["total"])
    except mysql.connector.Error:
        return False


def to_float(value, digits=2):
    if value is None:
        return None
    return round(float(value), digits)


def to_int(value):
    if value is None:
        return 0
    return int(round(float(value)))


def sum_field(rows, key):
    return sum(float(row.get(key) or 0) for row in rows)


def average(values):
    valid = [float(value) for value in values if value is not None]
    if not valid:
        return None
    return sum(valid) / len(valid)


def weighted_average(rows, value_key, weight_key):
    weighted_sum = 0.0
    total_weight = 0.0
    for row in rows:
        value = row.get(value_key)
        weight = float(row.get(weight_key) or 0)
        if value is None or weight <= 0:
            continue
        weighted_sum += float(value) * weight
        total_weight += weight
    if total_weight <= 0:
        return None
    return weighted_sum / total_weight


def normalize_scope(scope):
    normalized = (scope or "all").strip().lower()
    if normalized not in {"all", "guangxi", "national"}:
        return None
    return normalized


def parse_positive_int(value, field_name):
    if value in (None, ""):
        return None, None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None, f"{field_name} 必须为正整数"
    if parsed <= 0:
        return None, f"{field_name} 必须为正整数"
    return parsed, None


def get_population_totals():
    rows = query_all(
        """
        SELECT region, SUM(population_count) AS population
        FROM population_info
        GROUP BY region
        """
    )
    return {row["region"]: float(row["population"] or 0) for row in rows}


def calculate_per_1000(value, population):
    if not population:
        return None
    return (float(value) / float(population)) * 1000


def latest_rows_by_region(rows, year_key="year"):
    latest = {}
    for row in rows:
        region = row.get("region")
        if not region:
            continue
        current = latest.get(region)
        year = row.get(year_key)
        if current is None or (year is not None and year > current.get(year_key)):
            latest[region] = row
    return [latest[key] for key in sorted(latest.keys())]


def aggregate_yearly_sum(rows, field_mapping):
    yearly = defaultdict(dict)
    for row in rows:
        year = row.get("year")
        if year is None:
            continue
        if "year" not in yearly[year]:
            yearly[year]["year"] = year
            for output_key in field_mapping.values():
                yearly[year][output_key] = 0.0
        for source_key, output_key in field_mapping.items():
            yearly[year][output_key] += float(row.get(source_key) or 0)
    return [yearly[year] for year in sorted(yearly.keys())]


def aggregate_yearly_weighted(rows, sum_keys, avg_key=None, weight_key=None):
    yearly = {}
    for row in rows:
        year = row.get("year")
        if year is None:
            continue
        bucket = yearly.setdefault(
            year,
            {
                "year": year,
                **{output_key: 0.0 for output_key in sum_keys.values()},
                "_weighted_sum": 0.0,
                "_total_weight": 0.0,
            },
        )
        for source_key, output_key in sum_keys.items():
            bucket[output_key] += float(row.get(source_key) or 0)
        if avg_key and weight_key:
            value = row.get(avg_key)
            weight = float(row.get(weight_key) or 0)
            if value is not None and weight > 0:
                bucket["_weighted_sum"] += float(value) * weight
                bucket["_total_weight"] += weight

    result = []
    for year in sorted(yearly.keys()):
        item = yearly[year]
        if avg_key and weight_key:
            item_name = avg_key.replace("avg_", "")
            item[item_name] = item["_weighted_sum"] / item["_total_weight"] if item["_total_weight"] > 0 else None
        item.pop("_weighted_sum", None)
        item.pop("_total_weight", None)
        result.append(item)
    return result


def get_gender_ratio(region=None):
    sql = """
        SELECT gender, SUM(population_count) AS population
        FROM population_info
    """
    params = []
    if region:
        sql += " WHERE region = %s"
        params.append(region)
    sql += " GROUP BY gender"
    rows = query_all(sql, params)

    male_keys = {"男", "男性", "male", "Male", "M", "m"}
    female_keys = {"女", "女性", "female", "Female", "F", "f"}
    male = 0.0
    female = 0.0

    for row in rows:
        gender = (row.get("gender") or "").strip()
        value = float(row.get("population") or 0)
        if gender in male_keys:
            male += value
        elif gender in female_keys:
            female += value

    total = male + female
    if total <= 0:
        return {"male": 0, "female": 0}
    return {"male": round((male / total) * 100, 2), "female": round((female / total) * 100, 2)}


def get_population_analysis():
    """人口信息统计分析。"""
    try:
        region = (request.args.get("region") or "").strip() or None

        if region or not table_has_rows("analysis_population_region"):
            params = []
            by_region_sql = """
                SELECT region, SUM(population_count) AS population
                FROM population_info
            """
            if region:
                by_region_sql += " WHERE region = %s"
                params.append(region)
            by_region_sql += " GROUP BY region ORDER BY population DESC"
            by_region = query_all(by_region_sql, params)

            by_age_sql = """
                SELECT age_group, SUM(population_count) AS population
                FROM population_info
            """
            if region:
                by_age_sql += " WHERE region = %s"
            by_age_sql += " GROUP BY age_group ORDER BY age_group"
            by_age = query_all(by_age_sql, params)
        else:
            by_region = query_all(
                """
                SELECT region, total_population AS population
                FROM analysis_population_region
                ORDER BY total_population DESC
                """
            )
            by_age = query_all(
                """
                SELECT age_group, total_population AS population
                FROM analysis_population_age
                ORDER BY age_group
                """
            )

        data = {
            "total_population": to_int(sum_field(by_region, "population")),
            "by_region": [{"region": row["region"], "population": to_int(row["population"])} for row in by_region],
            "by_age_group": [{"age_group": row["age_group"], "population": to_int(row["population"])} for row in by_age],
            "gender_ratio": get_gender_ratio(region),
        }
        return success_response(data)
    except Exception as exc:
        return error_response(str(exc))


def get_institutions_analysis():
    """医疗卫生机构统计分析。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        institution_type = (request.args.get("type") or "").strip() or None
        level = (request.args.get("level") or "").strip() or None

        use_result_tables = (
            not region
            and not institution_type
            and not level
            and table_has_rows("analysis_institution_type")
            and table_has_rows("analysis_institution_level")
            and table_has_rows("analysis_institution_region")
        )

        if use_result_tables:
            by_type = query_all(
                """
                SELECT type, institution_count AS count
                FROM analysis_institution_type
                ORDER BY institution_count DESC
                """
            )
            by_level = query_all(
                """
                SELECT level, institution_count AS count
                FROM analysis_institution_level
                ORDER BY institution_count DESC
                """
            )
            by_region = query_all(
                """
                SELECT region, institution_count AS count
                FROM analysis_institution_region
                ORDER BY institution_count DESC
                """
            )
            total_count = to_int(sum_field(by_type, "count"))
        else:
            where_clauses = []
            params = []
            if region:
                where_clauses.append("region = %s")
                params.append(region)
            if institution_type:
                where_clauses.append("type = %s")
                params.append(institution_type)
            if level:
                where_clauses.append("level = %s")
                params.append(level)

            where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
            total_row = query_one(f"SELECT COUNT(*) AS total_count FROM medical_institution{where_sql}", params)
            total_count = to_int(total_row["total_count"] if total_row else 0)
            by_type = query_all(
                f"""
                SELECT type, COUNT(*) AS count
                FROM medical_institution
                {where_sql}
                GROUP BY type
                ORDER BY count DESC
                """,
                params,
            )
            by_level = query_all(
                f"""
                SELECT level, COUNT(*) AS count
                FROM medical_institution
                {where_sql}
                GROUP BY level
                ORDER BY count DESC
                """,
                params,
            )
            by_region = query_all(
                f"""
                SELECT region, COUNT(*) AS count
                FROM medical_institution
                {where_sql}
                GROUP BY region
                ORDER BY count DESC
                """,
                params,
            )

        data = {
            "total_count": total_count,
            "by_type": [{"type": row["type"], "count": to_int(row["count"])} for row in by_type],
            "by_level": [{"level": row["level"], "count": to_int(row["count"])} for row in by_level],
            "by_region": [{"region": row["region"], "count": to_int(row["count"])} for row in by_region],
        }
        return success_response(data)
    except Exception as exc:
        return error_response(str(exc))


def fetch_personnel_rows(region=None, year=None):
    if table_has_rows("analysis_personnel"):
        sql = """
            SELECT region, year, doctor_count, nurse_count
            FROM analysis_personnel
        """
        params = []
        where_clauses = []
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        if year:
            where_clauses.append("year = %s")
            params.append(year)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY year ASC, region ASC"
        rows = query_all(sql, params)
        if rows:
            return rows

    sql = f"""
        SELECT
            {RAW_REGION_SQL} AS region,
            year,
            SUM(CASE WHEN metric_key = 'doctor_count' THEN metric_value ELSE 0 END) AS doctor_count,
            SUM(CASE WHEN metric_key = 'nurse_count' THEN metric_value ELSE 0 END) AS nurse_count
        FROM health_ocr_metrics
        WHERE metric_key IN ('doctor_count', 'nurse_count')
          AND year IS NOT NULL
    """
    params = []
    if region:
        sql += f" AND {RAW_REGION_SQL} = %s"
        params.append(region)
    if year:
        sql += " AND year = %s"
        params.append(year)
    sql += f" GROUP BY {RAW_REGION_SQL}, year ORDER BY year ASC, region ASC"
    return query_all(sql, params)


def get_personnel_analysis():
    """医疗卫生人员统计分析。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        rows = fetch_personnel_rows(region, year)
        population_totals = get_population_totals()
        snapshot_rows = rows if year else latest_rows_by_region(rows)
        total_population = sum(population_totals.get(row["region"], 0) for row in snapshot_rows)
        total_doctors = to_int(sum_field(snapshot_rows, "doctor_count"))
        total_nurses = to_int(sum_field(snapshot_rows, "nurse_count"))
        ratio = f"1:{round(total_nurses / total_doctors, 2)}" if total_doctors else "0:0"

        by_region = []
        for row in snapshot_rows:
            region_population = population_totals.get(row["region"])
            by_region.append(
                {
                    "region": row["region"],
                    "year": row["year"],
                    "doctors": to_int(row["doctor_count"]),
                    "nurses": to_int(row["nurse_count"]),
                    "doctors_per_1000": to_float(calculate_per_1000(row.get("doctor_count") or 0, region_population), 2),
                }
            )

        yearly_trend_raw = aggregate_yearly_sum(rows, {"doctor_count": "doctors", "nurse_count": "nurses"})
        yearly_trend = [{"year": item["year"], "doctors": to_int(item["doctors"]), "nurses": to_int(item["nurses"])} for item in yearly_trend_raw]

        data = {
            "total_doctors": total_doctors,
            "total_nurses": total_nurses,
            "doctor_nurse_ratio": ratio,
            "doctors_per_1000": to_float(calculate_per_1000(total_doctors, total_population), 2),
            "by_region": by_region,
            "yearly_trend": yearly_trend,
        }
        return success_response(data)
    except Exception as exc:
        return error_response(str(exc))


def fetch_bed_rows(region=None, year=None):
    if table_has_rows("analysis_beds"):
        sql = """
            SELECT region, year, bed_count, avg_usage_rate
            FROM analysis_beds
        """
        params = []
        where_clauses = []
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        if year:
            where_clauses.append("year = %s")
            params.append(year)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY year ASC, region ASC"
        rows = query_all(sql, params)
        if rows:
            return rows

    sql = f"""
        SELECT
            {RAW_REGION_SQL} AS region,
            year,
            SUM(CASE WHEN metric_key = 'bed_count' THEN metric_value ELSE 0 END) AS bed_count,
            AVG(CASE WHEN metric_key = 'bed_usage_rate' THEN metric_value ELSE NULL END) AS avg_usage_rate
        FROM health_ocr_metrics
        WHERE metric_key IN ('bed_count', 'bed_usage_rate')
          AND year IS NOT NULL
    """
    params = []
    if region:
        sql += f" AND {RAW_REGION_SQL} = %s"
        params.append(region)
    if year:
        sql += " AND year = %s"
        params.append(year)
    sql += f" GROUP BY {RAW_REGION_SQL}, year ORDER BY year ASC, region ASC"
    return query_all(sql, params)


def get_beds_analysis():
    """医疗卫生床位统计分析。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        rows = fetch_bed_rows(region, year)
        population_totals = get_population_totals()
        snapshot_rows = rows if year else latest_rows_by_region(rows)
        total_population = sum(population_totals.get(row["region"], 0) for row in snapshot_rows)

        by_region = []
        for row in snapshot_rows:
            region_population = population_totals.get(row["region"])
            by_region.append(
                {
                    "region": row["region"],
                    "year": row["year"],
                    "beds": to_int(row["bed_count"]),
                    "usage_rate": to_float(row["avg_usage_rate"], 2),
                    "beds_per_1000": to_float(calculate_per_1000(row.get("bed_count") or 0, region_population), 2),
                }
            )

        yearly_trend_raw = aggregate_yearly_weighted(rows, {"bed_count": "beds"}, avg_key="avg_usage_rate", weight_key="bed_count")
        yearly_trend = [{"year": item["year"], "beds": to_int(item["beds"]), "usage_rate": to_float(item.get("usage_rate"), 2)} for item in yearly_trend_raw]

        data = {
            "total_beds": to_int(sum_field(snapshot_rows, "bed_count")),
            "avg_usage_rate": to_float(weighted_average(snapshot_rows, "avg_usage_rate", "bed_count"), 2),
            "beds_per_1000": to_float(calculate_per_1000(sum_field(snapshot_rows, "bed_count"), total_population), 2),
            "by_region": by_region,
            "yearly_trend": yearly_trend,
        }
        return success_response(data)
    except Exception as exc:
        return error_response(str(exc))


def fetch_service_rows(region=None, year=None):
    if table_has_rows("analysis_services"):
        sql = """
            SELECT region, year, outpatient_visits, discharge_count, avg_stay_days
            FROM analysis_services
        """
        params = []
        where_clauses = []
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        if year:
            where_clauses.append("year = %s")
            params.append(year)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY year ASC, region ASC"
        rows = query_all(sql, params)
        if rows:
            return rows

    sql = f"""
        SELECT
            {RAW_REGION_SQL} AS region,
            year,
            SUM(CASE WHEN metric_key = 'outpatient_visits' THEN metric_value ELSE 0 END) AS outpatient_visits,
            SUM(CASE WHEN metric_key = 'discharge_count' THEN metric_value ELSE 0 END) AS discharge_count,
            AVG(CASE WHEN metric_key = 'avg_stay_days' THEN metric_value ELSE NULL END) AS avg_stay_days
        FROM health_ocr_metrics
        WHERE metric_key IN ('outpatient_visits', 'discharge_count', 'avg_stay_days')
          AND year IS NOT NULL
    """
    params = []
    if region:
        sql += f" AND {RAW_REGION_SQL} = %s"
        params.append(region)
    if year:
        sql += " AND year = %s"
        params.append(year)
    sql += f" GROUP BY {RAW_REGION_SQL}, year ORDER BY year ASC, region ASC"
    return query_all(sql, params)


def calculate_outpatient_per_doctor_per_day(service_rows, region=None, year=None):
    personnel_rows = fetch_personnel_rows(region=region, year=year)
    personnel_index = {(row["region"], row["year"]): float(row.get("doctor_count") or 0) for row in personnel_rows}

    total_visits = 0.0
    total_doctors = 0.0
    for row in service_rows:
        key = (row["region"], row["year"])
        doctor_count = personnel_index.get(key, 0.0)
        if doctor_count <= 0:
            continue
        total_visits += float(row.get("outpatient_visits") or 0)
        total_doctors += doctor_count

    if total_doctors <= 0:
        return None
    return total_visits / total_doctors / 365


def get_services_analysis():
    """医疗服务统计分析。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        rows = fetch_service_rows(region, year)
        snapshot_rows = rows if year else latest_rows_by_region(rows)
        by_region = [
            {
                "region": row["region"],
                "year": row["year"],
                "outpatient_visits": to_int(row["outpatient_visits"]),
                "discharge": to_int(row["discharge_count"]),
                "avg_stay_days": to_float(row["avg_stay_days"], 2),
            }
            for row in snapshot_rows
        ]

        yearly_trend_raw = aggregate_yearly_weighted(
            rows,
            {"outpatient_visits": "outpatient", "discharge_count": "discharge"},
            avg_key="avg_stay_days",
            weight_key="discharge_count",
        )
        yearly_trend = [
            {
                "year": item["year"],
                "outpatient": to_int(item["outpatient"]),
                "discharge": to_int(item["discharge"]),
                "avg_stay_days": to_float(item.get("stay_days"), 2),
            }
            for item in yearly_trend_raw
        ]

        data = {
            "total_outpatient_visits": to_int(sum_field(snapshot_rows, "outpatient_visits")),
            "total_discharge": to_int(sum_field(snapshot_rows, "discharge_count")),
            "avg_stay_days": to_float(weighted_average(snapshot_rows, "avg_stay_days", "discharge_count"), 2),
            "outpatient_per_doctor_per_day": to_float(calculate_outpatient_per_doctor_per_day(snapshot_rows, region, year), 2),
            "by_region": by_region,
            "yearly_trend": yearly_trend,
        }
        return success_response(data)
    except Exception as exc:
        return error_response(str(exc))


def fetch_cost_rows(region=None, year=None):
    if table_has_rows("analysis_costs"):
        sql = """
            SELECT region, year, avg_outpatient_cost, avg_discharge_cost
            FROM analysis_costs
        """
        params = []
        where_clauses = []
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        if year:
            where_clauses.append("year = %s")
            params.append(year)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY year ASC, region ASC"
        rows = query_all(sql, params)
        if rows:
            return rows

    sql = f"""
        SELECT
            {RAW_REGION_SQL} AS region,
            year,
            AVG(CASE WHEN metric_key = 'outpatient_cost' THEN metric_value ELSE NULL END) AS avg_outpatient_cost,
            AVG(CASE WHEN metric_key = 'discharge_cost' THEN metric_value ELSE NULL END) AS avg_discharge_cost
        FROM health_ocr_metrics
        WHERE metric_key IN ('outpatient_cost', 'discharge_cost')
          AND year IS NOT NULL
    """
    params = []
    if region:
        sql += f" AND {RAW_REGION_SQL} = %s"
        params.append(region)
    if year:
        sql += " AND year = %s"
        params.append(year)
    sql += f" GROUP BY {RAW_REGION_SQL}, year ORDER BY year ASC, region ASC"
    return query_all(sql, params)


def calculate_cost_growth_rate(yearly_trend):
    if len(yearly_trend) < 2:
        return None
    current = yearly_trend[-1].get("outpatient_cost")
    previous = yearly_trend[-2].get("outpatient_cost")
    if current is None or previous in (None, 0):
        return None
    return ((float(current) - float(previous)) / float(previous)) * 100


def get_costs_analysis():
    """医疗费用统计分析。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        rows = fetch_cost_rows(region, year)
        snapshot_rows = rows if year else latest_rows_by_region(rows)
        by_region = [
            {
                "region": row["region"],
                "year": row["year"],
                "outpatient_cost": to_float(row["avg_outpatient_cost"], 2),
                "discharge_cost": to_float(row["avg_discharge_cost"], 2),
            }
            for row in snapshot_rows
        ]

        yearly_buckets = defaultdict(lambda: {"year": None, "outpatient_values": [], "discharge_values": []})
        for row in rows:
            year_value = row.get("year")
            if year_value is None:
                continue
            yearly_buckets[year_value]["year"] = year_value
            if row.get("avg_outpatient_cost") is not None:
                yearly_buckets[year_value]["outpatient_values"].append(float(row["avg_outpatient_cost"]))
            if row.get("avg_discharge_cost") is not None:
                yearly_buckets[year_value]["discharge_values"].append(float(row["avg_discharge_cost"]))

        yearly_trend = []
        for year_value in sorted(yearly_buckets.keys()):
            bucket = yearly_buckets[year_value]
            yearly_trend.append(
                {
                    "year": year_value,
                    "outpatient_cost": to_float(average(bucket["outpatient_values"]), 2),
                    "discharge_cost": to_float(average(bucket["discharge_values"]), 2),
                }
            )

        data = {
            "avg_outpatient_cost": to_float(average([row.get("avg_outpatient_cost") for row in snapshot_rows]), 2),
            "avg_discharge_cost": to_float(average([row.get("avg_discharge_cost") for row in snapshot_rows]), 2),
            "cost_growth_rate": to_float(calculate_cost_growth_rate(yearly_trend), 2),
            "by_region": by_region,
            "yearly_trend": yearly_trend,
        }
        return success_response(data)
    except Exception as exc:
        return error_response(str(exc))


def get_yearly_metrics():
    """年度指标汇总。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        scope = normalize_scope(request.args.get("scope"))
        if scope is None:
            return error_response("scope 参数不合法", "INVALID_PARAMETER", 400)

        where_clauses = []
        params = []
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        elif scope != "all":
            where_clauses.append("region = %s")
            params.append(SCOPE_TO_REGION[scope])
        if year:
            where_clauses.append("year = %s")
            params.append(year)

        sql = """
            SELECT
                region,
                year,
                doctor_count,
                nurse_count,
                bed_count,
                bed_usage_rate,
                outpatient_visits,
                discharge_count,
                avg_stay_days,
                outpatient_cost,
                discharge_cost,
                data_source,
                sample_count
            FROM ocr_metrics_yearly
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY year DESC, region ASC"

        rows = query_all(sql, params)
        data = [
            {
                "region": row["region"],
                "year": row["year"],
                "doctor_count": to_int(row["doctor_count"]),
                "nurse_count": to_int(row["nurse_count"]),
                "bed_count": to_int(row["bed_count"]),
                "bed_usage_rate": to_float(row["bed_usage_rate"], 2),
                "outpatient_visits": to_int(row["outpatient_visits"]),
                "discharge_count": to_int(row["discharge_count"]),
                "avg_stay_days": to_float(row["avg_stay_days"], 2),
                "outpatient_cost": to_float(row["outpatient_cost"], 2),
                "discharge_cost": to_float(row["discharge_cost"], 2),
                "data_source": row["data_source"],
                "sample_count": to_int(row["sample_count"]),
            }
            for row in rows
        ]
        meta = {"total": len(data), "scope": scope, "scope_label": SCOPE_LABELS.get(scope, scope)}
        return success_response(data, meta=meta)
    except Exception as exc:
        return error_response(str(exc))


def get_region_comparison():
    """跨地区对比分析。"""
    try:
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        if year is None:
            latest = query_one("SELECT MAX(analysis_year) AS analysis_year FROM region_comparison")
            year = latest["analysis_year"] if latest else None
        if year is None:
            return success_response([], meta={"total": 0})

        rows = query_all(
            """
            SELECT
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
            FROM region_comparison
            WHERE analysis_year = %s
            ORDER BY resource_score DESC, service_score DESC
            """,
            [year],
        )

        data = [
            {
                "region": row["region"],
                "analysis_year": row["analysis_year"],
                "institution_count": to_int(row["institution_count"]),
                "top_hospital_count": to_int(row["top_hospital_count"]),
                "doctors_per_10k": to_float(row["doctors_per_10k"], 2),
                "nurses_per_10k": to_float(row["nurses_per_10k"], 2),
                "beds_per_10k": to_float(row["beds_per_10k"], 2),
                "avg_outpatient_per_doctor": to_float(row["avg_outpatient_per_doctor"], 2),
                "bed_turnover_rate": to_float(row["bed_turnover_rate"], 2),
                "resource_score": to_float(row["resource_score"], 2),
                "service_score": to_float(row["service_score"], 2),
            }
            for row in rows
        ]
        return success_response(data, meta={"total": len(data), "analysis_year": year})
    except Exception as exc:
        return error_response(str(exc))


def get_prediction_results():
    """预测分析结果。"""
    try:
        region = (request.args.get("region") or "").strip() or None
        metric_key = (request.args.get("metric_key") or "").strip() or None
        years, years_error = parse_positive_int(request.args.get("years"), "years")
        if years_error:
            return error_response(years_error, "INVALID_PARAMETER", 400)

        where_clauses = []
        params = []
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        if metric_key:
            where_clauses.append("metric_key = %s")
            params.append(metric_key)

        sql = """
            SELECT
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
            FROM prediction_results
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " ORDER BY predict_year ASC, region ASC, metric_key ASC"

        rows = query_all(sql, params)
        if years:
            year_set = sorted({row["predict_year"] for row in rows})
            allowed_years = set(year_set[:years])
            rows = [row for row in rows if row["predict_year"] in allowed_years]

        data = [
            {
                "region": row["region"],
                "metric_key": row["metric_key"],
                "metric_name": row["metric_name"],
                "predict_year": row["predict_year"],
                "predict_value": to_float(row["predict_value"], 2),
                "confidence_lower": to_float(row["confidence_lower"], 2),
                "confidence_upper": to_float(row["confidence_upper"], 2),
                "model_type": row["model_type"],
                "model_accuracy": to_float(row["model_accuracy"], 4),
                "training_data_range": row["training_data_range"],
            }
            for row in rows
        ]
        return success_response(data, meta={"total": len(data)})
    except Exception as exc:
        return error_response(str(exc))


def get_anomaly_alerts():
    """异常检测预警。"""
    try:
        level = (request.args.get("level") or "").strip() or None
        region = (request.args.get("region") or "").strip() or None
        metric_key = (request.args.get("metric_key") or "").strip() or None
        year, year_error = parse_positive_int(request.args.get("year"), "year")
        if year_error:
            return error_response(year_error, "INVALID_PARAMETER", 400)

        limit, limit_error = parse_positive_int(request.args.get("limit"), "limit")
        if limit_error:
            return error_response(limit_error, "INVALID_PARAMETER", 400)
        limit = limit or 10
        if limit > 100:
            return error_response("limit 不能大于 100", "INVALID_PARAMETER", 400)
        if level and level not in {"warning", "critical"}:
            return error_response("level 只能为 warning 或 critical", "INVALID_PARAMETER", 400)

        where_clauses = []
        params = []
        if level:
            where_clauses.append("anomaly_level = %s")
            params.append(level)
        else:
            where_clauses.append("anomaly_level IN ('warning', 'critical')")
        if region:
            where_clauses.append("region = %s")
            params.append(region)
        if metric_key:
            where_clauses.append("metric_key = %s")
            params.append(metric_key)
        if year:
            where_clauses.append("year = %s")
            params.append(year)

        sql = """
            SELECT
                region,
                metric_key,
                year,
                actual_value,
                expected_value,
                deviation_rate,
                anomaly_level,
                description
            FROM anomaly_detection
            WHERE
        """
        sql += " AND ".join(where_clauses)
        sql += " ORDER BY ABS(deviation_rate) DESC LIMIT %s"
        params.append(limit)

        rows = query_all(sql, params)
        data = [
            {
                "region": row["region"],
                "metric_key": row["metric_key"],
                "year": row["year"],
                "actual_value": to_float(row["actual_value"], 2),
                "expected_value": to_float(row["expected_value"], 2),
                "deviation_rate": to_float(row["deviation_rate"], 2),
                "anomaly_level": row["anomaly_level"],
                "description": row["description"],
            }
            for row in rows
        ]
        return success_response(data, meta={"total": len(data), "limit": limit})
    except Exception as exc:
        return error_response(str(exc))
