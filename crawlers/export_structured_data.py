"""
导出结构化健康指标数据到 JSON / CSV
"""

import csv
import json
import os
import re
from datetime import datetime

import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "outputs")

SOURCE_LABELS = {
    "guangxi_news": {"province_code": "GX", "province_name": "广西", "source_name": "省级卫健委"},
    "sichuan_news": {"province_code": "SC", "province_name": "四川", "source_name": "省级卫健委"},
    "national_news": {"province_code": "NHC", "province_name": "国家", "source_name": "国家卫健委"},
}


def get_source_labels(source_table):
    return SOURCE_LABELS.get(
        source_table,
        {"province_code": "UNK", "province_name": "未知", "source_name": "未知来源"},
    )


def build_region_tag(source_tables):
    if len(source_tables) == 1:
        only_source = next(iter(source_tables))
        labels = get_source_labels(only_source)
        return labels["province_name"]
    if not source_tables:
        return "未知"
    return "多地区"


def parse_export_filename(filename):
    """解析导出文件名，兼容新旧两种命名格式。"""
    # 新格式: health_structured_广西_20260410_134045.json
    m = re.match(r"^health_structured_(.+)_(\d{8}_\d{6})\.(json|csv)$", filename)
    if m:
        region = m.group(1)
        timestamp = m.group(2)
        ext = m.group(3)
        return region, timestamp, ext

    # 旧格式: health_structured_20260410_115704.json
    m = re.match(r"^health_structured_(\d{8}_\d{6})\.(json|csv)$", filename)
    if m:
        timestamp = m.group(1)
        ext = m.group(2)
        # 旧数据历史上来自广西，归并到广西分组进行清理
        return "广西", timestamp, ext

    return None, None, None


def cleanup_old_exports():
    """仅保留每个地区最新一份导出（json/csv 配对保留）。"""
    files = [name for name in os.listdir(OUTPUT_DIR) if name.startswith("health_structured_")]

    grouped = {}
    for name in files:
        region, timestamp, ext = parse_export_filename(name)
        if not region or not timestamp or ext not in {"json", "csv"}:
            continue
        grouped.setdefault(region, {}).setdefault(timestamp, set()).add(ext)

    deleted = []
    for region, ts_map in grouped.items():
        latest_ts = max(ts_map.keys())
        for ts in ts_map.keys():
            if ts == latest_ts:
                continue
            for ext in ("json", "csv"):
                new_name = f"health_structured_{region}_{ts}.{ext}"
                old_name = f"health_structured_{ts}.{ext}"
                for candidate in (new_name, old_name):
                    candidate_path = os.path.join(OUTPUT_DIR, candidate)
                    if os.path.exists(candidate_path):
                        os.remove(candidate_path)
                        deleted.append(candidate)

    return deleted


def export_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(
            """
            SELECT
                news_id,
                source_table,
                title,
                publish_date,
                year,
                month,
                metric_key,
                metric_name,
                metric_value,
                metric_raw
            FROM health_ocr_metrics
            ORDER BY news_id, metric_key
            """
        )
        rows = cursor.fetchall()

        grouped = {}
        source_tables = set()
        for row in rows:
            news_id = int(row["news_id"])
            source_table = row.get("source_table") or "guangxi_news"
            source_labels = get_source_labels(source_table)
            source_tables.add(source_table)
            group_key = f"{source_table}:{news_id}"

            if group_key not in grouped:
                grouped[group_key] = {
                    "news_id": news_id,
                    "source_table": source_table,
                    "source_name": source_labels["source_name"],
                    "province_code": source_labels["province_code"],
                    "province_name": source_labels["province_name"],
                    "title": row["title"],
                    "publish_date": row.get("publish_date"),
                    "year": row.get("year"),
                    "month": row.get("month"),
                    "metrics": {},
                }

            grouped[group_key]["metrics"][row["metric_key"]] = {
                "metric_name": row["metric_name"],
                "value": float(row["metric_value"]) if row["metric_value"] is not None else None,
                "raw": row.get("metric_raw"),
            }

        data = list(grouped.values())
        region_tag = build_region_tag(source_tables)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join(OUTPUT_DIR, f"health_structured_{region_tag}_{timestamp}.json")
        csv_path = os.path.join(OUTPUT_DIR, f"health_structured_{region_tag}_{timestamp}.csv")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # 扁平化输出 CSV
        metric_keys = sorted({
            metric_key
            for item in data
            for metric_key in item["metrics"].keys()
        })

        fieldnames = [
            "news_id",
            "source_table",
            "source_name",
            "province_code",
            "province_name",
            "title",
            "publish_date",
            "year",
            "month",
        ] + metric_keys
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for item in data:
                row = {
                    "news_id": item["news_id"],
                    "source_table": item["source_table"],
                    "source_name": item["source_name"],
                    "province_code": item["province_code"],
                    "province_name": item["province_name"],
                    "title": item["title"],
                    "publish_date": item["publish_date"],
                    "year": item["year"],
                    "month": item["month"],
                }
                for key in metric_keys:
                    metric = item["metrics"].get(key)
                    row[key] = metric["value"] if metric else None
                writer.writerow(row)

        print(f"导出完成: {len(data)} 条记录")
        print(f"地区标记: {region_tag}")
        print(f"JSON: {json_path}")
        print(f"CSV : {csv_path}")

        deleted_files = cleanup_old_exports()
        if deleted_files:
            print(f"已清理旧导出文件: {len(deleted_files)} 个")
        else:
            print("无需清理旧导出文件")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    export_data()
