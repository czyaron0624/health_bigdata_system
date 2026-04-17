#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成与当前项目 schema 对齐的伪造数据集。

默认只落地到 outputs，不直接覆盖数据库；如需导入 MySQL，请显式传入
`--seed-db`，并可搭配 `--truncate-db`。
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any

try:
    import mysql.connector  # type: ignore
except Exception:  # pragma: no cover
    mysql = None


BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = BASE_DIR / "outputs"

DEFAULT_START_YEAR = 2018
DEFAULT_END_YEAR = 2024

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}

METRIC_LABELS = {
    "avg_stay_days": "出院者平均住院日",
    "bed_count": "实有床位数",
    "bed_usage_rate": "病床使用率",
    "discharge_cost": "出院病人人均医药费用",
    "discharge_count": "出院人数",
    "doctor_count": "执业(助理)医师数",
    "nurse_count": "注册护士数",
    "outpatient_cost": "门诊病人次均医药费用",
    "outpatient_visits": "总诊疗人次数",
}

METRIC_EXPORT_ORDER = [
    "avg_stay_days",
    "bed_count",
    "bed_usage_rate",
    "discharge_cost",
    "discharge_count",
    "doctor_count",
    "nurse_count",
    "outpatient_cost",
    "outpatient_visits",
]

REGION_CONFIGS = [
    {
        "region": "广西",
        "alias": "全区",
        "source_table": "guangxi_news",
        "province_code": "GX",
        "province_name": "广西",
        "source_name": "省级卫健委",
        "source_category": "广西壮族自治区卫生健康委员会",
        "population_base": 50_400_000,
        "population_growth": -0.0025,
        "doctor_per_1000_base": 2.95,
        "doctor_per_1000_step": 0.055,
        "nurse_ratio_base": 1.34,
        "nurse_ratio_step": 0.01,
        "beds_per_1000_base": 6.10,
        "beds_per_1000_step": 0.07,
        "visit_per_capita_base": 5.10,
        "visit_per_capita_step": 0.10,
        "discharge_per_capita_base": 0.187,
        "discharge_per_capita_step": 0.003,
        "avg_stay_base": 8.80,
        "avg_stay_step": -0.10,
        "bed_usage_base": 78.50,
        "bed_usage_step": 0.60,
        "outpatient_cost_base": 265.0,
        "outpatient_cost_growth": 0.047,
        "discharge_cost_base": 9450.0,
        "discharge_cost_growth": 0.055,
        "institution_count": 1600,
        "top_hospital_ratio": 0.018,
        "age_shares": {"0-14": 0.205, "15-64": 0.649, "65+": 0.146},
        "gender_shares": {
            "0-14": {"男": 0.524, "女": 0.476},
            "15-64": {"男": 0.512, "女": 0.488},
            "65+": {"男": 0.472, "女": 0.528},
        },
        "cities": [
            "南宁市",
            "柳州市",
            "桂林市",
            "梧州市",
            "北海市",
            "防城港市",
            "钦州市",
            "贵港市",
            "玉林市",
            "百色市",
            "贺州市",
            "河池市",
            "来宾市",
            "崇左市",
        ],
    },
    {
        "region": "四川",
        "alias": "全省",
        "source_table": "sichuan_news",
        "province_code": "SC",
        "province_name": "四川",
        "source_name": "省级卫健委",
        "source_category": "四川省卫生健康委员会",
        "population_base": 83_600_000,
        "population_growth": -0.0032,
        "doctor_per_1000_base": 3.25,
        "doctor_per_1000_step": 0.06,
        "nurse_ratio_base": 1.38,
        "nurse_ratio_step": 0.012,
        "beds_per_1000_base": 6.55,
        "beds_per_1000_step": 0.08,
        "visit_per_capita_base": 5.55,
        "visit_per_capita_step": 0.12,
        "discharge_per_capita_base": 0.208,
        "discharge_per_capita_step": 0.004,
        "avg_stay_base": 8.30,
        "avg_stay_step": -0.08,
        "bed_usage_base": 80.80,
        "bed_usage_step": 0.55,
        "outpatient_cost_base": 288.0,
        "outpatient_cost_growth": 0.05,
        "discharge_cost_base": 10250.0,
        "discharge_cost_growth": 0.057,
        "institution_count": 2100,
        "top_hospital_ratio": 0.022,
        "age_shares": {"0-14": 0.168, "15-64": 0.646, "65+": 0.186},
        "gender_shares": {
            "0-14": {"男": 0.525, "女": 0.475},
            "15-64": {"男": 0.509, "女": 0.491},
            "65+": {"男": 0.469, "女": 0.531},
        },
        "cities": [
            "成都市",
            "自贡市",
            "攀枝花市",
            "泸州市",
            "德阳市",
            "绵阳市",
            "广元市",
            "遂宁市",
            "内江市",
            "乐山市",
            "南充市",
            "眉山市",
            "宜宾市",
            "广安市",
            "达州市",
            "雅安市",
        ],
    },
    {
        "region": "国家",
        "alias": "全国",
        "source_table": "national_news",
        "province_code": "NHC",
        "province_name": "国家",
        "source_name": "国家卫健委",
        "source_category": "国家卫生健康委员会",
        "population_base": 1411000000,
        "population_growth": -0.0010,
        "doctor_per_1000_base": 3.05,
        "doctor_per_1000_step": 0.05,
        "nurse_ratio_base": 1.28,
        "nurse_ratio_step": 0.01,
        "beds_per_1000_base": 6.35,
        "beds_per_1000_step": 0.06,
        "visit_per_capita_base": 6.10,
        "visit_per_capita_step": 0.08,
        "discharge_per_capita_base": 0.185,
        "discharge_per_capita_step": 0.003,
        "avg_stay_base": 8.10,
        "avg_stay_step": -0.08,
        "bed_usage_base": 79.60,
        "bed_usage_step": 0.45,
        "outpatient_cost_base": 310.0,
        "outpatient_cost_growth": 0.053,
        "discharge_cost_base": 11800.0,
        "discharge_cost_growth": 0.06,
        "institution_count": 1000,
        "top_hospital_ratio": 0.03,
        "age_shares": {"0-14": 0.171, "15-64": 0.663, "65+": 0.166},
        "gender_shares": {
            "0-14": {"男": 0.526, "女": 0.474},
            "15-64": {"男": 0.511, "女": 0.489},
            "65+": {"男": 0.476, "女": 0.524},
        },
        "cities": [
            "北京市",
            "上海市",
            "天津市",
            "重庆市",
            "广州市",
            "深圳市",
            "武汉市",
            "杭州市",
            "南京市",
            "西安市",
        ],
    },
]

INSTITUTION_TYPE_CONFIG = [
    ("综合医院", 0.20),
    ("中医医院", 0.10),
    ("专科医院", 0.08),
    ("妇幼保健院", 0.05),
    ("基层卫生院", 0.24),
    ("社区卫生服务中心", 0.15),
    ("疾病预防控制中心", 0.03),
    ("康复医院", 0.04),
    ("口腔医院", 0.05),
    ("精神专科医院", 0.03),
    ("护理院", 0.03),
]

TYPE_NAME_PATTERNS = {
    "综合医院": ["人民医院", "中心医院", "总医院"],
    "中医医院": ["中医医院", "中西医结合医院"],
    "专科医院": ["肿瘤医院", "儿童医院", "胸科医院"],
    "妇幼保健院": ["妇幼保健院"],
    "基层卫生院": ["卫生院", "中心卫生院"],
    "社区卫生服务中心": ["社区卫生服务中心"],
    "疾病预防控制中心": ["疾病预防控制中心"],
    "康复医院": ["康复医院"],
    "口腔医院": ["口腔医院"],
    "精神专科医院": ["精神卫生中心", "精神专科医院"],
    "护理院": ["护理院"],
}

LEVEL_WEIGHTS = {
    "综合医院": [("三级甲等", 0.14), ("三级", 0.32), ("二级甲等", 0.24), ("二级", 0.18), ("一级", 0.12)],
    "中医医院": [("三级甲等", 0.10), ("三级", 0.26), ("二级甲等", 0.28), ("二级", 0.22), ("一级", 0.14)],
    "专科医院": [("三级甲等", 0.08), ("三级", 0.24), ("二级甲等", 0.26), ("二级", 0.24), ("一级", 0.18)],
    "妇幼保健院": [("三级甲等", 0.06), ("三级", 0.28), ("二级甲等", 0.30), ("二级", 0.24), ("一级", 0.12)],
    "基层卫生院": [("二级", 0.10), ("一级", 0.70), ("未定级", 0.20)],
    "社区卫生服务中心": [("二级", 0.08), ("一级", 0.64), ("未定级", 0.28)],
    "疾病预防控制中心": [("三级", 0.08), ("二级", 0.32), ("一级", 0.40), ("未定级", 0.20)],
    "康复医院": [("三级", 0.10), ("二级甲等", 0.30), ("二级", 0.34), ("一级", 0.18), ("未定级", 0.08)],
    "口腔医院": [("三级", 0.10), ("二级甲等", 0.22), ("二级", 0.36), ("一级", 0.22), ("未定级", 0.10)],
    "精神专科医院": [("三级", 0.18), ("二级甲等", 0.26), ("二级", 0.34), ("一级", 0.14), ("未定级", 0.08)],
    "护理院": [("二级", 0.20), ("一级", 0.50), ("未定级", 0.30)],
}

TABLE_ORDERS = {
    "medical_institution": ["id", "name", "type", "region", "level", "create_time"],
    "population_info": ["id", "region", "age_group", "gender", "population_count", "create_time"],
    "hospital_bed": ["id", "institution_id", "total_count", "occupied_count"],
    "population_data": ["id", "name", "age", "district", "health_score", "create_time"],
    "guangxi_news": ["id", "title", "link", "publish_date", "source_category", "ocr_content", "detail_context", "created_at", "updated_at"],
    "sichuan_news": ["id", "title", "link", "publish_date", "source_category", "ocr_content", "detail_context", "created_at", "updated_at"],
    "national_news": ["id", "title", "link", "source_category", "publish_date", "ocr_content", "detail_context", "created_at", "updated_at"],
    "report_metrics": ["id", "report_id", "metric_name", "metric_value", "created_at"],
    "health_ocr_metrics": [
        "id", "news_id", "title", "publish_date", "year", "month", "metric_key", "metric_name",
        "metric_value", "metric_raw", "source_table", "context_json", "evidence_json", "created_at", "updated_at",
    ],
    "institution_yearly_summary": [
        "id", "region", "year", "institution_count", "top_hospital_count", "hospital_health_center_count",
        "community_health_center_count", "township_health_center_count", "sanatorium_count", "clinic_count",
        "cdc_count", "health_supervision_count", "special_disease_center_count", "maternal_child_center_count",
        "research_institution_count", "other_institution_count", "data_source", "is_estimated", "notes",
        "created_at", "updated_at",
    ],
    "analysis_population_region": ["id", "region", "total_population", "metric_type", "created_at", "updated_at"],
    "analysis_population_age": ["id", "age_group", "total_population", "metric_type", "created_at", "updated_at"],
    "analysis_population_gender": ["id", "gender", "total_population", "metric_type", "created_at", "updated_at"],
    "analysis_institution_type": ["id", "type", "institution_count", "metric_type", "created_at", "updated_at"],
    "analysis_institution_level": ["id", "level", "institution_count", "metric_type", "created_at", "updated_at"],
    "analysis_institution_region": ["id", "region", "institution_count", "metric_type", "created_at", "updated_at"],
    "analysis_personnel": ["id", "region", "year", "doctor_count", "nurse_count", "doctor_nurse_ratio", "created_at", "updated_at"],
    "analysis_beds": ["id", "region", "year", "bed_count", "avg_usage_rate", "created_at", "updated_at"],
    "analysis_services": ["id", "region", "year", "outpatient_visits", "discharge_count", "avg_stay_days", "created_at", "updated_at"],
    "analysis_costs": ["id", "region", "year", "avg_outpatient_cost", "avg_discharge_cost", "created_at", "updated_at"],
    "ocr_metrics_yearly": [
        "id", "region", "year", "doctor_count", "nurse_count", "bed_count", "bed_usage_rate",
        "outpatient_visits", "discharge_count", "avg_stay_days", "outpatient_cost", "discharge_cost",
        "data_source", "sample_count", "created_at", "updated_at",
    ],
    "region_comparison": [
        "id", "region", "analysis_year", "institution_count", "top_hospital_count", "doctors_per_10k",
        "nurses_per_10k", "beds_per_10k", "avg_outpatient_per_doctor", "bed_turnover_rate",
        "resource_score", "service_score", "created_at",
    ],
    "prediction_results": [
        "id", "region", "metric_key", "metric_name", "predict_year", "predict_value", "confidence_lower",
        "confidence_upper", "model_type", "model_accuracy", "training_data_range", "created_at",
    ],
    "anomaly_detection": [
        "id", "region", "metric_key", "year", "actual_value", "expected_value", "deviation_rate",
        "anomaly_level", "description", "created_at",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic healthcare datasets aligned to this project.")
    parser.add_argument("--years", default=f"{DEFAULT_START_YEAR}-{DEFAULT_END_YEAR}", help="Year range, e.g. 2018-2024")
    parser.add_argument("--seed", type=int, default=20260417, help="Random seed")
    parser.add_argument("--output-dir", default=None, help="Custom output directory")
    parser.add_argument("--seed-db", action="store_true", help="Insert generated rows into MySQL tables")
    parser.add_argument("--truncate-db", action="store_true", help="Delete table rows before MySQL seeding")
    return parser.parse_args()


def parse_year_range(raw: str) -> tuple[int, int]:
    parts = raw.split("-", 1)
    if len(parts) != 2:
        raise ValueError("--years must be in START-END format")
    start_year = int(parts[0])
    end_year = int(parts[1])
    if start_year > end_year:
        raise ValueError("start year must be <= end year")
    return start_year, end_year


def weighted_choice(rng: random.Random, weighted_items: list[tuple[Any, float]]) -> Any:
    total = sum(weight for _, weight in weighted_items)
    cursor = rng.random() * total
    upto = 0.0
    for item, weight in weighted_items:
        upto += weight
        if upto >= cursor:
            return item
    return weighted_items[-1][0]


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def round_int(value: float) -> int:
    return int(round(value))


def round_float(value: float, digits: int = 2) -> float:
    return round(float(value), digits)


def timestamp_for_year(year: int, month: int = 12, day: int = 31) -> str:
    return f"{year:04d}-{month:02d}-{day:02d} 09:00:00"


def build_region_year_profiles(start_year: int, end_year: int, rng: random.Random) -> dict[tuple[str, int], dict[str, Any]]:
    profiles: dict[tuple[str, int], dict[str, Any]] = {}
    for config in REGION_CONFIGS:
        for year in range(start_year, end_year + 1):
            step = year - start_year
            population = config["population_base"] * ((1 + config["population_growth"]) ** step)
            doctor_rate = config["doctor_per_1000_base"] + step * config["doctor_per_1000_step"]
            nurse_ratio = config["nurse_ratio_base"] + step * config["nurse_ratio_step"]
            bed_rate = config["beds_per_1000_base"] + step * config["beds_per_1000_step"]
            visits_per_capita = config["visit_per_capita_base"] + step * config["visit_per_capita_step"]
            discharge_per_capita = config["discharge_per_capita_base"] + step * config["discharge_per_capita_step"]
            avg_stay = config["avg_stay_base"] + step * config["avg_stay_step"]
            bed_usage = config["bed_usage_base"] + step * config["bed_usage_step"]
            outpatient_cost = config["outpatient_cost_base"] * ((1 + config["outpatient_cost_growth"]) ** step)
            discharge_cost = config["discharge_cost_base"] * ((1 + config["discharge_cost_growth"]) ** step)

            if year == 2020:
                visits_per_capita *= 0.82
                discharge_per_capita *= 0.88
                bed_usage -= 9.5
                avg_stay += 0.45
            elif year == 2021:
                visits_per_capita *= 0.95
                discharge_per_capita *= 0.97
                bed_usage -= 2.5
            if config["region"] == "四川" and year == end_year:
                outpatient_cost *= 1.12
                discharge_cost *= 1.08
            if config["region"] == "广西" and year == end_year - 1:
                bed_usage -= 4.2
            if config["region"] == "国家" and year == end_year:
                discharge_per_capita *= 1.07

            profiles[(config["region"], year)] = {
                "population_total": round_int(population),
                "metrics": {
                    "doctor_count": round_int(population / 1000 * doctor_rate * rng.uniform(0.985, 1.015)),
                    "nurse_count": round_int(population / 1000 * doctor_rate * nurse_ratio * rng.uniform(0.985, 1.018)),
                    "bed_count": round_int(population / 1000 * bed_rate * rng.uniform(0.988, 1.014)),
                    "bed_usage_rate": round_float(clamp(bed_usage * rng.uniform(0.985, 1.015), 45.0, 95.0), 2),
                    "outpatient_visits": round_int(population * visits_per_capita * rng.uniform(0.975, 1.025)),
                    "discharge_count": round_int(population * discharge_per_capita * rng.uniform(0.978, 1.022)),
                    "avg_stay_days": round_float(clamp(avg_stay * rng.uniform(0.99, 1.01), 4.0, 15.0), 2),
                    "outpatient_cost": round_float(outpatient_cost * rng.uniform(0.985, 1.02), 2),
                    "discharge_cost": round_float(discharge_cost * rng.uniform(0.985, 1.025), 2),
                },
                "config": config,
            }
    return profiles


def generate_population_info(end_year: int, profiles: dict[tuple[str, int], dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    row_id = 1
    for config in REGION_CONFIGS:
        population_total = profiles[(config["region"], end_year)]["population_total"]
        created_at = timestamp_for_year(end_year)
        for age_group, age_share in config["age_shares"].items():
            age_population = population_total * age_share
            for gender, gender_share in config["gender_shares"][age_group].items():
                rows.append(
                    {
                        "id": row_id,
                        "region": config["region"],
                        "age_group": age_group,
                        "gender": gender,
                        "population_count": round_int(age_population * gender_share),
                        "create_time": created_at,
                    }
                )
                row_id += 1
    return rows


def build_institution_name(region_config: dict[str, Any], city: str, institution_type: str, serial: int, rng: random.Random) -> str:
    suffix = weighted_choice(rng, [(pattern, 1.0) for pattern in TYPE_NAME_PATTERNS[institution_type]])
    if institution_type in {"基层卫生院", "社区卫生服务中心"}:
        district_no = (serial % 12) + 1
        district = f"{district_no}号片区" if institution_type == "社区卫生服务中心" else f"{district_no}镇"
        return f"{city}{district}{suffix}"
    if institution_type == "疾病预防控制中心":
        return f"{city}{suffix}"
    if institution_type == "中医医院":
        return f"{city}第{(serial % 6) + 1}中医医院"
    if institution_type == "综合医院" and suffix == "人民医院":
        return f"{city}第{(serial % 8) + 1}人民医院"
    return f"{city}{suffix}"


def generate_medical_institutions(rng: random.Random) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    row_id = 1
    for config in REGION_CONFIGS:
        counters: Counter[tuple[str, str]] = Counter()
        city_weights = [(city, 1.0 + idx * 0.08) for idx, city in enumerate(config["cities"])]
        for _ in range(config["institution_count"]):
            institution_type = weighted_choice(rng, INSTITUTION_TYPE_CONFIG)
            level = weighted_choice(rng, LEVEL_WEIGHTS[institution_type])
            city = weighted_choice(rng, city_weights)
            counters[(city, institution_type)] += 1
            rows.append(
                {
                    "id": row_id,
                    "name": build_institution_name(config, city, institution_type, counters[(city, institution_type)], rng),
                    "type": institution_type,
                    "region": config["region"],
                    "level": level,
                    "create_time": timestamp_for_year(2024, month=((row_id % 11) + 1), day=((row_id % 27) + 1)),
                }
            )
            row_id += 1
    return rows


def generate_hospital_beds(institutions: list[dict[str, Any]], rng: random.Random) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row_id, institution in enumerate(institutions, start=1):
        institution_type = institution["type"]
        level = institution["level"]
        if institution_type == "综合医院":
            base = {"三级甲等": 950, "三级": 650, "二级甲等": 360, "二级": 220, "一级": 110}.get(level, 80)
        elif institution_type in {"中医医院", "专科医院", "精神专科医院"}:
            base = {"三级甲等": 750, "三级": 520, "二级甲等": 280, "二级": 180, "一级": 85}.get(level, 70)
        elif institution_type in {"妇幼保健院", "康复医院", "护理院"}:
            base = {"三级甲等": 420, "三级": 300, "二级甲等": 180, "二级": 120, "一级": 60}.get(level, 50)
        elif institution_type in {"基层卫生院", "社区卫生服务中心"}:
            base = {"二级": 90, "一级": 55, "未定级": 28}.get(level, 35)
        else:
            base = {"三级": 60, "二级": 30, "一级": 18, "未定级": 10}.get(level, 15)
        total_count = max(8, round_int(base * rng.uniform(0.80, 1.25)))
        occupied_count = min(total_count, round_int(total_count * clamp(rng.uniform(0.62, 0.93), 0.50, 0.96)))
        rows.append({"id": row_id, "institution_id": institution["id"], "total_count": total_count, "occupied_count": occupied_count})
    return rows


def generate_population_data(rng: random.Random) -> list[dict[str, Any]]:
    surnames = ["张", "王", "李", "刘", "陈", "杨", "赵", "黄", "周", "吴", "徐", "孙"]
    given_names = ["伟", "芳", "娜", "敏", "静", "磊", "洋", "艳", "强", "军", "婷", "杰", "勇", "娟"]
    districts = ["南宁青秀区", "南宁良庆区", "柳州城中区", "桂林七星区", "成都高新区", "成都武侯区", "绵阳涪城区", "北京朝阳区", "上海浦东新区", "广州天河区"]
    rows: list[dict[str, Any]] = []
    for row_id in range(1, 1201):
        age = int(clamp(rng.gauss(46, 16), 18, 89))
        health_score = int(clamp(92 - age * 0.35 + rng.gauss(0, 8), 32, 98))
        rows.append(
            {
                "id": row_id,
                "name": f"{rng.choice(surnames)}{rng.choice(given_names)}",
                "age": age,
                "district": rng.choice(districts),
                "health_score": health_score,
                "create_time": timestamp_for_year(2024, month=(row_id % 12) + 1, day=(row_id % 28) + 1),
            }
        )
    return rows


def format_metric_raw(metric_key: str, value: float | int) -> str:
    if metric_key == "bed_usage_rate":
        return f"{round_float(value, 2)}%"
    if metric_key in {"outpatient_cost", "discharge_cost"}:
        return f"{round_float(value, 2)}元"
    if metric_key == "avg_stay_days":
        return f"{round_float(value, 2)}天"
    return str(int(round(value)))


def build_ocr_content(title: str, region: str, year: int, metrics: dict[str, Any]) -> str:
    return (
        f"{title}。{region}{year}年医疗卫生服务运行总体平稳，"
        f"执业(助理)医师数约 {metrics['doctor_count']:,} 人，注册护士数约 {metrics['nurse_count']:,} 人，"
        f"实有床位数约 {metrics['bed_count']:,} 张，病床使用率 {metrics['bed_usage_rate']:.2f}%。"
        f"全年总诊疗人次数约 {metrics['outpatient_visits']:,} 人次，出院人数约 {metrics['discharge_count']:,} 人。"
    )


def build_detail_context(region: str, year: int, metrics: dict[str, Any]) -> str:
    return (
        f"{region}{year}年卫生资源配置与医疗服务数据摘要："
        f"门诊病人次均医药费用 {metrics['outpatient_cost']:.2f} 元，"
        f"出院病人人均医药费用 {metrics['discharge_cost']:.2f} 元，"
        f"出院者平均住院日 {metrics['avg_stay_days']:.2f} 天。"
    )


def generate_news_and_metrics(
    start_year: int,
    end_year: int,
    profiles: dict[tuple[str, int], dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    news_tables = {"guangxi_news": [], "sichuan_news": [], "national_news": []}
    report_metrics: list[dict[str, Any]] = []
    health_ocr_metrics: list[dict[str, Any]] = []
    metric_row_id = 1
    report_metric_id = 1
    table_id_offsets = {"guangxi_news": 100000, "sichuan_news": 200000, "national_news": 300000}

    for config in REGION_CONFIGS:
        table_name = config["source_table"]
        for year in range(start_year, end_year + 1):
            profile = profiles[(config["region"], year)]
            metrics = profile["metrics"]
            news_id = table_id_offsets[table_name] + (year - start_year + 1)
            if config["region"] == "国家":
                title = f"{year}年全国医疗卫生服务统计公报"
                publish_date = f"{year + 1}-03-18"
            else:
                title = f"{year}年{config['alias']}医疗卫生机构医疗服务情况"
                publish_date = f"{year + 1}-02-2{(year - start_year) % 7}"

            created_at = f"{publish_date} 10:00:00"
            common_row = {
                "id": news_id,
                "title": title,
                "link": f"https://synthetic.health.example/{table_name}/{year}",
                "publish_date": publish_date,
                "source_category": config["source_category"],
                "ocr_content": build_ocr_content(title, config["region"], year, metrics),
                "detail_context": build_detail_context(config["region"], year, metrics),
                "created_at": created_at,
                "updated_at": created_at,
            }
            if table_name == "national_news":
                news_tables[table_name].append(
                    {
                        "id": common_row["id"],
                        "title": common_row["title"],
                        "link": common_row["link"],
                        "source_category": common_row["source_category"],
                        "publish_date": common_row["publish_date"],
                        "ocr_content": common_row["ocr_content"],
                        "detail_context": common_row["detail_context"],
                        "created_at": common_row["created_at"],
                        "updated_at": common_row["updated_at"],
                    }
                )
            else:
                news_tables[table_name].append(common_row)

            for metric_key in METRIC_EXPORT_ORDER:
                metric_value = metrics[metric_key]
                health_ocr_metrics.append(
                    {
                        "id": metric_row_id,
                        "news_id": news_id,
                        "title": title,
                        "publish_date": publish_date,
                        "year": year,
                        "month": 12,
                        "metric_key": metric_key,
                        "metric_name": METRIC_LABELS[metric_key],
                        "metric_value": metric_value,
                        "metric_raw": format_metric_raw(metric_key, metric_value),
                        "source_table": table_name,
                        "context_json": json.dumps(
                            {"region": config["region"], "year": year, "title": title, "detail_context": common_row["detail_context"]},
                            ensure_ascii=False,
                        ),
                        "evidence_json": json.dumps(
                            {"source_sentence": f"{METRIC_LABELS[metric_key]}为 {format_metric_raw(metric_key, metric_value)}", "extraction_method": "synthetic_rule_engine"},
                            ensure_ascii=False,
                        ),
                        "created_at": created_at,
                        "updated_at": created_at,
                    }
                )
                metric_row_id += 1

            if table_name == "national_news":
                extra_metrics = [
                    ("卫生总费用", f"{round_float(metrics['discharge_cost'] * metrics['discharge_count'] / 100000000, 2)}亿元"),
                    ("每千人口执业医师数", f"{round_float(metrics['doctor_count'] / profile['population_total'] * 1000, 2)}"),
                    ("每千人口注册护士数", f"{round_float(metrics['nurse_count'] / profile['population_total'] * 1000, 2)}"),
                    ("每千人口床位数", f"{round_float(metrics['bed_count'] / profile['population_total'] * 1000, 2)}"),
                    ("门诊次均费用", f"{round_float(metrics['outpatient_cost'], 2)}元"),
                    ("住院人均费用", f"{round_float(metrics['discharge_cost'], 2)}元"),
                ]
                for metric_name, metric_value in extra_metrics:
                    report_metrics.append(
                        {
                            "id": report_metric_id,
                            "report_id": news_id,
                            "metric_name": metric_name,
                            "metric_value": metric_value,
                            "created_at": created_at,
                        }
                    )
                    report_metric_id += 1
    return news_tables, report_metrics, health_ocr_metrics


def generate_institution_yearly_summary(institutions: list[dict[str, Any]], start_year: int, end_year: int) -> list[dict[str, Any]]:
    current_by_region = defaultdict(list)
    for row in institutions:
        current_by_region[row["region"]].append(row)

    type_to_column = {
        "综合医院": "hospital_health_center_count",
        "中医医院": "hospital_health_center_count",
        "专科医院": "hospital_health_center_count",
        "妇幼保健院": "maternal_child_center_count",
        "基层卫生院": "township_health_center_count",
        "社区卫生服务中心": "community_health_center_count",
        "疾病预防控制中心": "cdc_count",
        "康复医院": "sanatorium_count",
        "口腔医院": "clinic_count",
        "精神专科医院": "special_disease_center_count",
        "护理院": "other_institution_count",
    }

    rows: list[dict[str, Any]] = []
    row_id = 1
    for config in REGION_CONFIGS:
        current_rows = current_by_region[config["region"]]
        current_total = len(current_rows)
        current_counts = Counter(type_to_column[row["type"]] for row in current_rows)
        growth_back = 0.026 if config["region"] != "国家" else 0.018
        for year in range(start_year, end_year + 1):
            distance = end_year - year
            institution_count = round_int(current_total / ((1 + growth_back) ** distance))
            row = {
                "id": row_id,
                "region": config["region"],
                "year": year,
                "institution_count": institution_count,
                "top_hospital_count": max(1, round_int(institution_count * config["top_hospital_ratio"])),
                "hospital_health_center_count": 0,
                "community_health_center_count": 0,
                "township_health_center_count": 0,
                "sanatorium_count": 0,
                "clinic_count": 0,
                "cdc_count": 0,
                "health_supervision_count": max(4, round_int(institution_count * 0.012)),
                "special_disease_center_count": 0,
                "maternal_child_center_count": 0,
                "research_institution_count": max(2, round_int(institution_count * 0.005)),
                "other_institution_count": 0,
                "data_source": "synthetic",
                "is_estimated": 0,
                "notes": None,
                "created_at": timestamp_for_year(year),
                "updated_at": timestamp_for_year(year),
            }
            for column_name, current_value in current_counts.items():
                share = current_value / current_total if current_total else 0
                row[column_name] = round_int(institution_count * share)
            rows.append(row)
            row_id += 1
    return rows


def build_analysis_population_tables(population_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_region = defaultdict(int)
    by_age = defaultdict(int)
    by_gender = defaultdict(int)
    for row in population_rows:
        by_region[row["region"]] += int(row["population_count"])
        by_age[row["age_group"]] += int(row["population_count"])
        by_gender[row["gender"]] += int(row["population_count"])
    created_at = timestamp_for_year(2024)
    return {
        "analysis_population_region": [
            {"id": idx, "region": region, "total_population": total_population, "metric_type": "by_region", "created_at": created_at, "updated_at": created_at}
            for idx, (region, total_population) in enumerate(sorted(by_region.items()), start=1)
        ],
        "analysis_population_age": [
            {"id": idx, "age_group": age_group, "total_population": by_age[age_group], "metric_type": "by_age_group", "created_at": created_at, "updated_at": created_at}
            for idx, age_group in enumerate(["0-14", "15-64", "65+"], start=1)
        ],
        "analysis_population_gender": [
            {"id": idx, "gender": gender, "total_population": by_gender[gender], "metric_type": "by_gender", "created_at": created_at, "updated_at": created_at}
            for idx, gender in enumerate(["男", "女"], start=1)
        ],
    }


def build_analysis_institution_tables(institutions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    by_type = Counter(row["type"] for row in institutions)
    by_level = Counter(row["level"] for row in institutions)
    by_region = Counter(row["region"] for row in institutions)
    created_at = timestamp_for_year(2024)
    return {
        "analysis_institution_type": [
            {"id": idx, "type": institution_type, "institution_count": count, "metric_type": "by_type", "created_at": created_at, "updated_at": created_at}
            for idx, (institution_type, count) in enumerate(by_type.most_common(), start=1)
        ],
        "analysis_institution_level": [
            {"id": idx, "level": level, "institution_count": count, "metric_type": "by_level", "created_at": created_at, "updated_at": created_at}
            for idx, (level, count) in enumerate(by_level.most_common(), start=1)
        ],
        "analysis_institution_region": [
            {"id": idx, "region": region, "institution_count": count, "metric_type": "by_region", "created_at": created_at, "updated_at": created_at}
            for idx, (region, count) in enumerate(by_region.most_common(), start=1)
        ],
    }


def aggregate_yearly_metrics(health_ocr_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], dict[str, Any]] = {}
    source_to_region = {config["source_table"]: config["region"] for config in REGION_CONFIGS}
    for row in health_ocr_metrics:
        region = source_to_region[row["source_table"]]
        year = int(row["year"])
        bucket = grouped.setdefault(
            (region, year),
            {
                "region": region,
                "year": year,
                "doctor_count": 0.0,
                "nurse_count": 0.0,
                "bed_count": 0.0,
                "outpatient_visits": 0.0,
                "discharge_count": 0.0,
                "bed_usage_rate_values": [],
                "avg_stay_days_values": [],
                "outpatient_cost_values": [],
                "discharge_cost_values": [],
                "sample_news_ids": set(),
                "source_tables": set(),
            },
        )
        metric_key = row["metric_key"]
        metric_value = float(row["metric_value"])
        bucket["sample_news_ids"].add(row["news_id"])
        bucket["source_tables"].add(row["source_table"])
        if metric_key in {"doctor_count", "nurse_count", "bed_count", "outpatient_visits", "discharge_count"}:
            bucket[metric_key] += metric_value
        elif metric_key == "bed_usage_rate":
            bucket["bed_usage_rate_values"].append(metric_value)
        elif metric_key == "avg_stay_days":
            bucket["avg_stay_days_values"].append(metric_value)
        elif metric_key == "outpatient_cost":
            bucket["outpatient_cost_values"].append(metric_value)
        elif metric_key == "discharge_cost":
            bucket["discharge_cost_values"].append(metric_value)

    rows: list[dict[str, Any]] = []
    row_id = 1
    for region, year in sorted(grouped.keys(), key=lambda item: (item[0], item[1])):
        bucket = grouped[(region, year)]
        source_tables = sorted(bucket["source_tables"])
        rows.append(
            {
                "id": row_id,
                "region": region,
                "year": year,
                "doctor_count": round_int(bucket["doctor_count"]),
                "nurse_count": round_int(bucket["nurse_count"]),
                "bed_count": round_int(bucket["bed_count"]),
                "bed_usage_rate": round_float(sum(bucket["bed_usage_rate_values"]) / len(bucket["bed_usage_rate_values"]), 2),
                "outpatient_visits": round_int(bucket["outpatient_visits"]),
                "discharge_count": round_int(bucket["discharge_count"]),
                "avg_stay_days": round_float(sum(bucket["avg_stay_days_values"]) / len(bucket["avg_stay_days_values"]), 2),
                "outpatient_cost": round_float(sum(bucket["outpatient_cost_values"]) / len(bucket["outpatient_cost_values"]), 2),
                "discharge_cost": round_float(sum(bucket["discharge_cost_values"]) / len(bucket["discharge_cost_values"]), 2),
                "data_source": source_tables[0] if len(source_tables) == 1 else "mixed",
                "sample_count": len(bucket["sample_news_ids"]),
                "created_at": timestamp_for_year(year + 1 if year < 2099 else year),
                "updated_at": timestamp_for_year(year + 1 if year < 2099 else year),
            }
        )
        row_id += 1
    return rows


def build_analysis_metric_tables(ocr_yearly: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    tables = {"analysis_personnel": [], "analysis_beds": [], "analysis_services": [], "analysis_costs": []}
    for row_id, row in enumerate(sorted(ocr_yearly, key=lambda item: (item["region"], item["year"])), start=1):
        created_at = row["created_at"]
        tables["analysis_personnel"].append(
            {
                "id": row_id,
                "region": row["region"],
                "year": row["year"],
                "doctor_count": row["doctor_count"],
                "nurse_count": row["nurse_count"],
                "doctor_nurse_ratio": round_float(row["nurse_count"] / row["doctor_count"], 2) if row["doctor_count"] else None,
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        tables["analysis_beds"].append(
            {
                "id": row_id,
                "region": row["region"],
                "year": row["year"],
                "bed_count": row["bed_count"],
                "avg_usage_rate": row["bed_usage_rate"],
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        tables["analysis_services"].append(
            {
                "id": row_id,
                "region": row["region"],
                "year": row["year"],
                "outpatient_visits": row["outpatient_visits"],
                "discharge_count": row["discharge_count"],
                "avg_stay_days": row["avg_stay_days"],
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
        tables["analysis_costs"].append(
            {
                "id": row_id,
                "region": row["region"],
                "year": row["year"],
                "avg_outpatient_cost": row["outpatient_cost"],
                "avg_discharge_cost": row["discharge_cost"],
                "created_at": created_at,
                "updated_at": created_at,
            }
        )
    return tables


def normalize_scores(values: dict[str, float | None], reverse: bool = False) -> dict[str, float]:
    valid = [value for value in values.values() if value is not None]
    if not valid:
        return {key: 50.0 for key in values}
    low = min(valid)
    high = max(valid)
    if math.isclose(low, high):
        return {key: 50.0 for key in values}
    scores = {}
    for key, value in values.items():
        if value is None:
            scores[key] = 0.0
            continue
        normalized = (float(value) - low) / (high - low) * 100
        if reverse:
            normalized = 100 - normalized
        scores[key] = round_float(normalized, 2)
    return scores


def build_region_comparison(ocr_yearly: list[dict[str, Any]], population_rows: list[dict[str, Any]], institution_summary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    population_totals = defaultdict(int)
    for row in population_rows:
        population_totals[row["region"]] += int(row["population_count"])
    institution_map = {(row["region"], row["year"]): row for row in institution_summary}
    by_year: defaultdict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in ocr_yearly:
        population = population_totals.get(row["region"])
        if not population:
            continue
        summary = institution_map.get((row["region"], row["year"]), {})
        by_year[row["year"]].append(
            {
                "region": row["region"],
                "analysis_year": row["year"],
                "institution_count": summary.get("institution_count", 0),
                "top_hospital_count": summary.get("top_hospital_count", 0),
                "doctors_per_10k": row["doctor_count"] / population * 10000,
                "nurses_per_10k": row["nurse_count"] / population * 10000,
                "beds_per_10k": row["bed_count"] / population * 10000,
                "avg_outpatient_per_doctor": row["outpatient_visits"] / (row["doctor_count"] * 365) if row["doctor_count"] else None,
                "bed_turnover_rate": row["discharge_count"] / row["bed_count"] if row["bed_count"] else None,
                "avg_stay_days": row["avg_stay_days"],
                "created_at": row["created_at"],
            }
        )

    rows: list[dict[str, Any]] = []
    row_id = 1
    for year in sorted(by_year.keys()):
        current_rows = by_year[year]
        doctor_scores = normalize_scores({row["region"]: row["doctors_per_10k"] for row in current_rows})
        nurse_scores = normalize_scores({row["region"]: row["nurses_per_10k"] for row in current_rows})
        bed_scores = normalize_scores({row["region"]: row["beds_per_10k"] for row in current_rows})
        outpatient_scores = normalize_scores({row["region"]: row["avg_outpatient_per_doctor"] for row in current_rows})
        turnover_scores = normalize_scores({row["region"]: row["bed_turnover_rate"] for row in current_rows})
        stay_scores = normalize_scores({row["region"]: row["avg_stay_days"] for row in current_rows}, reverse=True)
        for row in sorted(current_rows, key=lambda item: item["region"]):
            region = row["region"]
            rows.append(
                {
                    "id": row_id,
                    "region": region,
                    "analysis_year": year,
                    "institution_count": row["institution_count"],
                    "top_hospital_count": row["top_hospital_count"],
                    "doctors_per_10k": round_float(row["doctors_per_10k"], 2),
                    "nurses_per_10k": round_float(row["nurses_per_10k"], 2),
                    "beds_per_10k": round_float(row["beds_per_10k"], 2),
                    "avg_outpatient_per_doctor": round_float(row["avg_outpatient_per_doctor"] or 0, 2),
                    "bed_turnover_rate": round_float(row["bed_turnover_rate"] or 0, 2),
                    "resource_score": round_float((doctor_scores[region] + nurse_scores[region] + bed_scores[region]) / 3, 2),
                    "service_score": round_float((outpatient_scores[region] + turnover_scores[region] + stay_scores[region]) / 3, 2),
                    "created_at": row["created_at"],
                }
            )
            row_id += 1
    return rows


def linear_regression(points: list[tuple[int, float]]) -> dict[str, float]:
    x_values = [x for x, _ in points]
    y_values = [y for _, y in points]
    count = len(points)
    mean_x = sum(x_values) / count
    mean_y = sum(y_values) / count
    denominator = sum((x - mean_x) ** 2 for x in x_values)
    slope = 0.0 if math.isclose(denominator, 0.0) else sum((x - mean_x) * (y - mean_y) for x, y in points) / denominator
    intercept = mean_y - slope * mean_x
    predictions = [intercept + slope * x for x in x_values]
    residuals = [actual - predicted for actual, predicted in zip(y_values, predictions)]
    ss_total = sum((y - mean_y) ** 2 for y in y_values)
    ss_res = sum(residual ** 2 for residual in residuals)
    r2 = 1.0 if math.isclose(ss_total, 0.0) else clamp(1 - (ss_res / ss_total), 0.0, 1.0)
    residual_std = math.sqrt(sum(residual ** 2 for residual in residuals) / max(1, count - 2))
    return {"slope": slope, "intercept": intercept, "r2": r2, "residual_std": residual_std}


def build_prediction_results(ocr_yearly: list[dict[str, Any]], forecast_years: int = 3) -> list[dict[str, Any]]:
    metric_fields = ["doctor_count", "nurse_count", "bed_count", "outpatient_visits", "discharge_count", "outpatient_cost", "discharge_cost"]
    region_groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ocr_yearly:
        region_groups[row["region"]].append(row)
    for region in region_groups:
        region_groups[region] = sorted(region_groups[region], key=lambda item: item["year"])
    results: list[dict[str, Any]] = []
    row_id = 1
    for region, rows in sorted(region_groups.items()):
        for metric_key in metric_fields:
            points = [(int(row["year"]), float(row[metric_key])) for row in rows if row.get(metric_key) not in (None, 0)]
            if len(points) < 3:
                continue
            model = linear_regression(points)
            last_year = points[-1][0]
            training_range = f"{points[0][0]}-{points[-1][0]}"
            for step in range(1, forecast_years + 1):
                predict_year = last_year + step
                predict_value = max(0.0, model["intercept"] + model["slope"] * predict_year)
                margin = max(abs(predict_value) * 0.06, 1.96 * model["residual_std"] * (1 + 0.18 * (step - 1)))
                results.append(
                    {
                        "id": row_id,
                        "region": region,
                        "metric_key": metric_key,
                        "metric_name": METRIC_LABELS[metric_key],
                        "predict_year": predict_year,
                        "predict_value": round_float(predict_value, 2),
                        "confidence_lower": round_float(max(0.0, predict_value - margin), 2),
                        "confidence_upper": round_float(max(0.0, predict_value + margin), 2),
                        "model_type": "linear_regression",
                        "model_accuracy": round_float(model["r2"], 4),
                        "training_data_range": training_range,
                        "created_at": timestamp_for_year(last_year + 1, month=1, day=15),
                    }
                )
                row_id += 1
    return results


def build_anomaly_detection(ocr_yearly: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metric_fields = ["doctor_count", "nurse_count", "bed_count", "outpatient_visits", "discharge_count", "outpatient_cost", "discharge_cost"]
    region_groups: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in ocr_yearly:
        region_groups[row["region"]].append(row)
    for region in region_groups:
        region_groups[region] = sorted(region_groups[region], key=lambda item: item["year"])
    results: list[dict[str, Any]] = []
    row_id = 1
    for region, rows in sorted(region_groups.items()):
        for index, row in enumerate(rows):
            for metric_key in metric_fields:
                actual_value = float(row[metric_key])
                history = [float(prev[metric_key]) for prev in rows[max(0, index - 3):index] if float(prev[metric_key]) > 0]
                if len(history) < 3:
                    continue
                expected_value = float(median(history))
                deviation_rate = ((actual_value - expected_value) / expected_value) * 100 if expected_value else 0.0
                abs_rate = abs(deviation_rate)
                anomaly_level = "critical" if abs_rate >= 50 else "warning" if abs_rate >= 20 else "normal"
                direction = "高于" if deviation_rate >= 0 else "低于"
                results.append(
                    {
                        "id": row_id,
                        "region": region,
                        "metric_key": metric_key,
                        "year": row["year"],
                        "actual_value": round_float(actual_value, 2),
                        "expected_value": round_float(expected_value, 2),
                        "deviation_rate": round_float(deviation_rate, 4),
                        "anomaly_level": anomaly_level,
                        "description": f"{region}{row['year']}年{METRIC_LABELS[metric_key]}实际值 {round_float(actual_value, 2)}，{direction}近三年中位基线 {round_float(expected_value, 2)}，偏离 {round_float(abs_rate, 2)}%",
                        "created_at": row["created_at"],
                    }
                )
                row_id += 1
    return results


def build_structured_export_rows(health_ocr_metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], dict[str, Any]] = {}
    source_labels = {
        config["source_table"]: {
            "province_code": config["province_code"],
            "province_name": config["province_name"],
            "source_name": config["source_name"],
        }
        for config in REGION_CONFIGS
    }
    for row in sorted(health_ocr_metrics, key=lambda item: (item["source_table"], item["news_id"], item["metric_key"])):
        key = (row["source_table"], row["news_id"])
        labels = source_labels[row["source_table"]]
        bucket = grouped.setdefault(
            key,
            {
                "news_id": row["news_id"],
                "source_table": row["source_table"],
                "source_name": labels["source_name"],
                "province_code": labels["province_code"],
                "province_name": labels["province_name"],
                "title": row["title"],
                "publish_date": row["publish_date"],
                "year": row["year"],
                "month": row["month"],
                "metrics": {},
            },
        )
        bucket["metrics"][row["metric_key"]] = {"metric_name": row["metric_name"], "value": row["metric_value"], "raw": row["metric_raw"]}
    return list(grouped.values())


def ensure_output_dir(custom_output_dir: str | None) -> Path:
    if custom_output_dir:
        output_dir = Path(custom_output_dir)
    else:
        output_dir = OUTPUT_ROOT / f"synthetic_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def write_csv(rows: list[dict[str, Any]], path: Path, ordered_fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=ordered_fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field) for field in ordered_fields})


def write_json(data: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def export_dataset(output_dir: Path, tables: dict[str, list[dict[str, Any]]], structured_rows: list[dict[str, Any]]) -> dict[str, Any]:
    csv_dir = output_dir / "csv"
    json_dir = output_dir / "json"
    export_dir = output_dir / "exports"
    for table_name, rows in tables.items():
        write_csv(rows, csv_dir / f"{table_name}.csv", TABLE_ORDERS[table_name])
        write_json(rows, json_dir / f"{table_name}.json")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_json_path = export_dir / f"health_structured_多地区_{timestamp}.json"
    export_csv_path = export_dir / f"health_structured_多地区_{timestamp}.csv"
    write_json(structured_rows, export_json_path)

    flat_rows = []
    for item in structured_rows:
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
        for metric_key in METRIC_EXPORT_ORDER:
            metric = item["metrics"].get(metric_key)
            row[metric_key] = metric["value"] if metric else None
        flat_rows.append(row)

    write_csv(
        flat_rows,
        export_csv_path,
        ["news_id", "source_table", "source_name", "province_code", "province_name", "title", "publish_date", "year", "month", *METRIC_EXPORT_ORDER],
    )

    manifest = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dataset_root": str(output_dir),
        "tables": {table_name: len(rows) for table_name, rows in tables.items()},
        "structured_export": {"records": len(structured_rows), "json": str(export_json_path), "csv": str(export_csv_path)},
        "notes": [
            "population_info 采用 region x age_group x gender 明细，便于直接支撑人口分析。",
            "health_ocr_metrics 与 outputs 导出结构保持一致，字段名沿用现有项目约定。",
            "预测与异常检测结果来自合成历史序列，不依赖 Spark 运行。",
        ],
    }
    write_json(manifest, output_dir / "manifest.json")
    return manifest


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("\\", "\\\\").replace("'", "''")
    return f"'{text}'"


def export_sql_seed(output_dir: Path, tables: dict[str, list[dict[str, Any]]]) -> Path:
    sql_path = output_dir / "seed_mysql.sql"
    order = [
        "medical_institution", "population_info", "hospital_bed", "population_data", "guangxi_news", "sichuan_news",
        "national_news", "report_metrics", "health_ocr_metrics", "institution_yearly_summary", "analysis_population_region",
        "analysis_population_age", "analysis_population_gender", "analysis_institution_type", "analysis_institution_level",
        "analysis_institution_region", "analysis_personnel", "analysis_beds", "analysis_services", "analysis_costs",
        "ocr_metrics_yearly", "region_comparison", "prediction_results", "anomaly_detection",
    ]
    statements = ["SET NAMES utf8mb4;", "SET FOREIGN_KEY_CHECKS = 0;"]
    for table_name in order:
        rows = tables.get(table_name, [])
        if not rows:
            continue
        columns = TABLE_ORDERS[table_name]
        for row in rows:
            values = ", ".join(sql_literal(row.get(column)) for column in columns)
            statements.append(f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({values});")
    statements.append("SET FOREIGN_KEY_CHECKS = 1;")
    sql_path.write_text("\n".join(statements), encoding="utf-8")
    return sql_path


def seed_mysql(tables: dict[str, list[dict[str, Any]]], truncate_first: bool) -> None:
    if mysql is None:
        raise RuntimeError("mysql.connector is unavailable; install dependencies before using --seed-db")
    order = [
        "medical_institution", "population_info", "hospital_bed", "population_data", "guangxi_news", "sichuan_news",
        "national_news", "report_metrics", "health_ocr_metrics", "institution_yearly_summary", "analysis_population_region",
        "analysis_population_age", "analysis_population_gender", "analysis_institution_type", "analysis_institution_level",
        "analysis_institution_region", "analysis_personnel", "analysis_beds", "analysis_services", "analysis_costs",
        "ocr_metrics_yearly", "region_comparison", "prediction_results", "anomaly_detection",
    ]
    conn = mysql.connector.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()
        if truncate_first:
            for table_name in reversed(order):
                try:
                    cursor.execute(f"DELETE FROM {table_name}")
                except Exception:
                    conn.rollback()
        for table_name in order:
            rows = tables.get(table_name, [])
            if not rows:
                continue
            columns = TABLE_ORDERS[table_name]
            placeholders = ", ".join(["%s"] * len(columns))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            values = [tuple(row.get(column) for column in columns) for row in rows]
            cursor.executemany(sql, values)
        conn.commit()
        cursor.close()
    finally:
        conn.close()


def generate_dataset(start_year: int, end_year: int, seed: int) -> dict[str, list[dict[str, Any]]]:
    rng = random.Random(seed)
    profiles = build_region_year_profiles(start_year, end_year, rng)
    population_info = generate_population_info(end_year, profiles)
    medical_institution = generate_medical_institutions(rng)
    hospital_bed = generate_hospital_beds(medical_institution, rng)
    population_data = generate_population_data(rng)
    news_tables, report_metrics, health_ocr_metrics = generate_news_and_metrics(start_year, end_year, profiles)
    institution_yearly_summary = generate_institution_yearly_summary(medical_institution, start_year, end_year)
    analysis_population = build_analysis_population_tables(population_info)
    analysis_institution = build_analysis_institution_tables(medical_institution)
    ocr_metrics_yearly = aggregate_yearly_metrics(health_ocr_metrics)
    analysis_metrics = build_analysis_metric_tables(ocr_metrics_yearly)
    region_comparison = build_region_comparison(ocr_metrics_yearly, population_info, institution_yearly_summary)
    prediction_results = build_prediction_results(ocr_metrics_yearly)
    anomaly_detection = build_anomaly_detection(ocr_metrics_yearly)
    return {
        "medical_institution": medical_institution,
        "population_info": population_info,
        "hospital_bed": hospital_bed,
        "population_data": population_data,
        "guangxi_news": news_tables["guangxi_news"],
        "sichuan_news": news_tables["sichuan_news"],
        "national_news": news_tables["national_news"],
        "report_metrics": report_metrics,
        "health_ocr_metrics": health_ocr_metrics,
        "institution_yearly_summary": institution_yearly_summary,
        **analysis_population,
        **analysis_institution,
        **analysis_metrics,
        "ocr_metrics_yearly": ocr_metrics_yearly,
        "region_comparison": region_comparison,
        "prediction_results": prediction_results,
        "anomaly_detection": anomaly_detection,
    }


def main() -> None:
    args = parse_args()
    start_year, end_year = parse_year_range(args.years)
    output_dir = ensure_output_dir(args.output_dir)
    tables = generate_dataset(start_year, end_year, args.seed)
    structured_rows = build_structured_export_rows(tables["health_ocr_metrics"])
    manifest = export_dataset(output_dir, tables, structured_rows)
    sql_path = export_sql_seed(output_dir, tables)
    if args.seed_db:
        seed_mysql(tables, truncate_first=args.truncate_db)

    print("=" * 72)
    print("Synthetic dataset generated successfully")
    print("=" * 72)
    print(f"Output directory : {output_dir}")
    print(f"Seed            : {args.seed}")
    print(f"Year range      : {start_year}-{end_year}")
    print(f"SQL seed file   : {sql_path}")
    print(f"Structured rows : {manifest['structured_export']['records']}")
    for table_name, row_count in manifest["tables"].items():
        print(f"{table_name:<28} {row_count:>8}")
    print("MySQL seeding   :", "completed" if args.seed_db else "skipped (use --seed-db to enable)")


if __name__ == "__main__":
    main()
