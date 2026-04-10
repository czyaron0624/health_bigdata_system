import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple


METRIC_DEFINITIONS = {
    "bed_count": {
        "name": "实有床位数",
        "keywords": ["实有床位数", "床位数"],
    },
    "doctor_count": {
        "name": "执业(助理)医师数",
        "keywords": ["执业（助理）医师数", "执业(助理）医师数", "执业(助理)医师数", "执业医师数"],
    },
    "nurse_count": {
        "name": "注册护士数",
        "keywords": ["注册护士数"],
    },
    "outpatient_visits": {
        "name": "总诊疗人次数",
        "keywords": ["总诊疗人次数", "总诊疗人次"],
    },
    "discharge_count": {
        "name": "出院人数",
        "keywords": ["出院人数"],
    },
    "bed_usage_rate": {
        "name": "病床使用率",
        "keywords": ["病床使用率"],
    },
    "avg_stay_days": {
        "name": "出院者平均住院日",
        "keywords": ["出院者平均住院日", "平均住院日"],
    },
    "outpatient_cost": {
        "name": "门诊病人次均医药费用",
        "keywords": ["门诊病人次均医药费用"],
    },
    "discharge_cost": {
        "name": "出院病人人均医药费用",
        "keywords": ["出院病人人均医药费用"],
    },
}


def clean_ocr_text(text: str) -> str:
    if not text:
        return ""

    cleaned = text.replace("\r", "\n")
    cleaned = cleaned.replace("\u3000", " ")
    cleaned = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", " ", cleaned)
    cleaned = cleaned.replace("�", "")
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{2,}", "\n", cleaned)

    lines = [line.strip() for line in cleaned.split("\n") if line.strip()]
    return "\n".join(lines)


def infer_year_month(title: str, publish_date: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    title = title or ""

    match = re.search(r"(20\d{2})年\s*(\d{1,2})月", title)
    if match:
        return int(match.group(1)), int(match.group(2))

    year_match = re.search(r"(20\d{2})年", title)
    if year_match:
        return int(year_match.group(1)), None

    if publish_date:
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
            try:
                dt = datetime.strptime(publish_date[:10], fmt)
                return dt.year, dt.month
            except ValueError:
                continue

    return None, None


def _extract_numeric(value: str) -> Tuple[Optional[float], Optional[str]]:
    if not value:
        return None, None

    match = re.search(r"[-+]?\d+(?:[.,]\d+)?", value)
    if not match:
        return None, None

    raw = match.group(0).replace(",", "")
    try:
        return float(raw), raw
    except ValueError:
        return None, raw


def _find_metric_value(lines: List[str], keywords: List[str]) -> Tuple[Optional[float], Optional[str]]:
    for idx, line in enumerate(lines):
        if not any(keyword in line for keyword in keywords):
            continue

        # 优先尝试同一行
        number, raw = _extract_numeric(line)
        if number is not None:
            return number, raw

        # 回看后续两行（OCR经常拆行）
        for offset in (1, 2):
            next_idx = idx + offset
            if next_idx >= len(lines):
                break
            number, raw = _extract_numeric(lines[next_idx])
            if number is not None:
                return number, raw

    return None, None


def _normalize_metric_value(metric_key: str, value: Optional[float], raw: Optional[str]) -> Optional[float]:
    if value is None:
        return None

    # OCR 常见误差: 病床使用率 69.9% 可能被识别成 0699 或 699
    if metric_key == "bed_usage_rate":
        raw_text = (raw or "").replace(".", "").replace(",", "")
        if raw_text.isdigit() and len(raw_text) >= 3 and value > 100:
            return value / 10

    return value


def parse_structured_metrics(title: str, publish_date: Optional[str], ocr_text: str) -> Dict:
    cleaned_text = clean_ocr_text(ocr_text)
    lines = cleaned_text.split("\n") if cleaned_text else []
    year, month = infer_year_month(title, publish_date)

    metrics = {}
    for metric_key, definition in METRIC_DEFINITIONS.items():
        value, raw = _find_metric_value(lines, definition["keywords"])
        value = _normalize_metric_value(metric_key, value, raw)
        metrics[metric_key] = {
            "metric_name": definition["name"],
            "value": value,
            "raw": raw,
        }

    return {
        "year": year,
        "month": month,
        "cleaned_text": cleaned_text,
        "metrics": metrics,
    }
