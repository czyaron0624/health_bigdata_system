import re
import json
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

        number, raw = _extract_numeric(line)
        if number is not None:
            return number, raw

        for offset in (1, 2):
            next_idx = idx + offset
            if next_idx >= len(lines):
                break
            number, raw = _extract_numeric(lines[next_idx])
            if number is not None:
                return number, raw

    return None, None


def _find_metric_value_in_tables(tables, keywords: List[str]) -> Tuple[Optional[float], Optional[str], Optional[Dict]]:
    if not tables:
        return None, None, None

    for table_index, table in enumerate(tables):
        rows = table.get("rows") or []
        for row_index, row in enumerate(rows):
            row_text = " ".join(row)
            if not any(keyword in row_text for keyword in keywords):
                continue

            number, raw = _extract_numeric(row_text)
            if number is not None:
                return number, raw, {
                    "source": "table",
                    "table_index": table_index,
                    "row_index": row_index,
                    "row": row,
                    "matched_text": row_text,
                }

            for cell_index, cell in enumerate(row):
                if not any(keyword in cell for keyword in keywords):
                    continue
                for next_cell in row[cell_index + 1 :]:
                    number, raw = _extract_numeric(next_cell)
                    if number is not None:
                        return number, raw, {
                            "source": "table",
                            "table_index": table_index,
                            "row_index": row_index,
                            "row": row,
                            "matched_text": row_text,
                        }

            for offset in (1, 2):
                next_idx = row_index + offset
                if next_idx >= len(rows):
                    break
                next_row = rows[next_idx]
                next_row_text = " ".join(next_row)
                number, raw = _extract_numeric(next_row_text)
                if number is not None:
                    return number, raw, {
                        "source": "table",
                        "table_index": table_index,
                        "row_index": next_idx,
                        "row": next_row,
                        "matched_text": row_text,
                    }

    return None, None, None


def _normalize_metric_value(metric_key: str, value: Optional[float], raw: Optional[str]) -> Optional[float]:
    if value is None:
        return None

    if metric_key == "bed_usage_rate":
        raw_text = (raw or "").replace(".", "").replace(",", "")
        if raw_text.isdigit() and len(raw_text) >= 3 and value > 100:
            return value / 10

    return value


def _infer_report_category(title: str) -> str:
    text = title or ""
    if "公报" in text:
        return "统计公报"
    if "简报" in text:
        return "统计简报"
    if "指标表" in text or "指标" in text:
        return "指标表"
    if "情况" in text:
        return "情况通报"
    if "下载" in text:
        return "数据下载"
    return "其他"


def _infer_report_scope(title: str, detail_context: Optional[Dict]) -> str:
    text = (title or "") + " " + (detail_context.get("full_text", "") if detail_context else "")
    for scope in ("全省", "四川省", "广西", "国家", "全国"):
        if scope in text:
            return scope
    return "未知"


def _infer_source_unit(detail_context: Optional[Dict], cleaned_text: str) -> Optional[str]:
    candidates = []
    if detail_context:
        candidates.extend(detail_context.get("meta_lines", []))
        candidates.extend(detail_context.get("breadcrumbs", []))
        candidates.append(detail_context.get("full_text", ""))
    if cleaned_text:
        candidates.append(cleaned_text)

    patterns = [
        r"来源[:：]\s*([^\n\]]+)",
        r"发布机构[:：]\s*([^\n\]]+)",
        r"主办[:：]\s*([^\n\]]+)",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        for pattern in patterns:
            match = re.search(pattern, candidate)
            if match:
                return match.group(1).strip()
    return None


def _infer_quarter(year: Optional[int], month: Optional[int]) -> Optional[int]:
    if year is None or month is None:
        return None
    return ((month - 1) // 3) + 1


def _load_detail_context(detail_context):
    if detail_context is None:
        return None
    if isinstance(detail_context, dict):
        return detail_context
    if isinstance(detail_context, str) and detail_context.strip():
        try:
            return json.loads(detail_context)
        except Exception:
            return {"full_text": detail_context}
    return None


def parse_structured_metrics(title: str, publish_date: Optional[str], ocr_text: str, detail_context=None) -> Dict:
    detail_context = _load_detail_context(detail_context)
    context_text = detail_context.get("full_text", "") if detail_context else ""

    combined_text = "\n".join(part for part in (ocr_text, context_text) if part)
    cleaned_text = clean_ocr_text(combined_text)
    lines = cleaned_text.split("\n") if cleaned_text else []
    year, month = infer_year_month(title, publish_date)
    tables = detail_context.get("tables", []) if detail_context else []

    context_summary = {
        "title": title,
        "publish_date": publish_date,
        "year": year,
        "month": month,
        "quarter": _infer_quarter(year, month),
        "report_category": _infer_report_category(title),
        "report_scope": _infer_report_scope(title, detail_context),
        "source_unit": _infer_source_unit(detail_context, cleaned_text),
        "breadcrumbs": detail_context.get("breadcrumbs", []) if detail_context else [],
        "meta_lines": detail_context.get("meta_lines", []) if detail_context else [],
        "table_count": detail_context.get("table_count", 0) if detail_context else 0,
        "image_count": detail_context.get("image_count", 0) if detail_context else 0,
        "attachment_count": detail_context.get("attachment_count", 0) if detail_context else 0,
        "attachments": detail_context.get("attachments", []) if detail_context else [],
    }

    metrics = {}
    for metric_key, definition in METRIC_DEFINITIONS.items():
        value, raw, evidence = _find_metric_value_in_tables(tables, definition["keywords"])
        if value is None:
            value, raw = _find_metric_value(lines, definition["keywords"])
            if raw is not None:
                evidence = {
                    "source": "ocr_text",
                    "matched_text": raw,
                }
        else:
            evidence = {
                "source": "table",
                "matched_text": raw,
            }
        value = _normalize_metric_value(metric_key, value, raw)
        metrics[metric_key] = {
            "metric_name": definition["name"],
            "value": value,
            "raw": raw,
            "evidence": evidence,
        }

    return {
        "year": year,
        "month": month,
        "cleaned_text": cleaned_text,
        "context": context_summary,
        "metrics": metrics,
    }
