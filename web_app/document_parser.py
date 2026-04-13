import os
import pandas as pd
import PyPDF2
import docx

def parse_excel(file_path):
    df = pd.read_excel(file_path)
    # Fill NaN with empty string
    df = df.fillna('')
    return df.to_dict('records')

def parse_txt(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    return [{"content": text}]

def parse_word(file_path):
    doc = docx.Document(file_path)
    text = "\n".join([para.text for para in doc.paragraphs])
    return [{"content": text}]

def parse_pdf(file_path):
    text = ""
    with open(file_path, 'rb') as f:
        reader = PyPDF2.PdfReader(f)
        for page in reader.pages:
            if page.extract_text():
                text += page.extract_text() + "\n"
    return [{"content": text}]

def parse_document(file_path, filename):
    ext = os.path.splitext(filename)[1].lower()
    if ext in ['.xlsx', '.xls']:
        return parse_excel(file_path)
    elif ext == '.docx':
        return parse_word(file_path)
    elif ext == '.pdf':
        return parse_pdf(file_path)
    elif ext == '.txt':
        return parse_txt(file_path)
    else:
        raise ValueError(f"不支持的文件格式: {ext}")

def extract_healthcare_data(parsed_data, filename):
    target_table = "unknown"
    mapped_data = []
    
    if not parsed_data:
        return {"target_table": target_table, "data": []}
        
    first_row = parsed_data[0]
    keys = " ".join([str(k) for k in first_row.keys()]).lower()
    
    if 'population' in keys or '人口' in keys or 'age' in keys or '年龄' in keys:
        target_table = "population_info"
        for row in parsed_data:
            try:
                pop_count = int(row.get('人口数量', row.get('population_count', 0)) or 0)
            except ValueError:
                pop_count = 0
            mapped_data.append({
                "region": row.get('地区', row.get('region', '未知')),
                "age_group": row.get('年龄段', row.get('age_group', '')),
                "gender": row.get('性别', row.get('gender', '')),
                "population_count": pop_count
            })
    elif 'institution' in keys or '机构' in keys or 'hospital' in keys or '医院' in keys:
        target_table = "medical_institution"
        for row in parsed_data:
            mapped_data.append({
                "name": row.get('机构名称', row.get('name', '未知')),
                "type": row.get('类型', row.get('type', '')),
                "region": row.get('地区', row.get('region', '')),
                "level": row.get('等级', row.get('level', ''))
            })
    else:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from crawlers.ocr_structurer import parse_structured_metrics
        full_text = ""
        for item in parsed_data:
            full_text += str(item.get("content", "")) + "\n"
            
        metrics_data = parse_structured_metrics(filename, None, full_text)
        target_table = "health_ocr_metrics"
        
        for key, m in metrics_data.get("metrics", {}).items():
            if m.get("value") is not None:
                mapped_data.append({
                    "metric_key": key,
                    "metric_name": m.get("metric_name"),
                    "metric_value": m.get("value"),
                    "metric_raw": m.get("raw"),
                    "year": metrics_data.get("year"),
                    "month": metrics_data.get("month")
                })
            
    return {"target_table": target_table, "data": mapped_data}