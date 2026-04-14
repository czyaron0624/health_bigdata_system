import os
import sys
import uuid
from flask import Blueprint, render_template, request, jsonify, current_app
from werkzeug.utils import secure_filename
import pymysql

# Import parser - use absolute import to avoid module loading issues
try:
    from .document_parser import parse_document, extract_healthcare_data
except ImportError:
    # Fallback: direct import using file path
    import importlib.util
    spec = importlib.util.spec_from_file_location("document_parser", os.path.join(os.path.dirname(__file__), "document_parser.py"))
    document_parser = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(document_parser)
    parse_document = document_parser.parse_document
    extract_healthcare_data = document_parser.extract_healthcare_data

document_bp = Blueprint('document', __name__, url_prefix='/upload')

@document_bp.route('/', methods=['GET', 'POST'])
def upload_page():
    # 文档上传功能已集成到管理员仪表板中，此路由不再返回独立页面
    return jsonify({
        "message": "文档上传功能已集成到管理员仪表板中。请访问 /admin/dashboard 的文档上传功能。",
        "info": "使用 /upload/preview 接口预览，使用 /upload/confirm 接口确认入库。"
    }), 200

@document_bp.route('/preview', methods=['POST'])
def upload_preview():
    if 'file' not in request.files:
        return jsonify({"success": False, "message": "没有文件部分"})
    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "message": "没有选择文件"})
        
    upload_folder = current_app.config['UPLOAD_FOLDER']
    if not os.path.exists(upload_folder):
        os.makedirs(upload_folder)
        
    filename = secure_filename(file.filename)
    if not filename:
        ext = os.path.splitext(file.filename)[1]
        filename = str(uuid.uuid4()) + ext

    file_path = os.path.join(upload_folder, filename)
    file.save(file_path)
    
    try:
        parsed_data = parse_document(file_path, file.filename)
        result = extract_healthcare_data(parsed_data, file.filename)
        return jsonify({
            "success": True, 
            "target_table": result["target_table"],
            "data": result["data"],
            "file_path": file_path
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})

@document_bp.route('/confirm', methods=['POST'])
def upload_confirm():
    req_data = request.json
    target_table = req_data.get('target_table')
    data = req_data.get('data', [])
    
    if not data:
        return jsonify({"success": False, "message": "没有数据可导入"})
    
    conn = None
    try:
        conn = pymysql.connect(
            host='localhost',
            port=3306,
            user='root',
            password='rootpassword',
            database='health_db',
            charset='utf8mb4'
        )
        cursor = conn.cursor()
        inserted_count = 0
        
        if target_table == 'population_info':
            for row in data:
                sql = "INSERT INTO population_info (region, age_group, gender, population_count) VALUES (%s, %s, %s, %s)"
                val = (row.get('region'), row.get('age_group'), row.get('gender'), row.get('population_count'))
                cursor.execute(sql, val)
                inserted_count += 1
        elif target_table == 'medical_institution':
            for row in data:
                sql = "INSERT INTO medical_institution (name, type, region, level) VALUES (%s, %s, %s, %s)"
                val = (row.get('name'), row.get('type'), row.get('region'), row.get('level'))
                cursor.execute(sql, val)
                inserted_count += 1
        elif target_table == 'health_ocr_metrics':
            for row in data:
                sql = """
                INSERT INTO health_ocr_metrics 
                (news_id, title, year, month, metric_key, metric_name, metric_value, metric_raw)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE metric_value=VALUES(metric_value)
                """
                val = (
                    0, 
                    "Uploaded Document", 
                    row.get('year') or 2026, 
                    row.get('month') or 1, 
                    row.get('metric_key'), 
                    row.get('metric_name'), 
                    row.get('metric_value'), 
                    row.get('metric_raw')
                )
                cursor.execute(sql, val)
                inserted_count += 1
        else:
            return jsonify({"success": False, "message": "未知的目标表"})
            
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({"success": True, "inserted_count": inserted_count})
    except Exception as e:
        if conn is not None:
            try:
                conn.rollback()
                conn.close()
            except:
                pass
        return jsonify({"success": False, "message": str(e)})