from datetime import datetime
import re
import os

from flask import Flask, jsonify, render_template, request, redirect, url_for, session
import json
import redis

app = Flask(__name__)
app.secret_key = 'health_bigdata_secret_key_2026'  # 用于session加密
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max upload size
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')

# Create upload folder if not exists
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

# 连接 Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

ADMIN_ALERTS = [
    {"id": 1, "content": "广西区域床位利用率异常波动", "time": "2分钟前", "level": "高"},
    {"id": 2, "content": "人口数据接口延时超过阈值", "time": "8分钟前", "level": "中"},
    {"id": 3, "content": "某机构重复上报记录待核查", "time": "13分钟前", "level": "中"},
]


def push_admin_alert(content: str, level: str = '中'):
    alert_id = int(datetime.now().timestamp() * 1000)
    ADMIN_ALERTS.insert(
        0,
        {
            "id": alert_id,
            "content": content,
            "time": datetime.now().strftime('%H:%M:%S'),
            "level": level,
        },
    )
    del ADMIN_ALERTS[20:]

USER_TIPS = [
    "本周平均步数较上周下降 7%，建议晚间增加 20 分钟快走。",
    "睡眠时长达到建议标准，继续保持 23:30 前入睡习惯。",
    "体检记录显示血脂边缘偏高，建议控制高脂饮食并复查。",
]

USER_REMINDERS = [
    {"time": "04-10 08:30", "content": "慢病复诊提醒（社区医院门诊）"},
    {"time": "04-12 14:00", "content": "健康报告线上解读预约"},
    {"time": "04-14 10:15", "content": "个人健康档案更新计划"},
]

TREND_LABELS = ["一", "二", "三", "四", "五", "六", "日"]
TREND_VALUES = [68, 72, 70, 76, 79, 81, 82]

# OCR 指标有效范围过滤，避免异常识别值污染图表
OCR_METRIC_VALID_SQL = """
(
    (metric_key = 'doctor_count' AND metric_value BETWEEN 1000 AND 300000)
    OR (metric_key = 'nurse_count' AND metric_value BETWEEN 1000 AND 400000)
    OR (metric_key = 'bed_count' AND metric_value BETWEEN 1000 AND 600000)
    OR (metric_key = 'bed_usage_rate' AND metric_value BETWEEN 1 AND 100)
    OR (metric_key = 'outpatient_visits' AND metric_value BETWEEN 100000 AND 50000000)
    OR (metric_key = 'discharge_count' AND metric_value BETWEEN 1000 AND 5000000)
    OR (metric_key = 'avg_stay_days' AND metric_value BETWEEN 1 AND 30)
    OR (metric_key = 'outpatient_cost' AND metric_value BETWEEN 1 AND 2000)
    OR (metric_key = 'discharge_cost' AND metric_value BETWEEN 1 AND 50000)
)
"""

VALID_SCOPES = {'all', 'guangxi', 'national'}
SCOPE_LABELS = {
    'all': '全部来源',
    'guangxi': '省级卫健委（广西）',
    'national': '国家卫健委',
}


def is_role(role: str) -> bool:
    return session.get('role') == role


def admin_forbidden_response():
    return jsonify({"error": "无管理员权限"}), 403


def user_forbidden_response():
    return jsonify({"error": "无用户权限"}), 403


def get_scope() -> str:
    scope = (request.args.get('scope') or 'guangxi').strip().lower()
    if scope not in VALID_SCOPES:
        return 'guangxi'
    return scope


def build_metric_scope_filter(scope: str):
    if scope == 'guangxi':
        return "source_table = %s", ['guangxi_news']
    if scope == 'national':
        return "source_table = %s", ['national_news']
    return "", []


@app.route('/')
def index():
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        role = request.form.get('role')

        # 验证用户凭据
        if role == 'admin' and username == 'admin' and password == 'admin123':
            session['user'] = username
            session['role'] = 'admin'
            return redirect(url_for('admin_dashboard'))
        if role == 'user' and username == 'user' and password == 'user123':
            session['user'] = username
            session['role'] = 'user'
            return redirect(url_for('user_dashboard'))

        return render_template('login.html', error='用户名或密码错误')

    return render_template('login.html')


@app.route('/admin/dashboard')
def admin_dashboard():
    if not is_role('admin'):
        return redirect(url_for('login'))
    return render_template('admin_dashboard.html')


@app.route('/user/dashboard')
def user_dashboard():
    if not is_role('user'):
        return redirect(url_for('login'))
    return render_template('user_dashboard.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/api/health-stats', methods=['GET'])
def get_stats():
    scope = get_scope()

    # 从 Redis 取出缓存的统计结果
    try:
        data = r.get("health_stats")
    except redis.RedisError:
        data = None

    live_payload = {
        "source": "live",
        "scope": scope,
        "scope_label": SCOPE_LABELS.get(scope, scope),
        "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM medical_institution")
        live_payload["institution_count"] = int(cursor.fetchone()[0])

        if scope in {'guangxi', 'all'}:
            cursor.execute("SELECT COUNT(*) FROM population_data")
            live_payload["population_count"] = int(cursor.fetchone()[0])
        else:
            live_payload["population_count"] = 0

        if scope == 'guangxi':
            cursor.execute("SELECT COUNT(*) FROM guangxi_news")
            live_payload["news_count"] = int(cursor.fetchone()[0])
        elif scope == 'national':
            cursor.execute("SELECT COUNT(*) FROM national_news")
            live_payload["news_count"] = int(cursor.fetchone()[0])
        else:
            cursor.execute("SELECT COUNT(*) FROM guangxi_news")
            guangxi_count = int(cursor.fetchone()[0])
            cursor.execute("SELECT COUNT(*) FROM national_news")
            national_count = int(cursor.fetchone()[0])
            live_payload["news_count"] = guangxi_count + national_count

        cursor.execute("SELECT COUNT(*) FROM health_ocr_metrics")
        live_payload["metric_count"] = int(cursor.fetchone()[0])

        cursor.close()
        conn.close()
    except Exception:
        live_payload.setdefault("institution_count", 128)
        live_payload.setdefault("population_count", 50234 if scope in {'guangxi', 'all'} else 0)
        live_payload.setdefault("news_count", 0)
        live_payload.setdefault("metric_count", 0)

    if data:
        try:
            # 将字符串转回 JSON 格式发给前端
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                # 兼容旧字段命名，统一输出管理员页面所需的键
                if 'institution_count' not in parsed and 'inst_count' in parsed:
                    parsed['institution_count'] = parsed['inst_count']
                if 'population_count' not in parsed and 'pop_count' in parsed:
                    parsed['population_count'] = parsed['pop_count']
                if 'risk_events' not in parsed:
                    parsed['risk_events'] = 3
                if 'online_users' not in parsed:
                    parsed['online_users'] = 67
                if 'updated_at' not in parsed:
                    parsed['updated_at'] = live_payload['updated_at']
                parsed.update(live_payload)
            return jsonify(parsed)
        except (TypeError, ValueError, json.JSONDecodeError):
            # Redis 中存在脏数据时返回兜底，避免前端空白
            pass

    # 兜底数据，避免前端组件空白
    fallback = {
        **live_payload,
        "risk_events": 3,
        "online_users": 67,
    }
    return jsonify(fallback)


@app.route('/api/news/national', methods=['GET'])
def get_national_news():
    """ 获取国家卫健委新闻数据 """
    if not is_role('admin'):
        return admin_forbidden_response()
    
    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT id, title, source_category, publish_date FROM national_news ORDER BY id DESC LIMIT 10")
        items = cursor.fetchall()
        conn.close()
        
        return jsonify({"items": items, "source": "national"})
    except Exception as e:
        return jsonify({"error": str(e), "items": []}), 500


@app.route('/api/news/guangxi', methods=['GET'])
def get_guangxi_news():
    """ 获取广西卫健委新闻数据 """
    if not is_role('admin'):
        return admin_forbidden_response()
    
    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT id, title, publish_date FROM guangxi_news ORDER BY id DESC LIMIT 10")
        items = cursor.fetchall()
        conn.close()
        
        return jsonify({"items": items, "source": "guangxi"})
    except Exception as e:
        return jsonify({"error": str(e), "items": []}), 500


@app.route('/api/news/region', methods=['GET'])
def get_region_news():
    """按 scope 返回新闻列表，支持 guangxi / national / all"""
    if not is_role('admin'):
        return admin_forbidden_response()

    scope = get_scope()
    selected_year = request.args.get('year', '').strip()

    def build_year_filter(alias: str):
        if selected_year.isdigit():
            return f" AND YEAR(STR_TO_DATE({alias}.publish_date, '%%Y-%%m-%%d')) = %s", [int(selected_year)]
        return "", []

    def build_union_query(year_clause: str):
        return f"""
                SELECT id, title, publish_date, source, publish_year
                FROM (
                    SELECT id, title, publish_date, 'guangxi' AS source,
                           YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                    FROM guangxi_news
                    UNION ALL
                    SELECT id, title, publish_date, 'national' AS source,
                           YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                    FROM national_news
                ) t
                WHERE publish_year IS NOT NULL {year_clause}
                ORDER BY publish_year DESC, publish_date DESC, id DESC
                LIMIT 50
                """

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)

        year_clause = ''
        year_params = []

        if scope == 'guangxi':
            year_clause, year_params = build_year_filter('guangxi_news')
            cursor.execute(
                """
                SELECT id, title, publish_date, 'guangxi' AS source,
                       YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                FROM guangxi_news
                WHERE publish_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'""" + year_clause + """
                ORDER BY publish_year DESC, publish_date DESC, id DESC
                LIMIT 50
                """,
                tuple(year_params)
            )
        elif scope == 'national':
            year_clause, year_params = build_year_filter('national_news')
            cursor.execute(
                """
                SELECT id, title, publish_date, 'national' AS source,
                       YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                FROM national_news
                WHERE publish_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'""" + year_clause + """
                ORDER BY publish_year DESC, publish_date DESC, id DESC
                LIMIT 50
                """,
                tuple(year_params)
            )
        else:
            year_clause, year_params = build_year_filter('t')
            cursor.execute(build_union_query(year_clause), tuple(year_params))

        items = cursor.fetchall()

        cursor.execute(
            f"""
            SELECT MIN(publish_year) AS year_min, MAX(publish_year) AS year_max
            FROM (
                SELECT YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                FROM guangxi_news
                UNION ALL
                SELECT YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                FROM national_news
            ) y
            WHERE publish_year IS NOT NULL
            """
        )
        year_meta = cursor.fetchone() or {}
        conn.close()

        return jsonify({
            "items": items,
            "scope": scope,
            "scope_label": SCOPE_LABELS.get(scope, scope),
            "selected_year": int(selected_year) if selected_year.isdigit() else None,
            "year_min": year_meta.get('year_min'),
            "year_max": year_meta.get('year_max'),
        })
    except Exception as e:
        return jsonify({"error": str(e), "items": []}), 500


@app.route('/api/news/tjnb', methods=['GET'])
def get_tjnb_news():
    """统计年报（tjnb）专项数据，优先用于展示可分析条目"""
    if not is_role('admin'):
        return admin_forbidden_response()

    scope = get_scope()
    min_year_arg = (request.args.get('min_year') or '2015').strip()
    try:
        min_year = int(min_year_arg)
    except ValueError:
        min_year = 2015

    # 统计年报目前仅在广西来源中维护
    if scope == 'national':
        return jsonify({
            "items": [],
            "year_counts": [],
            "meta": {
                "scope": scope,
                "scope_label": SCOPE_LABELS.get(scope, scope),
                "min_year": min_year,
                "total": 0,
                "useful_total": 0,
                "message": "国家范围暂无统计年报（tjnb）数据",
            }
        })

    def infer_report_year(title: str, publish_date: str):
        text = title or ''
        title_match = re.search(r'(20\d{2})\s*年', text)
        if title_match:
            return int(title_match.group(1))

        if publish_date and re.match(r'^20\d{2}-\d{2}-\d{2}$', str(publish_date)):
            return int(str(publish_date)[:4])

        return None

    def infer_category(title: str):
        text = title or ''
        if '公报' in text:
            return '统计公报'
        if '简报' in text:
            return '统计简报'
        if '图解' in text:
            return '图解'
        if '统计' in text:
            return '统计信息'
        return '其他'

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)

        cursor.execute(
            """
            SELECT id, title, link, publish_date
            FROM guangxi_news
            WHERE link LIKE %s
              AND publish_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            ORDER BY publish_date DESC, id DESC
            """,
            ('%/tjnb/%',)
        )
        rows = cursor.fetchall()
        conn.close()

        normalized_items = []
        year_counter = {}
        useful_count = 0

        for row in rows:
            report_year = infer_report_year(row.get('title'), row.get('publish_date'))
            if report_year is not None and report_year < min_year:
                continue

            category = infer_category(row.get('title'))
            is_useful = category in {'统计公报', '统计简报', '统计信息'}
            if is_useful:
                useful_count += 1

            if report_year is not None:
                year_counter[report_year] = year_counter.get(report_year, 0) + 1

            normalized_items.append({
                "id": row.get('id'),
                "title": row.get('title'),
                "link": row.get('link'),
                "publish_date": row.get('publish_date'),
                "report_year": report_year,
                "category": category,
                "is_useful": is_useful,
            })

        year_counts = [
            {"year": year, "count": year_counter[year]}
            for year in sorted(year_counter.keys(), reverse=True)
        ]

        return jsonify({
            "items": normalized_items[:30],
            "year_counts": year_counts,
            "meta": {
                "scope": scope,
                "scope_label": SCOPE_LABELS.get(scope, scope),
                "min_year": min_year,
                "total": len(normalized_items),
                "useful_total": useful_count,
                "latest_publish_date": normalized_items[0]['publish_date'] if normalized_items else None,
            }
        })
    except Exception as e:
        return jsonify({"error": str(e), "items": [], "year_counts": []}), 500


@app.route('/api/metrics/summary', methods=['GET'])
def get_metrics_summary():
    """ 获取真实结构化指标汇总数据（按年） """
    if not is_role('admin'):
        return admin_forbidden_response()
    
    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)

        metric_meta = {
            'doctor_count': ('执业(助理)医师数', '人'),
            'nurse_count': ('注册护士数', '人'),
            'bed_count': ('实有床位数', '张'),
            'bed_usage_rate': ('病床使用率', '%'),
            'outpatient_visits': ('总诊疗人次数', '人次'),
            'discharge_count': ('出院人数', '人'),
            'avg_stay_days': ('出院者平均住院日', '天'),
            'outpatient_cost': ('门诊病人次均医药费用', '元'),
            'discharge_cost': ('出院病人人均医药费用', '元'),
        }

        metric_keys = tuple(metric_meta.keys())
        placeholders = ','.join(['%s'] * len(metric_keys))

        scope = get_scope()
        scope_clause, scope_params = build_metric_scope_filter(scope)

        sql = f"""
            SELECT
                year,
                metric_key,
                ROUND(AVG(metric_value), 4) AS avg_value,
                COUNT(*) AS sample_count
            FROM health_ocr_metrics
            WHERE metric_value IS NOT NULL
              AND year IS NOT NULL
              AND metric_key IN ({placeholders})
              AND {OCR_METRIC_VALID_SQL}
        """

        if scope_clause:
            sql += f"\n              AND {scope_clause}"

        sql += """
            GROUP BY year, metric_key
            ORDER BY year DESC, metric_key
        """

        params = list(metric_keys) + scope_params
        cursor.execute(sql, tuple(params))

        rows = cursor.fetchall()

        yearly_map = {}
        for row in rows:
            year = int(row['year'])
            if year not in yearly_map:
                scope_title = '广西' if scope == 'guangxi' else '国家卫健委' if scope == 'national' else '多来源'
                yearly_map[year] = {
                    'report_id': year,
                    'title': f'{year}年{scope_title}医疗服务核心指标汇总',
                    'category': '结构化OCR',
                    'publish_date': f'{year}-12-31',
                    'metric_count': 0,
                    'metrics': [],
                }

            metric_key = row['metric_key']
            metric_name, unit = metric_meta.get(metric_key, (metric_key, ''))
            yearly_map[year]['metrics'].append({
                'metric_name': metric_name,
                'metric_key': metric_key,
                'metric_value': row['avg_value'],
                'sample_count': int(row['sample_count'] or 0),
                'unit': unit,
            })

        summary_data = []
        for year in sorted(yearly_map.keys(), reverse=True):
            report = yearly_map[year]
            report['metric_count'] = len(report['metrics'])
            summary_data.append(report)
        
        conn.close()
        
        return jsonify({
            "status": "success",
            "total_reports": len(summary_data),
            "data": summary_data,
            "meta": {
                "source": "health_ocr_metrics",
                "scope": scope,
                "scope_label": SCOPE_LABELS.get(scope, scope),
                "year_min": summary_data[-1]['report_id'] if summary_data else None,
                "year_max": summary_data[0]['report_id'] if summary_data else None,
            }
        })
    except Exception as e:
        return jsonify({"error": str(e), "data": []}), 500


@app.route('/api/analysis/module-status', methods=['GET'])
def get_module_status():
    """按数据支撑情况返回模块完成度概览"""
    if not is_role('admin'):
        return admin_forbidden_response()

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor()

        scope = get_scope()

        table_counts = {}
        for table_name in ['medical_institution', 'hospital_bed', 'health_ocr_metrics']:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            table_counts[table_name] = int(cursor.fetchone()[0])

        if scope in {'guangxi', 'all'}:
            cursor.execute("SELECT COUNT(*) FROM population_data")
            table_counts['population_data'] = int(cursor.fetchone()[0])
        else:
            table_counts['population_data'] = 0

        metrics_map = {
            'personnel': ['doctor_count', 'nurse_count'],
            'bed': ['bed_count', 'bed_usage_rate'],
            'service': ['outpatient_visits', 'discharge_count', 'avg_stay_days'],
            'cost': ['outpatient_cost', 'discharge_cost'],
        }

        metric_support = {}
        scope_clause, scope_params = build_metric_scope_filter(scope)
        for key, metric_keys in metrics_map.items():
            placeholders = ','.join(['%s'] * len(metric_keys))
            sql = f"""
                SELECT COUNT(*) FROM health_ocr_metrics
                WHERE metric_key IN ({placeholders}) AND metric_value IS NOT NULL
            """
            params = list(metric_keys)
            if scope_clause:
                sql += f" AND {scope_clause}"
                params.extend(scope_params)
            cursor.execute(sql, tuple(params))
            metric_support[key] = int(cursor.fetchone()[0])

        conn.close()

        modules = [
            {
                'module': '首页模块',
                'status': 'completed',
                'detail': '登录/注册、仪表板、新闻面板已完成',
            },
            {
                'module': '人口信息统计分析',
                'status': 'partial' if table_counts['population_data'] > 0 else 'skipped',
                'detail': f"population_data 当前 {table_counts['population_data']} 条",
            },
            {
                'module': '医疗卫生机构统计分析',
                'status': 'skipped' if table_counts['medical_institution'] == 0 else 'partial',
                'detail': f"medical_institution 当前 {table_counts['medical_institution']} 条，缺数据支撑已跳过",
            },
            {
                'module': '医疗卫生人员统计分析',
                'status': 'partial' if metric_support['personnel'] > 0 else 'skipped',
                'detail': f"OCR 指标记录 {metric_support['personnel']} 条（doctor/nurse）",
            },
            {
                'module': '医疗卫生床位统计分析',
                'status': 'partial' if metric_support['bed'] > 0 else 'skipped',
                'detail': f"OCR 指标记录 {metric_support['bed']} 条（bed）",
            },
            {
                'module': '医疗服务统计分析',
                'status': 'partial' if metric_support['service'] > 0 else 'skipped',
                'detail': f"OCR 指标记录 {metric_support['service']} 条（service）",
            },
            {
                'module': '医疗费用统计分析',
                'status': 'partial' if metric_support['cost'] > 0 else 'skipped',
                'detail': f"OCR 指标记录 {metric_support['cost']} 条（cost）",
            },
        ]

        return jsonify({
            'status': 'success',
            'data': modules,
            'meta': {
                'scope': scope,
                'scope_label': SCOPE_LABELS.get(scope, scope),
                'table_counts': table_counts,
                'metric_support': metric_support,
            }
        })
    except Exception as e:
        return jsonify({'error': str(e), 'data': []}), 500


@app.route('/api/analysis/data-summary', methods=['GET'])
def get_analysis_data_summary():
    """基于结构化结果输出可分析摘要（人口 + 人员/床位/服务/费用）"""
    if not is_role('admin'):
        return admin_forbidden_response()

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)

        scope = get_scope()

        # 人口数据（目前仅在省级数据中启用）
        if scope in {'guangxi', 'all'}:
            cursor.execute(
                """
                SELECT district, COUNT(*) AS person_count, ROUND(AVG(health_score), 2) AS avg_health_score
                FROM population_data
                GROUP BY district
                ORDER BY person_count DESC
                """
            )
            population_by_district = cursor.fetchall()
        else:
            population_by_district = []

        # OCR结构化指标按年汇总
        scope_clause, scope_params = build_metric_scope_filter(scope)
        sql = f"""
            SELECT
                year,
                metric_key,
                ROUND(AVG(metric_value), 4) AS avg_value,
                COUNT(*) AS sample_count
            FROM health_ocr_metrics
            WHERE metric_value IS NOT NULL
              AND metric_key IN (
                  'doctor_count', 'nurse_count',
                  'bed_count', 'bed_usage_rate',
                  'outpatient_visits', 'discharge_count', 'avg_stay_days',
                  'outpatient_cost', 'discharge_cost'
              )
              AND year IS NOT NULL
              AND {OCR_METRIC_VALID_SQL}
        """

        if scope_clause:
            sql += f"\n              AND {scope_clause}"

        sql += """
            GROUP BY year, metric_key
            ORDER BY year, metric_key
        """

        cursor.execute(sql, tuple(scope_params))
        yearly_metrics = cursor.fetchall()

        conn.close()

        return jsonify({
            'status': 'success',
            'data': {
                'population_by_district': population_by_district,
                'yearly_metrics': yearly_metrics,
            },
            'meta': {
                'scope': scope,
                'scope_label': SCOPE_LABELS.get(scope, scope),
                'population_rows': len(population_by_district),
                'yearly_metric_rows': len(yearly_metrics),
            }
        })
    except Exception as e:
        return jsonify({'error': str(e), 'data': {}}), 500


@app.route('/admin/api/action', methods=['POST'])
def admin_action():
    if not is_role('admin'):
        return admin_forbidden_response()

    payload = request.get_json(silent=True) or {}
    action = payload.get('action', '').strip()

    action_messages = {
        'weekly_report': '周报任务已提交，预计 2 分钟后生成。',
        'configure_alert': '预警规则配置面板已记录操作请求。',
        'export_stats': '机构统计导出任务已加入队列。',
        'quality_check': '数据质量巡检已开始执行。',
    }

    if action not in action_messages:
        return jsonify({"error": "不支持的操作类型"}), 400

    push_admin_alert(f"管理快捷操作已执行: {action}", '中')

    return jsonify({"ok": True, "message": action_messages[action]})


@app.route('/admin/api/alerts', methods=['GET'])
def admin_alerts():
    if not is_role('admin'):
        return admin_forbidden_response()

    items = sorted(ADMIN_ALERTS, key=lambda item: int(item.get('id', 0)), reverse=True)
    return jsonify({"items": items[:20]})


@app.route('/user/api/profile', methods=['GET'])
def user_profile():
    if not is_role('user'):
        return user_forbidden_response()

    return jsonify(
        {
            "username": session.get('user', 'user'),
            "synced_at": "今日 09:30",
            "health_index": "稳定",
            "advice": "建议保持每周 4 次有氧运动",
            "score": 82,
            "integrity": "96%",
            "report_count": 3,
        }
    )


@app.route('/user/api/tips', methods=['GET'])
def user_tips():
    if not is_role('user'):
        return user_forbidden_response()

    return jsonify({"items": USER_TIPS})


@app.route('/user/api/reminders', methods=['GET'])
def user_reminders():
    if not is_role('user'):
        return user_forbidden_response()

    return jsonify({"items": USER_REMINDERS})


@app.route('/user/api/trend', methods=['GET'])
def user_trend():
    if not is_role('user'):
        return user_forbidden_response()

    return jsonify({"labels": TREND_LABELS, "values": TREND_VALUES})


# Register document upload blueprint
try:
    from .document import document_bp
except ImportError:
    from document import document_bp
app.register_blueprint(document_bp)


if __name__ == '__main__':
    # 启动 Flask 服务，默认 5000 端口
    print("Health BigData API Service Started: http://127.0.0.1:5000/api/health-stats")
    print("Login Page: http://127.0.0.1:5000/login")
    app.run(debug=True)
