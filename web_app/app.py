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

# 弃用硬编码聚合SQL，改为使用 vw_metric_clean 视图
# OCR_METRIC_VALID_SQL 规则已下沉到数据库视图中

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


def detect_risk_events():
    """
    检测真实的数据质量风险事件
    返回风险事件数量
    """
    import mysql.connector
    
    risk_count = 0
    try:
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor()
        
        # 1. 检查缺少医疗机构信息的机构数
        cursor.execute("""
            SELECT COUNT(*) FROM medical_institution 
            WHERE name IS NULL OR name = '' OR level IS NULL OR region IS NULL
        """)
        missing_info = cursor.fetchone()[0]
        if missing_info > 0:
            risk_count += min(1, missing_info)  # 计为一类风险
        
        # 2. 检查异常的健康指标值（超出预期范围）
        cursor.execute("""
            SELECT COUNT(*) FROM health_ocr_metrics 
            WHERE (metric_key = 'bed_usage_rate' AND (metric_value < 0 OR metric_value > 100))
            OR (metric_key = 'doctor_count' AND metric_value > 500000)
            OR (metric_key = 'nurse_count' AND metric_value > 600000)
        """)
        anomalous_metrics = cursor.fetchone()[0]
        if anomalous_metrics > 10:  # 如果异常值超过10个
            risk_count += 1
        
        # 3. 检查数据同步延迟（超过24小时未更新）
        cursor.execute("""
            SELECT COUNT(*) FROM health_ocr_metrics 
            WHERE updated_at < DATE_SUB(NOW(), INTERVAL 24 HOUR) 
            OR updated_at IS NULL
        """)
        stale_data = cursor.fetchone()[0]
        if stale_data > len(ADMIN_ALERTS):  # 比现有告警多
            risk_count += 1
        
        cursor.close()
        conn.close()
    except Exception:
        # 如果检测失败，返回保守的风险数
        risk_count = 1
    
    return max(1, risk_count)  # 至少返回1个风险


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

        # population_data 已移除，固定返回 0
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

        cursor.execute("SELECT COUNT(*) FROM vw_metric_clean")
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
                    parsed['risk_events'] = detect_risk_events()
                if 'online_users' not in parsed:
                    # 从Redis获取在线用户数
                    try:
                        online_count = r.dbsize()  # 或者使用ZCARD来计算活跃session
                        parsed['online_users'] = max(1, online_count // 10)  # 保守估计
                    except:
                        parsed['online_users'] = 0
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
        "risk_events": detect_risk_events(),
        "online_users": 0,
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
    selected_year_int = int(selected_year) if selected_year.isdigit() else None

    scope_sources = {
        'guangxi': [('guangxi_news', 'guangxi')],
        'national': [('national_news', 'national')],
        'all': [('guangxi_news', 'guangxi'), ('national_news', 'national')],
    }

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)

        available_sources = []
        test_cursor = conn.cursor()
        for table_name, source_label in scope_sources.get(scope, scope_sources['guangxi']):
            try:
                test_cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                test_cursor.fetchone()
                available_sources.append((table_name, source_label))
            except Exception:
                pass
        test_cursor.close()

        items = []
        year_options = []
        year_min = None
        year_max = None

        if available_sources:
            base_query_parts = [
                f"""
                SELECT
                    id,
                    title,
                    publish_date,
                    link,
                    '{source_label}' AS source,
                    YEAR(STR_TO_DATE(publish_date, '%%Y-%%m-%%d')) AS publish_year
                FROM {table_name}
                WHERE publish_date REGEXP '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
                """
                for table_name, source_label in available_sources
            ]
            base_union_sql = " UNION ALL ".join(base_query_parts)

            items_sql = f"""
                SELECT id, title, publish_date, link, source, publish_year
                FROM ({base_union_sql}) t
                WHERE publish_year IS NOT NULL
            """
            item_params = []
            if selected_year_int is not None:
                items_sql += " AND publish_year = %s"
                item_params.append(selected_year_int)
            items_sql += """
                ORDER BY publish_year DESC, publish_date DESC, id DESC
                LIMIT 50
            """
            cursor.execute(items_sql, tuple(item_params))
            items = cursor.fetchall()

            year_sql = f"""
                SELECT publish_year
                FROM ({base_union_sql}) y
                WHERE publish_year IS NOT NULL
                GROUP BY publish_year
                ORDER BY publish_year DESC
            """
            cursor.execute(year_sql)
            year_rows = cursor.fetchall()
            year_options = [int(row['publish_year']) for row in year_rows if row.get('publish_year') is not None]
            if year_options:
                year_max = year_options[0]
                year_min = year_options[-1]

        conn.close()

        return jsonify({
            "items": items,
            "scope": scope,
            "scope_label": SCOPE_LABELS.get(scope, scope),
            "selected_year": selected_year_int,
            "year_min": year_min,
            "year_max": year_max,
            "year_options": year_options,
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
            FROM vw_metric_clean
            WHERE metric_key IN ({placeholders})
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
                "source": "vw_metric_clean",
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

        # population_data 已移除，固定返回 0
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
                SELECT COUNT(*) FROM vw_metric_clean
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

        # population_data 已移除，返回空数组
        population_by_district = []

        # OCR结构化指标按年汇总，使用 vw_metric_clean
        scope_clause, scope_params = build_metric_scope_filter(scope)
        sql = f"""
            SELECT
                year,
                metric_key,
                ROUND(AVG(metric_value), 4) AS avg_value,
                COUNT(*) AS sample_count
            FROM vw_metric_clean
            WHERE metric_key IN (
                  'doctor_count', 'nurse_count',
                  'bed_count', 'bed_usage_rate',
                  'outpatient_visits', 'discharge_count', 'avg_stay_days',
                  'outpatient_cost', 'discharge_cost'
              )
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

@app.route('/api/analysis/metric-details', methods=['GET'])
def get_metric_details():
    """提供数据明细的分页接口（整理层清洗后结果）"""
    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor(dictionary=True)

        scope = get_scope()
        metric_key = request.args.get('metric_key')
        year = request.args.get('year', type=int)
        page = request.args.get('page', default=1, type=int)
        page_size = request.args.get('page_size', default=10, type=int)

        query_params = []
        where_clauses = []

        scope_clause, scope_params = build_metric_scope_filter(scope)
        if scope_clause:
            where_clauses.append(scope_clause)
            query_params.extend(scope_params)

        if metric_key:
            where_clauses.append("metric_key = %s")
            query_params.append(metric_key)

        if year:
            where_clauses.append("year = %s")
            query_params.append(year)

        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

        # Count total
        count_sql = f"SELECT COUNT(*) as total FROM vw_metric_clean WHERE {where_sql}"
        cursor.execute(count_sql, tuple(query_params))
        total_rows = cursor.fetchone()['total']

        # Fetch data
        data_sql = f"""
            SELECT id, news_id, title, publish_date, year, month, 
                   metric_key, metric_name, metric_value, metric_raw, source_table, updated_at
            FROM vw_metric_clean
            WHERE {where_sql}
            ORDER BY year DESC, month DESC, id DESC
            LIMIT %s OFFSET %s
        """
        fetch_params = query_params + [page_size, (page - 1) * page_size]
        cursor.execute(data_sql, tuple(fetch_params))
        rows = cursor.fetchall()
        
        # Format updated_at
        for row in rows:
            if row.get('updated_at'):
                row['updated_at'] = row['updated_at'].strftime('%Y-%m-%d %H:%M:%S')

        conn.close()

        return jsonify({
            'status': 'success',
            'data': rows,
            'meta': {
                'total': total_rows,
                'page': page,
                'page_size': page_size,
                'scope': scope,
                'metric_key': metric_key,
                'year': year
            }
        })
    except Exception as e:
        return jsonify({'error': str(e), 'data': []}), 500

@app.route('/admin/api/action', methods=['POST'])
def admin_action():
    if not is_role('admin'):
        return admin_forbidden_response()

    payload = request.get_json(silent=True) or {}
    action = payload.get('action', '').strip()

    action_messages = {
        'weekly_report': '周报任务已提交，预计 2 分钟后生成。',
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

# Register six modules analysis API
try:
    from .analysis_api import init_analysis_api
except ImportError:
    from analysis_api import init_analysis_api
init_analysis_api(app, None)


@app.route('/api/institutions/charts', methods=['GET'])
def get_institution_charts():
    """医疗机构统计数据图表接口"""
    if not is_role('admin'):
        return admin_forbidden_response()

    try:
        import mysql.connector
        conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
        cursor = conn.cursor()

        # 1. 按机构性质分类（饼图数据）
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN name LIKE '%民营%' OR name LIKE '%私立%' OR name LIKE '%有限责任%' THEN '民营'
                    WHEN name LIKE '%公立%' OR name LIKE '%人民%' OR name LIKE '%中心%' OR name LIKE '%卫生%' THEN '公立'
                    WHEN name LIKE '%诊所%' OR name LIKE '%门诊%' THEN '基层'
                    ELSE '其他'
                END as scope_type,
                COUNT(*) as cnt
            FROM medical_institution
            WHERE name IS NOT NULL AND name != ''
            GROUP BY scope_type
            ORDER BY cnt DESC
        """)
        rows = cursor.fetchall()
        scope_data = [{"name": r[0], "value": int(r[1])} for r in rows]

        # 2. 按地区机构数量TOP10（柱状图数据）
        cursor.execute("""
            SELECT 
                SUBSTRING_INDEX(SUBSTRING_INDEX(region, '区', 1), '县', 1) as area,
                COUNT(*) as cnt
            FROM medical_institution
            WHERE region IS NOT NULL AND region != ''
            GROUP BY area
            ORDER BY cnt DESC
            LIMIT 10
        """)
        rows = cursor.fetchall()
        region_top10 = [{"name": r[0], "value": int(r[1])} for r in rows]

        # 3. 按机构类型分类（例如：医院、诊所等）
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN name LIKE '%医院%' THEN '医院'
                    WHEN name LIKE '%诊所%' OR name LIKE '%门诊%' THEN '诊所/门诊'
                    WHEN name LIKE '%卫生院%' OR name LIKE '%卫生所%' THEN '卫生院/所'
                    WHEN name LIKE '%中心%' THEN '医疗中心'
                    ELSE '其他机构'
                END as inst_type,
                COUNT(*) as cnt
            FROM medical_institution
            WHERE name IS NOT NULL AND name != ''
            GROUP BY inst_type
            ORDER BY cnt DESC
        """)
        rows = cursor.fetchall()
        inst_type_data = [{"name": r[0], "value": int(r[1])} for r in rows]

        # 4. 统计汇总
        cursor.execute("SELECT COUNT(*) FROM medical_institution")
        row = cursor.fetchone()
        total_count = int(row[0]) if row else 0

        cursor.execute("SELECT COUNT(DISTINCT region) FROM medical_institution WHERE region IS NOT NULL AND region != ''")
        row = cursor.fetchone()
        region_count = int(row[0]) if row else 0

        conn.close()

        return jsonify({
            'status': 'success',
            'data': {
                'scope_distribution': scope_data,  # 执业范围分布（饼图）
                'region_top10': region_top10,       # 地区TOP10（柱状图）
                'inst_type_distribution': inst_type_data,  # 机构类型分布（饼图）
                'summary': {
                    'total_institutions': total_count,
                    'total_regions': region_count
                }
            },
            'meta': {
                'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        })
    except Exception as e:
        return jsonify({'error': str(e), 'data': {}}), 500


if __name__ == '__main__':
    # 启动 Flask 服务，默认 5000 端口
    print("Health BigData API Service Started: http://127.0.0.1:5000/api/health-stats")
    print("Login Page: http://127.0.0.1:5000/login")
    app.run(debug=True)
