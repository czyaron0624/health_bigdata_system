import requests
from bs4 import BeautifulSoup
import mysql.connector
import time

def crawl_report_detail():
    try:
        conn = mysql.connector.connect(host="localhost", user="root", password="rootpassword", database="health_db")
        cursor = conn.cursor(dictionary=True)

        # 1. 从主表拿到所有的公报链接
        cursor.execute("SELECT id, title, link FROM gov_news WHERE title LIKE '%指标表%'")
        reports = cursor.fetchall()

        for report in reports:
            print(f"🧐 正在解析: {report['title']}")
            
            # 2. 请求详情页
            headers = {'User-Agent': 'Mozilla/5.0...'}
            res = requests.get(report['link'], headers=headers, timeout=10, verify=False)
            res.encoding = 'utf-8'
            soup = BeautifulSoup(res.text, 'html.parser')

            # 3. 寻找表格 (政府公报数据通常在 table 标签里)
            table = soup.find('table')
            if not table:
                print(f"⚠️ {report['title']} 未找到表格数据，跳过。")
                continue

            rows = table.find_all('tr')
            for row in rows[1:]:  # 跳过表头
                cols = row.find_all('td')
                if len(cols) >= 2:
                    # 提取指标名称和数值（简单清洗）
                    name = cols[0].get_text(strip=True)
                    value = cols[1].get_text(strip=True)
                    
                    # 4. 存入指标明细表
                    insert_sql = "INSERT INTO report_metrics (report_id, metric_name, metric_value) VALUES (%s, %s, %s)"
                    cursor.execute(insert_sql, (report['id'], name, value))
            
            conn.commit()
            print(f"✅ {report['title']} 数据抓取成功！")
            time.sleep(2) # 优雅减速

    except Exception as e:
        print(f"❌ 深度爬取失败: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    crawl_report_detail()