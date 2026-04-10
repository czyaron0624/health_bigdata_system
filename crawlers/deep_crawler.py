"""
国家卫健委公报深度爬虫
功能：自动访问每条公报链接，提取内部表格数据并存储
"""

import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DeepHealthDataCrawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "health_db"
        }

    def connect_db(self):
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            raise

    def extract_table_data(self, html_content):
        """从HTML中提取表格数据"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        metrics = []
        tables = soup.find_all('table')
        
        logger.info(f"  发现 {len(tables)} 个表格")
        
        for table_idx, table in enumerate(tables):
            rows = table.find_all('tr')
            
            for row in rows[1:]:  # 跳过表头
                try:
                    cols = row.find_all('td')
                    
                    if len(cols) >= 2:
                        # 第一列是指标名，第二列是数值
                        metric_name = cols[0].get_text(strip=True)
                        metric_value = cols[1].get_text(strip=True)
                        
                        # 清洗数据
                        metric_name = re.sub(r'\s+', ' ', metric_name)
                        metric_value = re.sub(r'\s+', '', metric_value)
                        
                        # 尝试提取单位（如果有第三列）
                        unit = ""
                        if len(cols) >= 3:
                            unit = cols[2].get_text(strip=True)
                        
                        if metric_name and metric_value:
                            metrics.append({
                                'name': metric_name,
                                'value': metric_value,
                                'unit': unit
                            })
                
                except Exception as e:
                    logger.debug(f"行解析失败: {e}")
                    continue
        
        return metrics

    def crawl_reports_deep(self):
        """深度爬取所有公报的内部数据"""
        logger.info("🚀 开始深度爬取公报内部表格数据...")
        
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor(dictionary=True)
            
            # 获取所有需要爬的公报链接
            cursor.execute("""
                SELECT id, title, link FROM national_news 
                WHERE link IS NOT NULL AND link != ''
                ORDER BY id DESC LIMIT 10
            """)
            reports = cursor.fetchall()
            
            logger.info(f"找到 {len(reports)} 条公报待爬取")
            
            for report in reports:
                report_id = report['id']
                title = report['title']
                link = report['link']
                
                logger.info(f"\n📄 正在爬取: {title[:50]}")
                logger.info(f"   链接: {link}")
                
                try:
                    # 访问公报详情页
                    response = requests.get(link, headers=self.headers, timeout=15, verify=False)
                    response.encoding = 'utf-8'
                    
                    # 提取表格数据
                    metrics = self.extract_table_data(response.text)
                    
                    if metrics:
                        logger.info(f"   提取到 {len(metrics)} 个指标")
                        
                        # 批量插入数据库
                        for metric in metrics:
                            try:
                                insert_sql = """
                                    INSERT INTO report_metrics 
                                    (report_id, metric_name, metric_value, unit) 
                                    VALUES (%s, %s, %s, %s)
                                """
                                cursor.execute(insert_sql, (
                                    report_id,
                                    metric['name'][:100],
                                    metric['value'][:50],
                                    metric['unit'][:20]
                                ))
                                
                                logger.info(f"     ✅ {metric['name']}: {metric['value']} {metric['unit']}")
                            
                            except mysql.connector.Error as e:
                                if "Duplicate entry" in str(e):
                                    logger.debug(f"     ⚠️ 重复数据: {metric['name']}")
                                else:
                                    logger.warning(f"     ❌ 插入失败: {e}")
                        
                        conn.commit()
                    else:
                        logger.warning(f"   ⚠️ 未找到表格数据")
                    
                    # 减速
                    time.sleep(2)
                
                except requests.exceptions.RequestException as e:
                    logger.warning(f"   ❌ 网络请求失败: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"   ❌ 爬取失败: {e}")
                    continue
            
            logger.info("\n🎉 深度爬取完成！")
            
            # 统计结果
            cursor.execute("SELECT COUNT(*) as cnt FROM report_metrics")
            total = cursor.fetchone()['cnt']
            logger.info(f"📊 report_metrics 表现有 {total} 条指标数据")
        
        except Exception as e:
            logger.error(f"❌ 深度爬取失败: {e}")
        finally:
            if conn:
                conn.close()

    def show_collected_metrics(self):
        """显示已采集的指标数据"""
        logger.info("\n📊 已采集的指标数据:")
        logger.info("=" * 60)
        
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor(dictionary=True)
            
            # 按报告分组统计
            cursor.execute("""
                SELECT 
                    r.id, r.title,
                    COUNT(m.id) as metric_count,
                    GROUP_CONCAT(CONCAT(m.metric_name, ': ', m.metric_value, m.unit) SEPARATOR ' | ') as metrics
                FROM national_news r
                LEFT JOIN report_metrics m ON r.id = m.report_id
                GROUP BY r.id
                HAVING metric_count > 0
                LIMIT 5
            """)
            
            results = cursor.fetchall()
            
            if results:
                for row in results:
                    print(f"\n📄 {row['title'][:50]}")
                    print(f"   指标数: {row['metric_count']}")
                    if row['metrics']:
                        metrics_list = row['metrics'].split(' | ')
                        for metric in metrics_list[:5]:  # 只显示前5个
                            print(f"   - {metric}")
                        if len(metrics_list) > 5:
                            print(f"   ... 还有 {len(metrics_list) - 5} 个指标")
            else:
                print("\n⚠️ 暂无采集到的指标数据")
        
        except Exception as e:
            logger.error(f"查询失败: {e}")
        finally:
            if conn:
                conn.close()
        
        logger.info("=" * 60)

    def run(self):
        """运行深度爬虫"""
        logger.info("=" * 60)
        logger.info("国家卫健委公报深度爬虫启动")
        logger.info("=" * 60)
        
        try:
            self.crawl_reports_deep()
            time.sleep(1)
            self.show_collected_metrics()
        except KeyboardInterrupt:
            logger.info("\n⚠️ 用户中断爬虫")
        except Exception as e:
            logger.error(f"\n❌ 爬虫出错: {e}")


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    crawler = DeepHealthDataCrawler()
    crawler.run()
