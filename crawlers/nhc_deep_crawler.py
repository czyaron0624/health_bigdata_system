"""
国家卫健委医疗机构统计数据爬虫 (nhc.gov.cn)
功能：从国家卫健委的医疗机构运行情况统计数据中抽取表格
"""

import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NHCDeepCrawler:
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

    def parse_nhc_unit_table(self, html_content):
        """从医疗机构查询页面提取表格数据"""
        soup = BeautifulSoup(html_content, 'html.parser')
        
        metrics = []
        
        # 尝试多种表格选择器
        selectors = ['table', 'div.table', 'div[class*="table"]']
        
        for selector in selectors:
            tables = soup.select(selector)
            logger.info(f"  使用选择器 '{selector}' 找到 {len(tables)} 个表格")
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows:
                    cols = row.find_all(['td', 'th'])
                    
                    # 尝试提取数据行（通常是 名称 - 数值 - 单位的格式）
                    if len(cols) >= 2:
                        col_texts = [col.get_text(strip=True) for col in cols]
                        
                        # 跳过表头或空行
                        if col_texts[0] in ['序号', '名称', '机构'] or not col_texts[0]:
                            continue
                        
                        metric_name = col_texts[0]
                        metric_value = col_texts[1] if len(col_texts) > 1 else ""
                        unit = col_texts[2] if len(col_texts) > 2 else ""
                        
                        if metric_name and metric_value and metric_value != "":
                            metrics.append({
                                'name': metric_name,
                                'value': metric_value,
                                'unit': unit
                            })
                            logger.info(f"    找到: {metric_name} = {metric_value} {unit}")
        
        return metrics

    def crawl_nhc_stats(self):
        """从国家卫健委官网爬取卫生统计数据"""
        logger.info("🚀 开始从国家卫健委提取卫生统计数据...")
        
        # 国家卫健委的统计数据URL
        nhc_urls = {
            "医疗机构运行情况": "http://zgcx.nhc.gov.cn/unit",
            "卫生监督情况": "http://www.nhc.gov.cn/wjw/",
            "疾病防控": "http://www.nhc.gov.cn/jkfys/",
        }
        
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor(dictionary=True)
            
            for category, url in nhc_urls.items():
                logger.info(f"\n📄 爬取类别: {category}")
                logger.info(f"   URL: {url}")
                
                try:
                    response = requests.get(url, headers=self.headers, timeout=15, verify=False)
                    response.encoding = 'utf-8'
                    
                    logger.info(f"   状态码: {response.status_code}")
                    
                    # 解析表格
                    metrics = self.parse_nhc_unit_table(response.text)
                    
                    if metrics:
                        logger.info(f"   ✅ 提取到 {len(metrics)} 个指标")
                        
                        # 创建或更新国家报告记录
                        title = f"国家卫健委{category}统计表"
                        
                        # 检查是否已存在
                        cursor.execute(
                            "SELECT id FROM national_news WHERE title = %s",
                            (title,)
                        )
                        existing = cursor.fetchone()
                        
                        if existing:
                            report_id = existing['id']
                            logger.info(f"   使用已存在的报告ID: {report_id}")
                        else:
                            # 插入新报告
                            cursor.execute("""
                                INSERT INTO national_news (title, link, source_category, publish_date)
                                VALUES (%s, %s, %s, NOW())
                            """, (title, url, f"国家卫健委-{category}"))
                            conn.commit()
                            report_id = cursor.lastrowid
                            logger.info(f"   创建新报告ID: {report_id}")
                        
                        # 清空该报告已有的指标（防止重复）
                        cursor.execute(
                            "DELETE FROM report_metrics WHERE report_id = %s",
                            (report_id,)
                        )
                        conn.commit()
                        
                        # 插入指标数据
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
                            except Exception as e:
                                logger.debug(f"    ⚠️ 插入失败: {e}")
                        
                        conn.commit()
                        logger.info(f"   ✅ 已保存 {len(metrics)} 个指标到数据库")
                    else:
                        logger.warning(f"   ⚠️ 未找到表格数据")
                    
                    time.sleep(2)
                
                except Exception as e:
                    logger.warning(f"   ❌ 爬取失败: {e}")
                    continue
            
            logger.info("\n🎉 国家卫健委数据爬取完成！")
            self.show_stats()
        
        except Exception as e:
            logger.error(f"❌ 爬虫出错: {e}")
        finally:
            if conn:
                conn.close()

    def show_stats(self):
        """显示统计结果"""
        logger.info("\n📊 数据库统计结果:")
        logger.info("=" * 60)
        
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor(dictionary=True)
            
            # 时间内统计
            cursor.execute("""
                SELECT 
                    COUNT(DISTINCT report_id) as report_count,
                    COUNT(*) as metric_count
                FROM report_metrics
            """)
            
            stats = cursor.fetchone()
            logger.info(f"报告数: {stats['report_count']}")
            logger.info(f"指标数: {stats['metric_count']}")
            
            # 显示样本数据
            logger.info("\n📋 样本数据:")
            cursor.execute("""
                SELECT 
                    nn.title,
                    rm.metric_name,
                    rm.metric_value,
                    rm.unit
                FROM report_metrics rm
                JOIN national_news nn ON rm.report_id = nn.id
                LIMIT 10
            """)
            
            samples = cursor.fetchall()
            for row in samples:
                logger.info(f"  {row['title'][:40]}: {row['metric_name']} = {row['metric_value']} {row['unit']}")
        
        except Exception as e:
            logger.error(f"查询失败: {e}")
        finally:
            if conn:
                conn.close()
        
        logger.info("=" * 60)

    def run(self):
        logger.info("=" * 60)
        logger.info("国家卫健委深度数据爬虫启动")
        logger.info("=" * 60)
        
        try:
            self.crawl_nhc_stats()
        except KeyboardInterrupt:
            logger.info("\n⚠️ 用户中断")
        except Exception as e:
            logger.error(f"❌ 错误: {e}")


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    crawler = NHCDeepCrawler()
    crawler.run()
