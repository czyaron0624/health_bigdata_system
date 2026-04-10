"""
国家卫生健康委员会（nhc.gov.cn）数据爬虫
爬取目标：
  1. 全国医疗卫生机构数据
  2. 卫生统计年鉴数据
  3. 健康中国建设进展数据
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import mysql.connector
import time
import re
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NationalHealthCrawler:
    def __init__(self):
        self.base_url = "http://www.nhc.gov.cn"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "health_db"
        }

    def connect_db(self):
        """连接数据库"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            return conn
        except mysql.connector.Error as err:
            logger.error(f"数据库连接失败: {err}")
            raise

    def crawl_national_stats(self):
        """爬取全国卫生统计数据"""
        logger.info("🚀 开始爬取全国卫生健康统计数据...")
        
        # 卫生统计相关的新闻/公报页面
        urls = {
            "医疗卫生统计": "http://www.nhc.gov.cn/wjw/gfxwzx/",
            "健康中国建设": "http://www.nhc.gov.cn/jkzg/",
            "新闻发布会": "http://www.nhc.gov.cn/xcs/yqfkdt/",
        }

        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            for category, url in urls.items():
                logger.info(f"📥 正在爬取: {category}")
                
                try:
                    response = requests.get(url, headers=self.headers, timeout=20, verify=False)
                    response.encoding = 'utf-8'
                    soup = BeautifulSoup(response.text, 'html.parser')

                    # 寻找所有链接（新闻/公报）
                    links = soup.find_all('a', href=True)
                    count = 0

                    for link_tag in links:
                        title = link_tag.get_text(strip=True)
                        href = link_tag.get('href', '')

                        # 过滤：标题长度、排除空链接和无关链接
                        if len(title) < 8 or not href or '返回' in title or '首页' in title:
                            continue

                        full_url = urljoin(self.base_url, href)

                        # 查重防重复插入
                        check_sql = "SELECT id FROM national_news WHERE link = %s"
                        cursor.execute(check_sql, (full_url,))
                        if cursor.fetchone():
                            continue

                        # 插入数据库（全国数据）
                        insert_sql = "INSERT INTO national_news (title, link, source_category, publish_date) VALUES (%s, %s, %s, NOW())"
                        cursor.execute(insert_sql, (title, full_url, f"国家卫健委-{category}"))
                        conn.commit()

                        logger.info(f"  ✅ 已保存: {title[:50]}")
                        count += 1

                        # 减速
                        time.sleep(0.5)

                    logger.info(f"  📊 {category} 本轮新增 {count} 条记录\n")

                except Exception as e:
                    logger.warning(f"  ⚠️ 爬取 {category} 失败: {e}")
                    continue

            logger.info("🎉 国家卫健委数据爬取完成！")

        except Exception as e:
            logger.error(f"❌ 爬虫运行失败: {e}")
        finally:
            if conn:
                conn.close()

    def crawl_province_data(self):
        """爬取各省卫健部门纳入的数据汇总
        
        尝试从国家卫健委的数据汇总页获取各省统计数据
        """
        logger.info("🚀 开始爬取各省卫生统计汇总数据...")

        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            # 国家卫健委数据汇总页（示例）
            summary_url = "http://www.nhc.gov.cn/wjw/gfxwzx/list.shtml"

            response = requests.get(summary_url, headers=self.headers, timeout=20, verify=False)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            # 解析表格或列表（根据实际页面结构调整）
            items = soup.find_all('li')

            for item in items:
                try:
                    link_tag = item.find('a')
                    span_tag = item.find('span')

                    if not link_tag:
                        continue

                    title = link_tag.get_text(strip=True)
                    href = link_tag.get('href', '')
                    date = span_tag.get_text(strip=True) if span_tag else None

                    if not title or len(title) < 5:
                        continue

                    full_url = urljoin(self.base_url, href)

                    # 查重
                    check_sql = "SELECT id FROM national_news WHERE link = %s"
                    cursor.execute(check_sql, (full_url,))
                    if cursor.fetchone():
                        continue

                    # 保存（全国数据）
                    insert_sql = "INSERT INTO national_news (title, link, publish_date, source_category) VALUES (%s, %s, %s, %s)"
                    cursor.execute(insert_sql, (title, full_url, date or "未知", "国家卫健委-汇总"))
                    conn.commit()

                    logger.info(f"✅ 已保存: {title[:50]}")

                    time.sleep(0.5)

                except Exception as e:
                    logger.debug(f"单条解析失败: {e}")
                    continue

            logger.info("🎉 各省数据爬取完成！")

        except Exception as e:
            logger.error(f"❌ 省级数据爬取失败: {e}")
        finally:
            if conn:
                conn.close()

    def crawl_hospital_data(self):
        """爬取医疗机构相关统计信息
        
        从国家卫健委官网获取医院、基层卫生机构等统计数据
        """
        logger.info("🚀 开始爬取医疗机构统计数据...")

        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()

            # 医疗机构相关页面
            hospital_url = "http://www.nhc.gov.cn/wjw/yljgss/"

            response = requests.get(hospital_url, headers=self.headers, timeout=20, verify=False)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            # 尝试找到表格或结构化数据
            tables = soup.find_all('table')

            for table in tables:
                rows = table.find_all('tr')

                for row in rows[1:]:  # 跳过表头
                    try:
                        cols = row.find_all('td')
                        if len(cols) >= 2:
                            # 第一列为指标名，后续列为数值
                            metric_name = cols[0].get_text(strip=True)
                            metric_value = cols[1].get_text(strip=True)

                            # 清洗数据（去除多余空白）
                            metric_name = re.sub(r'\s+', ' ', metric_name)
                            metric_value = re.sub(r'\s+', '', metric_value)

                            # 存储到指标表
                            insert_sql = "INSERT INTO report_metrics (report_id, metric_name, metric_value) VALUES (1, %s, %s)"
                            cursor.execute(insert_sql, (metric_name, metric_value))

                            logger.info(f"  ✅ {metric_name}: {metric_value}")

                    except Exception as e:
                        logger.debug(f"行解析失败: {e}")
                        continue

                conn.commit()

            logger.info("🎉 医疗机构统计爬取完成！")

        except Exception as e:
            logger.error(f"❌ 医疗机构爬取失败: {e}")
        finally:
            if conn:
                conn.close()

    def run(self):
        """运行所有爬虫任务"""
        logger.info("=" * 60)
        logger.info("国家卫生健康委员会数据爬虫启动")
        logger.info("=" * 60)

        # 依次运行各个爬虫
        try:
            self.crawl_national_stats()
            logger.info("\n" + "-" * 60 + "\n")
            
            time.sleep(2)
            self.crawl_province_data()
            logger.info("\n" + "-" * 60 + "\n")
            
            time.sleep(2)
            self.crawl_hospital_data()

            logger.info("\n" + "=" * 60)
            logger.info("🎉 全部爬虫任务完成！")
            logger.info("=" * 60)

        except KeyboardInterrupt:
            logger.info("\n⚠️ 用户中断爬虫")
        except Exception as e:
            logger.error(f"\n❌ 爬虫出错: {e}")


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    crawler = NationalHealthCrawler()
    crawler.run()
