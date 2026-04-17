"""
国家卫生健康委员会（nhc.gov.cn）数据爬虫。

当前站点结构已经与早期脚本中的旧栏目 URL 不一致，这里改为抓取
仍然可访问的统计数据入口页，并统一写入 national_news。
"""

import logging
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import mysql.connector
import requests
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class NationalHealthCrawler:
    def __init__(self):
        self.base_url = "https://www.nhc.gov.cn"
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
        self.db_config = {
            "host": "localhost",
            "user": "root",
            "password": "rootpassword",
            "database": "health_db",
        }
        self.source_pages = {
            "统计数据总览": "https://www.nhc.gov.cn/mohwsbwstjxxzx/tjzxtjsj/tjsj_list.shtml",
            "统计法规": "https://www.nhc.gov.cn/mohwsbwstjxxzx/s7965/new_list.shtml",
            "统计与监测": "https://www.nhc.gov.cn/mohwsbwstjxxzx/s7967/new_list.shtml",
        }

    def connect_db(self):
        return mysql.connector.connect(**self.db_config)

    def ensure_table(self, cursor):
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS national_news (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                link VARCHAR(512) NOT NULL,
                source_category VARCHAR(100),
                publish_date VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_link (link)
            )
            """
        )

    def _fetch_page(self, url):
        response = requests.get(url, headers=self.headers, timeout=20, verify=False)
        response.raise_for_status()
        response.encoding = "utf-8"
        return response.text

    def _normalize_date(self, raw_text):
        if not raw_text:
            return None

        raw_text = raw_text.strip()
        patterns = [
            r"(20\d{2})[-/.年]\s*(\d{1,2})[-/.月]\s*(\d{1,2})",
            r"(20\d{2})[-/.](\d{1,2})[-/.](\d{1,2})",
            r"(20\d{2})[-/.年]\s*(\d{1,2})[-/.月]",
            r"(20\d{2})[-/.](\d{1,2})",
        ]

        for pattern in patterns:
            match = re.search(pattern, raw_text)
            if not match:
                continue

            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3)) if len(match.groups()) >= 3 else 1
            try:
                return datetime(year, month, day).strftime("%Y-%m-%d")
            except ValueError:
                return None

        return None

    def _extract_date_from_url(self, url):
        match = re.search(r"/(20\d{2})(\d{2})/", url)
        if not match:
            return None
        try:
            return datetime(int(match.group(1)), int(match.group(2)), 1).strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _is_article_link(self, title, full_url):
        if not title or len(title) < 8:
            return False
        if not full_url.endswith(".shtml"):
            return False
        if "/mohwsbwstjxxzx/" not in full_url:
            return False
        if any(token in full_url for token in ("new_index.shtml", "new_list.shtml", "lists.shtml")):
            return False
        if title in {"主站首页", "首页", "机构设置", "统计数据", "信息标准", "工作动态", "学会与杂志工作", "统计与监测", "公 文"}:
            return False
        return True

    def extract_articles(self, page_url):
        html = self._fetch_page(page_url)
        soup = BeautifulSoup(html, "html.parser")

        articles = []
        seen_links = set()

        for li in soup.find_all("li"):
            a_tag = li.find("a", href=True)
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            full_url = urljoin(self.base_url, a_tag["href"].strip())
            if not self._is_article_link(title, full_url):
                continue

            span_tag = li.find("span")
            raw_date = span_tag.get_text(strip=True) if span_tag else li.get_text(" ", strip=True)
            publish_date = self._normalize_date(raw_date) or self._extract_date_from_url(full_url) or "未知"

            if full_url in seen_links:
                continue
            seen_links.add(full_url)
            articles.append(
                {
                    "title": title,
                    "link": full_url,
                    "publish_date": publish_date,
                }
            )

        if articles:
            return articles

        for a_tag in soup.find_all("a", href=True):
            title = a_tag.get_text(strip=True)
            full_url = urljoin(self.base_url, a_tag["href"].strip())
            if not self._is_article_link(title, full_url):
                continue

            if full_url in seen_links:
                continue
            seen_links.add(full_url)
            articles.append(
                {
                    "title": title,
                    "link": full_url,
                    "publish_date": self._extract_date_from_url(full_url) or "未知",
                }
            )

        return articles

    def save_articles(self, cursor, conn, source_category, articles):
        inserted_count = 0
        updated_count = 0

        for article in articles:
            title = article["title"]
            link = article["link"]
            publish_date = article["publish_date"]

            cursor.execute("SELECT id FROM national_news WHERE link = %s", (link,))
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    """
                    UPDATE national_news
                    SET title = %s, source_category = %s, publish_date = %s
                    WHERE link = %s
                    """,
                    (title, source_category, publish_date, link),
                )
                updated_count += 1
            else:
                cursor.execute(
                    """
                    INSERT INTO national_news (title, link, source_category, publish_date)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (title, link, source_category, publish_date),
                )
                inserted_count += 1

        conn.commit()
        return inserted_count, updated_count

    def crawl_source(self, source_category, page_url):
        logger.info(f"开始抓取 {source_category}: {page_url}")
        articles = self.extract_articles(page_url)
        logger.info(f"{source_category} 提取到 {len(articles)} 条候选记录")

        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            self.ensure_table(cursor)
            conn.commit()

            inserted_count, updated_count = self.save_articles(cursor, conn, source_category, articles)
            logger.info(
                f"{source_category} 完成: 新增 {inserted_count} 条, 更新 {updated_count} 条"
            )
        finally:
            if conn:
                conn.close()

        time.sleep(0.5)
        return len(articles)

    def crawl_national_stats(self):
        return self.crawl_source("国家卫健委-统计与监测", self.source_pages["统计与监测"])

    def crawl_province_data(self):
        return self.crawl_source("国家卫健委-统计法规", self.source_pages["统计法规"])

    def crawl_hospital_data(self):
        return self.crawl_source("国家卫健委-统计数据总览", self.source_pages["统计数据总览"])

    def run(self):
        logger.info("=" * 60)
        logger.info("国家卫生健康委员会数据爬虫启动")
        logger.info("=" * 60)

        try:
            self.crawl_national_stats()
            logger.info("\n" + "-" * 60 + "\n")

            self.crawl_province_data()
            logger.info("\n" + "-" * 60 + "\n")

            self.crawl_hospital_data()
            logger.info("\n" + "=" * 60)
            logger.info("全部爬虫任务完成")
            logger.info("=" * 60)
        except KeyboardInterrupt:
            logger.info("用户中断爬虫")
        except Exception as e:
            logger.error(f"爬虫出错: {e}")


if __name__ == "__main__":
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    crawler = NationalHealthCrawler()
    crawler.run()
