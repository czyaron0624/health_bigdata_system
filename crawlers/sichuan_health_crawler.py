# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""
四川省卫生健康委员会数据爬虫（支持OCR）
功能：爬取四川省卫健委统计信息栏目，自动识别图片中的文字内容
"""

import argparse
import json
import logging
import re
import time
from datetime import datetime
from urllib.parse import urljoin, urlparse

import mysql.connector
import requests  # pyright: ignore[reportMissingImports]
from bs4 import BeautifulSoup  # pyright: ignore[reportMissingImports]

try:
    from ocr_utils import get_ocr_processor
except ImportError:
    from crawlers.ocr_utils import get_ocr_processor

try:
    from detail_context import extract_detail_context as build_detail_context
except ImportError:
    from crawlers.detail_context import extract_detail_context as build_detail_context


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SichuanHealthCrawler:
    def __init__(self, sections=None):
        self.section_configs = {
            'ylfw': {
                'name': '医疗服务',
                'base_url': 'https://wsjkw.sc.gov.cn/scwsjkw/ylfw/tygl.shtml',
                'link_hint': '/ylfw/',
            },
            'njgb': {
                'name': '年鉴公报',
                'base_url': 'https://wsjkw.sc.gov.cn/scwsjkw/njgb/tygl.shtml',
                'link_hint': '/njgb/',
            },
            'wszy': {
                'name': '数据下载',
                'base_url': 'https://wsjkw.sc.gov.cn/scwsjkw/wszy/tygl.shtml',
                'link_hint': '/wszy/',
            },
        }

        selected = sections or ['ylfw']
        self.sections = [key for key in selected if key in self.section_configs]
        if not self.sections:
            self.sections = ['ylfw']

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.ocr = None

    def extract_detail_context(self, detail_url):
        return build_detail_context(detail_url, self.headers)

    def connect_db(self):
        return mysql.connector.connect(
            host='localhost',
            user='root',
            password='rootpassword',
            database='health_db',
        )

    def ensure_table(self, cursor):
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS sichuan_news (
                id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                link VARCHAR(512) NOT NULL,
                publish_date VARCHAR(50),
                source_category VARCHAR(100) DEFAULT '四川省卫生健康委员会',
                ocr_content LONGTEXT,
                detail_context LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uk_link (link)
            )
            """
        )

    def _normalize_date(self, raw_text):
        if not raw_text:
            return None

        raw_text = raw_text.strip()
        date_patterns = [
            r'(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})',
            r'(20\d{2})[-/.](\d{1,2})',
        ]

        for pattern in date_patterns:
            match = re.search(pattern, raw_text)
            if not match:
                continue

            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3)) if len(match.groups()) >= 3 else 1

            try:
                return datetime(year, month, day).strftime('%Y-%m-%d')
            except ValueError:
                return None

        return None

    def _extract_report_year_from_title(self, title):
        if not title:
            return None

        match = re.search(r'(20\d{2})\s*年', title)
        if not match:
            return None

        year = int(match.group(1))
        if 2000 <= year <= 2099:
            return year
        return None

    def _extract_date_from_url(self, detail_url):
        match = re.search(r'/(20\d{2})/(\d{1,2})/(\d{1,2})/', detail_url)
        if not match:
            return None

        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).strftime('%Y-%m-%d')
        except ValueError:
            return None

    def _build_page_url(self, base_url, page_index):
        if page_index <= 1:
            return base_url
        if base_url.endswith('.shtml'):
            return base_url[:-6] + f'_{page_index}.shtml'
        return urljoin(base_url, f'index_{page_index}.shtml')

    def _collect_list_page_urls(self, base_url):
        response = requests.get(base_url, headers=self.headers, timeout=15, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        base_parsed = urlparse(base_url)
        base_host = base_parsed.netloc
        base_path_prefix = base_parsed.path.rsplit('/', 1)[0] + '/'

        page_urls = {base_url}

        full_text = soup.get_text(' ', strip=True)
        total_page_match = re.search(r'共\s*(\d+)\s*页', full_text)
        if total_page_match:
            total_pages = int(total_page_match.group(1))
            for page_idx in range(2, total_pages + 1):
                page_urls.add(self._build_page_url(base_url, page_idx))

        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '').strip()
            if not href:
                continue

            if 'tygl' not in href and 'index' not in href:
                continue

            full_url = urljoin(base_url, href)
            parsed_url = urlparse(full_url)
            if parsed_url.netloc == base_host and parsed_url.path.startswith(base_path_prefix):
                page_urls.add(full_url)

        if len(page_urls) == 1:
            for page_idx in range(2, 10):
                page_urls.add(self._build_page_url(base_url, page_idx))

        def page_sort_key(url):
            if url == base_url:
                return 0

            match = re.search(r'_(\d+)\.shtml$', url)
            if match:
                return int(match.group(1))

            return 9999

        return sorted(page_urls, key=page_sort_key)

    def _extract_items_from_page(self, list_page_url, section_key):
        response = requests.get(list_page_url, headers=self.headers, timeout=15, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        items = soup.select('div.list li') or soup.find_all('li')
        parsed_items = []
        link_hint = self.section_configs[section_key]['link_hint']

        for li in items:
            a_tag = li.find('a')
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            relative_href = a_tag.get('href', '').strip()
            if not relative_href or len(title) < 8:
                continue

            full_url = urljoin(list_page_url, relative_href)
            if link_hint not in full_url or not full_url.endswith('.shtml'):
                continue
            if any(token in full_url for token in ('tygl', 'index', 'list')):
                continue

            raw_date = ''
            span_tag = li.find('span')
            if span_tag:
                raw_date = span_tag.get_text(strip=True)
            else:
                raw_date = li.get_text(' ', strip=True)

            normalized_date = self._normalize_date(raw_date) or self._extract_date_from_url(full_url)
            report_year = self._extract_report_year_from_title(title)

            parsed_items.append({
                'title': title,
                'link': full_url,
                'publish_date': normalized_date or '未知',
                'publish_year': int(normalized_date[:4]) if normalized_date else report_year,
                'report_year': report_year,
            })

        return parsed_items

    def init_ocr(self):
        if self.ocr is None:
            logger.info('🔧 正在初始化OCR引擎...')
            self.ocr = get_ocr_processor()
            logger.info('✅ OCR引擎就绪')

    def _is_decorative_image(self, image_url):
        lowered = image_url.lower()
        excluded_tokens = (
            'logo', 'icon', 'un-collect', 'dzjg', 'beian',
            'conac', 'wx', 'share', 'print', 'scs_fxicon'
        )
        return (
            lowered.endswith(('.gif', '.ico', '.svg', '.html', '.shtml', '.jsp', '.php'))
            or any(token in lowered for token in excluded_tokens)
            or lowered.startswith('javascript:')
        )

    def _extract_image_urls_from_node(self, node, detail_url):
        image_urls = []

        for img in node.find_all('img'):
            src = (
                img.get('src')
                or img.get('data-src')
                or img.get('data-original')
                or img.get('data-lazy-src')
                or ''
            ).strip()

            if not src:
                srcset = (img.get('srcset') or '').strip()
                if srcset:
                    src = srcset.split(',')[0].strip().split(' ')[0]

            if not src:
                continue

            full_url = urljoin(detail_url, src)
            if self._is_decorative_image(full_url):
                continue

            if full_url not in image_urls:
                image_urls.append(full_url)

        return image_urls

    def extract_images_from_detail(self, detail_url):
        try:
            detail_context = self.extract_detail_context(detail_url)
            images = detail_context.get('images', [])
            if images:
                logger.info(f'   提取到 {len(images)} 张图片')
            else:
                logger.info('   未找到图片')
            return images

        except Exception as e:
            logger.error(f'   提取图片失败: {e}')
            return []

    def process_image_with_ocr(self, image_url):
        self.init_ocr()

        logger.info(f'   🔍 正在OCR识别: {image_url[:60]}...')
        text = self.ocr.recognize_to_text(image_url, self.headers)

        if text:
            logger.info(f'   ✅ 识别成功，提取 {len(text)} 字符')
        else:
            logger.warning('   ⚠️ 未识别到文字')

        return text

    def crawl_with_ocr(self, enable_ocr=True, min_year=2015, year_filter_source='title'):
        conn = None
        try:
            conn = self.connect_db()
            cursor = conn.cursor()
            self.ensure_table(cursor)
            conn.commit()

            logger.info('🚀 准备开始采集四川省卫健委数据...')
            logger.info(f"📁 采集栏目: {', '.join(self.sections)}")

            inserted_count = 0
            ocr_count = 0
            skipped_count = 0
            year_filtered_count = 0
            seen_links = set()

            for section_key in self.sections:
                section_cfg = self.section_configs[section_key]
                page_urls = self._collect_list_page_urls(section_cfg['base_url'])
                logger.info(f"📄 [{section_key}] 发现列表页 {len(page_urls)} 个")

                for page_idx, page_url in enumerate(page_urls, 1):
                    logger.info(f"📚 [{section_key}] 正在处理列表页 {page_idx}/{len(page_urls)}: {page_url}")
                    try:
                        page_items = self._extract_items_from_page(page_url, section_key)
                    except Exception as e:
                        logger.warning(f"⚠️ [{section_key}] 列表页解析失败，跳过: {e}")
                        continue

                    for item in page_items:
                        title = item['title']
                        full_url = item['link']
                        date = item['publish_date']
                        publish_year = item['publish_year']
                        report_year = item.get('report_year')

                        if full_url in seen_links:
                            skipped_count += 1
                            continue
                        seen_links.add(full_url)

                        filter_year = report_year if year_filter_source == 'title' else publish_year
                        if filter_year is not None and filter_year < min_year:
                            year_filtered_count += 1
                            continue

                        detail_context = None
                        ocr_content = ''
                        if enable_ocr:
                            try:
                                detail_context = self.extract_detail_context(full_url)
                                images = detail_context.get('images', [])
                                image_texts = []

                                for img_url in images[:5]:
                                    text = self.process_image_with_ocr(img_url)
                                    if text:
                                        image_texts.append(text)
                                    time.sleep(0.5)

                                if image_texts:
                                    ocr_content = '\n---\n'.join(image_texts)
                                    ocr_count += 1
                            except Exception as e:
                                logger.warning(f'   ⚠️ OCR处理异常: {e}')
                        if detail_context is None:
                            try:
                                detail_context = self.extract_detail_context(full_url)
                            except Exception as e:
                                logger.warning(f'   ⚠️ 上下文提取异常: {e}')

                        source_category = f"四川省卫健委-{section_cfg['name']}"
                        try:
                            sql = """
                                INSERT INTO sichuan_news
                                (title, link, publish_date, source_category, ocr_content, detail_context)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """
                            cursor.execute(sql, (title, full_url, date, source_category, ocr_content, json.dumps(detail_context, ensure_ascii=False) if detail_context else None))
                            conn.commit()

                            if ocr_content:
                                logger.info(f"✅ [{section_key}] 已保存（含OCR）: {title}")
                            else:
                                logger.info(f"✅ [{section_key}] 已保存: {title}")

                            inserted_count += 1

                        except mysql.connector.Error as e:
                            if 'Duplicate entry' in str(e):
                                update_sql = """
                                    UPDATE sichuan_news
                                    SET title = %s,
                                        publish_date = %s,
                                        source_category = %s,
                                        detail_context = CASE
                                            WHEN %s IS NOT NULL AND %s != '' THEN %s
                                            ELSE detail_context
                                        END,
                                        ocr_content = CASE
                                            WHEN %s IS NOT NULL AND %s != '' THEN %s
                                            ELSE ocr_content
                                        END
                                    WHERE link = %s
                                """
                                cursor.execute(
                                    update_sql,
                                    (
                                        title,
                                        date,
                                        source_category,
                                        json.dumps(detail_context, ensure_ascii=False) if detail_context else None,
                                        json.dumps(detail_context, ensure_ascii=False) if detail_context else None,
                                        json.dumps(detail_context, ensure_ascii=False) if detail_context else None,
                                        ocr_content,
                                        ocr_content,
                                        ocr_content,
                                        full_url,
                                    ),
                                )
                                conn.commit()

                                if ocr_content:
                                    logger.info(f"✅ [{section_key}] 已更新（含OCR）: {title}")
                                else:
                                    logger.info(f"✅ [{section_key}] 已更新: {title}")

                                inserted_count += 1
                            else:
                                logger.error(f'❌ 数据库错误: {e}')

                        time.sleep(1 if enable_ocr else 0.1)

            logger.info('\n' + '=' * 50)
            logger.info('🎉 爬取完成！')
            logger.info(f'   📊 新增数据: {inserted_count} 条')
            logger.info(f'   🔍 OCR识别: {ocr_count} 条')
            logger.info(f'   ⏭️ 跳过无效: {skipped_count} 条')
            logger.info(f'   📅 年份过滤(<{min_year}, 来源={year_filter_source}): {year_filtered_count} 条')
            logger.info('=' * 50)

        except Exception as e:
            logger.error(f'❌ 运行报错: {e}')
        finally:
            if conn:
                conn.close()


def slow_crawl_to_mysql():
    crawler = SichuanHealthCrawler()
    crawler.crawl_with_ocr(enable_ocr=True)


if __name__ == '__main__':
    import urllib3  # pyright: ignore[reportMissingImports]

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(description='四川省卫健委数据爬虫（支持OCR）')
    parser.add_argument('--disable-ocr', action='store_true', help='禁用OCR识别')
    parser.add_argument('--min-year', type=int, default=2015, help='仅采集该年份及之后的数据')
    parser.add_argument('--sections', default='ylfw', help='采集栏目，逗号分隔：ylfw,njgb,wszy')
    parser.add_argument('--year-filter-source', choices=['title', 'publish'], default='title', help='年份过滤来源：title(标题年份) 或 publish(发布日期年份)')
    args = parser.parse_args()

    selected_sections = [part.strip().lower() for part in (args.sections or '').split(',') if part.strip()]
    crawler = SichuanHealthCrawler(sections=selected_sections)
    crawler.crawl_with_ocr(
        enable_ocr=not args.disable_ocr,
        min_year=args.min_year,
        year_filter_source=args.year_filter_source,
    )