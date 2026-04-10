"""
广西卫健委数据爬虫（支持OCR）
功能：爬取广西卫健委公报，自动识别图片中的文字内容
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import mysql.connector
import logging
import argparse
import re
from datetime import datetime

# 导入OCR工具
try:
    from ocr_utils import get_ocr_processor
except ImportError:
    from crawlers.ocr_utils import get_ocr_processor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GuangxiHealthCrawler:
    def __init__(self):
        self.base_url = "https://wsjkw.gxzf.gov.cn/xxgk_49493/fdzdgk/tjxx/sjfb/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        self.ocr = None  # 延迟初始化

    def _normalize_date(self, raw_text):
        """将日期文本标准化为 YYYY-MM-DD"""
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

    def _collect_list_page_urls(self):
        """收集统计数据列表分页URL"""
        response = requests.get(self.base_url, headers=self.headers, timeout=15, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        page_urls = {self.base_url, urljoin(self.base_url, 'index.shtml')}

        # 优先从页面文本中提取总页数并生成分页URL
        full_text = soup.get_text(' ', strip=True)
        total_page_match = re.search(r'共\s*\d+\s*条\s*[，,]\s*(\d+)\s*页', full_text)
        if total_page_match:
            total_pages = int(total_page_match.group(1))
            for page_idx in range(1, total_pages):
                page_urls.add(urljoin(self.base_url, f'index_{page_idx}.shtml'))

        # 再兜底采集页面里已有的分页链接
        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href', '').strip()
            if not href:
                continue

            if 'index' in href:
                full_url = urljoin(self.base_url, href)
                if '/sjfb/' in full_url:
                    page_urls.add(full_url)

        # 主动探测分页（页面中未必直接暴露所有分页链接）
        for page_idx in range(1, 30):
            probe_url = urljoin(self.base_url, f'index_{page_idx}.shtml')
            try:
                probe_resp = requests.get(probe_url, headers=self.headers, timeout=10, verify=False)
                if probe_resp.status_code == 404:
                    break
                if probe_resp.status_code == 200:
                    page_urls.add(probe_url)
            except Exception:
                break

        def page_sort_key(url):
            if 'index.shtml' in url or url.rstrip('/').endswith('sjfb'):
                return 0

            match = re.search(r'index_(\d+)\.shtml', url)
            if match:
                return int(match.group(1)) + 1

            return 9999

        return sorted(page_urls, key=page_sort_key)

    def _extract_items_from_page(self, list_page_url):
        """提取单个列表页的新闻条目"""
        response = requests.get(list_page_url, headers=self.headers, timeout=15, verify=False)
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')

        items = soup.select('div.list ul li') or soup.find_all('li')
        parsed_items = []

        for li in items:
            a_tag = li.find('a')
            if not a_tag:
                continue

            title = a_tag.get_text(strip=True)
            relative_href = a_tag.get('href', '').strip()

            if not relative_href or 'index' in relative_href or len(title) < 8:
                continue

            full_url = urljoin(list_page_url, relative_href)
            if '/sjfb/t' not in full_url or not full_url.endswith('.shtml'):
                continue

            raw_date = ''
            span_tag = li.find('span')
            if span_tag:
                raw_date = span_tag.get_text(strip=True)
            else:
                raw_date = li.get_text(' ', strip=True)

            normalized_date = self._normalize_date(raw_date)

            if not normalized_date:
                title_match = re.search(r'(20\d{2})年\s*(\d{1,2})月', title)
                if title_match:
                    year = int(title_match.group(1))
                    month = int(title_match.group(2))
                    try:
                        normalized_date = datetime(year, month, 1).strftime('%Y-%m-%d')
                    except ValueError:
                        normalized_date = None

            parsed_items.append({
                'title': title,
                'link': full_url,
                'publish_date': normalized_date or '未知',
                'publish_year': int(normalized_date[:4]) if normalized_date else None,
            })

        return parsed_items
    
    def init_ocr(self):
        """初始化OCR（可选，按需启用）"""
        if self.ocr is None:
            logger.info("🔧 正在初始化OCR引擎...")
            self.ocr = get_ocr_processor()
            logger.info("✅ OCR引擎就绪")

    def _is_decorative_image(self, image_url):
        """过滤站点装饰图标，保留正文图片"""
        lowered = image_url.lower()
        excluded_tokens = (
            'logo', 'icon-gh', 'un-collect', 'dzjg', 'beian',
            'conac', 'wx', 'share', 'print'
        )
        return (
            lowered.endswith(('.gif', '.ico', '.svg', '.html', '.shtml', '.jsp', '.php'))
            or any(token in lowered for token in excluded_tokens)
            or lowered.startswith('javascript:')
        )

    def _extract_image_urls_from_node(self, node, detail_url):
        """从指定节点中提取图片地址"""
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
        """
        从详情页提取图片URL
        :param detail_url: 详情页链接
        :return: 图片URL列表
        """
        try:
            response = requests.get(detail_url, headers=self.headers, timeout=15, verify=False)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            images = []

            # 广西站点详情页常见正文容器，按命中优先级依次提取
            content_selectors = [
                'div.trs_editor_view',
                'div.TRS_UEDITOR',
                'div.trs_paper_default',
                'div.article-con',
                'div.TRS_Editor',
                'div.content',
                'div.article-content',
                'div.xxgk_content',
                'div#zoom',
                'article',
            ]

            for selector in content_selectors:
                content_node = soup.select_one(selector)
                if not content_node:
                    continue

                images = self._extract_image_urls_from_node(content_node, detail_url)
                if images:
                    logger.info(f"   命中文本容器 {selector}，找到 {len(images)} 张图片")
                    return images

            # 兜底：全文搜索图片，避免站点结构变动时完全抓不到
            images = self._extract_image_urls_from_node(soup, detail_url)

            if images:
                logger.info(f"   正文容器未命中，页面级兜底找到 {len(images)} 张图片")
            else:
                logger.info("   未找到图片")

            return images
        
        except Exception as e:
            logger.error(f"   提取图片失败: {e}")
            return []
    
    def process_image_with_ocr(self, image_url):
        """
        对图片进行OCR识别
        :param image_url: 图片链接
        :return: 识别出的文字
        """
        self.init_ocr()  # 确保OCR已初始化
        
        logger.info(f"   🔍 正在OCR识别: {image_url[:60]}...")
        text = self.ocr.recognize_to_text(image_url, self.headers)
        
        if text:
            logger.info(f"   ✅ 识别成功，提取 {len(text)} 字符")
        else:
            logger.warning(f"   ⚠️ 未识别到文字")
        
        return text
    
    def crawl_with_ocr(self, enable_ocr=True, min_year=2015):
        """
        主爬虫函数（支持OCR）
        :param enable_ocr: 是否启用OCR识别
        """
        conn = None
        try:
            # 连接数据库
            conn = mysql.connector.connect(
                host="localhost", user="root", 
                password="rootpassword", database="health_db"
            )
            cursor = conn.cursor()
            
            logger.info("🚀 准备开始采集广西卫健委数据...")
            page_urls = self._collect_list_page_urls()
            logger.info(f"📄 发现列表页 {len(page_urls)} 个")
            
            inserted_count = 0
            ocr_count = 0
            skipped_count = 0
            year_filtered_count = 0
            seen_links = set()
            
            for page_idx, page_url in enumerate(page_urls, 1):
                logger.info(f"📚 正在处理列表页 {page_idx}/{len(page_urls)}: {page_url}")
                try:
                    page_items = self._extract_items_from_page(page_url)
                except Exception as e:
                    logger.warning(f"⚠️ 列表页解析失败，跳过: {e}")
                    continue

                for item in page_items:
                    title = item['title']
                    full_url = item['link']
                    date = item['publish_date']
                    publish_year = item['publish_year']

                    if full_url in seen_links:
                        skipped_count += 1
                        continue
                    seen_links.add(full_url)

                    if publish_year is not None and publish_year < min_year:
                        year_filtered_count += 1
                        continue
                
                    # OCR处理（如果启用）
                    ocr_content = ""
                    if enable_ocr:
                        try:
                            # 提取详情页图片
                            images = self.extract_images_from_detail(full_url)

                            # 对每张图片进行OCR（最多处理前5张）
                            image_texts = []
                            for img_url in images[:5]:
                                text = self.process_image_with_ocr(img_url)
                                if text:
                                    image_texts.append(text)
                                time.sleep(0.5)  # 图片间减速

                            if image_texts:
                                ocr_content = "\n---\n".join(image_texts)
                                ocr_count += 1
                        except Exception as e:
                            logger.warning(f"   ⚠️ OCR处理异常: {e}")
                
                    # 保存到数据库
                    try:
                        sql = """INSERT INTO guangxi_news 
                                (title, link, publish_date, ocr_content) 
                                VALUES (%s, %s, %s, %s)"""
                        cursor.execute(sql, (title, full_url, date, ocr_content))
                        conn.commit()

                        if ocr_content:
                            logger.info(f"✅ 已保存（含OCR）: {title}")
                        else:
                            logger.info(f"✅ 已保存: {title}")

                        inserted_count += 1

                    except mysql.connector.Error as e:
                        if "Duplicate entry" in str(e):
                            update_sql = """
                                UPDATE guangxi_news
                                SET title = %s,
                                    publish_date = %s,
                                    ocr_content = CASE
                                        WHEN %s IS NOT NULL AND %s != '' THEN %s
                                        ELSE ocr_content
                                    END
                                WHERE link = %s
                            """
                            cursor.execute(update_sql, (title, date, ocr_content, ocr_content, ocr_content, full_url))
                            conn.commit()

                            if ocr_content:
                                logger.info(f"✅ 已更新（含OCR）: {title}")
                            else:
                                logger.info(f"✅ 已更新: {title}")

                            inserted_count += 1
                        else:
                            logger.error(f"❌ 数据库错误: {e}")

                    # 减速带：禁用OCR时可加速
                    time.sleep(1 if enable_ocr else 0.1)
            
            # 统计结果
            logger.info("\n" + "=" * 50)
            logger.info(f"🎉 爬取完成！")
            logger.info(f"   📊 新增数据: {inserted_count} 条")
            logger.info(f"   🔍 OCR识别: {ocr_count} 条")
            logger.info(f"   ⏭️ 跳过无效: {skipped_count} 条")
            logger.info(f"   📅 年份过滤(<{min_year}): {year_filtered_count} 条")
            logger.info("=" * 50)
        
        except Exception as e:
            logger.error(f"❌ 运行报错: {e}")
        finally:
            if conn:
                conn.close()


def slow_crawl_to_mysql():
    """向后兼容的入口函数"""
    crawler = GuangxiHealthCrawler()
    crawler.crawl_with_ocr(enable_ocr=True)


if __name__ == "__main__":
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(description="广西卫健委数据爬虫（支持OCR）")
    parser.add_argument('--disable-ocr', action='store_true', help='禁用OCR识别')
    parser.add_argument('--min-year', type=int, default=2015, help='仅采集该年份及之后的数据')
    args = parser.parse_args()

    crawler = GuangxiHealthCrawler()
    crawler.crawl_with_ocr(enable_ocr=not args.disable_ocr, min_year=args.min_year)
