"""详情页上下文提取工具。"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


DEFAULT_CONTENT_SELECTORS = [
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
    'main',
]

DEFAULT_TITLE_SELECTORS = [
    'h1',
    'h2',
    'div.article-title',
    'div.title',
    'div.TRS_Editor > h1',
    'div.tit',
]

DEFAULT_BREADCRUMB_HINTS = ('您的位置', '当前位置', '首页', '＞', '>')

DEFAULT_ATTACHMENT_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.csv', '.zip', '.rar', '.txt')


def _normalize_text(text: Optional[str]) -> str:
    if not text:
        return ''
    text = text.replace('\r', '\n').replace('\u3000', ' ')
    text = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{2,}', '\n', text)
    return text.strip()


def _unique_items(items: List[Any]) -> List[Any]:
    deduped = []
    seen = set()
    for item in items:
        key = repr(item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _extract_text_lines(node) -> List[str]:
    lines: List[str] = []
    for raw_line in node.get_text('\n', strip=True).split('\n'):
        line = _normalize_text(raw_line)
        if not line:
            continue
        if line not in lines:
            lines.append(line)
    return lines


def _extract_table_rows(table) -> List[List[str]]:
    rows: List[List[str]] = []
    for tr in table.find_all('tr'):
        cells = []
        for cell in tr.find_all(['th', 'td']):
            text = _normalize_text(cell.get_text(' ', strip=True))
            if text:
                cells.append(text)
        if cells:
            rows.append(cells)
    return rows


def _extract_tables(node) -> List[Dict[str, Any]]:
    tables: List[Dict[str, Any]] = []
    for table in node.find_all('table'):
        rows = _extract_table_rows(table)
        if not rows:
            continue

        caption = ''
        if table.caption:
            caption = _normalize_text(table.caption.get_text(' ', strip=True))

        tables.append({
            'caption': caption,
            'rows': rows,
            'text': '\n'.join(' | '.join(row) for row in rows),
        })

    return tables


def _extract_images(node, detail_url: str) -> List[str]:
    image_urls: List[str] = []
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
        lowered = full_url.lower()
        if lowered.endswith(('.gif', '.ico', '.svg', '.html', '.shtml', '.jsp', '.php')):
            continue
        if any(token in lowered for token in ('logo', 'icon', 'beian', 'conac', 'share', 'print', 'wx', 'scs_fxicon')):
            continue

        if full_url not in image_urls:
            image_urls.append(full_url)

    return image_urls


def _extract_attachments(node, detail_url: str) -> List[Dict[str, str]]:
    attachments: List[Dict[str, str]] = []
    for link in node.find_all('a', href=True):
        href = (link.get('href') or '').strip()
        text = _normalize_text(link.get_text(' ', strip=True))
        if not href:
            continue

        full_url = urljoin(detail_url, href)
        lowered = full_url.lower()
        if lowered.endswith(DEFAULT_ATTACHMENT_EXTENSIONS) or any(keyword in text for keyword in ('附件', '下载', '表格', 'Excel')):
            attachments.append({'text': text, 'url': full_url})

    return _unique_items(attachments)


def _find_first_text(soup: BeautifulSoup, selectors: List[str]) -> str:
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        text = _normalize_text(node.get_text(' ', strip=True))
        if text:
            return text
    return ''


def _extract_meta_lines(soup: BeautifulSoup) -> List[str]:
    meta_lines: List[str] = []
    page_lines = [
        _normalize_text(line)
        for line in soup.get_text('\n', strip=True).split('\n')
    ]
    for line in page_lines:
        if not line:
            continue
        if any(keyword in line for keyword in ('发布日期', '来源', '阅读量', '作者', '责任编辑', '发布时间', '时间')):
            if line not in meta_lines:
                meta_lines.append(line)
    return meta_lines


def _extract_breadcrumbs(soup: BeautifulSoup) -> List[str]:
    breadcrumb_nodes = []
    for selector in ('div.location', 'div.dqwz', 'div.crumb', 'div.breadcrumb', 'div.crumbs', 'div.pos', 'div.position'):
        node = soup.select_one(selector)
        if node:
            breadcrumb_nodes.append(_normalize_text(node.get_text(' ', strip=True)))

    for line in [
        _normalize_text(line)
        for line in soup.get_text('\n', strip=True).split('\n')
    ]:
        if any(hint in line for hint in DEFAULT_BREADCRUMB_HINTS):
            breadcrumb_nodes.append(line)

    return [item for item in _unique_items(breadcrumb_nodes) if item]


def extract_detail_context(detail_url: str, headers: Dict[str, str], content_selectors: Optional[List[str]] = None) -> Dict[str, Any]:
    response = requests.get(detail_url, headers=headers, timeout=15, verify=False)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')

    content_selectors = content_selectors or DEFAULT_CONTENT_SELECTORS
    content_node = None
    for selector in content_selectors:
        node = soup.select_one(selector)
        if node:
            content_node = node
            break

    if content_node is None:
        content_node = soup

    title = _find_first_text(soup, DEFAULT_TITLE_SELECTORS)
    if not title and soup.title:
        title = _normalize_text(soup.title.get_text(' ', strip=True))

    paragraphs = _extract_text_lines(content_node)
    tables = _extract_tables(content_node)
    images = _extract_images(content_node, detail_url)
    attachments = _extract_attachments(content_node, detail_url)
    breadcrumbs = _extract_breadcrumbs(soup)
    meta_lines = _extract_meta_lines(soup)

    content_text_parts = paragraphs + [table['text'] for table in tables if table.get('text')]
    content_text = '\n'.join(part for part in content_text_parts if part)

    full_text_parts = [title] if title else []
    full_text_parts.extend(breadcrumbs)
    full_text_parts.extend(meta_lines)
    full_text_parts.extend(content_text_parts)
    full_text_parts.extend([attachment['text'] for attachment in attachments if attachment.get('text')])

    return {
        'url': detail_url,
        'title': title,
        'breadcrumbs': breadcrumbs,
        'meta_lines': meta_lines,
        'paragraphs': paragraphs,
        'tables': tables,
        'images': images,
        'attachments': attachments,
        'content_text': content_text,
        'full_text': '\n'.join(part for part in full_text_parts if part),
        'image_count': len(images),
        'table_count': len(tables),
        'attachment_count': len(attachments),
    }