"""
OCR结果查看工具
功能：查询并展示数据库中的OCR识别结果
"""

import mysql.connector
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def view_ocr_results(limit=10):
    """查看OCR识别结果"""
    try:
        conn = mysql.connector.connect(
            host="localhost", user="root",
            password="rootpassword", database="health_db"
        )
        cursor = conn.cursor(dictionary=True)

        def table_exists(table_name: str) -> bool:
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = %s
                """,
                (table_name,),
            )
            return int(cursor.fetchone()["cnt"]) > 0

        def column_exists(table_name: str, column_name: str) -> bool:
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.columns
                WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s
                """,
                (table_name, column_name),
            )
            return int(cursor.fetchone()["cnt"]) > 0

        queries = []
        if table_exists('guangxi_news'):
            detail_context_expr = 'detail_context' if column_exists('guangxi_news', 'detail_context') else 'NULL AS detail_context'
            queries.append(
                """
                SELECT id, title, publish_date, link,
                       ocr_content, {detail_context_expr}, LENGTH(ocr_content) as content_length,
                       'guangxi_news' AS source_table
                FROM guangxi_news
                WHERE ocr_content IS NOT NULL AND ocr_content != ''
                """.format(detail_context_expr=detail_context_expr)
            )
        if table_exists('sichuan_news'):
            detail_context_expr = 'detail_context' if column_exists('sichuan_news', 'detail_context') else 'NULL AS detail_context'
            queries.append(
                """
                SELECT id, title, publish_date, link,
                       ocr_content, {detail_context_expr}, LENGTH(ocr_content) as content_length,
                       'sichuan_news' AS source_table
                FROM sichuan_news
                WHERE ocr_content IS NOT NULL AND ocr_content != ''
                """.format(detail_context_expr=detail_context_expr)
            )

        if not queries:
            print("\n数据库中暂无OCR识别结果")
            print("请先运行爬虫生成 guangxi_news 或 sichuan_news")
            return
        
        union_sql = "\nUNION ALL\n".join(queries)
        cursor.execute(
            f"""
            SELECT *
            FROM (
                {union_sql}
            ) t
            ORDER BY id DESC
            LIMIT %s
            """,
            (limit,),
        )
        
        results = cursor.fetchall()
        
        if not results:
            print("\n数据库中暂无OCR识别结果")
            print("请先运行: python crawlers/guangxi_health_crawler.py")
            return
        
        print("\n" + "=" * 60)
        print(f"OCR识别结果展示 (共 {len(results)} 条)")
        print("=" * 60)
        
        for idx, row in enumerate(results, 1):
            print(f"\n--- 第{idx}条 ---")
            print(f"来源: {row['source_table']}")
            print(f"标题: {row['title']}")
            print(f"日期: {row['publish_date']}")
            print(f"OCR长度: {row['content_length']} 字符")
            if row.get('detail_context'):
                try:
                    context = json.loads(row['detail_context']) if isinstance(row['detail_context'], str) else row['detail_context']
                    summary = {
                        'report_category': context.get('report_category'),
                        'report_scope': context.get('report_scope'),
                        'source_unit': context.get('source_unit'),
                        'table_count': context.get('table_count'),
                        'image_count': context.get('image_count'),
                        'attachment_count': context.get('attachment_count'),
                    }
                    print(f"上下文摘要: {summary}")
                except Exception:
                    print("上下文摘要: 无法解析")
            print(f"识别内容:")
            
            ocr_text = row['ocr_content']
            if len(ocr_text) > 300:
                print(ocr_text[:300] + "...")
            else:
                print(ocr_text)
        
        print("\n" + "=" * 60)
        conn.close()
        
    except Exception as e:
        logger.error(f"查询失败: {e}")


if __name__ == "__main__":
    view_ocr_results()