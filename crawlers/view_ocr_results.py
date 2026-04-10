"""
OCR结果查看工具
功能：查询并展示数据库中的OCR识别结果
"""

import mysql.connector
import logging

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
        
        cursor.execute("""
            SELECT id, title, publish_date, link,
                   ocr_content, LENGTH(ocr_content) as content_length
            FROM guangxi_news 
            WHERE ocr_content IS NOT NULL AND ocr_content != ''
            ORDER BY id DESC LIMIT %s
        """, (limit,))
        
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
            print(f"标题: {row['title']}")
            print(f"日期: {row['publish_date']}")
            print(f"OCR长度: {row['content_length']} 字符")
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