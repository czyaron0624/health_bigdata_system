"""
回填 OCR 结构化指标到 health_ocr_metrics 表
"""

import argparse
import json
import logging
import os
import sys

import mysql.connector

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from app.services.ocr_structurer import parse_structured_metrics


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db",
}


def ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS health_ocr_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            news_id INT NOT NULL,
            title VARCHAR(255) NOT NULL,
            publish_date VARCHAR(50),
            year INT,
            month INT,
            metric_key VARCHAR(64) NOT NULL,
            metric_name VARCHAR(100) NOT NULL,
            metric_value DECIMAL(18, 4),
            metric_raw VARCHAR(64),
            source_table VARCHAR(32) NOT NULL DEFAULT 'guangxi_news',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uk_news_metric (news_id, metric_key),
            KEY idx_year_month (year, month),
            KEY idx_metric_key (metric_key)
        )
        """
    )


def backfill(min_year=2015):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)

    try:
        ensure_table(cursor)
        conn.commit()

        cursor.execute(
            """
            SELECT id, title, publish_date, ocr_content
            FROM guangxi_news
            WHERE ocr_content IS NOT NULL AND ocr_content != ''
            ORDER BY id ASC
            """
        )
        rows = cursor.fetchall()

        logger.info("待处理 OCR 记录: %s", len(rows))
        logger.info("最小年份过滤: %s", min_year)

        skipped_by_year = 0

        for row in rows:
            parsed = parse_structured_metrics(
                title=row["title"],
                publish_date=row.get("publish_date"),
                ocr_text=row["ocr_content"],
            )

            year = parsed["year"]
            month = parsed["month"]
            metrics = parsed["metrics"]

            if year is not None and year < min_year:
                skipped_by_year += 1
                continue

            for metric_key, metric_data in metrics.items():
                insert_sql = """
                    INSERT INTO health_ocr_metrics
                    (news_id, title, publish_date, year, month, metric_key, metric_name, metric_value, metric_raw)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        title = VALUES(title),
                        publish_date = VALUES(publish_date),
                        year = VALUES(year),
                        month = VALUES(month),
                        metric_name = VALUES(metric_name),
                        metric_value = VALUES(metric_value),
                        metric_raw = VALUES(metric_raw)
                """

                cursor.execute(
                    insert_sql,
                    (
                        row["id"],
                        row["title"],
                        row.get("publish_date"),
                        year,
                        month,
                        metric_key,
                        metric_data["metric_name"],
                        metric_data["value"],
                        metric_data["raw"],
                    ),
                )

        conn.commit()

        cursor.execute("SELECT COUNT(*) AS cnt FROM health_ocr_metrics")
        total_metrics = cursor.fetchone()["cnt"]

        cursor.execute(
            """
            SELECT metric_key, COUNT(*) AS cnt
            FROM health_ocr_metrics
            GROUP BY metric_key
            ORDER BY cnt DESC
            """
        )
        summary = cursor.fetchall()

        logger.info("回填完成，总指标记录: %s", total_metrics)
        logger.info("年份过滤跳过记录: %s", skipped_by_year)
        logger.info("指标分布: %s", json.dumps(summary, ensure_ascii=False))

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="回填 OCR 结构化指标")
    parser.add_argument('--min-year', type=int, default=2015, help='仅回填该年份及之后的数据')
    args = parser.parse_args()

    backfill(min_year=args.min_year)
