"""校验核心数据表记录数量。"""

import argparse
import sys

import mysql.connector


def table_exists(cursor, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = %s
        """,
        (table_name,),
    )
    return int(cursor.fetchone()["cnt"]) > 0


def main() -> int:
    parser = argparse.ArgumentParser(description="验证数据库核心数据")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="rootpassword")
    parser.add_argument("--database", default="health_db")
    args = parser.parse_args()

    try:
        conn = mysql.connector.connect(
            host=args.host,
            user=args.user,
            password=args.password,
            database=args.database,
        )
    except mysql.connector.Error as exc:
        print(f"数据库连接失败: {exc}")
        return 1

    cursor = conn.cursor(dictionary=True)
    try:
        # 当前结构化指标表
        metrics_count = -1
        if table_exists(cursor, "health_ocr_metrics"):
            cursor.execute("SELECT COUNT(*) AS cnt FROM health_ocr_metrics")
            metrics_count = int(cursor.fetchone()["cnt"])

        # 新增统计年报条目
        tjnb_count = -1
        if table_exists(cursor, "guangxi_news"):
            cursor.execute("SELECT COUNT(*) AS cnt FROM guangxi_news WHERE link LIKE '%/tjnb/%'")
            tjnb_count = int(cursor.fetchone()["cnt"])

        # 演示类国家数据（若有source_category列）
        demo_reports_count = -1
        if table_exists(cursor, "national_news"):
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM national_news
                WHERE source_category LIKE '%demo%'
                   OR source_category LIKE '%演示%'
                """
            )
            demo_reports_count = int(cursor.fetchone()["cnt"])

        print(f"health_ocr_metrics records: {metrics_count}")
        print(f"guangxi tjnb records: {tjnb_count}")
        print(f"national demo reports: {demo_reports_count}")

        if metrics_count < 0:
            print("警告: health_ocr_metrics 表不存在")
            return 1
        return 0
    except mysql.connector.Error as exc:
        print(f"查询失败: {exc}")
        return 1
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
