"""查看 national_news 的分类字段分布与样例。"""

import argparse
import sys

import mysql.connector


def main() -> int:
    parser = argparse.ArgumentParser(description="检查国家新闻分类字段")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="rootpassword")
    parser.add_argument("--database", default="health_db")
    parser.add_argument("--min-id", type=int, default=1, help="最小ID")
    parser.add_argument("--limit", type=int, default=30, help="最多展示条数")
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
        cursor.execute(
            """
            SELECT id, title, source_category
            FROM national_news
            WHERE id >= %s
            ORDER BY id DESC
            LIMIT %s
            """,
            (args.min_id, args.limit),
        )
        rows = cursor.fetchall()

        print(f"查询到 {len(rows)} 条记录")
        for row in rows:
            title = (row.get("title") or "")[:50]
            category = row.get("source_category") or "--"
            print(f"ID {row.get('id')}: {title} | {category}")

        return 0
    except mysql.connector.Error as exc:
        print(f"查询失败: {exc}")
        return 1
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    sys.exit(main())