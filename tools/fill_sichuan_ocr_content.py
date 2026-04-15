import json

import mysql.connector

DB_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': 'rootpassword',
    'database': 'health_db',
}


def main():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id, detail_context FROM sichuan_news WHERE ocr_content IS NULL OR ocr_content = ''")
        rows = cursor.fetchall()
        updated = 0

        for row in rows:
            detail_context = row.get('detail_context')
            if not detail_context:
                continue

            if isinstance(detail_context, str):
                try:
                    parsed = json.loads(detail_context)
                except Exception:
                    parsed = {'content_text': detail_context}
            else:
                parsed = detail_context

            ocr_text = parsed.get('content_text') or parsed.get('full_text') or ''
            if not ocr_text:
                continue

            cursor.execute(
                'UPDATE sichuan_news SET ocr_content = %s WHERE id = %s',
                (ocr_text, row['id'])
            )
            updated += 1

        conn.commit()
        print(f'updated={updated}')
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    main()
