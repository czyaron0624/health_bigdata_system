import mysql.connector

db_config = {
    "host": "localhost",
    "user": "root",
    "password": "rootpassword",
    "database": "health_db"
}

conn = mysql.connector.connect(**db_config)
cursor = conn.cursor(dictionary=True)

cursor.execute('SELECT COUNT(*) as cnt FROM report_metrics')
metrics_count = cursor.fetchone()['cnt']

cursor.execute('SELECT COUNT(*) as cnt FROM national_news WHERE source_category LIKE "%demo%"')
demo_reports_count = cursor.fetchone()['cnt']

print(f"Report metrics records: {metrics_count}")
print(f"Demo reports count: {demo_reports_count}")

cursor.close()
conn.close()
