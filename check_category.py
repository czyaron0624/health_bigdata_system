import mysql.connector

conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
cursor = conn.cursor(dictionary=True)

cursor.execute('SELECT id, title, source_category FROM national_news WHERE id >= 64')
results = cursor.fetchall()

print("Demo reports with their categories:")
for r in results:
    print(f"  ID {r['id']}: {r['title'][:40]} | {r['source_category']}")

conn.close()
