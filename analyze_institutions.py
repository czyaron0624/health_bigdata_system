# -*- coding: utf-8 -*-
import mysql.connector

conn = mysql.connector.connect(host='localhost', user='root', password='rootpassword', database='health_db')
cursor = conn.cursor()

cursor.execute('SELECT COUNT(*) FROM medical_institution')
total = cursor.fetchone()[0]
print(f'总机构数: {total}')

cursor.execute("SELECT COUNT(*) FROM medical_institution WHERE region LIKE '%成都%'")
cd = cursor.fetchone()[0]
print(f'成都市机构数: {cd}')

cursor.execute("SELECT region, COUNT(*) as cnt FROM medical_institution WHERE region IS NOT NULL AND region!='' GROUP BY region ORDER BY cnt DESC LIMIT 15")
print('\n各地区机构数量:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

cursor.execute("SELECT level, COUNT(*) as cnt FROM medical_institution WHERE level IS NOT NULL AND level!='' GROUP BY level ORDER BY cnt DESC")
print('\n医院等级分布:')
for row in cursor.fetchall():
    print(f'  {row[0]}: {row[1]}')

cursor.execute("SELECT region, level, COUNT(*) as cnt FROM medical_institution WHERE region LIKE '%成都%' GROUP BY region, level ORDER BY cnt DESC LIMIT 30")
print('\n成都市机构按区域和等级分布:')
for row in cursor.fetchall():
    print(f'  {row[0]} | {row[1]}: {row[2]}')

conn.close()