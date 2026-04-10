import mysql.connector

# 1. 连接 MySQL
try:
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="rootpassword",
        database="health_db"
    )
    cursor = conn.cursor()

    # 2. 创建表（如果不存在）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS population_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            district VARCHAR(50),
            health_score INT
        )
    """)

    # 3. 插入一些测试数据
    test_data = [
        ('张三', 25, '朝阳区', 88),
        ('李四', 45, '海淀区', 72),
        ('王五', 62, '朝阳区', 65),
        ('赵六', 30, '西城区', 90),
        ('钱七', 70, '海淀区', 55)
    ]

    # 为了让数据多一点，我们循环插几次
    insert_query = "INSERT INTO population_data (name, age, district, health_score) VALUES (%s, %s, %s, %s)"
    cursor.executemany(insert_query, test_data * 8)  # 5条 * 8 = 40条

    conn.commit()
    print(f"✅ 成功插入了 {cursor.rowcount} 条数据到 population_data 表！")

except mysql.connector.Error as err:
    print(f"❌ 出错了: {err}")

finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()