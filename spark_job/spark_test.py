from pyspark.sql import SparkSession

# 1. 初始化 Spark 会话
# .config 里的那行代码会自动下载 MySQL 驱动，版本 8.0.33 是比较稳的
spark = SparkSession.builder \
    .appName("HealthDataAnalysis") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.0.33") \
    .getOrCreate()
print("==== Spark 会话已建立，准备连接 MySQL ====")

# 2. 配置 MySQL 连接参数
# 注意：因为你在 Windows 运行这个脚本，Docker 映射了 3306 到 localhost
jdbc_url = "jdbc:mysql://localhost:3306/health_db?useSSL=false&allowPublicKeyRetrieval=true"
jdbc_properties = {
    "user": "root",
    "password": "rootpassword",
    "driver": "com.mysql.cj.jdbc.Driver"
}

try:
    # 3. 让 Spark 读取 MySQL 里的数据表
    # 假设你之前 seed_data.py 创建的表名是 population_data
    df = spark.read.jdbc(url=jdbc_url, table="population_data", properties=jdbc_properties)

    # 4. 展示数据（看看 Spark 是不是真的读到了）
    print("成功从 MySQL 读取到数据！前 5 条展示：")
    df.show(5)

    # 5. 做一个极简单的统计（按地区算平均年龄）
    print("各地区平均年龄初步统计：")
    df.groupBy("district").avg("age").show()

except Exception as e:
    print(f"❌ 哎呀，连接出错了：{e}")

# 最后别忘了关掉
spark.stop()