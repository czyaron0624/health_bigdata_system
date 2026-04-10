from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when

# 1. 初始化（本地模式）
spark = SparkSession.builder \
    .appName("HealthDataProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "com.mysql:mysql-connector-j:8.0.33") \
    .getOrCreate()

# 2. 读取 MySQL 中的原始数据
jdbc_url = "jdbc:mysql://localhost:3306/health_db?useSSL=false&characterEncoding=UTF-8"
properties = {"user": "root", "password": "rootpassword", "driver": "com.mysql.cj.jdbc.Driver"}

raw_df = spark.read.jdbc(url=jdbc_url, table="gov_news", properties=properties)

# 3. 数据清洗与特征提取 (ETL)
# 我们使用正则表达式提取标题里的“年份”和“月份”
processed_df = raw_df.filter(col("title").rlike("年|月")) \
    .withColumn("year", regexp_extract(col("title"), r"(\d{4})年", 1)) \
    .withColumn("month", regexp_extract(col("title"), r"(\d{1,2})月", 1)) \
    .withColumn("category", 
        when(col("title").contains("医疗服务"), "医疗服务")
        .when(col("title").contains("人口") | col("title").contains("医师"), "资源统计")
        .otherwise("其他公告")
    )

# 4. 展示分析结果
print("✨ Spark 处理后的结构化数据：")
processed_df.select("publish_date", "year", "month", "category", "title") \
    .orderBy(col("year").desc(), col("month").desc()) \
    .show(10, truncate=False)

# 5. 统计：各年份发布的报告数量
print("📊 各年份发布报告分布：")
processed_df.groupBy("year").count().orderBy("year").show()


import redis
import json

# 1. 整理 Spark 的计算结果
print("💾 正在将分析结果同步至 Redis...")
# 我们把之前 groupBy("year").count() 的结果拿出来
year_dist_list = processed_df.groupBy("year").count().collect()

# 转换成 Python 字典，空年份记为 "历年/未知"
data_to_cache = {
    str(row['year'] if row['year'] != "" else "未知/汇总"): row['count'] 
    for row in year_dist_list
}

# 2. 连接 Redis
# 因为你 Docker 映射了 6379，所以这里用 localhost 没问题
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# 3. 存入 Redis (设置一个 Key 叫 'health_stats')
r.set("health_stats", json.dumps(data_to_cache))

print(f"✅ 成功写入 Redis！存储内容为: {data_to_cache}")

spark.stop()