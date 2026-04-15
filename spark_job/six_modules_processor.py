# -*- coding: utf-8 -*-
"""
六大模块数据聚合处理器。
功能: 从各原始表聚合数据，写入分析结果表。
执行: python spark_job/six_modules_processor.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, sum, trim, when


MYSQL_URL = "jdbc:mysql://localhost:3306/health_db"
MYSQL_PROPS = {
    "user": "root",
    "password": "rootpassword",
    "driver": "com.mysql.cj.jdbc.Driver",
}


def read_mysql_table(spark, table_name):
    return spark.read.jdbc(MYSQL_URL, table_name, properties=MYSQL_PROPS)


def normalize_metric_region(df):
    if "region" in df.columns:
        return df.withColumn("region", trim(col("region")))

    return df.withColumn(
        "region",
        when(col("source_table") == "guangxi_news", lit("广西"))
        .when(col("source_table") == "sichuan_news", lit("四川"))
        .when(col("source_table") == "national_news", lit("国家"))
        .otherwise(trim(col("source_table"))),
    )


def load_metric_df(spark):
    try:
        df = read_mysql_table(spark, "vw_metric_clean")
        print("✓ 使用 vw_metric_clean 视图作为指标来源")
    except Exception:
        df = read_mysql_table(spark, "health_ocr_metrics")
        print("! vw_metric_clean 不可用，回退到 health_ocr_metrics")

    df = normalize_metric_region(df)
    return df.filter(col("year").isNotNull()).filter(col("metric_value").isNotNull())


def process_population(spark):
    """模块1: 人口信息统计分析。"""
    print("正在处理: 人口信息统计...")
    df = read_mysql_table(spark, "population_info")

    by_region = (
        df.groupBy("region")
        .agg(sum("population_count").alias("total_population"))
        .withColumn("metric_type", lit("by_region"))
    )
    by_age = (
        df.groupBy("age_group")
        .agg(sum("population_count").alias("total_population"))
        .withColumn("metric_type", lit("by_age_group"))
    )
    by_gender = (
        df.groupBy("gender")
        .agg(sum("population_count").alias("total_population"))
        .withColumn("metric_type", lit("by_gender"))
    )

    by_region.write.jdbc(MYSQL_URL, "analysis_population_region", "overwrite", MYSQL_PROPS)
    by_age.write.jdbc(MYSQL_URL, "analysis_population_age", "overwrite", MYSQL_PROPS)
    by_gender.write.jdbc(MYSQL_URL, "analysis_population_gender", "overwrite", MYSQL_PROPS)
    print("✓ 人口信息统计完成")


def process_institutions(spark):
    """模块2: 医疗卫生机构统计分析。"""
    print("正在处理: 医疗卫生机构统计...")
    df = read_mysql_table(spark, "medical_institution")

    by_type = (
        df.groupBy("type")
        .agg(count("*").alias("institution_count"))
        .withColumn("metric_type", lit("by_type"))
    )
    by_level = (
        df.groupBy("level")
        .agg(count("*").alias("institution_count"))
        .withColumn("metric_type", lit("by_level"))
    )
    by_region = (
        df.groupBy("region")
        .agg(count("*").alias("institution_count"))
        .withColumn("metric_type", lit("by_region"))
    )

    by_type.write.jdbc(MYSQL_URL, "analysis_institution_type", "overwrite", MYSQL_PROPS)
    by_level.write.jdbc(MYSQL_URL, "analysis_institution_level", "overwrite", MYSQL_PROPS)
    by_region.write.jdbc(MYSQL_URL, "analysis_institution_region", "overwrite", MYSQL_PROPS)
    print("✓ 医疗卫生机构统计完成")


def process_personnel(spark, metric_df):
    """模块3: 医疗卫生人员统计分析。"""
    print("正在处理: 医疗卫生人员统计...")
    personnel_df = metric_df.filter(col("metric_key").isin("doctor_count", "nurse_count"))

    result = personnel_df.groupBy("region", "year").agg(
        sum(when(col("metric_key") == "doctor_count", col("metric_value")).otherwise(0)).alias("doctor_count"),
        sum(when(col("metric_key") == "nurse_count", col("metric_value")).otherwise(0)).alias("nurse_count"),
    )
    result = result.withColumn(
        "doctor_nurse_ratio",
        when(col("doctor_count") > 0, col("nurse_count") / col("doctor_count")).otherwise(None),
    )

    result.write.jdbc(MYSQL_URL, "analysis_personnel", "overwrite", MYSQL_PROPS)
    print("✓ 医疗卫生人员统计完成")


def process_beds(spark, metric_df):
    """模块4: 医疗卫生床位统计分析。"""
    print("正在处理: 医疗卫生床位统计...")
    bed_df = metric_df.filter(col("metric_key").isin("bed_count", "bed_usage_rate"))

    result = bed_df.groupBy("region", "year").agg(
        sum(when(col("metric_key") == "bed_count", col("metric_value")).otherwise(0)).alias("bed_count"),
        avg(when(col("metric_key") == "bed_usage_rate", col("metric_value")).otherwise(None)).alias("avg_usage_rate"),
    )

    result.write.jdbc(MYSQL_URL, "analysis_beds", "overwrite", MYSQL_PROPS)
    print("✓ 医疗卫生床位统计完成")


def process_services(spark, metric_df):
    """模块5: 医疗服务统计分析。"""
    print("正在处理: 医疗服务统计...")
    service_df = metric_df.filter(col("metric_key").isin("outpatient_visits", "discharge_count", "avg_stay_days"))

    result = service_df.groupBy("region", "year").agg(
        sum(when(col("metric_key") == "outpatient_visits", col("metric_value")).otherwise(0)).alias("outpatient_visits"),
        sum(when(col("metric_key") == "discharge_count", col("metric_value")).otherwise(0)).alias("discharge_count"),
        avg(when(col("metric_key") == "avg_stay_days", col("metric_value")).otherwise(None)).alias("avg_stay_days"),
    )

    result.write.jdbc(MYSQL_URL, "analysis_services", "overwrite", MYSQL_PROPS)
    print("✓ 医疗服务统计完成")


def process_costs(spark, metric_df):
    """模块6: 医疗费用统计分析。"""
    print("正在处理: 医疗费用统计...")
    cost_df = metric_df.filter(col("metric_key").isin("outpatient_cost", "discharge_cost"))

    result = cost_df.groupBy("region", "year").agg(
        avg(when(col("metric_key") == "outpatient_cost", col("metric_value")).otherwise(None)).alias("avg_outpatient_cost"),
        avg(when(col("metric_key") == "discharge_cost", col("metric_value")).otherwise(None)).alias("avg_discharge_cost"),
    )

    result.write.jdbc(MYSQL_URL, "analysis_costs", "overwrite", MYSQL_PROPS)
    print("✓ 医疗费用统计完成")


def main():
    """主函数：执行六大模块数据处理。"""
    print("=" * 60)
    print("六大模块数据聚合处理器")
    print("=" * 60)

    spark = (
        SparkSession.builder.appName("Six_Modules_Processor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        metric_df = load_metric_df(spark)
        process_population(spark)
        process_institutions(spark)
        process_personnel(spark, metric_df)
        process_beds(spark, metric_df)
        process_services(spark, metric_df)
        process_costs(spark, metric_df)

        print("=" * 60)
        print("六大模块数据处理完成")
        print("=" * 60)
    except Exception as exc:
        print(f"处理失败: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
