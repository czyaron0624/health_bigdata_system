# -*- coding: utf-8 -*-
"""
年度指标汇总处理器。
功能: 从 health_ocr_metrics / vw_metric_clean 聚合生成 ocr_metrics_yearly。
执行: python spark_job/yearly_metrics_processor.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct, first, lit, sum, trim, when


MYSQL_URL = "jdbc:mysql://localhost:3306/health_db"
MYSQL_PROPS = {
    "user": "root",
    "password": "rootpassword",
    "driver": "com.mysql.cj.jdbc.Driver",
}


def read_mysql_table(spark, table_name):
    return spark.read.jdbc(MYSQL_URL, table_name, properties=MYSQL_PROPS)


def load_metric_df(spark):
    try:
        df = read_mysql_table(spark, "vw_metric_clean")
        print("Using vw_metric_clean as yearly metrics source")
    except Exception:
        df = read_mysql_table(spark, "health_ocr_metrics")
        print("vw_metric_clean unavailable, fallback to health_ocr_metrics")

    region_expr = (
        when(col("source_table") == "guangxi_news", lit("广西"))
        .when(col("source_table") == "sichuan_news", lit("四川"))
        .when(col("source_table") == "national_news", lit("国家"))
        .otherwise(trim(col("source_table")))
    )

    return (
        df.filter(col("year").isNotNull())
        .filter(col("metric_value").isNotNull())
        .withColumn("region", region_expr)
    )


def build_yearly_summary(metric_df):
    summary_df = metric_df.groupBy("region", "year").agg(
        sum(when(col("metric_key") == "doctor_count", col("metric_value")).otherwise(0)).alias("doctor_count"),
        sum(when(col("metric_key") == "nurse_count", col("metric_value")).otherwise(0)).alias("nurse_count"),
        sum(when(col("metric_key") == "bed_count", col("metric_value")).otherwise(0)).alias("bed_count"),
        avg(when(col("metric_key") == "bed_usage_rate", col("metric_value")).otherwise(None)).alias("bed_usage_rate"),
        sum(when(col("metric_key") == "outpatient_visits", col("metric_value")).otherwise(0)).alias("outpatient_visits"),
        sum(when(col("metric_key") == "discharge_count", col("metric_value")).otherwise(0)).alias("discharge_count"),
        avg(when(col("metric_key") == "avg_stay_days", col("metric_value")).otherwise(None)).alias("avg_stay_days"),
        avg(when(col("metric_key") == "outpatient_cost", col("metric_value")).otherwise(None)).alias("outpatient_cost"),
        avg(when(col("metric_key") == "discharge_cost", col("metric_value")).otherwise(None)).alias("discharge_cost"),
        countDistinct("news_id").alias("sample_count"),
        countDistinct("source_table").alias("source_count"),
        first("source_table", ignorenulls=True).alias("primary_source"),
    )

    return (
        summary_df.withColumn(
            "data_source",
            when(col("source_count") <= 1, col("primary_source")).otherwise(lit("mixed")),
        )
        .drop("source_count", "primary_source")
    )


def main():
    print("=" * 60)
    print("年度指标汇总处理器")
    print("=" * 60)

    spark = (
        SparkSession.builder.appName("Yearly_Metrics_Processor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        metric_df = load_metric_df(spark)
        yearly_summary = build_yearly_summary(metric_df)
        yearly_summary.write.jdbc(MYSQL_URL, "ocr_metrics_yearly", "overwrite", MYSQL_PROPS)
        print("ocr_metrics_yearly updated")
    except Exception as exc:
        print(f"处理失败: {exc}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
