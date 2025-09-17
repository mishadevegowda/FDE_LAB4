# Databricks notebook source
from pyspark.sql.functions import sum as spark_sum
df_cleaned = spark.table("silver_healthcare_orders")
df_category_sales = (
    df_cleaned.groupBy("service_category")
              .agg(spark_sum("total_value").alias("total_revenue"))
)
df_category_sales.createOrReplaceTempView("tmp_healthcare_category_sales")
spark.sql("""
CREATE OR REPLACE TABLE gold_healthcare_category_sales AS
SELECT * FROM tmp_healthcare_category_sales
""")
df_daily_sales = (
    df_cleaned.groupBy("order_date")
              .agg(spark_sum("total_value").alias("daily_revenue"))
)
df_daily_sales.createOrReplaceTempView("tmp_healthcare_daily_sales")
spark.sql("""
CREATE OR REPLACE TABLE gold_healthcare_daily_sales AS
SELECT * FROM tmp_healthcare_daily_sales
""")