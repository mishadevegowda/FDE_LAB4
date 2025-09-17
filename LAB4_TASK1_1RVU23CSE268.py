# Databricks notebook source
from pyspark.sql.functions import col, to_date

df_raw = spark.table("healthcare_orders")

df_cleaned = (
    df_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
          .withColumn("total_value", col("quantity") * col("price"))
)

df_cleaned.createOrReplaceTempView("tmp_healthcare_orders_cleaned")

spark.sql("""
CREATE OR REPLACE TABLE silver_healthcare_orders AS
SELECT * FROM tmp_healthcare_orders_cleaned
""")