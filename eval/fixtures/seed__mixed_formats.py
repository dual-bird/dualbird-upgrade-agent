"""Seed fixture: PySpark pipeline reading non-parquet formats (CSV, JSON)."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("mixed_formats").getOrCreate()

# CSV read — parquet read is FPGA-accelerable, CSV is not
csv_df = spark.read.csv("s3://data/legacy.csv", header=True, inferSchema=True)

# JSON read — also not FPGA-accelerable for read
json_df = spark.read.json("s3://data/events.json")

joined = csv_df.join(json_df, csv_df.id == json_df.user_id)

result = (
    joined
    .groupBy("category")
    .agg({"amount": "sum", "id": "count"})
    .orderBy("category")
)

# Parquet write — FPGA-accelerable
result.write.parquet("s3://output/combined")
