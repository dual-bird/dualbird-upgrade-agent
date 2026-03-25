"""Seed fixture: PySpark pipeline with unsupported aggregate functions."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, collect_list, collect_set, approx_count_distinct,
    stddev, variance, percentile_approx, corr,
    sum, count, avg,
)

spark = SparkSession.builder.appName("unsupported_aggs").getOrCreate()

df = spark.read.parquet("s3://data/transactions")

# Mix of supported and unsupported aggregations
result = (
    df
    .groupBy("category")
    .agg(
        sum("amount").alias("total"),           # supported
        count("*").alias("tx_count"),            # supported
        avg("amount").alias("avg_amount"),       # supported
        stddev("amount").alias("stddev_amount"), # NOT supported
        collect_list("product").alias("products"),  # NOT supported
        approx_count_distinct("user_id").alias("approx_users"),  # NOT supported
    )
    .orderBy(col("total").desc())
)

result.write.parquet("s3://output/category_stats")
