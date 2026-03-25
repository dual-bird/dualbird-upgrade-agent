"""Seed fixture: fully accelerable PySpark pipeline — sort, join, aggregate, parquet I/O."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fully_accelerable").getOrCreate()

orders = spark.read.parquet("s3://data/orders")
customers = spark.read.parquet("s3://data/customers")

result = (
    orders
    .join(customers, orders.customer_id == customers.id)
    .filter(orders.amount > 100)
    .groupBy("city")
    .agg({"amount": "sum", "order_id": "count"})
    .orderBy("city")
)

result.write.parquet("s3://output/city_summary")
