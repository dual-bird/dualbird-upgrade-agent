"""Seed fixture: PySpark pipeline with window functions."""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, dense_rank, rank, row_number, sum

spark = SparkSession.builder.appName("window_functions").getOrCreate()

df = spark.read.parquet("s3://data/sales")

window = Window.partitionBy("region").orderBy(col("revenue").desc())
cumulative = (
    Window.partitionBy("region")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

result = (
    df.withColumn("rank", rank().over(window))
    .withColumn("dense_rank", dense_rank().over(window))
    .withColumn("row_num", row_number().over(window))
    .withColumn("cumulative_revenue", sum("revenue").over(cumulative))
    .withColumn("running_avg", avg("revenue").over(cumulative))
    .filter(col("rank") <= 10)
)

result.write.parquet("s3://output/top_sales")
