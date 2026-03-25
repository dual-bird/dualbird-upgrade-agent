"""Seed fixture: PySpark pipeline with UDFs that will block FPGA acceleration."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf, col
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

spark = SparkSession.builder.appName("udf_heavy").getOrCreate()

# Regular UDF
@udf(returnType=StringType())
def clean_name(name):
    return name.strip().title() if name else ""

# Pandas UDF
@pandas_udf(DoubleType())
def normalize_score(scores: pd.Series) -> pd.Series:
    return (scores - scores.mean()) / scores.std()

df = spark.read.parquet("s3://data/users")
result = (
    df
    .withColumn("clean_name", clean_name(col("name")))
    .withColumn("norm_score", normalize_score(col("score")))
    .groupBy("department")
    .agg({"norm_score": "avg"})
    .orderBy("department")
)
result.write.parquet("s3://output/normalized")
