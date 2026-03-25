"""Seed fixture: PySpark pipeline using spark.sql() with SQL strings."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("spark_sql").getOrCreate()

spark.read.parquet("s3://data/events").createOrReplaceTempView("events")
spark.read.parquet("s3://data/users").createOrReplaceTempView("users")

result = spark.sql("""
    SELECT
        u.region,
        COUNT(*) AS event_count,
        SUM(e.duration) AS total_duration,
        AVG(e.duration) AS avg_duration,
        MIN(e.timestamp) AS first_event,
        MAX(e.timestamp) AS last_event
    FROM events e
    JOIN users u ON e.user_id = u.id
    WHERE e.event_type = 'purchase'
    GROUP BY u.region
    ORDER BY total_duration DESC
""")

result.write.parquet("s3://output/region_summary")
