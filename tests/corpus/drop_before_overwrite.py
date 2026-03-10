"""Corpus tests for SDK020 — DROP TABLE before overwrite."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# DROP TABLE before overwrite — should trigger
spark.sql("DROP TABLE IF EXISTS my_table")  # expect: SDK020
df.write.format("delta").mode("overwrite").saveAsTable("my_table")

# Overwrite without drop — should not trigger
df.write.format("delta").mode("overwrite").save("/output")  # expect: none
