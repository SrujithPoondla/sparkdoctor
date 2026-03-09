# Select star — SDK017
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# select("*") — reads all columns unnecessarily
result = df.select("*")  # expect: SDK017

# Correct: select specific columns
result = df.select("user_id", "event_type")  # expect: none
