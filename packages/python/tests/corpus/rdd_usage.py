# RDD API on DataFrame — SDK013
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Using RDD API on a DataFrame — bypasses Catalyst optimizer
rdd = df.rdd.map(lambda row: row[0])  # expect: SDK013

# Correct: use DataFrame API
result = df.select("col1")  # expect: none
