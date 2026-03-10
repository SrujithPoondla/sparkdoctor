"""Corpus tests for SDK018 — inconsistent column reference style."""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# Mixed styles — should trigger (F.col + subscript)
mixed = df.select(F.col("name"), df["age"])  # expect: SDK018

# Consistent F.col — should not trigger
consistent = df.select(F.col("name"), F.col("age"))  # expect: none

# config subscript should not count as df["col"]
config = {"key": "value"}
x = config["key"]  # expect: none
