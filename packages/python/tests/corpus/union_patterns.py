# union() by position — SDK025
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.parquet("a")
df2 = spark.read.parquet("b")

# union() matches by position, not name — data corruption risk
result = df1.union(df2)  # expect: SDK025

# Correct: unionByName matches by column name
result = df1.unionByName(df2)  # expect: none
result = df1.unionByName(df2, allowMissingColumns=True)  # expect: none
