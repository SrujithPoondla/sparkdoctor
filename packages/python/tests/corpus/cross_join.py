# Cross join — SDK016
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.parquet("users")
df2 = spark.read.parquet("products")

# Explicit cross join — produces N*M rows
result = df1.crossJoin(df2)  # expect: SDK016

# Correct: regular join with condition
result = df1.join(df2, df1.id == df2.user_id)  # expect: none
