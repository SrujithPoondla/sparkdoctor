# Cross-DataFrame column reference — SDK008
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.read.parquet("orders")
df2 = spark.read.parquet("users")

# Using df1's column inside df2's select — AnalysisException risk
result = df2.select(df1.user_id, df2.name)  # expect: SDK008

# Same DataFrame ref — fine
result = df2.select(df2.user_id, df2.name)  # expect: none

# String column refs — fine
result = df2.select("user_id", "name")  # expect: none
