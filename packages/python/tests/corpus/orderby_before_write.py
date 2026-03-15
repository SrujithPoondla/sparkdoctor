# orderBy before write — SDK027
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# orderBy/sort right before write — expensive full shuffle for nothing
df.orderBy("timestamp").write.parquet("output")  # expect: SDK027, SDK029

# Correct: write without unnecessary ordering
df.write.mode("append").parquet("output")  # expect: none

# orderBy used for display or limit — fine
df.orderBy("timestamp").show()  # expect: SDK023
df.orderBy("timestamp").limit(10).collect()  # expect: none
