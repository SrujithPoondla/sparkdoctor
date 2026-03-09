# show() in production code — SDK023
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# show() is for debugging — triggers an action and prints to stdout
df.show()  # expect: SDK023
df.show(20, False)  # expect: SDK023

# Correct: use logging or write to output
df.write.parquet("output")  # expect: none
