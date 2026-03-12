# withColumn in loop — SDK004
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# withColumn inside a for loop — O(N²) plan compilation
columns = ["a", "b", "c"]
for col_name in columns:  # expect: SDK004
    df = df.withColumn(col_name, F.lit(0))

# withColumn inside a while loop
i = 0
while i < 3:  # expect: SDK004
    df = df.withColumn(f"col_{i}", F.lit(i))
    i += 1

# withColumn outside a loop — fine
df = df.withColumn("new_col", F.lit(1))  # expect: none

# Correct: use select once (select("*") triggers SDK017 — that's expected)
df = df.select("*", *[F.lit(0).alias(c) for c in columns])  # expect: SDK017
