# Repartition and coalesce patterns — SDK001, SDK006
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Hardcoded repartition with literal > 1
df.repartition(200)  # expect: SDK001
df.coalesce(50)  # expect: SDK001

# Repartition to 1 — almost always wrong in production
df.repartition(1)  # expect: SDK006
df.coalesce(1)  # expect: SDK006

# Dynamic repartition — correct
num_partitions = 200
df.repartition(num_partitions)  # expect: none

# Column-based repartition — correct
df.repartition("user_id")  # expect: none
df.repartition(200, "user_id")  # expect: SDK001
