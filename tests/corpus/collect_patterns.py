# Collect and toPandas patterns — SDK002, SDK012, SDK031
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Collect without limit — risky on large datasets
rows = df.collect()  # expect: SDK002

# toPandas without limit — same risk
pdf = df.toPandas()  # expect: SDK012

# Correct: limit before collect
rows = df.limit(100).collect()  # expect: none

# Correct: limit before toPandas
pdf = df.limit(100).toPandas()  # expect: none

# Collect inside a loop — double trouble
items = [("a", "b"), ("c", "d")]
for x, y in items:
    result = df.filter(df.col1 == x).collect()  # expect: SDK002, SDK031
