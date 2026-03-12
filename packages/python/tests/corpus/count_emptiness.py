# Count as emptiness check — SDK003
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Using count() for emptiness — triggers full scan
if df.count() == 0:  # expect: SDK003
    pass

if df.count() > 0:  # expect: SDK003
    pass

# Correct alternative
if df.head(1) is None:  # expect: none
    pass

# Count used for actual counting — not an emptiness check
total = df.count()  # expect: none
