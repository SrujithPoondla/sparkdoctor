# Cache/persist without unpersist — SDK007
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Cache without unpersist — memory leak
df.cache()  # expect: SDK007

# Correct: cache with unpersist
cached = df.cache()  # expect: none
cached.count()
cached.unpersist()
