"""Corpus tests for SDK009 — long transformation chains."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# Long chain — should trigger
result = (  # expect: SDK009
    df.filter("x > 1").withColumn("a", "b").groupBy("c").agg("d").filter("e").orderBy("f")
)

# Short chain — should not trigger
short = df.filter("x > 1").select("a", "b")  # expect: none

# Exactly at threshold (5) — should not trigger
at_threshold = (  # expect: none
    df.filter("x").withColumn("a", "b").groupBy("c").agg("d").orderBy("e")
)
