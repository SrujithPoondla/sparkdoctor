"""Corpus tests for SDK009 — long transformation chains."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# Long chain (6 calls) — should trigger SDK009
result = (
    df.select("a").filter(F.col("a") > F.lit(0)).groupBy("a").agg(F.sum("a")).orderBy("a").limit(10)
)  # expect: SDK009

# Short chain — should not trigger
short = df.filter(F.col("x") > F.lit(1)).select("a", "b")  # expect: none

# Exactly at threshold (5) — should not trigger
at_threshold = (
    df.select("a").filter(F.col("a") > F.lit(0)).groupBy("a").agg(F.sum("a")).orderBy("a")
)  # expect: none

# Schema builder — not a DataFrame chain
schema = (
    StructType()
    .add("name", "string")
    .add("age", "int")
    .add("email", "string")
    .add("city", "string")
    .add("state", "string")
    .add("zip", "string")
)  # expect: none
