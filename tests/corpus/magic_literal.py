"""Corpus tests for SDK011 — magic literals in filter/when."""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data")

# Magic string in filter — should trigger
filtered = df.filter(F.col("status") == "active")  # expect: SDK011

# Magic int in where — should trigger
high_tier = df.where(F.col("tier") > 3)  # expect: SDK011

# Named constant — should not trigger
ACTIVE = "active"
clean = df.filter(F.col("status") == ACTIVE)  # expect: none

# Allowed literal (0, 1) — should not trigger
nonzero = df.filter(F.col("count") > 0)  # expect: none
