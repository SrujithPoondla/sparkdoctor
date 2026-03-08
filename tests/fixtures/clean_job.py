"""
tests/fixtures/clean_job.py

The same pipeline as bad_job.py, written correctly.
SparkDoctor must produce zero findings on this file.

This demonstrates the correct alternative for every anti-pattern
in bad_job.py.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

spark = SparkSession.builder.appName("UserEventPipeline").getOrCreate()

raw_events = spark.read.parquet("s3://data-lake/events/")
users = spark.read.parquet("s3://data-lake/users/")


# ── Correct: no hardcoded repartition ────────────────────────────────────────
# Let AQE handle partitioning, or derive count dynamically if needed.
events = raw_events  # AQE manages partitioning automatically (Spark 3.2+)


# ── Correct: write single file without coalesce(1) on full dataset ───────────
daily_summary = (
    events
    .filter(F.col("date") == "2024-01-15")
    .groupBy("user_id")
    .agg(F.count("event_type").alias("event_count"))
)
# Write as a single file using maxRecordsPerFile, not by collapsing to 1 partition
daily_summary.write.option("maxRecordsPerFile", 5_000_000).parquet("s3://output/daily/")


# ── Correct: pandas_udf for vectorized execution ─────────────────────────────
COUNTRY_MAP = {"US": "United States", "GB": "United Kingdom", "DE": "Germany"}

@pandas_udf(returnType=StringType())
def normalize_country(series: pd.Series) -> pd.Series:
    return series.map(lambda c: COUNTRY_MAP.get(c, c) if c else c)


# ── Correct: use built-in for simple string transforms ───────────────────────
# No UDF needed — F.lower() and F.trim() are built-in Spark functions.
events = events.withColumn(
    "device_clean",
    F.coalesce(F.trim(F.lower(F.col("device_type"))), F.lit("unknown"))
)


# ── Correct: select() once with all column transformations ───────────────────
BOOLEAN_FLAGS = [
    "is_mobile", "is_premium", "has_notifications",
    "has_2fa", "email_verified", "phone_verified",
    "gdpr_consent", "marketing_opt_in", "beta_user",
    "internal_user", "test_account", "churned",
]

# One select call, not 12 withColumn calls
flag_exprs = [F.col(flag).cast("boolean").alias(flag) for flag in BOOLEAN_FLAGS]
other_cols = [F.col(c) for c in events.columns if c not in BOOLEAN_FLAGS]
enriched = events.select(*other_cols, *flag_exprs)


# ── Correct: isEmpty() instead of count() == 0 ───────────────────────────────
if enriched.isEmpty():
    print("No events found, exiting.")
    spark.stop()
    exit(0)

# For more complex checks, limit(1) is much cheaper than count()
error_sample = events.filter(F.col("status") == "error").limit(1).collect()
has_errors = len(error_sample) > 0


# ── Correct: limit() before collect() ────────────────────────────────────────
active_user_ids = (
    users
    .filter(F.col("status") == "active")
    .select("user_id")
    .limit(100_000)   # bounded; if you need all of them, write to storage instead
    .collect()
)


# ── Correct: cache() with corresponding unpersist() ──────────────────────────
joined = (
    events
    .join(users, on="user_id", how="left")
    .withColumn("country", normalize_country(F.col("country_code")))
)
joined.cache()

try:
    result = (
        joined
        .groupBy("country", "device_clean")
        .agg(
            F.count("event_id").alias("total_events"),
            F.countDistinct("user_id").alias("unique_users"),
            F.avg("session_duration").alias("avg_session_sec"),
        )
    )
    result.write.mode("overwrite").parquet("s3://output/summary/")
finally:
    joined.unpersist()   # Always release, even if write fails


spark.stop()
