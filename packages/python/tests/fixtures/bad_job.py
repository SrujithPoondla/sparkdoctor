"""
tests/fixtures/bad_job.py

A realistic-looking PySpark pipeline that contains every anti-pattern
SparkDoctor detects. Used to verify all rules fire correctly.

This should look like real (bad) production code, not a toy.
Every anti-pattern is commented so contributors can see what each one is.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UserEventPipeline").getOrCreate()

# Load raw events
raw_events = spark.read.parquet("s3://data-lake/events/")
users = spark.read.parquet("s3://data-lake/users/")


# ── SDK001: Hardcoded repartition count ─────────────────────────────────────
# Tuned for 50 GB in 2022. Data is now 500 GB. Nobody noticed.
events = raw_events.repartition(200)


# ── SDK006: repartition(1) — forces single-partition processing ──────────────
# Someone wanted a single output file and did this. Now 500 GB flows through
# one executor core.
daily_summary = (
    events.filter(F.col("date") == "2024-01-15")
    .groupBy("user_id")
    .agg(F.count("event_type").alias("event_count"))
    .coalesce(1)
)


# ── SDK005: Python UDF without Arrow ─────────────────────────────────────────
# Row-at-a-time serialization. 10-100x slower than a built-in equivalent.
@udf(returnType=StringType())
def normalize_country(country_code):
    mapping = {"US": "United States", "GB": "United Kingdom", "DE": "Germany"}
    return mapping.get(country_code, country_code)


# Another form of the same problem
clean_device = udf(lambda d: d.lower().strip() if d else "unknown", StringType())


# ── SDK004: withColumn inside a loop ─────────────────────────────────────────
# 12 columns = 12 nested projections in the query plan.
# Plan compilation time: seconds. At 100+ columns: minutes.
BOOLEAN_FLAGS = [
    "is_mobile",
    "is_premium",
    "has_notifications",
    "has_2fa",
    "email_verified",
    "phone_verified",
    "gdpr_consent",
    "marketing_opt_in",
    "beta_user",
    "internal_user",
    "test_account",
    "churned",
]

enriched = events
for flag in BOOLEAN_FLAGS:
    enriched = enriched.withColumn(flag, F.col(flag).cast("boolean"))


# ── SDK003: count() used as emptiness check ───────────────────────────────────
# Scans all 500 GB to answer a yes/no question.
if events.count() == 0:
    print("No events found, exiting.")
    spark.stop()
    exit(0)

# Another form of the same pattern
has_errors = events.filter(F.col("status") == "error").count() > 0


# ── SDK002: collect() without limit() ────────────────────────────────────────
# Pulls all matching rows to the driver. If the filter is selective, fine.
# If not, driver OOM.
active_user_ids = (
    users.filter(F.col("status") == "active")
    .select("user_id")
    .collect()  # No limit. "active" users could be 10M rows.
)


# ── SDK007: cache() without unpersist() ──────────────────────────────────────
# Cached in memory, never released. In a long-running job or notebook session,
# this accumulates until executor OOM.
joined = (
    events.join(users, on="user_id", how="left")
    .withColumn("country", normalize_country(F.col("country_code")))
    .withColumn("device", clean_device(F.col("device_type")))
)
joined.cache()  # Never unpersisted below


# Final aggregation
result = joined.groupBy("country", "device").agg(
    F.count("event_id").alias("total_events"),
    F.countDistinct("user_id").alias("unique_users"),
    F.avg("session_duration").alias("avg_session_sec"),
)

result.write.mode("overwrite").parquet("s3://output/summary/")


# ── SDK012: toPandas() without limit() ───────────────────────────────────────
# Pulls entire DataFrame to driver as pandas — OOM on large data.
pandas_summary = result.toPandas()


# ── SDK013: RDD API usage on DataFrame ───────────────────────────────────────
# Forces Python serialization, bypasses Catalyst optimizer.
country_list = joined.rdd.map(lambda row: row.country).distinct().collect()


# ── SDK016: crossJoin() — cartesian product ──────────────────────────────────
# N * M rows. Two 1M-row tables = 1 trillion rows.
categories = spark.read.parquet("s3://data-lake/categories/")
cross = events.crossJoin(categories)


# ── SDK023: show() left in production code ───────────────────────────────────
# Triggers a full Spark action. Nobody sees this output in production.
result.show()


# ── SDK025: union() instead of unionByName() ─────────────────────────────────
# Matches columns by position, not name. Silent data corruption risk.
old_events = spark.read.parquet("s3://data-lake/events_2023/")
all_events = events.union(old_events)


# ── SDK026: f-string in spark.sql() ──────────────────────────────────────────
# SQL injection risk and prevents Catalyst plan caching.
table_name = "events_raw"
spark.sql(f"SELECT count(*) FROM {table_name}")


# ── SDK031: collect() inside a loop ───────────────────────────────────────
# Triggers a full Spark action on every iteration. O(N) full-dataset scans.
groups = ["US", "GB", "DE"]
for g in groups:
    subset = events.filter(F.col("country") == g).collect()


# ── SDK019: inferSchema=True ──────────────────────────────────────────────
# Extra pass over data + unstable types.
csv_data = spark.read.csv("s3://data-lake/uploads/raw.csv", inferSchema=True, header=True)


# ── SDK015: Hardcoded shuffle partitions ──────────────────────────────────
# Overrides AQE — partition count tuned for 2022 data size.
spark.conf.set("spark.sql.shuffle.partitions", "200")


# ── SDK014: AQE explicitly disabled ──────────────────────────────────────
# Removes all adaptive optimizations.
spark.conf.set("spark.sql.adaptive.enabled", "false")


# ── SDK017: select("*") wildcard ──────────────────────────────────────────
# Defeats column pruning on wide tables.
everything = events.select("*")


# ── SDK027: orderBy() immediately before write ────────────────────────────
# Expensive global sort that's wasted — file order ≠ global order.
result.orderBy("country").write.parquet("s3://output/sorted/")


spark.stop()
