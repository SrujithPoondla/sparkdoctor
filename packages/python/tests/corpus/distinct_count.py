# distinct().count() anti-pattern — SDK028
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# distinct().count() — two-pass: shuffle + count
n = df.select("user_id").distinct().count()  # expect: SDK028

# dropDuplicates().count() — same problem
n = df.dropDuplicates(["user_id"]).count()  # expect: SDK028

# Correct: countDistinct in a single aggregate
n = df.select(F.countDistinct("user_id")).limit(1).collect()[0][0]  # expect: none

# Correct: approx_count_distinct for large data
n = df.select(F.approx_count_distinct("user_id")).limit(1).collect()[0][0]  # expect: none

# distinct() without count — fine
deduped = df.select("user_id").distinct()  # expect: none

# count() without distinct — fine
total = df.count()  # expect: none
