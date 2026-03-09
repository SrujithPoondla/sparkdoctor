# Spark configuration patterns — SDK014, SDK015
from pyspark.sql import SparkSession

# AQE disabled — SDK014 reports on the SparkSession.builder line
spark = (
    SparkSession.builder  # expect: SDK014
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

# Hardcoded shuffle partitions — SDK015
spark.conf.set("spark.sql.shuffle.partitions", "200")  # expect: SDK015

# Correct: AQE enabled (default in Spark 3.2+)
spark = (
    SparkSession.builder
    .config("spark.sql.adaptive.enabled", "true")  # expect: none
    .getOrCreate()
)
