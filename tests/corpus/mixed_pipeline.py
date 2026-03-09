# A realistic but badly-written ETL pipeline — exercises multiple rules
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

# Read data with inferSchema
raw = spark.read.csv("input.csv", inferSchema=True)  # expect: SDK019

# Cache without unpersist
raw.cache()  # expect: SDK007

# Debugging leftover
raw.show()  # expect: SDK023

# withColumn in a loop — SDK004 reports on the loop line
transforms = ["upper_name", "lower_name", "trim_name"]
for t in transforms:  # expect: SDK004
    raw = raw.withColumn(t, col("name"))

# Collect full dataset
all_rows = raw.collect()  # expect: SDK002

# Hardcoded repartition before write
raw.repartition(200).write.parquet("output/")  # expect: SDK001

# Count for emptiness check
if raw.count() == 0:  # expect: SDK003
    print("empty")
