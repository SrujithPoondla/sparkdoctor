# Streaming readStream without schema — SDK024
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

# readStream without explicit schema — fails or infers at runtime
df = spark.readStream.format("csv").load("data/")  # expect: SDK024

# Correct: readStream with schema
schema = StructType([StructField("value", StringType())])
df = spark.readStream.schema(schema).format("csv").load("data/")  # expect: none

# Kafka source — schema is implicit, but still flagged
df = spark.readStream.format("kafka").option("subscribe", "topic").load()  # expect: SDK024
