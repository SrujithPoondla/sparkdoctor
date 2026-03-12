# inferSchema — SDK019
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# inferSchema=True triggers a full scan to determine types
df = spark.read.csv("data.csv", inferSchema=True)  # expect: SDK019

# Correct: provide explicit schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
df = spark.read.schema(schema).csv("data.csv")  # expect: none
