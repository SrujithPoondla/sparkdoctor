# Python UDF patterns — SDK005
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")


# Python UDF without Arrow — slow serialization
@udf(StringType())  # expect: SDK005
def my_udf(x):
    return str(x).upper()


# udf() call — same issue
slow_udf = udf(lambda x: x + 1, StringType())  # expect: SDK005


# Correct: pandas_udf — vectorized
@pandas_udf(StringType())  # expect: none
def fast_udf(s):
    return s.str.upper()
