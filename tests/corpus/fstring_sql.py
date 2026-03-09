# F-string in SQL — SDK026
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

table = "users"
col = "name"

# f-string SQL — injection risk and no query plan caching
df = spark.sql(f"SELECT * FROM {table}")  # expect: SDK026
df = spark.sql(f"SELECT {col} FROM users WHERE id = 1")  # expect: SDK026

# Correct: parameterized or static SQL
df = spark.sql("SELECT * FROM users")  # expect: none
df = spark.table("users")  # expect: none
