# DataFrame write without explicit mode — SDK029
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Write without mode — defaults to errorifexists, fails on re-run
df.write.parquet("output")  # expect: SDK029
df.write.format("delta").save("path")  # expect: SDK029
df.write.partitionBy("date").csv("output")  # expect: SDK029
df.write.saveAsTable("my_table")  # expect: SDK029
df.write.option("compression", "snappy").json("output")  # expect: SDK029

# Correct: explicit mode specified
df.write.mode("overwrite").parquet("output")  # expect: none
df.write.format("delta").mode("append").save("path")  # expect: none
df.write.mode("ignore").saveAsTable("my_table")  # expect: none

# insertInto always appends — no mode needed
df.write.insertInto("table")  # expect: none

# writeStream has different semantics — not flagged
df.writeStream.format("kafka").start()  # expect: none
