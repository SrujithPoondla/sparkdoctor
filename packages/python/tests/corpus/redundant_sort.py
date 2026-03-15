# Redundant sort in chain — SDK030
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("events")

# Two orderBy calls — first is wasted
df.orderBy("date").orderBy("user_id").write.parquet("out")  # expect: SDK030, SDK027, SDK029

# Mixing sort() and orderBy() — first is wasted
df.sort("date").orderBy("user_id")  # expect: SDK030

# sort through a filter — still redundant
df.sort("a").filter(df.x > 0).sort("b")  # expect: SDK030

# Single sort — fine
df.orderBy("user_id", "date").write.parquet("out")  # expect: SDK027, SDK029

# Single sort — fine
df.sort("a")  # expect: none

# sortWithinPartitions then orderBy — different operations, fine
df.sortWithinPartitions("a").orderBy("b")  # expect: none
