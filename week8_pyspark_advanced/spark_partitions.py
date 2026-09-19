from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName("Partitions2") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

data = [(i, f"customer_{i%4}", ["North","South","East","West"][i%4]) 
        for i in range(1, 101)]
df = spark.createDataFrame(data, ["id", "customer", "region"])

print(f"Default partitions: {df.rdd.getNumPartitions()}")

# repartition — increase or decrease, full shuffle
df_rep = df.repartition(4)
print(f"After repartition(4): {df_rep.rdd.getNumPartitions()}")

# coalesce — reduce only, no shuffle
df_coal = df.coalesce(2)
print(f"After coalesce(2): {df_coal.rdd.getNumPartitions()}")

# repartition by column — data grouped by region in each partition
df_by_region = df.repartition(4, "region")
print(f"After repartition by region: {df_by_region.rdd.getNumPartitions()}")

# Write partitioned by region — creates separate folders
df.write.mode("overwrite").partitionBy("region").parquet("/tmp/partitioned_output")
print("Written partitioned output")

import subprocess
result = subprocess.run(["ls", "/tmp/partitioned_output"], capture_output=True, text=True)
print(f"Output folders: {result.stdout}")

spark.stop()



