from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
        .appName("ShuffleDemo")\
        .master("local[*]") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data = [(i, f"customer_{i%4}", ["North","South","East","West"][i%4], float(i*100))
        for i in range(1, 10001)]
df = spark.createDataFrame(data, ["id","customer","region","sales"])


# DEFAULT — 200 shuffle partitions
start = time.time()
df.groupBy("region").agg(F.sum("sales")).collect()
print(f"Default 200 partitions: {time.time()-start:.2f}s")
print(f"Shuffle partitions after groupBy: {df.groupBy('region').agg(F.sum('sales')).rdd.getNumPartitions()}")

# OPTIMISED — 4 shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "4")
start = time.time()
df.groupBy("region").agg(F.sum("sales")).collect()
print(f"Optimised 4 partitions: {time.time()-start:.2f}s")

spark.stop()

