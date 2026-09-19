
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

spark = SparkSession.builder \
        .appName("BroadcastJoin") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions","4")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Large DataFrame — orders
orders = spark.createDataFrame(
    [(i, ["North","South","East","West"][i%4], float(i*100))
     for i in range(1, 10001)],
    ["order_id", "region", "sales"]
)


# Small DataFrame — region targets (lookup table)
targets = spark.createDataFrame([
    ("North", 50000.0),
    ("South", 70000.0),
    ("East",  40000.0),
    ("West",  60000.0),
], ["region", "target"])

# Regular join — causes shuffle
start = time.time()
orders.join(targets, on="region", how="left").count()
print(f"Regular join: {time.time()-start:.2f}s")

# Broadcast join — sends small DataFrame to every executor, no shuffle
start = time.time()
orders.join(F.broadcast(targets), on="region", how="left").count()
print(f"Broadcast join: {time.time()-start:.2f}s")

spark.stop()

