from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

spark = SparkSession.builder \
        .appName("AirFlowSalesJob") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions",4)\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Accept Date Argument from Airflow

run_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
print(f"Processing for date:{run_date}")

# Data
data = [
    (1001, "Alice",   "North", 1200.0, 240.0),
    (1002, "Bob",     "South", 3500.0, 700.0),
    (1003, "Alice",   "North", 2200.0, 440.0),
    (1004, "Charlie", "East",   800.0, -80.0),
    (1005, "Bob",     "South", 1500.0, 300.0),
]

df = spark.createDataFrame(data,
    ["order_id","customer","region","sales","profit"])

result = df.groupBy("region") \
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("sales"), 2).alias("total_sales")
    )

result.show()

output_path = f"/tmp/spark_output/{run_date}"
result.coalesce(1).write.mode("overwrite").parquet(output_path)
print(f"Output written to {output_path}")

spark.stop()




