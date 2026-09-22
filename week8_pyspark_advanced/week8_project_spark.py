from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import sys

jdbc_jar = "/usr/local/lib/mysql-connector-j-8.0.33.jar"

spark = SparkSession.builder \
        .appName("Week8Project") \
        .master("local[*]") \
        .config("spark.jars",jdbc_jar) \
        .config("spark.sql.shuffle.partitions","4") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

run_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"

print(f"processing date {run_date}")

# Connection details
jdbc_url = "jdbc:mysql://127.0.0.1:3306/week3_sql"
properties = {
    "user"    : "dbt_user",
    "password": "dbt123",
    "driver"  : "com.mysql.cj.jdbc.Driver"
}

# Extract
df = spark.read.jdbc(url=jdbc_url, table="orders", properties=properties)
print(f"Extracted: {df.count()} rows")

# Validate
nulls = df.filter(F.col("order_id").isNull()).count()
if nulls > 0:
    raise ValueError(f"Found {nulls} null order_ids — stopping")
print("Validation passed")

# Transform
df_enriched = df \
    .withColumn("margin_pct",
        F.round(F.col("profit") / F.col("sales") * 100, 2)) \
    .withColumn("run_date", F.lit(run_date))

# Write partitioned Parquet
parquet_path = f"/tmp/week8_output/{run_date}"
df_enriched.write \
    .mode("overwrite") \
    .partitionBy("region") \
    .parquet(parquet_path)
print(f"Parquet written to {parquet_path}")

# Write to MySQL for dbt
df_enriched.write.jdbc(
    url        = jdbc_url,
    table      = "orders_enriched",
    mode       = "overwrite",
    properties = properties
)
print("MySQL table orders_enriched written")

spark.stop()




