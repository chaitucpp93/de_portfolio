
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

jdbc_jar =  "/usr/local/lib/mysql-connector-j-8.0.33.jar"

spark = SparkSession.builder \
        .appName("SalesProjectPipeline") \
        .master("local[*]") \
        .config("spark.jars",jdbc_jar)\
        .config("spark.sql.shuffle.partitions","4")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# MySQL connection details
jdbc_url = "jdbc:mysql://127.0.0.1:3306/week3_sql"
properties = {
    "user"    : "dbt_user",
    "password": "dbt123",
    "driver"  : "com.mysql.cj.jdbc.Driver"
}

# ── STEP 1: EXTRACT ───────────────────────────────────────────────
print("=== STEP 1: EXTRACT ===")
df = spark.read.jdbc(
    url        = jdbc_url,
    table      = "orders",
    properties = properties
)
print(f"Rows extracted: {df.count()}")
df.printSchema()

# ── STEP 2: VALIDATE ──────────────────────────────────────────────
print("=== STEP 2: VALIDATE ===")

# Check null counts per column
null_counts = df.select(
        [F.count(F.when(F.isnull(c),c)).alias(c) 
         for c in df.columns])
print("Null counts:")
null_counts.show()


# Stop pipeline if critical columns have nulls
order_id_nulls = df.filter(F.col("order_id").isNull()).count()
if order_id_nulls > 0:
    raise ValueError(f"order_id has {order_id_nulls} nulls — pipeline stopped")

print("Validation passed")


# ── STEP 3 : TRANSFORM ──────────────────────────────────────────────

df_enriched = df.withColumn(
    "margin_pct",
     F.round(F.col("profit") / F.col("sales") * 100, 2)
)

df_enriched.orderBy("region").show()



# Regional Summary

regional_summary = df_enriched \
        .groupBy("region") \
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("sales"), 2).alias("total_sales"),
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.round(F.avg("margin_pct"), 2).alias("avg_margin")
        ) \
        .orderBy("total_sales",ascending = False)

print("==== Regional Summary ===== ")

regional_summary.show()

# Cache — used twice below (write to parquet AND MySQL)

regional_summary.cache()

# Window function — rank customers within each region

window_spec = Window.partitionBy("region").orderBy(F.desc("sales"))
df_ranked = df_enriched.withColumn(
    "region_rank",
    F.dense_rank().over(window_spec)
)

# Top customer per region
top_customers = df_ranked.filter(F.col("region_rank") == 1) \
    .select("region", "customer", "sales", "region_rank")

print("Top Customer Per Region:")
top_customers.show()


# ── STEP 4 : LOAD ──────────────────────────────────────────────

# write regioin summary to parquet

regional_summary.coalesce(1) \
        .write.mode("overwrite") \
        .parquet("/tmp/region_summary_project")
print("Region summary written to parquet")

# write region summary to MySQL 

regional_summary.write.jdbc(
        url = jdbc_url,
        table = "region_summary_project",
        mode = "overwrite",
        properties = properties
)

print("Region summary printed to MySQL")

#Release the cache

regional_summary.unpersist()


#write top customer to parquet

top_customers.coalesce(1) \
        .write.mode("overwrite") \
        .parquet("/tmp/top_customers_project")

spark.stop()

print("==== PIPELINE COMPLETED ====")



