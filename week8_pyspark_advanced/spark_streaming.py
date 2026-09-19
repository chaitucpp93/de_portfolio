from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder \
        .appName("StructuredStreaming")\
        .master("local[*]")\
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema — must define explicitly for streaming

schema = StructType([
    StructField("Order_Id", IntegerType(), True) ,
    StructField("Customer", StringType(),   True) ,
    StructField("region"  , StringType(),   True) ,
    StructField("sales"   , DoubleType(),   True) 
])

# Create input folder
import os
os.makedirs("/tmp/stream_input", exist_ok=True)
os.makedirs("/tmp/stream_output", exist_ok=True)
os.makedirs("/tmp/stream_checkpoint", exist_ok=True)

# Read stream — watches folder for new CSV files
df_stream = spark.readStream \
    .schema(schema) \
    .csv("/tmp/stream_input")

# Transform — same API as batch
df_transformed = df_stream \
    .filter(F.col("sales") > 0) \
    .withColumn("sales_doubled", F.col("sales") * 2)

#df_transformed.show()

# Write stream — output to folder
query = df_transformed.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "/tmp/stream_output") \
    .option("checkpointLocation", "/tmp/stream_checkpoint") \
    .option("header", True) \
    .start()

print("Streaming started — waiting for files in /tmp/stream_input/")
print("Drop a CSV file there to see it processed")

# Run for 30 seconds then stop
query.awaitTermination(30)
spark.stop()
