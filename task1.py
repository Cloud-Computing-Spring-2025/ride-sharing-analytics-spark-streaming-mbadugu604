from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Step 1: Spark Session
spark = SparkSession.builder \
    .appName("RideSharingStreamingTask1") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Step 2: Define Schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", TimestampType())

# Step 3: Read socket stream
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse JSON
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Step 5: Pretty Console Output
console_query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Step 6: Write to CSV Files
csv_query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/parsed_csv") \
    .option("checkpointLocation", "output/checkpoint_csv") \
    .option("header", True) \
    .start()

# Await Termination
console_query.awaitTermination()
csv_query.awaitTermination()
