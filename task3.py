from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, window, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import uuid

# Step 1: Spark session
spark = SparkSession.builder \
    .appName("RideSharingStreamingTask3") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Step 2: Schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())  # incoming as string

# Step 3: Read from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse and cast timestamp
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*") \
 .withColumn("event_time", to_timestamp("timestamp"))

# Step 5: Windowed aggregation (5 min window, 1 min slide)
windowed_agg = parsed_stream.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    _sum("fare_amount").alias("total_fare")
)

# Step 6: Flatten window struct to two columns
flattened = windowed_agg.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare")
)

# Step 7: Write to CSV using foreachBatch
def write_to_csv(batch_df, epoch_id):
    if not batch_df.rdd.isEmpty():
        path = f"output/windowed_fares/batch_{str(uuid.uuid4())}"
        batch_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(path)

csv_query = flattened.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "output/checkpoint_windowed") \
    .start()

# Step 8: Await termination
csv_query.awaitTermination()
