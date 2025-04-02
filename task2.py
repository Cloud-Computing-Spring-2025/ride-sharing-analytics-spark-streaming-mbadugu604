from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum as _sum, avg
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import uuid

# Step 1: Create Spark session
spark = SparkSession.builder \
    .appName("RideSharingStreamingTask2") \
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation") \
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Step 2: Define schema
schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", TimestampType())

# Step 3: Read from socket
raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Step 4: Parse JSON
parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Step 5: Aggregation by driver_id
aggregated_stream = parsed_stream.groupBy("driver_id").agg(
    _sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

# Step 6: Write to console (in complete mode)
console_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Step 7: Write to CSV using foreachBatch (with dynamic unique folder)
def write_to_csv(batch_df, epoch_id):
    if not batch_df.rdd.isEmpty():
        unique_path = f"output/driver_aggregates/batch_{str(uuid.uuid4())}"
        batch_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv(unique_path)

csv_query = aggregated_stream.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "output/checkpoint_driver") \
    .start()

# Step 8: Await termination
console_query.awaitTermination()
csv_query.awaitTermination()
