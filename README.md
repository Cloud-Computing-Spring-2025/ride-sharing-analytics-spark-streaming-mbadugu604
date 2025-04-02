# ride-sharing-spark-streaming
Spark structured streaming example
# Real-Time Ride-Sharing Analytics with Apache Spark

## *Overview*
In this project, you will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. The goal is to process streaming ride data, compute real-time aggregations, and extract trends using time-based windows.

---

## *Project Structure*

RideSharingAnalytics/
├── output/
│   ├── driver_aggregate/
│   ├── parsed_csv/
│   └── windowed_fares/
├── data_generator.py
├── task1.py
├── task2.py
├── task3.py
└── README.md


---

## *Getting Started*

## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     bash
     python3 --version
     

2. *PySpark*:
   - Install using pip:
     bash
     pip install pyspark
     

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     bash
     spark-submit --version
     
  4. *PySpark*:
   - Install using pip:
     bash
     pip install faker
     
   

---

### *2. Dataset Generation*
Run the script to generate synthetic ride-sharing metadata and logs:

bash
python data_generator.py

This will populate the input/ folder with songs_metadata.csv and listening_logs.csv. It includes users with biased behavior to simulate genre loyalty.

---

## *Assignment Tasks*

### *Task 1: Ingest and Parse Streaming Data*
*Script:* task1.py

- Ingest data from a socket (e.g. localhost:9999)
- Parse incoming JSON data into a Spark DataFrame
- Display parsed data in the console

Run:
bash
python task1.py


---

### *Task 2: Real-Time Aggregations (Driver-Level)*
*Script:* task2.py

- Group by driver_id
- Compute:
  - Total fare amount (SUM(fare_amount))
  - Average distance (AVG(distance_km))
- Write results to output/task2/

Run:
bash
python task2.py


---

### *Task 3: Sliding Window Analysis*
*Script:* task3.py

- Convert timestamp to proper TimestampType
- Use window() for a *5-minute sliding window (every 1 minute)*
- Aggregate fare_amount in this window
- Write results to output/task3/

Run:
bash
python task3.py


---

### *Genre Loyalty Analysis*
*Script:* task3.py (or include within it as needed)

- Merges listening logs with song metadata
- Calculates each user’s top genre and loyalty score:
  
  loyalty_score = plays_in_top_genre / total_plays
  
- Outputs users with loyalty score > 0.8

---

## 📦 Outputs
- Parsed streaming input in output/parsed_csv/
- Driver-level aggregations in output/driver_aggregate/
- Sliding window fare sums in output/windowed_fares/


## Code Explanation
### Task 1
 python
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

#### Explanation:
This Spark Streaming application processes real-time ride-sharing trip data from a socket, parses JSON messages, displays them in the console, and stores them as CSV files.

Steps

Initialize Spark Session – Creates a Spark session and sets log level.

Define Schema – Specifies the structure for incoming JSON data.

Read Stream – Reads real-time data from localhost:9999.

Parse JSON – Converts raw data into structured format.

Console Output – Displays parsed data in the terminal.

Write to CSV – Stores data in output/parsed_csv/ with a checkpoint for fault tolerance.

Await Termination – Keeps the application running for continuous streaming

#### Output
[Task 1 outpout](./output/parsed_csv/)


### Task 2

 python
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


#### Explanation
This Spark Streaming application processes real-time ride-sharing trip data from a socket, aggregates fare and distance metrics per driver, displays results in the console, and stores aggregated data in dynamically named CSV files.

Steps

Initialize Spark Session – Configures and starts a Spark session with GC metrics.

Define Schema – Specifies the structure for incoming JSON data.

Read Stream – Reads real-time data from localhost:9999.

Parse JSON – Converts raw data into a structured format.

Aggregate Data – Computes total fare and average distance per driver_id.

Console Output – Displays aggregated results in complete mode.

Write to CSV – Saves batch data dynamically into uniquely named folders.

Await Termination – Keeps the application running for continuous streaming.

#### Output
[Task 2 outpout](./output/driver_aggregates/)

### Task 3

python
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


#### Code explanation
This Spark Streaming application processes real-time ride-sharing trip data, applies time-windowed aggregations on fare amounts, and saves the results in CSV files.

Steps

Initialize Spark Session – Configures and starts a Spark session with GC metrics.

Define Schema – Specifies the structure for incoming JSON data, treating timestamp as a string initially.

Read Stream – Reads real-time data from localhost:9999.

Parse & Convert Timestamp – Converts timestamp to event_time for time-based processing.

Windowed Aggregation – Groups data into 5-minute windows with a 1-minute slide, summing fare_amount.

Flatten Window Structure – Extracts window_start, window_end, and total_fare.

Write to CSV – Saves each batch in a uniquely named folder.

Await Termination – Keeps the application running for continuous streaming.

#### Output

[Task 3 outpout](./output/windowed_fares/)
