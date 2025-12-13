# PySpark Structured Streaming Guide

Complete guide to PySpark Structured Streaming with examples covering all major concepts.

## 📚 Table of Contents

1. [Overview](#overview)
2. [File Structure](#file-structure)
3. [Quick Start](#quick-start)
4. [Core Concepts](#core-concepts)
5. [Examples Summary](#examples-summary)
6. [Common Patterns](#common-patterns)
7. [Best Practices](#best-practices)
8. [Troubleshooting](#troubleshooting)

---

## Overview

**Structured Streaming** is a scalable and fault-tolerant stream processing engine built on Spark SQL. It allows you to process real-time data streams using the same DataFrame/Dataset API as batch processing.

### Key Features

- ✅ **Unified API**: Same DataFrame API for batch and streaming
- ✅ **Fault Tolerant**: Exactly-once semantics with checkpointing
- ✅ **Scalable**: Distributed processing across cluster
- ✅ **Low Latency**: Micro-batch or continuous processing
- ✅ **Event Time**: Handle late data with watermarks
- ✅ **Stateful Operations**: Maintain state across batches

---

## File Structure

```
streaming/
├── __init__.py                    # Package initialization
├── README.md                      # This file
├── 01_socket_streaming.py         # Socket source basics
├── 02_file_streaming.py           # File sources (CSV, JSON, Parquet)
├── 03_kafka_streaming.py          # Kafka integration
├── 04_windowing.py                # Time-based windows
├── 05_watermarking.py             # Late data handling
├── 06_stateful_operations.py     # MapGroupsWithState
├── 07_foreach_sink.py             # Custom output with foreachBatch
├── 08_stream_stream_join.py       # Joining two streams
├── 09_trigger_modes.py            # Different execution modes
└── 10_monitoring.py               # Query monitoring and metrics
```

---

## Quick Start

### 1. Basic Word Count (Socket)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read from socket
lines = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Count words
words = lines.select(explode(split("value", " ")).alias("word"))
word_counts = words.groupBy("word").count()

# Write to console
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
```

### 2. File Streaming (CSV)

```python
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("value", DoubleType())
])

df = spark.readStream \
    .format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("input/")

query = df.writeStream \
    .format("parquet") \
    .option("path", "output/") \
    .option("checkpointLocation", "checkpoint/") \
    .start()
```

---

## Core Concepts

### Sources

Where streaming data comes from:

| Source | Description | Use Case |
|--------|-------------|----------|
| **Socket** | TCP socket | Testing, debugging |
| **File** | CSV, JSON, Parquet, Text | File-based ingestion |
| **Kafka** | Apache Kafka topics | Production streaming |
| **Rate** | Synthetic data generation | Performance testing |
| **Memory** | In-memory table | Testing |

### Sinks

Where streaming data is written:

| Sink | Description | Output Modes |
|------|-------------|--------------|
| **Console** | Print to console | Append, Complete, Update |
| **File** | Parquet, JSON, CSV, ORC | Append |
| **Kafka** | Write to Kafka topic | Append, Complete, Update |
| **Memory** | In-memory table | Append, Complete |
| **ForeachBatch** | Custom processing | All modes |
| **Foreach** | Per-row processing | Append |

### Output Modes

How results are written:

```python
# Append: Only new rows (default)
.outputMode("append")

# Complete: Entire result table
.outputMode("complete")  # Only for aggregations

# Update: Only updated rows
.outputMode("update")  # For aggregations
```

### Trigger Modes

When to execute micro-batches:

```python
# Default: As fast as possible
.trigger(processingTime="0 seconds")

# Fixed interval
.trigger(processingTime="10 seconds")

# Once: Single batch then stop
.trigger(once=True)

# Continuous: Low-latency (experimental)
.trigger(continuous="1 second")
```

---

## Examples Summary

### 01_socket_streaming.py

**Basic socket streaming with netcat**

```bash
# Terminal 1: Start netcat
nc -lk 9999

# Terminal 2: Run script
python 01_socket_streaming.py

# Terminal 1: Type words
hello world spark streaming
```

**What you'll learn:**
- Reading from socket source
- Word count aggregation
- Console sink
- Query management (start, stop)

---

### 02_file_streaming.py

**File-based streaming (CSV, JSON, Parquet)**

```python
# Read CSV stream
df = spark.readStream.format("csv").schema(schema).load("input/")

# Write to Parquet
df.writeStream \
    .format("parquet") \
    .option("path", "output/") \
    .option("checkpointLocation", "checkpoint/") \
    .start()
```

**What you'll learn:**
- File sources (CSV, JSON, Parquet)
- Schema definition
- Checkpoint management
- MaxFilesPerTrigger option

---

### 03_kafka_streaming.py

**Kafka integration (read/write)**

```python
# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input_topic") \
    .load()

# Process and write back to Kafka
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output_topic") \
    .option("checkpointLocation", "checkpoint/") \
    .start()
```

**What you'll learn:**
- Kafka source configuration
- Reading/writing Kafka messages
- Key-value handling
- Offset management

**Setup Kafka:**
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka
bin/kafka-server-start.sh config/server.properties

# Create topic
bin/kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
```

---

### 04_windowing.py

**Time-based windows and aggregations**

```python
from pyspark.sql.functions import window

# Tumbling window (5 minutes)
windowed_counts = df \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id")
    ) \
    .count()

# Sliding window (10 min window, 5 min slide)
sliding_counts = df \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("user_id")
    ) \
    .count()
```

**Window Types:**

1. **Tumbling Window**: Non-overlapping, fixed-size
   ```
   [0-5min] [5-10min] [10-15min]
   ```

2. **Sliding Window**: Overlapping
   ```
   [0-10min]
        [5-15min]
             [10-20min]
   ```

3. **Session Window**: Based on inactivity gap
   ```python
   from pyspark.sql.functions import session_window
   session_window(col("timestamp"), "5 minutes")
   ```

**What you'll learn:**
- Tumbling windows
- Sliding windows
- Session windows
- Event-time processing

---

### 05_watermarking.py

**Late data handling with watermarks**

```python
# Define watermark (10 minutes late data tolerance)
df_with_watermark = df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id")
    ) \
    .count()
```

**How Watermarks Work:**

```
Current max event time: 12:15
Watermark: 10 minutes
Threshold: 12:05

Events with timestamp < 12:05 are dropped (too late)
Events >= 12:05 are processed
```

**What you'll learn:**
- Watermark definition
- Late data handling
- State cleanup
- Allowed lateness

---

### 06_stateful_operations.py

**Custom stateful processing**

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout

def update_state(key, values, state):
    """Custom state update logic."""
    if state.exists:
        old_count = state.get
    else:
        old_count = 0
    
    new_count = old_count + sum(values)
    state.update(new_count)
    
    return (key, new_count)

# Apply stateful operation
df.groupByKey(lambda x: x.user_id) \
    .mapGroupsWithState(update_state, ...) \
    .start()
```

**What you'll learn:**
- MapGroupsWithState
- FlatMapGroupsWithState
- State management
- Timeouts (ProcessingTime, EventTime)

---

### 07_foreach_sink.py

**Custom output processing**

```python
def process_batch(batch_df, batch_id):
    """Process each micro-batch."""
    print(f"Processing batch {batch_id}")
    
    # Custom logic
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/db") \
        .option("dbtable", "results") \
        .mode("append") \
        .save()

# Use foreachBatch
query = df.writeStream \
    .foreachBatch(process_batch) \
    .start()
```

**Use Cases:**
- Write to multiple sinks
- Custom business logic
- Database updates
- External API calls

**What you'll learn:**
- foreachBatch sink
- foreach (per-row) sink
- Custom output logic
- Error handling in sinks

---

### 08_stream_stream_join.py

**Joining two streams**

```python
# Stream 1: User clicks
clicks = spark.readStream.format("kafka") \
    .option("subscribe", "clicks") \
    .load()

# Stream 2: User purchases
purchases = spark.readStream.format("kafka") \
    .option("subscribe", "purchases") \
    .load()

# Join with watermarks
joined = clicks \
    .withWatermark("click_time", "10 minutes") \
    .join(
        purchases.withWatermark("purchase_time", "10 minutes"),
        expr("user_id = user_id AND purchase_time >= click_time AND purchase_time <= click_time + interval 1 hour"),
        "inner"
    )
```

**Join Types:**
- Inner join
- Outer join (left, right, full)
- Time-bounded joins

**What you'll learn:**
- Stream-stream joins
- Watermarks for joins
- Time constraints
- State management for joins

---

### 09_trigger_modes.py

**Different execution triggers**

```python
# Micro-batch (default)
.trigger(processingTime="5 seconds")

# One-time batch
.trigger(once=True)

# Continuous (low-latency, experimental)
.trigger(continuous="1 second")

# Available-now (Spark 3.3+)
.trigger(availableNow=True)
```

**What you'll learn:**
- Trigger modes
- Processing time triggers
- Once trigger for batch-like streaming
- Continuous processing

---

### 10_monitoring.py

**Query monitoring and metrics**

```python
# Get active queries
active_queries = spark.streams.active

# Query status
query.status

# Recent progress
query.recentProgress

# Last progress
query.lastProgress

# Stop query
query.stop()

# Check if query is active
query.isActive
```

**Monitoring Metrics:**
- Input rate
- Processing rate
- Batch duration
- Number of input rows
- State store metrics

**What you'll learn:**
- StreamingQuery API
- Progress reporting
- Performance metrics
- Query lifecycle management

---

## Common Patterns

### Pattern 1: ETL Pipeline

```python
# Read from Kafka
raw_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_data") \
    .load()

# Transform
from pyspark.sql.functions import from_json, col

schema = StructType([...])
transformed = raw_data \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .filter(col("status") == "active") \
    .withColumn("processed_time", current_timestamp())

# Load to warehouse
query = transformed.writeStream \
    .format("parquet") \
    .option("path", "s3://bucket/data/") \
    .option("checkpointLocation", "s3://bucket/checkpoint/") \
    .partitionBy("date") \
    .start()
```

### Pattern 2: Real-Time Aggregation

```python
# Streaming aggregation with watermark
aggregated = df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("category")
    ) \
    .agg(
        count("*").alias("count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    )

# Write to database
query = aggregated.writeStream \
    .foreachBatch(lambda batch_df, id: batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/db") \
        .option("dbtable", "aggregations") \
        .mode("append") \
        .save()
    ) \
    .start()
```

### Pattern 3: Deduplication

```python
# Deduplicate based on key with watermark
deduplicated = df \
    .withWatermark("timestamp", "10 minutes") \
    .dropDuplicates(["user_id", "event_type"])
```

### Pattern 4: Multiple Outputs

```python
def write_to_multiple_sinks(batch_df, batch_id):
    # Write to S3
    batch_df.write.parquet(f"s3://bucket/data/batch_{batch_id}")
    
    # Write to database
    batch_df.write.jdbc(...)
    
    # Send alerts
    alerts = batch_df.filter(col("priority") == "HIGH")
    send_alerts(alerts)

query = df.writeStream.foreachBatch(write_to_multiple_sinks).start()
```

---

## Best Practices

### 1. Always Use Checkpoints

```python
.option("checkpointLocation", "/path/to/checkpoint")
```

**Why?**
- Fault tolerance
- Exactly-once processing
- State recovery
- Progress tracking

### 2. Define Schema Explicitly

```python
# Good
schema = StructType([...])
df = spark.readStream.schema(schema).json("input/")

# Avoid (slow)
df = spark.readStream.json("input/")  # Schema inference
```

### 3. Use Watermarks for Event-Time Processing

```python
df.withWatermark("timestamp", "10 minutes")
```

### 4. Set MaxFilesPerTrigger

```python
.option("maxFilesPerTrigger", 10)
```

Prevents processing too many files at once.

### 5. Monitor Your Queries

```python
# Check progress
print(query.lastProgress)

# Check if running
if query.isActive:
    print(f"Status: {query.status}")
```

### 6. Handle Schema Evolution

```python
.option("mergeSchema", "true")
```

### 7. Use Appropriate Trigger Intervals

```python
# Balance latency vs throughput
.trigger(processingTime="10 seconds")
```

### 8. Partition Output Data

```python
.partitionBy("date", "hour")
```

### 9. Set Shuffle Partitions

```python
spark.conf.set("spark.sql.shuffle.partitions", 200)
```

### 10. Clean Up Checkpoints

Periodically remove old checkpoint directories.

---

## Troubleshooting

### Issue 1: Query Not Processing Data

**Symptoms:** Query starts but no data appears

**Solutions:**
```python
# Check if stream is detected
df.isStreaming  # Should be True

# Verify source
df.printSchema()

# Check query status
query.status
```

### Issue 2: Out of Memory

**Symptoms:** Executor OOM errors

**Solutions:**
```python
# Reduce batch size
.option("maxFilesPerTrigger", 1)

# Increase executor memory
spark.conf.set("spark.executor.memory", "4g")

# Use watermarks to limit state
.withWatermark("timestamp", "1 hour")
```

### Issue 3: Slow Processing

**Symptoms:** Batches taking too long

**Solutions:**
```python
# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", 400)

# Repartition input
df.repartition(200)

# Use predicate pushdown
df.filter(...)  # Before groupBy
```

### Issue 4: Late Data Not Processed

**Symptoms:** Some events missing from results

**Solutions:**
```python
# Increase watermark delay
.withWatermark("timestamp", "20 minutes")

# Check allowed lateness
# Events older than watermark are dropped
```

### Issue 5: Checkpoint Errors

**Symptoms:** "Incompatible checkpoint" errors

**Solutions:**
```bash
# Delete old checkpoint (development only!)
rm -rf /path/to/checkpoint

# Or use new checkpoint location
.option("checkpointLocation", "/new/path")
```

---

## Performance Tips

### 1. Optimize Shuffle Operations

```python
# Coalesce before shuffle
df.coalesce(10).groupBy("key").count()

# Use broadcast for small tables
df.join(broadcast(small_df), "id")
```

### 2. Use Appropriate File Formats

```python
# Parquet for analytics (columnar)
.format("parquet")

# JSON for flexibility (row-based)
.format("json")
```

### 3. Limit State Size

```python
# Use watermarks
.withWatermark("timestamp", "1 hour")

# Avoid unbounded state
# Bad: .groupBy("key").count()  # Grows forever
# Good: Use windows
.groupBy(window("timestamp", "1 hour"), "key").count()
```

### 4. Tune Kafka Consumer

```python
.option("maxOffsetsPerTrigger", 10000)
.option("startingOffsets", "latest")
.option("failOnDataLoss", "false")
```

---

## Running Examples

### Prerequisites

```bash
# Install PySpark
pip install pyspark

# For Kafka examples
pip install kafka-python

# For testing
pip install pandas pyarrow
```

### Run Basic Example

```bash
# Socket streaming
python src/streaming/01_socket_streaming.py

# File streaming
python src/streaming/02_file_streaming.py
```

### With Spark Submit

```bash
spark-submit \
    --master local[*] \
    --conf spark.sql.shuffle.partitions=10 \
    src/streaming/01_socket_streaming.py
```

---

## Resources

### Official Documentation
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

### Tutorials
- Databricks Structured Streaming Tutorial
- Apache Spark Streaming Examples

### Books
- "Spark: The Definitive Guide" - Chapter on Structured Streaming
- "Learning Spark" - Streaming Chapter

---

## Next Steps

1. ✅ Run basic examples (socket, file)
2. ✅ Set up Kafka for production examples
3. ✅ Practice windowing and watermarking
4. ✅ Build end-to-end streaming pipeline
5. ✅ Monitor and optimize performance
6. ✅ Deploy to production cluster

---

## Summary

**Key Takeaways:**
- Structured Streaming uses DataFrame API
- Always use checkpoints for fault tolerance
- Watermarks handle late data
- Multiple trigger modes for different use cases
- Monitor queries for performance
- Test with small datasets first

**When to Use Streaming:**
- Real-time analytics
- ETL pipelines
- Event processing
- Monitoring and alerting
- IoT data processing

🚀 **Ready to build streaming applications with PySpark!**
