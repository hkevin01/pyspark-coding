#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
KAFKA STREAMING - Real-Time Data Processing with Apache Kafka
================================================================================

MODULE OVERVIEW:
----------------
Apache Kafka is the industry-standard distributed streaming platform for
building real-time data pipelines and streaming applications. This module
demonstrates PySpark Structured Streaming integration with Kafka for:

• Real-time event processing
• Exactly-once semantics
• Stateful stream processing
• Window operations on streams
• Join streaming data with batch data
• Stream-to-stream joins
• Checkpointing and fault tolerance

WHY KAFKA + PYSPARK:
--------------------
┌─────────────────────────────────────────────────────────────────┐
│                    KAFKA + PYSPARK ARCHITECTURE                 │
│                                                                 │
│  Producers          Kafka Cluster         PySpark Consumers    │
│  ┌────────┐        ┌────────────┐        ┌────────────┐       │
│  │IoT     │───────▶│  Topic 1   │───────▶│ Streaming  │       │
│  │Sensors │        │  (events)  │        │ Queries    │       │
│  └────────┘        └────────────┘        └────────────┘       │
│  ┌────────┐        ┌────────────┐             │               │
│  │Web Apps│───────▶│  Topic 2   │───────▶     │               │
│  │Logs    │        │  (clicks)  │        ┌────▼────────┐      │
│  └────────┘        └────────────┘        │Aggregations │      │
│  ┌────────┐        ┌────────────┐        │Joins        │      │
│  │APIs    │───────▶│  Topic 3   │───────▶│Windowing    │      │
│  │Events  │        │  (payments)│        └─────────────┘      │
│  └────────┘        └────────────┘              │               │
│                                           ┌─────▼──────┐       │
│                                           │  Outputs   │       │
│                                           │ (Console,  │       │
│                                           │  Kafka,    │       │
│                                           │  Parquet)  │       │
│                                           └────────────┘       │
└─────────────────────────────────────────────────────────────────┘

KAFKA FUNDAMENTALS:
-------------------

Topics & Partitions:
┌──────────────────────────────────────────────────────────────┐
│  Topic: "user-events" (3 partitions)                         │
│  ┌────────────────┬────────────────┬────────────────┐        │
│  │  Partition 0   │  Partition 1   │  Partition 2   │        │
│  │ [msg0, msg3,   │ [msg1, msg4,   │ [msg2, msg5,   │        │
│  │  msg6, ...]    │  msg7, ...]    │  msg8, ...]    │        │
│  └────────────────┴────────────────┴────────────────┘        │
│                                                               │
│  • Messages distributed by key hash                          │
│  • Same key → Same partition (ordering guaranteed)           │
│  • Multiple partitions = Parallelism                         │
└──────────────────────────────────────────────────────────────┘

Kafka Guarantees:
• At-least-once delivery (default)
• Exactly-once semantics (with idempotent producer)
• Message ordering within partition
• Distributed, replicated, fault-tolerant

PYSPARK STRUCTURED STREAMING:
------------------------------

Stream Processing Model:
┌──────────────────────────────────────────────────────────────┐
│  Source (Kafka) → Transformations → Sink (Output)            │
│                                                               │
│  1. Read Stream:                                             │
│     spark.readStream.format("kafka")                         │
│                                                               │
│  2. Transform:                                               │
│     • select, filter, map, flatMap                           │
│     • groupBy, agg (with watermarks)                         │
│     • join (stream-stream, stream-batch)                     │
│     • window operations                                      │
│                                                               │
│  3. Write Stream:                                            │
│     .writeStream.format("kafka")                             │
│     .outputMode("append|complete|update")                    │
│     .option("checkpointLocation", "/path")                   │
│     .start()                                                 │
└──────────────────────────────────────────────────────────────┘

STREAMING CONCEPTS:
-------------------

1. Triggers:
   • ProcessingTime: Micro-batches at fixed intervals
   • Continuous: Low-latency continuous processing
   • AvailableNow: Process available data and stop

2. Output Modes:
   • Append: Only new rows (immutable)
   • Complete: Entire result table (aggregations)
   • Update: Only updated rows (stateful operations)

3. Watermarks:
   • Handle late data
   • Define "how late is too late"
   • Enable windowed aggregations with time bounds

4. Checkpointing:
   • Stores stream metadata & offsets
   • Enables fault tolerance
   • Required for production
   • Resume from failure point

REAL-WORLD USE CASES:
---------------------

1. IoT Sensor Monitoring:
   • Millions of sensors → Kafka topics
   • Real-time anomaly detection
   • Aggregated metrics dashboards

2. Click Stream Analysis:
   • Web events → Kafka
   • Session analytics
   • Real-time recommendations

3. Financial Transactions:
   • Payment events → Kafka
   • Fraud detection
   • Real-time risk scoring

4. Log Aggregation:
   • Application logs → Kafka
   • Real-time error alerting
   • Security monitoring

5. Supply Chain Tracking:
   • GPS/RFID data → Kafka
   • Real-time inventory
   • Delivery predictions

PERFORMANCE CONSIDERATIONS:
---------------------------

Kafka:
• Partition count = Parallelism
• Consumer group for load balancing
• Replication factor for fault tolerance
• Compression (gzip, snappy, lz4)

PySpark:
• Trigger interval vs throughput
• Checkpoint location (HDFS recommended)
• State store size management
• Shuffle partitions tuning

DEPENDENCIES:
-------------
pip install kafka-python pyspark

Spark packages:
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - Kafka Streaming Guide
UPDATED: 2024
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window, count, sum as _sum, avg,
    current_timestamp, expr, lit, concat, explode, split, regexp_extract
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
import time


def create_kafka_spark_session():
    """
    Create SparkSession with Kafka support.
    
    NOTE: Requires spark-sql-kafka package:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
    """
    print("=" * 80)
    print("CREATING KAFKA-ENABLED SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("KafkaStreaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark {spark.version} with Kafka support")
    print(f"✅ Streaming checkpoint: /tmp/checkpoint")
    print(f"✅ Ready to process Kafka streams")
    
    return spark


def example_1_basic_kafka_read(spark):
    """
    Example 1: Basic Kafka stream reading.
    
    Reads from Kafka topic and displays to console.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: BASIC KAFKA READ")
    print("=" * 80)
    
    print("""
📖 Reading from Kafka topic: "user-events"

Configuration:
• kafka.bootstrap.servers: localhost:9092
• subscribe: user-events
• startingOffsets: earliest (read from beginning)

Message Format:
• key: user_id (binary)
• value: JSON event data (binary)
• timestamp: Event timestamp
• partition: Kafka partition
• offset: Message offset in partition
    """)
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("\n📊 Kafka DataFrame Schema:")
    kafka_df.printSchema()
    
    print("""
Kafka Record Structure:
┌────────────┬─────────────────────────────────────────────┐
│ Column     │ Description                                 │
├────────────┼─────────────────────────────────────────────┤
│ key        │ Message key (binary)                        │
│ value      │ Message payload (binary)                    │
│ topic      │ Topic name                                  │
│ partition  │ Partition number (0 to N-1)                 │
│ offset     │ Message offset within partition             │
│ timestamp  │ Message timestamp                           │
│ timestamp  │ Type of timestamp (CreateTime/LogAppendTime)│
│ Type       │                                             │
└────────────┴─────────────────────────────────────────────┘

💡 Important: key and value are BINARY!
   Must cast to string and parse JSON
    """)
    
    # Convert binary to string
    events_df = kafka_df.select(
        col("key").cast("string").alias("user_id"),
        col("value").cast("string").alias("event_json"),
        col("timestamp"),
        col("partition"),
        col("offset")
    )
    
    print("\n▶️  Starting stream query (press Ctrl+C to stop)...")
    print("   (This is a simulated example - requires running Kafka cluster)")
    
    # This would start the actual stream
    # query = events_df.writeStream \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .outputMode("append") \
    #     .trigger(processingTime="5 seconds") \
    #     .start()
    # 
    # query.awaitTermination(30)  # Run for 30 seconds
    
    print("✅ Basic Kafka read example complete")


def example_2_json_parsing(spark):
    """
    Example 2: Parse JSON from Kafka messages.
    
    Demonstrates:
    • JSON schema definition
    • from_json() function
    • Extracting nested fields
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: JSON PARSING FROM KAFKA")
    print("=" * 80)
    
    print("""
📖 Parsing JSON events from Kafka

Sample Kafka Message:
{
  "user_id": "user_12345",
  "event_type": "page_view",
  "page": "/products/laptop",
  "timestamp": "2024-12-13T10:30:00Z",
  "session_id": "sess_xyz",
  "device": "mobile",
  "country": "US"
}
    """)
    
    # Define JSON schema
    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True)
    ])
    
    print("\n📋 Defined JSON Schema:")
    print(event_schema)
    
    # Simulated Kafka stream
    kafka_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", "10") \
        .load()
    
    # Simulate Kafka structure
    kafka_simulated = kafka_df.select(
        col("value").cast("string").alias("user_id"),
        to_json(struct(
            lit("user_12345").alias("user_id"),
            lit("page_view").alias("event_type"),
            lit("/products/laptop").alias("page"),
            current_timestamp().alias("timestamp"),
            lit("sess_xyz").alias("session_id"),
            lit("mobile").alias("device"),
            lit("US").alias("country")
        )).alias("value")
    )
    
    # Parse JSON
    parsed_events = kafka_simulated.select(
        col("user_id"),
        from_json(col("value"), event_schema).alias("event")
    ).select("user_id", "event.*")
    
    print("\n📊 Parsed Events Schema:")
    parsed_events.printSchema()
    
    print("""
✅ JSON Parsing Benefits:
• Type safety (schema validation)
• Nested field access
• Null handling
• Performance optimization (predicate pushdown)

💡 Best Practices:
• Define schema explicitly (faster than inferSchema)
• Use SELECT to extract only needed fields
• Handle malformed JSON with try-catch or options
    """)
    
    print("\n▶️  Sample output (simulated):")
    # query = parsed_events.writeStream \
    #     .format("console") \
    #     .outputMode("append") \
    #     .trigger(processingTime="5 seconds") \
    #     .start()
    
    print("✅ JSON parsing example complete")


def example_3_windowed_aggregations(spark):
    """
    Example 3: Windowed aggregations on streaming data.
    
    Demonstrates:
    • Tumbling windows
    • Sliding windows
    • Watermarks for late data
    • Stateful aggregations
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: WINDOWED AGGREGATIONS")
    print("=" * 80)
    
    print("""
📖 Aggregating events over time windows

Window Types:
┌──────────────────────────────────────────────────────────────┐
│  TUMBLING WINDOW (10 minutes, non-overlapping)              │
│  ├─────────┼─────────┼─────────┼─────────┼─────────┤        │
│  10:00    10:10    10:20    10:30    10:40    10:50         │
│                                                               │
│  SLIDING WINDOW (10 min window, 5 min slide, overlapping)   │
│  ├─────────┤                                                 │
│      ├─────────┤                                             │
│          ├─────────┤                                         │
│  10:00  10:05  10:10  10:15  10:20                          │
└──────────────────────────────────────────────────────────────┘

Watermark:
• Threshold for "how late is too late"
• Example: "10 minutes" → drop data >10 min late
• Enables state cleanup (prevents unbounded state)
    """)
    
    # Simulated event stream with timestamps
    event_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", "100") \
        .load() \
        .selectExpr(
            "value as event_id",
            "timestamp",
            "CAST((value % 5) AS STRING) as event_type",
            "CAST((value % 10) AS STRING) as country"
        )
    
    # Tumbling window aggregation (10 seconds)
    print("\n🕐 Tumbling Window Aggregation (10 seconds):")
    tumbling_agg = event_stream \
        .withWatermark("timestamp", "30 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            col("event_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            expr("min(timestamp) as window_start"),
            expr("max(timestamp) as window_end")
        )
    
    print("""
   • Each 10-second bucket counted separately
   • Watermark: 30 seconds (handle late data up to 30s)
   • Output: event counts per 10-second window
    """)
    
    # Sliding window aggregation (10s window, 5s slide)
    print("\n�� Sliding Window Aggregation (10s window, 5s slide):")
    sliding_agg = event_stream \
        .withWatermark("timestamp", "30 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds", "5 seconds"),
            col("country")
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("event_id").alias("avg_event_id")
        )
    
    print("""
   • Windows overlap (more granular view)
   • 10s window slides every 5s
   • Output: event counts for overlapping windows
    """)
    
    print("""
✅ Windowed Aggregation Benefits:
• Real-time metrics (events/minute, transactions/hour)
• Trend analysis
• Anomaly detection
• Session analytics

💡 Watermark Tuning:
• Too short: Drop valid late data
• Too long: Excessive state size
• Balance: Business latency requirements vs. resource usage

Example Metrics:
• Page views per minute
• Transactions per hour by region
• Error rate in 5-minute windows
• Active sessions in rolling 30-minute window
    """)
    
    print("✅ Windowed aggregation example complete")


def example_4_stream_to_stream_join(spark):
    """
    Example 4: Join two Kafka streams.
    
    Demonstrates:
    • Stream-to-stream joins
    • Join with watermarks
    • Time-bounded joins
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: STREAM-TO-STREAM JOIN")
    print("=" * 80)
    
    print("""
📖 Joining two Kafka streams: Clicks + Purchases

Use Case:
• Stream 1: User clicks (browsing behavior)
• Stream 2: User purchases (transactions)
• Goal: Correlate clicks to purchases

Time-Bounded Join:
┌──────────────────────────────────────────────────────────────┐
│  Clicks Stream:     [C1] [C2] [C3] [C4] [C5] [C6]            │
│  10:00             10:05 10:10 10:15 10:20 10:25             │
│                                                               │
│  Purchases Stream:      [P1] [P2]     [P3]                   │
│  10:00                 10:08 10:12    10:22                  │
│                                                               │
│  Join Window: 15 minutes                                     │
│  • C2 joins with P1 (3 min apart)                            │
│  • C3 joins with P2 (2 min apart)                            │
│  • C5 joins with P3 (2 min apart)                            │
└──────────────────────────────────────────────────────────────┘

Why Time-Bounded?
• Prevents unbounded state growth
• Defines "relevance window"
• Watermarks enable state cleanup
    """)
    
    # Simulate clicks stream
    clicks = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", "10") \
        .load() \
        .selectExpr(
            "CAST((value % 100) AS STRING) as user_id",
            "timestamp as click_time",
            "value as click_id"
        )
    
    # Simulate purchases stream
    purchases = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", "2") \
        .load() \
        .selectExpr(
            "CAST((value % 100) AS STRING) as user_id",
            "timestamp as purchase_time",
            "value as purchase_id",
            "CAST((value * 10) AS DOUBLE) as amount"
        )
    
    print("\n🔗 Stream-to-Stream Join Configuration:")
    print("   • Join key: user_id")
    print("   • Join type: inner")
    print("   • Time bound: 15 minutes")
    print("   • Watermark: 30 seconds")
    
    # Apply watermarks
    clicks_with_watermark = clicks.withWatermark("click_time", "30 seconds")
    purchases_with_watermark = purchases.withWatermark("purchase_time", "30 seconds")
    
    # Time-bounded join
    joined = clicks_with_watermark.join(
        purchases_with_watermark,
        expr("""
            user_id = user_id AND
            purchase_time >= click_time AND
            purchase_time <= click_time + interval 15 minutes
        """)
    )
    
    result = joined.select(
        "user_id",
        "click_time",
        "purchase_time",
        "purchase_id",
        "amount",
        expr("(unix_timestamp(purchase_time) - unix_timestamp(click_time)) / 60 as minutes_to_purchase")
    )
    
    print("""
✅ Stream-to-Stream Join Benefits:
• Attribution analysis (clicks → conversions)
• Funnel analytics
• Session stitching
• Real-time recommendations

💡 Performance Tips:
• Always use watermarks (required for stateful joins)
• Keep join window as small as reasonable
• Monitor state size in Spark UI
• Use checkpoint location on distributed storage (HDFS/S3)

Real-World Examples:
• Ad clicks + purchases (attribution)
• Login events + activity logs (session analysis)
• Sensor readings + alerts (anomaly correlation)
    """)
    
    print("✅ Stream-to-stream join example complete")


def example_5_exactly_once_semantics(spark):
    """
    Example 5: Exactly-once processing with Kafka.
    
    Demonstrates:
    • Idempotent writes
    • Checkpointing
    • Kafka transactions
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: EXACTLY-ONCE SEMANTICS")
    print("=" * 80)
    
    print("""
📖 Guaranteeing exactly-once processing

Delivery Semantics:
┌──────────────────────────────────────────────────────────────┐
│ 1. AT-MOST-ONCE:                                             │
│    • Fastest, but data loss possible                         │
│    • Use: Non-critical logs, metrics                         │
│                                                               │
│ 2. AT-LEAST-ONCE:                                            │
│    • No data loss, but duplicates possible                   │
│    • Use: Most streaming applications (with deduplication)   │
│                                                               │
│ 3. EXACTLY-ONCE:                                             │
│    • No data loss, no duplicates                             │
│    • Use: Financial transactions, critical events            │
│    • Requires: Idempotent sinks, checkpointing, transactions │
└──────────────────────────────────────────────────────────────┘

Exactly-Once Requirements:
1. Checkpointing (offset tracking)
2. Idempotent sink (same input → same output)
3. Kafka transactions (for Kafka sink)
4. Spark streaming state management
    """)
    
    print("""
🔧 Checkpoint Configuration:

.writeStream \\
  .format("kafka") \\
  .option("kafka.bootstrap.servers", "localhost:9092") \\
  .option("topic", "output-topic") \\
  .option("checkpointLocation", "/hdfs/checkpoints/app1") \\
  .option("kafka.transactional.id", "app1-txn") \\  # Transactions
  .outputMode("append") \\
  .start()

Checkpoint Directory Structure:
/hdfs/checkpoints/app1/
├── commits/          # Batch commit metadata
├── metadata          # Stream metadata
├── offsets/          # Kafka offsets per batch
├── sources/          # Source information
└── state/            # Stateful operation state

💡 Checkpoint Best Practices:
• Use distributed storage (HDFS, S3, not local disk)
• Never change query logic without new checkpoint location
• Monitor checkpoint size
• Clean up old checkpoints periodically
    """)
    
    print("""
✅ Exactly-Once Use Cases:
• Payment processing
• Account balance updates
• Order fulfillment
• Inventory management
• Financial transactions

⚠️  Caveats:
• Requires compatible sinks (not all support transactions)
• Performance overhead vs. at-least-once
• Complexity in error handling
• State management for large windows
    """)
    
    print("✅ Exactly-once semantics example complete")


def example_6_production_monitoring(spark):
    """
    Example 6: Production monitoring and best practices.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 6: PRODUCTION MONITORING & BEST PRACTICES")
    print("=" * 80)
    
    print("""
📊 Monitoring Kafka Streaming Applications

Key Metrics to Monitor:
┌────────────────────────────┬─────────────────────────────────┐
│ Metric                     │ What to Watch                   │
├────────────────────────────┼─────────────────────────────────┤
│ Processing Rate            │ Events/second throughput        │
│ Batch Duration             │ Time to process each batch      │
│ Input Rate                 │ Incoming events/second          │
│ Scheduling Delay           │ Backlog indicator               │
│ End-to-End Latency         │ Event → Output delay            │
│ State Store Size           │ Stateful operation memory       │
│ Kafka Consumer Lag         │ Offset lag per partition        │
└────────────────────────────┴─────────────────────────────────┘

Accessing Metrics in Spark UI:
• Navigate to: http://driver:4040/StreamingQuery/
• View: Progress reports, batch statistics, watermarks

Programmatic Monitoring:
    """)
    
    # Simulated query for monitoring example
    print("""
query = events_df.writeStream \\
    .format("console") \\
    .start()

# Monitor progress
while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"Batch: {progress['batchId']}")
        print(f"Input Rows: {progress['numInputRows']}")
        print(f"Processing Time: {progress['durationMs']['triggerExecution']}ms")
        print(f"Input Rate: {progress.get('inputRowsPerSecond', 0)} rows/sec")
        print(f"Process Rate: {progress.get('processedRowsPerSecond', 0)} rows/sec")
    
    time.sleep(10)
    """)
    
    print("""
🚨 Common Issues & Solutions:

1. Consumer Lag Growing:
   • Input rate > Processing rate
   • Solution: Scale up executors, optimize transformations
   
2. Out of Memory (State):
   • Stateful aggregations with no watermark
   • Solution: Add watermarks, tune state timeout
   
3. Slow Batches:
   • Complex transformations, skewed data
   • Solution: Optimize queries, add salting
   
4. Checkpoint Corruption:
   • Job restarted with incompatible code
   • Solution: Use new checkpoint location
   
5. Exactly-Once Failures:
   • Sink doesn't support transactions
   • Solution: Use compatible sink or implement idempotency

💡 Best Practices:

Performance:
✅ Set appropriate trigger interval (balance latency vs. throughput)
✅ Use Kafka partition count = Spark parallelism
✅ Enable compression in Kafka
✅ Tune fetch.min.bytes and fetch.max.wait.ms
✅ Use Kryo serialization

Reliability:
✅ Always use checkpointing in production
✅ Add watermarks for stateful operations
✅ Monitor consumer lag
✅ Set up alerts for backlog
✅ Test failure recovery

Security:
✅ Enable Kafka SSL/TLS
✅ Use SASL authentication
✅ Encrypt checkpoint location
✅ Rotate credentials regularly

Scalability:
✅ Partition Kafka topics appropriately (10-100 per node)
✅ Use dynamic allocation if workload varies
✅ Archive old checkpoints
✅ Clean up state periodically
    """)
    
    print("✅ Production monitoring example complete")


def kafka_setup_guide():
    """
    Print Kafka setup guide.
    """
    print("\n" + "=" * 80)
    print("KAFKA SETUP GUIDE")
    print("=" * 80)
    
    print("""
📦 1. Install Kafka:
──────────────────────────────────────────────────────────────────

# Download Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0


🚀 2. Start Kafka (Local Development):
──────────────────────────────────────────────────────────────────

# Terminal 1: Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka Broker
bin/kafka-server-start.sh config/server.properties


📝 3. Create Topics:
──────────────────────────────────────────────────────────────────

# Create topic with 3 partitions
bin/kafka-topics.sh --create \\
  --bootstrap-server localhost:9092 \\
  --replication-factor 1 \\
  --partitions 3 \\
  --topic user-events

# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
bin/kafka-topics.sh --describe \\
  --bootstrap-server localhost:9092 \\
  --topic user-events


📤 4. Produce Test Messages:
──────────────────────────────────────────────────────────────────

# Console producer (manual testing)
bin/kafka-console-producer.sh \\
  --bootstrap-server localhost:9092 \\
  --topic user-events \\
  --property "parse.key=true" \\
  --property "key.separator=:"

# Then type messages:
user1:{"event":"page_view","page":"/home"}
user2:{"event":"purchase","amount":99.99}


📥 5. Consume Messages:
──────────────────────────────────────────────────────────────────

# Console consumer (verify messages)
bin/kafka-console-consumer.sh \\
  --bootstrap-server localhost:9092 \\
  --topic user-events \\
  --from-beginning \\
  --property print.key=true \\
  --property key.separator=" : "


🐍 6. Python Producer (for testing):
──────────────────────────────────────────────────────────────────

from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send events
for i in range(100):
    event = {
        "user_id": f"user_{i % 10}",
        "event_type": "page_view",
        "timestamp": time.time()
    }
    producer.send('user-events', value=event)
    time.sleep(0.1)

producer.flush()


⚙️  7. Run PySpark with Kafka:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
  03_kafka_streaming.py


🐳 8. Docker Compose (Recommended for Development):
──────────────────────────────────────────────────────────────────

# docker-compose.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

# Start
docker-compose up -d

# Stop
docker-compose down
    """)


def main():
    """
    Main execution function.
    """
    print("\n" + "🔥 " * 40)
    print("KAFKA STREAMING WITH PYSPARK - COMPREHENSIVE GUIDE")
    print("🔥 " * 40)
    
    spark = create_kafka_spark_session()
    
    # Run examples
    example_1_basic_kafka_read(spark)
    example_2_json_parsing(spark)
    example_3_windowed_aggregations(spark)
    example_4_stream_to_stream_join(spark)
    example_5_exactly_once_semantics(spark)
    example_6_production_monitoring(spark)
    
    # Setup guide
    kafka_setup_guide()
    
    print("\n" + "=" * 80)
    print("✅ KAFKA STREAMING EXAMPLES COMPLETE")
    print("=" * 80)
    
    print("""
📚 Key Takeaways:

1. Kafka Integration:
   • readStream.format("kafka") for consuming
   • writeStream.format("kafka") for producing
   • Binary key/value → cast to string and parse JSON

2. Windowed Aggregations:
   • Tumbling windows (non-overlapping)
   • Sliding windows (overlapping)
   • Watermarks for late data handling

3. Stream-to-Stream Joins:
   • Requires watermarks on both streams
   • Time-bounded joins prevent unbounded state
   • Inner, left outer, right outer join support

4. Exactly-Once Semantics:
   • Checkpointing required
   • Kafka transactions for sink
   • Idempotent operations

5. Production Best Practices:
   • Monitor consumer lag
   • Set appropriate trigger intervals
   • Use distributed checkpoint location
   • Add watermarks to stateful operations
   • Test failure recovery

🎯 Next Steps:
   • Start Kafka cluster (local or Docker)
   • Create test topics
   • Run examples with real Kafka
   • Build custom streaming pipeline
   • Deploy to production with monitoring

🔗 Related Files:
   • 01_socket_streaming.py (basic streaming)
   • 02_file_streaming.py (file source)
   • cluster_computing/08_shuffle_optimization.py
   • optimization/01_join_strategies.py
    """)
    
    spark.stop()


if __name__ == "__main__":
    main()
