#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
BASIC KAFKA CONSUMER - Reading from Kafka Streams
================================================================================

📖 OVERVIEW:
This example demonstrates the fundamentals of consuming messages from Apache
Kafka using PySpark Structured Streaming. Learn how to:

• Connect to Kafka brokers
• Subscribe to topics
• Read streaming data
• Parse binary messages
• Display results to console

🎯 USE CASE:
Real-time log aggregation, event processing, or any scenario where you need
to consume and process messages from Kafka topics as they arrive.

📋 PREREQUISITES:
1. Kafka cluster running (local or remote)
2. Topic created with test data
3. spark-sql-kafka package installed

🚀 RUN:
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  01_basic_kafka_consumer.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time


def create_spark_session():
    """
    Create SparkSession with Kafka support.
    """
    print("=" * 80)
    print("🚀 CREATING SPARK SESSION WITH KAFKA SUPPORT")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("BasicKafkaConsumer") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_checkpoint") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark {spark.version} initialized")
    print(f"✅ Checkpoint location: /tmp/kafka_checkpoint")
    print()
    
    return spark


def example_1_read_kafka_raw(spark):
    """
    Example 1: Read raw Kafka messages and display schema.
    
    Shows the default Kafka DataFrame structure with binary key/value.
    """
    print("=" * 80)
    print("📥 EXAMPLE 1: READ RAW KAFKA MESSAGES")
    print("=" * 80)
    
    print("""
🔌 Connecting to Kafka:
   • Bootstrap servers: localhost:9092
   • Topic: user-events
   • Starting from: earliest offset
   • Fail on data loss: false (for demo resilience)

📊 Kafka Message Structure:
   Kafka messages arrive as binary data with metadata:
   - key: Binary message key (optional, for partitioning)
   - value: Binary message payload (your actual data)
   - topic: Source topic name
   - partition: Partition number (0 to N-1)
   - offset: Unique message ID within partition
   - timestamp: Message creation or append time
   - timestampType: CreateTime (0) or LogAppendTime (1)
    """)
    
    try:
        # Read from Kafka
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print("✅ Successfully connected to Kafka topic: user-events")
        print("\n📋 Kafka DataFrame Schema:")
        kafka_stream.printSchema()
        
        print("""
┌─────────────────────────────────────────────────────────────────┐
│ SCHEMA EXPLANATION:                                             │
├─────────────────────────────────────────────────────────────────┤
│ key             : binary (nullable)                             │
│   → Message key, used for partitioning (e.g., user_id)          │
│                                                                 │
│ value           : binary (non-nullable)                         │
│   → Your actual message payload (JSON, Avro, etc.)              │
│                                                                 │
│ topic           : string (non-nullable)                         │
│   → Topic name where message came from                          │
│                                                                 │
│ partition       : integer (non-nullable)                        │
│   → Kafka partition number (determines parallelism)             │
│                                                                 │
│ offset          : long (non-nullable)                           │
│   → Unique sequential ID per partition                          │
│                                                                 │
│ timestamp       : timestamp (non-nullable)                      │
│   → When message was created or appended to log                 │
│                                                                 │
│ timestampType   : integer (non-nullable)                        │
│   → 0 = CreateTime, 1 = LogAppendTime                           │
└─────────────────────────────────────────────────────────────────┘

⚠️  IMPORTANT: key and value are BINARY!
   You must cast them to string or parse them to use the data.
        """)
        
        # Convert binary to string for display
        readable_stream = kafka_stream.select(
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp")
        )
        
        print("\n▶️  Starting streaming query (simulated - requires running Kafka)...")
        print("   Output: First 20 messages from the stream\n")
        
        # In production, you would use:
        # query = readable_stream.writeStream \
        #     .format("console") \
        #     .option("truncate", "false") \
        #     .outputMode("append") \
        #     .trigger(processingTime="5 seconds") \
        #     .start()
        # query.awaitTermination(30)
        
        print("✅ Example 1 complete - raw Kafka message structure demonstrated")
        
    except Exception as e:
        print(f"⚠️  Note: This example requires a running Kafka cluster.")
        print(f"   Error: {str(e)}")
        print(f"   See README.md for Kafka setup instructions.")


def example_2_parse_json_messages(spark):
    """
    Example 2: Parse JSON messages from Kafka.
    
    Demonstrates extracting structured data from JSON payloads.
    """
    print("\n" + "=" * 80)
    print("🔍 EXAMPLE 2: PARSE JSON MESSAGES FROM KAFKA")
    print("=" * 80)
    
    print("""
📖 Scenario: User Events Stream
   Your application sends JSON events to Kafka:
   
   Sample Message Value:
   {
     "user_id": "user_12345",
     "event_type": "page_view",
     "page_url": "/products/laptop",
     "timestamp": "2024-12-15T10:30:00Z",
     "session_id": "sess_abc123",
     "device": "mobile",
     "country": "US",
     "referrer": "google.com"
   }
   
   Goal: Parse this JSON into structured columns
    """)
    
    # Define the JSON schema
    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("referrer", StringType(), True)
    ])
    
    print("\n📋 Defined JSON Schema:")
    for field in event_schema.fields:
        print(f"   • {field.name}: {field.dataType.simpleString()}")
    
    try:
        # Read from Kafka
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON from value column
        parsed_stream = kafka_stream.select(
            col("key").cast("string").alias("message_key"),
            from_json(col("value").cast("string"), event_schema).alias("event_data"),
            col("timestamp").alias("kafka_timestamp"),
            col("partition"),
            col("offset")
        ).select(
            "message_key",
            "event_data.*",
            "kafka_timestamp",
            "partition",
            "offset"
        )
        
        print("\n✅ Parsed DataFrame Schema:")
        parsed_stream.printSchema()
        
        print("""
🎯 Benefits of Schema Definition:
   ✓ Type safety - each field has correct data type
   ✓ Null handling - schema defines nullable fields
   ✓ Performance - Spark optimizes based on schema
   ✓ Validation - malformed JSON handled gracefully
   ✓ Predicate pushdown - filter on specific fields

💡 Best Practices:
   1. Always define schema explicitly (don't use inferSchema in streaming)
   2. Handle malformed JSON with try-catch or corrupt record column
   3. Use appropriate data types (TimestampType for dates, DoubleType for numbers)
   4. Extract only needed fields to reduce processing overhead
        """)
        
        # Filter example
        page_views = parsed_stream.filter(col("event_type") == "page_view")
        
        print("\n📊 Filtered Stream (page_view events only):")
        print("   Ready to process page view analytics...")
        
        print("\n✅ Example 2 complete - JSON parsing demonstrated")
        
    except Exception as e:
        print(f"⚠️  Note: This example requires a running Kafka cluster.")
        print(f"   Error: {str(e)}")


def example_3_multiple_topics(spark):
    """
    Example 3: Subscribe to multiple Kafka topics.
    
    Shows how to consume from multiple topics simultaneously.
    """
    print("\n" + "=" * 80)
    print("📚 EXAMPLE 3: CONSUME FROM MULTIPLE TOPICS")
    print("=" * 80)
    
    print("""
🎯 Use Case: Multi-Source Event Processing
   
   Your system has different event types in separate topics:
   • "user-events" - User actions (clicks, views, searches)
   • "payment-events" - Transaction data
   • "system-logs" - Application logs
   
   You want to process all of them in a single streaming job.

📋 Three Ways to Subscribe:

1. Specific Topics (Comma-Separated):
   .option("subscribe", "topic1,topic2,topic3")
   
2. Topic Pattern (Regex):
   .option("subscribePattern", "events-.*")
   
3. Assign Specific Partitions:
   .option("assign", '{"topic1":[0,1],"topic2":[0]}')
    """)
    
    try:
        # Method 1: Multiple specific topics
        print("\n🔹 Method 1: Subscribe to specific topics")
        multi_topic_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user-events,payment-events,system-logs") \
            .option("startingOffsets", "latest") \
            .load()
        
        print("   ✅ Subscribed to: user-events, payment-events, system-logs")
        
        # Method 2: Pattern-based subscription
        print("\n🔹 Method 2: Subscribe using pattern")
        pattern_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribePattern", "events-.*") \
            .option("startingOffsets", "latest") \
            .load()
        
        print("   ✅ Subscribed to all topics matching: events-.*")
        
        # Route messages based on topic
        print("\n🔀 Route Processing by Topic:")
        routed_stream = multi_topic_stream.select(
            col("topic"),
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
            col("partition"),
            col("offset")
        )
        
        print("""
   
   # Separate processing per topic:
   user_events = routed_stream.filter(col("topic") == "user-events")
   payments = routed_stream.filter(col("topic") == "payment-events")
   logs = routed_stream.filter(col("topic") == "system-logs")
   
   # Then apply topic-specific transformations
        """)
        
        print("""
💡 When to Use Each Method:

Multiple Topics (subscribe):
   ✓ Known, fixed set of topics
   ✓ Different schemas per topic
   ✓ Explicit control

Pattern (subscribePattern):
   ✓ Dynamic topics (new topics added over time)
   ✓ Consistent schema across topics
   ✓ Namespace-based organization (e.g., "prod-*")

Specific Partitions (assign):
   ✓ Need exact partition control
   ✓ Manual partition assignment
   ✓ Advanced use cases only
        """)
        
        print("\n✅ Example 3 complete - multi-topic consumption demonstrated")
        
    except Exception as e:
        print(f"⚠️  Note: This example requires a running Kafka cluster.")
        print(f"   Error: {str(e)}")


def example_4_kafka_options(spark):
    """
    Example 4: Important Kafka consumer options.
    
    Demonstrates key configuration options for production use.
    """
    print("\n" + "=" * 80)
    print("⚙️  EXAMPLE 4: KAFKA CONSUMER OPTIONS")
    print("=" * 80)
    
    print("""
🔧 Essential Kafka Options for PySpark Streaming:

┌─────────────────────────────────────────────────────────────────┐
│ OPTION                          │ PURPOSE                        │
├─────────────────────────────────┼────────────────────────────────┤
│ kafka.bootstrap.servers         │ Kafka broker addresses         │
│ subscribe / subscribePattern    │ Topics to consume              │
│ startingOffsets                 │ Where to start reading         │
│ failOnDataLoss                  │ Handle missing offsets         │
│ maxOffsetsPerTrigger            │ Rate limiting                  │
│ minPartitions                   │ Spark parallelism              │
│ kafka.group.id                  │ Consumer group ID              │
│ kafka.session.timeout.ms        │ Session timeout                │
│ kafka.request.timeout.ms        │ Request timeout                │
│ kafka.max.poll.records          │ Records per poll               │
└─────────────────────────────────────────────────────────────────┘
    """)
    
    # Example with common options
    print("\n📝 Example Configuration:")
    print("""
kafka_stream = spark.readStream \\
    .format("kafka") \\
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092") \\
    .option("subscribe", "high-volume-topic") \\
    .option("startingOffsets", "latest") \\
    .option("failOnDataLoss", "false") \\
    .option("maxOffsetsPerTrigger", "10000") \\
    .option("minPartitions", "10") \\
    .option("kafka.group.id", "pyspark-consumer-group-1") \\
    .option("kafka.session.timeout.ms", "30000") \\
    .option("kafka.request.timeout.ms", "40000") \\
    .option("kafka.max.poll.records", "500") \\
    .option("kafka.isolation.level", "read_committed") \\
    .load()
    """)
    
    print("""
📖 Option Details:

1. startingOffsets:
   • "earliest" - Read from beginning of topic (all historical data)
   • "latest" - Read only new messages (from now on)
   • '{"topic1":{"0":23,"1":-1}}' - Start from specific offsets
   
   Production: Use "latest" to avoid reprocessing old data

2. failOnDataLoss:
   • true (default) - Fail if data lost (strict mode)
   • false - Continue if offsets missing (resilient mode)
   
   Production: Set to "false" for long-running jobs

3. maxOffsetsPerTrigger:
   • Limit records per micro-batch
   • Prevents overwhelming cluster
   • Enables backpressure handling
   
   Example: 10000 = max 10,000 records per batch

4. minPartitions:
   • Minimum Spark partitions (overrides Kafka partitions)
   • Increases parallelism
   
   Rule of thumb: 2-3x number of Kafka partitions

5. kafka.group.id:
   • Consumer group identifier
   • Required for offset tracking
   • Multiple apps with same group.id = load balancing
   
   Production: Use unique, descriptive names

6. kafka.isolation.level:
   • "read_uncommitted" (default) - Read all messages
   • "read_committed" - Only read committed transactions
   
   Use "read_committed" for exactly-once semantics
    """)
    
    print("""
⚡ Performance Tuning:

High Throughput:
   ✓ Increase maxOffsetsPerTrigger (10000-100000)
   ✓ Increase minPartitions
   ✓ Use larger trigger intervals
   ✓ Enable compression

Low Latency:
   ✓ Decrease maxOffsetsPerTrigger (100-1000)
   ✓ Use continuous triggers
   ✓ Reduce kafka.session.timeout.ms
   ✓ More Kafka partitions

Resource-Constrained:
   ✓ Lower maxOffsetsPerTrigger
   ✓ Increase trigger interval
   ✓ Reduce minPartitions
   ✓ Enable data skipping
    """)
    
    print("\n✅ Example 4 complete - Kafka options explained")


def example_5_error_handling(spark):
    """
    Example 5: Error handling and resilience patterns.
    """
    print("\n" + "=" * 80)
    print("🛡️  EXAMPLE 5: ERROR HANDLING & RESILIENCE")
    print("=" * 80)
    
    print("""
🚨 Common Kafka Streaming Errors and Solutions:

1. CONNECTION ERRORS:
   Error: "Failed to resolve Kafka bootstrap servers"
   Cause: Kafka brokers unreachable
   Solution: ✓ Verify kafka.bootstrap.servers
            ✓ Check network connectivity
            ✓ Ensure Kafka is running

2. OFFSET OUT OF RANGE:
   Error: "Offset out of range"
   Cause: Requested offset no longer exists (retention policy)
   Solution: ✓ Set failOnDataLoss=false
            ✓ Use startingOffsets="latest"
            ✓ Adjust Kafka retention settings

3. DESERIALIZATION ERRORS:
   Error: "Failed to parse JSON" / "Cast error"
   Cause: Malformed messages or schema mismatch
   Solution: ✓ Add corrupt record column
            ✓ Use try-catch in UDFs
            ✓ Validate upstream producers

4. CONSUMER LAG:
   Error: Processing falls behind production
   Cause: Insufficient throughput
   Solution: ✓ Increase parallelism (minPartitions)
            ✓ Optimize transformations
            ✓ Add more executors
            ✓ Use maxOffsetsPerTrigger

5. CHECKPOINT CORRUPTION:
   Error: "Incompatible checkpoint"
   Cause: Code changed with existing checkpoint
   Solution: ✓ Use new checkpoint location
            ✓ Version checkpoints
            ✓ Plan for schema evolution
    """)
    
    print("""
💡 Best Practices for Resilient Streaming:

1. Checkpointing:
   .option("checkpointLocation", "/reliable/storage/checkpoints/app1")
   
   ✓ Use HDFS, S3, or other reliable storage
   ✓ Never use local disk in production
   ✓ Include app version in path

2. Malformed Data Handling:
   
   from pyspark.sql.functions import expr
   
   # Add corrupt record column
   schema_with_corrupt = StructType([
       StructField("_corrupt_record", StringType(), True),
       # ... other fields
   ])
   
   parsed = kafka_stream.select(
       from_json(col("value").cast("string"), schema_with_corrupt).alias("data")
   )
   
   # Separate good and bad records
   good_records = parsed.filter(col("data._corrupt_record").isNull())
   bad_records = parsed.filter(col("data._corrupt_record").isNotNull())
   
   # Write bad records to dead letter queue
   bad_records.writeStream \\
       .format("kafka") \\
       .option("topic", "dead-letter-queue") \\
       .start()

3. Monitoring:
   
   query = stream.writeStream.start()
   
   # Monitor progress
   while query.isActive:
       progress = query.lastProgress
       if progress:
           print(f"Input rate: {progress.get('inputRowsPerSecond', 0)}")
           print(f"Process rate: {progress.get('processedRowsPerSecond', 0)}")
           print(f"Batch duration: {progress['durationMs']['triggerExecution']}ms")
       time.sleep(10)

4. Graceful Shutdown:
   
   import signal
   
   def signal_handler(sig, frame):
       print("Shutting down gracefully...")
       query.stop()
       spark.stop()
       sys.exit(0)
   
   signal.signal(signal.SIGINT, signal_handler)
   signal.signal(signal.SIGTERM, signal_handler)
    """)
    
    print("\n✅ Example 5 complete - error handling covered")


def main():
    """
    Main execution function.
    """
    print("\n" + "🔥 " * 40)
    print("KAFKA CONSUMER EXAMPLES - COMPREHENSIVE GUIDE")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    # Run all examples
    example_1_read_kafka_raw(spark)
    example_2_parse_json_messages(spark)
    example_3_multiple_topics(spark)
    example_4_kafka_options(spark)
    example_5_error_handling(spark)
    
    print("\n" + "=" * 80)
    print("✅ ALL EXAMPLES COMPLETE")
    print("=" * 80)
    
    print("""
📚 Summary - What You Learned:

1. Basic Kafka Reading:
   ✓ Connect to Kafka brokers
   ✓ Subscribe to topics
   ✓ Understand Kafka message structure (key, value, metadata)

2. JSON Parsing:
   ✓ Define schemas for structured data
   ✓ Use from_json() to parse messages
   ✓ Extract nested fields

3. Multi-Topic Consumption:
   ✓ Subscribe to multiple topics
   ✓ Use patterns for dynamic topics
   ✓ Route processing by topic

4. Configuration Options:
   ✓ Essential Kafka options
   ✓ Performance tuning parameters
   ✓ Production-ready settings

5. Error Handling:
   ✓ Common errors and solutions
   ✓ Resilience patterns
   ✓ Monitoring and alerting

🎯 Next Steps:
   1. Set up Kafka locally (see README.md)
   2. Create test topics and produce sample data
   3. Run these examples with real Kafka
   4. Experiment with different options
   5. Move to advanced examples (windowing, joins, aggregations)

�� Related Files:
   • 02_kafka_json_producer.py - Produce test messages
   • 03_windowed_aggregations.py - Time-based analytics
   • 04_stream_joins.py - Join multiple streams
   • ../streaming/03_kafka_streaming.py - Complete streaming guide

📖 Documentation:
   • Kafka: https://kafka.apache.org/documentation/
   • PySpark Kafka: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    """)
    
    spark.stop()


if __name__ == "__main__":
    main()
