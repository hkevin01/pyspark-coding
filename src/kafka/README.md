# Kafka Streaming Examples 🔥

Comprehensive examples demonstrating how to consume and process data from Apache Kafka using PySpark Structured Streaming.

## 📁 Directory Structure

```
kafka/
├── 01_basic_kafka_consumer.py      # Connect to Kafka, read messages, parse JSON
├── 02_kafka_json_producer.py       # Generate test data (requires kafka-python)
├── 03_windowed_aggregations.py     # Time-window analytics (tumbling, sliding, session)
└── README.md                        # This file
```

## 🎯 What You'll Learn

### Example 1: Basic Kafka Consumer
- Connect to Kafka brokers
- Subscribe to topics (single, multiple, pattern-based)
- Understand Kafka message structure (key, value, metadata)
- Parse binary messages to strings
- Parse JSON from Kafka messages with schemas
- Configure essential Kafka options
- Handle errors and build resilient consumers

### Example 2: Kafka JSON Producer
- Generate realistic test data for development
- Produce messages with proper key-value pairs
- Use different event schemas (user events, IoT, logs)
- Configure producer for reliability and performance
- Understand Kafka producer best practices

### Example 3: Windowed Aggregations
- **Tumbling windows** - Non-overlapping time buckets
- **Sliding windows** - Overlapping time windows for moving averages
- **Session windows** - Activity-based grouping with gap thresholds
- **Watermarks** - Handle late-arriving data correctly
- Complex aggregations with multiple metrics
- Real-time dashboard queries

## 🚀 Quick Start

### Prerequisites

1. **Kafka Running Locally**
   ```bash
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka Broker
   bin/kafka-server-start.sh config/server.properties
   ```

2. **Python Dependencies**
   ```bash
   # For producer (example 2)
   pip install kafka-python faker
   
   # PySpark already installed
   ```

3. **Create Kafka Topics**
   ```bash
   # User events topic
   kafka-topics --create --bootstrap-server localhost:9092 \
     --replication-factor 1 --partitions 3 --topic user-events
   
   # Payment events topic
   kafka-topics --create --bootstrap-server localhost:9092 \
     --replication-factor 1 --partitions 3 --topic payment-events
   
   # IoT sensors topic
   kafka-topics --create --bootstrap-server localhost:9092 \
     --replication-factor 1 --partitions 3 --topic iot-sensors
   
   # System logs topic
   kafka-topics --create --bootstrap-server localhost:9092 \
     --replication-factor 1 --partitions 3 --topic system-logs
   ```

### Running the Examples

#### Step 1: Produce Test Data
```bash
# Run the producer to generate test messages
python 02_kafka_json_producer.py
```

Output:
```
📤 Producing 50 messages to topic: user-events
📤 Producing 20 messages to topic: payment-events
📤 Producing 100 messages to topic: iot-sensors
📤 Producing 30 messages to topic: system-logs
✅ ALL MESSAGES PRODUCED SUCCESSFULLY
```

#### Step 2: Run Basic Consumer
```bash
# Read and parse messages from Kafka
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  01_basic_kafka_consumer.py
```

#### Step 3: Run Windowed Aggregations
```bash
# Time-window analytics
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  03_windowed_aggregations.py
```

## 📊 Example Use Cases

### 1. Real-Time User Analytics
```python
# Count page views per 5-minute window by country
windowed_counts = events \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("country")
    ) \
    .agg(count("*").alias("page_views"))
```

### 2. IoT Sensor Monitoring
```python
# Moving average of temperature (10-min window, 5-min slide)
sliding_avg = sensors \
    .withWatermark("timestamp", "15 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes", "5 minutes"),
        col("location")
    ) \
    .agg(avg("value").alias("avg_temperature"))
```

### 3. User Session Analytics
```python
# Track user sessions (5-minute inactivity = new session)
user_sessions = events \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        col("user_id"),
        session_window(col("timestamp"), "5 minutes")
    ) \
    .agg(
        count("*").alias("events_in_session"),
        expr("(max(timestamp) - min(timestamp))").alias("duration")
    )
```

### 4. E-Commerce Dashboard
```python
# Revenue metrics per 10-minute window
ecommerce_metrics = purchases \
    .withWatermark("timestamp", "30 minutes") \
    .groupBy(
        window(col("timestamp"), "10 minutes"),
        col("category")
    ) \
    .agg(
        sum("total_amount").alias("revenue"),
        count("*").alias("orders"),
        count("distinct user_id").alias("unique_customers"),
        avg("total_amount").alias("avg_order_value")
    )
```

## 🔧 Essential Kafka Configuration

### Consumer Configuration
```python
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("maxOffsetsPerTrigger", "10000") \
    .option("minPartitions", "10") \
    .option("kafka.group.id", "my-consumer-group") \
    .load()
```

### Key Options Explained

| Option | Values | Purpose |
|--------|--------|---------|
| `startingOffsets` | `earliest`, `latest`, `{"topic":{"0":23}}` | Where to start reading |
| `failOnDataLoss` | `true`, `false` | Handle missing offsets |
| `maxOffsetsPerTrigger` | Integer | Rate limiting (records per batch) |
| `minPartitions` | Integer | Spark parallelism |
| `kafka.group.id` | String | Consumer group identifier |

### Writing Back to Kafka
```python
query = processed_stream \
    .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()
```

## ⚡ Performance Tuning

### For High Throughput
```python
.option("maxOffsetsPerTrigger", "100000")  # Large batches
.option("minPartitions", "20")              # More parallelism
.trigger(processingTime="30 seconds")       # Larger intervals
```

### For Low Latency
```python
.option("maxOffsetsPerTrigger", "1000")    # Small batches
.trigger(processingTime="1 second")         # Frequent processing
# OR
.trigger(continuous="1 second")             # Continuous mode
```

### For Resource-Constrained Environments
```python
.option("maxOffsetsPerTrigger", "5000")    # Moderate batches
.option("minPartitions", "4")               # Limited parallelism
.trigger(processingTime="10 seconds")       # Balanced interval
```

## ��️ Error Handling

### Common Issues and Solutions

#### 1. Connection Errors
```
Error: "Failed to resolve Kafka bootstrap servers"
```
**Solution**: Verify Kafka is running and accessible at the specified address.

#### 2. Offset Out of Range
```
Error: "Offset out of range"
```
**Solution**: Set `failOnDataLoss=false` and use `startingOffsets=latest`.

#### 3. Deserialization Errors
```python
# Handle malformed JSON with corrupt record column
schema_with_corrupt = StructType([
    StructField("_corrupt_record", StringType(), True),
    # ... other fields
])

parsed = kafka_stream.select(
    from_json(col("value").cast("string"), schema_with_corrupt).alias("data")
)

# Separate good and bad records
good = parsed.filter(col("data._corrupt_record").isNull())
bad = parsed.filter(col("data._corrupt_record").isNotNull())

# Write bad records to dead letter queue
bad.writeStream.format("kafka").option("topic", "dead-letter").start()
```

#### 4. Consumer Lag
**Symptoms**: Processing falls behind production

**Solutions**:
- Increase `maxOffsetsPerTrigger`
- Add more Kafka partitions
- Optimize transformations
- Scale up executors

## �� Best Practices

### 1. Always Use Watermarks with Stateful Operations
```python
# ✅ Good - with watermark
df.withWatermark("timestamp", "10 minutes") \
  .groupBy(window("timestamp", "5 minutes")) \
  .count()

# ❌ Bad - no watermark (unbounded state growth!)
df.groupBy(window("timestamp", "5 minutes")).count()
```

### 2. Choose Appropriate Watermark Duration
```python
# Balance between accepting late data vs. memory usage
.withWatermark("timestamp", "10 minutes")  # Good balance
# Too short: drops valid late data
# Too long: excessive memory for state
```

### 3. Use Distributed Checkpoint Storage
```python
# ✅ Production
.option("checkpointLocation", "hdfs://namenode/checkpoints/app1")
.option("checkpointLocation", "s3a://bucket/checkpoints/app1")

# ❌ Development only
.option("checkpointLocation", "/tmp/checkpoint")
```

### 4. Monitor Your Streaming Queries
```python
query = stream.writeStream.start()

while query.isActive:
    progress = query.lastProgress
    if progress:
        print(f"Input rate: {progress.get('inputRowsPerSecond', 0)}")
        print(f"Process rate: {progress.get('processedRowsPerSecond', 0)}")
        print(f"Batch duration: {progress['durationMs']['triggerExecution']}ms")
    time.sleep(10)
```

### 5. Schema Evolution Strategy
```python
# Version your checkpoints when schema changes
checkpoint_v1 = "/hdfs/checkpoints/app1-v1"  # Old schema
checkpoint_v2 = "/hdfs/checkpoints/app1-v2"  # New schema

# Or use schema versioning in messages (Avro, Protobuf)
```

## 📖 Additional Resources

### Kafka Documentation
- [Apache Kafka Official Docs](https://kafka.apache.org/documentation/)
- [Kafka Quickstart](https://kafka.apache.org/quickstart)
- [Kafka Configuration Reference](https://kafka.apache.org/documentation/#configuration)

### PySpark Streaming
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Spark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

### Related Examples
- `../streaming/03_kafka_streaming.py` - Complete Kafka streaming guide
- `../streaming/01_socket_streaming.py` - Socket-based streaming
- `../prometheus/` - Monitoring streaming applications

## 🐛 Troubleshooting

### Check Kafka is Running
```bash
# List topics
kafka-topics --list --bootstrap-server localhost:9092

# Check consumer groups
kafka-consumer-groups --list --bootstrap-server localhost:9092

# Describe consumer group
kafka-consumer-groups --describe --group my-group \
  --bootstrap-server localhost:9092
```

### View Messages in Topic
```bash
# Console consumer
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic user-events --from-beginning --max-messages 10
```

### Check Spark Streaming UI
```
Navigate to: http://localhost:4040/StreamingQuery/
View: Progress reports, batch duration, input/output rates
```

## 🎓 Learning Path

1. **Start Here**: `01_basic_kafka_consumer.py`
   - Understand Kafka message structure
   - Learn to parse JSON
   - Configure consumer options

2. **Generate Data**: `02_kafka_json_producer.py`
   - Produce realistic test data
   - Understand Kafka producer patterns

3. **Advanced Analytics**: `03_windowed_aggregations.py`
   - Master windowing strategies
   - Handle late data with watermarks
   - Build real-time dashboards

4. **Next Steps**:
   - Stream-to-stream joins
   - Stateful transformations
   - Production deployment
   - Monitoring and alerting

## 📝 Notes

- All examples are configured for **local development** (single broker)
- For **production**, adjust replication factor, partitions, and checkpoints
- **Docker Compose** setup available in project root for easy Kafka deployment
- Examples gracefully handle missing Kafka (won't crash, just show warnings)

## 🤝 Contributing

Found an issue or have suggestions? These examples are part of the PySpark learning project!

---

**Happy Streaming! 🚀**

For questions or issues, refer to the main project documentation.
