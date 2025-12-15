# Kafka ETL to Parquet - Complete Guide

## 📋 Overview

This directory contains production-ready examples of streaming ETL pipelines using PySpark and Kafka, focusing on real-time data ingestion into a data lake.

## 🎯 What You'll Learn

- **Kafka Integration**: Connect PySpark to Kafka topics
- **Schema Definition**: Structure incoming JSON data
- **Stream Processing**: Transform data in real-time
- **Parquet Writing**: Efficient storage with partitioning
- **Watermarking**: Handle late-arriving data
- **Checkpointing**: Fault-tolerant processing
- **Aggregations**: Window-based metrics
- **Production Patterns**: Error handling, logging, monitoring

## �� Files in This Directory

### Core ETL Pipeline
- **`kafka_to_parquet_example.py`** - Complete production ETL pipeline
  - Reads from Kafka topic
  - Parses JSON messages
  - Transforms and enriches data
  - Writes to partitioned Parquet files
  - Creates aggregated views

### Data Generation
- **`kafka_producer_orders.py`** - Test data generator
  - Generates realistic e-commerce orders
  - Sends to Kafka topic
  - Multiple traffic patterns
  - Late data simulation

### Infrastructure
- **`docker-compose.yml`** - Kafka environment setup
  - Zookeeper
  - Kafka broker
  - Kafka UI (web interface)

### Practice System
- **`etl_practice_gui.py`** - Interactive learning tool
  - Guided mode with hints
  - Step-by-step Kafka ETL practice
  - Code validation

## 🚀 Quick Start

### Step 1: Start Kafka Infrastructure

```bash
cd src/interview

# Start Kafka using Docker Compose
docker-compose up -d

# Verify services are running
docker-compose ps

# Check logs if needed
docker-compose logs -f kafka
```

**Services Available:**
- Kafka Broker: `localhost:9092`
- Kafka UI: `http://localhost:8080` (view topics, messages, etc.)
- Zookeeper: `localhost:2181`

### Step 2: Install Python Dependencies

```bash
# Install Kafka Python client
pip install kafka-python

# PySpark should already be installed
# If not: pip install pyspark
```

### Step 3: Generate Test Data

Open a new terminal and run the producer:

```bash
cd src/interview

python kafka_producer_orders.py

# Select mode:
# 1. Continuous orders (1 per second) - good for testing
# 2. Send N orders - send specific number
# 3. Burst traffic simulation - realistic patterns
# 4. Fast stream (10 orders per second) - stress test
```

### Step 4: Run the ETL Pipeline

Open another terminal and run the consumer:

```bash
cd src/interview

# Option 1: Run the complete example
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  kafka_to_parquet_example.py

# Option 2: Run with Python (if spark-submit not available)
python kafka_to_parquet_example.py
```

### Step 5: Monitor and Verify

**View in Kafka UI:**
- Open `http://localhost:8080`
- Navigate to Topics → `orders`
- See messages being produced

**Check Output Data:**
```bash
# View Parquet files
ls -lR /tmp/data_lake/orders

# Read with Python
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.parquet('/tmp/data_lake/orders')
df.show()
df.printSchema()
"
```

**View Aggregated Data:**
```bash
ls -lR /tmp/data_lake/orders_aggregated
```

## 📊 Data Flow

```
┌─────────────────┐
│  Kafka Producer │
│   (orders)      │
└────────┬────────┘
         │ JSON messages
         ▼
┌─────────────────┐
│  Kafka Topic    │
│    "orders"     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PySpark ETL    │
│  - Read Stream  │
│  - Parse JSON   │
│  - Transform    │
│  - Watermark    │
│  - Aggregate    │
└────────┬────────┘
         │
         ├──────────────────┬──────────────────┐
         ▼                  ▼                  ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Parquet      │  │ Parquet      │  │ Checkpoint   │
│ Raw Data     │  │ Aggregated   │  │ State        │
│ (Partitioned)│  │ Metrics      │  │ (Recovery)   │
└──────────────┘  └──────────────┘  └──────────────┘
```

## 🎓 Pipeline Architecture Explained

### 1. **SparkSession Configuration**
```python
spark = SparkSession.builder \
    .appName("Kafka_to_Parquet_ETL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_checkpoint") \
    .getOrCreate()
```
- Adds Kafka connector package
- Sets checkpoint location for fault tolerance
- Enables adaptive query execution

### 2. **Schema Definition**
```python
schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    # ... more fields
])
```
- Defines expected JSON structure
- `False` = non-nullable fields
- Used for parsing Kafka values

### 3. **Reading from Kafka**
```python
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .load()
```
- Creates streaming DataFrame
- `latest` = only new messages
- `earliest` = read all existing messages

### 4. **Parsing Messages**
```python
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
```
- Kafka value is binary
- Cast to string, parse as JSON
- Expand nested structure

### 5. **Transformations**
```python
df_transformed = df_parsed \
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
    .withColumn("year", year(col("order_timestamp"))) \
    .withColumn("month", month(col("order_timestamp"))) \
    .filter(col("amount") > 0)
```
- Convert timestamp strings to timestamps
- Add partition columns
- Filter invalid records

### 6. **Writing to Parquet**
```python
query = df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("year", "month", "day") \
    .trigger(processingTime="30 seconds") \
    .start()
```
- Append mode for continuous ingestion
- Partitioned for efficient querying
- Micro-batches every 30 seconds

### 7. **Aggregations with Watermarking**
```python
df_watermarked = df.withWatermark("order_timestamp", "10 minutes")

df_agg = df_watermarked.groupBy(
    window(col("order_timestamp"), "1 hour"),
    col("customer_id")
).agg(
    spark_sum("amount").alias("total_sales")
)
```
- Watermark handles late data (up to 10 minutes)
- 1-hour tumbling windows
- Customer-level aggregations

## 🎯 Common Use Cases

### Use Case 1: Real-Time Order Processing
**Scenario**: E-commerce platform processing orders
- **Input**: Order events from Kafka
- **Processing**: Validation, enrichment, aggregation
- **Output**: Data lake for analytics, dashboards

### Use Case 2: IoT Sensor Data
**Scenario**: Collect sensor readings from devices
- **Input**: Sensor telemetry from Kafka
- **Processing**: Filtering, anomaly detection, rollups
- **Output**: Time-series data for monitoring

### Use Case 3: Log Aggregation
**Scenario**: Centralized log processing
- **Input**: Application logs from Kafka
- **Processing**: Parsing, classification, metrics
- **Output**: Searchable log storage

### Use Case 4: CDC (Change Data Capture)
**Scenario**: Database changes streamed to data lake
- **Input**: Database change events from Kafka
- **Processing**: Schema evolution, deduplication
- **Output**: Historical data warehouse

## 🔧 Configuration Options

### Kafka Options
```python
.option("kafka.bootstrap.servers", "localhost:9092")  # Broker address
.option("subscribe", "topic1,topic2")                 # Multiple topics
.option("subscribePattern", "orders.*")               # Topic pattern
.option("startingOffsets", "earliest")                # Read all messages
.option("startingOffsets", "latest")                  # Only new messages
.option("maxOffsetsPerTrigger", 10000)               # Rate limiting
.option("failOnDataLoss", "false")                    # Handle topic deletion
```

### Checkpoint Options
```python
.option("checkpointLocation", "/path/to/checkpoint")  # Fault tolerance
```
**Important**: Never delete checkpoint directory while query is running!

### Trigger Options
```python
.trigger(processingTime="30 seconds")    # Micro-batch every 30s
.trigger(once=True)                      # Process available data and stop
.trigger(continuous="1 second")          # Experimental continuous mode
```

### Output Modes
```python
.outputMode("append")    # Only new rows (default for Parquet)
.outputMode("update")    # Updated rows (for aggregations)
.outputMode("complete")  # All rows every time
```

## 🐛 Troubleshooting

### Problem: Kafka Connection Refused
```
Solution:
1. Check Kafka is running: docker-compose ps
2. Verify port 9092 is open
3. Check firewall settings
4. Try: docker-compose restart kafka
```

### Problem: Checkpoint Already Exists
```
Solution:
1. Delete checkpoint if restarting fresh:
   rm -rf /tmp/kafka_checkpoint
2. Or use different checkpoint location
```

### Problem: No Data Appearing
```
Solution:
1. Check producer is sending: kafka-console-consumer --topic orders ...
2. Verify topic name matches
3. Check startingOffsets ("earliest" vs "latest")
4. View Kafka UI: http://localhost:8080
```

### Problem: Out of Memory
```
Solution:
1. Increase Spark memory: --driver-memory 4g --executor-memory 4g
2. Reduce shuffle partitions: .config("spark.sql.shuffle.partitions", "10")
3. Increase trigger interval (larger micro-batches)
```

## 📚 Learning Path

### Beginner
1. ✅ Start Kafka with docker-compose
2. ✅ Run producer to generate data
3. ✅ Run ETL pipeline
4. ✅ View output Parquet files
5. ✅ Explore Kafka UI

### Intermediate
1. ✅ Modify schema to add fields
2. ✅ Change transformations
3. ✅ Adjust partitioning strategy
4. ✅ Experiment with trigger intervals
5. ✅ Add custom aggregations

### Advanced
1. ✅ Implement foreachBatch for custom logic
2. ✅ Add multiple output sinks
3. ✅ Implement error handling and DLQ
4. ✅ Add monitoring and metrics
5. ✅ Optimize for production scale

## 🎤 Interview Practice

Use the interactive practice system:

```bash
python etl_practice_gui.py

# Select: 5. KAFKA ETL - 🔥 Real-time Kafka pipeline (with hints)
```

**What You'll Code:**
1. Create SparkSession with Kafka packages
2. Read from Kafka topic
3. Parse JSON with schema
4. Add watermark for late data
5. Create window aggregations
6. Write to Kafka output topic
7. Handle checkpointing

**Goal**: Code a complete Kafka streaming pipeline from memory!

## 🚦 Production Checklist

Before deploying to production:

- [ ] Proper error handling and logging
- [ ] Monitoring and alerting configured
- [ ] Checkpoint location on reliable storage (HDFS/S3)
- [ ] Output location on scalable storage
- [ ] Kafka cluster properly configured
- [ ] Resource allocation tuned (memory, cores)
- [ ] Backpressure handling configured
- [ ] Schema evolution strategy defined
- [ ] Data quality checks implemented
- [ ] Disaster recovery plan documented

## 🔗 Resources

- **Apache Spark Structured Streaming**: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- **Kafka Integration Guide**: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
- **PySpark SQL Functions**: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html

## 💡 Pro Tips

1. **Always use checkpointing** - It's your fault tolerance mechanism
2. **Monitor lag** - Track how far behind your consumer is
3. **Partition wisely** - Balance parallelism with file count
4. **Test with varied data** - Include edge cases, nulls, late data
5. **Start small** - Test with low volume before scaling up
6. **Use watermarks** - Essential for handling late data correctly
7. **Tune micro-batch size** - Balance latency vs throughput
8. **Plan for schema evolution** - Data formats change over time

## 🎉 Next Steps

Once comfortable with Kafka to Parquet:

1. Try Kafka to Kafka transformations
2. Implement exactly-once semantics
3. Add Delta Lake for ACID transactions
4. Integrate with streaming ML models
5. Build end-to-end real-time dashboards

---

**Happy Streaming!** 🚀

For questions or issues, check the logs and Kafka UI first!
