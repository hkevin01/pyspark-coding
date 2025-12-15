# 🔥 Kafka ETL Pipeline Practice - Setup Guide

## Overview

The Kafka ETL practice mode helps you master **real-time streaming data processing** with PySpark and Kafka. This is a critical skill for modern data engineering interviews!

## What You'll Learn

- ✅ **Structured Streaming** with PySpark
- ✅ **Kafka Integration** (reading and writing)
- ✅ **JSON Schema Parsing** from Kafka messages
- ✅ **Watermarking** for handling late data
- ✅ **Window Aggregations** (tumbling windows)
- ✅ **Checkpoint Management** for fault tolerance
- ✅ **Stream Processing Patterns**

## Prerequisites

### Option 1: Use Docker (Recommended - Easiest!)

We provide a `docker-compose.yml` that sets up Kafka + Zookeeper automatically.

```bash
# Start Kafka (from interview folder)
docker-compose up -d

# Verify it's running
docker-compose ps

# View logs
docker-compose logs -f kafka
```

### Option 2: Local Kafka Installation

If you prefer local installation:

```bash
# Download Kafka
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka (in another terminal)
bin/kafka-server-start.sh config/server.properties
```

### Option 3: Practice Without Kafka (Mock Mode)

You can practice the code structure without running Kafka:
- The GUI will still teach you the patterns
- Use the kafka_simulator.py in mock mode to see sample data
- Perfect for learning the syntax!

## Quick Start

### 1. Start Kafka

```bash
cd /home/kevin/Projects/pyspark-coding/src/interview

# Option A: Docker
docker-compose up -d

# Option B: Or use mock mode (no Kafka needed)
python kafka_simulator.py  # Choose option 2
```

### 2. Create Kafka Topics

```bash
# If using Docker:
docker-compose exec kafka kafka-topics --create \
    --topic orders \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

docker-compose exec kafka kafka-topics --create \
    --topic order_aggregates \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1

# List topics to verify
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Start the Data Simulator

In one terminal:
```bash
python kafka_simulator.py
# Choose option 1 (or 2 for mock mode)
```

This will send streaming order events to Kafka!

### 4. Practice the ETL Pipeline

In another terminal:
```bash
python etl_practice_gui.py
# Choose option 4: KAFKA ETL
```

Follow the step-by-step guide with hints!

### 5. Test Your Pipeline

Once you've written your code:
```bash
# Run your Kafka ETL pipeline
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    /tmp/etl_practice/kafka_etl.py
```

Watch it process streaming data in real-time! 🔥

## Data Format

The simulator sends JSON events to Kafka:

```json
{
  "order_id": "ORD-12345",
  "customer_id": "CUST-567",
  "amount": 129.99,
  "timestamp": "2025-12-15T10:30:45.123456"
}
```

Your job: Read this, aggregate by customer in 5-minute windows, write to output topic!

## Pipeline Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐     ┌───────────────┐
│   Orders    │────▶│   PySpark    │────▶│  Aggregation    │────▶│Order Aggregate│
│Kafka Topic  │     │   Streaming  │     │  (5-min window) │     │  Kafka Topic  │
└─────────────┘     └──────────────┘     └─────────────────┘     └───────────────┘
       │                    │                      │                      │
   Raw Events          Parse JSON           Sum by Customer         Output JSON
```

## 7-Step Pipeline

The practice guide walks you through:

1. **Setup SparkSession** with Kafka packages
2. **Read from Kafka** topic "orders"
3. **Parse JSON** from Kafka value column
4. **Add Watermark** (10 minutes for late data)
5. **Window Aggregation** (5-minute tumbling windows)
6. **Prepare Output** (convert to Kafka format)
7. **Write to Kafka** topic "order_aggregates"

## Interview Tips

### What Interviewers Look For:

✅ **Correct Kafka configuration**
   - Proper packages/dependencies
   - Bootstrap servers configuration
   - Topic subscription

✅ **Schema handling**
   - Define StructType schema
   - Parse JSON correctly
   - Handle nested data

✅ **Stream processing concepts**
   - Watermarking for late data
   - Window functions
   - Output modes (append, update, complete)

✅ **Fault tolerance**
   - Checkpoint location
   - Error handling
   - Recovery strategy

✅ **Performance awareness**
   - Partitioning strategy
   - Parallelism configuration
   - Resource management

### Common Mistakes to Avoid:

❌ Forgetting to add Kafka packages
❌ Not casting Kafka value to string before parsing
❌ Missing checkpoint location (required for streaming)
❌ Wrong output mode for aggregations
❌ Not handling late-arriving data
❌ Forgetting to call awaitTermination()

## Testing Your Code

### View Kafka Messages

```bash
# Consume from input topic
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --from-beginning

# Consume from output topic
docker-compose exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic order_aggregates \
    --from-beginning
```

### Monitor Your Streaming Query

PySpark provides a web UI at: http://localhost:4040

- Check "Streaming" tab
- View processing rates
- Monitor latency
- Check for failures

## Advanced Practice

Once you master the basic pipeline:

1. **Add more transformations**
   - Filter high-value orders
   - Detect fraud patterns
   - Calculate moving averages

2. **Implement multiple output sinks**
   - Write to both Kafka and Parquet
   - Send alerts to another topic
   - Update a database

3. **Handle schema evolution**
   - Use schema registry
   - Handle missing fields
   - Version your schemas

4. **Optimize performance**
   - Tune parallelism
   - Optimize partition count
   - Implement custom partitioning

## Troubleshooting

### "Kafka not running" error
```bash
# Check if Kafka is running
docker-compose ps

# Restart if needed
docker-compose restart kafka

# Check logs
docker-compose logs kafka
```

### "Package not found" error
```bash
# Make sure you include the Kafka package
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 your_script.py
```

### "Checkpoint location" error
```bash
# Make sure checkpoint directory exists and is writable
mkdir -p /tmp/kafka_checkpoint
chmod 777 /tmp/kafka_checkpoint
```

## Cleanup

```bash
# Stop Kafka
docker-compose down

# Remove volumes (clean slate)
docker-compose down -v

# Remove checkpoint data
rm -rf /tmp/kafka_checkpoint
```

## Practice Schedule

### Week 1: Learning
- Day 1-2: Complete guided mode with all hints
- Day 3-4: Complete without hints
- Day 5: Understand each step deeply

### Week 2: Mastery
- Day 1-3: Code from memory
- Day 4-5: Add custom features
- Day 6-7: Optimize performance

### Week 3: Interview Ready
- Practice explaining your code
- Answer "why" questions
- Handle edge cases
- Complete in 15-20 minutes

## Success Criteria

| Level | Time | Criteria |
|-------|------|----------|
| Beginner | 40+ min | Completes with hints |
| Intermediate | 25-30 min | Completes without hints |
| Advanced | 15-20 min | Clean code, handles edge cases |
| Expert | < 15 min | Production-ready, optimized |
| Interview Ready | 15-18 min | Can explain every line |

## Real Interview Questions

Practice answering:

1. "What is watermarking and why do we need it?"
2. "Explain the difference between append, update, and complete output modes"
3. "How do you handle late-arriving data?"
4. "What happens if your streaming query fails?"
5. "How would you scale this to handle 1M events/second?"
6. "Why use Kafka instead of batch processing?"

## Resources

- [PySpark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)

## Ready to Master Kafka ETL? 🚀

```bash
# Start practicing now!
cd /home/kevin/Projects/pyspark-coding/src/interview
python etl_practice_gui.py
# Choose option 4: KAFKA ETL
```

**Remember**: The key to mastery is **repetition with understanding**. 

Code it once with hints, then code it 10 more times from memory! 💪
