# 🚀 PySpark ETL Practice System

## Overview

Master three essential ETL patterns through **focused, repetitive practice**. Build **muscle memory** to confidently code complete pipelines in interviews - from traditional batch processing to real-time streaming data lakes.

## 🎯 Goals

1. **Batch ETL**: Code CSV → Parquet pipeline in **< 15 minutes**
2. **Kafka Streaming**: Code Kafka → Kafka pipeline in **< 20 minutes**
3. **Data Lake Ingestion**: Code Kafka → Parquet pipeline in **< 25 minutes**

## 🚀 Quick Start

### Fastest Way (Shell Script)

```bash
cd src/interview
./run_practice.sh
```

Choose from the interactive menu or use direct commands:
```bash
./run_practice.sh batch       # Batch ETL practice
./run_practice.sh kafka       # Kafka streaming practice
./run_practice.sh parquet     # Data lake ingestion practice
./run_practice.sh launcher    # Master Python launcher
```

### Alternative (Python)

Launch the master practice system:
```bash
cd src/interview
python practice_launcher.py
```

Or run individual practice systems:
```bash
python batch_etl_practice.py          # Batch ETL
python kafka_streaming_practice.py    # Kafka streaming
python kafka_to_parquet_practice.py   # Data lake ingestion
```

**📖 See [QUICK_START.md](QUICK_START.md) for more options!**

## 📚 Three Practice Systems

### 1. 📊 **BATCH ETL PRACTICE** (Beginner)
**Focus**: CSV → Transform → Parquet

Master traditional batch processing fundamentals.

**Guided Mode - 6 Steps:**
1. Create SparkSession with AQE
2. Read customers.csv and orders.csv
3. Clean: dropDuplicates, dropna
4. Join: Left join on customer_id
5. Aggregate: groupBy with sum, count, avg
6. Write to Parquet with overwrite mode

**Practice Modes:**
- **Guided**: Step-by-step with hints and validation
- **Timed**: 15-minute target, 10-minute expert
- **Interview**: 20-minute limit, no hints
- **Reference**: Complete production solution

**Run**: `python batch_etl_practice.py`

---

### 2. 🌊 **KAFKA STREAMING PRACTICE** (Intermediate)
**Focus**: Kafka → Transform → Kafka

Learn real-time stream processing with watermarking and windowing.

**Guided Mode - 7 Steps:**
1. SparkSession with spark-sql-kafka package
2. readStream from Kafka topic "orders"
3. Parse JSON with StructType schema
4. withWatermark("timestamp", "10 minutes")
5. window("timestamp", "5 minutes") aggregation
6. Format output with to_json(struct())
7. writeStream to "order_aggregates" topic

**Practice Modes:**
- **Guided**: Step-by-step with Kafka-specific hints
- **Timed**: 20-minute target, 15-minute expert
- **Interview**: 25-minute limit, no hints
- **Reference**: Complete streaming solution

**Prerequisites**: Docker with Kafka running
**Run**: `python kafka_streaming_practice.py`

---

### 3. 💾 **KAFKA-TO-PARQUET PRACTICE** (Advanced)
**Focus**: Kafka → Transform → Data Lake

Master streaming data lake ingestion with partitioning and checkpointing.

**Guided Mode - 7 Steps:**
1. SparkSession with Kafka packages
2. readStream from Kafka topic
3. Parse JSON with complete order schema (9 fields)
4. Convert timestamp string to timestamp type
5. Add partition columns (year, month, day, hour)
6. Filter invalid records (null IDs, negative amounts)
7. writeStream to partitioned Parquet with checkpointing

**Practice Modes:**
- **Guided**: Step-by-step with data lake best practices
- **Timed**: 25-minute target, 18-minute expert
- **Interview**: 30-minute limit, no hints
- **Reference**: Production pipeline with logging

**Prerequisites**: Docker with Kafka running
**Run**: `python kafka_to_parquet_practice.py`

## 💡 Learning Path Recommendation

**Week 1**: Batch ETL Practice
- Master fundamentals with CSV-based pipelines
- Build confidence with step-by-step guidance
- Target: Code complete pipeline in 15 minutes

**Week 2**: Kafka Streaming Practice
- Add real-time processing skills
- Learn watermarking and windowing
- Target: Code streaming pipeline in 20 minutes

**Week 3**: Kafka-to-Parquet Practice
- Integrate streaming with data lake patterns
- Master partitioning and checkpointing
- Target: Code data lake ingestion in 25 minutes

**Result**: Interview-ready on all three essential patterns!

---

## 🐳 Kafka Setup (Required for Streaming Practice)

### Start Kafka Infrastructure

```bash
cd src/interview

# Start Kafka (Zookeeper, Broker, UI)
docker-compose up -d

# Verify Kafka is running
docker ps

# Access Kafka UI
open http://localhost:8080
```

### Generate Test Data

```bash
# Start order producer (continuous mode)
python kafka_producer_orders.py

# Or in burst mode for testing
python kafka_producer_orders.py --mode burst
```

### Run Production Examples

```bash
# Kafka streaming (Kafka → Kafka)
python kafka_to_parquet_example.py

# Data lake ingestion (Kafka → Parquet)
python kafka_to_parquet_example.py
```

**📖 See [KAFKA_ETL_README.md](KAFKA_ETL_README.md) for complete setup guide!**

---

## 📦 Sample Data Generation (Batch Practice Only)

For batch ETL practice, generate sample CSV files:

```bash
cd src/interview
python sample_data_generator.py
```

This creates:
- `data/customers.csv` (1000 customers)
- `data/orders.csv` (5000 orders)

---

## 📁 Project Structure

```
src/interview/
├── practice_launcher.py              # Master launcher (START HERE!)
│
├── batch_etl_practice.py             # System 1: Batch ETL
├── kafka_streaming_practice.py       # System 2: Kafka streaming
├── kafka_to_parquet_practice.py      # System 3: Data lake ingestion
│
├── kafka_to_parquet_example.py       # Production streaming example
├── kafka_producer_orders.py          # Test data generator
│
├── docker-compose.yml                # Kafka infrastructure
├── sample_data_generator.py          # CSV test data
│
├── README.md                         # This file
└── KAFKA_ETL_README.md               # Kafka setup guide
```

---

## 🎯 What You'll Build

### Batch ETL Pipeline
- Read customer and order CSV files
- Clean: Remove duplicates, filter nulls
- Transform: Join tables, aggregate metrics
- Load: Write to Parquet with proper modes
- Target: 15 minutes from memory

### Kafka Streaming Pipeline
- Read from Kafka topic ("orders")
- Parse JSON with schema validation
- Handle late data with watermarking
- Perform window aggregations (5-minute tumbling)
- Write back to Kafka topic ("order_aggregates")
- Target: 20 minutes from memory

### Data Lake Ingestion Pipeline
- Read streaming data from Kafka
- Parse and validate JSON schema (9 fields)
- Transform: Add timestamps and partition columns
- Filter invalid records for data quality
- Write to partitioned Parquet (year/month/day)
- Configure checkpointing for fault tolerance
- Target: 25 minutes from memory
---

## 💡 Practice Strategy

### For Each Practice System

**Phase 1: Learning (Week 1)**
- **Days 1-2**: GUIDED mode - Complete with hints
- **Days 3-4**: GUIDED mode - Complete without hints
- **Days 5-7**: TIMED mode - Build speed

**Phase 2: Mastery (Week 2)**
- **Days 1-3**: TIMED mode - Beat target time
- **Days 4-5**: INTERVIEW mode - Simulate pressure
- **Days 6-7**: TIMED mode - Target expert time

**Phase 3: Interview Ready (Week 3)**
- **Daily**: INTERVIEW mode practice
- **Goal**: Consistent completion under target time
- **Focus**: Clean code, no errors, confident execution

### Multi-System Approach

1. **Master Batch ETL first** (1-2 weeks)
   - Build strong fundamentals
   - Get comfortable with PySpark API
   - Achieve 15-minute target consistently

2. **Add Kafka Streaming** (1-2 weeks)
   - Learn streaming concepts
   - Master watermarking and windowing
   - Achieve 20-minute target

3. **Master Data Lake Ingestion** (1-2 weeks)
   - Integrate batch and streaming knowledge
   - Master partitioning strategies
   - Achieve 25-minute target

**Total Time to Interview-Ready**: 6-8 weeks with daily practice

---

## 📝 Practice Directories

Your code will be saved to organized practice directories:

```
/tmp/etl_practice/
├── batch/                    # Batch ETL practice files
│   ├── my_etl.py
│   ├── timed_etl.py
│   └── interview_etl.py
│
├── kafka_stream/             # Kafka streaming practice files
│   ├── my_streaming.py
│   ├── timed_streaming.py
│   └── interview_streaming.py
│
└── kafka_parquet/            # Data lake ingestion practice files
    ├── my_kafka_parquet.py
    ├── timed_kafka_parquet.py
    └── interview_kafka_parquet.py
```
- **GUIDED:** `my_etl.py`
- **TIMED:** `timed_etl.py`
- **INTERVIEW:** `interview_etl.py`

## 🎓 Learning Outcomes

After mastering this practice system, you'll be able to:

✅ Code a complete ETL pipeline from memory  
✅ Handle data quality issues confidently  
✅ Implement proper error handling  
✅ Work under interview time pressure  
✅ Write production-ready code quickly  
✅ Explain your code clearly  

## 🏆 Success Metrics

| <sub>Level</sub> | <sub>Time</sub> | <sub>Criteria</sub> |
|-------|------|----------|
| <sub>**Beginner**</sub> | <sub>30+ min</sub> | <sub>Completes with hints</sub> |
| <sub>**Intermediate**</sub> | <sub>15-20 min</sub> | <sub>Completes without hints</sub> |
| <sub>**Advanced**</sub> | <sub>10-15 min</sub> | <sub>Clean code, no errors</sub> |
| <sub>**Expert**</sub> | <sub>< 10 min</sub> | <sub>Production-ready, confident</sub> |
| <sub>**Interview Ready**</sub> | <sub>10-12 min</sub> | <sub>Consistent, explainable</sub> |

## 📖 Reference Solution Structure

```python
# 1. Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, col, current_timestamp
import logging

# 2. SparkSession
spark = SparkSession.builder.appName("ETL").getOrCreate()

# 3. Extract
df_customers = spark.read.csv("customers.csv", header=True, inferSchema=True)
df_orders = spark.read.csv("orders.csv", header=True, inferSchema=True)

# 4. Transform - Clean
df_customers_clean = df_customers.dropDuplicates().dropna()
df_orders_clean = df_orders.dropDuplicates().dropna()

# 5. Transform - Join
df_joined = df_orders_clean.join(df_customers_clean, "customer_id", "left")

# 6. Transform - Aggregate
df_summary = df_joined.groupBy("customer_id").agg(
    sum("amount").alias("total_sales"),
    count("order_id").alias("order_count"),
    avg("amount").alias("avg_order_value")
)

# 7. Add metadata
df_summary = df_summary.withColumn("processed_at", current_timestamp())

# 8. Data quality check
total = df_summary.count()
nulls = df_summary.filter(col("total_sales").isNull()).count()
print(f"Quality: {nulls}/{total} null records")

# 9. Load
df_summary.write.mode("overwrite").parquet("output/customer_summary")

# 10. Cleanup
spark.stop()
```

## 🔧 Technical Requirements

- Python 3.8+
- PySpark 3.x
- Terminal/Command line access
- Text editor (VS Code, vim, nano, etc.)

## 🎯 Interview Tips

1. **Start with imports** - Get them right first
2. **Create SparkSession early** - Standard pattern
3. **Read data with proper options** - header=True, inferSchema=True
4. **Clean before join** - More efficient
5. **Use meaningful aliases** - Makes code readable
6. **Add error handling** - Shows production mindset
7. **Include logging** - Demonstrates best practices
8. **Test incrementally** - Use `.show()` to validate
9. **Clean up resources** - Always stop SparkSession
10. **Practice explaining** - Talk through your code

## 📁 Project Structure

```
src/interview/
├── etl_practice_gui.py          # Main practice system
├── sample_data_generator.py     # Generate practice data
├── README.md                     # This file
└── solutions/
    ├── beginner_solution.py      # Heavily commented
    ├── intermediate_solution.py  # Clean code
    └── expert_solution.py        # Production-ready
```

## 🎉 Ready to Practice?

```bash
python etl_practice_gui.py
```

**Practice makes perfect! The only way to master ETL is to code it again and again.**

---

*🏆 Goal: Code this so many times it becomes second nature!*