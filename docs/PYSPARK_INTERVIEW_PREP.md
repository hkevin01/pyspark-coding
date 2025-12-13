# �� PySpark Interview Preparation Guide

**Complete preparation for PySpark roles covering Core, Cluster Computing, ML Integration, and Production**

---

## 📚 Table of Contents

1. [Core PySpark](#core-pyspark)
2. [Cluster Computing](#cluster-computing)
3. [ML Integration](#ml-integration)
4. [Cloud & Production](#cloud--production)

---

## Core PySpark

### ✅ Explain Spark Architecture (driver, executors, tasks)

**Architecture Components:**

```
┌────────────────────────────────────────────────────────────────┐
│                         DRIVER PROGRAM                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  SparkContext / SparkSession                             │  │
│  │  • Creates DAG (Directed Acyclic Graph)                  │  │
│  │  • Schedules tasks                                       │  │
│  │  • Monitors executors                                    │  │
│  │  • Collects results                                      │  │
│  └────────────────┬─────────────────────────────────────────┘  │
└───────────────────┼────────────────────────────────────────────┘
                    │
       ┌────────────┼────────────┐
       │            │            │
┌──────▼──────┐ ┌──▼────────┐ ┌─▼───────────┐
│  EXECUTOR 1 │ │ EXECUTOR 2│ │ EXECUTOR 3  │
│  ┌────────┐ │ │ ┌────────┐│ │ ┌────────┐  │
│  │ Task 1 │ │ │ │ Task 3 ││ │ │ Task 5 │  │
│  │ Task 2 │ │ │ │ Task 4 ││ │ │ Task 6 │  │
│  └────────┘ │ │ └────────┘│ │ └────────┘  │
│  Cache      │ │  Cache    │ │  Cache      │
└─────────────┘ └───────────┘ └─────────────┘
```

**Key Concepts:**

- **Driver**: 
  - Runs main() program
  - Creates SparkContext
  - Converts user code to DAG
  - Schedules tasks across executors
  - Collects final results

- **Executors**:
  - JVM processes on worker nodes
  - Execute tasks assigned by driver
  - Store cached data
  - Return results to driver
  - Lifecycle: Spawned at start, persist for app duration

- **Tasks**:
  - Smallest unit of work
  - 1 task = 1 partition = 1 core
  - Run in parallel on executors
  - Types: ShuffleMapTask, ResultTask

**Interview Answer Template:**
```
"Spark uses a master-slave architecture. The Driver runs on master and contains
SparkContext which coordinates execution. Executors run on worker nodes, executing
tasks in parallel. Each task processes one partition. For example, if we have
100 partitions and 10 executors with 4 cores each (40 total cores), 40 tasks
run simultaneously, completing in 3 waves (40+40+20)."
```

---

### ✅ Write ETL Pipeline from Scratch in 15 Minutes

**Quick ETL Template:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# 1. CREATE SESSION (1 min)
spark = SparkSession.builder \
    .appName("ETL_Pipeline") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 2. DEFINE SCHEMA (2 min)
schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("date", StringType(), False),
    StructField("category", StringType(), True)
])

# 3. EXTRACT (2 min)
df_raw = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv("s3://bucket/input/")

# 4. TRANSFORM (7 min)
df_clean = df_raw \
    .filter(col("amount") > 0) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
    .withColumn("amount_category",
        when(col("amount") < 100, "Small")
        .when(col("amount") < 1000, "Medium")
        .otherwise("Large")
    ) \
    .dropDuplicates(["transaction_id"])

# Aggregations
df_summary = df_clean.groupBy("customer_id", "category").agg(
    sum("amount").alias("total_spent"),
    avg("amount").alias("avg_transaction"),
    count("*").alias("transaction_count")
)

# 5. LOAD (3 min)
df_summary.write \
    .mode("overwrite") \
    .partitionBy("category") \
    .parquet("s3://bucket/output/")

spark.stop()
```

**Time Breakdown:**
- Session + Schema: 3 min
- Extract: 2 min
- Transform (filter, derive, dedupe): 5 min
- Aggregate: 2 min
- Load: 2 min
- Buffer: 1 min

---

### ✅ Optimize Slow Joins (broadcast, partitioning, salting)

**Optimization Strategies:**

#### 1. Broadcast Join (Small Table < 10MB)
```python
from pyspark.sql.functions import broadcast

# Regular join (shuffle both sides)
df_result = df_large.join(df_small, "key")  # ❌ Slow

# Broadcast join (no shuffle for small table)
df_result = df_large.join(broadcast(df_small), "key")  # ✅ 2-3× faster

# Why: Small table copied to all executors, large table doesn't move
# When: Small table < 10MB, large table > 1GB
```

#### 2. Partitioning (Pre-partition on join key)
```python
# Repartition before join
df_large = df_large.repartition(200, "join_key")
df_medium = df_medium.repartition(200, "join_key")

df_result = df_large.join(df_medium, "join_key")

# Why: Co-located partitions (same key on same executor)
# Benefit: Reduces shuffle, 2-5× speedup
```

#### 3. Salting (Handle skewed keys)
```python
# Problem: One key has 90% of data (hot key)
# Solution: Add random salt to split hot key

from pyspark.sql.functions import rand, concat, lit

# Add salt to both DataFrames
df_left_salted = df_left.withColumn("salt", (rand() * 10).cast("int")) \
    .withColumn("key_salted", concat(col("key"), lit("_"), col("salt")))

df_right_exploded = df_right.withColumn("salt", explode(array([lit(i) for i in range(10)]))) \
    .withColumn("key_salted", concat(col("key"), lit("_"), col("salt")))

# Join on salted key
df_result = df_left_salted.join(df_right_exploded, "key_salted")

# Why: Splits hot key across 10 partitions
# Benefit: 5-50× speedup for skewed joins
```

**Decision Matrix:**
| Scenario | Solution | Speedup |
|----------|----------|---------|
| Small table (< 10MB) | Broadcast | 2-3× |
| Large tables, evenly distributed | Repartition | 2-5× |
| Skewed keys (hot keys) | Salting | 5-50× |
| Already partitioned (bucketed) | No change | Optimal |

---

### ✅ Handle Data Quality Issues (nulls, duplicates, skew)

**Comprehensive Data Quality Patterns:**

```python
from pyspark.sql.functions import col, count, when, isnan, isnull, monotonically_increasing_id

# 1. NULL HANDLING
df_clean = df.filter(col("critical_column").isNotNull())  # Drop nulls
df_filled = df.fillna({"amount": 0, "category": "Unknown"})  # Fill nulls
df_coalesced = df.withColumn("value", coalesce(col("col1"), col("col2"), lit(0)))  # First non-null

# 2. DUPLICATE HANDLING
# Drop exact duplicates
df_deduped = df.dropDuplicates()

# Drop based on subset
df_deduped = df.dropDuplicates(["transaction_id"])

# Keep latest (window function)
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("id").orderBy(col("timestamp").desc())
df_latest = df.withColumn("row_num", row_number().over(windowSpec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# 3. DATA SKEW HANDLING
# Detect skew
df.groupBy("key").count().orderBy(col("count").desc()).show()

# Solution 1: Salting (see above)
# Solution 2: Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Solution 3: Increase parallelism
df_repartitioned = df.repartition(400)  # Default 200 → 400

# 4. VALIDATION
df_validated = df.filter(
    (col("amount") > 0) &
    (col("date").between("2020-01-01", "2025-12-31")) &
    (col("category").isin(["A", "B", "C"]))
)

# Data quality report
df.select([
    count(when(col(c).isNull(), c)).alias(f"{c}_nulls")
    for c in df.columns
]).show()
```

---

### ✅ Use Window Functions for Complex Analytics

**Window Function Patterns:**

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, lag, lead, sum, avg

# 1. RANKING (Top N per group)
windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
df_ranked = df.withColumn("rank", row_number().over(windowSpec))
top_earners = df_ranked.filter(col("rank") <= 3)

# 2. RUNNING TOTALS
windowSpec = Window.partitionBy("customer_id") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_cumulative = df.withColumn("running_total", sum("amount").over(windowSpec))

# 3. MOVING AVERAGES (Time-series)
windowSpec = Window.partitionBy("stock") \
    .orderBy("date") \
    .rowsBetween(-2, 0)  # 3-day MA
df_ma = df.withColumn("ma_3day", avg("close").over(windowSpec))

# 4. LAG/LEAD (Compare to previous/next)
windowSpec = Window.partitionBy("product").orderBy("month")
df_compare = df.withColumn("prev_sales", lag("sales", 1).over(windowSpec)) \
    .withColumn("next_sales", lead("sales", 1).over(windowSpec)) \
    .withColumn("growth", (col("sales") - col("prev_sales")) / col("prev_sales") * 100)

# 5. PERCENTILES (Segmentation)
windowSpec = Window.partitionBy("category").orderBy(col("value").desc())
df_segments = df.withColumn("quartile", ntile(4).over(windowSpec))
```

**Key Difference:**
- `groupBy()`: Collapses rows (aggregation)
- `Window`: Preserves rows (adds computed columns)

---

### ✅ Read/Write Multiple Data Formats

**Format Cheat Sheet:**

```python
# CSV
df = spark.read.csv("path/", header=True, schema=schema, inferSchema=False)
df.write.csv("output/", header=True, mode="overwrite")

# Parquet (Recommended for analytics)
df = spark.read.parquet("path/")
df.write.parquet("output/", mode="overwrite", compression="snappy")

# JSON
df = spark.read.json("path/")
df.write.json("output/", mode="overwrite")

# Avro
df = spark.read.format("avro").load("path/")
df.write.format("avro").save("output/")

# ORC
df = spark.read.orc("path/")
df.write.orc("output/", compression="zlib")

# Delta Lake (ACID transactions)
df = spark.read.format("delta").load("path/")
df.write.format("delta").mode("overwrite").save("output/")

# JDBC (Databases)
df = spark.read.jdbc(
    url="jdbc:postgresql://host:5432/db",
    table="my_table",
    properties={"user": "admin", "password": "pass"}
)
df.write.jdbc(url, table="output_table", mode="append", properties=props)

# Kafka (Streaming)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic") \
    .load()
```

**Format Comparison:**
| Format | Use Case | Compression | Schema Evolution |
|--------|----------|-------------|------------------|
| CSV | Simple exchange | Poor | No |
| JSON | Nested data | Poor | Yes |
| Parquet | Analytics (columnar) | Excellent | Yes |
| Avro | Row-based, streaming | Good | Yes |
| ORC | Hive integration | Excellent | Yes |
| Delta | ACID, time travel | Excellent | Yes |

---

### ✅ Explain Trade-offs Between Pandas and PySpark

**Comparison Matrix:**

| Feature | Pandas | PySpark |
|---------|--------|---------|
| **Data Size** | < 10GB (single machine memory) | Petabytes (distributed) |
| **Speed (10GB)** | Baseline | 5-10× faster with cluster |
| **API** | Rich, mature | Similar but limited |
| **Learning Curve** | Easy | Moderate |
| **Dependencies** | None | Spark cluster |
| **Execution** | Eager (immediate) | Lazy (optimized) |
| **Memory** | All in RAM | Disk + RAM |

**When to Use Pandas:**
```python
✅ Data fits in memory (< 10GB)
✅ Quick prototyping and exploration
✅ Rich library ecosystem (scikit-learn, matplotlib)
✅ Single machine is sufficient
✅ Complex Python UDFs needed

Example:
import pandas as pd
df = pd.read_csv("small_file.csv")  # 100MB
df['new_col'] = df['col1'] * df['col2']
df.groupby('category').mean()
```

**When to Use PySpark:**
```python
✅ Data > 10GB (doesn't fit in memory)
✅ Need horizontal scalability
✅ ETL on large datasets (TB/PB scale)
✅ Cluster already available
✅ Streaming data processing

Example:
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("s3://bucket/large_data/")  # 500GB
df.groupBy("category").avg("value").show()
```

**Hybrid Approach (Pandas on Spark):**
```python
import pyspark.pandas as ps

# Pandas API on Spark engine
df = ps.read_parquet("large_data/")  # Distributed
result = df.groupby("key").sum()  # Pandas syntax, Spark execution
```

**Performance Example:**
```
Task: Group by + aggregate on 10GB data

Pandas (16GB RAM machine):
• Time: 120 seconds
• Memory: 10GB+ (may OOM)

PySpark (5-node cluster, 8GB each):
• Time: 15 seconds (8× faster)
• Memory: Distributed across nodes (no OOM)
```

---

## Cluster Computing

### ✅ Choose Optimal Partition Strategy for Workload

**Partitioning Strategies:**

#### 1. Number of Partitions
```python
# Rule of thumb: 2-4× number of CPU cores
# Example: 10 executors × 4 cores = 40 cores → 80-160 partitions

df = spark.read.csv("data.csv")
print(f"Current partitions: {df.rdd.getNumPartitions()}")

# Repartition (full shuffle)
df_repart = df.repartition(100)  # Increase parallelism

# Coalesce (reduce, no shuffle)
df_coalesced = df.coalesce(10)  # Reduce for final write
```

#### 2. Partition by Column (Bucketing)
```python
# For joins and aggregations
df.write.bucketBy(100, "user_id").sortBy("timestamp").saveAsTable("users")

# Benefits:
# • No shuffle on joins (pre-partitioned)
# • Faster aggregations
# • Sorted within buckets
```

#### 3. Adaptive Query Execution (AQE)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Spark automatically:
# • Coalesces small partitions
# • Splits large partitions
# • Handles skewed joins
```

**Decision Matrix:**
| Scenario | Strategy | Partitions |
|----------|----------|------------|
| Initial read | Default | Auto (128MB/partition) |
| Wide transformations (join, groupBy) | Repartition by key | 2-4× cores |
| Final write | Coalesce | 1 file per output partition |
| Repeated joins | Bucketing | Match join cardinality |
| Skewed data | Salting + repartition | 10× skew factor |

---

### ✅ Implement Broadcast Joins for 2-3× Speedup

**Broadcast Join Pattern:**

```python
from pyspark.sql.functions import broadcast

# Scenario: Large fact table (1TB) join small dimension (10MB)

# ❌ Regular join (shuffle both sides)
df_result = df_fact.join(df_dim, "product_id")
# Cost: Shuffle 1TB + shuffle 10MB = 1.01TB shuffled

# ✅ Broadcast join (no shuffle for small table)
df_result = df_fact.join(broadcast(df_dim), "product_id")
# Cost: Broadcast 10MB to all nodes, no shuffle for 1TB
# Speedup: 2-3× faster

# Automatic broadcast (config)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")  # Default: 10MB
```

**When to Broadcast:**
```
✅ Small table < 10MB (default threshold)
✅ One side much smaller than other (1:100 ratio)
✅ Repeated joins (cache broadcast variable)
✅ Star schema (fact table with dimension tables)

❌ Both tables large (> 100MB)
❌ Small table frequently changing
❌ Limited driver memory
```

**Multi-level Broadcast:**
```python
# Join multiple dimension tables
df_result = df_fact \
    .join(broadcast(df_products), "product_id") \
    .join(broadcast(df_customers), "customer_id") \
    .join(broadcast(df_regions), "region_id")

# All dimensions broadcast, only fact table processed in place
```

---

### ✅ Use Salting to Handle Skewed Joins

**Salting Implementation:**

```python
from pyspark.sql.functions import rand, concat, lit, explode, array

# Problem: Customer "WHALE" has 90% of orders (hot key)

# Step 1: Add salt to left table (fact table)
salt_factor = 10
df_orders_salted = df_orders.withColumn(
    "salt", (rand() * salt_factor).cast("int")
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

# Step 2: Explode right table (dimension table)
df_customers_exploded = df_customers.withColumn(
    "salt", explode(array([lit(i) for i in range(salt_factor)]))
).withColumn(
    "customer_id_salted",
    concat(col("customer_id"), lit("_"), col("salt"))
)

# Step 3: Join on salted key
df_result = df_orders_salted.join(
    df_customers_exploded,
    "customer_id_salted"
).drop("salt", "customer_id_salted")

# Result: WHALE customer split across 10 partitions
# • Before: 1 partition with 90% data (takes 100 seconds)
# • After: 10 partitions with 9% each (takes 10 seconds)
# • Speedup: 10× faster
```

**Automatic Skew Handling (AQE):**
```python
# Spark 3.0+ can handle skew automatically
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Spark will:
# • Detect partitions > 256MB and 5× median
# • Split skewed partitions automatically
# • Replicate join side
```

---

### ✅ Configure Cluster Managers (YARN, Kubernetes, Standalone)

**Cluster Manager Comparison:**

| Feature | YARN | Kubernetes | Standalone |
|---------|------|------------|------------|
| **Best For** | Hadoop ecosystem | Cloud-native | Development |
| **Complexity** | High | Medium | Low |
| **Resource Mgmt** | Excellent | Good | Basic |
| **Multi-tenancy** | Yes | Yes | No |
| **Dynamic Allocation** | Yes | Yes (3.1+) | No |

#### YARN Configuration
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 4 \
  --executor-memory 8G \
  --driver-memory 4G \
  --conf spark.yarn.submit.waitAppCompletion=false \
  app.py
```

#### Kubernetes Configuration
```yaml
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: spark
    image: spark:3.5.0
    env:
    - name: SPARK_MASTER
      value: "k8s://https://kubernetes:443"
```

```bash
spark-submit \
  --master k8s://https://k8s-api:443 \
  --deploy-mode cluster \
  --name spark-app \
  --conf spark.kubernetes.container.image=spark:3.5.0 \
  --conf spark.executor.instances=10 \
  --conf spark.kubernetes.namespace=spark \
  app.py
```

#### Standalone Configuration
```bash
# Start master
$SPARK_HOME/sbin/start-master.sh

# Start workers
$SPARK_HOME/sbin/start-worker.sh spark://master:7077

# Submit application
spark-submit \
  --master spark://master:7077 \
  --executor-memory 4G \
  --total-executor-cores 16 \
  app.py
```

---

### ✅ Tune Memory and Executor Resources

**Memory Configuration:**

```bash
spark-submit \
  --executor-memory 10G \
  --executor-cores 5 \
  --num-executors 20 \
  --driver-memory 4G \
  --conf spark.memory.fraction=0.6 \         # 60% for execution + storage
  --conf spark.memory.storageFraction=0.5 \  # 50% of above for storage
  --conf spark.executor.memoryOverhead=2G \  # Off-heap memory
  app.py
```

**Executor Memory Breakdown:**
```
Total Executor Memory (10G)
├─ Reserved (300MB) - Spark internal
├─ Spark Memory (6G) - 60% of (10G - 300MB)
│  ├─ Execution Memory (3G) - Shuffles, joins, sorts
│  └─ Storage Memory (3G) - Caching, broadcasts
└─ User Memory (3.7G) - User data structures, UDFs
```

**Sizing Guidelines:**
```
Executor Cores: 4-5 per executor (optimal)
• Too few (1-2): Underutilization
• Too many (> 5): GC overhead

Executor Memory: 32-64GB max
• Too little: Frequent spilling to disk
• Too much: GC pauses, memory waste

Number of Executors:
• Formula: (Total Cluster Cores / Executor Cores)
• Leave 1 core + 1GB per node for OS

Example Cluster (10 nodes, 16 cores each, 64GB RAM):
• Executor Cores: 5
• Executor Memory: 10G (64GB / 6 executors per node)
• Number of Executors: (10 × 16) / 5 = 32
• Driver Memory: 4G
```

**Dynamic Allocation:**
```bash
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=5 \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.initialExecutors=10
```

---

### ✅ Minimize Shuffles Using AQE and Bucketing

**Shuffle Minimization Strategies:**

#### 1. Adaptive Query Execution (AQE)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Benefits:
# • Coalesces small partitions post-shuffle
# • Converts sort-merge join to broadcast if small
# • Handles skewed partitions
# • Optimizes based on runtime statistics
```

#### 2. Bucketing (Pre-shuffle)
```python
# Write data pre-partitioned by join key
df.write \
  .bucketBy(100, "user_id") \
  .sortBy("timestamp") \
  .mode("overwrite") \
  .saveAsTable("users")

df2.write \
  .bucketBy(100, "user_id") \  # Same number of buckets!
  .mode("overwrite") \
  .saveAsTable("transactions")

# Join without shuffle (co-located partitions)
result = spark.table("users").join(
    spark.table("transactions"),
    "user_id"
)
# No shuffle! Both tables already partitioned by user_id
```

#### 3. Partition Pruning
```python
# Write partitioned
df.write.partitionBy("year", "month").parquet("output/")

# Read only needed partitions (no full scan)
df_filtered = spark.read.parquet("output/") \
    .filter(col("year") == 2024) \
    .filter(col("month") == "01")
# Reads only year=2024/month=01/ partitions
```

#### 4. Reduce Partition Count Pre-Shuffle
```python
# Before shuffle operation
df.repartition(100, "join_key") \  # Fewer partitions
  .join(df2.repartition(100, "join_key"), "join_key")
```

**Shuffle Cost Comparison:**
| Strategy | Shuffle Size | Speedup |
|----------|--------------|---------|
| No optimization | 100GB | Baseline |
| Broadcast join | 0GB | 10× |
| Bucketing | 0GB | 5× |
| AQE coalesce | 50GB | 2× |
| Partition pruning | 10GB (only needed data) | 10× |

---

### ✅ Read Spark UI to Debug Performance Issues

**Spark UI Navigation:**

```
Spark UI (http://driver:4040)
├─ Jobs Tab
│  └─ Shows jobs and their stages
├─ Stages Tab
│  └─ Stage details, tasks, shuffle metrics
├─ Storage Tab
│  └─ Cached DataFrames
├─ Environment Tab
│  └─ Spark configuration
├─ Executors Tab
│  └─ Executor metrics (memory, GC, tasks)
└─ SQL Tab
   └─ Query plans and DAG visualization
```

**Common Performance Issues:**

#### 1. Task Skew
```
Symptoms:
• Stages Tab: One task takes 100 seconds, others finish in 1 second
• 99% of cluster idle waiting for one slow task

Diagnosis:
• Check "Duration" column in Tasks table
• Look for max/median ratio > 10×

Solution:
• Salting (add random key to split hot partition)
• Enable AQE skew join
```

#### 2. Excessive Shuffles
```
Symptoms:
• Stages Tab: High "Shuffle Read" and "Shuffle Write"
• Long stage duration

Diagnosis:
• Check "Shuffle Read/Write" metrics
• Look for > 100GB shuffles

Solution:
• Broadcast small tables
• Bucketing for repeated joins
• Reduce partition count
```

#### 3. Memory Pressure
```
Symptoms:
• Executors Tab: High GC time (> 10% of task time)
• Tasks spilling to disk (Spill Memory/Disk columns)

Diagnosis:
• Check "GC Time" in Executors tab
• Look for "Spill" metrics in Stages tab

Solution:
• Increase executor memory
• Reduce executor cores (more memory per core)
• Add more executors
• Cache less data
```

#### 4. Driver OOM
```
Symptoms:
• Job fails with "OutOfMemoryError" on driver
• Large "Result Serialization Time"

Diagnosis:
• Stages Tab: Check "Result Size"
• Look for collect() or take(large_n) in code

Solution:
• Avoid collect() on large DataFrames
• Use show(n) or write() instead
• Increase driver memory
```

---

### ✅ Implement Checkpointing for Fault Tolerance

**Checkpointing Strategies:**

#### 1. RDD Checkpointing
```python
# Set checkpoint directory (HDFS/S3 for reliability)
spark.sparkContext.setCheckpointDir("s3://bucket/checkpoints/")

# Checkpoint expensive RDDs
rdd_expensive = df.rdd.map(expensive_operation)
rdd_expensive.checkpoint()  # Save to storage
rdd_expensive.count()  # Trigger checkpoint

# Benefits:
# • Truncates lineage (prevents stack overflow on long DAGs)
# • Fault tolerance (recompute from checkpoint, not source)
```

#### 2. DataFrame Checkpointing
```python
# Eager checkpoint (saves immediately)
df_checkpointed = df.checkpoint(eager=True)

# Lazy checkpoint (saves on first action)
df_checkpointed = df.checkpoint(eager=False)

# Use case: Long iterative algorithms
for i in range(100):
    df = df.transform(expensive_transformation)
    if i % 10 == 0:
        df = df.checkpoint(eager=True)  # Checkpoint every 10 iterations
```

#### 3. Streaming Checkpointing
```python
# Structured Streaming with checkpoints
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3://bucket/output/") \
    .option("checkpointLocation", "s3://bucket/checkpoints/") \
    .start()

# Checkpoint stores:
# • Stream offsets (where we read from)
# • State store (for stateful operations)
# • Enables exactly-once processing
```

**Checkpoint vs Cache:**
| Feature | Cache | Checkpoint |
|---------|-------|------------|
| Storage | Memory (+ disk if spill) | Disk only |
| Lineage | Retained | Truncated |
| Reliability | Lost if executor fails | Persists across failures |
| Speed | Faster (memory) | Slower (disk I/O) |
| Use Case | Temporary reuse | Fault tolerance, long DAGs |

---

## ML Integration

### ✅ Deploy PyTorch Model Using Pandas UDFs

**Pandas UDF Pattern for Model Inference:**

```python
import torch
import pandas as pd
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import FloatType

# 1. Load model and broadcast
model = torch.load("model.pth")
model.eval()
broadcast_model = spark.sparkContext.broadcast(model)

# 2. Define Pandas UDF (vectorized inference)
@pandas_udf(FloatType(), PandasUDFType.SCALAR)
def predict_udf(features: pd.Series) -> pd.Series:
    """
    Vectorized inference on batches of rows.
    
    Why Pandas UDF:
    • Processes batches (not row-by-row)
    • 10-100× faster than regular UDF
    • Uses Arrow for efficient serialization
    """
    # Get model from broadcast
    model = broadcast_model.value
    
    # Convert pandas Series to tensor
    features_array = np.stack(features.values)
    features_tensor = torch.tensor(features_array, dtype=torch.float32)
    
    # Batch inference
    with torch.no_grad():
        predictions = model(features_tensor)
    
    # Return as pandas Series
    return pd.Series(predictions.numpy())

# 3. Apply to DataFrame
df_with_predictions = df.withColumn(
    "prediction",
    predict_udf(col("features"))
)

df_with_predictions.show()
```

**Batch Inference Pattern:**
```python
# Process data in batches (efficient for large datasets)
@pandas_udf("double", PandasUDFType.SCALAR_ITER)
def predict_batch_udf(iterator):
    """
    Process iterator of pandas Series (one per partition).
    Loads model once per partition (not per batch).
    """
    # Load model once per partition
    model = torch.load("model.pth")
    model.eval()
    
    # Process each batch
    for features_series in iterator:
        features_tensor = torch.tensor(
            np.stack(features_series.values),
            dtype=torch.float32
        )
        
        with torch.no_grad():
            predictions = model(features_tensor)
        
        yield pd.Series(predictions.numpy())

# Apply
df_predictions = df.withColumn("pred", predict_batch_udf(col("features")))
```

---

### ✅ Choose Between GPU and CPU for Inference

**Decision Matrix:**

| Factor | CPU | GPU |
|--------|-----|-----|
| **Model Size** | < 100MB | > 100MB |
| **Batch Size** | < 32 | > 64 |
| **Throughput** | < 1000 req/sec | > 10,000 req/sec |
| **Latency** | < 10ms critical | Batch latency OK |
| **Cost** | Lower (existing cluster) | Higher (GPU nodes) |

**GPU Configuration:**
```python
# Spark with GPU support
spark = SparkSession.builder \
    .config("spark.task.resource.gpu.amount", "1") \
    .config("spark.executor.resource.gpu.amount", "1") \
    .config("spark.rapids.sql.enabled", "true") \
    .getOrCreate()

# GPU-accelerated inference
@pandas_udf("double")
def gpu_predict_udf(features: pd.Series) -> pd.Series:
    import torch
    
    # Get broadcast model
    model = broadcast_model.value
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    
    # Move data to GPU
    features_tensor = torch.tensor(
        np.stack(features.values),
        dtype=torch.float32,
        device=device
    )
    
    with torch.no_grad():
        predictions = model(features_tensor)
    
    return pd.Series(predictions.cpu().numpy())
```

**Performance Comparison:**
```
Scenario: Inference on 1M images (ResNet-50)

CPU (16 cores):
• Batch size: 32
• Throughput: 500 images/sec
• Time: 2000 seconds (33 minutes)
• Cost: $5

GPU (8× V100):
• Batch size: 256
• Throughput: 25,000 images/sec
• Time: 40 seconds
• Cost: $2 (spot instances)

GPU Speedup: 50× faster, 60% cheaper!
```

**When to Use CPU:**
- Small models (< 100MB)
- Simple inference (< 100 ops)
- Low latency required (< 10ms)
- Existing CPU cluster available
- Small batch sizes (< 32)

**When to Use GPU:**
- Large models (> 100MB)
- Complex operations (CNNs, Transformers)
- High throughput needed (> 1000/sec)
- Batch inference acceptable
- Large batch sizes (> 64)

---

### ✅ Implement Batch Inference for 5-10× Speedup

**Batch Inference Optimization:**

```python
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import numpy as np

# ❌ Row-by-row inference (SLOW)
def predict_row(features):
    model = load_model()  # Loads model per row!
    return model.predict([features])[0]

df_slow = df.withColumn("pred", udf(predict_row)(col("features")))
# 1M rows × 10ms per row = 10,000 seconds (2.7 hours)

# ✅ Batch inference (FAST)
@pandas_udf("double", PandasUDFType.SCALAR_ITER)
def predict_batch(iterator):
    """
    Iterator of pandas Series (one per partition).
    Processes batches of 1000 rows.
    """
    # Load model once per partition
    model = load_model()
    
    for features_series in iterator:
        # Convert to numpy array (batch)
        features_batch = np.stack(features_series.values)
        
        # Batch prediction
        predictions = model.predict(features_batch)
        
        yield pd.Series(predictions)

df_fast = df.withColumn("pred", predict_batch(col("features")))
# 1M rows / 1000 batch size = 1000 batches × 10ms = 10 seconds
# Speedup: 10,000 / 10 = 1000× faster!
```

**Batch Size Tuning:**
```python
# Small batch: More overhead, less throughput
batch_size = 32  # 100 images/sec

# Medium batch: Balanced
batch_size = 128  # 500 images/sec (optimal for CPU)

# Large batch: Maximum throughput
batch_size = 512  # 2000 images/sec (optimal for GPU)

# Implement dynamic batching
@pandas_udf("double", PandasUDFType.SCALAR_ITER)
def predict_dynamic_batch(iterator):
    model = load_model()
    batch_size = 128
    batch = []
    
    for features_series in iterator:
        for features in features_series:
            batch.append(features)
            
            if len(batch) >= batch_size:
                predictions = model.predict(np.array(batch))
                yield pd.Series(predictions)
                batch = []
    
    # Process remaining
    if batch:
        predictions = model.predict(np.array(batch))
        yield pd.Series(predictions)
```

**Speedup Factors:**
| Batch Size | Overhead | Throughput | Speedup |
|------------|----------|------------|---------|
| 1 (row-by-row) | 95% | 100/sec | 1× |
| 32 | 50% | 500/sec | 5× |
| 128 | 10% | 2000/sec | 20× |
| 512 | 2% | 5000/sec | 50× |

---

### ✅ Broadcast Models Efficiently to Executors

**Model Broadcasting Patterns:**

```python
import torch
from pyspark.broadcast import Broadcast

# 1. Basic Broadcasting
model = torch.load("large_model.pth")
model.eval()
broadcast_model = spark.sparkContext.broadcast(model)

# 2. Use in Pandas UDF
@pandas_udf("double")
def predict_udf(features: pd.Series) -> pd.Series:
    # Access broadcast model (already loaded on executor)
    model = broadcast_model.value
    
    features_tensor = torch.tensor(np.stack(features.values))
    with torch.no_grad():
        predictions = model(features_tensor)
    
    return pd.Series(predictions.numpy())

# 3. Apply inference
df_predictions = df.withColumn("pred", predict_udf(col("features")))
```

**Memory-Efficient Broadcasting (Large Models > 1GB):**
```python
# Option 1: Compress model before broadcast
import pickle
import gzip

# Save compressed
with gzip.open("model_compressed.pkl.gz", "wb") as f:
    pickle.dump(model, f)

# Broadcast compressed
broadcast_compressed = spark.sparkContext.broadcast(
    open("model_compressed.pkl.gz", "rb").read()
)

# Decompress on executor
@pandas_udf("double")
def predict_compressed_udf(features: pd.Series) -> pd.Series:
    import gzip, pickle, io
    
    # Decompress once per executor
    compressed_data = broadcast_compressed.value
    with gzip.open(io.BytesIO(compressed_data), "rb") as f:
        model = pickle.load(f)
    
    # Inference...
    return predictions

# Option 2: Load from distributed storage (S3/HDFS)
@pandas_udf("double")
def predict_from_storage_udf(features: pd.Series) -> pd.Series:
    # Each executor loads model from S3/HDFS (cached)
    model = torch.load("s3://bucket/models/model.pth")
    # Inference...
    return predictions
```

**Performance Comparison:**
```
Model Size: 500MB
Number of Executors: 100

Without Broadcasting:
• Each executor loads model: 500MB × 100 = 50GB network transfer
• Time: 100 seconds (loading from S3)
• Per-partition overhead: 1 second

With Broadcasting:
• Driver broadcasts once: 500MB × 1 = 500MB network transfer
• Time: 5 seconds (broadcast distribution)
• Per-partition overhead: 0 seconds (already loaded)

Speedup: 20× faster startup!
```

---

## Cloud & Production

### ✅ Choose Cloud Provider for PySpark Workload

**Cloud Provider Comparison:**

| Feature | AWS | GCP | Azure |
|---------|-----|-----|-------|
| **Managed Spark** | EMR | Dataproc | HDInsight, Synapse |
| **Pricing** | $$ | $ | $$ |
| **Ease of Use** | Medium | Easy | Medium |
| **Integration** | S3, Glue, Athena | GCS, BigQuery | Blob, Data Factory |
| **Performance** | Good | Excellent | Good |
| **Best For** | Enterprise, AWS ecosystem | Data science, cost | Microsoft ecosystem |

#### AWS EMR Configuration
```bash
aws emr create-cluster \
  --name "Spark Cluster" \
  --release-label emr-6.15.0 \
  --applications Name=Spark \
  --instance-type m5.xlarge \
  --instance-count 10 \
  --use-default-roles \
  --bootstrap-actions Path=s3://bucket/bootstrap.sh
```

#### GCP Dataproc Configuration
```bash
gcloud dataproc clusters create spark-cluster \
  --region us-central1 \
  --num-workers 10 \
  --worker-machine-type n1-standard-4 \
  --image-version 2.1-debian11 \
  --initialization-actions gs://bucket/init.sh
```

#### Azure Synapse Configuration
```python
# Synapse notebook
spark.conf.set("spark.executor.instances", "10")
spark.conf.set("spark.executor.cores", "4")
spark.conf.set("spark.executor.memory", "16g")

df = spark.read.parquet("wasbs://container@account.blob.core.windows.net/data/")
```

**Cost Optimization:**
```
Spot/Preemptible Instances:
• AWS Spot: 70-90% discount
• GCP Preemptible: 80% discount
• Azure Spot: 70-90% discount

Best Practices:
✅ Use spot/preemptible for non-critical workloads
✅ Set max price (avoid price spikes)
✅ Implement checkpointing (handle interruptions)
✅ Use mixed instances (on-demand for driver, spot for executors)

Example (AWS EMR):
--instance-groups \
  InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1,Market=ON_DEMAND \
  InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=5,Market=ON_DEMAND \
  InstanceGroupType=TASK,InstanceType=m5.xlarge,InstanceCount=10,Market=SPOT
```

---

### ✅ Implement Fault-Tolerant Streaming Pipeline

**Structured Streaming with Fault Tolerance:**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count

spark = SparkSession.builder \
    .appName("FaultTolerantStreaming") \
    .config("spark.sql.streaming.checkpointLocation", "s3://bucket/checkpoints/") \
    .getOrCreate()

# 1. Read from Kafka (exactly-once)
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# 2. Parse JSON and transform
df_parsed = df_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 3. Windowed aggregation (stateful)
df_windowed = df_parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("user_id")
    ).agg(count("*").alias("event_count"))

# 4. Write with checkpointing (exactly-once)
query = df_windowed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "s3://bucket/output/") \
    .option("checkpointLocation", "s3://bucket/checkpoints/stream1/") \
    .trigger(processingTime="30 seconds") \
    .start()

query.awaitTermination()
```

**Fault Tolerance Features:**
```python
# 1. Checkpointing (track offsets and state)
.option("checkpointLocation", "s3://bucket/checkpoints/")

# 2. Write-Ahead Log (WAL) for exactly-once
spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")

# 3. Idempotent sinks (prevent duplicates)
# Parquet with checkpoint = exactly-once
# Kafka with transactional writes = exactly-once

# 4. Automatic restart on failure
spark-submit \
  --conf spark.streaming.driver.restartOnFailure=true \
  --conf spark.streaming.backpressure.enabled=true \
  app.py
```

**Recovery Scenarios:**
```
Scenario 1: Executor Failure
• Spark automatically restarts failed tasks
• Reads from last checkpoint
• No data loss (offsets preserved)

Scenario 2: Driver Failure
• Cluster manager restarts driver
• Reads checkpoint directory
• Resumes from last successful batch

Scenario 3: Kafka Broker Failure
• Spark retries with exponential backoff
• Continues from last committed offset
• No data loss (offsets in checkpoint)
```

---

### ✅ Write Unit Tests for Transformations

**PySpark Unit Testing Pattern:**

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Fixture: Create SparkSession
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("unit-tests") \
        .getOrCreate()
    yield spark
    spark.stop()

# Test transformation functions
def clean_data(df):
    """Remove nulls and filter invalid amounts."""
    return df.filter(col("amount").isNotNull()) \
             .filter(col("amount") > 0)

def test_clean_data(spark):
    """Test data cleaning transformation."""
    # Arrange: Create test data
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("amount", IntegerType(), True)
    ])
    
    input_data = [
        ("A", 100),
        ("B", None),  # Should be filtered
        ("C", -50),   # Should be filtered
        ("D", 200)
    ]
    
    df_input = spark.createDataFrame(input_data, schema=schema)
    
    # Act: Apply transformation
    df_result = clean_data(df_input)
    
    # Assert: Check results
    assert df_result.count() == 2
    assert df_result.filter(col("id") == "A").count() == 1
    assert df_result.filter(col("id") == "D").count() == 1
    assert df_result.filter(col("amount") <= 0).count() == 0

# Test aggregations
def test_aggregation(spark):
    """Test groupBy aggregation."""
    # Arrange
    data = [
        ("Sales", 100),
        ("Sales", 200),
        ("Engineering", 300)
    ]
    schema = StructType([
        StructField("dept", StringType()),
        StructField("amount", IntegerType())
    ])
    df = spark.createDataFrame(data, schema=schema)
    
    # Act
    df_agg = df.groupBy("dept").agg(sum("amount").alias("total"))
    
    # Assert
    sales_total = df_agg.filter(col("dept") == "Sales").select("total").first()[0]
    assert sales_total == 300
    
    eng_total = df_agg.filter(col("dept") == "Engineering").select("total").first()[0]
    assert eng_total == 300

# Test with fixtures (reusable test data)
@pytest.fixture
def sample_df(spark):
    """Reusable sample DataFrame."""
    data = [("A", 1), ("B", 2), ("C", 3)]
    return spark.createDataFrame(data, ["key", "value"])

def test_with_fixture(sample_df):
    """Test using fixture."""
    assert sample_df.count() == 3
    assert sample_df.filter(col("value") > 1).count() == 2

# Run tests
# pytest test_transformations.py -v
```

**Testing Best Practices:**
```python
# 1. Test helper function (compare DataFrames)
def assert_dataframes_equal(df1, df2):
    """Compare two DataFrames (schema and data)."""
    # Check schema
    assert df1.schema == df2.schema
    
    # Check row count
    assert df1.count() == df2.count()
    
    # Check data (sorted for consistent comparison)
    diff = df1.exceptAll(df2).union(df2.exceptAll(df1))
    assert diff.count() == 0, f"DataFrames differ: {diff.show()}"

# 2. Parametrized tests
@pytest.mark.parametrize("input,expected", [
    (100, "Small"),
    (500, "Medium"),
    (1500, "Large")
])
def test_categorize(spark, input, expected):
    df = spark.createDataFrame([(input,)], ["amount"])
    df_result = df.withColumn("category",
        when(col("amount") < 100, "Small")
        .when(col("amount") < 1000, "Medium")
        .otherwise("Large")
    )
    actual = df_result.select("category").first()[0]
    assert actual == expected

# 3. Mock external dependencies
from unittest.mock import Mock, patch

def test_with_external_api(spark):
    with patch('mymodule.call_api') as mock_api:
        mock_api.return_value = {"result": "success"}
        # Test code that calls API
        assert process_data(spark) == "success"
```

---

### ✅ Configure Cost-Optimized Cluster

**Cost Optimization Strategies:**

```python
# 1. Spot/Preemptible Instances (70-90% discount)
# AWS EMR
aws emr create-cluster \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceType=m5.xlarge,InstanceCount=1,Market=ON_DEMAND \
    InstanceGroupType=CORE,InstanceType=m5.xlarge,InstanceCount=3,Market=ON_DEMAND \
    InstanceGroupType=TASK,InstanceType=m5.xlarge,InstanceCount=10,Market=SPOT,BidPrice=0.10

# 2. Autoscaling (scale down when idle)
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")

# 3. Right-size executors (avoid over-provisioning)
# Before: 10 executors × 32GB = 320GB
# After: 20 executors × 16GB = 320GB (same total, more parallelism)

# 4. Compress data (reduce storage and I/O costs)
df.write.option("compression", "snappy").parquet("output/")  # 5-10× smaller

# 5. Partition pruning (read only needed data)
df = spark.read.parquet("data/")
df_filtered = df.filter(col("year") == 2024)  # Only reads year=2024 partitions

# 6. Cache wisely (avoid unnecessary caching)
# Cache only frequently-reused DataFrames
df_expensive.cache()  # ✅ Reused 10 times
df_transient.cache()  # ❌ Used once (waste of memory)
```

**Cost Comparison:**
```
Scenario: Process 1TB data daily

Configuration 1 (No Optimization):
• 10 × m5.4xlarge (16 core, 64GB) on-demand
• Cost: $1.92/hour × 10 = $19.20/hour
• Runtime: 4 hours
• Daily cost: $76.80
• Monthly cost: $2,304

Configuration 2 (Optimized):
• 1 × m5.xlarge (4 core, 16GB) on-demand (driver)
• 3 × m5.xlarge on-demand (core nodes, data locality)
• 10 × m5.xlarge spot (task nodes, 80% discount)
• Cost: $0.48/hour × 4 + $0.10/hour × 10 = $2.92/hour
• Runtime: 2 hours (optimized, parallelism)
• Daily cost: $5.84
• Monthly cost: $175

Savings: 92% cheaper ($2,129/month saved)!
```

**Resource Right-Sizing:**
| Workload | Executor Cores | Executor Memory | Count |
|----------|----------------|-----------------|-------|
| Light (< 100GB) | 2 | 4G | 5-10 |
| Medium (100GB-1TB) | 4 | 8G | 10-20 |
| Heavy (> 1TB) | 5 | 16G | 20-50 |

---

## 📚 Additional Resources

**Practice Exercises:**
1. ETL pipeline: Process 100GB CSV → Parquet with transformations
2. Streaming: Kafka source → windowed aggregations → sink
3. ML inference: Deploy PyTorch model using Pandas UDF (batch processing)
4. Optimization: Profile slow join, apply broadcast/salting techniques

**Reference Documentation:**
- Apache Spark Official Docs: https://spark.apache.org/docs/latest/
- Spark SQL Programming Guide
- Structured Streaming Guide
- Performance Tuning Guide
- Spark UI and Monitoring

---

**Last Updated:** December 2024
