# PySpark Interview Quiz Pack - Enhanced Edition
## 40+ Questions with Code Examples, Explanations, and Rationale

---

## 1) What is the difference between Narrow and Wide Transformations?

### Answer:
- **Narrow Transformation**: Each input partition contributes to at most ONE output partition. No shuffle required. Data stays on the same executor.
- **Wide Transformation**: Each input partition contributes to MULTIPLE output partitions. Requires shuffle (data movement across network).

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("NarrowWide").getOrCreate()

# Sample data
data = [("Alice", 25, "NY"), ("Bob", 30, "CA"), ("Charlie", 35, "NY")]
df = spark.createDataFrame(data, ["name", "age", "state"])

# NARROW TRANSFORMATIONS (no shuffle)
df_filtered = df.filter(col("age") > 25)  # Each partition processed independently
df_selected = df.select("name", "age")     # Each partition processed independently
df_mapped = df.withColumn("age_plus_10", col("age") + 10)  # Each partition independent

print("Narrow transformation - partitions:", df_filtered.rdd.getNumPartitions())

# WIDE TRANSFORMATIONS (requires shuffle)
df_grouped = df.groupBy("state").count()   # Must collect all NY records together
df_sorted = df.orderBy("age")              # Must reorder across partitions
df_distinct = df.distinct()                # Must check all partitions for duplicates

print("Wide transformation - partitions:", df_grouped.rdd.getNumPartitions())
```

### Rationale:
- **GOOD**: Narrow transformations are fast - no network I/O, pipelined execution
- **BAD**: Wide transformations are expensive - shuffle writes to disk, network transfer, shuffle reads
- **Best Practice**: Chain narrow transformations together, minimize wide transformations

---

## 2) What is a Directed Acyclic Graph (DAG)? What is it really doing?

### Answer:
A DAG is Spark's execution plan that represents the sequence of transformations as a graph:
- **Directed**: Operations flow in one direction (from source to result)
- **Acyclic**: No loops/cycles (can't go back to previous step)
- **Graph**: Nodes are RDDs/DataFrames, edges are transformations

The DAG scheduler optimizes this graph by:
1. Combining transformations (pipelining)
2. Identifying stage boundaries (at shuffles)
3. Scheduling tasks efficiently

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("DAGDemo").getOrCreate()

# Create data
sales = spark.createDataFrame([
    (1, "Electronics", 1000),
    (2, "Clothing", 500),
    (3, "Electronics", 1500),
    (4, "Clothing", 700)
], ["id", "category", "amount"])

# Build a DAG of transformations (LAZY - not executed yet)
filtered = sales.filter(col("amount") > 600)      # Transformation 1
grouped = filtered.groupBy("category")            # Transformation 2
aggregated = grouped.agg(_sum("amount").alias("total"))  # Transformation 3
sorted_result = aggregated.orderBy("total")       # Transformation 4

# At this point, NOTHING has executed - just building the DAG

# View the execution plan
sorted_result.explain(True)

# NOW trigger execution with an action
sorted_result.show()  # Action - DAG gets optimized and executed
```

### What the DAG is really doing:
```
Stage 1 (no shuffle):
  Read data → filter (amount > 600) → map to (category, amount)
  
Stage 2 (shuffle boundary):
  Shuffle data by category → groupBy → sum aggregation
  
Stage 3 (shuffle boundary):
  Shuffle for global sort → orderBy
  
Stage 4 (no shuffle):
  Collect results → show()
```

### Rationale:
- **GOOD**: Optimizer can reorder operations (predicate pushdown, projection pruning)
- **GOOD**: Combines multiple transformations into single stages
- **GOOD**: Identifies opportunities for broadcast joins
- **Best Practice**: Use `.explain()` to understand your execution plan

---

## 3) What does "waiting for an action to be called" mean in Lazy Evaluation?

### Answer:
Lazy evaluation means transformations DON'T execute immediately. Spark builds a DAG and waits until an ACTION is called to:
1. Optimize the entire plan
2. Execute all transformations at once
3. Return results or write output

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder.appName("LazyEval").getOrCreate()

# Large dataset simulation
data = [(i, f"name_{i}", i * 10) for i in range(1000000)]
df = spark.createDataFrame(data, ["id", "name", "value"])

print("Starting transformations...")
start_time = time.time()

# LAZY TRANSFORMATIONS (executed instantly - just building plan)
df1 = df.filter(col("value") > 5000)           # Not executed
df2 = df1.select("id", "name")                 # Not executed
df3 = df2.filter(col("id") % 2 == 0)          # Not executed
df4 = df3.withColumn("double_id", col("id") * 2)  # Not executed

print(f"Transformations 'completed' in: {time.time() - start_time:.4f}s")  # ~0.001s

# Nothing has actually executed yet!
# Spark has only recorded the transformations in a DAG

print("\nNow calling action...")
action_start = time.time()

# ACTION - triggers execution of ALL transformations
result = df4.count()  # NOW everything executes

print(f"Action completed in: {time.time() - action_start:.4f}s")  # ~2-5s
print(f"Result count: {result}")

# Another action - Spark re-executes the entire DAG
print("\nCalling another action...")
df4.show(5)  # Re-executes all transformations again

# To avoid re-execution, cache the result
df4_cached = df4.cache()
df4_cached.count()  # First execution + caching
df4_cached.show(5)  # Uses cached data - much faster
```

### Why Lazy Evaluation?

```python
# WITHOUT LAZY EVALUATION (hypothetical):
df1 = df.filter(col("value") > 5000)     # Executes, writes to disk
df2 = df1.select("id", "name")           # Executes, writes to disk
df3 = df2.filter(col("id") % 2 == 0)    # Executes, writes to disk
# Many disk I/O operations, no optimization

# WITH LAZY EVALUATION (actual Spark):
df1 = df.filter(col("value") > 5000)     # Just records
df2 = df1.select("id", "name")           # Just records
df3 = df2.filter(col("id") % 2 == 0)    # Just records
df3.show()  # Optimizes: combines all filters, avoids intermediate writes
# Single optimized execution, pipelined operations
```

### Rationale:
- **GOOD**: Spark can optimize the entire plan before execution
- **GOOD**: Avoids unnecessary intermediate computations
- **GOOD**: Can push filters down to data source (Parquet)
- **BAD**: Debugging is harder - errors appear at action, not transformation
- **Best Practice**: Cache DataFrames you'll reuse to avoid re-execution

---

## 4) What does Spark's DAG Scheduler do?

### Answer:
The DAG scheduler converts your logical plan into physical execution:
1. **Builds DAG**: Creates directed acyclic graph of all transformations
2. **Identifies Stages**: Splits job at shuffle boundaries (wide transformations)
3. **Schedules Tasks**: Creates one task per partition within each stage
4. **Optimizes**: Pipelines narrow transformations, identifies broadcast opportunities

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

spark = SparkSession.builder.appName("DAGScheduler").getOrCreate()

# Sample data
orders = spark.createDataFrame([
    (1, "2024-01-01", "Alice", 100),
    (2, "2024-01-01", "Bob", 200),
    (3, "2024-01-02", "Alice", 150),
    (4, "2024-01-02", "Charlie", 300)
], ["order_id", "date", "customer", "amount"])

# Complex query with multiple stages
result = orders \
    .filter(col("amount") > 50) \        # Stage 1: Narrow transformation
    .groupBy("customer") \                # Stage 2: Wide - shuffle by customer
    .agg(_sum("amount").alias("total")) \ # Stage 2: Continues
    .filter(col("total") > 200) \         # Stage 3: Narrow after shuffle
    .orderBy(col("total").desc())         # Stage 4: Wide - global sort

# View how DAG scheduler breaks this into stages
result.explain(mode="formatted")

# Trigger execution - watch Spark UI for stages
result.show()
```

### How DAG Scheduler Breaks This Down:

```
Job 0: result.show()
│
├─ Stage 0 (ShuffleMapStage)
│  │  Tasks: 8 (one per partition)
│  │  Operations: Read data → filter(amount > 50) → map to (customer, amount)
│  │  Output: Write shuffle files for Stage 1
│  
├─ Stage 1 (ShuffleMapStage)  
│  │  Dependency: Shuffle Read from Stage 0
│  │  Tasks: 8 (configurable via spark.sql.shuffle.partitions)
│  │  Operations: Read shuffle → groupBy → sum → filter(total > 200) → map for sort
│  │  Output: Write shuffle files for Stage 2
│
└─ Stage 2 (ResultStage)
   │  Dependency: Shuffle Read from Stage 1
   │  Tasks: 8
   │  Operations: Read shuffle → global sort → collect results
   │  Output: Return to driver for show()
```

### Rationale:
- **GOOD**: Parallelizes work across executors (8 tasks = 8 parallel operations)
- **GOOD**: Pipelines narrow transformations within stages (no intermediate writes)
- **BAD**: Shuffles between stages are expensive (disk + network)
- **Best Practice**: Minimize shuffles by filtering early, using broadcast joins

---

## 5) Why is `collect()` dangerous?

### Answer:
`collect()` brings ALL data from all executors to the driver. If your dataset is large, you'll:
- Run out of driver memory
- Crash your Spark application
- Lose distributed processing benefits

### Code Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CollectDanger") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Simulate large dataset (10 million rows)
large_df = spark.range(0, 10000000)

# DANGEROUS - tries to collect 10M rows to driver
try:
    all_data = large_df.collect()  # Out of memory!
    print(f"Collected {len(all_data)} rows")  # May never reach here
except Exception as e:
    print(f"ERROR: {e}")

# SAFE ALTERNATIVES:

# 1. Use show() to see sample
large_df.show(20)  # Only brings 20 rows to driver

# 2. Use take(n) for limited results
sample = large_df.take(100)  # Only 100 rows to driver
print(f"Sample size: {len(sample)}")

# 3. Use count() for aggregated info
total = large_df.count()  # Only the count (one number) to driver
print(f"Total rows: {total}")

# 4. Write to storage instead
large_df.write.mode("overwrite").parquet("/tmp/output")
print("Data written to storage")

# 5. Use toPandas() only on small aggregated results
summary = large_df.groupBy().count()  # One row
pandas_df = summary.toPandas()  # Safe - only 1 row
print(pandas_df)
```

### Rationale:
- **BAD**: `collect()` defeats distributed computing purpose
- **BAD**: Driver becomes bottleneck and single point of failure
- **GOOD**: Actions like `count()`, `show()`, `write()` keep processing distributed
- **Best Practice**: Only collect aggregated/filtered small results

---

## 6) When to use Broadcast Joins?

### Answer:
Use broadcast joins when joining a **large table** with a **small table** (< 10MB by default, configurable up to ~100MB). Spark copies the small table to every executor, avoiding shuffle of the large table.

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastJoin").getOrCreate()

# LARGE table - 10 million sales records
sales = spark.range(0, 10000000).toDF("sale_id") \
    .withColumn("product_id", (col("sale_id") % 1000).cast("int")) \
    .withColumn("amount", (col("sale_id") % 100) + 10)

# SMALL table - 1000 products (fits in memory)
products = spark.range(0, 1000).toDF("product_id") \
    .withColumn("product_name", concat(lit("Product_"), col("product_id")))

print(f"Sales size: {sales.count()} rows")
print(f"Products size: {products.count()} rows")

# BAD: Regular join - shuffles BOTH tables (expensive!)
regular_join = sales.join(products, "product_id")
print("\nRegular Join Plan:")
regular_join.explain()
# You'll see: Exchange (shuffle) on BOTH sides

# GOOD: Broadcast join - only small table copied, no shuffle of large table
broadcast_join = sales.join(broadcast(products), "product_id")
print("\nBroadcast Join Plan:")
broadcast_join.explain()
# You'll see: BroadcastHashJoin, no shuffle of sales table

# Execute and compare
import time

start = time.time()
regular_join.count()
print(f"Regular join time: {time.time() - start:.2f}s")

start = time.time()
broadcast_join.count()
print(f"Broadcast join time: {time.time() - start:.2f}s")
```

### When NOT to broadcast:

```python
# DON'T broadcast if small table is actually large
large_products = spark.range(0, 50000000)  # 50M rows - too big!
# This will cause out-of-memory errors on executors
bad_join = sales.join(broadcast(large_products), "product_id")  # ❌ BAD

# Spark auto-broadcasts if < 10MB, but you can configure:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600)  # 100MB
```

### Rationale:
- **GOOD**: Avoids expensive shuffle of large table (10x-100x faster)
- **GOOD**: Reduces network I/O and disk writes
- **BAD**: If broadcasted table is too large → executor OOM
- **Best Practice**: Broadcast dimension tables, not fact tables

---

## 7) What is a Shuffle? (Deep Dive with Visual Examples)

### Answer:
A **shuffle** is the process of redistributing data across partitions and executors in a Spark cluster. It's one of the most expensive operations in Spark because it involves:

1. **Disk I/O**: Writing shuffle files to local disk (shuffle write)
2. **Network I/O**: Transferring data across the network between executors
3. **Serialization/Deserialization**: Converting data to bytes and back
4. **Blocking**: Stage boundaries - next stage can't start until shuffle completes
5. **Memory Pressure**: Sorting/hashing data in memory before writing

### When Does a Shuffle Happen?
- **groupBy()** - Data with same key must be on same partition
- **join()** - Matching keys from both tables must be co-located
- **distinct()** - Must check all partitions for duplicates
- **repartition()** - Explicitly redistributing data
- **orderBy()/sort()** - Global sorting requires all data reorganization
- **aggregateByKey()** - Similar to groupBy

### Visual Explanation: What Really Happens During a Shuffle

```
BEFORE SHUFFLE (4 Executors, 4 Partitions):
========================================

Executor 1          Executor 2          Executor 3          Executor 4
┌─────────┐         ┌─────────┐         ┌─────────┐         ┌─────────┐
│ Part 0  │         │ Part 1  │         │ Part 2  │         │ Part 3  │
│ A: 100  │         │ B: 200  │         │ A: 150  │         │ B: 250  │
│ C: 300  │         │ A: 120  │         │ C: 350  │         │ A: 180  │
│ B: 400  │         │ C: 220  │         │ B: 450  │         │ C: 280  │
└─────────┘         └─────────┘         └─────────┘         └─────────┘

Operation: df.groupBy("key").sum("value")

SHUFFLE PHASE:
==============
Each executor writes shuffle files (one file per output partition):

Executor 1 writes:
  → shuffle_0_A.data (contains A: 100)
  → shuffle_0_B.data (contains B: 400)
  → shuffle_0_C.data (contains C: 300)

Executor 2 writes:
  → shuffle_1_A.data (contains B: 200, A: 120)
  → shuffle_1_B.data (contains empty)
  → shuffle_1_C.data (contains C: 220)
  
... Network Transfer ...

AFTER SHUFFLE (Redistributed by Key):
======================================

Executor 1          Executor 2          Executor 3
┌─────────┐         ┌─────────┐         ┌─────────┐
│ All A's │         │ All B's │         │ All C's │
│ 100     │         │ 200     │         │ 300     │
│ 120     │         │ 250     │         │ 220     │
│ 150     │         │ 400     │         │ 350     │
│ 180     │         │ 450     │         │ 280     │
└─────────┘         └─────────┘         └─────────┘
  ↓ Sum               ↓ Sum               ↓ Sum
A: 550             B: 1300            C: 1150
```

### Comprehensive Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg
import time

spark = SparkSession.builder \
    .appName("ShuffleDeepDive") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Create sample data across 4 partitions
data = []
for i in range(100000):
    category = f"cat_{i % 10}"  # 10 different categories
    data.append((i, category, i * 10, f"2024-01-{(i % 28) + 1:02d}"))

df = spark.createDataFrame(data, ["id", "category", "value", "date"]).repartition(4)

print(f"Initial partitions: {df.rdd.getNumPartitions()}")
print(f"Initial data distribution:")
df.rdd.mapPartitionsWithIndex(
    lambda idx, it: [(idx, sum(1 for _ in it))]
).collect()

# ============================================
# EXAMPLE 1: NO SHUFFLE (Narrow Transformation)
# ============================================
print("\n" + "="*80)
print("EXAMPLE 1: NO SHUFFLE - Filter + Select")
print("="*80)

start = time.time()
no_shuffle = df.filter(col("value") > 50000).select("category", "value")
no_shuffle.explain()
result1 = no_shuffle.count()
print(f"Time: {time.time() - start:.3f}s")
print("Notice: NO 'Exchange' in plan = NO SHUFFLE")

# ============================================
# EXAMPLE 2: SHUFFLE - GroupBy Aggregation
# ============================================
print("\n" + "="*80)
print("EXAMPLE 2: SHUFFLE - GroupBy")
print("="*80)

start = time.time()
shuffle_groupby = df.groupBy("category").agg(
    count("*").alias("count"),
    _sum("value").alias("total"),
    avg("value").alias("average")
)
shuffle_groupby.explain()
print("\nNotice: 'Exchange hashpartitioning(category)' = SHUFFLE!")
result2 = shuffle_groupby.collect()
print(f"Time: {time.time() - start:.3f}s")
print(f"Result partitions: {shuffle_groupby.rdd.getNumPartitions()}")

# ============================================
# EXAMPLE 3: SHUFFLE - Join
# ============================================
print("\n" + "="*80)
print("EXAMPLE 3: SHUFFLE - Join (without broadcast)")
print("="*80)

# Second dataset
categories_df = spark.createDataFrame([
    ("cat_0", "Electronics"),
    ("cat_1", "Clothing"),
    ("cat_2", "Food"),
    ("cat_3", "Books"),
    ("cat_4", "Toys"),
], ["category", "name"])

start = time.time()
joined = df.join(categories_df, "category", "inner")
joined.explain()
print("\nNotice: 'Exchange' on BOTH sides = BOTH tables shuffled!")
result3 = joined.count()
print(f"Time: {time.time() - start:.3f}s")

# ============================================
# EXAMPLE 4: SHUFFLE - Distinct
# ============================================
print("\n" + "="*80)
print("EXAMPLE 4: SHUFFLE - Distinct")
print("="*80)

start = time.time()
distinct_vals = df.select("category").distinct()
distinct_vals.explain()
print("\nNotice: 'Exchange' = Must check all partitions for duplicates")
result4 = distinct_vals.count()
print(f"Time: {time.time() - start:.3f}s")

# ============================================
# EXAMPLE 5: SHUFFLE - OrderBy (Global Sort)
# ============================================
print("\n" + "="*80)
print("EXAMPLE 5: SHUFFLE - Global Sort")
print("="*80)

start = time.time()
sorted_df = df.orderBy(col("value").desc())
sorted_df.explain()
print("\nNotice: 'Exchange rangepartitioning' = Shuffle for global sort")
result5 = sorted_df.take(10)
print(f"Time: {time.time() - start:.3f}s")

# ============================================
# VISUALIZING SHUFFLE IN SPARK UI
# ============================================
print("\n" + "="*80)
print("HOW TO MONITOR SHUFFLES IN SPARK UI")
print("="*80)
print("""
1. Open http://localhost:4040 in your browser
2. Go to 'Stages' tab
3. Look for stages with 'Exchange' operations
4. Check metrics:
   - Shuffle Write: How much data written to disk
   - Shuffle Read: How much data read from other executors
   - Shuffle Spill (Memory): Data spilled to disk due to memory pressure
   - Task Duration: Look for slow tasks (skew!)

Example metrics you might see:
┌──────────────────┬────────────────┬───────────────┐
│ Stage            │ Shuffle Write  │ Shuffle Read  │
├──────────────────┼────────────────┼───────────────┤
│ Stage 0 (scan)   │ 150 MB         │ 0 MB          │
│ Stage 1 (group)  │ 0 MB           │ 150 MB        │
└──────────────────┴────────────────┴───────────────┘
""")

# ============================================
# WHAT HAPPENS UNDER THE HOOD
# ============================================
print("\n" + "="*80)
print("WHAT HAPPENS DURING A SHUFFLE (Step by Step)")
print("="*80)
print("""
1. MAP SIDE (Shuffle Write):
   - Task computes result for its partition
   - Groups records by hash(key) % num_output_partitions
   - Writes one file per output partition to local disk
   - Example: 4 input tasks × 8 output partitions = 32 shuffle files

2. SHUFFLE SERVICE:
   - External shuffle service (or executor) serves shuffle files
   - Files stay on disk even if executor dies (fault tolerance)

3. REDUCE SIDE (Shuffle Read):
   - Tasks in next stage fetch their partition data
   - Reads from ALL upstream tasks (network transfer)
   - Example: Task 0 reads partition 0 from all 4 upstream tasks
   - Sorts/hashes data in memory (may spill to disk if too large)

4. AGGREGATION/JOIN:
   - Now all records with same key are on same partition
   - Can perform groupBy, join, etc. locally

COST BREAKDOWN:
- Disk Write: Serialization + write shuffle files (slow)
- Network Transfer: Move data between executors (slow + network limited)
- Disk Read: Deserialize shuffle files (slow)
- Memory: Sort/hash data (can cause GC pressure)
- Blocking: Stage barrier (next stage waits for all shuffle writes)
""")
```

### Real-World Example: Shuffle Cost

```python
# Scenario: 1 TB dataset, 100 executors, groupBy operation

# Stage 0: Map side (shuffle write)
# - Each executor processes 10 GB
# - Writes shuffle files: 10 GB × 100 executors = 1 TB disk writes
# - Time: ~30 seconds (disk I/O bound)

# Shuffle Transfer:
# - 1 TB data transfer across network
# - With 10 Gbps network: ~800 seconds (13 minutes!)

# Stage 1: Reduce side (shuffle read)
# - Each executor reads its partition: ~10 GB
# - Time: ~30 seconds

# TOTAL SHUFFLE COST: ~15 minutes
# vs narrow transformation: ~30 seconds

# That's 30x slower due to shuffle!
```

### Minimizing Shuffle Cost:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder.appName("MinimizeShuffle").getOrCreate()

# ❌ BAD: Multiple unnecessary shuffles
result = df \
    .repartition(10) \              # Shuffle 1
    .filter(col("value") > 1000) \  # Narrow (but too late!)
    .groupBy("category") \           # Shuffle 2
    .count()

# ✅ GOOD: Filter first, minimize data
result = df \
    .filter(col("value") > 1000) \  # Narrow - reduce data by 90%
    .groupBy("category") \           # Shuffle only 10% of data
    .count()

# ✅ BETTER: Use broadcast for small tables
small_table = spark.range(10).toDF("category")
result = df.join(broadcast(small_table), "category")  # No shuffle of large table!

# ✅ BEST: Tune shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")  # Default
# For small data: lower (8-16)
# For huge data: higher (500-1000)

# ✅ ADVANCED: Pre-partition data during write
df.write \
    .partitionBy("date") \           # Physical partitioning
    .parquet("/data/output")

# Later reads with date filter avoid shuffle:
spark.read.parquet("/data/output") \
    .filter(col("date") == "2024-01-01")  # Reads only one folder!
```

### Key Shuffle Metrics to Monitor:

```python
# Monitor shuffle metrics in your code
print(f"Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# After running a query, check:
# 1. Spark UI → SQL tab → Query execution
# 2. Look for 'Exchange' nodes (shuffle operations)
# 3. Check 'Details' for shuffle read/write sizes
# 4. Look at task metrics for data skew

# Enable verbose metrics
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Common Shuffle Operations and Their Cost:

```python
# OPERATION          SHUFFLE?   COST    WHEN TO USE
# ================================================================
# filter()           NO         Low     Always use early
# select()           NO         Low     Always use early
# map()              NO         Low     Use for transformations
# withColumn()       NO         Low     Add computed columns
# ----------------------------------------------------------------
# groupBy()          YES        High    When you need aggregation
# join() (regular)   YES        High    Large-large table joins
# distinct()         YES        Medium  Remove duplicates
# repartition()      YES        Medium  Increase parallelism
# orderBy()          YES        High    Global sorting needed
# ----------------------------------------------------------------
# join(broadcast)    NO         Low     Small-large table joins
# coalesce()         NO/MIN     Low     Reduce partitions
```

### Rationale:
- **BAD**: Shuffle involves disk I/O + network I/O + serialization = SLOW (10-100x slower)
- **BAD**: Creates stage boundaries (blocks pipeline execution)
- **BAD**: Can cause memory pressure (sorting/hashing in memory)
- **BAD**: Potential for data skew (some tasks much slower)
- **GOOD**: Necessary for distributed aggregations and joins
- **GOOD**: Can be optimized with proper partitioning and broadcast
- **Best Practice**: 
  - Filter early (before shuffle) to reduce data volume
  - Use broadcast joins for small tables
  - Tune `spark.sql.shuffle.partitions` based on data size
  - Monitor Spark UI for shuffle metrics
  - Enable Adaptive Query Execution (AQE) for automatic optimization
  - Pre-partition data during write for common query patterns

---

## 8) Explain `repartition` vs `coalesce`

### Answer:
Both change partition count, but differently:
- **repartition(n)**: Full shuffle, can increase/decrease, balanced distribution
- **coalesce(n)**: Minimal shuffle (only when increasing), can only decrease efficiently, may be unbalanced

### Code Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RepartitionDemo").getOrCreate()

# Start with 100 partitions
df = spark.range(0, 1000000).repartition(100)
print(f"Initial: {df.rdd.getNumPartitions()} partitions")

# REPARTITION - full shuffle, balanced
repartitioned = df.repartition(10)
print(f"Repartition to 10: {repartitioned.rdd.getNumPartitions()} partitions")

# Check partition sizes
partition_sizes = repartitioned.rdd.mapPartitions(
    lambda it: [sum(1 for _ in it)]
).collect()
print(f"Partition sizes (repartition): {partition_sizes}")
# Output: [100000, 100000, 100000, ...] - balanced

# COALESCE - minimal shuffle, may be unbalanced
coalesced = df.coalesce(10)
print(f"Coalesce to 10: {coalesced.rdd.getNumPartitions()} partitions")

partition_sizes = coalesced.rdd.mapPartitions(
    lambda it: [sum(1 for _ in it)]
).collect()
print(f"Partition sizes (coalesce): {partition_sizes}")
# Output: [10000, 10000, 10000, ...] - may vary

# Use cases:

# 1. INCREASE partitions (must use repartition)
increased = df.repartition(200)  # ✅ Works
# coalesce(200) won't work efficiently

# 2. DECREASE partitions before write
df.coalesce(1).write.csv("/tmp/output")  # ✅ One file
# Cheaper than repartition(1)

# 3. REPARTITION by column for joins
df_repart = df.repartition(10, "id")  # Hash partition by id
# Better join performance

# 4. COALESCE after filter
filtered = df.filter(col("id") > 900000)  # Only 10% of data remains
coalesced = filtered.coalesce(10)  # Reduce empty partitions
```

### Visual Example:

```
REPARTITION (10 → 3):
Before: [P0] [P1] [P2] [P3] [P4] [P5] [P6] [P7] [P8] [P9]
        ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓
        └────┴────┴────┴────┼────┴────┴────┴────┴────┘
                          SHUFFLE (full redistribution)
        ↓              ↓              ↓
After: [P0-balanced] [P1-balanced] [P2-balanced]

COALESCE (10 → 3):
Before: [P0] [P1] [P2] [P3] [P4] [P5] [P6] [P7] [P8] [P9]
        └─┬─┘ └─┬─┘ └─┬─┘ └──┘ └──┘ └──┘ └──┘ └──┘ └──┘
          ↓     ↓     ↓        (minimal movement)
After: [P0] [P1] [P2]
```

### Rationale:
- **repartition**: Use when increasing partitions or need balanced distribution
- **coalesce**: Use when decreasing partitions and don't need perfect balance
- **GOOD**: coalesce is faster (less shuffle)
- **BAD**: coalesce can create unbalanced partitions
- **Best Practice**: coalesce before write to reduce output files

---

## 9) What is Caching/Persisting and when to use it?

### Answer:
Caching stores a DataFrame/RDD in memory (or memory+disk) to avoid recomputing. Use when you access the same DataFrame multiple times.

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

spark = SparkSession.builder.appName("CachingDemo").getOrCreate()

# Expensive transformation
df = spark.range(0, 10000000)
expensive_df = df.filter(col("id") % 2 == 0) \
    .withColumn("squared", col("id") * col("id")) \
    .filter(col("squared") > 1000000)

# WITHOUT CACHING - recomputes every time
print("WITHOUT CACHING:")
start = time.time()
count1 = expensive_df.count()
print(f"First count: {time.time() - start:.2f}s - {count1}")

start = time.time()
count2 = expensive_df.count()
print(f"Second count: {time.time() - start:.2f}s - {count2}")
# Both take ~same time - recomputed from scratch

# WITH CACHING
print("\nWITH CACHING:")
cached_df = expensive_df.cache()  # Mark for caching

start = time.time()
count1 = cached_df.count()  # First action: computes + caches
print(f"First count (compute+cache): {time.time() - start:.2f}s")

start = time.time()
count2 = cached_df.count()  # Second action: reads from cache
print(f"Second count (from cache): {time.time() - start:.2f}s")
# Second is much faster!

start = time.time()
cached_df.show(5)  # Also uses cached data
print(f"Show (from cache): {time.time() - start:.2f}s")

# Storage levels
from pyspark import StorageLevel

# Memory only (default for cache())
df.persist(StorageLevel.MEMORY_ONLY)

# Memory and disk (spillover if not enough memory)
df.persist(StorageLevel.MEMORY_AND_DISK)

# Serialized in memory (less space, more CPU)
df.persist(StorageLevel.MEMORY_ONLY_SER)

# Replicated (for fault tolerance)
df.persist(StorageLevel.MEMORY_AND_DISK_2)

# Check cached data in Spark UI
# http://localhost:4040 → Storage tab

# Unpersist when done
cached_df.unpersist()
```

### When to Cache:

```python
# GOOD: Reused DataFrame
base_df = spark.read.parquet("/data/large_dataset")
cached_base = base_df.cache()

# Use multiple times
result1 = cached_base.filter(col("category") == "A").count()
result2 = cached_base.filter(col("category") == "B").count()
result3 = cached_base.groupBy("region").count()
# Each reuses cached data

# BAD: Don't cache if used once
df = spark.read.csv("/data/file.csv")
df.cache()  # ❌ Unnecessary
df.count()  # Only used once

# BAD: Don't cache before filtering
df = spark.read.parquet("/data/large")
df.cache()  # ❌ Caching 100GB
filtered = df.filter(col("date") == "2024-01-01")  # Only 1GB needed

# GOOD: Cache after filtering
df = spark.read.parquet("/data/large")
filtered = df.filter(col("date") == "2024-01-01")
filtered.cache()  # ✅ Caching 1GB
```

### Rationale:
- **GOOD**: Avoids recomputation (faster)
- **BAD**: Uses memory (less memory for tasks)
- **GOOD**: Critical for iterative algorithms (ML)
- **Best Practice**: Cache after expensive filters, unpersist when done

---

## 10) What is Predicate Pushdown?

### Answer:
Predicate pushdown moves filter operations down to the data source (Parquet, JDBC, etc.) so Spark reads LESS data. The data source applies the filter before sending data to Spark.

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("PredicatePushdown").getOrCreate()

# Create sample Parquet file
data = [(i, f"2024-01-{i%31+1:02d}", f"category_{i%5}") 
        for i in range(1000000)]
df = spark.createDataFrame(data, ["id", "date", "category"])
df.write.mode("overwrite").parquet("/tmp/sample_data")

# WITHOUT predicate pushdown (CSV - doesn't support it)
df.write.mode("overwrite").csv("/tmp/sample_csv")
csv_df = spark.read.csv("/tmp/sample_csv", header=True)
filtered_csv = csv_df.filter(col("category") == "category_1")

print("CSV (no pushdown):")
filtered_csv.explain()
# You'll see: reads ALL data, then Filter

# WITH predicate pushdown (Parquet)
parquet_df = spark.read.parquet("/tmp/sample_data")
filtered_parquet = parquet_df.filter(col("category") == "category_1")

print("\nParquet (with pushdown):")
filtered_parquet.explain()
# You'll see: PushedFilters: [IsNotNull(category), EqualTo(category,category_1)]

# The difference:
"""
CSV:
1. Read 1,000,000 rows from disk
2. Transfer 1,000,000 rows to Spark
3. Apply filter in Spark
4. Result: 200,000 rows

Parquet with pushdown:
1. Apply filter at Parquet reader
2. Read only 200,000 rows from disk (skip others)
3. Transfer 200,000 rows to Spark
4. Result: 200,000 rows

5x less I/O!
"""

# Multiple predicates pushed down
complex_filter = parquet_df.filter(
    (col("category") == "category_1") & 
    (col("id") > 500000)
)
complex_filter.explain()
# Both filters pushed to Parquet

# Predicate pushdown with JDBC
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host/db") \
    .option("dbtable", "large_table") \
    .load()

filtered_jdbc = jdbc_df.filter(col("amount") > 1000)
# Spark generates: SELECT * FROM large_table WHERE amount > 1000
# Database does the filtering!
```

### Rationale:
- **GOOD**: Reads less data from disk (faster)
- **GOOD**: Less network transfer from storage
- **GOOD**: Reduces memory pressure on Spark
- **BAD**: Not all formats support it (CSV doesn't, Parquet does)
- **Best Practice**: Use Parquet, filter early, check explain() for PushedFilters

---

## Additional Interview Questions (11-40)

## 11) What is Data Skew and how do you detect it?

### Answer:
Data skew occurs when data is unevenly distributed across partitions, causing some tasks to process much more data than others.

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum

spark = SparkSession.builder.appName("SkewDetection").getOrCreate()

# Create skewed data (90% of data has same key)
skewed_data = []
for i in range(100000):
    if i < 90000:
        skewed_data.append(("skewed_key", i))
    else:
        skewed_data.append((f"key_{i}", i))

df = spark.createDataFrame(skewed_data, ["key", "value"])

# Detect skew: check key distribution
key_distribution = df.groupBy("key").agg(
    count("*").alias("count")
).orderBy(col("count").desc())

print("Key distribution:")
key_distribution.show(10)
# You'll see: skewed_key has 90,000 records, others have 1 each

# This causes problems in groupBy
result = df.groupBy("key").agg(_sum("value"))
result.explain()
result.show()
# One task processes 90K records, others process 1 record each
# Check Spark UI: one task takes 90x longer than others
```

### Solutions:

```python
# Solution 1: Salting (add random prefix to skewed keys)
from pyspark.sql.functions import rand, concat, lit

salted = df.withColumn(
    "salted_key",
    when(col("key") == "skewed_key",
         concat(col("key"), lit("_"), (rand() * 10).cast("int")))
    .otherwise(col("key"))
)

# Now groupBy on salted_key distributes load
result = salted.groupBy("salted_key").agg(_sum("value"))

# Solution 2: Broadcast join (if skewed side is small after agg)
# Solution 3: Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Rationale:
- **BAD**: Causes straggler tasks (one task takes 10x longer)
- **BAD**: Underutilizes cluster (99 cores idle waiting for 1 task)
- **GOOD**: Salting distributes load evenly
- **Best Practice**: Monitor task duration in Spark UI, use AQE

---

## 12) Explain Spark's Memory Management

### Answer:
Spark divides executor memory into:
1. **Storage Memory**: Caching, broadcast variables (default: 60% of heap)
2. **Execution Memory**: Shuffles, joins, sorts, aggregations (40% of heap)
3. **User Memory**: User data structures (Reserved memory)
4. **Reserved Memory**: Spark internal objects (300MB)

### Code Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MemoryManagement") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.6") \        # 60% for Spark (rest for user)
    .config("spark.memory.storageFraction", "0.5") \ # 50% of Spark memory for storage
    .getOrCreate()

# With 4GB executor:
# Total: 4GB
# Reserved: 300MB
# Usable: 3.7GB
# Spark Memory (60%): 2.22GB
#   - Storage (50% of 2.22GB): 1.11GB for caching
#   - Execution (50% of 2.22GB): 1.11GB for shuffles
# User Memory (40%): 1.48GB for Python objects

# Monitor memory usage
df = spark.range(0, 10000000)
df.cache()
df.count()  # Materializes cache

# Check Spark UI → Storage tab for memory usage
# Check Executors tab for memory metrics
```

### Rationale:
- **GOOD**: Unified memory manager allows storage/execution to borrow from each other
- **BAD**: Cache too much → less memory for shuffles → spill to disk
- **Best Practice**: Monitor memory usage, adjust fractions, use off-heap memory for large workloads

---

## 13) What are Spark's Join Strategies?

### Answer:
Spark supports 5 join strategies:
1. **Broadcast Hash Join**: Small table broadcast to all executors
2. **Shuffle Hash Join**: Both sides shuffled, hash table built
3. **Sort Merge Join**: Both sides sorted then merged (default for large joins)
4. **Cartesian Join**: Cross product (very expensive)
5. **Broadcast Nested Loop Join**: Broadcast + nested loop

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("JoinStrategies").getOrCreate()

# Large table
large_df = spark.range(0, 1000000).toDF("id") \
    .withColumn("value", col("id") * 2)

# Small table
small_df = spark.range(0, 100).toDF("id") \
    .withColumn("category", concat(lit("cat_"), col("id")))

# 1. BROADCAST HASH JOIN (best for small tables)
broadcast_result = large_df.join(broadcast(small_df), "id")
broadcast_result.explain()
# You'll see: BroadcastHashJoin

# 2. SORT MERGE JOIN (default for large-large joins)
large_df2 = spark.range(0, 1000000).toDF("id")
sortmerge_result = large_df.join(large_df2, "id")
sortmerge_result.explain()
# You'll see: SortMergeJoin

# 3. SHUFFLE HASH JOIN (when sort isn't beneficial)
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
shuffle_result = large_df.join(small_df, "id")
shuffle_result.explain()

# 4. CARTESIAN JOIN (avoid!)
cartesian = large_df.crossJoin(small_df)  # Every row with every row
# 1M * 100 = 100M rows!
```

### Rationale:
- **Broadcast**: Fastest, no shuffle, but only for small tables
- **Sort Merge**: Scalable for large-large joins, requires sort
- **Shuffle Hash**: Good when data already partitioned, no sort needed
- **Cartesian**: Almost always bad (O(n²) complexity)
- **Best Practice**: Use broadcast for dimension tables, let Catalyst choose for others

---

## 14) What is Adaptive Query Execution (AQE)?

### Answer:
AQE optimizes query plans at runtime based on actual data statistics:
1. **Dynamically coalescing shuffle partitions**: Reduces partition count for small data
2. **Dynamically switching join strategies**: Switches to broadcast if table is small
3. **Dynamically optimizing skew joins**: Splits skewed partitions

### Code Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AQE") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# Without AQE: 200 shuffle partitions (default)
spark.conf.set("spark.sql.shuffle.partitions", "200")

df = spark.range(0, 10000)  # Small dataset
result = df.groupBy((col("id") % 10).alias("key")).count()

result.explain()
# Without AQE: Creates 200 partitions (most empty)
# With AQE: Dynamically reduces to ~10 partitions

# AQE switch join strategy
large = spark.range(0, 1000000)
maybe_small = spark.range(0, 100)  # Unknown size at planning time

result = large.join(maybe_small, large["id"] == maybe_small["id"])
# AQE realizes maybe_small is tiny, switches to broadcast join at runtime
```

### Rationale:
- **GOOD**: Optimizes based on actual data, not estimates
- **GOOD**: Handles skew automatically
- **GOOD**: Reduces number of empty tasks
- **Best Practice**: Enable AQE in Spark 3.0+ (default in 3.2+)

---

## 15) Explain the difference between `map` and `flatMap`

### Answer:
- **map**: Transforms each element to exactly ONE output element (1-to-1)
- **flatMap**: Transforms each element to ZERO or MORE output elements (1-to-many)

### Code Example:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MapFlatMap").getOrCreate()

# Sample data
data = ["hello world", "spark is great", "python rocks"]
rdd = spark.sparkContext.parallelize(data)

# MAP: Each line becomes one element
mapped = rdd.map(lambda line: line.upper())
print("MAP result:")
print(mapped.collect())
# Output: ['HELLO WORLD', 'SPARK IS GREAT', 'PYTHON ROCKS']
# 3 inputs → 3 outputs

# FLATMAP: Each line becomes multiple elements (words)
flatmapped = rdd.flatMap(lambda line: line.split(" "))
print("\nFLATMAP result:")
print(flatmapped.collect())
# Output: ['hello', 'world', 'spark', 'is', 'great', 'python', 'rocks']
# 3 inputs → 7 outputs

# Word count example (classic flatMap use case)
word_counts = rdd \
    .flatMap(lambda line: line.split(" ")) \  # Split into words
    .map(lambda word: (word, 1)) \            # Map to (word, 1)
    .reduceByKey(lambda a, b: a + b)          # Count occurrences

print("\nWord counts:")
print(word_counts.collect())

# With DataFrames (using explode - equivalent to flatMap)
from pyspark.sql.functions import explode, split

df = spark.createDataFrame(data, "string").toDF("line")
words_df = df.select(explode(split(col("line"), " ")).alias("word"))
words_df.show()
```

### Rationale:
- **map**: Use for 1-to-1 transformations (converting formats, calculations)
- **flatMap**: Use for 1-to-many (splitting, expanding nested structures)
- **Best Practice**: In DataFrames, use explode() instead of flatMap for better optimization

---

## 16) What is the Catalyst Optimizer?

### Answer:
Catalyst is Spark SQL's query optimizer that transforms logical plans into optimized physical plans through multiple phases:
1. **Analysis**: Resolves column names, validates types
2. **Logical Optimization**: Predicate pushdown, constant folding, projection pruning
3. **Physical Planning**: Chooses join strategies, selects access methods
4. **Code Generation**: Generates optimized Java bytecode

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Catalyst").getOrCreate()

df = spark.range(0, 1000000).toDF("id") \
    .withColumn("value", col("id") * 2) \
    .withColumn("category", (col("id") % 10))

# Query with multiple optimizations
result = df \
    .select("id", "value", "category") \
    .filter(col("value") > 1000) \
    .filter(col("category") == 5) \
    .select("id", "value")

# See how Catalyst optimizes
result.explain(True)

# Optimizations applied:
"""
1. Projection Pruning:
   - Only reads needed columns (id, value, category)
   - Doesn't read other columns even if they exist

2. Predicate Pushdown:
   - Applies filters early (before other operations)
   - Combines multiple filters: (value > 1000) AND (category == 5)

3. Constant Folding:
   - Pre-computes constants at planning time

4. Column Pruning:
   - Removes category from final select (not needed)
"""

# Compare to non-optimized (RDD):
rdd_result = df.rdd \
    .map(lambda row: (row.id, row.value, row.category)) \
    .filter(lambda row: row[1] > 1000) \
    .filter(lambda row: row[2] == 5) \
    .map(lambda row: (row[0], row[1]))
# No optimization! Executes exactly as written
```

### Rationale:
- **GOOD**: Automatically optimizes queries (no manual tuning)
- **GOOD**: Same optimization for SQL, DataFrame, Dataset APIs
- **GOOD**: Gets better with each Spark version
- **Best Practice**: Use DataFrame API (not RDD) to leverage Catalyst

---

## 17) What are the different ways to create a DataFrame?

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("CreateDF").getOrCreate()

# 1. From Python list/tuple
data = [(1, "Alice", 25), (2, "Bob", 30)]
df1 = spark.createDataFrame(data, ["id", "name", "age"])
df1.show()

# 2. From RDD
rdd = spark.sparkContext.parallelize(data)
df2 = spark.createDataFrame(rdd, ["id", "name", "age"])

# 3. From explicit schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df3 = spark.createDataFrame(data, schema)

# 4. From CSV file
df4 = spark.read.option("header", True).csv("path/to/file.csv")

# 5. From Parquet
df5 = spark.read.parquet("path/to/file.parquet")

# 6. From JSON
df6 = spark.read.json("path/to/file.json")

# 7. From JDBC
df7 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("dbtable", "table_name") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# 8. From Hive table
df8 = spark.sql("SELECT * FROM hive_table")

# 9. From Pandas (for small data)
import pandas as pd
pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
df9 = spark.createDataFrame(pandas_df)

# 10. Using range (for testing)
df10 = spark.range(0, 1000000)
```

---

## 18) How do you handle NULL values in PySpark?

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit

spark = SparkSession.builder.appName("NullHandling").getOrCreate()

data = [
    (1, "Alice", 25, "NY"),
    (2, "Bob", None, "CA"),
    (3, None, 30, None),
    (4, "David", 35, "TX")
]
df = spark.createDataFrame(data, ["id", "name", "age", "state"])

# 1. Drop rows with any nulls
df_no_nulls = df.dropna()
df_no_nulls.show()

# 2. Drop rows only if ALL values are null
df_drop_all = df.dropna(how='all')

# 3. Drop based on specific columns
df_drop_subset = df.dropna(subset=["name", "age"])

# 4. Fill nulls with default values
df_filled = df.fillna({"age": 0, "state": "Unknown"})
df_filled.show()

# 5. Fill with different values per column
df_fill_multi = df.fillna({
    "name": "Anonymous",
    "age": -1,
    "state": "N/A"
})

# 6. Replace nulls using coalesce (first non-null value)
df_coalesce = df.withColumn(
    "state",
    coalesce(col("state"), lit("Unknown"))
)

# 7. Conditional null handling
df_conditional = df.withColumn(
    "age_category",
    when(col("age").isNull(), "Unknown")
    .when(col("age") < 30, "Young")
    .otherwise("Adult")
)

# 8. Filter out nulls
df_filtered = df.filter(col("age").isNotNull())
```

---

## 19) What is the difference between `count()` and `count(column)`?

### Code Example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

spark = SparkSession.builder.appName("CountDemo").getOrCreate()

data = [
    (1, "Alice", 25),
    (2, "Bob", None),
    (3, None, 30),
    (4, "David", None)
]
df = spark.createDataFrame(data, ["id", "name", "age"])

# count() - action that counts ALL rows (including nulls)
total_rows = df.count()
print(f"Total rows: {total_rows}")  # 4

# count(column) - counts NON-NULL values in specific column
from pyspark.sql.functions import count

df.select(
    count("*").alias("all_rows"),       # 4 (includes nulls)
    count("name").alias("non_null_names"),  # 3 (Bob, Alice, David)
    count("age").alias("non_null_ages")     # 2 (Alice:25, null:30)
).show()

# count distinct
df.select(count(col("name")).alias("distinct_names")).show()
```

---

## 20) Explain Window Functions in detail

### Code Example:

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead, sum as _sum

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()

data = [
    ("Alice", "Sales", 5000),
    ("Bob", "Sales", 4500),
    ("Charlie", "Sales", 4500),
    ("David", "Engineering", 6000),
    ("Eve", "Engineering", 5500),
    ("Frank", "Engineering", 5500)
]
df = spark.createDataFrame(data, ["name", "dept", "salary"])

# Window specification: partition by dept, order by salary desc
window_spec = Window.partitionBy("dept").orderBy(col("salary").desc())

# Ranking functions
df_ranked = df.withColumn("row_num", row_number().over(window_spec)) \
    .withColumn("rank", rank().over(window_spec)) \
    .withColumn("dense_rank", dense_rank().over(window_spec))

df_ranked.show()
"""
Differences:
- row_number: 1, 2, 3, 4, ... (unique even for ties)
- rank: 1, 2, 2, 4, ... (ties get same rank, next skips)
- dense_rank: 1, 2, 2, 3, ... (ties get same rank, next is consecutive)
"""

# Lag/Lead (access previous/next row)
window_order = Window.partitionBy("dept").orderBy("salary")
df_lag_lead = df.withColumn("prev_salary", lag("salary", 1).over(window_order)) \
    .withColumn("next_salary", lead("salary", 1).over(window_order))

# Running total
df_running = df.withColumn(
    "running_total",
    _sum("salary").over(window_order.rowsBetween(Window.unboundedPreceding, Window.currentRow))
)

# Top N per group
top_earners = df_ranked.filter(col("row_num") <= 2)  # Top 2 per department
```

---

## Summary: Top 20 Must-Know Concepts

1. ✅ Narrow vs Wide Transformations
2. ✅ Lazy Evaluation & DAG
3. ✅ Shuffles & Cost
4. ✅ Broadcast Joins
5. ✅ collect() dangers
6. ✅ Caching/Persistence
7. ✅ Predicate Pushdown
8. ✅ repartition vs coalesce
9. ✅ Data Skew Detection & Mitigation
10. ✅ Memory Management
11. ✅ Join Strategies
12. ✅ Adaptive Query Execution (AQE)
13. ✅ map vs flatMap
14. ✅ Catalyst Optimizer
15. ✅ Creating DataFrames
16. ✅ NULL handling
17. ✅ count() variations
18. ✅ Window Functions
19. ✅ Spark Architecture (Driver/Executors)
20. ✅ Jobs/Stages/Tasks

---

## Practice Exercise: Put it all together

```python
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("ComprehensiveExample") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Read large dataset
sales = spark.read.parquet("/data/sales")  # Predicate pushdown support

# Optimization 1: Filter early (predicate pushdown)
filtered = sales.filter(
    (col("date") >= "2024-01-01") &
    (col("amount") > 100)
)

# Optimization 2: Cache if reused
filtered.cache()

# Narrow transformations (pipelined)
enriched = filtered \
    .withColumn("year", year(col("date"))) \
    .withColumn("month", month(col("date"))) \
    .select("customer_id", "product_id", "amount", "year", "month")

# Wide transformation (shuffle) - but optimized with broadcast
products = spark.read.parquet("/data/products")  # Small dimension table
result = enriched.join(broadcast(products), "product_id")

# Window function for ranking
window_spec = Window.partitionBy("customer_id").orderBy(col("amount").desc())
ranked = result.withColumn("rank", row_number().over(window_spec))

# Final aggregation
final = ranked \
    .filter(col("rank") <= 5) \     # Top 5 per customer
    .groupBy("customer_id") \
    .agg(_sum("amount").alias("top_5_total"))

# Write efficiently
final.coalesce(10).write.mode("overwrite").parquet("/output/top_customers")

# Cleanup
filtered.unpersist()
spark.stop()
```

This example demonstrates:
- ✅ Predicate pushdown (Parquet filter)
- ✅ Early filtering (reduces data size)
- ✅ Caching (reused DataFrame)
- ✅ Broadcast join (small dimension table)
- ✅ Window functions (ranking)
- ✅ Coalesce before write (reduce files)
- ✅ Proper cleanup (unpersist)

---

**End of Enhanced Quiz Pack** - Practice these concepts with real data to solidify your understanding!
