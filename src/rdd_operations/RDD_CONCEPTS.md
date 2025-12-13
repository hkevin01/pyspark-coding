# RDD Concepts - Complete Guide

## Table of Contents
1. [What is RDD?](#what-is-rdd)
2. [RDD Properties](#rdd-properties)
3. [When to Use RDD](#when-to-use-rdd)
4. [RDD Problems](#rdd-problems)

---

## What is RDD?

### Definition
**RDD (Resilient Distributed Dataset)** is the fundamental data structure of Apache Spark. It is an immutable, distributed collection of objects that can be processed in parallel across a cluster.

### Key Characteristics

#### 1. **Resilient** - Fault Tolerant
```
If a partition is lost, Spark can recompute it using lineage information.

Example:
  RDD1 → map() → RDD2 → filter() → RDD3
  
If RDD3's partition is lost, Spark recomputes:
  RDD1 → map() → filter() → RDD3 ✅
```

#### 2. **Distributed** - Partitioned Across Cluster
```
Data is split across multiple nodes:

Node 1: [Partition 0] [Partition 1]
Node 2: [Partition 2] [Partition 3]
Node 3: [Partition 4] [Partition 5]

Parallel processing on all partitions!
```

#### 3. **Dataset** - Collection of Data
- Can be created from files (HDFS, S3, local)
- Can be created from collections (lists, arrays)
- Can be created from other RDDs

### RDD Creation

#### From Collection
```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "RDD_Example")

# Create RDD from list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data, numSlices=2)  # 2 partitions

print(rdd.collect())  # [1, 2, 3, 4, 5]
```

#### From External File
```python
# From text file
text_rdd = sc.textFile("data.txt")

# From multiple files
multi_rdd = sc.textFile("data/*.txt")

# From HDFS
hdfs_rdd = sc.textFile("hdfs://namenode:9000/data/file.txt")

# From S3
s3_rdd = sc.textFile("s3a://bucket/data/")
```

#### From DataFrame
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RDD").getOrCreate()

# DataFrame to RDD
df = spark.read.csv("data.csv", header=True)
rdd = df.rdd  # Each row becomes a Row object

# Access Row fields
first_row = rdd.first()
print(first_row.columnName)
```

---

## RDD Properties

### 1. **Immutability**

RDDs are **immutable** - once created, they cannot be changed.

```python
# Original RDD
rdd1 = sc.parallelize([1, 2, 3, 4, 5])

# Transformation creates NEW RDD
rdd2 = rdd1.map(lambda x: x * 2)

print(rdd1.collect())  # [1, 2, 3, 4, 5] - unchanged!
print(rdd2.collect())  # [2, 4, 6, 8, 10] - new RDD
```

**Why Immutability?**
- ✅ Thread-safe (no concurrent modification)
- ✅ Enables lineage tracking for fault tolerance
- ✅ Simplifies reasoning about transformations
- ✅ Allows caching without corruption

### 2. **Lazy Evaluation**

Transformations are **lazy** - computed only when an action is called.

```python
# These don't execute yet!
rdd1 = sc.textFile("large_file.txt")  # ❌ Not loaded
rdd2 = rdd1.filter(lambda x: "ERROR" in x)  # ❌ Not filtered
rdd3 = rdd2.map(lambda x: x.upper())  # ❌ Not mapped

# NOW everything executes!
count = rdd3.count()  # ✅ Action triggers execution
```

**Benefits of Lazy Evaluation**:
- 🚀 Optimization: Spark optimizes entire pipeline
- 💾 Memory efficiency: Only computes what's needed
- ⚡ Combines operations: Multiple transformations → single pass

### 3. **Partitioning**

RDDs are divided into **partitions** for parallel processing.

```python
# Check partitions
rdd = sc.parallelize(range(100), numSlices=4)
print(f"Partitions: {rdd.getNumPartitions()}")  # 4

# View partition distribution
def show_partition(index, iterator):
    yield f"Partition {index}: {list(iterator)}"

result = rdd.mapPartitionsWithIndex(show_partition).collect()
for line in result:
    print(line)

# Output:
# Partition 0: [0, 1, 2, ..., 24]
# Partition 1: [25, 26, 27, ..., 49]
# Partition 2: [50, 51, 52, ..., 74]
# Partition 3: [75, 76, 77, ..., 99]
```

**Partition Rules**:
- Default: 1 partition per CPU core (for parallelize)
- Files: 1 partition per HDFS block (128MB default)
- Rule of thumb: 2-4x number of cores
- Too few: Underutilization
- Too many: Overhead from task scheduling

### 4. **Lineage (Logical Execution Plan)**

RDDs remember their **lineage** - the chain of transformations used to build them.

```python
# Build lineage
rdd1 = sc.textFile("logs.txt")
rdd2 = rdd1.filter(lambda x: "ERROR" in x)
rdd3 = rdd2.map(lambda x: (x.split()[0], 1))
rdd4 = rdd3.reduceByKey(lambda a, b: a + b)

# View lineage
print(rdd4.toDebugString().decode())

# Output:
# (4) PythonRDD[4] at RDD at PythonRDD.scala:53 []
#  |  MapPartitionsRDD[3] at mapPartitions at PythonRDD.scala:133 []
#  |  ShuffledRDD[2] at partitionBy at NativeMethodAccessorImpl.java:0 []
#  +-(4) PairwiseRDD[1] at reduceByKey at <stdin>:1 []
#     |  PythonRDD[0] at RDD at PythonRDD.scala:53 []
#     |  logs.txt MapPartitionsRDD[0] at textFile at NativeMethodAccessorImpl.java:0 []
#     |  logs.txt HadoopRDD[0] at textFile at NativeMethodAccessorImpl.java:0 []
```

**Lineage Benefits**:
- ✅ Fault tolerance: Recompute lost partitions
- ✅ No replication needed: Save storage
- ✅ Optimization: Spark can optimize DAG

### 5. **In-Memory Computation**

RDDs can be cached in memory for reuse.

```python
# Without caching - recomputes each time
rdd = sc.textFile("large.txt").filter(lambda x: "ERROR" in x)
count1 = rdd.count()  # Reads file + filters
count2 = rdd.count()  # Reads file + filters AGAIN!

# With caching - compute once, reuse
rdd = sc.textFile("large.txt").filter(lambda x: "ERROR" in x)
rdd.cache()  # or rdd.persist()

count1 = rdd.count()  # Reads file + filters + caches
count2 = rdd.count()  # Uses cache! ⚡
```

**Persistence Levels**:
```python
from pyspark import StorageLevel

# Memory only (default for cache())
rdd.persist(StorageLevel.MEMORY_ONLY)

# Memory + disk spillover
rdd.persist(StorageLevel.MEMORY_AND_DISK)

# Serialized in memory (save space)
rdd.persist(StorageLevel.MEMORY_ONLY_SER)

# Replicated for fault tolerance
rdd.persist(StorageLevel.MEMORY_AND_DISK_2)

# Remove from cache
rdd.unpersist()
```

### 6. **Type Safety (Compile-Time)**

In Scala/Java, RDDs are type-safe at compile time.

```scala
// Scala example
val rdd: RDD[Int] = sc.parallelize(Array(1, 2, 3))
val doubled: RDD[Int] = rdd.map(_ * 2)  // Type-safe!

// This won't compile:
val invalid: RDD[String] = rdd.map(_ * 2)  // ❌ Type error
```

**Note**: PySpark RDDs are dynamically typed (runtime checking).

---

## When to Use RDD

### ✅ Use RDDs When:

#### 1. **You Need Low-Level Control**
```python
# Custom partitioning logic
def custom_partitioner(key):
    """Route specific keys to specific partitions"""
    if key.startswith("A"):
        return 0
    elif key.startswith("B"):
        return 1
    else:
        return 2

rdd = sc.parallelize([("Apple", 1), ("Banana", 2), ("Cat", 3)])
partitioned = rdd.partitionBy(3, custom_partitioner)
```

#### 2. **Unstructured or Semi-Structured Data**
```python
# Parse complex log formats
log_rdd = sc.textFile("logs/*.txt")
parsed = log_rdd.map(parse_complex_log_line)

# Process binary data
binary_rdd = sc.binaryFiles("images/*.jpg")
processed = binary_rdd.map(process_image_bytes)
```

#### 3. **You're Working with Key-Value Pairs**
```python
# Word count - classic RDD use case
words = sc.textFile("document.txt") \
    .flatMap(lambda line: line.split()) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
```

#### 4. **Need mapPartitions for Efficiency**
```python
# Expensive setup (DB connection, ML model) per partition
def process_partition_with_db(iterator):
    conn = create_db_connection()  # Once per partition!
    results = [conn.query(record) for record in iterator]
    conn.close()
    return results

rdd.mapPartitions(process_partition_with_db)

# vs map (creates connection PER RECORD - expensive!)
rdd.map(lambda x: query_db(x))  # ❌ Inefficient
```

#### 5. **Legacy Spark Code / Maintenance**
```python
# Many older Spark applications use RDDs
# You may need to maintain or understand them
```

#### 6. **Fine-Grained Control Over Partitioning**
```python
# Control exactly how data is distributed
rdd.repartition(100)  # Increase parallelism
rdd.coalesce(10)  # Decrease without shuffle
rdd.repartitionAndSortWithinPartitions(partitioner)  # Efficient sorted partitions
```

### ❌ DON'T Use RDDs When:

#### 1. **Structured Data with Schema**
```python
# ❌ Don't use RDD for structured data
rdd = sc.textFile("users.csv").map(parse_csv)

# ✅ Use DataFrame instead
df = spark.read.csv("users.csv", header=True)
```

**Why DataFrames are better**:
- Catalyst optimizer
- Tungsten execution engine
- Column pruning and predicate pushdown
- More concise syntax

#### 2. **SQL-Like Operations**
```python
# ❌ RDD way - verbose and unoptimized
rdd.filter(lambda x: x['age'] > 25) \
   .map(lambda x: (x['city'], x['salary'])) \
   .reduceByKey(lambda a, b: a + b)

# ✅ DataFrame way - optimized and clear
df.filter(col("age") > 25) \
  .groupBy("city") \
  .agg(sum("salary"))
```

#### 3. **You Want Automatic Optimization**
```python
# DataFrames get automatic query optimization
# RDDs execute exactly as written (no optimization)
```

---

## RDD Problems

### Problem 1: **No Schema Enforcement**

```python
# RDD - No schema, runtime errors
rdd = sc.parallelize([
    ("John", 25, "NY"),
    ("Jane", "unknown", "LA"),  # Type mismatch!
])

# This will fail at runtime when you try to sum ages
rdd.map(lambda x: x[1]).sum()  # ❌ Runtime error!

# DataFrame - Schema enforcement
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), False),
    StructField("age", IntegerType(), False),  # Must be int!
    StructField("city", StringType(), False)
])

# This fails immediately with clear error
df = spark.createDataFrame([("John", 25, "NY"), ("Jane", "unknown", "LA")], schema)
# ❌ Error: cannot cast 'unknown' to IntegerType
```

### Problem 2: **Verbose Syntax**

```python
# RDD - Many lines
result = sc.textFile("sales.csv") \
    .map(lambda line: line.split(",")) \
    .filter(lambda fields: int(fields[2]) > 1000) \
    .map(lambda fields: (fields[1], float(fields[3]))) \
    .reduceByKey(lambda a, b: a + b) \
    .map(lambda x: (x[0], x[1])) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

# DataFrame - Clean and clear
result = spark.read.csv("sales.csv", header=True) \
    .filter(col("amount") > 1000) \
    .groupBy("category") \
    .agg(sum("price").alias("total")) \
    .orderBy(col("total").desc()) \
    .limit(10)
```

### Problem 3: **No Automatic Optimization**

```python
# RDD - No optimization, executes as written
rdd1 = sc.parallelize(range(1000000))
rdd2 = rdd1.filter(lambda x: x > 500000)  # Scans all records
rdd3 = rdd2.map(lambda x: x * 2)  # Separate pass
result = rdd3.count()

# DataFrame - Catalyst optimizes automatically
df1 = spark.range(1000000)
df2 = df1.filter(col("id") > 500000)  # Catalyst may push down
df3 = df2.select((col("id") * 2).alias("doubled"))  # Fuses operations
result = df3.count()  # Single optimized pass!
```

### Problem 4: **Slower Serialization**

```python
# RDDs use Java/Python serialization (slow)
# DataFrames use Tungsten binary format (fast!)

import time

# RDD - Python objects, pickle serialization
start = time.time()
rdd = sc.parallelize(range(10_000_000))
rdd.map(lambda x: (x, x * 2)).count()
rdd_time = time.time() - start

# DataFrame - Optimized binary format
start = time.time()
df = spark.range(10_000_000)
df.select(col("id"), (col("id") * 2).alias("doubled")).count()
df_time = time.time() - start

print(f"RDD: {rdd_time:.2f}s")
print(f"DataFrame: {df_time:.2f}s")
# DataFrame is typically 2-5x faster!
```

### Problem 5: **Type Errors Only at Runtime**

```python
# RDD - Type errors discovered when action runs
rdd = sc.parallelize([1, 2, "three", 4])
result = rdd.map(lambda x: x * 2)  # ✅ No error yet (lazy)

try:
    print(result.collect())  # ❌ Error here at runtime!
except TypeError as e:
    print(f"Runtime error: {e}")

# DataFrame - Type checking earlier
df = spark.createDataFrame([(1,), (2,), ("three",), (4,)], ["num"])
# If schema specified, error caught at creation time
```

### Problem 6: **Garbage Collection Overhead**

```python
# RDDs with Python objects create GC pressure
# Each object needs to be tracked by Python GC

# Example: Processing 100M records
rdd = sc.parallelize(range(100_000_000))
result = rdd.map(lambda x: {"value": x, "squared": x**2})  # Many Python dicts!
# Result: Slow GC pauses

# DataFrames use off-heap memory (no GC pressure)
df = spark.range(100_000_000)
result = df.select(col("id").alias("value"), (col("id") ** 2).alias("squared"))
# Result: No GC pauses! ⚡
```

### Problem 7: **Limited Built-in Functions**

```python
# RDD - Must implement everything yourself
def parse_date(date_str):
    from datetime import datetime
    return datetime.strptime(date_str, "%Y-%m-%d")

rdd.map(lambda x: parse_date(x[0]))

# DataFrame - Hundreds of built-in functions
from pyspark.sql.functions import to_date, date_format, datediff

df.select(
    to_date(col("date_str"), "yyyy-MM-dd"),
    date_format(col("timestamp"), "yyyy-MM-dd"),
    datediff(col("end_date"), col("start_date"))
)
```

---

## Quick Decision Matrix

| Criteria | Use RDD | Use DataFrame |
|----------|---------|---------------|
| Structured data with schema | ❌ | ✅ |
| SQL-like operations | ❌ | ✅ |
| Need optimization | ❌ | ✅ |
| Unstructured text/binary | ✅ | ❌ |
| Fine-grained partition control | ✅ | ❌ |
| mapPartitions pattern | ✅ | ⚠️ |
| Performance critical | ❌ | ✅ |
| Code maintainability | ❌ | ✅ |
| Legacy Spark code | ✅ | ❌ |

---

## Summary

### RDD Strengths ✅
- Low-level control over partitioning
- Works with unstructured data
- mapPartitions for efficient batch processing
- Foundation of Spark (understand for debugging)

### RDD Weaknesses ❌
- No automatic optimization
- Verbose syntax
- Slower serialization
- No schema enforcement
- GC overhead with Python objects

### Modern Recommendation 🎯
**Use DataFrames by default**, drop to RDDs only when you need:
1. Fine-grained partition control
2. Unstructured/binary data processing
3. mapPartitions for expensive setup operations
4. Maintaining legacy code

---

## Next Steps

1. ✅ Completed: RDD operations examples (src/rdd_operations/)
2. 📚 Review: DataFrame APIs for modern development
3. 🔄 Practice: Converting RDD code to DataFrame equivalents
4. 📊 Learn: When RDDs are actually needed in production

For practical RDD examples, see:
- `01_transformations_lowlevel_part1.py` - Core transformations
- `05_shuffle_and_key_operations.py` - Performance optimization
- `06_partitions_sorting_ranking.py` - Partition management

**Remember**: DataFrames are built on top of RDDs. Understanding RDDs helps you understand what's happening under the hood! 🚀
