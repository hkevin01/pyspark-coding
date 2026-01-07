# Understanding Shuffle Partitions in PySpark

## 🔀 What Are Shuffle Partitions?

**Shuffle partitions** are the number of partitions created when Spark performs a **shuffle operation** - redistributing data across the cluster.

### 📊 Key Concept

```
Before Shuffle:        After Shuffle:
┌──────────┐          ┌──────────┐
│ Part 1   │          │ Part 1   │ ← Data from all previous partitions
│ Part 2   │  ─────→  │ Part 2   │   with same key goes to same partition
│ Part 3   │          │ Part 3   │
│ Part 4   │          │ Part 4   │
└──────────┘          └──────────┘
```

## 🎯 When Does Shuffling Happen?

Shuffle operations occur during:

1. **JOIN operations** - Data must be co-located by join key
2. **GROUP BY / aggregations** - Data grouped by key
3. **DISTINCT** - Find unique values across partitions
4. **REPARTITION** - Explicitly redistribute data
5. **Window functions** - When PARTITION BY is used
6. **SORT / ORDER BY** - Reorder data globally

## ⚙️ Configuration

```python
# Default shuffle partitions (often 200)
spark.conf.get("spark.sql.shuffle.partitions")

# Set shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "4")

# Or during session creation
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

## 📏 How Many Partitions Should You Use?

### Rule of Thumb:
- **Small datasets (< 1GB)**: 2-10 partitions
- **Medium datasets (1-100GB)**: 100-200 partitions
- **Large datasets (> 100GB)**: 200-1000+ partitions

### Calculation Formula:
```
Target partitions = Total data size / Target partition size
Target partition size = 128-200 MB (ideal range)
```

### Examples:

```python
# 10 GB dataset
10 GB / 128 MB = ~78 partitions → Set to 80

# 100 GB dataset
100 GB / 128 MB = ~780 partitions → Set to 800

# 1 TB dataset
1000 GB / 128 MB = ~7800 partitions → Set to 8000
```

## 🎭 Too Few vs Too Many Partitions

### ⚠️ Too Few Partitions:
- **Symptoms**: Slow performance, OOM errors
- **Cause**: Each partition too large, limited parallelism
- **Example**: 100GB data with 4 partitions = 25GB per partition ❌

```python
# BAD: Only 4 partitions for large data
spark.conf.set("spark.sql.shuffle.partitions", "4")
large_df.groupBy("customer_id").sum("amount")  # Slow!
```

### ⚠️ Too Many Partitions:
- **Symptoms**: Task overhead, many small files
- **Cause**: Overhead of managing thousands of tiny tasks
- **Example**: 1GB data with 10,000 partitions = 100KB per partition ❌

```python
# BAD: Too many partitions for small data
spark.conf.set("spark.sql.shuffle.partitions", "10000")
small_df.groupBy("category").count()  # Overhead!
```

## ✅ Optimal Configuration Examples

### Local Development (Small Data):
```python
spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

### Production (Medium Data - 10-100GB):
```python
spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### Production (Large Data - 100GB+):
```python
spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.sql.shuffle.partitions", "1000") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

## 🔍 How to Check Partition Count

```python
# Check shuffle partition config
current = spark.conf.get("spark.sql.shuffle.partitions")
print(f"Shuffle partitions: {current}")

# Check actual partitions in a DataFrame
df.rdd.getNumPartitions()  # Before shuffle
result.rdd.getNumPartitions()  # After shuffle operation
```

## 🚀 Adaptive Query Execution (AQE)

Spark 3.0+ can **automatically adjust** partition count:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**Benefits:**
- Combines small partitions after shuffle
- Reduces task overhead
- Dynamically adjusts based on data statistics

## 💡 Interview Questions & Answers

### Q: "What are shuffle partitions?"
**A:** "Shuffle partitions are the number of partitions created when Spark redistributes data across the cluster during operations like joins, group by, or window functions."

### Q: "How do you optimize shuffle partitions?"
**A:** "Set `spark.sql.shuffle.partitions` based on data size. Aim for 128-200MB per partition. For 100GB data, use ~600-800 partitions. Enable AQE for automatic optimization."

### Q: "What happens if shuffle partitions is too low?"
**A:** "Large partitions cause OOM errors and limit parallelism. For example, 100GB with 4 partitions means 25GB per partition, which won't fit in memory."

### Q: "What happens if shuffle partitions is too high?"
**A:** "Too many partitions create overhead - more tasks to schedule, more files to manage. For 1GB with 10,000 partitions, each partition is only 100KB, causing inefficiency."

### Q: "How does shuffle partitions differ from input partitions?"
**A:** "Input partitions come from reading data (based on HDFS blocks, file size). Shuffle partitions are created during transformations. They're independent settings."

## 📊 Visual Example

```python
# Example: Impact of shuffle partitions on GROUP BY

from pyspark.sql.functions import col, sum as _sum
import time

# Create 100MB dataset
large_df = spark.range(0, 10000000).toDF("id") \
    .withColumn("category", (col("id") % 100).cast("string")) \
    .withColumn("amount", (col("id") % 1000))

# TEST 1: Too few partitions (2)
spark.conf.set("spark.sql.shuffle.partitions", "2")
start = time.time()
result = large_df.groupBy("category").agg(_sum("amount"))
result.count()
time_2_partitions = time.time() - start

# TEST 2: Optimal partitions (20)
spark.conf.set("spark.sql.shuffle.partitions", "20")
start = time.time()
result = large_df.groupBy("category").agg(_sum("amount"))
result.count()
time_20_partitions = time.time() - start

# TEST 3: Too many partitions (1000)
spark.conf.set("spark.sql.shuffle.partitions", "1000")
start = time.time()
result = large_df.groupBy("category").agg(_sum("amount"))
result.count()
time_1000_partitions = time.time() - start

print(f"2 partitions:    {time_2_partitions:.2f}s")
print(f"20 partitions:   {time_20_partitions:.2f}s ← Optimal")
print(f"1000 partitions: {time_1000_partitions:.2f}s")
```

## 🎓 Best Practices

1. **Always set shuffle partitions** explicitly in production
2. **Calculate based on data size**: Target 128-200MB per partition
3. **Use AQE** in Spark 3.0+ for automatic optimization
4. **Monitor Spark UI** to see actual partition distribution
5. **Test different values** - profile your workload
6. **Consider operation type**:
   - Joins: More partitions often better
   - Aggregations: Moderate partitions
   - Window functions: Match your partition keys

## 🔗 Related Configurations

```python
# Number of partitions for RDD operations
spark.conf.set("spark.default.parallelism", "100")

# Repartition DataFrame explicitly
df.repartition(50)  # Change to 50 partitions
df.repartition(50, "customer_id")  # Partition by key

# Coalesce (reduce only, no shuffle)
df.coalesce(10)  # Reduce to 10 partitions efficiently
```

---

**Remember:** Shuffle partitions is one of the **most important** performance tuning parameters in PySpark!
