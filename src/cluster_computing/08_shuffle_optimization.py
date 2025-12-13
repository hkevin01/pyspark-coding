"""
08_shuffle_optimization.py
==========================

Master shuffle optimization to minimize expensive data movement across
network in distributed clusters.

Shuffles are the most expensive operations in Spark. Learn how to
minimize, optimize, and avoid them when possible.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, rand, count as spark_count
import time


def create_spark():
    return SparkSession.builder \
        .appName("ShuffleOptimization") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def demonstrate_what_causes_shuffles():
    """Understand what operations trigger shuffles."""
    print("=" * 70)
    print("1. WHAT CAUSES SHUFFLES?")
    print("=" * 70)
    
    print("""
🔄 Shuffle: Data redistribution across nodes

Operations that ALWAYS shuffle:
-------------------------------
1. **Joins** (except broadcast joins)
   df1.join(df2, "key")
   
2. **GroupBy / Aggregations**
   df.groupBy("category").count()
   
3. **Distinct / DropDuplicates**
   df.distinct()
   df.dropDuplicates(["id"])
   
4. **Repartition**
   df.repartition(200, "key")
   
5. **Sort**
   df.orderBy("column")
   
6. **Window Functions** (with PARTITION BY)
   df.withColumn("rank", row_number().over(window_spec))

Operations that DON'T shuffle:
------------------------------
✅ filter, select, withColumn
✅ map, flatMap (on RDDs)
✅ union (if same partitioning)
✅ coalesce (reduce partitions only)
✅ broadcast joins

Shuffle Costs:
--------------
1. **Disk I/O**: Write shuffle data to disk
2. **Network**: Transfer data between nodes
3. **Serialization**: Serialize/deserialize objects
4. **Memory**: Buffer data during shuffle

Example:
--------
                Before Shuffle (2 nodes)
                Node 1: [A1, B1, C1]
                Node 2: [A2, B2, C2]
                         ↓
        groupBy("letter") - SHUFFLE!
                         ↓
                After Shuffle (2 nodes)
                Node 1: [A1, A2]  ← All A's
                Node 2: [B1, B2, C1, C2]  ← All B's and C's
    """)


def demonstrate_minimize_shuffles(spark):
    """Strategies to minimize shuffles."""
    print("\n" + "=" * 70)
    print("2. MINIMIZE SHUFFLES")
    print("=" * 70)
    
    # Create datasets
    orders = spark.range(100000).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("amount", (col("order_id") % 500 + 10).cast("double"))
    
    customers = spark.range(1000).toDF("customer_id") \
        .withColumn("name", col("customer_id").cast("string"))
    
    print(f"📊 Orders: {orders.count():,}, Customers: {customers.count():,}")
    
    # Strategy 1: Filter before shuffle
    print("\n❌ Bad: Shuffle then filter")
    start = time.time()
    result1 = orders.join(customers, "customer_id") \
        .filter(col("amount") > 400) \
        .count()
    time1 = time.time() - start
    print(f"   Result: {result1:,}, Time: {time1:.3f}s")
    print("   ⚠️  Shuffles 100K orders, then filters")
    
    print("\n✅ Good: Filter before shuffle")
    start = time.time()
    result2 = orders.filter(col("amount") > 400) \
        .join(customers, "customer_id") \
        .count()
    time2 = time.time() - start
    print(f"   Result: {result2:,}, Time: {time2:.3f}s")
    print(f"   Speedup: {time1 / time2:.2f}x")
    print("   ✅ Shuffles ~20K orders (80% reduction)")
    
    # Strategy 2: Broadcast small tables
    print("\n✅ Best: Broadcast join (no shuffle)")
    start = time.time()
    result3 = orders.filter(col("amount") > 400) \
        .join(broadcast(customers), "customer_id") \
        .count()
    time3 = time.time() - start
    print(f"   Result: {result3:,}, Time: {time3:.3f}s")
    print(f"   Speedup: {time1 / time3:.2f}x vs original")
    print("   ✅ No shuffle at all!")


def demonstrate_partition_key_optimization(spark):
    """Optimize partitioning for joins."""
    print("\n" + "=" * 70)
    print("3. PARTITION KEY OPTIMIZATION")
    print("=" * 70)
    
    # Create datasets
    orders = spark.range(1, 500001).toDF("order_id") \
        .withColumn("product_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("quantity", (col("order_id") % 10 + 1).cast("int"))
    
    products = spark.range(1, 1001).toDF("product_id") \
        .withColumn("price", (col("product_id") % 100 + 10).cast("double"))
    
    print(f"📊 Orders: {orders.count():,}, Products: {products.count():,}")
    
    # Approach 1: Default shuffle join
    print("\n❌ Approach 1: Default shuffle (random partitioning)")
    start = time.time()
    result1 = orders.join(products, "product_id")
    result1.write.mode("overwrite").format("noop").save()
    time1 = time.time() - start
    print(f"   Time: {time1:.3f}s")
    print("   ⚠️  Both DataFrames shuffled with random partitioning")
    
    # Approach 2: Pre-partition on join key
    print("\n✅ Approach 2: Pre-partition on join key")
    start = time.time()
    orders_partitioned = orders.repartition(8, "product_id")
    products_partitioned = products.repartition(8, "product_id")
    result2 = orders_partitioned.join(products_partitioned, "product_id")
    result2.write.mode("overwrite").format("noop").save()
    time2 = time.time() - start
    print(f"   Time: {time2:.3f}s")
    print(f"   Speedup: {time1 / time2:.2f}x")
    print("   ✅ Co-located partitions reduce shuffle")
    
    # Approach 3: Cache and reuse
    print("\n✅ Approach 3: Cache partitioned DataFrames")
    start = time.time()
    orders_cached = orders.repartition(8, "product_id").cache()
    products_cached = products.repartition(8, "product_id").cache()
    orders_cached.count()  # Materialize
    products_cached.count()
    
    # Multiple joins reuse cache
    result3a = orders_cached.join(products_cached, "product_id")
    result3a.write.mode("overwrite").format("noop").save()
    
    result3b = orders_cached.join(products_cached, "product_id")
    result3b.write.mode("overwrite").format("noop").save()
    
    time3 = time.time() - start
    print(f"   Time (2 joins): {time3:.3f}s")
    print(f"   Per join: {time3/2:.3f}s")
    print("   ✅ Second join reuses partitioned cache")
    
    orders_cached.unpersist()
    products_cached.unpersist()


def demonstrate_bucketing(spark):
    """Use bucketing for repeated joins."""
    print("\n" + "=" * 70)
    print("4. BUCKETING (Pre-Partitioned Tables)")
    print("=" * 70)
    
    print("""
🪣 Bucketing: Pre-partition data at write time

Concept:
--------
Instead of shuffling every time you join, pre-partition data when saving.
Future joins on bucketed columns avoid shuffle!

How it works:
-------------
1. Write data with .bucketBy(num_buckets, "column")
2. Spark stores data in buckets based on column hash
3. When joining on bucketed column, no shuffle needed!

Code Example:
-------------
# Write with bucketing
orders.write \\
    .bucketBy(100, "customer_id") \\
    .sortBy("order_date") \\
    .mode("overwrite") \\
    .saveAsTable("orders_bucketed")

customers.write \\
    .bucketBy(100, "customer_id") \\
    .mode("overwrite") \\
    .saveAsTable("customers_bucketed")

# Read and join (NO SHUFFLE!)
orders_bucketed = spark.table("orders_bucketed")
customers_bucketed = spark.table("customers_bucketed")
result = orders_bucketed.join(customers_bucketed, "customer_id")

Benefits:
---------
✅ No shuffle for joins on bucketed columns
✅ Faster repeated joins (data warehousing)
✅ Sorted data (if using .sortBy())
✅ Better for star schema queries

Trade-offs:
-----------
⚠️  Slower writes (bucketing overhead)
⚠️  Need to choose bucket count carefully
⚠️  Only works with Hive tables
⚠️  Requires same bucket count for all tables

When to use:
------------
✅ Repeated joins on same columns
✅ Data warehouse with star schema
✅ Read-heavy workloads
❌ Ad-hoc analysis (overhead not worth it)

Bucket Count Guidelines:
------------------------
Data Size     Bucket Count
---------     ------------
< 1 GB        10-50
1-10 GB       50-200
10-100 GB     200-500
> 100 GB      500-1000

Rule: ~128-256 MB per bucket after join
    """)


def demonstrate_shuffle_tuning(spark):
    """Tune shuffle parameters."""
    print("\n" + "=" * 70)
    print("5. SHUFFLE TUNING PARAMETERS")
    print("=" * 70)
    
    print("""
⚙️  Key Shuffle Configuration:

1. **spark.sql.shuffle.partitions** (Default: 200)
   Number of partitions after shuffle (groupBy, join)
   
   Too high: Many small tasks (overhead)
   Too low: Large partitions (OOM, spill)
   
   Rule of thumb: total_cores × 2-4
   
   spark.conf.set("spark.sql.shuffle.partitions", "200")


2. **spark.sql.autoBroadcastJoinThreshold** (Default: 10 MB)
   Auto-broadcast tables smaller than this
   
   Increase for more broadcast joins:
   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100485760")  # 100 MB


3. **spark.shuffle.compress** (Default: true)
   Compress shuffle data (saves network, costs CPU)
   
   spark.conf.set("spark.shuffle.compress", "true")


4. **spark.shuffle.file.buffer** (Default: 32 KB)
   Buffer size for shuffle writes
   
   Larger = fewer writes, more memory
   spark.conf.set("spark.shuffle.file.buffer", "1m")


5. **spark.reducer.maxSizeInFlight** (Default: 48 MB)
   Max data fetched per reduce task
   
   Larger = faster, more memory
   spark.conf.set("spark.reducer.maxSizeInFlight", "96m")


6. **spark.shuffle.sort.bypassMergeThreshold** (Default: 200)
   Bypass merge-sort if partitions < threshold
   
   spark.conf.set("spark.shuffle.sort.bypassMergeThreshold", "200")


7. **spark.sql.adaptive.enabled** (Default: true in Spark 3+)
   Adaptive Query Execution (AQE)
   
   ✅ Auto-optimize shuffle partitions
   ✅ Auto-convert to broadcast joins
   ✅ Handle skewed joins
   
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    """)
    
    # Demonstrate partition tuning
    data = spark.range(1, 10000001).toDF("id") \
        .withColumn("category", (col("id") % 100).cast("int"))
    
    print("\n📊 Testing Shuffle Partition Counts:")
    
    # Default (200 partitions)
    print("\n❌ Default (200 partitions) - too many for local")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    start = time.time()
    result = data.groupBy("category").count()
    result.write.mode("overwrite").format("noop").save()
    time1 = time.time() - start
    print(f"   Time: {time1:.3f}s")
    
    # Tuned (16 partitions)
    print("\n✅ Tuned (16 partitions = 4 cores × 4)")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    start = time.time()
    result = data.groupBy("category").count()
    result.write.mode("overwrite").format("noop").save()
    time2 = time.time() - start
    print(f"   Time: {time2:.3f}s")
    print(f"   Speedup: {time1 / time2:.2f}x")


def demonstrate_adaptive_query_execution(spark):
    """Adaptive Query Execution (AQE) for automatic optimization."""
    print("\n" + "=" * 70)
    print("6. ADAPTIVE QUERY EXECUTION (AQE)")
    print("=" * 70)
    
    print("""
🧠 AQE: Spark 3+ automatic shuffle optimization

Features:
---------
1. **Coalesce Shuffle Partitions**
   Automatically reduce partition count after shuffle
   
2. **Convert to Broadcast Joins**
   Auto-convert sort-merge to broadcast if small enough
   
3. **Optimize Skewed Joins**
   Split large partitions to handle data skew

Configuration:
--------------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

Example:
--------
Initial shuffle: 200 partitions
After filter: Only 10 partitions have data
AQE: Coalesce to 10 partitions automatically!

Benefits:
---------
✅ No manual tuning needed
✅ Handles varying data sizes
✅ Optimizes at runtime
✅ Improves query performance 2-10x

When to disable:
----------------
❌ Very small datasets (overhead not worth it)
❌ Benchmarking (non-deterministic execution)
    """)
    
    # Enable AQE
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Create skewed dataset
    data = spark.range(1, 1000001).toDF("id") \
        .withColumn("category", 
            when(col("id") < 900000, lit(1))  # 90% in category 1
            .otherwise((col("id") % 10).cast("int")))
    
    print("\n📊 Testing AQE with skewed data:")
    print("   Category 1: 900,000 rows (90%)")
    print("   Categories 2-10: 11,111 rows each")
    
    from pyspark.sql.functions import lit, when
    
    start = time.time()
    result = data.groupBy("category").count().orderBy("count", ascending=False)
    result.show(10)
    print(f"   Time: {time.time() - start:.3f}s")
    print("   ✅ AQE automatically handles skew!")


def demonstrate_best_practices():
    """Best practices for shuffle optimization."""
    print("\n" + "=" * 70)
    print("7. SHUFFLE OPTIMIZATION BEST PRACTICES")
    print("=" * 70)
    
    print("""
🎯 Optimization Checklist:

1. ✅ Filter data BEFORE shuffles
   df.filter(...).groupBy(...)  # Good
   df.groupBy(...).filter(...)  # Bad

2. ✅ Use broadcast joins for small tables (< 100 MB)
   large_df.join(broadcast(small_df), "key")

3. ✅ Pre-partition on join keys for repeated joins
   df1 = df1.repartition(200, "key").cache()
   df2 = df2.repartition(200, "key").cache()

4. ✅ Tune spark.sql.shuffle.partitions
   total_cores × 2-4 partitions
   
5. ✅ Enable Adaptive Query Execution (AQE)
   spark.conf.set("spark.sql.adaptive.enabled", "true")

6. ✅ Use bucketing for star schema
   df.write.bucketBy(100, "key").saveAsTable("table")

7. ✅ Coalesce to reduce partitions (no shuffle)
   df.coalesce(10)  # Good for reducing output files

8. ✅ Monitor Spark UI shuffle metrics
   - Shuffle Read/Write sizes
   - Spill to disk warnings
   - Skewed partitions


⚠️  Shuffle Red Flags:

1. ❌ Shuffle Read > 10 GB per partition
   → Increase spark.sql.shuffle.partitions

2. ❌ Shuffle Write >> Shuffle Read
   → Data skew, use salting

3. ❌ Spill to disk
   → Increase executor memory or partition count

4. ❌ Long shuffle read time
   → Network bottleneck or disk I/O

5. ❌ Many small partitions (< 10 MB each)
   → Decrease spark.sql.shuffle.partitions


📊 Shuffle Optimization Impact:

Optimization                    Speedup    When to Use
-----------                     -------    -----------
Broadcast join                  5-10x      Small tables
Filter before shuffle           2-5x       Selective filters
Pre-partition + cache           2-3x       Repeated joins
Bucketing                       3-5x       Star schema
Tune shuffle partitions         2x         Always
Enable AQE                      2-10x      Always (Spark 3+)


🔍 Debugging Shuffles:

1. Check Spark UI → SQL Tab → Query Plan
   Look for "Exchange" operations (shuffles)

2. Use .explain() to see physical plan
   df.explain(mode="formatted")

3. Count shuffle bytes
   Spark UI → Stages → Shuffle Read/Write

4. Identify skewed partitions
   Spark UI → Stages → Task times (wide range = skew)


💡 Example Query Optimization:

# Before (3 shuffles)
result = orders \\
    .join(customers, "customer_id") \\
    .join(products, "product_id") \\
    .groupBy("category").count()

# After (1 shuffle)
result = orders \\
    .filter(col("amount") > 100) \\  # Filter early
    .join(broadcast(customers), "customer_id") \\  # Broadcast
    .join(broadcast(products), "product_id") \\    # Broadcast
    .groupBy("category").count()  # Only 1 shuffle!

Speedup: 5-10x depending on data size
    """)


def main():
    spark = create_spark()
    
    print("🔄 SHUFFLE OPTIMIZATION")
    print("=" * 70)
    print("\nMinimize expensive data movement for faster queries!")
    print()
    
    demonstrate_what_causes_shuffles()
    demonstrate_minimize_shuffles(spark)
    demonstrate_partition_key_optimization(spark)
    demonstrate_bucketing(spark)
    demonstrate_shuffle_tuning(spark)
    demonstrate_adaptive_query_execution(spark)
    demonstrate_best_practices()
    
    print("\n" + "=" * 70)
    print("✅ SHUFFLE OPTIMIZATION DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Filter before shuffles (2-5x speedup)")
    print("   2. Broadcast small tables (5-10x speedup)")
    print("   3. Pre-partition on join keys for repeated joins")
    print("   4. Tune shuffle partitions: total_cores × 2-4")
    print("   5. Enable AQE for automatic optimization")
    print("   6. Use bucketing for star schema")
    print("   7. Monitor Spark UI for shuffle bottlenecks")
    
    spark.stop()


if __name__ == "__main__":
    main()
