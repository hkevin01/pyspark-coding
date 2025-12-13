#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
DISTRIBUTED JOINS - Optimization Techniques for Production Scale
================================================================================

MODULE OVERVIEW:
----------------
Joins are one of the most expensive operations in distributed computing because
they typically require shuffling large amounts of data across the network. A
naive join can shuffle terabytes of data, causing jobs to run for hours instead
of minutes. Understanding join optimization is critical for production performance.

This module provides a comprehensive guide to:
1. How distributed joins work under the hood
2. Join strategies (Broadcast, Sort-Merge, Shuffle Hash)
3. Optimization techniques to minimize shuffle
4. Handling data skew in joins
5. Different join types and their performance
6. Production best practices

PURPOSE:
--------
Learn to:
• Choose the right join strategy for your data
• Minimize network shuffle with broadcast joins
• Partition data correctly for efficient joins
• Handle data skew that causes join bottlenecks
• Understand join execution in Spark UI
• Optimize multi-table joins

DISTRIBUTED JOIN FUNDAMENTALS:
------------------------------

Problem: Data on Different Nodes
┌─────────────────────────────────────────────────────────────────┐
│                    BEFORE JOIN                                  │
├─────────────────────────────────────────────────────────────────┤
│  Table A (Large - 100GB)           Table B (Large - 50GB)       │
│  ┌──────────────────────┐          ┌──────────────────┐         │
│  │ Node 1: A1 (25GB)    │          │ Node 1: B1 (12GB)│         │
│  │ Node 2: A2 (25GB)    │          │ Node 2: B2 (13GB)│         │
│  │ Node 3: A3 (25GB)    │          │ Node 3: B3 (12GB)│         │
│  │ Node 4: A4 (25GB)    │          │ Node 4: B4 (13GB)│         │
│  └──────────────────────┘          └──────────────────┘         │
│                                                                 │
│  Problem: Records with same join key on different nodes!       │
│  Solution: Shuffle data so matching keys are on same node      │
└─────────────────────────────────────────────────────────────────┘

JOIN EXECUTION STRATEGIES:
--------------------------

Strategy 1: Broadcast Hash Join (Small Table)
┌─────────────────────────────────────────────────────────────────┐
│  BROADCAST JOIN (Optimal for small dimension tables)            │
├─────────────────────────────────────────────────────────────────┤
│  Small Table B (100MB) →  Broadcast to ALL nodes               │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │          Broadcast (B copied to all executors)          │   │
│  │  ┌──────────────────────────────────────────────────┐   │   │
│  │  │  B│  B│  B│  B│  B│  B│  B│  B│  B│  B│  B│  B│   │   │
│  │  └──────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────┘   │
│           ↓    ↓    ↓    ↓    ↓    ↓    ↓    ↓                │
│  Large Table A (100GB) stays partitioned:                      │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐         │
│  │ A1 │ A2 │ A3 │ A4 │ A5 │ A6 │ A7 │ A8 │ A9 │A10 │         │
│  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘         │
│   Join locally on each executor (no shuffle for A!)           │
│                                                                │
│  ✅ Pros: No shuffle of large table, very fast                │
│  ❌ Cons: Small table must fit in executor memory             │
│  📏 Threshold: spark.sql.autoBroadcastJoinThreshold (10MB)    │
└─────────────────────────────────────────────────────────────────┘

Strategy 2: Sort-Merge Join (Large-Large)
┌─────────────────────────────────────────────────────────────────┐
│  SORT-MERGE JOIN (Default for large-large joins)               │
├─────────────────────────────────────────────────────────────────┤
│  1. Shuffle both tables by join key (hash partitioning)        │
│                                                                 │
│  Table A (before shuffle):        Table B (before shuffle):    │
│  ┌─────┬─────┬─────┬─────┐       ┌─────┬─────┬─────┐          │
│  │ A1  │ A2  │ A3  │ A4  │       │ B1  │ B2  │ B3  │          │
│  │mixed│mixed│mixed│mixed│       │mixed│mixed│mixed│          │
│  └─────┴─────┴─────┴─────┘       └─────┴─────┴─────┘          │
│         ↓                                ↓                      │
│  ═══════════════════════════════════════════════════           │
│         SHUFFLE BY KEY (Expensive!)                            │
│  ═══════════════════════════════════════════════════           │
│         ↓                                ↓                      │
│  Table A (after shuffle):         Table B (after shuffle):     │
│  ┌─────┬─────┬─────┬─────┐       ┌─────┬─────┬─────┐          │
│  │key=1│key=2│key=3│key=4│       │key=1│key=2│key=3│          │
│  └─────┴─────┴─────┴─────┘       └─────┴─────┴─────┘          │
│                                                                 │
│  2. Sort each partition by join key                            │
│  3. Merge sorted partitions (efficient)                        │
│                                                                 │
│  ✅ Pros: Works for large-large joins, scalable                │
│  ❌ Cons: Expensive shuffle, memory for sorting                │
│  📊 Use: Default for DataFrames, most common                   │
└─────────────────────────────────────────────────────────────────┘

Strategy 3: Shuffle Hash Join
┌─────────────────────────────────────────────────────────────────┐
│  SHUFFLE HASH JOIN (Less common)                               │
├─────────────────────────────────────────────────────────────────┤
│  1. Shuffle both tables by join key                            │
│  2. Build hash table for smaller side (per partition)          │
│  3. Probe with larger side                                     │
│                                                                 │
│  ✅ Pros: No sorting needed                                    │
│  ❌ Cons: Hash table must fit in memory, shuffle both sides    │
│  📊 Use: spark.sql.join.preferSortMergeJoin=false              │
└─────────────────────────────────────────────────────────────────┘

JOIN TYPES AND PERFORMANCE:
---------------------------

Join Type Comparison:
┌────────────────┬─────────────────────┬──────────────┬───────────────┐
│ Join Type      │ Result              │ Shuffle      │ Use Case      │
├────────────────┼─────────────────────┼──────────────┼───────────────┤
│ INNER          │ Only matching rows  │ Both sides   │ Standard join │
│ LEFT OUTER     │ All left + matched  │ Both sides   │ Keep all left │
│ RIGHT OUTER    │ All right + matched │ Both sides   │ Keep all right│
│ FULL OUTER     │ All from both       │ Both sides   │ Union-like    │
│ LEFT SEMI      │ Left rows that match│ Both sides   │ Filtering     │
│ LEFT ANTI      │ Left rows no match  │ Both sides   │ Exclusion     │
│ CROSS          │ Cartesian product   │ Huge shuffle │ Rare (avoid!) │
└────────────────┴─────────────────────┴──────────────┴───────────────┘

Detailed Join Type Behavior:

INNER JOIN:
  A: [1, 2, 3]          B: [2, 3, 4]
  Result: [2, 3]  (only matching keys)

LEFT OUTER JOIN:
  A: [1, 2, 3]          B: [2, 3, 4]
  Result: [1, 2, 3]  (all from A, nulls for 1)

FULL OUTER JOIN:
  A: [1, 2, 3]          B: [2, 3, 4]
  Result: [1, 2, 3, 4]  (all from both, nulls for non-matches)

LEFT SEMI JOIN (Efficient Filtering):
  A: [1, 2, 3]          B: [2, 3, 4]
  Result: [2, 3]  (same as INNER but only A columns, no duplicates)
  
  Equivalent SQL: SELECT * FROM A WHERE A.key IN (SELECT key FROM B)
  ✅ Better than: INNER + SELECT DISTINCT + DROP B columns

LEFT ANTI JOIN (Exclusion):
  A: [1, 2, 3]          B: [2, 3, 4]
  Result: [1]  (only A rows with no match in B)
  
  Equivalent SQL: SELECT * FROM A WHERE A.key NOT IN (SELECT key FROM B)

OPTIMIZATION TECHNIQUES:
------------------------

Optimization 1: Broadcast Join for Small Tables
Rule: If one table < 10MB, ALWAYS broadcast

❌ Bad (shuffle both):
orders.join(products, "product_id")  # Both tables shuffled

✅ Good (broadcast small):
from pyspark.sql.functions import broadcast
orders.join(broadcast(products), "product_id")  # Only orders shuffled

Performance: 5-10x faster for large-small joins

Optimization 2: Pre-partition on Join Key
Rule: Partition both tables on join key BEFORE multiple joins

❌ Bad (random partitions):
df1.join(df2, "key")  # Random partitions, full shuffle

✅ Good (aligned partitions):
df1_partitioned = df1.repartition(100, "key")
df2_partitioned = df2.repartition(100, "key")
df1_partitioned.join(df2_partitioned, "key")  # Co-located data, minimal shuffle

Optimization 3: Cache After Partitioning
Rule: If joining same table multiple times, cache after partitioning

✅ Best:
df_partitioned = df.repartition(100, "key").cache()
df_partitioned.count()  # Materialize cache
result1 = df_partitioned.join(other1, "key")
result2 = df_partitioned.join(other2, "key")  # Uses cached partitions

Optimization 4: Filter BEFORE Join
Rule: Reduce data size before expensive operations

❌ Bad (join then filter):
df1.join(df2, "key").filter(col("amount") > 1000)  # Full shuffle, then filter

✅ Good (filter then join):
df1_filtered = df1.filter(col("amount") > 1000)
df1_filtered.join(df2, "key")  # Less data to shuffle

Optimization 5: Use Appropriate Join Type
Rule: Use LEFT SEMI for existence checks

❌ Bad (inefficient):
df1.join(df2, "key", "inner") \\
   .select(df1.columns) \\
   .distinct()  # Shuffle, duplicate elimination

✅ Good (efficient):
df1.join(df2, "key", "left_semi")  # No duplicates, only df1 columns

DATA SKEW IN JOINS:
-------------------

Problem: Skewed Join Keys
┌─────────────────────────────────────────────────────────────────┐
│              UNBALANCED JOIN (Data Skew)                        │
├─────────────────────────────────────────────────────────────────┤
│  Partition distribution after shuffle by join key:             │
│  ┌─┬─┬─┬──────────────────────────────────────────────┬─┬─┐    │
│  │P│P│P│      Partition 3 (90% of data)              │P│P│    │
│  │0│1│2│      One executor overloaded                │4│5│    │
│  └─┴─┴─┴──────────────────────────────────────────────┴─┴─┘    │
│   2m 2m 2m          1 hour (bottleneck!)            2m 2m      │
│                                                                 │
│  Cause: One key (e.g., user_id="popular") has 90% of data      │
│  Effect: Job takes 1 hour instead of 2 minutes                 │
└─────────────────────────────────────────────────────────────────┘

Skew Mitigation Technique 1: Salting
For skewed fact table joining dimension table:

# Step 1: Add salt to fact table (split hot keys)
from pyspark.sql.functions import concat, lit, rand
fact_salted = fact.withColumn("salt", (rand() * 10).cast("int")) \\
    .withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))

# Step 2: Replicate dimension table with all salts
from pyspark.sql.functions import explode, array
dim_replicated = dim.withColumn("salt", explode(array([lit(i) for i in range(10)]))) \\
    .withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))

# Step 3: Join on salted key (distributed across 10 partitions per key)
result = fact_salted.join(dim_replicated, "salted_key")

Skew Mitigation Technique 2: Separate Hot Keys
# Step 1: Identify hot keys
hot_keys = fact.groupBy("user_id").count() \\
    .filter(col("count") > 100000) \\
    .select("user_id")

# Step 2: Split into hot and cold
fact_hot = fact.join(broadcast(hot_keys), "user_id", "left_semi")
fact_cold = fact.join(broadcast(hot_keys), "user_id", "left_anti")

# Step 3: Process separately
result_hot = fact_hot.join(broadcast(dim), "user_id")  # Broadcast for hot
result_cold = fact_cold.join(dim, "user_id")  # Regular join for cold
result = result_hot.union(result_cold)

Skew Mitigation Technique 3: Adaptive Query Execution (Spark 3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# Spark automatically detects and handles skew

MULTI-TABLE JOINS:
------------------

Optimization: Join Order Matters
┌─────────────────────────────────────────────────────────────────┐
│  ❌ BAD ORDER (Large → Medium → Small):                         │
│                                                                 │
│  large_df (100GB)                                               │
│      .join(medium_df (10GB), "key1")  ← Shuffle 100GB + 10GB   │
│      .join(small_df (100MB), "key2")  ← Shuffle 110GB + 100MB  │
│                                                                 │
│  Total shuffle: ~220GB                                          │
├─────────────────────────────────────────────────────────────────┤
│  ✅ GOOD ORDER (Small → Medium → Large):                        │
│                                                                 │
│  small_df (100MB)                                               │
│      .join(medium_df (10GB), "key1")  ← Broadcast 100MB        │
│      .join(large_df (100GB), "key2")  ← Shuffle 10GB + 100GB   │
│                                                                 │
│  Total shuffle: ~110GB (2x improvement!)                        │
│                                                                 │
│  Rule: Join smallest tables first, largest last                │
└─────────────────────────────────────────────────────────────────┘

Star Schema Joins (Fact + Multiple Dimensions):
# Broadcast all small dimension tables
fact.join(broadcast(dim1), "dim1_id") \\
    .join(broadcast(dim2), "dim2_id") \\
    .join(broadcast(dim3), "dim3_id")

# Only fact table shuffled once

PERFORMANCE BENCHMARKS:
-----------------------

Typical Performance Impact:
┌──────────────────────────┬──────────────┬──────────────┬───────────┐
│ Scenario                 │ Naive        │ Optimized    │ Speedup   │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ Large-Small Join         │ 45 min       │ 5 min        │ 9x        │
│ (with broadcast)         │              │              │           │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ Large-Large Join         │ 30 min       │ 10 min       │ 3x        │
│ (with pre-partition)     │              │              │           │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ Skewed Join              │ 2 hours      │ 20 min       │ 6x        │
│ (with salting)           │              │              │           │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ Multi-table Join         │ 1 hour       │ 15 min       │ 4x        │
│ (join order + broadcast) │              │              │           │
└──────────────────────────┴──────────────┴──────────────┴───────────┘

MONITORING & DEBUGGING:
-----------------------

Spark UI Metrics to Check:
1. SQL Tab → Query Plan:
   • Look for "Exchange" (shuffle operations)
   • BroadcastHashJoin vs SortMergeJoin
   • Shuffle read/write sizes

2. Stages Tab → Task Metrics:
   • Shuffle Read Size: Look for skew (max >> median)
   • Task Duration: Identify stragglers
   • GC Time: Should be < 10% of task time

3. Executors Tab:
   • Memory usage during join
   • Shuffle read/write per executor

SQL Plan Example:
== Physical Plan ==
*(5) Project [...]
+- *(5) SortMergeJoin [id#1], [id#2]  ← Join type
   :- *(2) Sort [id#1]
   :  +- Exchange hashpartitioning(id#1, 200)  ← Shuffle!
   :     +- *(1) Filter [...]
   +- *(4) Sort [id#2]
      +- Exchange hashpartitioning(id#2, 200)  ← Shuffle!
         +- *(3) Filter [...]

BEST PRACTICES CHECKLIST:
-------------------------

☐ Use broadcast() for tables < 10MB
☐ Filter data before joins
☐ Partition both tables on join key (same partition count)
☐ Cache if joining same table multiple times
☐ Use left_semi for existence checks
☐ Avoid full outer joins when possible
☐ Join smallest tables first in multi-joins
☐ Monitor shuffle size in Spark UI
☐ Enable Adaptive Query Execution (Spark 3+)
☐ Handle data skew (salting or separate processing)
☐ Use appropriate join type (don't default to inner)
☐ Avoid cross joins (cartesian products)

COMMON MISTAKES:
----------------

❌ #1: Not broadcasting small tables
❌ #2: Different partition counts on join tables
❌ #3: Joining without filtering first
❌ #4: Ignoring data skew
❌ #5: Using inner join + distinct instead of left_semi
❌ #6: Wrong join order in multi-table joins
❌ #7: Not caching repeatedly joined tables
❌ #8: Using show() to inspect large join results (use explain())

TARGET AUDIENCE:
----------------
• Data engineers optimizing slow joins
• Anyone experiencing shuffle-related performance issues
• Teams handling multi-TB join operations
• Developers debugging OOM errors during joins

RELATED RESOURCES:
------------------
• cluster_computing/02_data_partitioning.py (partitioning strategies)
• cluster_computing/04_aggregations_at_scale.py
• security/02_common_mistakes.py (#8 Cartesian Joins, #9 Broadcast Joins)
• Spark SQL Performance Tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 2.0.0 - Comprehensive Distributed Joins Guide
UPDATED: 2024
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, count, lit
import time


def create_spark():
    return SparkSession.builder \
        .appName("DistributedJoins") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB \
        .getOrCreate()


def demonstrate_naive_join(spark):
    """Show the problem with naive joins."""
    print("=" * 70)
    print("1. NAIVE JOIN (Unoptimized)")
    print("=" * 70)
    
    # Create large datasets
    orders = spark.range(1, 100001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("amount", (col("order_id") * 10).cast("int"))
    
    customers = spark.range(1, 1001).toDF("customer_id") \
        .withColumn("name", lit("Customer"))
    
    print(f"📊 Orders: {orders.count():,} rows")
    print(f"📊 Customers: {customers.count():,} rows")
    
    # Naive join
    print("\n⚠️  Naive join (no optimization):")
    start = time.time()
    result = orders.join(customers, "customer_id")
    result.write.mode("overwrite").format("noop").save()
    naive_time = time.time() - start
    
    print(f"   Rows: {result.count():,}")
    print(f"   Time: {naive_time:.3f}s")
    print(f"   ⚠️  Full shuffle: Both tables shuffled across network")
    
    return naive_time


def demonstrate_broadcast_join(spark):
    """Optimize with broadcast join for small tables."""
    print("\n" + "=" * 70)
    print("2. BROADCAST JOIN (Optimized for Small Tables)")
    print("=" * 70)
    
    # Same datasets
    orders = spark.range(1, 100001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("amount", (col("order_id") * 10).cast("int"))
    
    customers = spark.range(1, 1001).toDF("customer_id") \
        .withColumn("name", lit("Customer"))
    
    # Broadcast join
    print("\n✅ Broadcast join (small table sent to all nodes):")
    start = time.time()
    result = orders.join(broadcast(customers), "customer_id")
    result.write.mode("overwrite").format("noop").save()
    broadcast_time = time.time() - start
    
    print(f"   Rows: {result.count():,}")
    print(f"   Time: {broadcast_time:.3f}s")
    print(f"   ✅ No shuffle: Customers broadcast to all executors")
    print(f"   ✅ Only orders partitioned")
    
    print("\n💡 When to use broadcast join:")
    print("   - Small table < 10MB (default threshold)")
    print("   - Small table fits in executor memory")
    print("   - Avoid shuffle for large table")
    
    return broadcast_time


def demonstrate_partition_join(spark):
    """Optimize with pre-partitioning on join key."""
    print("\n" + "=" * 70)
    print("3. PARTITIONED JOIN (Optimized for Large Tables)")
    print("=" * 70)
    
    # Larger datasets where broadcast won't work
    orders = spark.range(1, 500001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 5000).cast("int")) \
        .withColumn("amount", (col("order_id") * 10).cast("int"))
    
    customers = spark.range(1, 5001).toDF("customer_id") \
        .withColumn("name", lit("Customer")) \
        .withColumn("city", lit("City"))
    
    print(f"📊 Orders: {orders.count():,} rows")
    print(f"📊 Customers: {customers.count():,} rows")
    print("   (Too large for broadcast)")
    
    # Without pre-partitioning
    print("\n❌ Without pre-partitioning:")
    start = time.time()
    result_bad = orders.join(customers, "customer_id")
    result_bad.write.mode("overwrite").format("noop").save()
    bad_time = time.time() - start
    print(f"   Time: {bad_time:.3f}s")
    
    # With pre-partitioning
    print("\n✅ With pre-partitioning on join key:")
    start = time.time()
    orders_partitioned = orders.repartition(8, "customer_id")
    customers_partitioned = customers.repartition(8, "customer_id")
    result_good = orders_partitioned.join(customers_partitioned, "customer_id")
    result_good.write.mode("overwrite").format("noop").save()
    good_time = time.time() - start
    
    print(f"   Time: {good_time:.3f}s")
    print(f"   Speedup: {bad_time / good_time:.2f}x")
    print(f"   ✅ Co-located: Same keys on same nodes")
    print(f"   ✅ Reduced shuffle: Data already partitioned correctly")


def demonstrate_join_types(spark):
    """Compare different join types and their shuffle behavior."""
    print("\n" + "=" * 70)
    print("4. JOIN TYPES & SHUFFLE BEHAVIOR")
    print("=" * 70)
    
    # Create datasets
    df1 = spark.range(1, 10001).toDF("id") \
        .withColumn("value1", col("id") * 10)
    
    df2 = spark.range(5000, 15001).toDF("id") \
        .withColumn("value2", col("id") * 20)
    
    print(f"📊 DataFrame 1: {df1.count():,} rows (1-10,000)")
    print(f"📊 DataFrame 2: {df2.count():,} rows (5,000-15,000)")
    print(f"   Overlap: 5,000 rows (5,000-10,000)")
    
    # Inner join
    print("\n🔗 INNER JOIN:")
    inner_result = df1.join(df2, "id", "inner")
    inner_count = inner_result.count()
    print(f"   Result: {inner_count:,} rows (only overlapping)")
    print(f"   Shuffle: Both sides")
    
    # Left outer join
    print("\n🔗 LEFT OUTER JOIN:")
    left_result = df1.join(df2, "id", "left")
    left_count = left_result.count()
    print(f"   Result: {left_count:,} rows (all from left)")
    print(f"   Shuffle: Both sides")
    
    # Right outer join
    print("\n🔗 RIGHT OUTER JOIN:")
    right_result = df1.join(df2, "id", "right")
    right_count = right_result.count()
    print(f"   Result: {right_count:,} rows (all from right)")
    print(f"   Shuffle: Both sides")
    
    # Full outer join
    print("\n🔗 FULL OUTER JOIN:")
    full_result = df1.join(df2, "id", "full")
    full_count = full_result.count()
    print(f"   Result: {full_count:,} rows (all from both)")
    print(f"   Shuffle: Both sides (most expensive)")
    
    # Left semi join (filtering)
    print("\n🔗 LEFT SEMI JOIN:")
    semi_result = df1.join(df2, "id", "left_semi")
    semi_count = semi_result.count()
    print(f"   Result: {semi_count:,} rows (left rows that match)")
    print(f"   Columns: Only from left table")
    print(f"   Use case: Filtering without duplicating data")


def demonstrate_skewed_join(spark):
    """Handle data skew in joins."""
    print("\n" + "=" * 70)
    print("5. HANDLING SKEWED JOINS")
    print("=" * 70)
    
    # Create skewed dataset (90% have same key)
    skewed_data = []
    for i in range(1, 10001):
        if i < 9000:
            key = 1  # 90% of data
        else:
            key = i % 100
        skewed_data.append((i, key, i * 100))
    
    orders_skewed = spark.createDataFrame(skewed_data, ["order_id", "customer_id", "amount"])
    customers = spark.range(1, 101).toDF("customer_id") \
        .withColumn("name", lit("Customer"))
    
    print("📊 Skewed dataset:")
    orders_skewed.groupBy("customer_id").count() \
        .orderBy(col("count").desc()).show(5)
    
    # Problem: Skewed join
    print("\n❌ Problem: Skewed join (one partition overloaded):")
    start = time.time()
    result_skewed = orders_skewed.join(customers, "customer_id")
    result_skewed.write.mode("overwrite").format("noop").save()
    skewed_time = time.time() - start
    print(f"   Time: {skewed_time:.3f}s")
    print(f"   ⚠️  One partition processes 90% of data")
    
    # Solution: Salting + broadcast
    print("\n✅ Solution 1: Broadcast join (if customers small):")
    start = time.time()
    result_broadcast = orders_skewed.join(broadcast(customers), "customer_id")
    result_broadcast.write.mode("overwrite").format("noop").save()
    broadcast_time = time.time() - start
    print(f"   Time: {broadcast_time:.3f}s")
    print(f"   Speedup: {skewed_time / broadcast_time:.2f}x")
    print(f"   ✅ No shuffle, no skew issue")
    
    # Solution: Salting (for large dimension tables)
    print("\n✅ Solution 2: Salting (if both tables large):")
    from pyspark.sql.functions import concat, rand, lit as spark_lit, explode, array
    
    # Add salt to fact table
    orders_salted = orders_skewed.withColumn(
        "salt",
        (rand() * 10).cast("int")
    ).withColumn(
        "salted_key",
        concat(col("customer_id").cast("string"), spark_lit("_"), col("salt").cast("string"))
    )
    
    # Explode dimension table with all salt values
    customers_exploded = customers.withColumn(
        "salt",
        explode(array([lit(i) for i in range(10)]))
    ).withColumn(
        "salted_key",
        concat(col("customer_id").cast("string"), spark_lit("_"), col("salt").cast("string"))
    )
    
    start = time.time()
    result_salted = orders_salted.join(customers_exploded, "salted_key")
    result_salted.write.mode("overwrite").format("noop").save()
    salted_time = time.time() - start
    
    print(f"   Time: {salted_time:.3f}s")
    print(f"   ✅ Distributed: Skewed key split across 10 partitions")
    print(f"   ✅ Balanced: Each partition processes ~10% of data")


def demonstrate_join_best_practices(spark):
    """Summary of join best practices."""
    print("\n" + "=" * 70)
    print("6. JOIN BEST PRACTICES")
    print("=" * 70)
    
    print("""
📋 Decision Tree for Joins:

┌─ Small table (< 10MB)?
│  └─ YES → Use broadcast join ✅
│     from pyspark.sql.functions import broadcast
│     result = large.join(broadcast(small), "key")
│
└─ Both tables large?
   │
   ├─ Same partition count & key?
   │  └─ YES → Already optimized ✅
   │
   └─ Different partitions?
      └─ Repartition both on join key
         df1 = df1.repartition(N, "key")
         df2 = df2.repartition(N, "key")
         result = df1.join(df2, "key")

🎯 Optimization Checklist:

1. ✅ Filter before join (reduce data size)
2. ✅ Use broadcast for small tables (< 10MB)
3. ✅ Partition both tables on join key
4. ✅ Use same partition count for both tables
5. ✅ Cache tables if joining multiple times
6. ✅ Use left semi join for filtering
7. ✅ Handle skew with salting or broadcast
8. ✅ Monitor Spark UI for shuffle size

⚠️  Common Mistakes:

1. ❌ Joining without filtering first
2. ❌ Not broadcasting small tables
3. ❌ Different partition counts for join tables
4. ❌ Ignoring data skew
5. ❌ Using full outer join when not needed
6. ❌ Multiple joins without caching
    """)


def main():
    spark = create_spark()
    
    print("🔗 DISTRIBUTED JOINS IN PYSPARK")
    print("=" * 70)
    
    # 1. Naive join
    naive_time = demonstrate_naive_join(spark)
    
    # 2. Broadcast join
    broadcast_time = demonstrate_broadcast_join(spark)
    
    # 3. Partitioned join
    demonstrate_partition_join(spark)
    
    # 4. Join types
    demonstrate_join_types(spark)
    
    # 5. Skewed joins
    demonstrate_skewed_join(spark)
    
    # 6. Best practices
    demonstrate_join_best_practices(spark)
    
    print("\n" + "=" * 70)
    print("✅ DISTRIBUTED JOINS DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print(f"   1. Broadcast join: {naive_time / broadcast_time:.2f}x faster for small tables")
    print("   2. Partition on join key for large-large joins")
    print("   3. Handle skew with salting or broadcast")
    print("   4. Filter before join to reduce shuffle")
    print("   5. Monitor Spark UI shuffle metrics")
    print("   6. Cache if joining same tables multiple times")
    
    spark.stop()


if __name__ == "__main__":
    main()
