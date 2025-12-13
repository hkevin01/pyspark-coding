#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
DATA PARTITIONING - Mastering Distribution for Performance
================================================================================

MODULE OVERVIEW:
----------------
Data partitioning is the foundation of distributed computing performance in
Spark. A partition is a logical chunk of data that resides on a single executor.
Understanding partitioning is crucial because it directly impacts:
• Parallelism: More partitions = more parallel tasks
• Network shuffle: Bad partitioning = excessive data movement
• Memory usage: Partition size affects memory pressure
• Performance: Balanced partitions = balanced execution

This module covers:
1. How Spark partitions data by default
2. When and how to repartition data
3. Difference between repartition() and coalesce()
4. Partitioning strategies (random, hash, range)
5. Handling data skew
6. Production best practices

PURPOSE:
--------
Learn to:
• Optimize partition count for your data size
• Choose the right partitioning strategy
• Balance workload across executors
• Minimize shuffle operations
• Handle data skew effectively
• Tune for joins and aggregations

PARTITIONING FUNDAMENTALS:
--------------------------

What is a Partition?
┌─────────────────────────────────────────────────────────────────┐
│                       DATAFRAME (Logical)                       │
│                  1 Million Rows Total                           │
├─────────────────────────────────────────────────────────────────┤
│                  Divided into Partitions                        │
│  ┌────────────┬────────────┬────────────┬────────────┐         │
│  │Partition 0 │Partition 1 │Partition 2 │Partition 3 │         │
│  │ 250K rows  │ 250K rows  │ 250K rows  │ 250K rows  │         │
│  └────────────┴────────────┴────────────┴────────────┘         │
│        ↓            ↓            ↓            ↓                 │
│  ┌────────────┬────────────┬────────────┬────────────┐         │
│  │Executor 1  │Executor 2  │Executor 3  │Executor 4  │         │
│  │ Task 1     │ Task 2     │ Task 3     │ Task 4     │         │
│  │ Processes  │ Processes  │ Processes  │ Processes  │         │
│  │ 250K rows  │ 250K rows  │ 250K rows  │ 250K rows  │         │
│  └────────────┴────────────┴────────────┴────────────┘         │
│                                                                 │
│  Key: Each partition processed by one task on one executor      │
└─────────────────────────────────────────────────────────────────┘

Partition Size Guidelines:
┌──────────────────────────┬───────────────┬──────────────────────┐
│ Partition Size           │ Status        │ Impact               │
├──────────────────────────┼───────────────┼──────────────────────┤
│ < 10 MB                  │ ❌ Too Small  │ Task overhead        │
│ 10 MB - 128 MB           │ ⚠️  Small     │ More tasks needed    │
│ 128 MB - 256 MB          │ ✅ Optimal    │ Best performance     │
│ 256 MB - 1 GB            │ ✅ Good       │ Acceptable           │
│ 1 GB - 2 GB              │ ⚠️  Large     │ Memory pressure      │
│ > 2 GB                   │ ❌ Too Large  │ OOM, slow tasks      │
└──────────────────────────┴───────────────┴──────────────────────┘

HOW SPARK CREATES PARTITIONS:
------------------------------

1. Reading Data:
   • HDFS/S3: One partition per file block (typically 128MB)
   • Parquet: Based on row group size and schema
   • JDBC: Based on partitionColumn, lowerBound, upperBound
   • CSV: Based on spark.sql.files.maxPartitionBytes (128MB default)

   Example:
   10GB file on HDFS (128MB blocks)
   → 10,000MB / 128MB = ~78 partitions

2. Transformations:
   • Narrow transformations: Preserve parent partitioning
     (map, filter, union)
   • Wide transformations: Shuffle to new partitioning
     (groupBy, join, distinct)

3. Default Parallelism:
   spark.default.parallelism:
   • Local mode: Number of cores on local machine
   • Cluster: Total cores across all executors
   
   spark.sql.shuffle.partitions (default: 200):
   • Used after shuffle operations (join, groupBy)
   • Often needs tuning based on data size

PARTITIONING OPERATIONS:
------------------------

repartition(n) - Full Shuffle:
┌────────────────────────────────────────────────────────────┐
│  BEFORE: 4 partitions            AFTER: 8 partitions       │
│  ┌────┬────┬────┬────┐          ┌─┬─┬─┬─┬─┬─┬─┬─┐         │
│  │ P0 │ P1 │ P2 │ P3 │          │0│1│2│3│4│5│6│7│         │
│  └────┴────┴────┴────┘          └─┴─┴─┴─┴─┴─┴─┴─┘         │
│         ↓                               ↑                   │
│     Full Shuffle ─────────────────────────                 │
│     (All data redistributed)                               │
│                                                            │
│  • Can increase OR decrease partitions                    │
│  • Data redistributed randomly (or by column)             │
│  • Expensive: Full network shuffle                        │
│  • Use for: Increasing parallelism, rebalancing skew      │
└────────────────────────────────────────────────────────────┘

coalesce(n) - Optimized Reduction:
┌────────────────────────────────────────────────────────────┐
│  BEFORE: 8 partitions            AFTER: 4 partitions       │
│  ┌─┬─┬─┬─┬─┬─┬─┬─┐              ┌────┬────┬────┬────┐     │
│  │0│1│2│3│4│5│6│7│              │ P0 │ P1 │ P2 │ P3 │     │
│  └─┴─┴─┴─┴─┴─┴─┴─┘              └────┴────┴────┴────┘     │
│   └─┴ └─┴ └─┴ └─┴                                         │
│    Merge   Merge   Merge   Merge                          │
│    (No shuffle)                                            │
│                                                            │
│  • Only decreases partitions                              │
│  • Merges adjacent partitions                             │
│  • Cheap: No network shuffle                              │
│  • Use for: Reducing output files, final optimization     │
└────────────────────────────────────────────────────────────┘

PARTITIONING STRATEGIES:
------------------------

1. Random Partitioning (Default):
   df.repartition(10)
   
   • Data distributed randomly
   • Balanced distribution (usually)
   • No data locality
   • Use for: General purpose, balanced workload

2. Hash Partitioning (By Column):
   df.repartition(10, "user_id")
   
   • Same key → Same partition
   • Deterministic (consistent hashing)
   • Enables partition-wise joins
   • Use for: groupBy, joins on same column
   
   Example:
   ┌──────────────────────────────────────────────────────┐
   │  user_id=123 → hash(123) % 10 → Partition 3         │
   │  user_id=456 → hash(456) % 10 → Partition 6         │
   │  user_id=789 → hash(789) % 10 → Partition 9         │
   │                                                      │
   │  All operations on user_id=123 go to Partition 3    │
   └──────────────────────────────────────────────────────┘

3. Range Partitioning:
   df.repartitionByRange(10, "timestamp")
   
   • Data sorted into ranges
   • Maintains sort order
   • Can have skew if data not uniform
   • Use for: Sorting, range queries, time-series
   
   Example:
   ┌──────────────────────────────────────────────────────┐
   │  Partition 0: timestamp < 2024-01-01                 │
   │  Partition 1: 2024-01-01 ≤ timestamp < 2024-02-01    │
   │  Partition 2: 2024-02-01 ≤ timestamp < 2024-03-01    │
   │  ...                                                 │
   └──────────────────────────────────────────────────────┘

CALCULATING OPTIMAL PARTITIONS:
--------------------------------

Formula:
optimal_partitions = (total_data_size_MB / target_partition_size_MB)

Where:
• target_partition_size_MB: 128-256 MB (sweet spot)
• Minimum: num_executors * executor_cores
• Maximum: No hard limit, but overhead increases

Examples:

Example 1: Small Data (1 GB)
data_size = 1,000 MB
target_size = 200 MB
optimal_partitions = 1,000 / 200 = 5 partitions

Example 2: Medium Data (50 GB)
data_size = 50,000 MB
target_size = 200 MB
optimal_partitions = 50,000 / 200 = 250 partitions

Example 3: Large Data (1 TB)
data_size = 1,000,000 MB
target_size = 256 MB
optimal_partitions = 1,000,000 / 256 = 3,906 partitions

Alternative Formula (Core-based):
partitions = num_executors * executor_cores * (2 to 4)

Example: 10 executors × 4 cores = 40 cores
Recommended: 80-160 partitions (2-4x cores)

DATA SKEW PROBLEM:
------------------

What is Data Skew?
┌──────────────────────────────────────────────────────────┐
│           BALANCED (Good)         SKEWED (Bad)           │
│  ┌────┬────┬────┬────┐      ┌────┬─┬─┬──────────────┐   │
│  │ 25%│ 25%│ 25%│ 25%│      │ 5% │5│5│    85%       │   │
│  │    │    │    │    │      │    │ │ │              │   │
│  └────┴────┴────┴────┘      └────┴─┴─┴──────────────┘   │
│   4 tasks @ 10 min each      3 tasks @ 2 min            │
│   Total: 10 minutes          1 task @ 1 hour            │
│                              Total: 1 hour (bottleneck!)│
└──────────────────────────────────────────────────────────┘

Causes of Skew:
• Uneven key distribution (popular keys get more data)
• NULL values grouped together
• Natural data patterns (Zipf distribution)

Symptoms:
• One or few tasks take much longer
• Executor OOM on specific partitions
• Shuffle read skew in Spark UI
• 90% tasks complete quickly, 10% run forever

SKEW MITIGATION TECHNIQUES:
---------------------------

Technique 1: Salting (Add Random Key)
Original:
  groupBy("user_id")  # 90% of data has user_id = "popular_user"

Salted:
  # Add random salt to split hot key
  df.withColumn("salt", (rand() * 10).cast("int")) \\
    .withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt"))) \\
    .groupBy("salted_key") \\
    .agg(...) \\
    .groupBy("user_id") \\  # Remove salt, re-aggregate
    .agg(...)

Technique 2: Broadcast Join (for small dimension tables)
  # Instead of shuffling large fact table
  large_df.join(broadcast(small_df), "key")

Technique 3: Adaptive Query Execution (Spark 3.0+)
  spark.conf.set("spark.sql.adaptive.enabled", "true")
  spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
  # Spark automatically detects and handles skew

Technique 4: Separate Processing for Hot Keys
  # Process hot keys separately with broadcast
  # Process normal keys with standard join

BEST PRACTICES:
---------------

✅ DO:
1. Set spark.sql.shuffle.partitions based on data size
2. Use coalesce() when reducing partitions
3. Partition on join keys before joins
4. Cache after expensive repartitioning
5. Monitor partition sizes in Spark UI
6. Use salting for skewed data
7. Enable Adaptive Query Execution (Spark 3+)

❌ DON'T:
1. Use default 200 shuffle partitions for all data sizes
2. Use repartition() when coalesce() works
3. Create too many small partitions (< 10 MB)
4. Create too few large partitions (> 2 GB)
5. Ignore data skew
6. Repartition without checking current partition count

PERFORMANCE IMPACT:
-------------------

Bad Partitioning Costs:
• Too many partitions: Task scheduling overhead (milliseconds × thousands)
• Too few partitions: Underutilized cluster, memory pressure
• Unbalanced partitions: Stragglers delay entire job
• Wrong key: Excessive shuffle, slow joins

Good Partitioning Benefits:
• Maximizes parallelism
• Minimizes shuffle
• Balances workload
• Reduces memory pressure
• Faster joins and aggregations

Example Performance:
┌──────────────────────────┬──────────────┬──────────────┐
│ Scenario                 │ Bad Practice │ Good Practice│
├──────────────────────────┼──────────────┼──────────────┤
│ 1 GB data                │ 200 parts    │ 5 parts      │
│                          │ 15 min       │ 3 min (5x)   │
├──────────────────────────┼──────────────┼──────────────┤
│ 100 GB join              │ No partition │ Partition key│
│                          │ 45 min       │ 12 min (3.7x)│
├──────────────────────────┼──────────────┼──────────────┤
│ Skewed group by          │ No handling  │ Salting      │
│                          │ 1 hour       │ 15 min (4x)  │
└──────────────────────────┴──────────────┴──────────────┘

MONITORING PARTITIONS:
----------------------

Check Partition Count:
>>> df.rdd.getNumPartitions()
8

View Partition Distribution:
>>> df.withColumn("partition", spark_partition_id()) \\
...   .groupBy("partition").count() \\
...   .orderBy("partition").show()

Spark UI Metrics:
• Stage Details: Task duration distribution (look for stragglers)
• Shuffle Read: Size per task (look for skew)
• Executor Memory: Usage per executor

TARGET AUDIENCE:
----------------
• Data engineers optimizing Spark jobs
• Anyone experiencing slow joins or aggregations
• Developers handling large-scale data processing
• Teams debugging performance bottlenecks

RELATED RESOURCES:
------------------
• cluster_computing/03_distributed_joins.py (partition-aware joins)
• cluster_computing/04_aggregations_at_scale.py
• cluster_computing/08_shuffle_optimization.py
• security/02_common_mistakes.py (#6 Wrong Shuffle Partitions)

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 2.0.0 - Comprehensive Partitioning Guide
UPDATED: 2024
================================================================================
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, spark_partition_id


def create_spark():
    return (
        SparkSession.builder.appName("DataPartitioning")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def demonstrate_default_partitioning(spark):
    """Show how Spark partitions data by default."""
    print("=" * 70)
    print("1. DEFAULT PARTITIONING")
    print("=" * 70)

    # Create large dataset
    df = spark.range(1, 1000001)  # 1 million rows

    print(f"✅ Created DataFrame with {df.count():,} rows")
    print(f"📦 Default partitions: {df.rdd.getNumPartitions()}")

    # Show partition distribution
    partition_df = (
        df.withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id")
        .agg(count("*").alias("row_count"))
    )

    print("\n📊 Rows per partition:")
    partition_df.orderBy("partition_id").show()

    # Rule of thumb
    num_cores = spark.sparkContext.defaultParallelism
    print(f"\n💡 Default parallelism: {num_cores} cores")
    print(f"💡 Recommended partitions: {num_cores * 2} to {num_cores * 4}")


def demonstrate_repartition_vs_coalesce(spark):
    """Compare repartition() vs coalesce()."""
    print("\n" + "=" * 70)
    print("2. REPARTITION VS COALESCE")
    print("=" * 70)

    # Start with 8 partitions
    df = spark.range(1, 100001).repartition(8)
    print(f"📦 Initial partitions: {df.rdd.getNumPartitions()}")

    # REPARTITION: Increase partitions (full shuffle)
    print("\n🔄 Using repartition(16) - FULL SHUFFLE:")
    start = time.time()
    df_repartitioned = df.repartition(16)
    df_repartitioned.write.mode("overwrite").format("noop").save()
    duration = time.time() - start
    print(f"   New partitions: {df_repartitioned.rdd.getNumPartitions()}")
    print(f"   Time: {duration:.3f}s")
    print(f"   ⚠️  Full shuffle: Data moved across all nodes")

    # COALESCE: Decrease partitions (optimized, no shuffle)
    print("\n🔄 Using coalesce(4) - NO SHUFFLE:")
    start = time.time()
    df_coalesced = df.coalesce(4)
    df_coalesced.write.mode("overwrite").format("noop").save()
    duration = time.time() - start
    print(f"   New partitions: {df_coalesced.rdd.getNumPartitions()}")
    print(f"   Time: {duration:.3f}s")
    print(f"   ✅ Optimized: Merges adjacent partitions, no shuffle")

    # When to use each
    print("\n📝 When to use:")
    print("   repartition(N): Increase parallelism, balance skewed data")
    print("   coalesce(N): Reduce output files, final stage optimization")


def demonstrate_partitioning_strategies(spark):
    """Show different partitioning strategies."""
    print("\n" + "=" * 70)
    print("3. PARTITIONING STRATEGIES")
    print("=" * 70)

    # Create dataset with categories
    data = [(i, f"category_{i % 10}", i * 100) for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "category", "amount"])

    # Strategy 1: Random partitioning
    print("\n📊 Strategy 1: Random Partitioning")
    df_random = df.repartition(4)
    print(f"   Partitions: {df_random.rdd.getNumPartitions()}")
    print("   ✅ Good for: General balanced distribution")
    print("   ❌ Bad for: Operations on specific keys")

    # Strategy 2: Hash partitioning by key
    print("\n�� Strategy 2: Hash Partitioning by Key")
    df_hash = df.repartition(4, "category")
    print(f"   Partitions: {df_hash.rdd.getNumPartitions()}")
    print("   ✅ Good for: Group-by, joins on same key")
    print("   ✅ Same category always goes to same partition")

    # Show distribution
    partition_dist = (
        df_hash.withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id", "category")
        .agg(count("*").alias("count"))
    )

    print("\n   Distribution by partition and category:")
    partition_dist.orderBy("partition_id", "category").show(20)

    # Strategy 3: Range partitioning
    print("\n📊 Strategy 3: Range Partitioning")
    df_range = df.repartitionByRange(4, "amount")
    print(f"   Partitions: {df_range.rdd.getNumPartitions()}")
    print("   ✅ Good for: Sorting, range queries")
    print("   ✅ Sorted data distribution")


def demonstrate_partition_best_practices(spark):
    """Best practices for production."""
    print("\n" + "=" * 70)
    print("4. PARTITION BEST PRACTICES")
    print("=" * 70)

    # Create large dataset
    df = spark.range(1, 1000001)

    # Practice 1: Right-size partitions
    print("\n💡 Practice 1: Right-Size Partitions")
    print("   Rule: 128MB - 256MB per partition")
    print("   Formula: num_partitions = data_size_MB / 256")
    print("   Example: 10GB data → 10,000MB / 256 ≈ 40 partitions")

    # Practice 2: Partition before expensive operations
    print("\n💡 Practice 2: Partition Before Expensive Operations")

    # Bad: Join without partitioning
    df1 = spark.range(1, 100001).withColumnRenamed("id", "key")
    df2 = spark.range(1, 50001).withColumnRenamed("id", "key")

    print("\n   ❌ Bad: Direct join (unbalanced):")
    start = time.time()
    result_bad = df1.join(df2, "key")
    result_bad.count()
    bad_time = time.time() - start
    print(f"      Time: {bad_time:.3f}s")

    # Good: Partition before join
    print("\n   ✅ Good: Partition before join:")
    start = time.time()
    df1_partitioned = df1.repartition(4, "key")
    df2_partitioned = df2.repartition(4, "key")
    result_good = df1_partitioned.join(df2_partitioned, "key")
    result_good.count()
    good_time = time.time() - start
    print(f"      Time: {good_time:.3f}s")
    print(f"      Speedup: {bad_time / good_time:.2f}x")

    # Practice 3: Cache after repartitioning
    print("\n💡 Practice 3: Cache After Repartitioning")
    print("   If using DataFrame multiple times:")
    df_partitioned = df.repartition(8)
    df_partitioned.cache()
    df_partitioned.count()  # Materialize cache
    print("   ✅ Cached partitioned data")
    print("   ✅ Future operations use cached partitions")


def demonstrate_skew_handling(spark):
    """Handle data skew with salting."""
    print("\n" + "=" * 70)
    print("5. HANDLING DATA SKEW")
    print("=" * 70)

    # Create skewed dataset (90% of data in one category)
    skewed_data = []
    for i in range(1, 10001):
        if i < 9000:
            category = "popular"  # 90% of data
        else:
            category = f"cat_{i % 10}"
        skewed_data.append((i, category, i * 100))

    df_skewed = spark.createDataFrame(skewed_data, ["id", "category", "amount"])

    print("📊 Skewed Dataset:")
    df_skewed.groupBy("category").count().orderBy(col("count").desc()).show(5)

    # Problem: One partition gets 90% of data
    print("\n❌ Problem: Unbalanced partitions")
    df_skewed_partitioned = df_skewed.repartition(4, "category")
    partition_sizes = (
        df_skewed_partitioned.withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id")
        .count()
    )
    print("   Partition sizes:")
    partition_sizes.orderBy("partition_id").show()

    # Solution: Salting
    print("\n✅ Solution: Salting Technique")
    from pyspark.sql.functions import concat, lit, rand

    # SALTING EXPLAINED:
    # ==================
    # Problem: "popular" category has 90% of data → One partition is overloaded
    # Solution: Split the hot key into multiple keys using random "salt"
    #
    # How it works:
    # 1. rand() generates random number between 0.0 and 1.0 for each row
    # 2. Multiply by N (here 4) to get range [0.0, 4.0)
    # 3. Cast to int to get discrete values: 0, 1, 2, 3
    # 4. Append salt to key: "popular" becomes "popular_0", "popular_1", "popular_2", "popular_3"
    # 5. Now "popular" is split across 4 partitions instead of 1!
    #
    # Example transformation:
    #   Original:  category="popular" (90% of data in one partition)
    #   Salted:    "popular_0", "popular_1", "popular_2", "popular_3"
    #   Result:    Each partition gets ~22.5% of data (90% / 4)
    #
    # Why rand() works:
    # - Random distribution ensures even split across salt values
    # - Each row independently gets random salt (0-3)
    # - Over large dataset, ~25% of rows get each salt value
    # - Hash partitioning on salted key distributes evenly
    #
    # Trade-off:
    # - Pros: Balanced partitions, better parallelism, faster execution
    # - Cons: Need extra aggregation step to remove salt later (if needed)
    #
    # When to use:
    # - Skewed groupBy (one key has majority of data)
    # - Skewed joins (hot keys cause stragglers)
    # - Uneven partition sizes visible in Spark UI
    df_salted = df_skewed.withColumn(
        "salted_category", concat(col("category"), lit("_"), (rand() * 4).cast("int"))
    )

    df_salted_partitioned = df_salted.repartition(4, "salted_category")
    salted_partition_sizes = (
        df_salted_partitioned.withColumn("partition_id", spark_partition_id())
        .groupBy("partition_id")
        .count()
    )

    print("   Salted partition sizes:")
    salted_partition_sizes.orderBy("partition_id").show()
    print("   ✅ More balanced distribution!")


def main():
    spark = create_spark()

    print("🔧 DATA PARTITIONING STRATEGIES")
    print("=" * 70)

    demonstrate_default_partitioning(spark)
    demonstrate_repartition_vs_coalesce(spark)
    demonstrate_partitioning_strategies(spark)
    demonstrate_partition_best_practices(spark)
    demonstrate_skew_handling(spark)

    print("\n" + "=" * 70)
    print("✅ PARTITIONING DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. repartition(): Full shuffle, increases/decreases partitions")
    print("   2. coalesce(): No shuffle, only decreases partitions")
    print("   3. Partition on join key for better performance")
    print("   4. Target 128-256MB per partition")
    print("   5. Use salting to handle data skew")
    print("   6. Cache after expensive repartitioning")

    spark.stop()


if __name__ == "__main__":
    main()
