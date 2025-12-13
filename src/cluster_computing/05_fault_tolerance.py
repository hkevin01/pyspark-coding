#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
FAULT TOLERANCE - Resilience, Recovery, and Checkpointing in Production
================================================================================

MODULE OVERVIEW:
----------------
Fault tolerance is Spark's superpower - the ability to automatically recover
from failures without losing work. In distributed systems, failures are not
edge cases but normal operations: executors crash, network partitions occur,
spot instances get reclaimed, and disks fill up. Spark handles all of this
through lineage-based recovery and checkpointing.

This module provides a comprehensive guide to:
1. How Spark achieves fault tolerance through RDD lineage
2. Persistence levels and their trade-offs
3. Checkpointing to truncate long lineage
4. Recovery strategies and optimization
5. Best practices for production resilience
6. Cost-performance trade-offs

PURPOSE:
--------
Learn to:
• Understand how lineage enables automatic recovery
• Choose appropriate persistence strategies (cache vs persist)
• Use checkpointing to optimize recovery time
• Handle executor failures gracefully
• Minimize recomputation costs
• Build resilient production pipelines

FAULT TOLERANCE FUNDAMENTALS:
------------------------------

How Spark Achieves Fault Tolerance:
┌─────────────────────────────────────────────────────────────────┐
│                    RDD LINEAGE GRAPH                            │
├─────────────────────────────────────────────────────────────────┤
│  Input Data                                                     │
│  ┌────────────┐                                                 │
│  │   HDFS     │                                                 │
│  │ (Durable)  │                                                 │
│  └─────┬──────┘                                                 │
│        │ read()                                                 │
│        ↓                                                        │
│  ┌─────────────┐                                               │
│  │    RDD1     │ ← Lineage: read("hdfs://data")                │
│  └─────┬───────┘                                               │
│        │ map(x => x * 2)                                       │
│        ↓                                                        │
│  ┌─────────────┐                                               │
│  │    RDD2     │ ← Lineage: RDD1.map(x => x * 2)               │
│  └─────┬───────┘                                               │
│        │ filter(x => x > 100)                                  │
│        ↓                                                        │
│  ┌─────────────┐                                               │
│  │    RDD3     │ ← Lineage: RDD2.filter(x => x > 100)          │
│  └─────────────┘                                               │
│                                                                 │
│  If RDD3 partition lost:                                       │
│  1. Check lineage: RDD2.filter(x => x > 100)                   │
│  2. Check lineage: RDD1.map(x => x * 2)                        │
│  3. Recompute from source: read("hdfs://data")                 │
│  4. Replay transformations: map → filter                       │
│  5. Recovery complete! ✅                                      │
└─────────────────────────────────────────────────────────────────┘

Key Concepts:

1. Lineage (DAG):
   • Spark tracks the sequence of transformations (lineage)
   • Forms a Directed Acyclic Graph (DAG)
   • Enables recomputation of lost partitions
   
2. Lazy Evaluation:
   • Transformations are not executed immediately
   • Spark builds execution plan
   • Optimizes before execution
   
3. Recomputation:
   • Lost data recomputed from source
   • Only affected partitions recomputed (not entire dataset)
   • Automatic and transparent

LINEAGE VISUALIZATION:
----------------------

Example: WordCount Pipeline
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  textFile ──► map ──► flatMap ──► map ──► reduceByKey          │
│     │          │        │          │           │                │
│   (HDFS)    (split)  (words)   (word,1)    (word,count)        │
│                                                                 │
│  Narrow Dependencies: ──►  (No shuffle, partition preserved)   │
│  Wide Dependencies:  ═══►  (Shuffle, all-to-all communication) │
│                                                                 │
│  If partition lost at reduceByKey:                             │
│  • Recompute from previous shuffle boundary                    │
│  • Don't need to re-read from HDFS (shuffle data cached)       │
└─────────────────────────────────────────────────────────────────┘

Transformation Types:

Narrow Transformations (Fast Recovery):
• map, filter, union, mapPartitions
• Each parent partition contributes to at most one child partition
• Lost partition recomputed from single parent partition
• Example: If partition 3 lost, only recompute from parent partition 3

Wide Transformations (Expensive Recovery):
• groupBy, join, reduce, distinct, repartition
• Each parent partition contributes to multiple child partitions
• Lost partition may require recomputing multiple parent partitions
• Example: If partition 3 lost, may need data from ALL parent partitions

PERSISTENCE STRATEGIES:
-----------------------

Storage Levels Comparison:
┌───────────────────────┬────────────┬────────┬──────────┬──────────┐
│ Storage Level         │ Memory     │ Disk   │ Serializ │ Recovery │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ NONE (default)        │ ❌         │ ❌     │ ❌       │ Slowest  │
│ Recompute every time  │            │        │          │          │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ MEMORY_ONLY           │ ✅         │ ❌     │ ❌       │ Fastest  │
│ Deserialized objects  │ Highest    │        │          │ Risky    │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ MEMORY_AND_DISK       │ ✅         │ ✅     │ ❌       │ Fast     │
│ Spill to disk         │ High       │ Backup │          │ Safe     │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ MEMORY_ONLY_SER       │ ✅         │ ❌     │ ✅       │ Medium   │
│ Serialized (compact)  │ Medium     │        │ CPU cost │          │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ MEMORY_AND_DISK_SER   │ ✅         │ ✅     │ ✅       │ Medium   │
│ Serialized + spill    │ Medium     │ Backup │ CPU cost │ Safe     │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ DISK_ONLY             │ ❌         │ ✅     │ ✅       │ Slow     │
│ Only on disk          │            │ All    │          │ Durable  │
├───────────────────────┼────────────┼────────┼──────────┼──────────┤
│ OFF_HEAP              │ Off-heap   │ ❌     │ ✅       │ Fast     │
│ Tachyon/Alluxio       │ (external) │        │          │ Advanced │
└───────────────────────┴────────────┴────────┴──────────┴──────────┘

When to Use Each:

MEMORY_ONLY:
✅ Use when:
• Dataset fits in cluster memory
• Performance is critical
• Interactive queries (notebooks)
❌ Avoid when:
• Dataset larger than memory (eviction thrashing)
• Production jobs (risk of data loss)

MEMORY_AND_DISK (Recommended for Production):
✅ Use when:
• Dataset may exceed memory
• Need reliability
• Production jobs
✅ Benefits:
• Automatic spill to disk
• No data loss
• Good performance

MEMORY_ONLY_SER:
✅ Use when:
• Memory constrained
• Can tolerate serialization overhead
• Java/Scala (Kryo serialization efficient)
❌ Avoid when:
• CPU is bottleneck

DISK_ONLY:
✅ Use when:
• Very large datasets
• Memory extremely limited
• Cost optimization (smaller cluster)

Persistence API:
# cache() is alias for persist(MEMORY_ONLY)
df.cache()

# persist() with explicit level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)

# Unpersist to free resources
df.unpersist()

CHECKPOINTING:
--------------

Why Checkpoint?
┌─────────────────────────────────────────────────────────────────┐
│                   LONG LINEAGE PROBLEM                          │
├─────────────────────────────────────────────────────────────────┤
│  Without Checkpointing:                                         │
│                                                                 │
│  RDD1 → RDD2 → RDD3 → ... → RDD50 → RDD51                      │
│   │                                      │                      │
│   └──────── 50 transformations ─────────┘                      │
│                                                                 │
│  If RDD51 partition lost:                                      │
│  • Must recompute all 50 transformations                       │
│  • If any intermediate step fails, restart from beginning      │
│  • Long recovery time (minutes to hours)                       │
│  • Risk: Cascading failures                                    │
├─────────────────────────────────────────────────────────────────┤
│  With Checkpointing (after RDD25):                             │
│                                                                 │
│  RDD1 → RDD2 → ... → RDD25 → [CHECKPOINT] → RDD26 → ... → RDD51│
│                        │                              │         │
│                   (saved to HDFS)                     │         │
│                                                                 │
│  If RDD51 partition lost:                                      │
│  • Start recovery from checkpoint (RDD25)                      │
│  • Only recompute RDD26-RDD51 (25 steps)                       │
│  • Recovery time cut in half! ✅                               │
│  • Lineage truncated (reduced memory footprint)                │
└─────────────────────────────────────────────────────────────────┘

Checkpointing vs Caching:
┌────────────────────────┬──────────────────┬──────────────────────┐
│ Feature                │ Cache/Persist    │ Checkpoint           │
├────────────────────────┼──────────────────┼──────────────────────┤
│ Storage                │ Executor memory  │ HDFS/S3 (reliable)   │
│                        │ /disk            │                      │
├────────────────────────┼──────────────────┼──────────────────────┤
│ Durability             │ Lost on executor │ Durable              │
│                        │ failure          │                      │
├────────────────────────┼──────────────────┼──────────────────────┤
│ Lineage                │ Preserved        │ Truncated ✅         │
├────────────────────────┼──────────────────┼──────────────────────┤
│ Recovery               │ From lineage     │ From checkpoint file │
├────────────────────────┼──────────────────┼──────────────────────┤
│ Cost                   │ Free (memory)    │ Write to HDFS/S3     │
├────────────────────────┼──────────────────┼──────────────────────┤
│ Use Case               │ Reuse in same job│ Long jobs, iterative │
└────────────────────────┴──────────────────┴──────────────────────┘

Checkpointing API:

# Set checkpoint directory (HDFS or S3)
spark.sparkContext.setCheckpointDir("hdfs:///checkpoint")

# Checkpoint DataFrame (truncates lineage)
df_checkpointed = df.checkpoint()
# OR for lazy checkpoint (doesn't trigger computation)
df_checkpointed = df.checkpoint(eager=False)

# Checkpoint triggers a job (writes to HDFS)
# After checkpoint, lineage is truncated

When to Checkpoint:
✅ After 10-20 transformations
✅ Before iterative algorithms (ML training loops)
✅ After expensive operations (joins, aggregations)
✅ In long-running streaming jobs
✅ When lineage becomes very complex

❌ Don't checkpoint:
• Every transformation (overhead)
• Short pipelines (< 5 transformations)
• When caching is sufficient

RECOVERY STRATEGIES:
--------------------

Strategy 1: Automatic Task Retry
spark.conf.set("spark.task.maxFailures", "4")  # Retry up to 4 times
• Spark automatically retries failed tasks
• Default: 4 attempts
• Exponential backoff

Strategy 2: Stage Retry
spark.conf.set("spark.stage.maxConsecutiveAttempts", "4")
• If all tasks in stage fail, retry entire stage
• Useful for transient failures (network, spot instance)

Strategy 3: Speculation
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")
• Launch duplicate tasks for stragglers
• If one task is 1.5x slower than median, launch backup
• First to complete wins

Strategy 4: Dynamic Executor Allocation
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
• Auto-scale executors based on workload
• Replace failed executors automatically

FAILURE SCENARIOS:
------------------

Scenario 1: Executor Failure
┌─────────────────────────────────────────────────────────────────┐
│  Executor Crashes (OOM, spot instance reclaimed)               │
│                                                                 │
│  Before:                                                        │
│  ┌────────┬────────┬────────┬────────┐                         │
│  │ Exec 1 │ Exec 2 │ Exec 3 │ Exec 4 │                         │
│  │ Part 0 │ Part 1 │ Part 2 │ Part 3 │                         │
│  └────────┴────────┴────────┴────────┘                         │
│                        ↓                                        │
│                     💥 CRASH                                    │
│                                                                 │
│  After (Auto-recovery):                                        │
│  1. Driver detects executor loss                               │
│  2. Reschedules tasks from Exec 3 to other executors          │
│  3. Recompute lost partitions from lineage                     │
│  4. Job continues! ✅                                          │
└─────────────────────────────────────────────────────────────────┘

Scenario 2: Driver Failure (More Serious)
• Client mode: Job fails (driver on user machine)
• Cluster mode: Can recover with checkpoint (driver on cluster)
• Solution: Use cluster mode + checkpointing for production

Scenario 3: Shuffle Data Loss
• Shuffle data stored on executor disk
• If executor fails during shuffle, data lost
• Recovery: Recompute upstream tasks (expensive!)
• Mitigation: Enable shuffle service
  spark.conf.set("spark.shuffle.service.enabled", "true")

BEST PRACTICES:
---------------

✅ Production Checklist:
☐ Use cluster deploy mode (not client)
☐ Enable checkpointing for long jobs
☐ Use MEMORY_AND_DISK persistence (not MEMORY_ONLY)
☐ Enable shuffle service (spark.shuffle.service.enabled)
☐ Enable speculation for heterogeneous clusters
☐ Set appropriate task retry limits
☐ Monitor executor failures in Spark UI
☐ Use reliable storage for checkpoints (HDFS/S3, not local disk)
☐ Clean up checkpoint directories periodically
☐ Test recovery by killing executors in staging

Checkpoint Strategy:
1. Checkpoint after expensive operations:
   df_joined = fact.join(dim, "key")
   df_joined = df_joined.checkpoint()  # Save expensive join result

2. Checkpoint in iterative algorithms:
   for i in range(10):
       df = df.withColumn(f"iter_{i}", ...)
       if i % 3 == 0:
           df = df.checkpoint()  # Truncate lineage every 3 iterations

3. Checkpoint in streaming:
   query = df.writeStream \\
       .option("checkpointLocation", "s3://bucket/checkpoint") \\
       .start()

Cache vs Checkpoint Decision Tree:
┌─────────────────────────────────────────────────────────────────┐
│  START                                                          │
│    │                                                            │
│    ├─ Will reuse data multiple times?                          │
│    │  └─ YES → Use cache() or persist()                        │
│    │                                                            │
│    ├─ Lineage > 10 transformations?                            │
│    │  └─ YES → Use checkpoint() to truncate                    │
│    │                                                            │
│    ├─ Iterative algorithm (ML)?                                │
│    │  └─ YES → Use both (cache + checkpoint every N iters)     │
│    │                                                            │
│    ├─ Streaming job?                                           │
│    │  └─ YES → MUST use checkpoint for fault tolerance         │
│    │                                                            │
│    └─ Otherwise: No caching/checkpointing needed               │
└─────────────────────────────────────────────────────────────────┘

COST-PERFORMANCE TRADE-OFFS:
-----------------------------

Checkpointing Costs:
• Storage: Checkpoint files stored in HDFS/S3 (pay for storage)
• Write latency: Job blocked while writing checkpoint
• Cleanup: Must manually delete old checkpoints

Example Cost Calculation:
Dataset: 100 GB
Checkpoint every hour: 100 GB × 24 = 2.4 TB/day
S3 storage cost: $0.023/GB/month
Monthly cost: 2.4 TB × 30 × $0.023 = ~$1,650/month

Optimization:
• Checkpoint less frequently (trade-off: longer recovery)
• Compress checkpoints (Parquet with snappy)
• Clean up old checkpoints automatically

Recovery Time Comparison:
┌──────────────────────────┬──────────────┬──────────────────────┐
│ Scenario                 │ No Checkpoint│ With Checkpoint      │
├──────────────────────────┼──────────────┼──────────────────────┤
│ 50-step pipeline         │ Recompute 50 │ Recompute 25 steps   │
│ (checkpoint at step 25)  │ steps        │                      │
│                          │ 1 hour       │ 30 min (2x faster)   │
├──────────────────────────┼──────────────┼──────────────────────┤
│ ML training (10 iters)   │ Restart from │ Restart from last    │
│ (checkpoint every 3)     │ iteration 1  │ checkpoint (iter 6-9)│
│ Failure at iteration 9   │ 9 hours lost │ 3 hours lost (3x)    │
└──────────────────────────┴──────────────┴──────────────────────┘

MONITORING:
-----------

Spark UI Metrics:
1. Executors Tab:
   • "Status" column: Active/Dead/Lost
   • "Failed Tasks" column: Identify problematic executors
   • "Blacklisted" column: Spark blacklists faulty nodes

2. Stages Tab:
   • "Shuffle Write" → "Spill": Memory pressure
   • "Task Time": Look for long-running tasks (skew/issues)
   • "Failed Tasks": Transient errors

3. Storage Tab:
   • "Cached Partitions": Check cache effectiveness
   • "Fraction Cached": Should be 100% for cached data

4. Environment Tab:
   • Check checkpoint directory setting
   • Verify fault tolerance configs

Key Metrics to Monitor:
• Task failure rate: Should be < 1%
• Executor lost events: Investigate if frequent
• GC time: High GC = memory pressure
• Checkpoint write time: Should be predictable

TARGET AUDIENCE:
----------------
• Production engineers ensuring job reliability
• Data engineers debugging job failures
• DevOps teams managing Spark clusters
• Anyone running long-running or mission-critical jobs

RELATED RESOURCES:
------------------
• cluster_computing/01_cluster_setup.py (resource configuration)
• cluster_computing/07_resource_management.py
• Spark Configuration: https://spark.apache.org/docs/latest/configuration.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 2.0.0 - Comprehensive Fault Tolerance Guide
UPDATED: 2024
================================================================================
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when


def create_spark(checkpoint_dir=None):
    """Create Spark session with optional checkpoint directory."""
    builder = (
        SparkSession.builder.appName("FaultTolerance")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
    )

    if checkpoint_dir:
        builder = builder.config(
            "spark.sql.streaming.checkpointLocation", checkpoint_dir
        )

    spark = builder.getOrCreate()

    if checkpoint_dir:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        print(f"✅ Checkpoint directory: {checkpoint_dir}")

    return spark


def demonstrate_lineage_basics(spark):
    """Understand RDD lineage and DAG."""
    print("=" * 70)
    print("1. LINEAGE BASICS")
    print("=" * 70)

    # Create transformation chain
    df1 = spark.range(1, 1000001).toDF("id")
    df2 = df1.withColumn("value", col("id") * 2)
    df3 = df2.filter(col("value") > 100)
    df4 = df3.withColumn("squared", col("value") * col("value"))
    df5 = df4.groupBy((col("id") % 10).alias("bucket")).count()

    print("\n📊 Transformation chain:")
    print("   1. spark.range(1000000) - Create DataFrame")
    print("   2. withColumn('value') - Transform")
    print("   3. filter(value > 100) - Filter")
    print("   4. withColumn('squared') - Transform")
    print("   5. groupBy('bucket').count() - Aggregate")

    # Explain plan
    print("\n🔍 Execution plan:")
    df5.explain(extended=False)

    print("\n💡 Lineage Concept:")
    print("   - Spark tracks transformation chain (lineage)")
    print("   - If data is lost, recompute from source")
    print("   - Long lineage = expensive recovery")
    print("   - Checkpointing truncates lineage")


def demonstrate_persistence_levels(spark):
    """Compare different persistence strategies."""
    print("\n" + "=" * 70)
    print("2. PERSISTENCE LEVELS")
    print("=" * 70)

    from pyspark import StorageLevel

    # Create expensive computation
    data = (
        spark.range(1, 5000001)
        .toDF("id")
        .withColumn("value1", rand() * 1000)
        .withColumn("value2", rand() * 1000)
    )

    print("📊 Testing persistence levels:")
    print(f"   Dataset: {data.count():,} rows")

    # No caching
    print("\n❌ No caching (recompute every time):")
    start = time.time()
    data.filter(col("value1") > 500).count()
    time1 = time.time() - start
    data.filter(col("value2") > 500).count()
    time2 = time.time() - start - time1
    print(f"   First query: {time1:.3f}s")
    print(f"   Second query: {time2:.3f}s")
    print(f"   Total: {time1 + time2:.3f}s")

    # Memory only
    print("\n✅ MEMORY_ONLY:")
    data_mem = data.persist(StorageLevel.MEMORY_ONLY)
    data_mem.count()  # Materialize
    start = time.time()
    data_mem.filter(col("value1") > 500).count()
    time3 = time.time() - start
    data_mem.filter(col("value2") > 500).count()
    time4 = time.time() - start - time3
    print(f"   First query: {time3:.3f}s")
    print(f"   Second query: {time4:.3f}s")
    print(f"   Total: {time3 + time4:.3f}s")
    print(f"   Speedup: {(time1 + time2) / (time3 + time4):.2f}x")
    print(f"   ⚠️  Risk: If memory full, partitions evicted")
    data_mem.unpersist()

    # Memory and disk
    print("\n✅ MEMORY_AND_DISK:")
    data_disk = data.persist(StorageLevel.MEMORY_AND_DISK)
    data_disk.count()  # Materialize
    start = time.time()
    data_disk.filter(col("value1") > 500).count()
    time5 = time.time() - start
    data_disk.filter(col("value2") > 500).count()
    time6 = time.time() - start - time5
    print(f"   First query: {time5:.3f}s")
    print(f"   Second query: {time6:.3f}s")
    print(f"   Total: {time5 + time6:.3f}s")
    print(f"   ✅ Safer: Spills to disk if memory full")
    data_disk.unpersist()

    print("\n📊 Persistence Levels:")
    print(
        """
    MEMORY_ONLY          - Fastest, risky if memory full
    MEMORY_AND_DISK      - Safe fallback to disk
    MEMORY_ONLY_SER      - Serialized (saves memory, slower)
    MEMORY_AND_DISK_SER  - Serialized with disk fallback
    DISK_ONLY            - Only disk (slowest)
    OFF_HEAP             - Use off-heap memory (advanced)
    """
    )


def demonstrate_checkpointing(spark):
    """Demonstrate checkpointing to truncate lineage."""
    print("\n" + "=" * 70)
    print("3. CHECKPOINTING (Lineage Truncation)")
    print("=" * 70)

    # Setup checkpoint directory
    checkpoint_dir = "/tmp/spark-checkpoint"
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    print(f"📁 Checkpoint directory: {checkpoint_dir}")

    # Create long lineage
    print("\n📊 Building long lineage (20 transformations):")
    df = spark.range(1, 1000001).toDF("id")

    for i in range(20):
        df = df.withColumn(f"col_{i}", col("id") * (i + 1))
        if i % 5 == 0:
            print(f"   Transformation {i + 1}/20...")

    print("\n❌ Without checkpointing:")
    start = time.time()
    result1 = df.count()
    time1 = time.time() - start
    print(f"   Count: {result1:,}")
    print(f"   Time: {time1:.3f}s")
    print(f"   ⚠️  Long lineage: If failure, recompute all 20 steps")

    # With checkpointing
    print("\n✅ With checkpointing after 10 transformations:")
    df = spark.range(1, 1000001).toDF("id")

    for i in range(10):
        df = df.withColumn(f"col_{i}", col("id") * (i + 1))

    # Checkpoint here (truncates lineage)
    print("   💾 Checkpointing at step 10...")
    df = df.checkpoint()  # Triggers computation and saves
    print("   ✅ Lineage truncated! Starting from checkpoint.")

    for i in range(10, 20):
        df = df.withColumn(f"col_{i}", col("id") * (i + 1))

    start = time.time()
    result2 = df.count()
    time2 = time.time() - start
    print(f"   Count: {result2:,}")
    print(f"   Time: {time2:.3f}s")
    print(f"   ✅ Recovery: Only recompute steps 11-20 (not 1-10)")

    print("\n💡 When to Checkpoint:")
    print("   1. After expensive shuffles/joins")
    print("   2. Before iterative algorithms (ML)")
    print("   3. After 10+ transformations")
    print("   4. When lineage becomes complex")


def demonstrate_fault_recovery(spark):
    """Simulate fault recovery scenarios."""
    print("\n" + "=" * 70)
    print("4. FAULT RECOVERY SIMULATION")
    print("=" * 70)

    print(
        """
📊 Scenario: Processing 1TB dataset with 100 workers

WITHOUT CHECKPOINTING:
----------------------
1. ❌ Worker 50 fails at 80% progress
2. ⚠️  Recompute ALL partitions from source (1TB)
3. ❌ Restart from beginning
4. ⏱️  Total time: 2x original time

WITH CHECKPOINTING (every 25%):
--------------------------------
1. ✅ Worker 50 fails at 80% progress
2. ✅ Last checkpoint at 75%
3. ✅ Recompute only 75% → 80% (250GB)
4. ⏱️  Total time: 1.05x original time (5% overhead)

Recovery Time Comparison:
-------------------------
No Checkpoint:    100% recomputation
Checkpoint 50%:   50% recomputation (avg)
Checkpoint 25%:   25% recomputation (avg)
Checkpoint 10%:   10% recomputation (avg)

⚠️  Trade-off: More checkpoints = more disk I/O overhead
    """
    )

    # Simulate recovery
    print("\n🔄 Simulating recovery:")
    data = spark.range(1, 100001).toDF("id").withColumn("value", rand() * 1000)

    # Stage 1: Load and transform
    print("   Stage 1: Load and transform (25%)")
    stage1 = data.withColumn("transformed", col("value") * 2)

    # Stage 2: Filter and aggregate
    print("   Stage 2: Filter and aggregate (50%)")
    stage2 = (
        stage1.filter(col("transformed") > 100)
        .groupBy((col("id") % 10).alias("bucket"))
        .count()
    )

    # Checkpoint
    print("   💾 Checkpoint at 50% (truncate lineage)")
    stage2_checkpoint = stage2.checkpoint()

    # Stage 3: More transformations
    print("   Stage 3: Additional transformations (75%)")
    stage3 = stage2_checkpoint.withColumn("doubled", col("count") * 2)

    # Stage 4: Final aggregation
    print("   Stage 4: Final aggregation (100%)")
    result = stage3.agg({"doubled": "sum"}).collect()[0][0]

    print(f"\n   ✅ Result: {result:,.0f}")
    print("   ✅ If failure after checkpoint, only recompute stages 3-4")


def demonstrate_storage_strategies(spark):
    """Compare storage strategies for fault tolerance."""
    print("\n" + "=" * 70)
    print("5. STORAGE STRATEGIES")
    print("=" * 70)

    print(
        """
🗄️  Storage Options for Checkpoints:

1. Local Disk (Development):
   ✅ Fast for testing
   ❌ Lost if node fails
   Path: file:///tmp/spark-checkpoint

2. HDFS (Hadoop Clusters):
   ✅ Replicated across nodes (3x default)
   ✅ Survives node failures
   ✅ High throughput
   Path: hdfs://namenode:8020/checkpoint

3. Amazon S3 (Cloud):
   ✅ Durable (11 9's)
   ✅ No cluster dependency
   ⚠️  Higher latency
   Path: s3a://bucket/checkpoint

4. Azure Blob Storage:
   ✅ Durable cloud storage
   ✅ Integrated with Azure
   Path: wasbs://container@account.blob.core.windows.net/checkpoint

5. Google Cloud Storage:
   ✅ Durable cloud storage
   ✅ Integrated with GCP
   Path: gs://bucket/checkpoint

📊 Performance Comparison:

Storage          Write Speed   Read Speed   Durability
--------         -----------   ----------   ----------
Local Disk       Fastest       Fastest      Low (single node)
HDFS             Fast          Fast         High (3x replication)
S3               Medium        Medium       Very High (11 9's)
Azure Blob       Medium        Medium       Very High
GCS              Medium        Medium       Very High

💡 Recommendation:
   - Development: Local disk
   - Production (on-prem): HDFS
   - Production (cloud): S3/Azure/GCS
    """
    )


def demonstrate_best_practices(spark):
    """Best practices for fault tolerance."""
    print("\n" + "=" * 70)
    print("6. FAULT TOLERANCE BEST PRACTICES")
    print("=" * 70)

    print(
        """
🎯 Checkpointing Strategy:

1. ✅ Checkpoint after expensive operations
   - Large shuffles (join, groupBy)
   - Complex aggregations
   - 10+ sequential transformations

2. ✅ Choose right checkpoint frequency
   Checkpoint every:    Recovery cost:
   ------------------   ---------------
   Never                100% recompute
   End only             50% recompute (avg)
   2 checkpoints        33% recompute (avg)
   4 checkpoints        20% recompute (avg)

3. ✅ Use durable storage in production
   # Development
   sc.setCheckpointDir("file:///tmp/checkpoint")
   
   # Production (HDFS)
   sc.setCheckpointDir("hdfs://namenode:8020/checkpoint")
   
   # Production (S3)
   sc.setCheckpointDir("s3a://bucket/checkpoint")

4. ✅ Clean up old checkpoints
   # Streaming: automatic cleanup
   spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
   
   # Batch: manual cleanup with retention
   # Keep last 7 days of checkpoints

5. ✅ Combine caching and checkpointing
   df.cache()       # Fast repeated access
   df.checkpoint()  # Truncate lineage, durable storage

⚠️  Common Mistakes:

1. ❌ Over-checkpointing
   Too frequent = excessive disk I/O overhead
   
2. ❌ No checkpointing for iterative algorithms
   ML training (100+ iterations) needs checkpoints
   
3. ❌ Using local disk in production
   Node failure loses checkpoint data
   
4. ❌ Not cleaning up checkpoints
   Fills up disk over time
   
5. ❌ Checkpointing small datasets
   Overhead > benefit for < 1GB data

📊 Decision Tree:

                    Start
                      |
           Is lineage complex (>10 steps)?
          /                                \\
        No                                 Yes
        |                                   |
   Skip checkpoint              Is data > 1GB?
                              /                \\
                            No                 Yes
                            |                   |
                       Use cache()        Use checkpoint()
                                               |
                                    Which storage?
                                    /      |      \\
                                 Dev    HDFS    Cloud
                                  |       |       |
                              Local   Hadoop   S3/Azure

🔧 Configuration:

# Enable checkpoint cleanup
spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")

# Checkpoint interval for streaming
spark.conf.set("spark.streaming.checkpoint.interval", "10s")

# Reliable checkpointing (write twice for durability)
spark.conf.set("spark.checkpoint.compress", "true")
    """
    )


def main():
    # Create checkpoint directory
    checkpoint_dir = "/tmp/spark-checkpoint-demo"
    os.makedirs(checkpoint_dir, exist_ok=True)

    spark = create_spark(checkpoint_dir)

    print("💾 FAULT TOLERANCE & CHECKPOINTING")
    print("=" * 70)

    demonstrate_lineage_basics(spark)
    demonstrate_persistence_levels(spark)
    demonstrate_checkpointing(spark)
    demonstrate_fault_recovery(spark)
    demonstrate_storage_strategies(spark)
    demonstrate_best_practices(spark)

    print("\n" + "=" * 70)
    print("✅ FAULT TOLERANCE DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Checkpointing truncates lineage for faster recovery")
    print("   2. Checkpoint after expensive shuffles (>10 transformations)")
    print("   3. Use HDFS/S3 for production (not local disk)")
    print("   4. Balance checkpoint frequency vs overhead")
    print("   5. Cache for speed, checkpoint for fault tolerance")
    print("   6. Enable automatic checkpoint cleanup")
    print(f"\n📁 Checkpoint directory: {checkpoint_dir}")

    spark.stop()


if __name__ == "__main__":
    main()
