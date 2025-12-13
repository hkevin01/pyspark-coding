"""
================================================================================
CLUSTER COMPUTING #9 - Cluster Monitoring and Performance Debugging
================================================================================

MODULE OVERVIEW:
----------------
Effective monitoring is essential for production Spark applications. The Spark UI
provides comprehensive metrics for debugging performance issues, identifying
bottlenecks, and optimizing resource utilization.

This module teaches you to master the Spark UI, interpret metrics, and debug
common performance problems in distributed clusters.

PURPOSE:
--------
Master cluster monitoring and debugging:
• Navigate Spark UI tabs and metrics
• Identify performance bottlenecks
• Debug executor failures and stragglers
• Monitor shuffle and memory usage
• Analyze query execution plans
• Set up production monitoring
• Troubleshoot common issues

SPARK UI ARCHITECTURE:
-----------------------

┌─────────────────────────────────────────────────────────────────┐
│                      SPARK UI OVERVIEW                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Access URLs:                                                   │
│  • Local: http://localhost:4040                                │
│  • Driver: http://<driver-ip>:4040                             │
│  • History Server: http://<history-server>:18080               │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ TABS                                                    │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │                                                         │   │
│  │  1. Jobs Tab                                            │   │
│  │     • High-level job overview                           │   │
│  │     • Job duration and status                           │   │
│  │     • Timeline visualization                            │   │
│  │                                                         │   │
│  │  2. Stages Tab                                          │   │
│  │     • Detailed stage metrics                            │   │
│  │     • Task distribution                                 │   │
│  │     • Shuffle read/write                                │   │
│  │     • Input/output sizes                                │   │
│  │                                                         │   │
│  │  3. Storage Tab                                         │   │
│  │     • Cached RDDs/DataFrames                            │   │
│  │     • Memory usage                                      │   │
│  │     • Persistence levels                                │   │
│  │                                                         │   │
│  │  4. Environment Tab                                     │   │
│  │     • Spark configuration                               │   │
│  │     • System properties                                 │   │
│  │     • Classpath entries                                 │   │
│  │                                                         │   │
│  │  5. Executors Tab                                       │   │
│  │     • Executor resource usage                           │   │
│  │     • GC time                                           │   │
│  │     • Shuffle metrics                                   │   │
│  │     • Task failures                                     │   │
│  │                                                         │   │
│  │  6. SQL Tab                                             │   │
│  │     • Query execution plans                             │   │
│  │     • Physical vs logical plans                         │   │
│  │     • Metrics per operation                             │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

JOBS TAB - HIGH-LEVEL OVERVIEW:
--------------------------------

What to Look For:
-----------------
✅ **Job Status**:
   - Succeeded: ✅ Job completed
   - Running: ⏳ In progress
   - Failed: ❌ Check errors

✅ **Duration**:
   - Compare similar jobs
   - Identify slow jobs
   - Track performance trends

✅ **Stages**:
   - Number of stages (fewer is better)
   - Stage dependencies
   - Parallel vs sequential execution

Example Timeline Visualization:
```
Job 0: ████████████████████ (45s)
  Stage 0: ██████ (15s)
  Stage 1: ████████████ (30s)  ← Slow stage!
  
Job 1: ████ (8s)
  Stage 2: ████ (8s)
```

Red Flags:
❌ Jobs taking much longer than expected
❌ High number of stages (> 100)
❌ Stages not running in parallel

STAGES TAB - DETAILED METRICS:
-------------------------------

┌─────────────────────────────────────────────────────────────────┐
│                    STAGE METRICS DASHBOARD                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Stage: Stage 5 (join)                                         │
│  Status: ✅ Succeeded                                           │
│  Duration: 2.3 min                                             │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ TASK METRICS                                            │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ Total Tasks: 200                                        │   │
│  │ Succeeded: 200  Failed: 0  Running: 0                   │   │
│  │ Task Duration: min=2s, median=5s, max=45s ⚠️           │   │
│  │                                                         │   │
│  │ Task Timeline:                                          │   │
│  │ [████████████████████████████████████████]              │   │
│  │ └── Straggler task (45s) ──────────────┘               │   │
│  │     ⚠️  10x slower than median!                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ SHUFFLE METRICS                                         │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ Shuffle Read: 2.5 GB                                    │   │
│  │ Shuffle Write: 1.8 GB                                   │   │
│  │ Shuffle Spill (Memory): 500 MB ⚠️                       │   │
│  │ Shuffle Spill (Disk): 2.1 GB   ❌ BAD!                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ MEMORY METRICS                                          │   │
│  ├─────────────────────────────────────────────────────────┤   │
│  │ Input Size: 3.2 GB                                      │   │
│  │ Output Size: 1.5 GB                                     │   │
│  │ Peak Execution Memory: 6.8 GB                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Key Metrics Explained:
----------------------

1. **Task Duration Distribution**:
   - Min: Fastest task
   - Median: Typical task time
   - Max: Slowest task (straggler)
   
   ⚠️  If max >> median: Data skew or resource contention

2. **Shuffle Metrics**:
   - Shuffle Read: Data read from other executors
   - Shuffle Write: Data written for next stage
   - Spill (Memory): Temp storage in memory (OK)
   - Spill (Disk): Spilled to disk (SLOW! ❌)

3. **Input/Output Size**:
   - Input: Data read by stage
   - Output: Data produced by stage
   - Large input → Consider filtering earlier

EXECUTORS TAB - RESOURCE MONITORING:
-------------------------------------

┌─────────────────────────────────────────────────────────────────┐
│                    EXECUTORS DASHBOARD                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Executor ID | Address        | Status | Memory   | Disk       │
│  ──────────────────────────────────────────────────────────────│
│  driver      | 192.168.1.10   | Active | 2.0 GB   | 0 B        │
│  1           | 192.168.1.11   | Active | 8.0 GB   | 120 GB     │
│  2           | 192.168.1.12   | Active | 7.8 GB   | 118 GB     │
│  3           | 192.168.1.13   | Active | 0.2 GB ⚠️| 15 GB      │
│  4           | 192.168.1.14   | Dead ❌|          |            │
│                                                                 │
│  Per-Executor Metrics:                                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Executor 1                                               │  │
│  │ ──────────────────────────────────────────────────────── │  │
│  │ Tasks: 245 (Success: 245, Failed: 0)                    │  │
│  │ Duration: 2.3 hours                                      │  │
│  │ GC Time: 12 min (8.6% of total) ✅ Good                 │  │
│  │ Input: 45 GB                                             │  │
│  │ Shuffle Read: 12 GB                                      │  │
│  │ Shuffle Write: 8 GB                                      │  │
│  │ Memory Used: 6.5 GB / 8.0 GB (81%)                       │  │
│  │ Disk Used: 120 GB                                        │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Executor 3  ⚠️  UNDERUTILIZED                            │  │
│  │ ──────────────────────────────────────────────────────── │  │
│  │ Tasks: 15 (Success: 15, Failed: 0)                      │  │
│  │ Duration: 10 min                                         │  │
│  │ GC Time: 5 min (50% of total) ❌ BAD!                   │  │
│  │ Input: 2 GB                                              │  │
│  │ Memory Used: 0.2 GB / 8.0 GB (2.5%)                      │  │
│  │                                                           │  │
│  │ Issue: Data skew → executor has very few tasks          │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Red Flags:
----------
❌ **High GC Time** (> 10% of task time):
   - Symptom: Executor spending too much time in garbage collection
   - Cause: Insufficient memory or memory leaks
   - Fix: Increase executor memory, reduce cached data

❌ **Unbalanced Task Distribution**:
   - Symptom: Some executors idle, others overloaded
   - Cause: Data skew or wrong partition count
   - Fix: Repartition data, use salting for skewed keys

❌ **Dead Executors**:
   - Symptom: Executor marked as "Dead"
   - Cause: OOM, network issues, or task timeout
   - Fix: Check logs, increase memory, check network

STORAGE TAB - CACHE MONITORING:
--------------------------------

┌─────────────────────────────────────────────────────────────────┐
│                     CACHED DATAFRAMES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  RDD/DF Name       | Storage Level | Size     | Partitions     │
│  ────────────────────────────────────────────────────────────  │
│  customers         | Memory Only   | 2.5 GB   | 200            │
│  orders_cached     | Memory & Disk | 8.2 GB   | 400            │
│  large_dataset ⚠️  | Memory Only   | 15 GB    | 100 (50% cached)│
│                                                                 │
│  Details: large_dataset                                         │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Storage Level: MEMORY_ONLY                               │  │
│  │ Cached Partitions: 50 / 100 (50%)  ⚠️                   │  │
│  │ Size in Memory: 7.5 GB / 15 GB                           │  │
│  │ Size on Disk: 0 B                                        │  │
│  │                                                           │  │
│  │ Issue: Not enough memory → only 50% cached              │  │
│  │ Solution:                                                │  │
│  │   1. Increase executor memory                            │  │
│  │   2. Use MEMORY_AND_DISK storage                         │  │
│  │   3. Unpersist unused data                               │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Storage Levels:
```python
# MEMORY_ONLY: Fast but limited by memory
df.persist(StorageLevel.MEMORY_ONLY)

# MEMORY_AND_DISK: Fallback to disk if OOM
df.persist(StorageLevel.MEMORY_AND_DISK)

# MEMORY_ONLY_SER: Serialized (more compact, slower access)
df.persist(StorageLevel.MEMORY_ONLY_SER)

# OFF_HEAP: Use off-heap memory (no GC overhead)
df.persist(StorageLevel.OFF_HEAP)
```

SQL TAB - QUERY ANALYSIS:
--------------------------

Most important tab for DataFrame/SQL optimization!

What to Look For:
-----------------

1. **Query Execution Time**:
   - Duration per query
   - Compare similar queries
   - Identify slow queries

2. **Physical Plan**:
   - Operations performed
   - Look for "Exchange" (= shuffle)
   - Check join strategies (BroadcastHashJoin vs SortMergeJoin)

3. **Metrics per Operation**:
   - Number of rows
   - Data size
   - Time spent in each operation

Example:
```
Query: SELECT category, SUM(sales) FROM orders GROUP BY category

Execution Plan:
┌────────────────────────────────────────────────────────────┐
│ HashAggregate (final)                                      │
│ • Output: [category, sum(sales)]                           │
│ • Time: 500 ms                                             │
│ • Rows: 10                                                 │
└──────────────────┬─────────────────────────────────────────┘
                   │
┌──────────────────▼─────────────────────────────────────────┐
│ Exchange (Shuffle)  ⚠️                                     │
│ • Shuffle Read: 2.5 GB                                     │
│ • Time: 8 seconds  ← BOTTLENECK!                           │
└──────────────────┬─────────────────────────────────────────┘
                   │
┌──────────────────▼─────────────────────────────────────────┐
│ HashAggregate (partial)                                    │
│ • Time: 1 second                                           │
│ • Rows: 10,000,000                                         │
└──────────────────┬─────────────────────────────────────────┘
                   │
┌──────────────────▼─────────────────────────────────────────┐
│ Scan Parquet                                               │
│ • Time: 2 seconds                                          │
│ • Rows: 10,000,000                                         │
└────────────────────────────────────────────────────────────┘
```

Analysis: Shuffle is the bottleneck (8 seconds out of 11.5 total)

PERFORMANCE DEBUGGING WORKFLOW:
--------------------------------

Step 1: Identify Slow Job
```
Jobs Tab → Find job with long duration
```

Step 2: Find Slow Stage
```
Stages Tab → Look at stage durations
→ Identify stage taking most time
```

Step 3: Analyze Stage Metrics
```
Click on slow stage → Check:
• Task duration distribution (stragglers?)
• Shuffle metrics (spill to disk?)
• Input/output sizes (too much data?)
```

Step 4: Check Executors
```
Executors Tab → Look for:
• High GC time (> 10%)
• Unbalanced task distribution
• Dead executors
```

Step 5: Examine Query Plan
```
SQL Tab → Check:
• Exchange operations (shuffles)
• Join strategies
• Filter pushdown
```

Step 6: Fix and Re-run
```
Apply optimization → Monitor improvement
```

COMMON PERFORMANCE ISSUES:
---------------------------

1. **Data Skew** (Unbalanced Partitions):

Symptoms:
- One task takes 10-100x longer than others
- One executor has most of the data
- High memory usage on one executor

Detection:
```
Stages Tab → Task metrics:
Min: 2s, Median: 3s, Max: 300s  ← Skew!
```

Solutions:
```python
# Add salt to skewed key
from pyspark.sql.functions import rand, floor
df = df.withColumn("salt", (floor(rand() * 10)).cast("int"))
df = df.repartition("key", "salt")

# Or use AQE (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

2. **Excessive Shuffles**:

Symptoms:
- Long shuffle read/write times
- High network usage
- Disk spill

Detection:
```
SQL Tab → Look for many "Exchange" operations
Stages Tab → High shuffle read/write sizes
```

Solutions:
```python
# Use broadcast joins
result = large_df.join(broadcast(small_df), "key")

# Filter early
df = df.filter(col("date") > "2024-01-01").join(...)

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

3. **Memory Issues**:

Symptoms:
- Executor OOM errors
- High GC time (> 10%)
- Disk spill (memory → disk)

Detection:
```
Executors Tab → GC Time > 10%
Stages Tab → Spill (Disk) > 0
```

Solutions:
```python
# Increase executor memory
--executor-memory 16g  # was 8g

# Reduce cached data
df.unpersist()

# Use off-heap memory
--conf spark.memory.offHeap.enabled=true
--conf spark.memory.offHeap.size=4g
```

4. **Too Many Small Files**:

Symptoms:
- Many tasks (> 10,000)
- High task scheduling overhead
- Slow reads

Detection:
```
Stages Tab → Total Tasks: 50,000  ← Too many!
Input: 500 MB across 50,000 files
```

Solutions:
```python
# Coalesce before writing
df.coalesce(100).write.parquet("output")

# Or repartition
df.repartition(200).write.parquet("output")
```

MONITORING IN PRODUCTION:
--------------------------

1. **Enable Event Logs**:
```python
spark = SparkSession.builder \\
    .config("spark.eventLog.enabled", "true") \\
    .config("spark.eventLog.dir", "hdfs:///spark-logs") \\
    .getOrCreate()
```

2. **Set Up History Server**:
```bash
# Start Spark History Server
$SPARK_HOME/sbin/start-history-server.sh

# Access: http://localhost:18080
```

3. **Configure Metrics**:
```python
spark.conf.set("spark.metrics.namespace", "myapp")
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

4. **Monitoring Tools**:
- Ganglia: Cluster-wide metrics
- Prometheus + Grafana: Custom dashboards
- DataDog: APM integration
- AWS CloudWatch: For EMR clusters

KEY METRICS TO TRACK:
----------------------

✅ **Job-Level**:
   - Job duration (target: < 5 min)
   - Success rate (target: > 99%)
   - Jobs per hour (throughput)

✅ **Stage-Level**:
   - Shuffle read/write sizes
   - Spill to disk (target: 0)
   - Task duration distribution

✅ **Executor-Level**:
   - GC time % (target: < 10%)
   - Memory utilization (target: 70-85%)
   - Task failures (target: 0)

✅ **Query-Level**:
   - Query execution time
   - Number of shuffles
   - Data scanned vs returned

SPARK UI ACCESS:
----------------

Local Mode:
```bash
# Default: http://localhost:4040
# If port taken: http://localhost:4041, 4042, etc.
```

Cluster Mode (YARN):
```bash
# While running:
yarn application -status <app_id>
# → Get Tracking URL

# After completion:
# History Server: http://<history-server>:18080
```

Kubernetes:
```bash
# Port-forward to driver pod
kubectl port-forward <driver-pod> 4040:4040

# Access: http://localhost:4040
```

EXPLAIN() FOR MONITORING:
--------------------------

Always use explain() to preview query plan:

```python
# Simple explain
df.groupBy("category").count().explain()

# Extended explain (all plans)
df.groupBy("category").count().explain(mode="extended")

# Cost-based explain (statistics)
df.groupBy("category").count().explain(mode="cost")

# Formatted explain (Spark 3.0+)
df.groupBy("category").count().explain(mode="formatted")
```

Look for:
• Number of stages
• Exchange operations (shuffles)
• Join strategies
• Estimated data sizes

PRODUCTION CHECKLIST:
---------------------

✅ Monitoring Setup:
   - Event logs enabled
   - History server running
   - Alerts configured
   - Metrics collection

✅ Regular Checks:
   - Review slow queries daily
   - Monitor executor failures
   - Track memory usage trends
   - Analyze shuffle patterns

✅ Optimization:
   - Enable AQE (Spark 3.0+)
   - Configure appropriate partitions
   - Use broadcast joins
   - Cache wisely

✅ Alerting:
   - Job failures
   - Executor OOM
   - High GC time
   - Abnormal duration

See Also:
---------
• 08_shuffle_optimization.py - Reduce shuffles
• 07_resource_management.py - Memory tuning
• 05_fault_tolerance.py - Handle failures
• ../spark_execution_architecture/ - Execution internals
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand


def create_spark():
    return (
        SparkSession.builder.appName("ClusterMonitoring")
        .master("local[4]")
        .config("spark.ui.port", "4040")
        .config("spark.eventLog.enabled", "true")
        .getOrCreate()
    )


def demonstrate_spark_ui_overview():
    """Overview of Spark UI tabs."""
    print("=" * 70)
    print("1. SPARK UI OVERVIEW")
    print("=" * 70)

    print(
        """
🖥️  Spark UI: Your monitoring dashboard

Access:
-------
Local: http://localhost:4040
Driver: http://<driver-ip>:4040
History Server: http://<history-server>:18080

🗂️  Main Tabs:

1. **Jobs Tab**
   - Overview of all Spark jobs
   - Success/failure status
   - Duration of each job
   - Stages per job
   
   Use for: High-level job monitoring

2. **Stages Tab** ⭐ MOST IMPORTANT
   - Detailed stage metrics
   - Task distribution
   - Shuffle read/write
   - Spill to disk
   - Data skew detection
   
   Use for: Performance debugging

3. **Storage Tab**
   - Cached RDDs/DataFrames
   - Memory usage per partition
   - Storage levels (MEMORY_ONLY, DISK, etc.)
   
   Use for: Memory management

4. **Environment Tab**
   - Spark configuration
   - System properties
   - Classpath entries
   
   Use for: Configuration verification

5. **Executors Tab**
   - Executor memory usage
   - Task distribution
   - Shuffle read/write per executor
   - GC time
   
   Use for: Resource utilization

6. **SQL Tab**
   - DataFrame/SQL query plans
   - Physical vs logical plans
   - DAG visualization
   
   Use for: Query optimization

7. **Streaming Tab** (if using Structured Streaming)
   - Batch processing time
   - Input rate
   - Processing rate
   
   Use for: Streaming performance
    """
    )


def demonstrate_stages_tab_metrics(spark):
    """Key metrics in Stages tab."""
    print("\n" + "=" * 70)
    print("2. STAGES TAB METRICS")
    print("=" * 70)

    print(
        """
📊 Key Metrics to Monitor:

1. **Duration**
   Total time for stage
   Look for: Stages taking much longer than others

2. **Input Size / Records**
   Data read into stage
   Look for: Very large input causing slowdowns

3. **Shuffle Read / Write**
   Data moved between nodes
   Look for: Large shuffle sizes (> 1 GB per task)

4. **Spill (Memory / Disk)**
   Data that didn't fit in memory
   Look for: Any spill indicates memory pressure
   
   ⚠️  Spill to Memory: Moderate issue
   ❌ Spill to Disk: Serious performance problem

5. **Task Time Distribution**
   Min, 25%, Median, 75%, Max task times
   Look for: Wide spread = data skew
   
   Example:
   Min: 1s, Median: 2s, Max: 60s ← SKEW!

6. **GC Time**
   Time spent in garbage collection
   Look for: > 10% of task time = memory issues


🔍 Reading the Stages Table:

Stage ID | Description      | Duration | Input   | Shuffle R | Shuffle W | Spill
---------|------------------|----------|---------|-----------|-----------|-------
0        | map              | 2s       | 1 GB    | 0 B       | 500 MB    | 0 B
1        | reduceByKey      | 10s      | 0 B     | 500 MB    | 100 MB    | 2 GB ⚠️
2        | collect          | 1s       | 100 MB  | 0 B       | 0 B       | 0 B

Analysis:
---------
✅ Stage 0: Clean (no spill)
❌ Stage 1: 2 GB spill to disk! Increase memory or partitions
✅ Stage 2: Clean
    """
    )

    # Generate some data to monitor
    print("\n📊 Generating sample workload to monitor...")
    data = spark.range(1, 5000001).toDF("id").withColumn("value", rand() * 1000)

    result = data.groupBy((col("id") % 100).alias("bucket")).count()
    result.collect()

    print("\n✅ Check Spark UI at http://localhost:4040")
    print("   → Go to 'Stages' tab")
    print("   → Click on latest stage")
    print("   → Review metrics: Duration, Shuffle, Spill")


def demonstrate_task_metrics():
    """Understanding task-level metrics."""
    print("\n" + "=" * 70)
    print("3. TASK-LEVEL METRICS")
    print("=" * 70)

    print(
        """
🔬 Task Details (Click on stage → Task table):

Columns to Watch:
-----------------
1. **Task ID**
   Unique identifier for each task

2. **Index**
   Partition number being processed

3. **Status**
   SUCCESS, RUNNING, FAILED
   Look for: Failed tasks

4. **Duration**
   Total time for task
   Look for: Tasks taking 10x longer than median

5. **GC Time**
   Garbage collection time
   Look for: > 10% of duration

6. **Shuffle Read / Write**
   Data read/written per task
   Look for: Imbalanced sizes across tasks

7. **Spill (Memory / Disk)**
   Per-task spill
   Look for: Any spill

8. **Executor ID / Host**
   Which executor ran the task
   Look for: All tasks on same executor = poor distribution


🎯 Identifying Data Skew:

Task Durations:
---------------
Task 0:  2s  |■■
Task 1:  2s  |■■
Task 2:  2s  |■■
Task 3:  45s |■■■■■■■■■■■■■■■■■■■■■  ← SKEWED PARTITION!
Task 4:  2s  |■■
Task 5:  2s  |■■

Diagnosis: Partition 3 has 20x more data
Solution: Use salting or repartition


📊 Shuffle Metrics:

Good Distribution:
-----------------
Task | Shuffle Read
-----|-------------
0    | 100 MB
1    | 105 MB
2    | 98 MB
3    | 102 MB
✅ Balanced!

Bad Distribution (Skew):
------------------------
Task | Shuffle Read
-----|-------------
0    | 50 MB
1    | 60 MB
2    | 2 GB  ← 90% of data in one partition!
3    | 55 MB
❌ Skewed! Use salting technique
    """
    )


def demonstrate_executor_metrics(spark):
    """Monitor executor utilization."""
    print("\n" + "=" * 70)
    print("4. EXECUTOR METRICS")
    print("=" * 70)

    print(
        """
🖥️  Executors Tab Metrics:

Key Columns:
------------
1. **Executor ID**
   Unique identifier (0 = driver)

2. **Address**
   Host:port where executor runs

3. **Status**
   Active, Dead, Lost
   Look for: Dead executors

4. **RDD Blocks**
   Number of cached RDD partitions

5. **Storage Memory**
   Memory used for cached data
   Look for: Nearing storage memory limit

6. **Disk Used**
   Disk space for spilled data
   Look for: Large disk usage

7. **Cores**
   CPU cores allocated

8. **Active Tasks / Total Tasks**
   Current and historical task counts
   Look for: Imbalanced task distribution

9. **Failed Tasks**
   Number of failed tasks
   Look for: > 0 (indicates issues)

10. **GC Time / Duration**
    GC time as % of execution time
    Look for: > 10%

11. **Shuffle Read / Write**
    Total shuffle data per executor
    Look for: Imbalanced shuffles


📊 Example Executor Dashboard:

Executor | Cores | Memory | Tasks | Failed | GC Time | Shuffle R | Shuffle W
---------|-------|--------|-------|--------|---------|-----------|----------
0        | 4     | 4 GB   | 120   | 0      | 5%      | 2 GB      | 1 GB
1        | 4     | 4 GB   | 115   | 0      | 6%      | 1.9 GB    | 950 MB
2        | 4     | 4 GB   | 10    | 5 ❌   | 45% ❌  | 200 MB    | 100 MB
3        | 4     | 4 GB   | 118   | 0      | 5%      | 2.1 GB    | 1.1 GB

Analysis:
---------
❌ Executor 2 problems:
   - Only 10 tasks (underutilized)
   - 5 failed tasks
   - 45% GC time (memory pressure)
   
Action: Check executor logs, may need restart or more memory
    """
    )


def demonstrate_sql_query_plan(spark):
    """Understand SQL query plans."""
    print("\n" + "=" * 70)
    print("5. SQL QUERY PLANS")
    print("=" * 70)

    print(
        """
🗺️  Query Plan Visualization:

SQL Tab shows:
--------------
1. **Logical Plan**
   High-level operations (filter, join, aggregation)

2. **Optimized Logical Plan**
   After Catalyst optimizer

3. **Physical Plan**
   Actual execution (Exchange = shuffle)

4. **DAG Visualization**
   Visual graph of stages


🔍 Reading Physical Plan:

Key Operations:
---------------
- **FileScan**: Read from source
- **Filter**: Filter rows (no shuffle)
- **Project**: Select columns (no shuffle)
- **Exchange**: SHUFFLE POINT! ⚠️
- **HashAggregate**: Group by aggregation
- **SortMergeJoin**: Join with sort
- **BroadcastHashJoin**: Broadcast join ✅


Example Plan:
-------------
== Physical Plan ==
*(3) HashAggregate(keys=[category], functions=[count(1)])
+- Exchange hashpartitioning(category, 200) ← SHUFFLE!
   +- *(2) HashAggregate(keys=[category], functions=[partial_count(1)])
      +- *(2) Project [category]
         +- *(2) Filter (value > 100)
            +- *(1) FileScan parquet [value, category]

Reading the plan:
-----------------
1. FileScan: Read parquet
2. Filter: Filter value > 100 (no shuffle)
3. Partial aggregation (map side)
4. Exchange: SHUFFLE on category key
5. Final aggregation (reduce side)


🎯 Optimization Opportunities:

Before:
-------
Exchange hashpartitioning (200 partitions) ← Expensive!

After tuning:
-------------
Exchange hashpartitioning (50 partitions) ← Better!

Or even better:
---------------
BroadcastHashJoin ← No shuffle at all!
    """
    )

    # Demonstrate explain
    print("\n💻 Example: Using .explain()")
    data = (
        spark.range(10000)
        .toDF("id")
        .withColumn("category", (col("id") % 10).cast("int"))
    )

    result = data.filter(col("id") > 5000).groupBy("category").count()

    print("\nPhysical Plan:")
    result.explain()


def demonstrate_monitoring_best_practices():
    """Best practices for cluster monitoring."""
    print("\n" + "=" * 70)
    print("6. MONITORING BEST PRACTICES")
    print("=" * 70)

    print(
        """
🎯 Monitoring Checklist:

Daily Checks:
-------------
1. ✅ Job success rate
   Target: > 99%

2. ✅ Average job duration
   Look for: Increasing trends

3. ✅ Executor utilization
   Target: > 80% task distribution

4. ✅ Memory usage
   Target: < 90% to avoid spills

5. ✅ GC time percentage
   Target: < 10% of execution time


Per-Job Analysis:
-----------------
1. ✅ Check Stages tab for slowest stage
2. ✅ Identify shuffle sizes (should be < 1 GB/task)
3. ✅ Look for spill to disk (should be 0)
4. ✅ Check task time distribution for skew
5. ✅ Verify all executors are being used


🚨 Red Flags:

1. ❌ Spill to disk > 0
   → Increase executor memory or partitions

2. ❌ Task time max >> median
   → Data skew, use salting

3. ❌ GC time > 10%
   → Memory pressure, increase executor memory

4. ❌ Shuffle read > 10 GB per partition
   → Increase spark.sql.shuffle.partitions

5. ❌ Failed tasks > 0
   → Check executor logs

6. ❌ One executor with most tasks
   → Poor data distribution

7. ❌ Long shuffle read time
   → Network or disk bottleneck


📊 Performance Metrics to Track:

Metric                    Good      Warning   Critical
------                    ----      -------   --------
GC Time %                 < 5%      5-10%     > 10%
Spill to Memory          0         < 1 GB    > 1 GB
Spill to Disk            0         > 0       > 5 GB
Task time variance       < 2x      2-5x      > 5x
Shuffle per partition    < 1 GB    1-5 GB    > 5 GB
Executor utilization     > 80%     60-80%    < 60%


🔧 Debugging Workflow:

1. Job is slow → Check Stages tab
2. Find slowest stage → Click stage ID
3. Check metrics:
   - Large shuffle? → Use broadcast or pre-partition
   - Spill to disk? → Increase memory or partitions
   - Task skew? → Use salting technique
   - High GC time? → Increase executor memory
4. Apply fix and re-run
5. Compare metrics before/after


💡 Pro Tips:

1. Enable event log for history
   spark.conf.set("spark.eventLog.enabled", "true")
   spark.conf.set("spark.eventLog.dir", "hdfs://logs")

2. Use Spark History Server for past jobs
   spark-history-server.sh start

3. Export metrics to monitoring systems
   - Prometheus + Grafana
   - Datadog
   - CloudWatch (AWS)

4. Set up alerts
   - Job failure alerts
   - Long-running job alerts
   - Resource usage alerts

5. Regular log review
   - Executor logs for errors
   - Driver logs for application issues


📈 Grafana Dashboard Metrics:

Panel 1: Job Success Rate
- Line chart of % successful jobs

Panel 2: Average Job Duration
- Track trends over time

Panel 3: Executor Memory Usage
- Gauge showing % used

Panel 4: Active Executors
- Count of active vs total

Panel 5: Shuffle Read/Write
- Bar chart per job

Panel 6: GC Time %
- Line chart with 10% threshold


🔍 Log Analysis Commands:

# Find OOM errors
grep "OutOfMemoryError" executor-*.log

# Find failed tasks
grep "Task.*FAILED" executor-*.log

# Check GC logs
grep "GC" executor-*.log | tail -100

# Find slow tasks
grep "Task.*took" executor-*.log | sort -k4 -n | tail -10

# Check shuffle errors
grep "shuffle" executor-*.log | grep -i error
    """
    )


def demonstrate_common_issues():
    """Common issues and solutions."""
    print("\n" + "=" * 70)
    print("7. COMMON ISSUES & SOLUTIONS")
    print("=" * 70)

    print(
        """
🐛 Issue 1: Job Stuck / Very Slow

Symptoms:
---------
- Job runs for hours
- Progress bar stuck at same %
- Few active tasks

Diagnosis:
----------
1. Check Stages tab → Look for long-running stage
2. Check task distribution → All tasks on 1-2 executors?
3. Check shuffle sizes → > 10 GB per partition?

Solutions:
----------
✅ Increase spark.sql.shuffle.partitions
✅ Check for data skew (use salting)
✅ Add more executors


🐛 Issue 2: OutOfMemoryError

Symptoms:
---------
- Job fails with OOM
- Executor lost
- Container killed by YARN

Diagnosis:
----------
1. Check Stages tab → Spill to disk?
2. Check Executors tab → Memory usage near limit?
3. Check task size → Processing huge partition?

Solutions:
----------
✅ Increase executor memory: --executor-memory 16g
✅ Increase memory overhead: --executor-memoryOverhead 4g
✅ Increase partitions to reduce per-task data
✅ Don't cache too much data
✅ Use broadcast for small tables


🐛 Issue 3: Data Skew

Symptoms:
---------
- Most tasks finish quickly
- 1-2 tasks take 10x longer
- Median task time: 2s, Max: 60s

Diagnosis:
----------
1. Check Stages tab → Task time distribution
2. Click stage → Look at task table
3. Identify which partition is large

Solutions:
----------
✅ Use salting technique for skewed keys
✅ Broadcast join if one side is small
✅ Increase partitions


🐛 Issue 4: Shuffle Performance

Symptoms:
---------
- Long shuffle read/write times
- Large shuffle sizes (> 1 GB/task)

Diagnosis:
----------
1. Check Stages tab → Shuffle Read/Write columns
2. Check if shuffle is necessary

Solutions:
----------
✅ Filter before shuffle
✅ Use broadcast join for small tables
✅ Pre-partition on join key
✅ Increase spark.sql.shuffle.partitions
✅ Enable compression (default on)


🐛 Issue 5: Executor Lost

Symptoms:
---------
- Executor becomes unresponsive
- Tasks fail and retry
- "Executor lost" in logs

Diagnosis:
----------
1. Check executor logs
2. Look for OOM, killed by YARN, network timeout

Solutions:
----------
✅ Increase executor memory
✅ Increase network timeout: spark.network.timeout=800s
✅ Check node health (disk, memory, network)
✅ Increase heartbeat: spark.executor.heartbeatInterval=30s


🐛 Issue 6: High GC Time

Symptoms:
---------
- GC time > 10% of task time
- Slow task execution
- Frequent GC pauses

Diagnosis:
----------
1. Check Executors tab → GC Time column
2. Check memory usage near limit

Solutions:
----------
✅ Increase executor memory
✅ Reduce cached data
✅ Use off-heap memory
✅ Tune GC settings (G1GC recommended)
✅ Increase partitions to reduce per-task memory
    """
    )


def main():
    spark = create_spark()

    print("📊 CLUSTER MONITORING")
    print("=" * 70)
    print("\nMaster Spark UI and performance debugging!")
    print()

    demonstrate_spark_ui_overview()
    demonstrate_stages_tab_metrics(spark)
    demonstrate_task_metrics()
    demonstrate_executor_metrics(spark)
    demonstrate_sql_query_plan(spark)
    demonstrate_monitoring_best_practices()
    demonstrate_common_issues()

    print("\n" + "=" * 70)
    print("✅ CLUSTER MONITORING DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Always check Stages tab first for bottlenecks")
    print("   2. Look for: Spill, Skew, Large shuffles")
    print("   3. Monitor: GC time < 10%, No spill to disk")
    print("   4. Use .explain() to understand query plans")
    print("   5. Enable event logs for history analysis")
    print("   6. Set up alerts for failures and slow jobs")
    print("   7. Regular log review prevents issues")
    print("\n🖥️  Access Spark UI: http://localhost:4040")

    spark.stop()


if __name__ == "__main__":
    main()
