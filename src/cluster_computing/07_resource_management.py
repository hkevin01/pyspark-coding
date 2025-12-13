"""
================================================================================
CLUSTER COMPUTING #7 - Resource Management and Dynamic Allocation
================================================================================

MODULE OVERVIEW:
----------------
Resource management is critical for cluster efficiency and cost optimization.
Proper configuration of memory, CPU cores, and dynamic allocation ensures
maximum performance while minimizing waste.

This module covers memory hierarchy, executor sizing, dynamic allocation,
and troubleshooting resource-related issues in production clusters.

PURPOSE:
--------
Master cluster resource management:
• Spark memory hierarchy (execution vs storage)
• Executor sizing formulas and best practices
• Driver memory configuration
• Dynamic allocation for auto-scaling
• Memory overhead and off-heap memory
• Troubleshooting OOM errors and spills

SPARK RESOURCE ARCHITECTURE:
-----------------------------

┌─────────────────────────────────────────────────────────────────┐
│                 CLUSTER RESOURCE TOPOLOGY                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Driver (Coordinator)                                          │
│  ┌────────────────────────────────────────────────────────┐        │
│  │ Memory: spark.driver.memory (2-8 GB typical)      │        │
│  │ Cores: spark.driver.cores (usually 1-2)           │        │
│  │                                                    │        │
│  │ Responsibilities:                                  │        │
│  │ • Job scheduling                                   │        │
│  │ • DAG compilation                                  │        │
│  │ • Result aggregation (collect, show)              │        │
│  │ • Broadcast variable creation                      │        │
│  └────────────────────────────────────────────────────────┘        │
│                         ↓                                       │
│                  Task Distribution                              │
│                         ↓                                       │
│  ┌──────────────┬──────────────┬──────────────┐               │
│  │ Executor 1   │ Executor 2   │ Executor N   │               │
│  │ ┌──────────┐ │ ┌──────────┐ │ ┌──────────┐ │               │
│  │ │ Memory   │ │ │ Memory   │ │ │ Memory   │ │               │
│  │ │ 8 GB     │ │ │ 8 GB     │ │ │ 8 GB     │ │               │
│  │ │ Cores: 4 │ │ │ Cores: 4 │ │ │ Cores: 4 │ │               │
│  │ └──────────┘ │ └──────────┘ │ └──────────┘ │               │
│  │ Tasks: 1-4   │ Tasks: 1-4   │ Tasks: 1-4   │               │
│  └──────────────┴──────────────┴──────────────┘               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

MEMORY HIERARCHY (DETAILED):
-----------------------------

┌─────────────────────────────────────────────────────────────────┐
│        EXECUTOR MEMORY LAYOUT (Example: 8 GB)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  JVM HEAP MEMORY (spark.executor.memory = 8 GB)         │ │
│  │                                                           │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │ UNIFIED MEMORY (spark.memory.fraction = 0.6)       │ │ │
│  │  │ = 4.8 GB (60% of 8 GB)                             │ │ │
│  │  │                                                     │ │ │
│  │  │ ┌──────────────┬──────────────────────────────────┐  │ │ │
│  │  │ │ Execution    │ Storage (Cache)                │  │ │ │
│  │  │ │ (Shuffles,   │ (DataFrame.cache(),            │  │ │ │
│  │  │ │  Joins,      │  RDD.persist())                │  │ │ │
│  │  │ │  Sorts,      │                                │  │ │ │
│  │  │ │  Aggregates) │ Can borrow from execution      │  │ │ │
│  │  │ │              │ when needed!                   │  │ │ │
│  │  │ │ 2.4 GB (50%) │ 2.4 GB (50%)                   │  │ │ │
│  │  │ └──────────────┴──────────────────────────────────┘  │ │ │
│  │  │                                                     │ │ │
│  │  │ ⚠️  Eviction: Storage can evict cached data to    │ │ │
│  │  │    make room for execution if needed              │ │ │
│  │  └─────────────────────────────────────────────────────┘ │ │
│  │                                                           │ │
│  │  ┌─────────────────────────────────────────────────────┐ │ │
│  │  │ USER MEMORY (1 - fraction = 0.4)                   │ │ │
│  │  │ = 3.2 GB (40% of 8 GB)                             │ │ │
│  │  │                                                     │ │ │
│  │  │ • User data structures                              │ │ │
│  │  │ • Spark internal metadata                           │ │ │
│  │  │ • Safeguard against OOM                            │ │ │
│  │  │ • Reserved (300 MB minimum)                         │ │ │
│  │  └─────────────────────────────────────────────────────┘ │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  OFF-HEAP MEMORY (Optional)                              │ │
│  │  spark.memory.offHeap.enabled = true                    │ │
│  │  spark.memory.offHeap.size = 2 GB                       │ │
│  │                                                           │ │
│  │  Benefits:                                                │ │
│  │  • Avoids JVM garbage collection overhead               │ │
│  │  • Better for large cached datasets                     │ │
│  │  • More predictable performance                          │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  MEMORY OVERHEAD (Off-heap, separate from JVM)          │ │
│  │  spark.executor.memoryOverhead                           │ │
│  │  = max(384 MB, 0.1 × executor.memory)                   │ │
│  │  = max(384 MB, 819 MB) = 819 MB                         │ │
│  │                                                           │ │
│  │  Used for:                                                │ │
│  │  • JVM overhead                                          │ │
│  │  • Native libraries                                      │ │
│  │  • Interprocess communication                            │ │
│  │  • Off-heap data structures                              │ │
│  └───────────────────────────────────────────────────────────┘ │
│                                                                 │
│  TOTAL CONTAINER MEMORY:                                       │
│  = executor.memory + memoryOverhead                            │
│  = 8 GB + 819 MB = 8.8 GB                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

EXECUTOR SIZING FORMULA:
------------------------

Given Cluster Resources:
• 10 worker nodes
• 16 cores per node  
• 64 GB RAM per node
• Total: 160 cores, 640 GB RAM

Step-by-Step Sizing:
--------------------

1. **Reserve Resources for OS/Daemons**:
   - Reserve 1 core per node
   - Reserve ~10% RAM per node
   
   Available per node:
   - Cores: 16 - 1 = 15 cores
   - RAM: 64 GB - 6.4 GB = 57.6 GB

2. **Choose Executor Cores** (Rule: 4-5 cores optimal):
   - Too few cores: Underutilized resources
   - Too many cores (> 5): HDFS throughput bottleneck
   - Sweet spot: 4-5 cores per executor
   
   Let's use: **5 cores per executor**

3. **Calculate Executors per Node**:
   Executors per node = floor(15 cores / 5 cores) = **3 executors**

4. **Calculate Executor Memory**:
   Memory per executor = floor(57.6 GB / 3 executors) = **19 GB**
   
   Accounting for overhead (10%):
   - executor.memory = 19 GB / 1.1 = **17 GB**
   - memoryOverhead = 19 GB - 17 GB = **2 GB**

5. **Total Cluster Configuration**:
   ```
   --num-executors 30              (10 nodes × 3 executors - 1 for driver)
   --executor-cores 5
   --executor-memory 17g
   --conf spark.executor.memoryOverhead=2g
   --driver-memory 8g
   --driver-cores 2
   ```

Alternative Configurations:
---------------------------

**Small Executors (More Parallelism)**:
```bash
# Good for: Many small tasks, high parallelism
--num-executors 59          # 10 nodes × 6 executors - 1
--executor-cores 2
--executor-memory 8g
--conf spark.executor.memoryOverhead=1g
```

**Large Executors (Memory-Intensive)**:
```bash
# Good for: Large shuffles, caching, aggregations
--num-executors 9           # 10 nodes × 1 executor - 1
--executor-cores 15
--executor-memory 52g
--conf spark.executor.memoryOverhead=6g
```

**Balanced (Recommended)**:
```bash
# Best for: Most workloads
--num-executors 29          # 10 nodes × 3 executors - 1
--executor-cores 5
--executor-memory 17g
--conf spark.executor.memoryOverhead=2g
```

DYNAMIC ALLOCATION:
-------------------

Automatically scale executors based on workload!

┌─────────────────────────────────────────────────────────────────┐
│               DYNAMIC ALLOCATION LIFECYCLE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Job Start:                                                    │
│  ┌─────┐                                                       │
│  │ 2   │ Initial executors                                    │
│  └─────┘                                                       │
│     ↓                                                          │
│  Tasks pending → Scale Up                                      │
│  ┌─────┬─────┬─────┬─────┐                                    │
│  │ 1   │ 2   │ 3   │ 4   │ Add executors quickly             │
│  └─────┴─────┴─────┴─────┘                                    │
│     ↓                                                          │
│  High load → Continue Scaling                                  │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐           │
│  │ 1   │ 2   │ 3   │ 4   │ 5   │ 6   │ 7   │ 8   │           │
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘           │
│     ↓                                                          │
│  Peak workload → At Maximum                                    │
│  ┌─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┬─┐ (16 max)                 │
│  └─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┴─┘                           │
│     ↓                                                          │
│  Load decreases → Scale Down (after 60s idle)                │
│  ┌─────┬─────┬─────┬─────┐                                    │
│  │ 1   │ 2   │ 3   │ 4   │                                    │
│  └─────┴─────┴─────┴─────┘                                    │
│     ↓                                                          │
│  Job complete → Minimum Executors                              │
│  ┌─────┐                                                       │
│  │ 2   │                                                       │
│  └─────┘                                                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Configuration:
```python
spark = SparkSession.builder \\
    .config("spark.dynamicAllocation.enabled", "true") \\
    .config("spark.dynamicAllocation.minExecutors", "2") \\
    .config("spark.dynamicAllocation.maxExecutors", "20") \\
    .config("spark.dynamicAllocation.initialExecutors", "4") \\
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \\
    .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1s") \\
    .config("spark.shuffle.service.enabled", "true") \\
    .getOrCreate()
```

Parameters Explained:
- **minExecutors**: Minimum executors (always running)
- **maxExecutors**: Maximum executors (cost ceiling)
- **initialExecutors**: Starting executors (warm start)
- **executorIdleTimeout**: Time before removing idle executor (60s default)
- **schedulerBacklogTimeout**: Time before adding executor for pending tasks (1s)

⚠️  **Requirement**: External shuffle service must be enabled!
   - YARN: yarn.nodemanager.aux-services=spark_shuffle
   - Kubernetes: Use shuffle service DaemonSet
   - Standalone: spark.shuffle.service.enabled=true

Benefits:
✅ Cost optimization (pay for what you use)
✅ Automatic scaling (no manual intervention)
✅ Handle variable workloads
✅ Better resource sharing

Drawbacks:
❌ Slight latency for executor startup
❌ Requires external shuffle service
❌ More complex to debug

COMMON MEMORY ISSUES:
---------------------

1. **Executor OOM (Out of Memory)**:

Symptoms:
```
java.lang.OutOfMemoryError: Java heap space
Container killed by YARN for exceeding memory limits
```

Causes:
• Large shuffles
• Too much cached data
• Skewed partitions (one partition too large)
• Insufficient executor memory

Solutions:
✅ Increase executor memory:
   --executor-memory 16g (was 8g)

✅ Increase memory overhead:
   --conf spark.executor.memoryOverhead=3g

✅ Reduce data skew:
   df.repartition(200, "key")  # Better distribution

✅ Reduce cached data:
   df.unpersist()  # Free up storage memory

✅ Use off-heap memory:
   --conf spark.memory.offHeap.enabled=true
   --conf spark.memory.offHeap.size=4g

2. **Driver OOM**:

Symptoms:
```
Driver OutOfMemoryError during collect()
Broadcasting large variable fails
```

Causes:
• collect() on large DataFrame
• Large broadcast variables (> 2 GB)
• Too many partitions (metadata overhead)

Solutions:
✅ Increase driver memory:
   --driver-memory 8g (was 2g)

✅ Avoid collect() on large data:
   df.write.parquet("output")  # Don't collect!

✅ Use broadcast carefully:
   # Broadcast only if < 10 MB
   if df.count() < 100000:
       broadcast(df)

3. **Disk Spill**:

Symptoms:
```
Spill (memory): 2.5 GB
Spill (disk): 8.3 GB
```

Cause: Execution memory full, spilling to disk (slow!)

Solutions:
✅ Increase execution memory:
   --conf spark.memory.fraction=0.7 (was 0.6)

✅ Reduce partition size:
   df.repartition(400)  # More partitions = smaller each

✅ Filter early:
   df.filter("date > '2024-01-01'").join(...)

✅ Optimize shuffles:
   See 08_shuffle_optimization.py

MONITORING RESOURCES:
---------------------

1. **Spark UI → Executors Tab**:
   - Memory usage per executor
   - GC time (> 10% = problem!)
   - Tasks completed
   - Shuffle read/write

2. **Spark UI → Storage Tab**:
   - Cached data size
   - Memory vs disk storage
   - Evicted blocks

3. **YARN/Kubernetes UI**:
   - Container memory usage
   - CPU utilization
   - Network I/O

4. **Command Line**:
```bash
# YARN
yarn application -status application_xxx

# Monitor containers
yarn logs -applicationId application_xxx

# Kubernetes
kubectl top pods -l spark-role=executor
```

EXPLAIN() AND RESOURCES:
------------------------
explain() shows memory estimates:

```python
df.join(df2, "key").explain(mode="cost")
```

Output:
```
Statistics(sizeInBytes=2.5 GB, rowCount=10000000)
^^^^^^^^^^^^^^^^^
Estimated size helps plan executor memory
```

If estimated size > executor memory:
• Increase executor memory
• Repartition data
• Filter earlier

See Also:
---------
• 02_data_partitioning.py - Partition sizing
• 08_shuffle_optimization.py - Reduce memory pressure
• 05_fault_tolerance.py - Handle executor failures
• 09_cluster_monitoring.py - Monitoring tools
"""

import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, rand


def create_spark_with_resources(app_name, executor_memory="2g", executor_cores=2):
    """Create Spark session with specific resource configuration."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[4]")
        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", str(executor_cores))
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def demonstrate_memory_hierarchy():
    """Understand Spark's memory hierarchy."""
    print("=" * 70)
    print("1. MEMORY HIERARCHY")
    print("=" * 70)

    print(
        """
🧠 Spark Memory Architecture:

┌─────────────────────────────────────────────────────────┐
│                    JVM HEAP MEMORY                      │
│  ┌──────────────────────────────────────────────────┐  │
│  │  spark.memory.fraction (default: 0.6)           │  │
│  │  ┌────────────────┬─────────────────────────┐   │  │
│  │  │ Execution      │ Storage (Cache)         │   │  │
│  │  │ (Shuffles,     │ (cached DataFrames)     │   │  │
│  │  │  Joins, Aggs)  │                         │   │  │
│  │  │ 50%            │ 50%                     │   │  │
│  │  └────────────────┴─────────────────────────┘   │  │
│  │  Unified Memory (spark.memory.fraction = 0.6)   │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Reserved Memory (0.3 or 300MB)                 │  │
│  │  - User data structures                          │  │
│  │  - Internal metadata                             │  │
│  └──────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────┐  │
│  │  Reserved (0.1)                                   │  │
│  │  - Spark internal overhead                       │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
┌─────────────────────────────────────────────────────────┐
│          OFF-HEAP MEMORY (Optional)                     │
│  spark.memory.offHeap.enabled = true                   │
│  spark.memory.offHeap.size = 2g                        │
│  - Avoids GC overhead                                   │
│  - Good for large cached data                           │
└─────────────────────────────────────────────────────────┘

📊 Memory Calculation Example:
------------------------------
Executor Memory: 8 GB
- Unified Memory (60%): 4.8 GB
  - Execution: 2.4 GB
  - Storage: 2.4 GB
- Reserved (30%): 2.4 GB
- Internal (10%): 0.8 GB

Memory Overhead (off-heap):
- spark.executor.memoryOverhead = max(384MB, 0.1 * executor_memory)
- For 8GB executor: max(384MB, 819MB) = 819MB
    """
    )


def demonstrate_executor_sizing():
    """Calculate optimal executor sizing."""
    print("\n" + "=" * 70)
    print("2. EXECUTOR SIZING")
    print("=" * 70)

    print(
        """
🎯 Executor Sizing Formula:

Total Cluster Resources:
- 10 worker nodes
- 16 cores per node
- 64 GB RAM per node
Total: 160 cores, 640 GB RAM

Rule of Thumb:
--------------
1. Reserve 1 core per node for OS/daemons
2. Reserve 1 GB per node for OS
3. Executor cores: 4-8 (optimal for HDFS throughput)
4. Executor memory: Total RAM / executors per node

Calculation:
-----------
Available per node: 15 cores, 63 GB RAM

Option 1: Small Executors (More Parallelism)
--------------------------------------------
Cores per executor: 3
Executors per node: 15 / 3 = 5
Memory per executor: 63 GB / 5 = 12.6 GB
Total executors: 10 nodes × 5 = 50 executors
Total cores: 50 × 3 = 150 cores

spark-submit \\
    --num-executors 50 \\
    --executor-cores 3 \\
    --executor-memory 12g \\
    --executor-memoryOverhead 2g

✅ Good for: Many small tasks, high parallelism
⚠️  Overhead: More executors = more overhead


Option 2: Medium Executors (Balanced) ⭐ RECOMMENDED
----------------------------------------------------
Cores per executor: 5
Executors per node: 15 / 5 = 3
Memory per executor: 63 GB / 3 = 21 GB
Total executors: 10 nodes × 3 = 30 executors
Total cores: 30 × 5 = 150 cores

spark-submit \\
    --num-executors 30 \\
    --executor-cores 5 \\
    --executor-memory 19g \\
    --executor-memoryOverhead 3g

✅ Good for: Most workloads, balanced performance
✅ Sweet spot: 4-8 cores per executor


Option 3: Large Executors (Memory-Intensive)
---------------------------------------------
Cores per executor: 15
Executors per node: 1
Memory per executor: 63 GB
Total executors: 10 nodes × 1 = 10 executors
Total cores: 10 × 15 = 150 cores

spark-submit \\
    --num-executors 10 \\
    --executor-cores 15 \\
    --executor-memory 56g \\
    --executor-memoryOverhead 8g

✅ Good for: Large cached DataFrames, memory-heavy ops
⚠️  Risk: Too many cores per executor can hurt HDFS throughput
⚠️  Risk: Large executors = less fault tolerance


📊 Comparison:

Configuration     Executors  Cores/Exec  Memory/Exec  Use Case
-------------     ---------  ----------  -----------  --------
Small (high ||)   50         3           12 GB        Many tasks
Medium (balanced) 30         5           19 GB        General ⭐
Large (mem-heavy) 10         15          56 GB        Caching
    """
    )


def demonstrate_dynamic_allocation(spark):
    """Dynamic allocation of executors."""
    print("\n" + "=" * 70)
    print("3. DYNAMIC ALLOCATION")
    print("=" * 70)

    print(
        """
🔄 Dynamic Allocation: Auto-scale executors based on workload

Configuration:
--------------
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 20
spark.dynamicAllocation.initialExecutors = 5
spark.dynamicAllocation.executorIdleTimeout = 60s
spark.dynamicAllocation.schedulerBacklogTimeout = 1s

How it works:
-------------
1. Start with initialExecutors (5)
2. If tasks are pending, add executors (up to maxExecutors)
3. If executor idle for 60s, remove it (down to minExecutors)
4. Request new executors when tasks backlog > 1s

Benefits:
---------
✅ No manual tuning
✅ Cost savings (release idle executors)
✅ Auto-scale for bursty workloads
✅ Better resource utilization

Example Timeline:
-----------------
Time 0s:   5 executors (initial)
Time 10s:  Large job submitted → 20 tasks pending
Time 11s:  Scale up to 15 executors
Time 30s:  Job processing → all executors busy
Time 90s:  Job complete → executors idle
Time 150s: Scale down to 2 executors (minExecutors)
    """
    )

    # Demonstrate with different workload sizes
    print("\n📊 Simulating Dynamic Allocation:")

    # Small workload
    print("\n1. Small workload (1K rows):")
    small_df = spark.range(1000).toDF("id")
    start = time.time()
    result = small_df.count()
    print(f"   Rows: {result:,}, Time: {time.time() - start:.3f}s")
    print("   → Uses minimum executors")

    # Large workload
    print("\n2. Large workload (10M rows):")
    large_df = spark.range(10000000).toDF("id").withColumn("value", rand() * 1000)
    start = time.time()
    result = large_df.groupBy((col("id") % 100).alias("bucket")).count().count()
    print(f"   Buckets: {result}, Time: {time.time() - start:.3f}s")
    print("   → Would scale up to handle load")


def demonstrate_memory_tuning(spark):
    """Memory tuning strategies."""
    print("\n" + "=" * 70)
    print("4. MEMORY TUNING")
    print("=" * 70)

    # Create dataset
    data = (
        spark.range(1, 5000001)
        .toDF("id")
        .withColumn("value1", rand() * 1000)
        .withColumn("value2", rand() * 1000)
        .withColumn("value3", rand() * 1000)
    )

    print(f"📊 Dataset: {data.count():,} rows")

    # Scenario 1: No caching
    print("\n❌ Scenario 1: No caching (recompute each time)")
    start = time.time()
    data.filter(col("value1") > 500).count()
    time1 = time.time() - start
    data.filter(col("value2") > 500).count()
    time2 = time.time() - start - time1
    print(f"   Query 1: {time1:.3f}s")
    print(f"   Query 2: {time2:.3f}s")
    print(f"   Total: {time1 + time2:.3f}s")

    # Scenario 2: Memory caching
    print("\n✅ Scenario 2: MEMORY_ONLY caching")
    from pyspark import StorageLevel

    cached_data = data.persist(StorageLevel.MEMORY_ONLY)
    cached_data.count()  # Materialize
    start = time.time()
    cached_data.filter(col("value1") > 500).count()
    time3 = time.time() - start
    cached_data.filter(col("value2") > 500).count()
    time4 = time.time() - start - time3
    print(f"   Query 1: {time3:.3f}s")
    print(f"   Query 2: {time4:.3f}s")
    print(f"   Total: {time3 + time4:.3f}s")
    print(f"   Speedup: {(time1 + time2) / (time3 + time4):.2f}x")
    cached_data.unpersist()

    print("\n💡 Tuning Tips:")
    print(
        """
1. Increase memory fraction for cache-heavy workloads:
   spark.conf.set("spark.memory.fraction", "0.8")  # Default: 0.6

2. Adjust storage fraction within unified memory:
   spark.conf.set("spark.memory.storageFraction", "0.6")  # Default: 0.5

3. Enable off-heap memory for large caches:
   spark.conf.set("spark.memory.offHeap.enabled", "true")
   spark.conf.set("spark.memory.offHeap.size", "4g")

4. Increase executor overhead for large DataFrames:
   --executor-memoryOverhead 4g  # Default: 10% of executor memory
    """
    )


def demonstrate_cpu_allocation(spark):
    """CPU core allocation strategies."""
    print("\n" + "=" * 70)
    print("5. CPU CORE ALLOCATION")
    print("=" * 70)

    print(
        """
🔧 Core Allocation Strategies:

Parallelism Formula:
--------------------
total_cores = num_executors × cores_per_executor
parallelism = total_cores × 2 to 4

Example: 10 executors × 5 cores = 50 cores
Parallelism: 100-200 partitions

Configuration:
--------------
# RDD operations
spark.conf.set("spark.default.parallelism", "200")

# DataFrame operations (shuffles)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Task cores (usually 1)
spark.conf.set("spark.task.cpus", "1")


📊 Cores per Executor Impact:

Cores  Parallelism  HDFS Throughput  Use Case
-----  -----------  ---------------  --------
1-2    Low          Poor             Not recommended
3-4    Medium       Good             Small clusters
5-8    High         Optimal          Recommended ⭐
9-15   Very High    Good             Memory-heavy
16+    Too High     Poor             Avoid

Why 5-8 is optimal:
-------------------
✅ Good HDFS read throughput (5 concurrent reads)
✅ Balanced task scheduling
✅ Not too much overhead per executor
✅ Good fault tolerance (more executors)


Task Scheduling:
----------------
Each core can run 1 task at a time
More cores = more concurrent tasks

Example:
- 50 total cores
- 200 partitions
- Each core processes 4 partitions sequentially
- Wave 1: Tasks 1-50 (parallel)
- Wave 2: Tasks 51-100 (parallel)
- ...
- Wave 4: Tasks 151-200 (parallel)
    """
    )

    # Demonstrate parallelism
    data = spark.range(10000000).toDF("id")

    print("\n📊 Testing Different Partition Counts:")

    # Too few partitions
    print("\n❌ Too few partitions (10):")
    start = time.time()
    result = data.repartition(10).groupBy((col("id") % 100).alias("bucket")).count()
    result.write.mode("overwrite").format("noop").save()
    time1 = time.time() - start
    print(f"   Time: {time1:.3f}s")
    print("   ⚠️  Underutilized: 4 cores but only 10 tasks")

    # Optimal partitions
    print("\n✅ Optimal partitions (16 = 4 cores × 4):")
    start = time.time()
    result = data.repartition(16).groupBy((col("id") % 100).alias("bucket")).count()
    result.write.mode("overwrite").format("noop").save()
    time2 = time.time() - start
    print(f"   Time: {time2:.3f}s")
    print(f"   Speedup: {time1 / time2:.2f}x")

    # Too many partitions
    print("\n⚠️  Too many partitions (1000):")
    start = time.time()
    result = data.repartition(1000).groupBy((col("id") % 100).alias("bucket")).count()
    result.write.mode("overwrite").format("noop").save()
    time3 = time.time() - start
    print(f"   Time: {time3:.3f}s")
    print("   ⚠️  Overhead: Too many small tasks")


def demonstrate_resource_best_practices():
    """Best practices for resource management."""
    print("\n" + "=" * 70)
    print("6. RESOURCE MANAGEMENT BEST PRACTICES")
    print("=" * 70)

    print(
        """
🎯 Production Configuration Template:

# ============================================================
# SMALL CLUSTER (3 nodes, 16 cores, 64 GB each)
# ============================================================
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 9 \\
    --executor-cores 5 \\
    --executor-memory 19g \\
    --executor-memoryOverhead 3g \\
    --driver-memory 4g \\
    --driver-cores 2 \\
    --conf spark.default.parallelism=180 \\
    --conf spark.sql.shuffle.partitions=180 \\
    --conf spark.dynamicAllocation.enabled=false \\
    your_script.py

# Total: 9 executors × 5 cores = 45 cores (out of 48)
# Parallelism: 45 × 4 = 180 partitions


# ============================================================
# MEDIUM CLUSTER (10 nodes, 16 cores, 64 GB each)
# ============================================================
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 30 \\
    --executor-cores 5 \\
    --executor-memory 19g \\
    --executor-memoryOverhead 3g \\
    --driver-memory 8g \\
    --driver-cores 4 \\
    --conf spark.default.parallelism=600 \\
    --conf spark.sql.shuffle.partitions=600 \\
    --conf spark.dynamicAllocation.enabled=true \\
    --conf spark.dynamicAllocation.minExecutors=10 \\
    --conf spark.dynamicAllocation.maxExecutors=50 \\
    --conf spark.dynamicAllocation.initialExecutors=30 \\
    your_script.py

# Total: 30 executors × 5 cores = 150 cores (dynamic 50-250)
# Parallelism: 150 × 4 = 600 partitions


# ============================================================
# LARGE CLUSTER (100 nodes, 32 cores, 256 GB each)
# ============================================================
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 600 \\
    --executor-cores 5 \\
    --executor-memory 40g \\
    --executor-memoryOverhead 6g \\
    --driver-memory 16g \\
    --driver-cores 8 \\
    --conf spark.default.parallelism=12000 \\
    --conf spark.sql.shuffle.partitions=12000 \\
    --conf spark.dynamicAllocation.enabled=true \\
    --conf spark.dynamicAllocation.minExecutors=100 \\
    --conf spark.dynamicAllocation.maxExecutors=800 \\
    your_script.py

# Total: 600 executors × 5 cores = 3000 cores
# Parallelism: 3000 × 4 = 12000 partitions


# ============================================================
# GPU CLUSTER (10 nodes, 16 cores, 64 GB, 1 GPU each)
# ============================================================
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 10 \\
    --executor-cores 4 \\
    --executor-memory 16g \\
    --executor-memoryOverhead 4g \\
    --driver-memory 8g \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.task.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh \\
    --conf spark.default.parallelism=40 \\
    --conf spark.sql.shuffle.partitions=40 \\
    gpu_inference.py

# Total: 10 executors × 1 GPU = 10 GPUs
# Parallelism: 10 × 4 = 40 tasks (GPU-bound, not CPU-bound)


✅ Best Practices Summary:

1. Executor Cores: 5 (sweet spot for HDFS)
2. Executor Memory: (Node RAM - 1GB) / (executors per node)
3. Memory Overhead: max(384MB, 10% of executor memory)
4. Driver Memory: 2-16 GB (depends on collect size)
5. Parallelism: total_cores × 2 to 4
6. Dynamic Allocation: Enable for bursty workloads
7. Reserve Resources: 1 core + 1 GB per node for OS

⚠️ Common Mistakes:

1. ❌ Too many cores per executor (>8)
   → Poor HDFS throughput

2. ❌ Too little memory overhead
   → Container killed by YARN

3. ❌ Too many/few partitions
   → Scheduler overhead or underutilization

4. ❌ Forgetting driver memory
   → OOM when collecting results

5. ❌ Not using dynamic allocation
   → Wasting resources or insufficient resources


📊 Resource Calculation Tool:

def calculate_executors(nodes, cores_per_node, ram_per_node_gb):
    \"\"\"Calculate optimal executor configuration.\"\"\"
    # Reserve 1 core and 1 GB per node
    available_cores = cores_per_node - 1
    available_ram = ram_per_node_gb - 1
    
    # Optimal: 5 cores per executor
    cores_per_executor = 5
    executors_per_node = available_cores // cores_per_executor
    
    # Memory per executor
    memory_per_executor = available_ram // executors_per_node
    executor_memory = int(memory_per_executor * 0.9)  # 90% for heap
    memory_overhead = int(memory_per_executor * 0.1)  # 10% overhead
    
    # Total
    total_executors = nodes * executors_per_node
    total_cores = total_executors * cores_per_executor
    parallelism = total_cores * 4
    
    print(f"Nodes: {nodes}")
    print(f"Executors: {total_executors}")
    print(f"Cores per executor: {cores_per_executor}")
    print(f"Memory per executor: {executor_memory}g")
    print(f"Memory overhead: {memory_overhead}g")
    print(f"Total cores: {total_cores}")
    print(f"Recommended parallelism: {parallelism}")
    
    return {
        'executors': total_executors,
        'cores': cores_per_executor,
        'memory': executor_memory,
        'overhead': memory_overhead
    }

# Example usage:
calculate_executors(nodes=10, cores_per_node=16, ram_per_node_gb=64)
    """
    )


def main():
    spark = create_spark_with_resources("ResourceManagement")

    print("⚙️  RESOURCE MANAGEMENT")
    print("=" * 70)
    print("\nMaster memory, CPU, and dynamic allocation for optimal performance!")
    print()

    demonstrate_memory_hierarchy()
    demonstrate_executor_sizing()
    demonstrate_dynamic_allocation(spark)
    demonstrate_memory_tuning(spark)
    demonstrate_cpu_allocation(spark)
    demonstrate_resource_best_practices()

    print("\n" + "=" * 70)
    print("✅ RESOURCE MANAGEMENT DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Use 5 cores per executor (optimal for HDFS)")
    print("   2. Memory: (Node RAM - 1GB) / executors_per_node")
    print("   3. Parallelism: total_cores × 2-4")
    print("   4. Enable dynamic allocation for bursty workloads")
    print("   5. Reserve 1 core + 1GB per node for OS")
    print("   6. Memory overhead: max(384MB, 10% executor memory)")
    print("   7. Monitor and tune based on Spark UI metrics")

    spark.stop()


if __name__ == "__main__":
    main()
