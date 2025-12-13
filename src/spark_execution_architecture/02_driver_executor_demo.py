#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
DRIVER AND EXECUTOR RESPONSIBILITIES - Complete Demonstration
================================================================================

MODULE OVERVIEW:
----------------
This module provides a comprehensive demonstration of the Driver-Executor
architecture in Apache Spark. It shows the distinct responsibilities,
communication patterns, and resource allocation between the Driver program
and Executor processes that form the foundation of Spark's distributed
computing model.

Understanding the Driver-Executor relationship is CRITICAL for:
• Debugging performance issues
• Optimizing resource allocation
• Understanding failure modes
• Writing efficient Spark applications
• Troubleshooting memory problems

PURPOSE:
--------
Spark uses a master-worker architecture where:
• DRIVER = Master (plans and coordinates)
• EXECUTORS = Workers (execute and store data)

This module demonstrates:
1. What the Driver does (planning, scheduling, coordination)
2. What Executors do (task execution, data storage, computation)
3. How they communicate (task assignment, result collection, shuffles)
4. Failure handling (task retries, executor failures, lineage)
5. Resource allocation (memory, cores, parallelism)

TARGET AUDIENCE:
----------------
• Data engineers learning Spark architecture
• Developers debugging Spark applications
• System administrators configuring clusters
• Anyone experiencing memory or performance issues

================================================================================
DRIVER RESPONSIBILITIES (The "Brain"):
================================================================================

The Driver is the control center of your Spark application. It runs your
main() function and coordinates all work across the cluster.

┌─────────────────────────────────────────────────────────────────┐
│                        DRIVER PROCESS                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. MAINTAINS SPARKSESSION                                      │
│     • Entry point for all Spark functionality                   │
│     • Holds configuration                                       │
│     • Manages SparkContext                                      │
│                                                                  │
│  2. BUILDS EXECUTION PLANS                                      │
│     • Parses user code into logical plan                        │
│     • Optimizes with Catalyst optimizer                         │
│     • Generates physical execution plan                         │
│     • Creates DAG (Directed Acyclic Graph)                      │
│                                                                  │
│  3. SCHEDULES JOBS/STAGES/TASKS                                 │
│     ┌────────────────────────────────────────┐                 │
│     │ ACTION (e.g., .count())                │                 │
│     │    ↓                                   │                 │
│     │ JOB (one per action)                   │                 │
│     │    ↓                                   │                 │
│     │ STAGES (split at shuffle boundaries)   │                 │
│     │    ↓                                   │                 │
│     │ TASKS (one per partition)              │                 │
│     └────────────────────────────────────────┘                 │
│                                                                  │
│  4. TRACKS EXECUTOR STATUS                                      │
│     • Which executors are available?                            │
│     • How much memory do they have?                             │
│     • Are they responding?                                      │
│                                                                  │
│  5. COLLECTS RESULTS                                            │
│     • Receives task completion notifications                    │
│     • Aggregates results from all executors                     │
│     • Returns final result to user                              │
│                                                                  │
│  6. MANAGES BROADCAST VARIABLES                                 │
│     • Efficiently distributes read-only data                    │
│     • Sends to all executors once                               │
│     • Avoids sending same data with every task                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

DRIVER MEMORY CONTAINS:
• SparkSession and SparkContext
• Execution plans (DAGs)
• Job/stage/task tracking metadata
• Broadcast variables
• Results from collect(), take(), etc. (⚠️ Can cause OOM!)

⚠️  DRIVER FAILURE = ENTIRE APPLICATION FAILS (single point of failure)

================================================================================
EXECUTOR RESPONSIBILITIES (The "Workers"):
================================================================================

Executors are worker processes that run on cluster nodes. They do the actual
data processing work.

┌─────────────────────────────────────────────────────────────────┐
│                      EXECUTOR PROCESS                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. EXECUTE TASKS ON DATA PARTITIONS                            │
│     • Receives serialized task code from driver                 │
│     • Runs task on local partition of data                      │
│     • Each task processes ONE partition                         │
│                                                                  │
│  2. STORE DATA PARTITIONS IN MEMORY/DISK                        │
│     • cache() / persist() stores data locally                   │
│     • Intermediate shuffle data                                 │
│     • Broadcast variable copies                                 │
│                                                                  │
│  3. PERFORM COMPUTATIONS                                        │
│     • Filter, map, flatMap, etc.                                │
│     • All transformations run on executors                      │
│     • Use executor cores for parallel execution                 │
│                                                                  │
│  4. HANDLE SHUFFLE OPERATIONS                                   │
│     • SHUFFLE WRITE: Group data and write to disk               │
│     • SHUFFLE READ: Read data from other executors              │
│     • Shuffle files survive executor failures                   │
│                                                                  │
│  5. REPORT METRICS TO DRIVER                                    │
│     • Task completion status                                    │
│     • Records processed                                         │
│     • Bytes read/written                                        │
│     • Execution time                                            │
│     • Memory usage                                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

EXECUTOR MEMORY CONTAINS:
• Data partitions (cached DataFrames/RDDs)
• Task execution memory (for joins, aggregations)
• Shuffle buffers (shuffle read/write)
• Broadcast variable copies

⚠️  EXECUTOR FAILURE = TASKS RECOMPUTED (fault tolerant via lineage)

================================================================================
DRIVER ↔ EXECUTOR COMMUNICATION FLOW:
================================================================================

Step-by-step communication for a simple count() operation:

1. JOB SUBMISSION (Driver → Executors):
   ┌──────────┐                                ┌──────────┐
   │          │  "Execute Task 0: count ids"   │          │
   │  DRIVER  │ ─────────────────────────────> │ Exec 1   │
   │          │  "Execute Task 1: count ids"   │          │
   │          │ ─────────────────────────────> │ Exec 2   │
   │          │  "Execute Task 2: count ids"   │          │
   │          │ ─────────────────────────────> │ Exec 3   │
   └──────────┘                                └──────────┘

2. TASK EXECUTION (Executors work independently):
   Exec 1: Partition 0 → count = 250
   Exec 2: Partition 1 → count = 250
   Exec 3: Partition 2 → count = 250

3. RESULT COLLECTION (Executors → Driver):
   ┌──────────┐                                ┌──────────┐
   │          │ <───── "Task 0 complete: 250" ─│ Exec 1   │
   │  DRIVER  │ <───── "Task 1 complete: 250" ─│ Exec 2   │
   │          │ <───── "Task 2 complete: 250" ─│ Exec 3   │
   └──────────┘                                └──────────┘

4. FINAL AGGREGATION (Driver):
   Driver sums: 250 + 250 + 250 = 750
   Returns 750 to user

SHUFFLE COMMUNICATION (Executor ↔ Executor):
When doing groupBy, join, or aggregation:

   ┌──────────┐         ┌──────────┐
   │ Exec 1   │ <────> │ Exec 2   │   Exchange data
   │ (writes) │         │ (reads)  │   across network
   └──────────┘         └──────────┘
        ↕                    ↕
   ┌──────────┐         ┌──────────┐
   │ Exec 3   │ <────> │ Exec 4   │
   │ (writes) │         │ (reads)  │
   └──────────┘         └──────────┘

Executors write shuffle files to local disk, then other executors
read those files over the network. This is called a "shuffle".

================================================================================
FAILURE HANDLING & FAULT TOLERANCE:
================================================================================

Spark provides fault tolerance through LINEAGE - the DAG of operations
that tracks how to recompute lost data.

SCENARIO 1: Task Failure
────────────────────────
Problem: Task fails due to temporary issue (network timeout, etc.)
Solution: Driver detects failure → Reschedules task on same/different executor
Result: ✅ Transparent retry, user doesn't see failure

SCENARIO 2: Executor Failure
─────────────────────────────
Problem: Executor process crashes or becomes unresponsive
Solution:
  1. Driver marks executor as lost
  2. Driver requests new executor from cluster manager
  3. Driver reschedules all tasks from failed executor
  4. Uses lineage to recompute lost partitions
Result: ✅ Application continues, recomputes lost data

SCENARIO 3: Driver Failure
───────────────────────────
Problem: Driver process crashes
Solution: ⚠️  NO AUTOMATIC RECOVERY - entire application fails!
Why: Driver holds all state (execution plans, task tracking, etc.)
Mitigation:
  • Use --deploy-mode cluster (driver runs on cluster, not client)
  • Enable checkpointing for Structured Streaming
  • Use external orchestration (Kubernetes, Airflow) to restart

SCENARIO 4: Data Loss (Cached RDD)
───────────────────────────────────
Problem: Executor with cached partition crashes
Solution:
  1. Driver detects cache partition is lost
  2. Uses lineage to recompute partition from source
  3. Caches recomputed partition on available executor
Result: ✅ Cache rebuilt automatically

SHUFFLE PERSISTENCE:
When shuffle data is written to disk:
  • Shuffle files persist even if executor fails
  • External shuffle service keeps shuffle data available
  • Reduces recomputation after executor failures

================================================================================
RESOURCE ALLOCATION & CONFIGURATION:
================================================================================

TYPICAL CLUSTER SETUP:

┌────────────────────────────────────────────────────────────┐
│  CLIENT MACHINE (where you run spark-submit)              │
│  ┌──────────────────────────────────────────────────────┐ │
│  │  spark-submit --master yarn \\                       │ │
│  │    --deploy-mode cluster \\                          │ │
│  │    --driver-memory 4g \\                             │ │
│  │    --driver-cores 2 \\                               │ │
│  │    --executor-memory 8g \\                           │ │
│  │    --executor-cores 4 \\                             │ │
│  │    --num-executors 10 \\                             │ │
│  │    my_app.py                                         │ │
│  └──────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────┘
                           ↓ Submits to Cluster Manager
┌────────────────────────────────────────────────────────────┐
│  CLUSTER (YARN / Kubernetes / Standalone)                 │
│                                                            │
│  ┌──────────────┐                                         │
│  │   DRIVER     │  Node 1                                 │
│  │   4GB / 2c   │  • Plans execution                      │
│  └──────────────┘  • Schedules tasks                      │
│                    • Collects results                      │
│                                                            │
│  ┌─────────────────────────────────────────────────────┐ │
│  │  EXECUTORS                                          │ │
│  ├─────────────────────────────────────────────────────┤ │
│  │                                                     │ │
│  │  Node 2-11 (10 executor nodes):                    │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐   │ │
│  │  │ Executor 1 │  │ Executor 2 │  │    ...     │   │ │
│  │  │  8GB / 4c  │  │  8GB / 4c  │  │ Executor 10│   │ │
│  │  └────────────┘  └────────────┘  └────────────┘   │ │
│  │                                                     │ │
│  │  Each executor:                                     │ │
│  │  • Runs 4 tasks concurrently (4 cores)             │ │
│  │  • Stores up to 8GB of cached data                 │ │
│  │  • Handles local shuffle read/write                │ │
│  │                                                     │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                            │
│  TOTAL CLUSTER RESOURCES:                                 │
│  • Driver: 4GB memory, 2 cores                            │
│  • Executors: 10 × 8GB = 80GB memory                      │
│  • Parallelism: 10 × 4 = 40 concurrent tasks              │
│                                                            │
└────────────────────────────────────────────────────────────┘

KEY CONFIGURATION PARAMETERS:

--driver-memory 4g
  └─> Memory for driver process
      • Holds SparkSession, execution plans, broadcast vars
      • ⚠️  collect() results come here (can cause OOM!)
      • Typical: 2-8GB

--driver-cores 2
  └─> CPU cores for driver
      • Schedules tasks, collects results
      • Usually 1-4 cores sufficient

--executor-memory 8g
  └─> Memory per executor
      • Caches data partitions
      • Task execution memory (joins, aggregations)
      • Shuffle buffers
      • Typical: 4-16GB per executor

--executor-cores 4
  └─> CPU cores per executor
      • Each core runs 1 task at a time
      • More cores = more parallel tasks
      • Typical: 4-8 cores per executor

--num-executors 10
  └─> Number of executor processes
      • More executors = more parallelism
      • Balance: too many = overhead, too few = underutilized

MEMORY BREAKDOWN (per Executor):

┌────────────────────────────────────────┐
│  EXECUTOR MEMORY (8GB example)         │
├────────────────────────────────────────┤
│  Reserved Memory (300MB)      │ 4%     │  Spark overhead
├────────────────────────────────────────┤
│  Storage Memory (3.85GB)      │ 48%    │  cache(), persist()
├────────────────────────────────────────┤
│  Execution Memory (3.85GB)    │ 48%    │  Shuffles, joins, sorts
└────────────────────────────────────────┘

Storage and Execution memory can borrow from each other
(spark.memory.fraction = 0.6, spark.memory.storageFraction = 0.5)

================================================================================
USAGE:
================================================================================

Run this script to see Driver-Executor interaction:

    python 02_driver_executor_demo.py

The script demonstrates:
1. Driver responsibilities (planning, scheduling, coordination)
2. Executor responsibilities (task execution, data storage)
3. Communication patterns (task assignment, result collection)
4. Failure handling (retries, executor failures, lineage)
5. Resource allocation (memory and core distribution)

MONITORING:
Access Spark UI at http://localhost:4040 (or 4041, 4042, etc.)
• Jobs tab: See jobs, stages, tasks
• Storage tab: View cached data on executors
• Executors tab: Monitor executor health and resources
• SQL tab: View query execution plans

================================================================================
RELATED RESOURCES:
================================================================================

Spark Architecture:
  https://spark.apache.org/docs/latest/cluster-overview.html

Job Scheduling:
  https://spark.apache.org/docs/latest/job-scheduling.html

Configuration:
  https://spark.apache.org/docs/latest/configuration.html

Tuning Guide:
  https://spark.apache.org/docs/latest/tuning.html

Related files in this project:
  • 01_dag_visualization.py - Shows execution plan visualization
  • 03_driver_yarn_cluster_interaction.py - YARN cluster lifecycle
  • 04_standalone_cluster_mode.py - Standalone cluster setup

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 2.0.0 - Comprehensive Driver-Executor Documentation
CREATED: 2024
UPDATED: 2024 - Added extensive module header and inline comments
================================================================================
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def create_spark():
    """
    Create Spark session with explicit configuration.

    This function demonstrates DRIVER INITIALIZATION - the first responsibility
    of the driver process. The SparkSession creation happens on the driver and
    establishes the connection to executors.

    Configuration Breakdown:
    ------------------------
    .appName("DriverExecutorDemo")
      └─> Sets application name (visible in Spark UI and cluster manager)

    .master("local[*]")
      └─> Run locally with as many worker threads as logical cores
          In real clusters: "yarn", "k8s://...", or "spark://..."

    .config("spark.executor.memory", "1g")
      └─> Each executor gets 1GB memory
          Used for: caching data, task execution, shuffle buffers

    .config("spark.driver.memory", "1g")
      └─> Driver gets 1GB memory
          Used for: SparkSession, execution plans, broadcast vars, collect()

    .config("spark.executor.cores", "2")
      └─> Each executor uses 2 CPU cores
          Each core runs 1 task at a time → 2 concurrent tasks per executor

    .config("spark.sql.shuffle.partitions", "8")
      └─> When shuffling (groupBy, join), create 8 output partitions
          Default is 200 (too high for small data)

    .getOrCreate()
      └─> Creates new session or returns existing one
          Only ONE active SparkSession per JVM recommended

    Returns:
    --------
    SparkSession - The entry point for all Spark functionality
    """
    return (
        SparkSession.builder.appName("DriverExecutorDemo")
        .master("local[*]")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.cores", "2")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def demonstrate_driver_responsibilities():
    """
    Show what the driver does.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITIES")
    print("=" * 80)

    spark = create_spark()

    print("\n🎯 DRIVER TASKS:")
    print("=" * 80)

    # ========================================================================
    # 1. DRIVER RESPONSIBILITY: Maintains SparkSession
    # ========================================================================
    # The driver process holds the SparkSession object in memory. This is
    # the entry point that gives you access to all Spark APIs (SQL, DataFrame,
    # RDD, Streaming, ML). If the driver fails, the SparkSession is lost and
    # the entire application terminates.
    # ========================================================================
    print("\n1️⃣  Maintains SparkSession")
    print(f"   SparkSession created: {spark}")
    print(f"   App Name: {spark.sparkContext.appName}")
    print(f"   Master: {spark.sparkContext.master}")

    # ========================================================================
    # 2. DRIVER RESPONSIBILITY: Builds Execution Plans
    # ========================================================================
    # When you write DataFrame operations, NO DATA is processed yet (lazy eval).
    # The driver analyzes your code and builds TWO PLANS:
    #
    # LOGICAL PLAN: What you want to do (high-level operations)
    #   • Parsed from your Python/Scala/SQL code
    #   • Optimized by Catalyst optimizer (predicate pushdown, column pruning)
    #
    # PHYSICAL PLAN: How to execute it (low-level operations)
    #   • Actual execution strategy (scan, filter, project)
    #   • Chooses join strategies (broadcast, sort-merge, shuffle hash)
    #   • Determines partition count and distribution
    #
    # The driver builds these plans BEFORE any executor starts working.
    # Use .explain() to see what the driver planned.
    # ========================================================================
    print("\n2️⃣  Builds Execution Plans (Logical → Physical)")

    # Create a simple DataFrame (lazy - no execution yet)
    df = spark.range(0, 1000).toDF("id")  # Numbers 0-999

    # Add transformations (still lazy - driver just builds the plan)
    result = df.filter(col("id") > 500).select("id")  # Filter and select

    print("   Logical Plan built by driver:")
    # explain(extended=True) shows the complete plan pipeline:
    # 1. Parsed Logical Plan (your code as-is)
    # 2. Analyzed Logical Plan (with schema info)
    # 3. Optimized Logical Plan (after Catalyst optimization)
    # 4. Physical Plan (actual execution strategy)
    result.explain(extended=True)

    # ========================================================================
    # 3. DRIVER RESPONSIBILITY: Schedules Jobs, Stages, and Tasks
    # ========================================================================
    # The driver breaks your query into a THREE-LEVEL hierarchy:
    #
    # ACTION (e.g., .count(), .show(), .write())
    #   └─> Triggers JOB execution
    #        └─> Job contains STAGES (separated by shuffles)
    #             └─> Each Stage contains TASKS (one per partition)
    #
    # Example: df.groupBy("key").count()
    #   ACTION: .count()
    #   JOB: Single job created
    #   STAGES:
    #     Stage 1: Read data, compute partial counts (map side)
    #     Stage 2: Shuffle and combine counts (reduce side)
    #   TASKS:
    #     Stage 1: If 8 partitions → 8 tasks
    #     Stage 2: If shuffle creates 4 partitions → 4 tasks
    #
    # The driver sends each task to an available executor core.
    # ========================================================================
    print("\n3️⃣  Schedules Jobs, Stages, and Tasks")
    print("   Driver breaks query into:")
    print("   • Jobs (one per action like count, show, write)")
    print("   • Stages (split at shuffle boundaries like groupBy, join)")
    print("   • Tasks (one per data partition - unit of parallel work)")
    print("")
    print("   Hierarchy: ACTION → JOB → STAGES → TASKS")
    print("   Example: 8 partitions + no shuffle = 1 stage with 8 tasks")

    # ========================================================================
    # 4. DRIVER RESPONSIBILITY: Tracks Executor Status
    # ========================================================================
    # The driver maintains a registry of ALL executors in the cluster:
    # • Executor ID and location (host:port)
    # • Available memory and cores
    # • Current task assignments
    # • Heartbeat status (is executor still alive?)
    #
    # The driver uses this info to make scheduling decisions:
    # • Which executor should run which task?
    # • Is data locality possible? (send task to executor with data)
    # • Which executors have failed?
    #
    # defaultParallelism = total cores available across all executors
    # This determines how many tasks can run concurrently.
    # ========================================================================
    print("\n4️⃣  Tracks Executor Status")
    print(f"   Active executors: {spark.sparkContext.defaultParallelism}")
    print(
        f"   Default parallelism: {spark.sparkContext.defaultParallelism} concurrent tasks"
    )
    print("   ")
    print("   Driver tracks:")
    print("   • Which executors are alive")
    print("   • Available memory and cores per executor")
    print("   • Current task assignments")
    print("   • Data locality (which partitions are cached where)")

    # ========================================================================
    # 5. DRIVER RESPONSIBILITY: Collects Results from Executors
    # ========================================================================
    # When you trigger an action (.count(), .collect(), .show()), the driver:
    # 1. Sends tasks to executors
    # 2. Waits for executors to complete tasks
    # 3. Receives results from each task
    # 4. Aggregates results (e.g., sum partial counts)
    # 5. Returns final result to user
    #
    # ⚠️  WARNING: result.collect() brings ALL data to driver!
    #    • Driver memory can OOM (Out Of Memory)
    #    • Use .take(N) or write to disk instead
    #
    # count() is safe - only returns a single Long value
    # ========================================================================
    print("\n5️⃣  Collects Results from Executors")

    # Trigger the action - this is when execution actually happens!
    count = result.count()  # Executor tasks run, driver collects counts

    print(f"   Driver received: {count} rows from executors")
    print("   ")
    print("   What happened:")
    print("   1. Driver sent count tasks to executors")
    print("   2. Each executor counted rows in its partitions")
    print("   3. Executors sent partial counts back to driver")
    print("   4. Driver summed partial counts → final result")

    # ========================================================================
    # 6. DRIVER RESPONSIBILITY: Manages Broadcast Variables
    # ========================================================================
    # Broadcast variables efficiently share READ-ONLY data across executors.
    #
    # WITHOUT BROADCAST:
    #   If you use a large lookup dict in a UDF, Spark sends it with EVERY task
    #   Example: 1000 tasks × 100MB dict = 100GB network traffic!
    #
    # WITH BROADCAST:
    #   Driver sends dict ONCE to each executor → reused by all tasks on that executor
    #   Example: 10 executors × 100MB = 1GB network traffic ✅
    #
    # HOW IT WORKS:
    # 1. Driver holds master copy of broadcast variable
    # 2. Driver uses BitTorrent-like protocol to distribute efficiently
    # 3. Each executor caches broadcast data in memory
    # 4. All tasks on that executor access the same copy
    #
    # USE CASES:
    # • Lookup dictionaries for enrichment
    # • Small dimension tables (for broadcast joins)
    # • ML model parameters
    # • Configuration data
    # ========================================================================
    print("\n6️⃣  Manages Broadcast Variables")

    # Create broadcast variable (driver holds master copy)
    broadcast_var = spark.sparkContext.broadcast({"key": "value"})

    print(f"   Broadcast variable created: {broadcast_var}")
    print("   Driver sends broadcast to all executors")
    print("   ")
    print("   Benefits:")
    print("   • Sent once per executor (not once per task)")
    print("   • Reduces network traffic")
    print("   • Shared memory across tasks on same executor")
    print("   • Efficient for large read-only data (ML models, lookup tables)")

    print("\n📊 DRIVER MEMORY USAGE:")
    print("   What the driver stores in memory:")
    print("   • SparkSession and SparkContext metadata")
    print("   • Execution plans (DAGs) for all jobs")
    print("   • Job/stage/task tracking info")
    print("   • Broadcast variables (master copy)")
    print("   • Results from collect() ⚠️  Can cause OOM!")
    print("   • Accumulated metrics from executors")
    print("")
    print("   Typical driver memory: 2-8GB")
    print("   More if you collect() large results or have many broadcast vars")


def demonstrate_executor_responsibilities():
    """
    Show what executors do.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR RESPONSIBILITIES")
    print("=" * 80)

    spark = create_spark()

    print("\n🔧 EXECUTOR TASKS:")
    print("=" * 80)

    # 1. Run tasks
    print("\n1️⃣  Execute Tasks on Partitions")
    df = spark.range(0, 10000).repartition(4)

    # UDF to show which executor is running
    @udf(StringType())
    def get_partition_info(value):
        import os

        return f"PID:{os.getpid()}"

    result = df.withColumn("executor_pid", get_partition_info(col("id")))

    print("   Tasks distributed across executors:")
    result.select("executor_pid").distinct().show()

    # ========================================================================
    # 2. EXECUTOR RESPONSIBILITY: Store Data Partitions in Memory/Disk
    # ========================================================================
    # When you call .cache() or .persist(), data is stored ON EXECUTORS.
    #
    # CACHE WORKFLOW:
    # 1. First action (count, show) computes data
    # 2. Each executor stores its partitions in memory
    # 3. Future actions reuse cached data (no recomputation)
    #
    # STORAGE LEVELS:
    # • MEMORY_ONLY: Store in memory, drop if not enough space
    # • MEMORY_AND_DISK: Spill to disk if memory full
    # • DISK_ONLY: Store only on disk
    # • OFF_HEAP: Use off-heap memory (outside JVM)
    #
    # Cache is distributed - each executor stores its partitions locally.
    # ========================================================================
    print("\n2️⃣  Store Data Partitions in Memory")

    # .cache() marks DataFrame for caching (lazy)
    cached_df = df.cache()

    # .count() triggers computation AND caching
    cached_df.count()  # Materialize cache - data now stored on executors

    print("   Data cached on executors (each stores its partitions)")
    print("   Check Storage tab in Spark UI to see memory usage")
    print("   Future queries will reuse cached data ✅")

    # 3. Execute computations
    print("\n3️⃣  Execute Transformations")

    start = time.time()
    computed = (
        df.filter(col("id") > 1000)
        .withColumn("squared", col("id") * col("id"))
        .filter(col("squared") < 50000000)
    )

    result_count = computed.count()
    exec_time = time.time() - start

    print(f"   Executors processed {result_count} rows")
    print(f"   Execution time: {exec_time:.4f}s")
    print("   All computation done on executors, not driver")

    # 4. Shuffle read/write
    print("\n4️⃣  Handle Shuffle Operations")
    grouped = df.groupBy((col("id") % 10).alias("bucket")).count()
    grouped.show()

    print("   Executors wrote shuffle files")
    print("   Executors read shuffle files")
    print("   Check Stages tab for shuffle metrics")

    # 5. Report metrics
    print("\n5️⃣  Report Metrics to Driver")
    print("   • Task completion status")
    print("   • Shuffle bytes written/read")
    print("   • Records processed")
    print("   • Execution time")
    print("   • Memory usage")

    print("\n📊 EXECUTOR MEMORY USAGE:")
    print("   • Cached data partitions")
    print("   • Task execution memory")
    print("   • Shuffle buffers")
    print("   • Broadcast variable copies")

    cached_df.unpersist()


def demonstrate_driver_executor_communication():
    """
    Show communication patterns between driver and executors.
    """
    print("\n" + "=" * 80)
    print("DRIVER ↔ EXECUTOR COMMUNICATION")
    print("=" * 80)

    spark = create_spark()

    print("\n📡 COMMUNICATION FLOW:")
    print("=" * 80)

    df = spark.range(0, 10000).repartition(4)

    print("\n1️⃣  JOB SUBMISSION:")
    print("   Driver → Executors: 'Execute these tasks'")
    print("   ┌─────────┐         ┌──────────┐")
    print("   │ Driver  │ ------> │ Executor │")
    print("   └─────────┘         └──────────┘")
    print("              Task 0-3 (by partition)")

    print("\n2️⃣  TASK EXECUTION:")
    result = df.filter(col("id") > 5000).count()

    print("   Executors: Running tasks in parallel")
    print("   ┌──────────┐")
    print("   │ Executor │ Task 0: Process partition 0")
    print("   │ Executor │ Task 1: Process partition 1")
    print("   │ Executor │ Task 2: Process partition 2")
    print("   │ Executor │ Task 3: Process partition 3")
    print("   └──────────┘")

    print("\n3️⃣  RESULT COLLECTION:")
    print("   Executors → Driver: 'Task complete, here are results'")
    print("   ┌──────────┐         ┌─────────┐")
    print("   │ Executor │ ------> │ Driver  │")
    print("   └──────────┘         └─────────┘")
    print("           Task results: counts from each partition")

    print(f"\n   Driver aggregates: {result} rows total")

    print("\n4️⃣  SHUFFLE COMMUNICATION:")
    grouped = df.groupBy((col("id") % 3).alias("key")).count()
    grouped.show()

    print("   Executors ↔ Executors: Shuffle data exchange")
    print("   ┌──────────┐         ┌──────────┐")
    print("   │ Exec 1   │ <-----> │ Exec 2   │")
    print("   └──────────┘         └──────────┘")
    print("        ↕                    ↕")
    print("   ┌──────────┐         ┌──────────┐")
    print("   │ Exec 3   │ <-----> │ Exec 4   │")
    print("   └──────────┘         └──────────┘")
    print("   (via shuffle service or executor)")


def demonstrate_failure_handling():
    """
    Show how driver handles executor failures.
    """
    print("\n" + "=" * 80)
    print("FAILURE HANDLING")
    print("=" * 80)

    spark = create_spark()

    print("\n🔄 FAULT TOLERANCE:")
    print("=" * 80)

    print("\n1️⃣  Task Failure:")
    print("   • Executor fails during task")
    print("   • Driver detects failure")
    print("   • Driver reschedules task on different executor")
    print("   • Uses lineage (DAG) to recompute lost data")

    print("\n2️⃣  Executor Failure:")
    print("   • Executor process crashes")
    print("   • Driver marks executor as lost")
    print("   • Driver requests new executor from cluster manager")
    print("   • Reschedules all tasks from failed executor")

    print("\n3️⃣  Driver Failure:")
    print("   • ⚠️  Driver crash = entire application fails")
    print("   • No automatic recovery (single point of failure)")
    print("   • Solution: Use cluster mode + checkpointing")

    print("\n4️⃣  Shuffle Data Persistence:")
    df = spark.range(0, 10000).repartition(4)
    grouped = df.groupBy((col("id") % 5).alias("key")).count()
    grouped.show()

    print("   • Shuffle files written to disk")
    print("   • Survive executor failures")
    print("   • External shuffle service keeps data")


def demonstrate_resource_allocation():
    """
    Show resource allocation between driver and executors.
    """
    print("\n" + "=" * 80)
    print("RESOURCE ALLOCATION")
    print("=" * 80)

    spark = create_spark()

    print("\n💾 TYPICAL CLUSTER CONFIGURATION:")
    print("=" * 80)

    print(
        """
    ┌─────────────────────────────────────────────────────────┐
    │                    CLUSTER RESOURCES                     │
    ├─────────────────────────────────────────────────────────┤
    │                                                          │
    │  ┌──────────────┐                                       │
    │  │   DRIVER     │  (1 node)                             │
    │  │              │                                        │
    │  │  Memory: 2GB │  • Plans execution                    │
    │  │  Cores: 2    │  • Schedules tasks                    │
    │  │              │  • Collects results                   │
    │  └──────────────┘                                       │
    │         ↓                                                │
    │  ┌──────────────────────────────────────────────────┐  │
    │  │            EXECUTORS (Worker Nodes)              │  │
    │  ├──────────────────────────────────────────────────┤  │
    │  │  ┌────────────┐  ┌────────────┐  ┌────────────┐ │  │
    │  │  │ Executor 1 │  │ Executor 2 │  │ Executor 3 │ │  │
    │  │  │            │  │            │  │            │ │  │
    │  │  │ Memory: 8GB│  │ Memory: 8GB│  │ Memory: 8GB│ │  │
    │  │  │ Cores: 4   │  │ Cores: 4   │  │ Cores: 4   │ │  │
    │  │  │            │  │            │  │            │ │  │
    │  │  │ • Run tasks│  │ • Run tasks│  │ • Run tasks│ │  │
    │  │  │ • Store data│ │ • Store data│ │ • Store data│ │  │
    │  │  │ • Shuffle  │  │ • Shuffle  │  │ • Shuffle  │ │  │
    │  │  └────────────┘  └────────────┘  └────────────┘ │  │
    │  └──────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────┘
    
    Total Cluster:
    • Driver: 1 node × 2GB = 2GB
    • Executors: 3 nodes × 8GB = 24GB
    • Total: 26GB cluster memory
    • Parallelism: 3 executors × 4 cores = 12 concurrent tasks
    """
    )

    print("\n⚙️  CONFIGURATION PARAMETERS:")
    print("   --driver-memory 2g")
    print("   --executor-memory 8g")
    print("   --executor-cores 4")
    print("   --num-executors 3")


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("DRIVER AND EXECUTOR RESPONSIBILITIES")
    print("🎯" * 40)

    demonstrate_driver_responsibilities()
    demonstrate_executor_responsibilities()
    demonstrate_driver_executor_communication()
    demonstrate_failure_handling()
    demonstrate_resource_allocation()

    print("\n" + "=" * 80)
    print("✅ DRIVER/EXECUTOR DEMO COMPLETE")
    print("=" * 80)

    print("\n📚 Summary:")
    print("   DRIVER:")
    print("   ✅ Maintains SparkSession")
    print("   ✅ Builds execution plans")
    print("   ✅ Schedules jobs/stages/tasks")
    print("   ✅ Collects results")
    print("   ✅ Manages broadcasts")

    print("\n   EXECUTORS:")
    print("   ✅ Execute tasks on partitions")
    print("   ✅ Store cached data")
    print("   ✅ Perform computations")
    print("   ✅ Handle shuffles")
    print("   ✅ Report metrics")

    spark = SparkSession.builder.getOrCreate()
    spark.stop()


if __name__ == "__main__":
    main()
