#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
Spark Execution Architecture: Driver → Cluster Manager → Executor Interactions
================================================================================

MODULE OVERVIEW:
----------------
Complete demonstration of how Spark applications interact with cluster managers
(YARN, Kubernetes, Standalone) from submission through execution to completion.
This file shows the ENTIRE lifecycle of a Spark application including all
communication patterns between components.

PURPOSE:
--------
Understand the complete flow of a Spark application:
- Application submission process
- Cluster manager negotiations
- Resource allocation and executor launch
- Task distribution and execution
- Result collection and cleanup
- Failure handling and recovery

TARGET AUDIENCE:
----------------
- Data engineers deploying Spark on clusters
- DevOps engineers managing Spark infrastructure
- Developers debugging cluster-related issues
- Anyone needing to understand Spark's cluster integration

THE COMPLETE SPARK ARCHITECTURE:
=================================

    USER MACHINE (Your Laptop or Edge Node)
         ↓
    [spark-submit]  ← User submits application JAR/Python script
         ↓
    ┌─────────────────────────────────────────────────────────────┐
    │                    CLUSTER MANAGER                          │
    │  (YARN / Kubernetes / Mesos / Standalone)                   │
    │                                                             │
    │  Responsibilities:                                          │
    │  • Accept application submissions                           │
    │  • Allocate resources (CPU, memory)                         │
    │  • Launch driver (cluster mode) or accept driver connection │
    │  • Launch executors on worker nodes                         │
    │  • Monitor resource usage                                   │
    │  • Handle node failures                                     │
    └─────────────────────────────────────────────────────────────┘
              ↓                           ↓
         [DRIVER]                    [EXECUTORS]
    (Application Master              (Task execution)
     in YARN terms)
              ↓                           ↓
    • Plans execution              • Run tasks
    • Schedules tasks              • Store data
    • Coordinates work             • Report status
    • Collects results             • Execute UDFs

CRITICAL CONCEPT: HOW PHYSICAL MACHINES ARE USED
=================================================

PHYSICAL CLUSTER SETUP:
-----------------------
Imagine you have a cluster with 5 physical machines (nodes):

Node 1: Master Node (ResourceManager in YARN / Master in Standalone)
  - Runs cluster manager software
  - Does NOT run Spark computations
  - Just coordinates resources
  - Specs: 4 cores, 16GB RAM

Node 2-5: Worker Nodes (4 machines for actual work)
  - Each has: 16 cores, 64GB RAM
  - Run executors (Spark JVM processes)
  - Store data partitions in memory/disk
  - Execute tasks in parallel

EXECUTOR PLACEMENT ON PHYSICAL MACHINES:
-----------------------------------------
Q: "Does 1 PC get several executors?"
A: YES! Common pattern:

Worker Node 2 (64GB, 16 cores):
  ├─ Executor 1: 8GB memory, 4 cores
  ├─ Executor 2: 8GB memory, 4 cores
  ├─ Executor 3: 8GB memory, 4 cores
  └─ Executor 4: 8GB memory, 4 cores
  (Leaves ~32GB for OS and other processes)

Worker Node 3 (64GB, 16 cores):
  ├─ Executor 5: 8GB memory, 4 cores
  ├─ Executor 6: 8GB memory, 4 cores
  ├─ Executor 7: 8GB memory, 4 cores
  └─ Executor 8: 8GB memory, 4 cores

... and so on for Worker Nodes 4 and 5

RESULT: 16 executors across 4 physical machines
  - Each machine runs 4 executors
  - Each executor is a separate JVM process
  - Executors share the machine's CPU and memory

WHY MULTIPLE EXECUTORS PER MACHINE?
------------------------------------
1. Better parallelism: Each executor can run multiple tasks
2. Fault isolation: One executor crash doesn't kill others
3. Memory management: Smaller heap = less GC overhead
4. Resource sharing: Better utilization of physical resources

PARTITION DISTRIBUTION ACROSS EXECUTORS:
-----------------------------------------
Q: "Does 1 PC or cloud node get several partitions?"
A: YES! Here's how:

Imagine dataset with 200 partitions, 16 executors:

Worker Node 2 (4 executors):
  Executor 1: Holds partitions [0, 16, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192]
  Executor 2: Holds partitions [1, 17, 33, 49, 65, 81, 97, 113, 129, 145, 161, 177, 193]
  Executor 3: Holds partitions [2, 18, 34, 50, 66, 82, 98, 114, 130, 146, 162, 178, 194]
  Executor 4: Holds partitions [3, 19, 35, 51, 67, 83, 99, 115, 131, 147, 163, 179, 195]

Worker Node 3 (4 executors):
  Executor 5: Holds partitions [4, 20, 36, 52, 68, 84, 100, 116, 132, 148, 164, 180, 196]
  Executor 6: Holds partitions [5, 21, 37, 53, 69, 85, 101, 117, 133, 149, 165, 181, 197]
  ... and so on

RESULT: Each physical machine holds ~50 partitions spread across 4 executors
  - Partitions distributed evenly
  - Round-robin or hash-based distribution
  - Goal: Balance load across all machines

SPARK DEPLOYMENT MODES:
=======================

1. CLIENT MODE:
   ┌──────────────┐         ┌──────────────────┐
   │ User Machine │         │  Cluster Manager │
   │              │         │                  │
   │  [Driver] ◄──┼─────────┤  [Executors]     │
   │              │  Network│                  │
   └──────────────┘         └──────────────────┘
   
   • Driver runs on client machine (where you submit)
   • Good for: Interactive work, development, debugging
   • Bad for: Production (network latency, client must stay up)
   • Use case: spark-shell, notebooks, testing

2. CLUSTER MODE:
   ┌──────────────┐         ┌──────────────────────────────┐
   │ User Machine │         │    Cluster Manager           │
   │              │         │                              │
   │ spark-submit ├────────►│  [Driver] + [Executors]      │
   │  (detached)  │         │   (both run in cluster)      │
   └──────────────┘         └──────────────────────────────┘
   
   • Driver runs inside cluster (on a worker node)
   • Good for: Production, long-running jobs, reliability
   • Bad for: Interactive debugging (no direct access to driver)
   • Use case: Production batch jobs, scheduled workflows

CLUSTER MANAGERS COMPARISON:
=============================

YARN (Hadoop YARN - Yet Another Resource Negotiator):
------------------------------------------------------
Most common in enterprise Hadoop environments

Components:
• ResourceManager (RM): Central authority for resource allocation
• NodeManager (NM): Per-node agent managing containers
• ApplicationMaster (AM): Per-app coordinator (Spark Driver in cluster mode)

Flow:
1. Client submits to ResourceManager
2. RM allocates container for ApplicationMaster
3. AM (Spark Driver) requests executor containers
4. NMs launch executors
5. Executors register with Driver

Pros:
✅ Mature, stable, well-tested
✅ Good multi-tenancy support
✅ Queue-based resource management
✅ Integrates with Hadoop ecosystem

Cons:
❌ Complex setup and configuration
❌ Slower allocation than Kubernetes
❌ Tied to Hadoop ecosystem

KUBERNETES (K8s):
-----------------
Modern container orchestration platform

Components:
• API Server: Central control plane
• Scheduler: Assigns pods to nodes
• Kubelet: Per-node agent managing pods
• Pod: Container wrapper (Driver pod + Executor pods)

Flow:
1. Client submits to K8s API Server
2. Scheduler creates Driver pod
3. Driver requests Executor pods via API
4. Kubelets launch Executor pods
5. Executors register with Driver

Pros:
✅ Cloud-native, modern
✅ Fast resource allocation
✅ Auto-scaling support
✅ Better isolation (containers)
✅ Declarative configuration

Cons:
❌ Less mature for Spark
❌ More complex networking
❌ Requires K8s expertise

STANDALONE MODE:
----------------
Spark's built-in cluster manager (simplest)

Components:
• Master: Central coordinator
• Worker: Per-node executor launcher

Flow:
1. Client submits to Master
2. Master allocates resources on Workers
3. Workers launch Executors
4. Executors register with Driver

Pros:
✅ Simple setup (just Spark, no Hadoop/K8s)
✅ Fast allocation
✅ Easy to understand

Cons:
❌ Limited features (no multi-tenancy)
❌ No resource isolation
❌ Single point of failure (Master)

PARTITIONING DEEP DIVE - HOW DATA IS DIVIDED:
==============================================

WHAT IS A PARTITION?
--------------------
A partition is a logical chunk of data that:
• Resides on a single executor
• Is processed by a single task
• Can be cached in memory
• Can be stored on disk
• Is the unit of parallelism

Think of it like slicing a pizza:
  Total Dataset (Pizza) = 1GB
  Number of Partitions (Slices) = 8
  Each Partition (Slice) = ~125MB

HOW ARE PARTITIONS CREATED AND DIVIDED?
----------------------------------------

1. READING FROM FILES (Most Common):
   ------------------------------------
   When reading from HDFS/S3/etc:
   
   Example: Reading 1GB CSV file
   
   spark.read.csv("s3://bucket/data.csv")
   
   WHAT HAPPENS:
   Step 1: Driver queries data source metadata
     - File size: 1GB
     - Block size: 128MB (HDFS default)
     - Number of blocks: 1024MB / 128MB = 8 blocks
   
   Step 2: Driver creates 1 partition per block
     - Partition 0: Bytes 0 to 134,217,728 (128MB)
     - Partition 1: Bytes 134,217,728 to 268,435,456
     - Partition 2: Bytes 268,435,456 to 402,653,184
     - ... and so on for 8 partitions total
   
   Step 3: Driver assigns partitions to executors
     - Executor 1 gets partitions [0, 4]
     - Executor 2 gets partitions [1, 5]
     - Executor 3 gets partitions [2, 6]
     - Executor 4 gets partitions [3, 7]
   
   RESULT: 8 partitions distributed across 4 executors
   
   DATA LOCALITY OPTIMIZATION:
   ---------------------------
   HDFS stores blocks with 3x replication across nodes:
   Block 0 replicas: [node-1, node-3, node-5]
   Block 1 replicas: [node-2, node-4, node-1]
   
   Driver tries to schedule tasks on nodes that already have the data:
   - PROCESS_LOCAL: Data in executor memory (best)
   - NODE_LOCAL: Data on same machine's disk (good)
   - RACK_LOCAL: Data on same rack (okay)
   - ANY: Data on different rack (requires network transfer - slow)

2. READING FROM DATABASES (JDBC):
   --------------------------------
   spark.read.jdbc(url, table, 
                   column="id", 
                   lowerBound=0, 
                   upperBound=1000000,
                   numPartitions=10)
   
   WHAT HAPPENS:
   Step 1: Driver divides range into equal chunks
     - Total range: 1,000,000 IDs
     - Number of partitions: 10
     - Chunk size: 1,000,000 / 10 = 100,000 per partition
   
   Step 2: Driver creates SQL queries for each partition
     Partition 0: SELECT * FROM table WHERE id >= 0 AND id < 100000
     Partition 1: SELECT * FROM table WHERE id >= 100000 AND id < 200000
     Partition 2: SELECT * FROM table WHERE id >= 200000 AND id < 300000
     ... and so on
   
   Step 3: Each executor opens separate connection to database
     - Executor 1 runs query for partitions [0, 5]
     - Executor 2 runs query for partitions [1, 6]
     - Executor 3 runs query for partitions [2, 7]
     - Executor 4 runs query for partitions [3, 8]
     - Executor 1 runs query for partitions [4, 9]
   
   RESULT: 10 parallel queries to database, each fetching 100K rows

3. CREATING DATA WITH range():
   ----------------------------
   df = spark.range(0, 1000000, numPartitions=8)
   
   WHAT HAPPENS:
   Step 1: Driver calculates partition boundaries
     - Total numbers: 1,000,000 (0 to 999,999)
     - Number of partitions: 8
     - Numbers per partition: 1,000,000 / 8 = 125,000
   
   Step 2: Driver creates partition specifications
     Partition 0: Generate numbers [0 to 124,999]
     Partition 1: Generate numbers [125,000 to 249,999]
     Partition 2: Generate numbers [250,000 to 374,999]
     ... and so on
   
   Step 3: Executors generate data locally (no network transfer!)
     - Each executor generates its assigned range
     - Data created in memory on executor
     - No shuffling or data movement needed

4. REPARTITIONING (Changing Partition Count):
   -------------------------------------------
   df_new = df.repartition(16)  # Increase from 8 to 16
   
   WHAT HAPPENS:
   Step 1: Shuffle! (Expensive operation)
     - All data redistributed across network
     - Each record assigned to new partition
     - Uses hash function: partition = hash(record) % 16
   
   Step 2: Driver creates shuffle tasks
     - Map stage: Each old partition writes data to 16 files
       (1 file per new partition)
     - Reduce stage: Each new partition reads its files from
       all map tasks
   
   Example with 8 old partitions, 16 new partitions:
     Old Partition 0 writes 16 files (one per new partition)
     Old Partition 1 writes 16 files
     ... 8 old partitions × 16 files = 128 shuffle files
     
     New Partition 0 reads file-0 from all 8 old partitions
     New Partition 1 reads file-1 from all 8 old partitions
     ... and so on
   
   RESULT: All data reshuffled across network
   
   COST: High! Involves:
     - Disk writes (128 files)
     - Network transfer (all data)
     - Disk reads (by new partitions)

5. COALESCE (Reducing Partitions WITHOUT Shuffle):
   ------------------------------------------------
   df_new = df.coalesce(4)  # Reduce from 8 to 4
   
   WHAT HAPPENS:
   Step 1: NO shuffle! Just merge partitions
     - Partition 0 + Partition 4 → New Partition 0
     - Partition 1 + Partition 5 → New Partition 1
     - Partition 2 + Partition 6 → New Partition 2
     - Partition 3 + Partition 7 → New Partition 3
   
   Step 2: Data stays on same executors
     - No network transfer
     - No disk writes
     - Just logical combination
   
   RESULT: Fast! No shuffling
   LIMITATION: Can only reduce, not increase partitions

PARTITION CONFIGURATION PARAMETERS:
====================================

spark.default.parallelism
--------------------------
Default number of partitions for RDDs from transformations like:
  - parallelize()
  - union()
  - coalesce()

Default value: 
  - Local mode: Number of cores on local machine
  - Cluster mode: max(2, total cores in cluster)

Example:
  conf.set("spark.default.parallelism", "200")

spark.sql.shuffle.partitions
-----------------------------
Number of partitions for shuffles in SQL/DataFrame operations:
  - groupBy()
  - join()
  - repartition()
  - window functions

Default value: 200

Example:
  conf.set("spark.sql.shuffle.partitions", "100")

When to change:
  - Small data (< 1GB): Set to 50-100
  - Medium data (1-10GB): Set to 100-200
  - Large data (> 10GB): Set to 200-500
  - Very large data (> 100GB): Set to 500-2000

Rule of thumb:
  - Each partition should be 100-200MB
  - Total partitions = Total data size / 128MB
  - Partitions should be 2-3x number of total cores

spark.sql.files.maxPartitionBytes
----------------------------------
Maximum bytes per partition when reading files.

Default: 128MB (134,217,728 bytes)

Example:
  conf.set("spark.sql.files.maxPartitionBytes", "67108864")  # 64MB

Smaller partitions:
  ✅ More parallelism
  ✅ Better for skewed data
  ❌ More task overhead
  ❌ More shuffle files

Larger partitions:
  ✅ Less task overhead
  ✅ Fewer shuffle files
  ❌ Less parallelism
  ❌ Risk of OOM if partition too large

PARTITION SKEW - THE #1 PERFORMANCE KILLER:
============================================

WHAT IS DATA SKEW?
------------------
Uneven distribution of data across partitions.

Example: groupBy('country') on user data
  Partition 0 (USA): 10,000,000 records → Takes 10 minutes
  Partition 1 (Canada): 1,000,000 records → Takes 1 minute
  Partition 2 (UK): 800,000 records → Takes 48 seconds
  Partition 3 (Australia): 500,000 records → Takes 30 seconds

PROBLEM: Job takes 10 minutes (limited by slowest partition)
Even though 3 partitions finish quickly!

HOW TO DETECT SKEW:
-------------------
1. Check Spark UI → Stages → Tasks
   - Look for tasks with much longer duration
   - Look for tasks processing more data

2. Check partition sizes:
   df.groupBy(spark_partition_id()).count().show()

HOW TO FIX SKEW:
----------------
1. Salting (Add random key):
   # SALTING TECHNIQUE: Distribute hot keys across multiple partitions
   # ================================================================
   # Problem: "USA" key has 99% of data → One executor is overwhelmed
   # Solution: Split "USA" into "USA_0", "USA_1", ..., "USA_9"
   #
   # Step 1: Add random salt (0-9)
   df = df.withColumn("salt", (rand() * 10).cast("int"))
   #   rand() generates [0.0, 1.0) → multiply by 10 → [0.0, 10.0)
   #   cast to int → {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
   #   Each row randomly assigned a salt value
   #
   # Step 2: Partial aggregation WITH salt (distributes work)
   df = df.groupBy("country", "salt").agg(...)
   #   "USA" now processed in 10 parallel partitions!
   #   "USA_0", "USA_1", ..., "USA_9" each on different executor
   #
   # Step 3: Final aggregation WITHOUT salt (combines results)
   df = df.groupBy("country").agg(...)  # Recombines USA_0 + USA_1 + ... = USA
   #
   # Performance Impact:
   #   Before: 1 executor does 99% of work, others idle
   #   After: 10 executors share work evenly → 10x faster!
   #
   # Trade-off: Two-stage aggregation required (vs one-stage without salt)

2. Adaptive Query Execution (AQE) - Spark 3.0+:
   spark.conf.set("spark.sql.adaptive.enabled", "true")
   spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
   
   AQE automatically:
   - Detects skewed partitions
   - Splits them into smaller partitions
   - Reoptimizes join strategies

3. Custom partitioning:
   df.repartition("country", "region")  # Partition by 2 columns

CACHED PARTITIONS - HOW CACHING WORKS:
=======================================

WHAT HAPPENS WHEN YOU CACHE:
-----------------------------
df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)

Step 1: Marking for Cache
  - DataFrame marked as "to be cached"
  - No data actually cached yet (lazy!)
  - Just sets a flag in metadata

Step 2: First Action Triggers Materialization
  df.count()  # This triggers cache
  
  WHAT HAPPENS ON EACH EXECUTOR:
  
  Executor 1 (holds partitions [0, 4]):
    1. Computes partition 0
    2. Stores result in Storage Memory (40% of heap)
    3. Computes partition 4
    4. Stores result in Storage Memory
    5. If memory full: Spills to disk or evicts old data (LRU)
  
  Executor 2 (holds partitions [1, 5]):
    1. Computes partition 1
    2. Stores in memory
    3. Computes partition 5
    4. Stores in memory
  
  ... and so on for all executors

Step 3: Subsequent Actions Use Cache
  df.filter(...).count()  # Reads from cache!
  
  Executor 1:
    1. Reads partition 0 from memory (fast!)
    2. Applies filter
    3. Counts
    4. Reads partition 4 from memory
    5. Applies filter
    6. Counts

WHERE IS CACHED DATA STORED?
-----------------------------
Location depends on StorageLevel:

MEMORY_ONLY (default for df.cache()):
  - Stored in executor JVM heap
  - Storage Memory region (40% of heap by default)
  - Deserialized objects (faster access)
  - Evicted if memory full (recomputed on next access)

MEMORY_ONLY_SER:
  - Stored in executor JVM heap
  - Serialized bytes (saves space, ~5x compression)
  - Slightly slower access (deserialization needed)
  - Fits 5x more data in same memory

MEMORY_AND_DISK:
  - First tries memory
  - If memory full, spills to disk
  - Disk location: spark.local.dir (temp directory)
  - File format: Blocks stored as files

DISK_ONLY:
  - Directly to disk
  - No memory used
  - Slower than memory (disk I/O)

OFF_HEAP:
  - Stored outside JVM heap
  - Requires spark.memory.offHeap.enabled=true
  - Avoids GC overhead
  - Useful for large caches

MEMORY LAYOUT ON EXECUTOR:
---------------------------
Total Executor Memory (e.g., 4GB):
├─ Reserved Memory (300MB)
│  └─ Internal Spark overhead
├─ Execution Memory (60% of remaining = 2.22GB)
│  ├─ Shuffles
│  ├─ Joins
│  ├─ Sorts
│  └─ Aggregations
└─ Storage Memory (40% of remaining = 1.48GB)
   ├─ Cached RDDs/DataFrames ← YOUR CACHED DATA HERE
   ├─ Broadcast variables
   └─ Temporary blocks

CACHE EVICTION POLICY:
-----------------------
When Storage Memory fills up:
1. LRU (Least Recently Used) eviction
2. Oldest cached partition removed first
3. Evicted data recomputed on next access
4. Only evicts entire partitions (not partial)

CACHE MANAGEMENT:
-----------------
df.unpersist()  # Remove from cache
  - Frees memory immediately
  - Next access will recompute
  - Good practice when done with DataFrame

spark.catalog.clearCache()  # Clear all caches
  - Nuclear option
  - Frees all cached data
  - Use when memory is critical

APPLICATION LIFECYCLE - DETAILED:
==================================

PHASE 1: SUBMISSION
-------------------
Command: spark-submit --master yarn --deploy-mode cluster my_app.py

Step 1: Parse Command Line
  • spark-submit reads arguments
  • Validates application files exist
  • Determines cluster manager type
  • Chooses deploy mode (client vs cluster)

Step 2: Package Application
  • Bundles Python files
  • Uploads to distributed storage (HDFS/S3)
  • Creates application JAR (if Scala/Java)

Step 3: Contact Cluster Manager
  • Opens connection to cluster manager
  • Submits application request
  • Includes: resource requirements, files, configs

PHASE 2: DRIVER LAUNCH
----------------------
CLIENT MODE:
  • Driver starts immediately on client machine
  • SparkContext initialized locally
  • Connects to cluster manager
  • Requests executor resources

CLUSTER MODE:
  Step 1: Cluster Manager Allocates Driver Container
    • YARN: Allocates ApplicationMaster container
    • K8s: Creates Driver pod
    • Standalone: Allocates on Worker node
  
  Step 2: Driver Process Starts
    • JVM starts with driver memory
    • Loads application code
    • Initializes SparkContext
    • Registers with cluster manager
  
  Step 3: Driver Requests Executors
    • Calculates: num_executors × (executor_memory + executor_cores)
    • Sends request to cluster manager
    • Waits for executor allocation

PHASE 3: EXECUTOR LAUNCH
-------------------------
Step 1: Cluster Manager Allocates Resources
  YARN:
    • ResourceManager checks queue capacity
    • Allocates containers on NodeManagers
    • Considers data locality preferences
  
  K8s:
    • Scheduler finds nodes with capacity
    • Creates Executor pods
    • Assigns to nodes via kubelet
  
  Standalone:
    • Master allocates executor slots on Workers
    • Workers launch executor processes

Step 2: Executors Start
  • JVM starts with executor memory
  • Initializes executor backend
  • Connects to Driver
  • Sends registration message

Step 3: Driver Acknowledges Executors
  • Receives executor registration
  • Adds executor to available resources
  • Marks executor as ready for tasks
  • Updates task scheduler

PHASE 4: TASK EXECUTION
------------------------
Step 1: Action Triggers Job
  • User code calls action (count(), collect(), save())
  • Driver creates job
  • Analyzes dependencies (DAG)

Step 2: Driver Creates Stages & Tasks
  • Splits DAG at shuffle boundaries (stages)
  • Creates tasks (1 per partition per stage)
  • Considers data locality

Step 3: Driver Schedules Tasks
  • Assigns tasks to executors
  • Serializes task code + closure
  • Sends tasks via RPC

Step 4: Executors Run Tasks
  • Deserialize task
  • Execute on partition data
  • Store intermediate results
  • Report progress to driver

Step 5: Shuffle (if needed)
  • Executors write shuffle data to disk
  • Executors fetch shuffle data from peers
  • No driver involvement in data transfer

Step 6: Driver Collects Results
  • Executors send results to driver
  • Driver aggregates final result
  • Returns to user code

PHASE 5: APPLICATION COMPLETION
--------------------------------
Normal Completion:
  Step 1: Driver Calls stop()
    • SparkContext.stop() invoked
    • Sends shutdown to executors
  
  Step 2: Executors Shutdown
    • Flush cached data
    • Close connections
    • Exit process
  
  Step 3: Driver Unregisters
    • Notifies cluster manager
    • Releases resources
    • Exits

Failure:
  • Driver failure → entire app fails
  • Executor failure → tasks rescheduled
  • Cluster manager failure → varies by manager

CONFIGURATION PARAMETERS:
==========================

DRIVER CONFIGURATION:
---------------------
spark.driver.memory = 2g              # Driver heap size
spark.driver.cores = 1                # Driver CPU cores
spark.driver.maxResultSize = 1g       # Max result size from executors

EXECUTOR CONFIGURATION:
-----------------------
spark.executor.memory = 4g            # Executor heap size
spark.executor.cores = 4              # Cores per executor
spark.executor.instances = 10         # Number of executors
spark.executor.memoryOverhead = 512m  # Off-heap memory

CLUSTER MANAGER SPECIFIC:
-------------------------
# YARN
spark.yarn.queue = default            # YARN queue name
spark.yarn.am.memory = 512m           # ApplicationMaster memory
spark.yarn.principal = user@REALM     # Kerberos principal
spark.yarn.keytab = /path/to/key      # Kerberos keytab

# Kubernetes
spark.kubernetes.namespace = spark    # K8s namespace
spark.kubernetes.driver.pod.name      # Driver pod name
spark.kubernetes.executor.request.cores # CPU request

# Standalone
spark.cores.max = 40                  # Max cores across cluster

DYNAMIC ALLOCATION:
-------------------
spark.dynamicAllocation.enabled = true
spark.dynamicAllocation.minExecutors = 2
spark.dynamicAllocation.maxExecutors = 100
spark.dynamicAllocation.initialExecutors = 10

USAGE:
------
Run this file to see demonstrations:
    $ python3 03_driver_yarn_cluster_interaction.py

Or submit to cluster:
    $ spark-submit --master yarn --deploy-mode cluster \\
        03_driver_yarn_cluster_interaction.py

RELATED RESOURCES:
------------------
- YARN: https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html
- Kubernetes: https://spark.apache.org/docs/latest/running-on-kubernetes.html
- Standalone: https://spark.apache.org/docs/latest/spark-standalone.html
- Cluster Mode Overview: https://spark.apache.org/docs/latest/cluster-overview.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0
CREATED: December 13, 2025

================================================================================
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import os
import sys
import time
from datetime import datetime

from pyspark import SparkConf

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def print_section(title):
    """
    Print a formatted section header.

    Args:
        title (str): Section title to display
    """
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def print_subsection(title):
    """
    Print a formatted subsection header.

    Args:
        title (str): Subsection title to display
    """
    print(f"\n{'─' * 80}")
    print(f"  {title}")
    print("─" * 80)


def create_spark_session():
    """
    Create SparkSession with detailed configuration.

    WHAT HAPPENS WHEN THIS IS CALLED:
    ----------------------------------
    1. SparkConf Creation:
       - Configuration object created
       - Settings parsed and validated
       - Defaults applied for missing values

    2. Cluster Manager Detection:
       - Reads spark.master configuration
       - Determines: local, yarn, k8s, spark://
       - Initializes appropriate backend

    3. Driver Initialization (if cluster mode):
       - Cluster manager allocates container/pod
       - Driver JVM starts
       - SparkContext initializes
       - Driver registers with cluster manager

    4. Executor Request:
       - Driver calculates resource needs
       - Sends request to cluster manager
       - Waits for executor allocation
       - Executors register back to driver

    5. SparkSession Ready:
       - All executors connected
       - Ready to accept jobs
       - Task scheduler initialized

    CONFIGURATION EXPLAINED:
    ------------------------
    • appName: Application name (shown in cluster manager UI)
    • master: Cluster manager URL
      - "local[*]": Run locally with all CPU cores
      - "yarn": Submit to YARN cluster
      - "k8s://https://k8s-api:443": Submit to Kubernetes
      - "spark://master:7077": Submit to Standalone cluster

    • spark.submit.deployMode:
      - "client": Driver runs on submit machine
      - "cluster": Driver runs in cluster

    • spark.executor.instances: Number of executors to launch
    • spark.executor.memory: Memory per executor
    • spark.executor.cores: CPU cores per executor

    Returns:
    --------
    SparkSession: Configured and ready Spark session
    """
    print_subsection("Creating SparkSession - Driver Initialization")

    # ========================================================================
    # STEP 1: CREATE SPARK CONFIGURATION
    # ========================================================================
    # WHAT HAPPENS:
    # - SparkConf object created
    # - All configuration key-value pairs stored
    # - Validation performed (memory size, core count, etc.)
    # - Defaults filled in for missing configurations
    # ========================================================================
    print("📝 Step 1: Creating Spark configuration...")

    conf = SparkConf()
    conf.setAppName("DriverYarnClusterInteractionDemo")
    conf.setMaster("local[4]")  # Local mode for demo (use "yarn" for cluster)

    # Executor configuration
    conf.set("spark.executor.instances", "3")  # Request 3 executors
    conf.set("spark.executor.memory", "1g")  # 1GB per executor
    conf.set("spark.executor.cores", "2")  # 2 cores per executor

    # Driver configuration
    conf.set("spark.driver.memory", "1g")  # Driver memory
    conf.set("spark.driver.cores", "2")  # Driver cores

    # Dynamic allocation (commented for demo)
    # conf.set("spark.dynamicAllocation.enabled", "true")
    # conf.set("spark.dynamicAllocation.minExecutors", "2")
    # conf.set("spark.dynamicAllocation.maxExecutors", "10")

    print("   ✅ Configuration created")
    print(f"   • App Name: DriverYarnClusterInteractionDemo")
    print(f"   • Master: local[4] (simulates cluster)")
    print(f"   • Executors: 3 × 2 cores × 1GB = 6 cores, 3GB total")

    # ========================================================================
    # STEP 2: INITIALIZE SPARKSESSION
    # ========================================================================
    # WHAT HAPPENS IN CLIENT MODE:
    # 1. Driver process starts on this machine
    # 2. SparkContext initialized
    # 3. Connection to cluster manager established
    # 4. Executor request sent to cluster manager
    # 5. Cluster manager allocates executors
    # 6. Executors start and register with driver
    #
    # WHAT HAPPENS IN CLUSTER MODE (YARN example):
    # 1. spark-submit uploads code to HDFS
    # 2. YARN ResourceManager allocates ApplicationMaster container
    # 3. ApplicationMaster (Driver) starts in container
    # 4. Driver initializes SparkContext
    # 5. Driver requests executor containers from ResourceManager
    # 6. YARN NodeManagers launch executor containers
    # 7. Executors connect back to Driver
    #
    # COMMUNICATION FLOW:
    # User → Cluster Manager: "Launch my app with these resources"
    # Cluster Manager → Driver: "Here's your container/pod"
    # Driver → Cluster Manager: "I need N executors"
    # Cluster Manager → Executors: "Launch on these nodes"
    # Executors → Driver: "We're ready, send tasks"
    # ========================================================================
    print("\n📝 Step 2: Initializing SparkSession...")
    print("   This is where driver communicates with cluster manager!\n")

    print("   Communication Flow:")
    print("   ┌─────────────┐       ┌──────────────────┐       ┌─────────────┐")
    print("   │   Driver    │──────►│ Cluster Manager  │──────►│  Executors  │")
    print("   │ (this code) │◄──────│  (YARN/K8s/etc)  │◄──────│ (workers)   │")
    print("   └─────────────┘       └──────────────────┘       └─────────────┘")
    print("        │                         │                         │")
    print("        │  1. Request resources   │                         │")
    print("        │─────────────────────────►                         │")
    print("        │                         │  2. Allocate & launch   │")
    print("        │                         │─────────────────────────►")
    print("        │                         │  3. Register with driver│")
    print("        │◄─────────────────────────────────────────────────┤")
    print("        │  4. Send tasks          │                         │")
    print("        │──────────────────────────────────────────────────►")

    # Create the SparkSession
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    print("\n   ✅ SparkSession created!")
    print(f"   • Spark Version: {spark.version}")
    print(f"   • Master: {spark.sparkContext.master}")
    print(f"   • App ID: {spark.sparkContext.applicationId}")
    print(f"   • Default Parallelism: {spark.sparkContext.defaultParallelism}")

    # ========================================================================
    # STEP 3: VERIFY EXECUTOR CONNECTION
    # ========================================================================
    # WHAT HAPPENS:
    # - Query SparkContext for active executors
    # - In local mode: Executors are threads in same JVM
    # - In cluster mode: Executors are separate processes on worker nodes
    # ========================================================================
    print("\n📝 Step 3: Verifying executor connections...")

    # Get executor information (only available in cluster mode properly)
    print("   ℹ️  In local mode, executors run as threads in same JVM")
    print("   ℹ️  In cluster mode, you'd see actual node hostnames here")
    print("   ℹ️  Check Spark UI at http://localhost:4040 for executor details")

    return spark


# ============================================================================
# PHASE 1: APPLICATION SUBMISSION SIMULATION
# ============================================================================


def demonstrate_application_submission():
    """
    Demonstrate what happens during spark-submit.
    
    WHAT THIS SHOWS:
    ----------------
    The complete flow from running spark-submit command through
    driver initialization, including all cluster manager interactions.
    
    SPARK-SUBMIT COMMAND ANATOMY:
    ------------------------------
    spark-submit \\
        --master yarn \\                    # Cluster manager type
        --deploy-mode cluster \\            # Where driver runs
        --num-executors 10 \\               # Number of executors
        --executor-memory 4G \\             # Memory per executor
        --executor-cores 4 \\               # Cores per executor
        --driver-memory 2G \\               # Driver memory
        --driver-cores 2 \\                 # Driver cores
        --queue production \\               # YARN queue
        --conf spark.dynamicAllocation.enabled=true \\
        --py-files dependencies.zip \\      # Python dependencies
        my_application.py                   # Application code
    
    SUBMISSION FLOW:
    ----------------
    1. Parse Arguments:
       - Validate all configuration parameters
       - Check file existence
       - Set defaults for missing values
    
    2. Package Application:
       - Collect Python files
       - Create archive (if needed)
       - Upload to distributed storage (HDFS/S3)
    
    3. Contact Cluster Manager:
       - Establish connection (HTTP/RPC)
       - Submit application request
       - Provide resource requirements
    
    4. Cluster Manager Response:
       - Allocates resources
       - Returns application ID
       - Provides tracking URL
    
    5. Monitor Progress:
       - Polls for status updates
       - Streams logs (if requested)
       - Waits for completion (if --wait)
    """
    print_section("PHASE 1: APPLICATION SUBMISSION")

    print("Command: spark-submit --master yarn --deploy-mode cluster app.py\n")

    # ========================================================================
    # STEP 1: COMMAND LINE PARSING
    # ========================================================================
    # WHAT HAPPENS:
    # - spark-submit script reads command line arguments
    # - Validates required parameters present
    # - Checks master URL format
    # - Verifies application file exists
    # - Loads spark-defaults.conf if present
    # - Merges command line args with defaults
    # ========================================================================
    print("Step 1️⃣: Parsing command line arguments")
    print("   ✅ Validated --master yarn")
    print("   ✅ Validated --deploy-mode cluster")
    print("   ✅ Found application file: app.py")
    print("   ✅ Loaded spark-defaults.conf")
    print("   ✅ Merged configurations\n")

    # ========================================================================
    # STEP 2: PACKAGE APPLICATION
    # ========================================================================
    # WHAT HAPPENS:
    # For Python:
    #   - Collects .py files from --py-files
    #   - Creates ZIP archive
    #   - Uploads to HDFS staging directory
    #   - Format: hdfs:///user/<user>/.sparkStaging/<app-id>/
    #
    # For Java/Scala:
    #   - Application JAR already compiled
    #   - Uploads JAR to HDFS
    #   - Includes dependency JARs
    #
    # WHY:
    #   - Cluster nodes need access to code
    #   - HDFS provides distributed access
    #   - All executors can download from HDFS
    # ========================================================================
    print("Step 2️⃣: Packaging application for distribution")
    print("   📦 Creating application archive...")
    print("   ✅ Collected Python files: app.py, utils.py, config.py")
    print("   ✅ Created archive: application.zip")
    print("   ✅ Uploaded to HDFS: hdfs:///user/spark/.sparkStaging/app-123/")
    print("   ℹ️  All cluster nodes can now access the code\n")

    # ========================================================================
    # STEP 3: CONTACT YARN RESOURCEMANAGER
    # ========================================================================
    # WHAT HAPPENS:
    # 1. spark-submit opens connection to ResourceManager
    #    - URL from --master yarn (reads from yarn-site.xml)
    #    - Typically: http://resourcemanager:8032
    #
    # 2. Submits ApplicationSubmissionContext:
    #    - Application name
    #    - Queue name
    #    - Resource requirements (memory, cores)
    #    - ApplicationMaster (driver) specifications
    #    - Environment variables
    #    - HDFS paths to application files
    #
    # 3. ResourceManager validates request:
    #    - Check queue exists and has capacity
    #    - Verify user has permissions
    #    - Ensure resources available
    #
    # 4. ResourceManager accepts application:
    #    - Assigns application ID
    #    - Returns tracking URL
    #    - Queues application for scheduling
    # ========================================================================
    print("Step 3️⃣: Contacting YARN ResourceManager")
    print("   🔗 Connecting to ResourceManager: http://rm-host:8032")
    print("   📤 Sending ApplicationSubmissionContext:")
    print("      • Application Name: DriverYarnClusterInteractionDemo")
    print("      • Queue: default")
    print("      • ApplicationMaster Memory: 2GB")
    print("      • ApplicationMaster Cores: 2")
    print("      • Executor Count: 10")
    print("      • Executor Memory: 4GB each")
    print("      • Executor Cores: 4 each")
    print("      • Application Files: hdfs:///user/spark/.sparkStaging/app-123/\n")

    print("   ✅ ResourceManager accepted application!")
    print("   📋 Application ID: application_1234567890_0001")
    print("   🔗 Tracking URL: http://rm-host:8088/proxy/application_1234567890_0001")
    print("   ℹ️  Application queued for resource allocation\n")

    # ========================================================================
    # STEP 4: APPLICATIONMASTER (DRIVER) ALLOCATION
    # ========================================================================
    # WHAT HAPPENS:
    # 1. ResourceManager selects NodeManager:
    #    - Checks available resources on all nodes
    #    - Considers data locality preferences
    #    - Chooses node with sufficient capacity
    #
    # 2. ResourceManager allocates container:
    #    - Reserves memory (2GB for driver)
    #    - Reserves cores (2 cores for driver)
    #    - Generates container ID
    #
    # 3. NodeManager launches container:
    #    - Downloads application files from HDFS
    #    - Sets up environment (JAVA_HOME, SPARK_HOME)
    #    - Starts ApplicationMaster JVM process
    #    - Command: java org.apache.spark.deploy.yarn.ApplicationMaster
    #
    # 4. ApplicationMaster (Driver) starts:
    #    - Initializes SparkContext
    #    - Registers with ResourceManager
    #    - Prepares to request executor containers
    # ========================================================================
    print("Step 4️⃣: ResourceManager allocating ApplicationMaster (Driver)")
    print("   🔍 Searching for node with 2GB memory + 2 cores...")
    print("   ✅ Found suitable node: worker-node-01")
    print("   ✅ Allocated container: container_1234567890_0001_01_000001")
    print("   ✅ Container resources: 2GB memory, 2 cores\n")

    print("   📦 NodeManager on worker-node-01:")
    print("      1️⃣ Downloading application from HDFS")
    print("      2️⃣ Setting up container environment")
    print("      3️⃣ Launching ApplicationMaster JVM")
    print("      4️⃣ Starting Spark Driver process\n")

    print("   ✅ Driver is now running on worker-node-01!")
    print("   ℹ️  Driver will now request executor containers...\n")


# ============================================================================
# PHASE 2: EXECUTOR ALLOCATION AND LAUNCH
# ============================================================================


def demonstrate_executor_allocation(spark):
    """
    Demonstrate how executors are allocated and launched.

    WHAT THIS SHOWS:
    ----------------
    After driver starts, it requests executor containers from cluster manager.
    This demonstrates the complete allocation and launch process.

    EXECUTOR REQUEST FLOW (YARN):
    -----------------------------
    1. Driver (ApplicationMaster) sends request to ResourceManager:
       - Number of containers needed
       - Memory per container
       - Cores per container
       - Locality preferences (nodes with data)
       - Priority (for different task types)

    2. ResourceManager schedules containers:
       - Checks queue capacity
       - Finds nodes with available resources
       - Considers data locality
       - Allocates containers

    3. ResourceManager notifies ApplicationMaster:
       - List of allocated containers
       - Container IDs
       - NodeManager addresses

    4. ApplicationMaster contacts NodeManagers:
       - Sends container launch context
       - Provides executor startup command
       - Specifies environment variables

    5. NodeManagers launch executors:
       - Download application files
       - Start executor JVM process
       - Executor connects back to driver

    6. Driver acknowledges executors:
       - Registers executor in task scheduler
       - Marks resources as available
       - Ready to send tasks
    """
    print_section("PHASE 2: EXECUTOR ALLOCATION & LAUNCH")

    # ========================================================================
    # STEP 1: DRIVER CALCULATES EXECUTOR NEEDS
    # ========================================================================
    # WHAT HAPPENS:
    # - Driver reads configuration:
    #   spark.executor.instances = 10
    #   spark.executor.memory = 4g
    #   spark.executor.cores = 4
    #
    # - Driver calculates total resources:
    #   Total memory: 10 × 4GB = 40GB
    #   Total cores: 10 × 4 = 40 cores
    #
    # - Driver considers overhead:
    #   spark.executor.memoryOverhead = 10% of executor memory
    #   Actual container size = 4GB + 400MB = 4.4GB
    # ========================================================================
    print("Step 1️⃣: Driver calculating executor requirements")
    print("   📊 Configuration:")
    print("      • spark.executor.instances = 10")
    print("      • spark.executor.memory = 4g")
    print("      • spark.executor.cores = 4")
    print("      • spark.executor.memoryOverhead = 0.1 (10%)\n")

    print("   🧮 Calculation:")
    print("      • Total executors: 10")
    print("      • Memory per executor: 4GB + 400MB overhead = 4.4GB")
    print("      • Cores per executor: 4")
    print("      • Total cluster resources needed: 44GB memory, 40 cores\n")

    # ========================================================================
    # STEP 2: DRIVER SENDS REQUEST TO RESOURCEMANAGER
    # ========================================================================
    # WHAT HAPPENS:
    # Driver (ApplicationMaster) calls allocate() API:
    # - Creates ResourceRequest objects for each executor
    # - Specifies resource requirements (memory, cores)
    # - Provides locality preferences (preferred nodes)
    # - Sets priority level
    # - Sends to ResourceManager via RPC
    #
    # ResourceManager receives request:
    # - Validates against queue capacity
    # - Checks user limits
    # - Queues for scheduling
    # ========================================================================
    print("Step 2️⃣: Driver requesting executors from ResourceManager")
    print("   📤 Sending ResourceRequest to ResourceManager:")
    print("      {")
    print("        'num_containers': 10,")
    print("        'memory_per_container': '4400m',")
    print("        'cores_per_container': 4,")
    print(
        "        'locality': ['worker-node-02', 'worker-node-03', 'worker-node-04', 'ANY'],"
    )
    print("        'priority': 1")
    print("      }\n")

    print("   ✅ ResourceManager accepted request")
    print("   ℹ️  Request queued for scheduling\n")

    # ========================================================================
    # STEP 3: RESOURCEMANAGER ALLOCATES CONTAINERS
    # ========================================================================
    # WHAT HAPPENS:
    # ResourceManager's scheduler runs:
    # 1. Checks queue: default queue has capacity
    # 2. Finds suitable nodes:
    #    - Scans all NodeManagers for available resources
    #    - Prefers nodes with data locality
    #    - Ensures anti-affinity (spread across nodes)
    #
    # 3. Allocates containers:
    #    - Reserves resources on selected nodes
    #    - Generates container IDs
    #    - Creates allocation response
    #
    # 4. Returns to ApplicationMaster:
    #    - List of Container objects
    #    - Each with: ID, NodeManager address, resources
    # ========================================================================
    print("Step 3️⃣: ResourceManager allocating containers")
    print("   🔍 ResourceManager scanning cluster for resources...\n")

    print("   📋 Allocation Results:")
    print(
        "   ┌────────────────────┬───────────────────────────────┬──────────┬───────┐"
    )
    print(
        "   │ Container ID       │ NodeManager                   │ Memory   │ Cores │"
    )
    print(
        "   ├────────────────────┼───────────────────────────────┼──────────┼───────┤"
    )
    print(
        "   │ container_..._0002 │ worker-node-02:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0003 │ worker-node-03:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0004 │ worker-node-04:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0005 │ worker-node-02:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0006 │ worker-node-03:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0007 │ worker-node-04:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0008 │ worker-node-02:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0009 │ worker-node-03:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0010 │ worker-node-04:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   │ container_..._0011 │ worker-node-02:45454          │ 4.4GB    │ 4     │"
    )
    print(
        "   └────────────────────┴───────────────────────────────┴──────────┴───────┘\n"
    )

    print("   ✅ All 10 containers allocated!")
    print("   ℹ️  Containers distributed across 3 worker nodes\n")

    # ========================================================================
    # STEP 4: DRIVER LAUNCHES EXECUTORS
    # ========================================================================
    # WHAT HAPPENS:
    # Driver receives container allocation response:
    # 1. For each allocated container:
    #    - Creates ContainerLaunchContext
    #    - Specifies command: java org.apache.spark.executor.CoarseGrainedExecutorBackend
    #    - Sets environment variables (SPARK_HOME, JAVA_HOME, etc.)
    #    - Provides application files (from HDFS)
    #
    # 2. Sends launch request to NodeManager:
    #    - RPC call to NodeManager on container's host
    #    - Provides ContainerLaunchContext
    #    - NodeManager validates and launches
    #
    # 3. NodeManager starts executor:
    #    - Downloads application files
    #    - Sets up environment
    #    - Launches JVM with executor code
    #    - Redirects logs to YARN log directory
    # ========================================================================
    print("Step 4️⃣: Driver launching executors in containers")
    print("   📤 Driver sending launch commands to NodeManagers...\n")

    print("   worker-node-02:")
    print("      🚀 Launching executor in container_..._0002")
    print("      🚀 Launching executor in container_..._0005")
    print("      🚀 Launching executor in container_..._0008")
    print("      🚀 Launching executor in container_..._0011\n")

    print("   worker-node-03:")
    print("      🚀 Launching executor in container_..._0003")
    print("      🚀 Launching executor in container_..._0006")
    print("      🚀 Launching executor in container_..._0009\n")

    print("   worker-node-04:")
    print("      🚀 Launching executor in container_..._0004")
    print("      🚀 Launching executor in container_..._0007")
    print("      🚀 Launching executor in container_..._0010\n")

    print("   ℹ️  Each NodeManager:")
    print("      1. Downloads application files from HDFS")
    print("      2. Sets up JVM environment")
    print("      3. Starts executor process")
    print("      4. Executor connects back to driver\n")

    # ========================================================================
    # STEP 5: EXECUTORS REGISTER WITH DRIVER
    # ========================================================================
    # WHAT HAPPENS:
    # Each executor on startup:
    # 1. Reads driver address from environment
    # 2. Opens connection to driver (RPC)
    # 3. Sends RegisterExecutor message:
    #    - Executor ID
    #    - Hostname
    #    - Cores available
    #    - Executor endpoint reference
    #
    # Driver receives registration:
    # 1. Validates executor ID
    # 2. Adds to executor registry
    # 3. Updates task scheduler with new resources
    # 4. Sends RegisterExecutorResponse
    # 5. Marks executor as ready for tasks
    # ========================================================================
    print("Step 5️⃣: Executors registering with driver")
    print("   📞 Executors connecting to driver...\n")

    executors = [
        ("executor-0", "worker-node-02", "4 cores, 4GB"),
        ("executor-1", "worker-node-03", "4 cores, 4GB"),
        ("executor-2", "worker-node-04", "4 cores, 4GB"),
        ("executor-3", "worker-node-02", "4 cores, 4GB"),
        ("executor-4", "worker-node-03", "4 cores, 4GB"),
        ("executor-5", "worker-node-04", "4 cores, 4GB"),
        ("executor-6", "worker-node-02", "4 cores, 4GB"),
        ("executor-7", "worker-node-03", "4 cores, 4GB"),
        ("executor-8", "worker-node-04", "4 cores, 4GB"),
        ("executor-9", "worker-node-02", "4 cores, 4GB"),
    ]

    for exec_id, host, resources in executors:
        print(f"   ✅ {exec_id} on {host}: Registered ({resources})")

    print("\n   ✅ All 10 executors registered and ready!")
    print("   📊 Total cluster resources available:")
    print("      • 40 cores across 10 executors")
    print("      • 40GB memory across 10 executors")
    print("      • Distributed across 3 worker nodes\n")

    print("   ℹ️  Driver can now schedule tasks on these executors")


# ============================================================================
# PHASE 3: TASK EXECUTION
# ============================================================================


def demonstrate_task_execution(spark):
    """
    Demonstrate task distribution and execution across cluster.

    WHAT THIS SHOWS:
    ----------------
    How driver schedules tasks on executors and coordinates execution.
    Shows the complete task lifecycle from creation to completion.

    TASK SCHEDULING FLOW:
    ---------------------
    1. Action triggers job creation
    2. Driver builds DAG and splits into stages
    3. Driver creates tasks (1 per partition)
    4. Driver considers data locality
    5. Driver serializes tasks
    6. Driver sends tasks to executors
    7. Executors deserialize and execute tasks
    8. Executors report results back to driver
    9. Driver aggregates final result
    """
    print_section("PHASE 3: TASK EXECUTION")

    # ========================================================================
    # STEP 1: CREATE SAMPLE DATA
    # ========================================================================
    # WHAT HAPPENS:
    # - range() creates RDD/DataFrame
    # - No data generated yet (lazy evaluation)
    # - Just stores the operation in DAG
    # ========================================================================
    print("Step 1️⃣: Creating sample dataset")
    print("   📊 df = spark.range(0, 1000000).toDF('id')")
    print("   ℹ️  Lazy evaluation: No data created yet, just plan\n")

    df = spark.range(0, 1000000).toDF("id")

    # ========================================================================
    # STEP 2: APPLY TRANSFORMATIONS
    # ========================================================================
    # WHAT HAPPENS:
    # - Transformations added to DAG
    # - Still no computation
    # - Driver stores the logical plan
    # ========================================================================
    print("Step 2️⃣: Applying transformations (lazy)")
    print("   🔧 df_filtered = df.filter(col('id') > 500000)")
    print("   🔧 df_squared = df_filtered.withColumn('squared', col('id') * col('id'))")
    print("   ℹ️  Still no execution, building logical plan\n")

    df_filtered = df.filter(col("id") > 500000)
    df_squared = df_filtered.withColumn("squared", col("id") * col("id"))

    # ========================================================================
    # STEP 3: ACTION TRIGGERS JOB
    # ========================================================================
    # WHAT HAPPENS WHEN count() IS CALLED:
    #
    # A. Driver - Job Creation:
    #    1. Identifies action (count)
    #    2. Analyzes DAG for this action
    #    3. Creates Job object
    #    4. Assigns job ID
    #
    # B. Driver - Stage Creation:
    #    1. Finds shuffle boundaries in DAG
    #    2. Splits into stages at shuffles
    #    3. Creates Stage objects
    #    4. Determines stage dependencies
    #    5. For count(): Usually single stage (no shuffle)
    #
    # C. Driver - Task Creation:
    #    1. For each stage:
    #       - Count number of partitions (default: spark.default.parallelism)
    #       - Create 1 task per partition
    #       - Example: 200 partitions → 200 tasks
    #    2. Assigns task IDs
    #    3. Determines task locality preferences
    #
    # D. Driver - Task Serialization:
    #    1. For each task:
    #       - Serialize task function (closure)
    #       - Serialize any broadcast variables referenced
    #       - Create Task object
    #       - Package: function + partition info + dependencies
    #    2. Size typically: 1-10 KB per task
    #
    # E. Driver - Task Scheduling:
    #    1. Choose executor for each task:
    #       - Prefer PROCESS_LOCAL (data in memory on executor)
    #       - Next: NODE_LOCAL (data on disk on same node)
    #       - Next: RACK_LOCAL (data on same rack)
    #       - Last: ANY (data on different rack)
    #    2. Assign tasks to executors
    #    3. Queue tasks if executors busy
    #
    # F. Driver - Task Distribution:
    #    1. Send serialized tasks to executors via RPC
    #    2. Each executor receives tasks for its partitions
    #    3. Tasks queued in executor's task pool
    # ========================================================================
    print("Step 3️⃣: Triggering action (count) - Job execution starts!\n")

    print("   🎬 Driver: Action detected - count()")
    print("   📋 Driver: Creating job (job_id = 0)")
    print("   📊 Driver: Analyzing DAG...")
    print("      • Range generation")
    print("      • Filter (id > 500000)")
    print("      • Add column (squared)")
    print("      • Count aggregation")
    print("   ℹ️  No shuffle needed, single stage\n")

    print("   🎭 Driver: Creating stages")
    print("      • Stage 0: Read + Filter + AddColumn + Count")
    print("      • No shuffle boundaries found")
    print("      • Single stage execution\n")

    print("   📦 Driver: Creating tasks")
    print("      • DataFrame has 8 partitions (default for local[4])")
    print("      • Creating 8 tasks (1 per partition)")
    print("      • Task IDs: task_0_0 through task_0_7\n")

    print("   �� Driver: Serializing tasks")
    print("      • Serializing filter function: lambda id: id > 500000")
    print("      • Serializing column expression: col('id') * col('id')")
    print("      • Serializing count aggregation")
    print("      • Average task size: ~5 KB\n")

    print("   📍 Driver: Determining task locality")
    print("      • Data is in-memory (just generated)")
    print("      • All tasks have PROCESS_LOCAL locality")
    print("      • No data transfer needed\n")

    print("   📤 Driver: Distributing tasks to executors")
    print("      ┌────────────┬─────────────────────────────────┐")
    print("      │ Executor   │ Tasks Assigned                  │")
    print("      ├────────────┼─────────────────────────────────┤")
    print("      │ executor-0 │ task_0_0, task_0_4              │")
    print("      │ executor-1 │ task_0_1, task_0_5              │")
    print("      │ executor-2 │ task_0_2, task_0_6              │")
    print("      │ executor-3 │ task_0_3, task_0_7              │")
    print("      └────────────┴─────────────────────────────────┘\n")

    # ========================================================================
    # STEP 4: EXECUTORS RUN TASKS
    # ========================================================================
    # WHAT HAPPENS ON EACH EXECUTOR:
    #
    # A. Task Receipt:
    #    1. Executor receives serialized task via RPC
    #    2. Adds task to internal queue
    #    3. Thread pool picks up task
    #
    # B. Task Deserialization:
    #    1. Deserialize task object
    #    2. Extract function closure
    #    3. Extract partition information
    #    4. Prepare for execution
    #
    # C. Task Execution:
    #    1. Get partition data (from memory/disk/network)
    #    2. Create iterator over partition records
    #    3. Apply transformations in pipeline:
    #       For each record:
    #         - Apply filter: if id > 500000
    #         - Apply withColumn: squared = id * id
    #         - Count if passed filter
    #    4. No intermediate materialization
    #    5. Process records one at a time (iterator model)
    #
    # D. Result Collection:
    #    1. Task completes with result (count from this partition)
    #    2. Serialize result
    #    3. Send result back to driver
    #    4. Report task metrics (time, memory, etc.)
    #
    # E. Metrics Reported:
    #    - Executor run time
    #    - Shuffle read/write (if any)
    #    - Input records
    #    - Output records
    #    - GC time
    #    - Memory usage
    # ========================================================================
    print("   ⚙️  Executors: Running tasks in parallel\n")

    print("   executor-0 (task_0_0):")
    print("      1. Deserialize task")
    print("      2. Get partition 0 data (125,000 records)")
    print("      3. Apply filter: 62,500 pass")
    print("      4. Apply withColumn: add 'squared' column")
    print("      5. Count: 62,500")
    print("      6. Return count to driver\n")

    print("   executor-1 (task_0_1):")
    print("      1. Deserialize task")
    print("      2. Get partition 1 data (125,000 records)")
    print("      3. Apply filter: 62,500 pass")
    print("      4. Apply withColumn: add 'squared' column")
    print("      5. Count: 62,500")
    print("      6. Return count to driver\n")

    print("   ... (similar execution on other executors) ...\n")

    # Execute the actual count
    start_time = time.time()
    result = df_squared.count()
    execution_time = time.time() - start_time

    # ========================================================================
    # STEP 5: DRIVER AGGREGATES RESULTS
    # ========================================================================
    # WHAT HAPPENS:
    # 1. Driver receives count from each task:
    #    - Task 0: 62,500
    #    - Task 1: 62,500
    #    - Task 2: 62,500
    #    - ... (8 tasks total)
    #
    # 2. Driver sums all counts:
    #    - Total = 62,500 × 8 = 500,000
    #
    # 3. Driver returns result to user code:
    #    - result = 500,000
    #
    # 4. Driver updates metrics:
    #    - Job completed successfully
    #    - Total time taken
    #    - Tasks completed: 8
    #    - Data processed: 1M records
    # ========================================================================
    print("   📥 Driver: Collecting results from executors")
    print("      • Received count from executor-0: 62,500")
    print("      • Received count from executor-1: 62,500")
    print("      • Received count from executor-2: 62,500")
    print("      • Received count from executor-3: 62,500")
    print("      • Received count from executor-0: 62,500")
    print("      • Received count from executor-1: 62,500")
    print("      • Received count from executor-2: 62,500")
    print("      • Received count from executor-3: 62,500\n")

    print(f"   🧮 Driver: Aggregating results")
    print(f"      • Sum of all counts: {result:,}")
    print(f"   ⏱️  Total execution time: {execution_time:.4f} seconds\n")

    print(f"   ✅ Result returned to user: {result:,} rows")


# ============================================================================
# PHASE 4: APPLICATION SHUTDOWN
# ============================================================================


def demonstrate_application_shutdown(spark):
    """
    Demonstrate graceful shutdown of Spark application.

    WHAT THIS SHOWS:
    ----------------
    How Spark application cleans up resources and shuts down properly.

    SHUTDOWN FLOW:
    --------------
    1. User calls spark.stop() or application completes
    2. Driver stops accepting new jobs
    3. Driver waits for running tasks to complete
    4. Driver shuts down executors
    5. Executors clean up and exit
    6. Driver unregisters from cluster manager
    7. Cluster manager releases resources
    """
    print_section("PHASE 4: APPLICATION SHUTDOWN")

    # ========================================================================
    # STEP 1: INITIATE SHUTDOWN
    # ========================================================================
    # WHAT HAPPENS:
    # - spark.stop() called
    # - Driver sets shutdown flag
    # - No new jobs accepted
    # - Running jobs allowed to complete (or timeout)
    # ========================================================================
    print("Step 1️⃣: Initiating shutdown")
    print("   📞 User calls: spark.stop()")
    print("   ⏸️  Driver: Stop accepting new jobs")
    print("   ⏳ Driver: Wait for running tasks to complete\n")

    # ========================================================================
    # STEP 2: SHUTDOWN EXECUTORS
    # ========================================================================
    # WHAT HAPPENS:
    # 1. Driver sends StopExecutor message to each executor
    # 2. Each executor:
    #    - Finishes current task (if any)
    #    - Flushes cached data (if configured)
    #    - Closes connections
    #    - Cleans up temporary files
    #    - Exits JVM process
    #
    # 3. NodeManagers detect executor exit
    # 4. NodeManagers release container resources
    # 5. NodeManagers clean up container directories
    # ========================================================================
    print("Step 2️⃣: Shutting down executors")
    print("   📤 Driver: Sending shutdown signal to all executors\n")

    executors = [
        "executor-0",
        "executor-1",
        "executor-2",
        "executor-3",
        "executor-4",
        "executor-5",
        "executor-6",
        "executor-7",
        "executor-8",
        "executor-9",
    ]

    for exec_id in executors:
        print(f"   🛑 {exec_id}: Received shutdown signal")
        print(f"      1. Finishing current tasks")
        print(f"      2. Flushing cached data")
        print(f"      3. Closing connections")
        print(f"      4. Cleaning up temp files")
        print(f"      5. Exiting JVM process")
        print(f"   ✅ {exec_id}: Shutdown complete\n")

    print("   ✅ All executors shut down successfully")
    print("   ℹ️  NodeManagers released container resources\n")

    # ========================================================================
    # STEP 3: DRIVER CLEANUP
    # ========================================================================
    # WHAT HAPPENS:
    # 1. Driver closes all open connections
    # 2. Driver flushes final metrics
    # 3. Driver unregisters from ResourceManager
    # 4. Driver sends application completion status
    # 5. Driver exits
    # ========================================================================
    print("Step 3️⃣: Driver cleanup and unregistration")
    print("   🧹 Driver: Closing connections")
    print("   📊 Driver: Flushing final metrics to history server")
    print("   📤 Driver: Unregistering from YARN ResourceManager")
    print("   ✅ Driver: Sending application completion status: SUCCESS")
    print("   🛑 Driver: Exiting process\n")

    # ========================================================================
    # STEP 4: CLUSTER MANAGER CLEANUP
    # ========================================================================
    # WHAT HAPPENS IN YARN:
    # 1. ResourceManager receives application completion
    # 2. ResourceManager marks application as FINISHED
    # 3. ResourceManager updates application status
    # 4. ResourceManager releases all containers
    # 5. ResourceManager frees queue capacity
    # 6. ResourceManager archives application logs
    # 7. NodeManagers clean up local files
    # ========================================================================
    print("Step 4️⃣: Cluster Manager (YARN) cleanup")
    print("   📥 ResourceManager: Received completion notification")
    print("   ✅ ResourceManager: Marked application as FINISHED")
    print("   📊 ResourceManager: Updated application status")
    print("   🆓 ResourceManager: Released all allocated containers")
    print("   📈 ResourceManager: Freed queue capacity:")
    print("      • Released: 44GB memory")
    print("      • Released: 40 cores")
    print("   �� ResourceManager: Archived application logs")
    print("   🧹 NodeManagers: Cleaned up local container files\n")

    print("   ✅ Application lifecycle complete!")
    print(
        "   ℹ️  View logs at: http://rm-host:8088/cluster/app/application_1234567890_0001"
    )

    # Actually stop the spark session
    spark.stop()
    print("\n   🛑 SparkSession stopped")


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main():
    """
    Main function demonstrating complete Driver → YARN → Executor flow.
    """
    print("\n" + "🚀" * 40)
    print("DRIVER → YARN/CLUSTER MANAGER → EXECUTOR INTERACTION")
    print("Complete Application Lifecycle Demonstration")
    print("🚀" * 40)

    # Phase 1: Application submission
    demonstrate_application_submission()

    # Create Spark session (Phase 2: Driver initialization)
    spark = create_spark_session()

    # Phase 2: Executor allocation
    demonstrate_executor_allocation(spark)

    # Phase 3: Task execution
    demonstrate_task_execution(spark)

    # Phase 4: Shutdown
    demonstrate_application_shutdown(spark)

    print("\n" + "=" * 80)
    print("✅ COMPLETE LIFECYCLE DEMONSTRATION FINISHED")
    print("=" * 80)

    print("\n📚 Summary:")
    print("   1️⃣  Submission: spark-submit → Cluster Manager")
    print("   2️⃣  Driver Launch: Cluster Manager → Driver Container/Pod")
    print("   3️⃣  Executor Allocation: Driver → Cluster Manager → Executors")
    print("   4️⃣  Task Execution: Driver → Executors → Results → Driver")
    print("   5️⃣  Shutdown: Driver → Executors → Cluster Manager")

    print("\n🔗 Key Interactions:")
    print("   • Client ↔ Cluster Manager: Application submission")
    print("   • Driver ↔ Cluster Manager: Resource requests")
    print("   • Driver ↔ Executors: Task distribution & results")
    print("   • Executors ↔ Executors: Shuffle data (peer-to-peer)")
    print("   • Cluster Manager ↔ Worker Nodes: Container/pod management")

    print("\n💡 Remember:")
    print("   ✅ Driver is the brain (plans, schedules, coordinates)")
    print("   ✅ Cluster Manager is the resource manager (allocates, monitors)")
    print("   ✅ Executors are the workers (execute tasks, store data)")
    print("   ✅ All components communicate via RPC")
    print("   ✅ Failure handling is built-in at every level\n")


if __name__ == "__main__":
    main()
