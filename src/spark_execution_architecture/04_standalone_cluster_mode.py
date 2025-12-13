#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
Spark Standalone Cluster Mode - Built-in Clustering
================================================================================

MODULE OVERVIEW:
----------------
Complete guide to Spark's built-in Standalone cluster manager - the simplest
way to deploy Spark on a cluster without requiring Hadoop YARN or Kubernetes.
This demonstrates how Spark's native clustering works, how to set it up, and
how it compares to other cluster managers.

PURPOSE:
--------
Learn Spark Standalone clustering:
- How to set up a Standalone cluster (Master + Workers)
- Application submission and execution
- Resource allocation and management
- Differences from YARN/Kubernetes
- When to use Standalone mode

TARGET AUDIENCE:
----------------
- Developers learning Spark clustering
- Teams wanting simple cluster deployment
- Users migrating from local to cluster mode
- Anyone needing quick cluster setup without Hadoop/K8s

SPARK STANDALONE ARCHITECTURE:
===============================

    ┌─────────────────────────────────────────────────────────────┐
    │                    SPARK STANDALONE CLUSTER                  │
    ├─────────────────────────────────────────────────────────────┤
    │                                                              │
    │  ┌──────────────────────────────────────────────────────┐  │
    │  │                    MASTER NODE                       │  │
    │  │  (spark://master-host:7077)                          │  │
    │  │                                                       │  │
    │  │  Responsibilities:                                    │  │
    │  │  • Accept application submissions                     │  │
    │  │  • Manage worker registration                         │  │
    │  │  • Allocate resources to applications                 │  │
    │  │  • Monitor worker/executor health                     │  │
    │  │  • Provide Web UI (port 8080)                         │  │
    │  │  • Handle failover (if HA configured)                 │  │
    │  └──────────────────────────────────────────────────────┘  │
    │         │                    │                    │         │
    │         ↓                    ↓                    ↓         │
    │  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐│
    │  │  WORKER 1   │      │  WORKER 2   │      │  WORKER 3   ││
    │  │             │      │             │      │             ││
    │  │  Resources: │      │  Resources: │      │  Resources: ││
    │  │  • 8 cores  │      │  • 8 cores  │      │  • 8 cores  ││
    │  │  • 16GB RAM │      │  • 16GB RAM │      │  • 16GB RAM ││
    │  │             │      │             │      │             ││
    │  │  Executors: │      │  Executors: │      │  Executors: ││
    │  │  [Exec 1]   │      │  [Exec 3]   │      │  [Exec 5]   ││
    │  │  [Exec 2]   │      │  [Exec 4]   │      │  [Exec 6]   ││
    │  └─────────────┘      └─────────────┘      └─────────────┘│
    └─────────────────────────────────────────────────────────────┘

HOW STANDALONE MODE WORKS:
===========================

STARTUP SEQUENCE:
-----------------
1. Start Master:
   $ $SPARK_HOME/sbin/start-master.sh
   - Master starts on port 7077 (default)
   - Web UI available at http://master:8080
   - Master URL: spark://master-host:7077

2. Start Workers:
   $ $SPARK_HOME/sbin/start-worker.sh spark://master-host:7077
   - Worker connects to Master
   - Registers available resources (cores, memory)
   - Starts heartbeat mechanism
   - Web UI available at http://worker:8081

3. Submit Application:
   $ spark-submit --master spark://master-host:7077 \\
                  --deploy-mode client \\
                  app.py

RESOURCE ALLOCATION MODES:
===========================

1. CORES ALLOCATION:
--------------------
--total-executor-cores 24  # Total cores across ALL executors
--executor-cores 4          # Cores per executor
Result: 24 / 4 = 6 executors

2. MEMORY ALLOCATION:
---------------------
--executor-memory 4g        # Memory per executor
Workers must have enough memory to satisfy requests

3. SPREAD VS CONSOLIDATE:
--------------------------
spark.deploy.spreadOut = true (default)
- Spreads executors across all available workers
- Good for: Data locality, fault tolerance
- Example: 6 executors on 3 workers = 2 per worker

spark.deploy.spreadOut = false
- Packs executors on fewer workers
- Good for: Resource efficiency, fewer JVMs
- Example: 6 executors might go on 2 workers only

APPLICATION SUBMISSION FLOW:
=============================

CLIENT MODE (default):
----------------------
1. spark-submit connects to Master
2. Master allocates resources on Workers
3. Workers launch Executors
4. Driver (on client machine) connects to Executors
5. Application runs
6. Results return to Driver on client

CLUSTER MODE:
-------------
1. spark-submit connects to Master
2. Master selects a Worker to run Driver
3. Worker launches Driver in executor process
4. Driver requests more Executors from Master
5. Workers launch additional Executors
6. Application runs entirely in cluster
7. Client can disconnect

KEY DIFFERENCES FROM YARN/KUBERNETES:
======================================

STANDALONE vs YARN:
-------------------
                   Standalone          YARN
Setup             Simple              Complex (needs Hadoop)
Multi-tenancy     Basic               Advanced (queues, ACLs)
Resource Sharing  Per-app             Fine-grained
Dynamic Alloc     Limited             Full support
Security          Basic               Kerberos, ACLs
Monitoring        Web UI              ResourceManager UI
Failover          Optional HA         Built-in HA

STANDALONE vs KUBERNETES:
-------------------------
                   Standalone          Kubernetes
Setup             Simple              Moderate (needs K8s)
Containers        No                  Yes (pods)
Scaling           Manual              Auto-scaling
Orchestration     Basic               Advanced
Cloud Native      No                  Yes
Resource Limits   Soft                Hard (cgroups)

WHEN TO USE STANDALONE:
=======================

✅ GOOD FOR:
- Development and testing
- Small to medium clusters (< 50 nodes)
- Single organization/team
- Quick cluster setup
- Learning Spark clustering
- Don't have Hadoop/K8s infrastructure

❌ NOT GOOD FOR:
- Multi-tenant production environments
- Need fine-grained resource sharing
- Require advanced security (Kerberos)
- Need container isolation
- Cloud-native deployments

CONFIGURATION DEEP DIVE:
=========================

MASTER CONFIGURATION:
---------------------
spark.deploy.defaultCores = 1
  - Cores given to each application by default
  - Set to limit resource hogging

SPARK_MASTER_OPTS:
  -Dspark.deploy.defaultCores=8
  -Dspark.worker.timeout=60
  -Dspark.deploy.retainedApplications=200
  -Dspark.deploy.retainedDrivers=200

WORKER CONFIGURATION:
---------------------
SPARK_WORKER_CORES = 8
  - Number of cores available on this worker
  - Usually set to physical core count

SPARK_WORKER_MEMORY = 16g
  - Memory available for executors
  - Leave some for OS (typically 1-2GB)

SPARK_WORKER_INSTANCES = 1
  - Number of worker instances per machine
  - Usually 1, but can run multiple for testing

SPARK_WORKER_DIR = /var/spark/work
  - Working directory for executor logs

EXECUTOR CONFIGURATION:
-----------------------
spark.executor.cores = 4
  - Cores per executor (parallelism)
  - Too high: memory contention
  - Too low: underutilization

spark.executor.memory = 4g
  - Heap size per executor
  - Actual container size = memory + memoryOverhead

spark.executor.memoryOverhead = 0.1 (10%)
  - Off-heap memory for VM overheads
  - Min 384MB

HIGH AVAILABILITY (HA) SETUP:
==============================

ZOOKEEPER-BASED HA:
-------------------
Multiple Master nodes with ZooKeeper coordination

Configuration:
spark.deploy.recoveryMode = ZOOKEEPER
spark.deploy.zookeeper.url = zk1:2181,zk2:2181,zk3:2181
spark.deploy.zookeeper.dir = /spark

Benefits:
✅ Automatic failover
✅ No single point of failure
✅ State persisted in ZooKeeper

Setup:
1. Start ZooKeeper ensemble
2. Start multiple Masters with same config
3. Workers connect to all Masters
4. Active Master elected via ZooKeeper
5. If Active fails, Standby becomes Active

FILESYSTEM-BASED HA:
--------------------
Single Master with state stored on shared filesystem (NFS, HDFS)

Configuration:
spark.deploy.recoveryMode = FILESYSTEM
spark.deploy.recoveryDirectory = hdfs://namenode/spark/recovery

Benefits:
✅ Simpler than ZooKeeper
✅ Master can restart and recover state

Limitations:
❌ Manual restart required
❌ Longer recovery time

MONITORING AND DEBUGGING:
==========================

WEB UIs:
--------
Master UI: http://master:8080
  - Active/completed applications
  - Worker list and resources
  - Application history

Worker UI: http://worker:8081
  - Running executors
  - Resource usage
  - Logs

Application UI: http://driver:4040
  - Jobs, stages, tasks
  - Storage (cached data)
  - Environment, executors

METRICS:
--------
spark.metrics.conf = /path/to/metrics.properties
  - Enable metrics reporting
  - Graphite, Ganglia, Prometheus

LOG FILES:
----------
Master logs: $SPARK_HOME/logs/spark-*-master-*.out
Worker logs: $SPARK_HOME/logs/spark-*-worker-*.out
Executor logs: $SPARK_WORKER_DIR/app-*/executor-*.log

RESOURCE ALLOCATION EXAMPLES:
==============================

EXAMPLE 1: Basic Setup
----------------------
Cluster: 3 workers × 8 cores × 16GB = 24 cores, 48GB

Request:
--total-executor-cores 12
--executor-memory 4g

Result:
- 12 cores allocated across executors
- Each executor gets 4GB
- With default 1 core per executor: 12 executors
- With --executor-cores 4: 3 executors
- Spread across all 3 workers

EXAMPLE 2: Memory-Intensive
----------------------------
Cluster: 3 workers × 8 cores × 64GB = 24 cores, 192GB

Request:
--executor-memory 16g
--executor-cores 2
--total-executor-cores 6

Result:
- 6 cores / 2 = 3 executors
- Each: 16GB memory, 2 cores
- Total: 48GB memory, 6 cores
- Leaves resources for other apps

EXAMPLE 3: CPU-Intensive
-------------------------
Cluster: 3 workers × 16 cores × 32GB = 48 cores, 96GB

Request:
--total-executor-cores 48
--executor-cores 4
--executor-memory 2g

Result:
- 48 cores / 4 = 12 executors
- Each: 2GB memory, 4 cores
- Total: 24GB memory, 48 cores
- Maximizes CPU utilization

USAGE:
------
# Start cluster (on each machine):
$SPARK_HOME/sbin/start-master.sh  # On master node
$SPARK_HOME/sbin/start-worker.sh spark://master:7077  # On worker nodes

# Submit application:
spark-submit --master spark://master:7077 \\
             --deploy-mode client \\
             --total-executor-cores 12 \\
             --executor-memory 4g \\
             04_standalone_cluster_mode.py

# Stop cluster:
$SPARK_HOME/sbin/stop-worker.sh  # On workers
$SPARK_HOME/sbin/stop-master.sh  # On master

RELATED RESOURCES:
------------------
- Standalone Mode: https://spark.apache.org/docs/latest/spark-standalone.html
- Submitting Applications: https://spark.apache.org/docs/latest/submitting-applications.html
- Monitoring: https://spark.apache.org/docs/latest/monitoring.html

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

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, max as _max
from pyspark import SparkConf


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def print_section(title):
    """Print formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def print_subsection(title):
    """Print formatted subsection header."""
    print(f"\n{'─' * 80}")
    print(f"  {title}")
    print('─' * 80)


# ============================================================================
# STANDALONE CLUSTER SETUP SIMULATION
# ============================================================================

def demonstrate_cluster_setup():
    """
    Demonstrate how to set up a Spark Standalone cluster.
    
    WHAT THIS SHOWS:
    ----------------
    The complete process of setting up Master and Worker nodes in a
    Standalone cluster, including configuration and startup.
    
    CLUSTER TOPOLOGY:
    -----------------
    1 Master node + 3 Worker nodes
    
    Master:
    - hostname: spark-master
    - cores: 4 (for master process, not for executors)
    - memory: 4GB
    - ports: 7077 (RPC), 8080 (Web UI)
    
    Workers (each):
    - cores: 8 (available for executors)
    - memory: 16GB
    - ports: 8081, 8082, 8083 (Web UIs)
    """
    print_section("STANDALONE CLUSTER SETUP")
    
    # ========================================================================
    # STEP 1: START MASTER
    # ========================================================================
    # WHAT HAPPENS:
    # When you run: $SPARK_HOME/sbin/start-master.sh
    #
    # 1. Script reads configuration from:
    #    - $SPARK_HOME/conf/spark-env.sh
    #    - $SPARK_HOME/conf/spark-defaults.conf
    #
    # 2. Master process starts:
    #    - Class: org.apache.spark.deploy.master.Master
    #    - Binds to port 7077 for RPC (application submissions)
    #    - Binds to port 8080 for Web UI
    #    - Starts REST server (port 6066) for submissions
    #
    # 3. Master initializes:
    #    - Creates empty worker registry
    #    - Creates empty application registry
    #    - Starts heartbeat checker thread
    #    - Starts resource scheduler thread
    #
    # 4. Master is ready:
    #    - URL: spark://spark-master:7077
    #    - Web UI: http://spark-master:8080
    #    - Waiting for worker registrations
    # ========================================================================
    print("Step 1️⃣: Starting Spark Master")
    print("   Command: $SPARK_HOME/sbin/start-master.sh\n")
    
    print("   📋 Master Configuration (spark-env.sh):")
    print("      export SPARK_MASTER_HOST=spark-master")
    print("      export SPARK_MASTER_PORT=7077")
    print("      export SPARK_MASTER_WEBUI_PORT=8080")
    print("      export SPARK_MASTER_OPTS='-Dspark.deploy.defaultCores=4'")
    print("")
    
    print("   🚀 Master Starting...")
    print("      • Reading configuration files")
    print("      • Binding to spark-master:7077 (RPC)")
    print("      • Binding to spark-master:8080 (Web UI)")
    print("      • Binding to spark-master:6066 (REST)")
    print("      • Initializing worker registry")
    print("      • Starting heartbeat checker")
    print("      • Starting resource scheduler")
    print("")
    
    print("   ✅ Master Started Successfully!")
    print("      • Master URL: spark://spark-master:7077")
    print("      • Web UI: http://spark-master:8080")
    print("      • Status: ALIVE")
    print("      • Workers: 0 (waiting for registrations)")
    print("")
    
    # ========================================================================
    # STEP 2: START WORKERS
    # ========================================================================
    # WHAT HAPPENS:
    # When you run: $SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077
    #
    # 1. Script reads configuration:
    #    - SPARK_WORKER_CORES (available cores)
    #    - SPARK_WORKER_MEMORY (available memory)
    #    - SPARK_WORKER_DIR (work directory for logs)
    #
    # 2. Worker process starts:
    #    - Class: org.apache.spark.deploy.worker.Worker
    #    - Connects to Master at spark://spark-master:7077
    #    - Sends RegisterWorker message
    #
    # 3. Master receives registration:
    #    - Validates worker information
    #    - Assigns worker ID
    #    - Adds to worker registry
    #    - Sends RegisteredWorker response
    #
    # 4. Worker starts services:
    #    - Binds Web UI (port 8081, 8082, 8083...)
    #    - Starts heartbeat thread (sends to Master every 60s)
    #    - Starts executor launcher thread
    #    - Creates work directory
    #
    # 5. Worker is ready:
    #    - Registered with Master
    #    - Resources advertised
    #    - Ready to launch executors
    # ========================================================================
    print("Step 2️⃣: Starting Spark Workers")
    print("   Command: $SPARK_HOME/sbin/start-worker.sh spark://spark-master:7077\n")
    
    workers = [
        ("worker-1", "192.168.1.101", 8, "16g", 8081),
        ("worker-2", "192.168.1.102", 8, "16g", 8082),
        ("worker-3", "192.168.1.103", 8, "16g", 8083),
    ]
    
    for worker_name, ip, cores, memory, webui_port in workers:
        print(f"   🔧 Starting {worker_name} ({ip})...")
        print(f"      Configuration:")
        print(f"         SPARK_WORKER_CORES={cores}")
        print(f"         SPARK_WORKER_MEMORY={memory}")
        print(f"         SPARK_WORKER_DIR=/var/spark/work")
        print("")
        
        print(f"      Registration Process:")
        print(f"         1. Worker connects to Master: spark://spark-master:7077")
        print(f"         2. Worker sends: RegisterWorker(")
        print(f"               id={worker_name},")
        print(f"               host={ip},")
        print(f"               port=7078,")
        print(f"               cores={cores},")
        print(f"               memory={memory}")
        print(f"            )")
        print(f"         3. Master validates and assigns ID")
        print(f"         4. Master sends: RegisteredWorker(workerId=worker-xyz)")
        print(f"         5. Worker starts heartbeat every 60s")
        print("")
        
        print(f"      ✅ {worker_name} Registered!")
        print(f"         • Worker ID: worker-{worker_name}")
        print(f"         • Web UI: http://{ip}:{webui_port}")
        print(f"         • Available: {cores} cores, {memory}")
        print(f"         • Status: ALIVE")
        print("")
    
    print("   📊 Cluster Resources Summary:")
    print("      ┌──────────────┬─────────┬──────────┬────────────────────┐")
    print("      │ Worker       │ Cores   │ Memory   │ Status             │")
    print("      ├──────────────┼─────────┼──────────┼────────────────────┤")
    for worker_name, ip, cores, memory, _ in workers:
        print(f"      │ {worker_name:<12} │ {cores:<7} │ {memory:<8} │ {'ALIVE':<18} │")
    print("      ├──────────────┼─────────┼──────────┼────────────────────┤")
    print(f"      │ {'TOTAL':<12} │ {24:<7} │ {'48g':<8} │ {'3 workers online':<18} │")
    print("      └──────────────┴─────────┴──────────┴────────────────────┘")
    print("")
    
    print("   ✅ Standalone Cluster Ready!")
    print("      • Master: spark://spark-master:7077")
    print("      • Workers: 3 active")
    print("      • Total Resources: 24 cores, 48GB memory")
    print("      • Ready to accept applications")


# ============================================================================
# APPLICATION SUBMISSION
# ============================================================================

def demonstrate_application_submission():
    """
    Demonstrate submitting an application to Standalone cluster.
    
    WHAT THIS SHOWS:
    ----------------
    Complete flow from spark-submit command through executor launch,
    showing all Master-Worker interactions.
    
    SUBMISSION MODES:
    -----------------
    CLIENT MODE:
    - Driver runs on machine where spark-submit executed
    - Driver connects directly to executors
    - Good for interactive work
    - Client must stay connected
    
    CLUSTER MODE:
    - Driver runs on a worker node
    - Cluster runs driver as supervised process
    - Good for production
    - Client can disconnect after submission
    """
    print_section("APPLICATION SUBMISSION TO STANDALONE CLUSTER")
    
    # ========================================================================
    # CLIENT MODE SUBMISSION
    # ========================================================================
    # COMMAND:
    # spark-submit --master spark://spark-master:7077 \\
    #              --deploy-mode client \\
    #              --total-executor-cores 12 \\
    #              --executor-memory 4g \\
    #              --executor-cores 2 \\
    #              my_app.py
    #
    # WHAT HAPPENS:
    # 1. spark-submit parses arguments
    # 2. Driver starts on client machine (where you ran command)
    # 3. Driver connects to Master at spark://spark-master:7077
    # 4. Driver requests executors: 12 cores total, 4GB each, 2 cores/executor
    # 5. Master calculates: 12 cores / 2 cores per executor = 6 executors
    # 6. Master allocates executors across workers
    # 7. Workers launch executor JVM processes
    # 8. Executors connect back to Driver
    # 9. Application runs
    # ========================================================================
    print("Submission Mode: CLIENT MODE")
    print("Command:")
    print("   spark-submit --master spark://spark-master:7077 \\")
    print("                --deploy-mode client \\")
    print("                --total-executor-cores 12 \\")
    print("                --executor-memory 4g \\")
    print("                --executor-cores 2 \\")
    print("                my_app.py")
    print("")
    
    print("Step 1️⃣: Driver Initialization (on client machine)")
    print("   • SparkContext created")
    print("   • Connects to Master: spark://spark-master:7077")
    print("   • Sends application registration:")
    print("      - App Name: MySparkApp")
    print("      - Cores requested: 12")
    print("      - Memory per executor: 4g")
    print("      - Cores per executor: 2")
    print("")
    
    print("Step 2️⃣: Master Processes Request")
    print("   • Master receives registration")
    print("   • Assigns application ID: app-20251213143000-0001")
    print("   • Calculates executor requirements:")
    print("      - Total cores: 12")
    print("      - Cores per executor: 2")
    print("      - Number of executors: 12 / 2 = 6 executors")
    print("      - Memory per executor: 4GB")
    print("")
    
    print("   • Master checks available resources:")
    print("      - worker-1: 8 cores, 16GB available")
    print("      - worker-2: 8 cores, 16GB available")
    print("      - worker-3: 8 cores, 16GB available")
    print("      - Total: 24 cores, 48GB (sufficient!)")
    print("")
    
    print("   • Master allocates executors (spreadOut=true):")
    print("      - Spreads across all workers for fault tolerance")
    print("      ┌──────────┬──────────────┬───────┬────────┐")
    print("      │ Worker   │ Executor ID  │ Cores │ Memory │")
    print("      ├──────────┼──────────────┼───────┼────────┤")
    print("      │ worker-1 │ executor-0   │ 2     │ 4GB    │")
    print("      │ worker-1 │ executor-1   │ 2     │ 4GB    │")
    print("      │ worker-2 │ executor-2   │ 2     │ 4GB    │")
    print("      │ worker-2 │ executor-3   │ 2     │ 4GB    │")
    print("      │ worker-3 │ executor-4   │ 2     │ 4GB    │")
    print("      │ worker-3 │ executor-5   │ 2     │ 4GB    │")
    print("      └──────────┴──────────────┴───────┴────────┘")
    print("")
    
    print("Step 3️⃣: Workers Launch Executors")
    print("   worker-1:")
    print("      • Receives LaunchExecutor(executor-0, 2 cores, 4GB)")
    print("      • Creates work directory: /var/spark/work/app-001/0")
    print("      • Launches JVM:")
    print("         java -Xmx4g -cp spark-assembly.jar \\")
    print("              org.apache.spark.executor.CoarseGrainedExecutorBackend \\")
    print("              --driver-url spark://CoarseGrainedScheduler@client:12345 \\")
    print("              --executor-id 0 --cores 2 --app-id app-001")
    print("      • Executor connects to Driver")
    print("      ✅ executor-0 running on worker-1")
    print("")
    
    print("      • (Same process for executor-1)")
    print("      ✅ executor-1 running on worker-1")
    print("")
    
    print("   worker-2:")
    print("      ✅ executor-2 running on worker-2")
    print("      ✅ executor-3 running on worker-2")
    print("")
    
    print("   worker-3:")
    print("      ✅ executor-4 running on worker-3")
    print("      ✅ executor-5 running on worker-3")
    print("")
    
    print("Step 4️⃣: Driver Ready for Execution")
    print("   • All 6 executors registered with Driver")
    print("   • Total resources available:")
    print("      - 12 cores (6 executors × 2 cores)")
    print("      - 24GB memory (6 executors × 4GB)")
    print("   • Driver can now schedule tasks")
    print("   • Max parallelism: 12 tasks at once")
    print("")


# ============================================================================
# CREATE DEMONSTRATION SPARK SESSION
# ============================================================================

def create_spark_session():
    """
    Create SparkSession configured for Standalone mode.
    
    WHAT THIS DEMONSTRATES:
    -----------------------
    In this demo, we use local mode to simulate Standalone behavior.
    In production, you would use:
        .master("spark://spark-master:7077")
    
    CONFIGURATION EXPLAINED:
    ------------------------
    spark.cores.max = 12
      - Maximum cores to use across ALL executors
      - In Standalone: hard limit, won't use more
      - Equivalent to --total-executor-cores
    
    spark.executor.cores = 2
      - Cores per executor
      - Controls parallelism per executor
      - With 12 max cores: 12/2 = 6 executors
    
    spark.executor.memory = 4g
      - Heap memory per executor
      - Actual memory = 4g + overhead (10% = 400MB)
      - Total = 4.4GB per executor container
    
    spark.deploy.spreadOut = true (default)
      - Spread executors across workers
      - false: pack on fewer workers
    """
    print_section("CREATING SPARK SESSION (STANDALONE MODE)")
    
    print("Configuration for Standalone Cluster:")
    print("   spark.master = spark://spark-master:7077")
    print("   spark.cores.max = 12")
    print("   spark.executor.cores = 2")
    print("   spark.executor.memory = 4g")
    print("   spark.deploy.spreadOut = true")
    print("")
    
    # For demo purposes, use local mode
    print("ℹ️  Demo Mode: Using local[12] to simulate 12-core cluster")
    print("")
    
    spark = (SparkSession.builder
        .appName("StandaloneClusterDemo")
        .master("local[12]")  # Simulates 12 cores
        .config("spark.sql.shuffle.partitions", "12")  # Match core count
        .getOrCreate())
    
    print("✅ SparkSession Created")
    print(f"   • Spark Version: {spark.version}")
    print(f"   • Master: {spark.sparkContext.master}")
    print(f"   • App ID: {spark.sparkContext.applicationId}")
    print(f"   • Default Parallelism: {spark.sparkContext.defaultParallelism}")
    print("")
    
    return spark


# ============================================================================
# RESOURCE ALLOCATION DEMONSTRATION
# ============================================================================

def demonstrate_resource_allocation(spark):
    """
    Demonstrate how Standalone cluster allocates resources during execution.
    
    WHAT THIS SHOWS:
    ----------------
    How tasks are distributed across executors and how Spark uses the
    allocated resources (12 cores across 6 executors).
    
    TASK SCHEDULING:
    ----------------
    1. Action triggers job
    2. Driver creates stages
    3. Driver creates tasks (1 per partition)
    4. Driver schedules tasks on executor cores
    5. Each executor runs up to 'cores' tasks in parallel
    6. With 6 executors × 2 cores = 12 parallel tasks max
    """
    print_section("RESOURCE ALLOCATION & TASK EXECUTION")
    
    print("Creating sample dataset with 12 partitions...")
    print("   df = spark.range(0, 1_200_000).repartition(12)")
    print("")
    
    # Create DataFrame with 12 partitions (matches our 12 cores)
    df = spark.range(0, 1_200_000).repartition(12)
    
    print("✅ DataFrame created with 12 partitions")
    print("   • Each partition: ~100,000 rows")
    print("   • 1 task will be created per partition")
    print("   • 12 tasks total")
    print("")
    
    print("Applying transformations:")
    print("   df_filtered = df.filter(col('id') > 500_000)")
    print("   df_squared = df_filtered.withColumn('squared', col('id') * col('id'))")
    print("")
    
    df_filtered = df.filter(col("id") > 500_000)
    df_squared = df_filtered.withColumn("squared", col("id") * col("id"))
    
    print("Triggering action: count()")
    print("")
    print("Driver's Scheduling Plan:")
    print("   • Job: count()")
    print("   • Stages: 1 (no shuffle needed)")
    print("   • Tasks: 12 (one per partition)")
    print("")
    
    print("Task Distribution Across Executors:")
    print("   ┌─────────────┬──────────┬─────────────────┬──────────────┐")
    print("   │ Executor    │ Worker   │ Tasks Assigned  │ Parallelism  │")
    print("   ├─────────────┼──────────┼─────────────────┼──────────────┤")
    print("   │ executor-0  │ worker-1 │ task-0, task-6  │ 2 at a time  │")
    print("   │ executor-1  │ worker-1 │ task-1, task-7  │ 2 at a time  │")
    print("   │ executor-2  │ worker-2 │ task-2, task-8  │ 2 at a time  │")
    print("   │ executor-3  │ worker-2 │ task-3, task-9  │ 2 at a time  │")
    print("   │ executor-4  │ worker-3 │ task-4, task-10 │ 2 at a time  │")
    print("   │ executor-5  │ worker-3 │ task-5, task-11 │ 2 at a time  │")
    print("   └─────────────┴──────────┴─────────────────┴──────────────┘")
    print("")
    
    print("Execution Timeline:")
    print("   Wave 1 (tasks 0-11 start simultaneously):")
    print("      • All 6 executors start working")
    print("      • 12 tasks running in parallel")
    print("      • Using all 12 cores")
    print("")
    
    start_time = time.time()
    result = df_squared.count()
    execution_time = time.time() - start_time
    
    print(f"✅ Execution Complete!")
    print(f"   • Result: {result:,} rows")
    print(f"   • Execution Time: {execution_time:.4f} seconds")
    print(f"   • All 12 tasks completed")
    print(f"   • Peak parallelism: 12 tasks")
    print("")
    
    print("Resource Utilization:")
    print("   • Cores: 12/12 used (100%)")
    print("   • Executors: 6/6 used (100%)")
    print("   • Workers: 3/3 used (100%)")
    print("   • Efficient distribution across cluster")


# ============================================================================
# MONITORING AND WEB UI
# ============================================================================

def demonstrate_monitoring():
    """
    Demonstrate monitoring capabilities in Standalone mode.
    
    WHAT THIS SHOWS:
    ----------------
    How to monitor your Standalone cluster and running applications
    using the built-in Web UIs.
    
    WEB UIs AVAILABLE:
    ------------------
    1. Master Web UI (port 8080)
    2. Worker Web UIs (ports 8081, 8082, 8083)
    3. Application Web UI (port 4040)
    4. History Server (port 18080)
    """
    print_section("MONITORING STANDALONE CLUSTER")
    
    print("Available Web UIs:")
    print("")
    
    print("1️⃣  Master Web UI: http://spark-master:8080")
    print("   Information Available:")
    print("      • Cluster Summary")
    print("         - Workers: 3")
    print("         - Cores: 24 total, 12 used, 12 free")
    print("         - Memory: 48GB total, 24GB used, 24GB free")
    print("      • Running Applications")
    print("         - App ID, Name, User")
    print("         - Cores used, Memory used")
    print("         - Duration, State")
    print("      • Completed Applications")
    print("         - Application history")
    print("         - Final status (FINISHED, FAILED, KILLED)")
    print("      • Workers List")
    print("         - Worker ID, Address")
    print("         - State (ALIVE, DEAD, DECOMMISSIONED)")
    print("         - Cores, Memory")
    print("         - Running executors")
    print("")
    
    print("2️⃣  Worker Web UIs:")
    print("   worker-1: http://192.168.1.101:8081")
    print("   worker-2: http://192.168.1.102:8082")
    print("   worker-3: http://192.168.1.103:8083")
    print("")
    print("   Information Per Worker:")
    print("      • Worker Summary")
    print("         - Worker ID, State")
    print("         - Cores: 8 total, 4 used, 4 free")
    print("         - Memory: 16GB total, 8GB used, 8GB free")
    print("      • Running Executors")
    print("         - Executor ID, Application")
    print("         - Cores used, Memory used")
    print("         - Logs (stdout, stderr)")
    print("      • Finished Executors")
    print("         - Exit code, duration")
    print("")
    
    print("3️⃣  Application Web UI: http://driver:4040")
    print("   Available During Application Run:")
    print("      • Jobs")
    print("         - Active, completed, failed jobs")
    print("         - Duration, stages, tasks")
    print("      • Stages")
    print("         - Active, pending, completed stages")
    print("         - Input/output size")
    print("         - Shuffle read/write")
    print("         - Task metrics")
    print("      • Storage")
    print("         - Cached RDDs/DataFrames")
    print("         - Memory used per RDD")
    print("         - Partitions cached")
    print("      • Environment")
    print("         - Spark properties")
    print("         - System properties")
    print("         - Classpath entries")
    print("      • Executors")
    print("         - Executor list")
    print("         - Memory usage")
    print("         - Task time, GC time")
    print("         - Shuffle read/write")
    print("         - Thread dump")
    print("      • SQL")
    print("         - Executed SQL queries")
    print("         - Physical plans")
    print("         - Query duration")
    print("")
    
    print("4️⃣  History Server: http://spark-master:18080")
    print("   For Completed Applications:")
    print("      • View all past applications")
    print("      • Same views as Application UI")
    print("      • Requires event logging enabled:")
    print("         spark.eventLog.enabled=true")
    print("         spark.eventLog.dir=hdfs://namenode/spark-logs")
    print("")
    
    print("Monitoring Best Practices:")
    print("   ✅ Check Master UI for cluster health")
    print("   ✅ Monitor Worker UIs for resource usage")
    print("   ✅ Use Application UI during development")
    print("   ✅ Enable History Server for production")
    print("   ✅ Set up external monitoring (Prometheus, Grafana)")
    print("   ✅ Configure alerts for worker failures")


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main function demonstrating Spark Standalone cluster.
    """
    print("\n" + "🎯" * 40)
    print("SPARK STANDALONE CLUSTER MODE")
    print("Built-in Clustering Without YARN/Kubernetes")
    print("🎯" * 40)
    
    # Demonstrate cluster setup
    demonstrate_cluster_setup()
    
    # Demonstrate application submission
    demonstrate_application_submission()
    
    # Create Spark session
    spark = create_spark_session()
    
    # Demonstrate resource allocation
    demonstrate_resource_allocation(spark)
    
    # Demonstrate monitoring
    demonstrate_monitoring()
    
    # Cleanup
    spark.stop()
    
    print("\n" + "=" * 80)
    print("✅ STANDALONE CLUSTER DEMONSTRATION COMPLETE")
    print("=" * 80)
    
    print("\n📚 Summary:")
    print("   ✅ Standalone = Simple, built-in clustering")
    print("   ✅ Master manages resources and scheduling")
    print("   ✅ Workers launch and manage executors")
    print("   ✅ No Hadoop or Kubernetes required")
    print("   ✅ Perfect for dev/test and small clusters")
    
    print("\n🔗 Key Commands:")
    print("   Start Master: $SPARK_HOME/sbin/start-master.sh")
    print("   Start Worker: $SPARK_HOME/sbin/start-worker.sh spark://master:7077")
    print("   Submit App:   spark-submit --master spark://master:7077 app.py")
    print("   Stop All:     $SPARK_HOME/sbin/stop-all.sh")
    
    print("\n💡 When to Use Standalone:")
    print("   ✅ Learning Spark clustering")
    print("   ✅ Development and testing")
    print("   ✅ Small to medium clusters")
    print("   ✅ Simple setup requirements")
    print("   ✅ No existing Hadoop/K8s infrastructure")


if __name__ == "__main__":
    main()
