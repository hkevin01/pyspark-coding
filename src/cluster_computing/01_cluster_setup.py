#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
CLUSTER SETUP - Complete Configuration Guide for Production Deployments
================================================================================

MODULE OVERVIEW:
----------------
This module provides a comprehensive guide to setting up Apache Spark clusters
across different cluster managers (YARN, Kubernetes, Standalone). Understanding
cluster configuration is essential for running Spark in production environments
where you need to process large datasets across multiple machines.

A Spark cluster consists of:
• Driver: Coordinates execution, maintains SparkContext
• Executors: Run tasks, store cached data
• Cluster Manager: Allocates resources (YARN/K8s/Standalone/Mesos)

This guide covers:
1. Cluster architecture fundamentals
2. Configuration for each cluster manager type
3. Memory and core allocation strategies
4. Production deployment best practices
5. Resource tuning and optimization

PURPOSE:
--------
Learn to:
• Deploy Spark on YARN (Hadoop ecosystem)
• Deploy Spark on Kubernetes (cloud-native)
• Deploy Spark on Standalone mode (simplest)
• Calculate optimal memory and core allocation
• Tune for performance and cost efficiency
• Avoid common configuration mistakes

CLUSTER ARCHITECTURE:
---------------------

Single-Node vs Cluster:
┌─────────────────────────────────────────────────────────────────┐
│                        SINGLE NODE (Local Mode)                 │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Driver + Executors (all in one JVM)                        │ │
│  │ master = "local[*]"                                        │ │
│  │ Use case: Development, testing, small data                │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                        CLUSTER MODE                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌──────────────┐         Cluster Manager                     │
│   │    Driver    │◄────── (YARN/K8s/Standalone)                │
│   │  (Master)    │                   │                         │
│   └──────────────┘                   │                         │
│          │                           │                         │
│          │ Coordinates               │ Allocates Resources     │
│          │                           │                         │
│   ┌──────▼──────────────────────────▼───────────────────┐     │
│   │                  Worker Nodes                        │     │
│   ├─────────────────┬─────────────────┬─────────────────┤     │
│   │ Executor 1      │ Executor 2      │ Executor 3      │     │
│   │ • 4 cores       │ • 4 cores       │ • 4 cores       │     │
│   │ • 8GB memory    │ • 8GB memory    │ • 8GB memory    │     │
│   │ • Runs tasks    │ • Runs tasks    │ • Runs tasks    │     │
│   │ • Stores cache  │ • Stores cache  │ • Stores cache  │     │
│   └─────────────────┴─────────────────┴─────────────────┘     │
│                                                                 │
│   Use case: Production, large data, high performance           │
└─────────────────────────────────────────────────────────────────┘

CLUSTER MANAGER COMPARISON:
----------------------------

┌────────────────┬──────────────────────┬─────────────────────────┐
│ Manager        │ Best For             │ Complexity              │
├────────────────┼──────────────────────┼─────────────────────────┤
│ Local          │ Development/Testing  │ ⭐ Simplest             │
│ Standalone     │ Spark-only clusters  │ ⭐⭐ Simple             │
│ YARN           │ Hadoop ecosystem     │ ⭐⭐⭐ Moderate         │
│ Kubernetes     │ Cloud-native/Hybrid  │ ⭐⭐⭐⭐ Complex        │
│ Mesos          │ Multi-framework      │ ⭐⭐⭐⭐ Complex        │
└────────────────┴──────────────────────┴─────────────────────────┘

DEPLOYMENT MODES:
-----------------

Client Mode:
┌────────────────────────────────────────────────────┐
│  Your Machine                    Cluster           │
│  ┌────────────┐                 ┌────────────┐    │
│  │   Driver   │◄───────────────►│ Executors  │    │
│  │ (On client)│                 │            │    │
│  └────────────┘                 └────────────┘    │
│                                                    │
│  • Driver on your machine                         │
│  • Executors on cluster                           │
│  • Good for: Interactive sessions, notebooks      │
│  • Bad for: Production (driver can fail)          │
└────────────────────────────────────────────────────┘

Cluster Mode:
┌────────────────────────────────────────────────────┐
│              All on Cluster                        │
│  ┌────────────┐              ┌────────────┐       │
│  │   Driver   │◄────────────►│ Executors  │       │
│  │ (On cluster)│              │            │       │
│  └────────────┘              └────────────┘       │
│                                                    │
│  • Driver on cluster node                         │
│  • Executors on cluster                           │
│  • Good for: Production (fault tolerant)          │
│  • Bad for: Interactive work (can't see driver)   │
└────────────────────────────────────────────────────┘

RESOURCE ALLOCATION FORMULA:
----------------------------

Memory Hierarchy:
Node Memory (Total RAM on machine)
  ├─ OS Reserved: ~8GB (for Linux OS)
  ├─ Cluster Manager Overhead: ~2-4GB (YARN/K8s)
  └─ Available for Spark: remaining
      ├─ Driver Memory: 2-8GB
      └─ Executor Memory × Number of Executors
          ├─ Execution Memory: 60% (for computations)
          ├─ Storage Memory: 40% (for caching)
          ├─ User Memory: 300MB (for user objects)
          └─ Memory Overhead: 10% (off-heap, JVM)

Formula for Executor Memory:
executor_memory = (node_memory - os_reserved - yarn_overhead) / executors_per_node
memory_overhead = max(executor_memory * 0.10, 384MB)

Example (64GB node):
64GB total
- 8GB OS
- 4GB YARN overhead
= 52GB available
÷ 4 executors per node
= 13GB per executor
  - 1.3GB memory overhead (10%)
  = 11.7GB actual executor memory

Core Allocation:
cores_per_executor = 2-6 (sweet spot: 4-5)
executors_per_node = available_cores / cores_per_executor
total_executors = num_nodes * executors_per_node

Example (32 cores per node):
32 cores
÷ 5 cores per executor
= 6 executors per node

CONFIGURATION PARAMETERS:
-------------------------

Key Configuration Settings:

1. Executor Settings:
   spark.executor.instances         Number of executors
   spark.executor.cores             Cores per executor (2-6)
   spark.executor.memory            Heap memory per executor
   spark.executor.memoryOverhead    Off-heap memory (10% of heap)

2. Driver Settings:
   spark.driver.cores               Driver cores (1-4)
   spark.driver.memory              Driver memory (2-8GB)
   spark.driver.maxResultSize       Max size for results (1GB)

3. Dynamic Allocation:
   spark.dynamicAllocation.enabled  Auto-scale executors
   spark.dynamicAllocation.minExecutors  Minimum (e.g., 2)
   spark.dynamicAllocation.maxExecutors  Maximum (e.g., 100)
   spark.dynamicAllocation.initialExecutors  Starting count

4. Parallelism:
   spark.default.parallelism        RDD parallelism
   spark.sql.shuffle.partitions     DataFrame shuffle partitions

5. Memory Management:
   spark.memory.fraction            Execution + storage (default: 0.6)
   spark.memory.storageFraction     Cache fraction (default: 0.5)
   spark.memory.offHeap.enabled     Use off-heap memory
   spark.memory.offHeap.size        Off-heap size

PERFORMANCE TUNING:
-------------------

Best Practices:
1. Executor Sizing:
   • Small executors (< 4 cores): More parallelism, overhead
   • Large executors (> 8 cores): HDFS throughput issues
   • Sweet spot: 4-6 cores per executor

2. Memory Allocation:
   • Too little: OOM errors, spilling to disk
   • Too much: Wasted resources, long GC pauses
   • Rule: 128MB - 1GB per task

3. Dynamic Allocation:
   • Enable for variable workloads
   • Set reasonable min/max bounds
   • Monitor allocation in Spark UI

4. Partitioning:
   • Target: 128MB - 256MB per partition
   • Rule: 2-4 partitions per executor core
   • Adjust spark.sql.shuffle.partitions

5. Network Optimization:
   • Minimize shuffles (use broadcast joins)
   • Co-locate data (partition by key)
   • Use efficient serialization (Kryo)

COST OPTIMIZATION:
------------------

Cloud Cost Factors:
• Instance type: Compute-optimized vs Memory-optimized
• Spot instances: 70% cheaper, can be interrupted
• Reserved instances: Committed savings
• Right-sizing: Don't over-provision

Cost-Performance Trade-offs:
┌──────────────────┬─────────────┬──────────────┬─────────────┐
│ Configuration    │ Performance │ Cost         │ Use Case    │
├──────────────────┼─────────────┼──────────────┼─────────────┤
│ Few large        │ ⭐⭐⭐      │ $$$$         │ Memory-     │
│ executors        │             │              │ intensive   │
├──────────────────┼─────────────┼──────────────┼─────────────┤
│ Many small       │ ⭐⭐⭐⭐    │ $$$          │ Parallel    │
│ executors        │             │              │ processing  │
├──────────────────┼─────────────┼──────────────┼─────────────┤
│ Spot instances   │ ⭐⭐⭐      │ $            │ Fault-      │
│                  │             │              │ tolerant    │
└──────────────────┴─────────────┴──────────────┴─────────────┘

COMMON MISTAKES:
----------------

❌ Mistake #1: Too few executors
   • Problem: Underutilizes cluster
   • Fix: Calculate optimal executor count

❌ Mistake #2: Too many cores per executor
   • Problem: HDFS throughput bottleneck (> 5 cores)
   • Fix: Keep cores per executor ≤ 5

❌ Mistake #3: Ignoring memory overhead
   • Problem: OOM errors from off-heap memory
   • Fix: Set spark.executor.memoryOverhead (10-15%)

❌ Mistake #4: Not using dynamic allocation
   • Problem: Wasted resources during idle time
   • Fix: Enable dynamic allocation for variable workloads

❌ Mistake #5: Wrong shuffle partitions
   • Problem: Too many small tasks or too few large tasks
   • Fix: Set spark.sql.shuffle.partitions = 2x total cores

SECURITY CONSIDERATIONS:
------------------------

Production Security Checklist:
☐ Enable Spark authentication (spark.authenticate = true)
☐ Enable encryption (spark.network.crypto.enabled = true)
☐ Use Kerberos for Hadoop clusters
☐ Secure Spark UI (spark.ui.filters)
☐ Use secrets management (not hardcoded credentials)
☐ Enable audit logging
☐ Network isolation (VPC, firewalls)
☐ Least privilege IAM roles

See: src/security/01_security_best_practices.py for details

MONITORING & DEBUGGING:
-----------------------

Key Metrics to Monitor:
• Executor Memory Usage: Watch for OOM errors
• GC Time: Should be < 10% of task time
• Shuffle Read/Write: Minimize shuffle size
• Task Duration: Identify slow tasks (skew)
• Spill to Disk: Indicates memory pressure
• Failed Tasks: Retry failures, investigate causes

Spark UI Tabs:
• Jobs: Overall progress and stages
• Stages: Task-level metrics, shuffle data
• Storage: Cached RDD/DataFrame size
• Executors: Per-executor metrics, GC time
• SQL: Query plans, execution times

USAGE EXAMPLES:
---------------

Example 1: Local Development
>>> spark = SparkSession.builder \\
...     .master("local[4]") \\
...     .config("spark.driver.memory", "4g") \\
...     .getOrCreate()

Example 2: YARN Production
>>> spark-submit \\
...     --master yarn \\
...     --deploy-mode cluster \\
...     --num-executors 50 \\
...     --executor-cores 4 \\
...     --executor-memory 16g \\
...     --driver-memory 8g \\
...     my_spark_job.py

Example 3: Kubernetes
>>> spark-submit \\
...     --master k8s://https://k8s-api:443 \\
...     --deploy-mode cluster \\
...     --conf spark.executor.instances=20 \\
...     --conf spark.kubernetes.container.image=spark:3.5.0 \\
...     my_spark_job.py

TARGET AUDIENCE:
----------------
• DevOps engineers deploying Spark clusters
• Data engineers optimizing production jobs
• Platform teams managing shared Spark infrastructure
• Anyone moving from local development to production

RELATED RESOURCES:
------------------
• spark_execution_architecture/03_driver_yarn_cluster_interaction.py
• cluster_computing/07_resource_management.py
• security/01_security_best_practices.py
• Spark Configuration Guide: https://spark.apache.org/docs/latest/configuration.html
• Tuning Guide: https://spark.apache.org/docs/latest/tuning.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 2.0.0 - Comprehensive Cluster Setup Guide
UPDATED: 2024
================================================================================
"""

import os

from pyspark import SparkConf
from pyspark.sql import SparkSession


def create_local_cluster_spark():
    """
    Create a Spark session that simulates a cluster locally.

    Good for testing and development.
    """
    print("=" * 70)
    print("1. LOCAL CLUSTER MODE (Simulation)")
    print("=" * 70)

    spark = (
        SparkSession.builder.appName("LocalClusterDemo")
        .master("local[4]")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    print(f"✅ Spark version: {spark.version}")
    print(f"✅ Master: {spark.sparkContext.master}")
    print(f"✅ Default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"✅ Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

    return spark


def create_yarn_cluster_config():
    """
    Configuration for YARN cluster (Hadoop ecosystem).

    YARN (Yet Another Resource Negotiator) is common in enterprise Hadoop clusters.
    """
    print("\n" + "=" * 70)
    print("2. YARN CLUSTER CONFIGURATION")
    print("=" * 70)

    conf = (
        SparkConf()
        .setAppName("YARNClusterApp")
        .setMaster("yarn")
        .set("spark.submit.deployMode", "cluster")
        .set("spark.executor.instances", "10")
        .set("spark.executor.cores", "4")
        .set("spark.executor.memory", "8g")
        .set("spark.executor.memoryOverhead", "2g")
        .set("spark.driver.memory", "4g")
        .set("spark.driver.cores", "2")
        .set("spark.default.parallelism", "80")
        .set("spark.sql.shuffle.partitions", "80")
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.dynamicAllocation.minExecutors", "2")
        .set("spark.dynamicAllocation.maxExecutors", "20")
        .set("spark.yarn.queue", "default")
    )

    print("📋 YARN Configuration:")
    print(f"   Executors: 10 (dynamic: 2-20)")
    print(f"   Cores per executor: 4")
    print(f"   Memory per executor: 8GB + 2GB overhead")
    print(f"   Total cores: 10 * 4 = 40 cores")
    print(f"   Parallelism: 80 partitions")

    print("\n💻 Submit command:")
    print(
        """
    spark-submit \\
        --master yarn \\
        --deploy-mode cluster \\
        --num-executors 10 \\
        --executor-cores 4 \\
        --executor-memory 8g \\
        --driver-memory 4g \\
        01_cluster_setup.py
    """
    )

    return conf


def create_kubernetes_cluster_config():
    """
    Configuration for Kubernetes cluster (modern cloud-native).

    K8s is popular for cloud deployments (AWS EKS, GCP GKE, Azure AKS).
    """
    print("\n" + "=" * 70)
    print("3. KUBERNETES CLUSTER CONFIGURATION")
    print("=" * 70)

    conf = (
        SparkConf()
        .setAppName("K8sClusterApp")
        .setMaster("k8s://https://kubernetes-api:443")
        .set("spark.submit.deployMode", "cluster")
        .set("spark.executor.instances", "10")
        .set("spark.executor.cores", "4")
        .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "4g")
        .set("spark.kubernetes.container.image", "apache/spark:3.5.0")
        .set("spark.kubernetes.namespace", "spark")
        .set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
        .set("spark.kubernetes.executor.request.cores", "3")
        .set("spark.kubernetes.executor.limit.cores", "4")
    )

    print("📋 Kubernetes Configuration:")
    print(f"   Container image: apache/spark:3.5.0")
    print(f"   Namespace: spark")
    print(f"   Executors: 10 pods")
    print(f"   Cores per pod: 3 requested, 4 limit")
    print(f"   Memory per pod: 8GB")

    print("\n💻 Submit command:")
    print(
        """
    spark-submit \\
        --master k8s://https://kubernetes-api:443 \\
        --deploy-mode cluster \\
        --conf spark.executor.instances=10 \\
        --conf spark.kubernetes.container.image=apache/spark:3.5.0 \\
        --conf spark.kubernetes.namespace=spark \\
        01_cluster_setup.py
    """
    )

    return conf


def create_standalone_cluster_config():
    """
    Configuration for Spark Standalone cluster.

    Simplest cluster manager, built into Spark.
    """
    print("\n" + "=" * 70)
    print("4. STANDALONE CLUSTER CONFIGURATION")
    print("=" * 70)

    conf = (
        SparkConf()
        .setAppName("StandaloneClusterApp")
        .setMaster("spark://master-node:7077")
        .set("spark.submit.deployMode", "cluster")
        .set("spark.executor.cores", "4")
        .set("spark.executor.memory", "8g")
        .set("spark.cores.max", "40")
        .set("spark.driver.memory", "4g")
    )

    print("📋 Standalone Configuration:")
    print(f"   Master URL: spark://master-node:7077")
    print(f"   Max cores: 40 across cluster")
    print(f"   Cores per executor: 4")
    print(f"   Memory per executor: 8GB")

    print("\n💻 Submit command:")
    print(
        """
    spark-submit \\
        --master spark://master-node:7077 \\
        --deploy-mode cluster \\
        --executor-memory 8g \\
        --executor-cores 4 \\
        --total-executor-cores 40 \\
        01_cluster_setup.py
    """
    )

    return conf


def demonstrate_cluster_resources(spark):
    """
    Show cluster resources and create sample distributed workload.
    """
    print("\n" + "=" * 70)
    print("5. CLUSTER RESOURCES & DEMO")
    print("=" * 70)

    # Get cluster info
    sc = spark.sparkContext
    print(f"📊 Cluster Information:")
    print(f"   Master: {sc.master}")
    print(f"   App ID: {sc.applicationId}")
    print(f"   Default parallelism: {sc.defaultParallelism}")

    # Create sample data distributed across cluster
    print("\n🔄 Creating distributed dataset...")
    data = range(1, 1000001)  # 1 million numbers
    rdd = sc.parallelize(data, numSlices=sc.defaultParallelism)

    print(f"   Total records: {rdd.count():,}")
    print(f"   Partitions: {rdd.getNumPartitions()}")
    print(f"   Records per partition: ~{1000000 // rdd.getNumPartitions():,}")

    # Simple distributed computation
    print("\n⚡ Running distributed computation...")
    sum_result = rdd.sum()
    print(f"   Sum of 1 to 1,000,000 = {sum_result:,}")

    # Show partition distribution
    print("\n�� Partition distribution:")
    partition_sizes = rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
    for i, size in enumerate(partition_sizes):
        print(f"   Partition {i}: {size:,} records")


def demonstrate_memory_tuning():
    """
    Show memory configuration best practices.
    """
    print("\n" + "=" * 70)
    print("6. MEMORY TUNING GUIDE")
    print("=" * 70)

    print(
        """
📊 Memory Hierarchy:
┌─────────────────────────────────────────┐
│         Total Node Memory (64GB)        │
├─────────────────────────────────────────┤
│  OS & System: ~8GB                      │
├─────────────────────────────────────────┤
│  YARN/K8s Overhead: ~4GB                │
├─────────────────────────────────────────┤
│  Available for Spark: 52GB              │
│  ┌────────────────────────────────────┐ │
│  │ Driver: 4GB                        │ │
│  ├────────────────────────────────────┤ │
│  │ Executors (4x): 12GB each          │ │
│  │ ┌────────────────────────────────┐ │ │
│  │ │ Executor Memory: 10GB          │ │ │
│  │ │ ├─ Execution: 6GB (60%)        │ │ │
│  │ │ ├─ Storage: 4GB (40%)          │ │ │
│  │ │ └─ Other: 0.5GB                │ │ │
│  │ ├────────────────────────────────┤ │ │
│  │ │ Memory Overhead: 2GB           │ │ │
│  │ └────────────────────────────────┘ │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘

💡 Formula:
   executor_memory = (node_memory - OS - overhead) / executors_per_node
   memory_overhead = max(executor_memory * 0.1, 384MB)
   
📋 Recommended Settings:
   Small cluster (< 10 nodes):
   - Executor memory: 4-8GB
   - Executor cores: 2-4
   
   Medium cluster (10-50 nodes):
   - Executor memory: 8-16GB
   - Executor cores: 4-6
   
   Large cluster (> 50 nodes):
   - Executor memory: 16-32GB
   - Executor cores: 4-8
    """
    )


def main():
    """
    Main function to demonstrate cluster configurations.
    """
    print("🖥️ PYSPARK CLUSTER COMPUTING SETUP")
    print("=" * 70)

    # 1. Create local cluster for demo
    spark = create_local_cluster_spark()

    # 2. Show different cluster configurations
    yarn_conf = create_yarn_cluster_config()
    k8s_conf = create_kubernetes_cluster_config()
    standalone_conf = create_standalone_cluster_config()

    # 3. Demonstrate cluster resources
    demonstrate_cluster_resources(spark)

    # 4. Memory tuning guide
    demonstrate_memory_tuning()

    print("\n" + "=" * 70)
    print("✅ SETUP COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Local mode: Good for testing (local[N])")
    print("   2. YARN: Enterprise Hadoop clusters")
    print("   3. Kubernetes: Cloud-native deployments")
    print("   4. Standalone: Simple Spark-only clusters")
    print("   5. Memory: executor_memory + overhead ~= node_memory / executors")
    print("   6. Cores: 2-8 cores per executor is typical")
    print("   7. Parallelism: 2-4 partitions per core")

    spark.stop()


if __name__ == "__main__":
    main()
