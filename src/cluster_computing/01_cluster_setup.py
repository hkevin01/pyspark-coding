"""
01_cluster_setup.py
===================

Configure PySpark for cluster computing across multiple machines.

This example shows how to:
1. Set up Spark for different cluster managers
2. Configure executors, memory, and cores
3. Optimize for distributed processing
"""

from pyspark.sql import SparkSession
from pyspark import SparkConf
import os


def create_local_cluster_spark():
    """
    Create a Spark session that simulates a cluster locally.
    
    Good for testing and development.
    """
    print("=" * 70)
    print("1. LOCAL CLUSTER MODE (Simulation)")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("LocalClusterDemo") \
        .master("local[4]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
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
    
    conf = SparkConf() \
        .setAppName("YARNClusterApp") \
        .setMaster("yarn") \
        .set("spark.submit.deployMode", "cluster") \
        .set("spark.executor.instances", "10") \
        .set("spark.executor.cores", "4") \
        .set("spark.executor.memory", "8g") \
        .set("spark.executor.memoryOverhead", "2g") \
        .set("spark.driver.memory", "4g") \
        .set("spark.driver.cores", "2") \
        .set("spark.default.parallelism", "80") \
        .set("spark.sql.shuffle.partitions", "80") \
        .set("spark.dynamicAllocation.enabled", "true") \
        .set("spark.dynamicAllocation.minExecutors", "2") \
        .set("spark.dynamicAllocation.maxExecutors", "20") \
        .set("spark.yarn.queue", "default")
    
    print("📋 YARN Configuration:")
    print(f"   Executors: 10 (dynamic: 2-20)")
    print(f"   Cores per executor: 4")
    print(f"   Memory per executor: 8GB + 2GB overhead")
    print(f"   Total cores: 10 * 4 = 40 cores")
    print(f"   Parallelism: 80 partitions")
    
    print("\n💻 Submit command:")
    print("""
    spark-submit \\
        --master yarn \\
        --deploy-mode cluster \\
        --num-executors 10 \\
        --executor-cores 4 \\
        --executor-memory 8g \\
        --driver-memory 4g \\
        01_cluster_setup.py
    """)
    
    return conf


def create_kubernetes_cluster_config():
    """
    Configuration for Kubernetes cluster (modern cloud-native).
    
    K8s is popular for cloud deployments (AWS EKS, GCP GKE, Azure AKS).
    """
    print("\n" + "=" * 70)
    print("3. KUBERNETES CLUSTER CONFIGURATION")
    print("=" * 70)
    
    conf = SparkConf() \
        .setAppName("K8sClusterApp") \
        .setMaster("k8s://https://kubernetes-api:443") \
        .set("spark.submit.deployMode", "cluster") \
        .set("spark.executor.instances", "10") \
        .set("spark.executor.cores", "4") \
        .set("spark.executor.memory", "8g") \
        .set("spark.driver.memory", "4g") \
        .set("spark.kubernetes.container.image", "apache/spark:3.5.0") \
        .set("spark.kubernetes.namespace", "spark") \
        .set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \
        .set("spark.kubernetes.executor.request.cores", "3") \
        .set("spark.kubernetes.executor.limit.cores", "4")
    
    print("📋 Kubernetes Configuration:")
    print(f"   Container image: apache/spark:3.5.0")
    print(f"   Namespace: spark")
    print(f"   Executors: 10 pods")
    print(f"   Cores per pod: 3 requested, 4 limit")
    print(f"   Memory per pod: 8GB")
    
    print("\n💻 Submit command:")
    print("""
    spark-submit \\
        --master k8s://https://kubernetes-api:443 \\
        --deploy-mode cluster \\
        --conf spark.executor.instances=10 \\
        --conf spark.kubernetes.container.image=apache/spark:3.5.0 \\
        --conf spark.kubernetes.namespace=spark \\
        01_cluster_setup.py
    """)
    
    return conf


def create_standalone_cluster_config():
    """
    Configuration for Spark Standalone cluster.
    
    Simplest cluster manager, built into Spark.
    """
    print("\n" + "=" * 70)
    print("4. STANDALONE CLUSTER CONFIGURATION")
    print("=" * 70)
    
    conf = SparkConf() \
        .setAppName("StandaloneClusterApp") \
        .setMaster("spark://master-node:7077") \
        .set("spark.submit.deployMode", "cluster") \
        .set("spark.executor.cores", "4") \
        .set("spark.executor.memory", "8g") \
        .set("spark.cores.max", "40") \
        .set("spark.driver.memory", "4g")
    
    print("📋 Standalone Configuration:")
    print(f"   Master URL: spark://master-node:7077")
    print(f"   Max cores: 40 across cluster")
    print(f"   Cores per executor: 4")
    print(f"   Memory per executor: 8GB")
    
    print("\n💻 Submit command:")
    print("""
    spark-submit \\
        --master spark://master-node:7077 \\
        --deploy-mode cluster \\
        --executor-memory 8g \\
        --executor-cores 4 \\
        --total-executor-cores 40 \\
        01_cluster_setup.py
    """)
    
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
    
    print("""
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
    """)


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
