# Cluster Computing with PySpark 🖥️⚡

This folder contains examples of using **PySpark on distributed clusters** to process massive datasets across multiple computers.

## 📚 What is Cluster Computing?

**Cluster computing** means distributing computation across multiple machines (nodes) to process data that's too large for a single computer. PySpark orchestrates this distribution automatically.

### Key Concepts

1. **Driver Node**: The main program that coordinates work
2. **Worker Nodes**: Machines that execute tasks in parallel
3. **Executors**: Processes on worker nodes that run computations
4. **Partitions**: Data splits distributed across executors
5. **Shuffling**: Data movement between nodes during operations

---

## 🏗️ Cluster Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      DRIVER NODE                        │
│  ┌──────────────┐         ┌─────────────────────────┐  │
│  │ SparkContext │ ────▶   │   Cluster Manager       │  │
│  │  (Your Code) │         │  (YARN/Mesos/K8s/Local) │  │
│  └──────────────┘         └─────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┐
        ▼               ▼               ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ WORKER 1    │ │ WORKER 2    │ │ WORKER 3    │
│ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │
│ │Executor │ │ │ │Executor │ │ │ │Executor │ │
│ │ Core 1  │ │ │ │ Core 1  │ │ │ │ Core 1  │ │
│ │ Core 2  │ │ │ │ Core 2  │ │ │ │ Core 2  │ │
│ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │
│  Partition  │ │  Partition  │ │  Partition  │
│   1, 2, 3   │ │   4, 5, 6   │ │   7, 8, 9   │
└─────────────┘ └─────────────┘ └─────────────┘
```

---

## 📦 Examples in This Folder

### Core Cluster Computing Concepts

### 1. **01_cluster_setup.py**
- Configure Spark for cluster mode
- Set executor memory, cores, and parallelism
- Different cluster managers (YARN, Kubernetes, Standalone)

### 2. **02_data_partitioning.py**
- Understand partitioning strategies
- Repartition vs coalesce
- Optimize for cluster performance

### 3. **03_distributed_joins.py**
- Join large datasets across nodes
- Broadcast joins for small tables
- Shuffle optimization

### 4. **04_aggregations_at_scale.py**
- Aggregate billions of rows
- Window functions across partitions
- Group-by optimization

### 5. **05_fault_tolerance.py**
- Checkpointing for lineage truncation
- Handle node failures
- Data recovery strategies

### 6. **06_gpu_accelerated_udfs.py** ✨
- Use GPUs on each cluster node
- PyTorch and TensorFlow GPU UDFs
- Distributed deep learning inference
- Batch processing optimization
- Multi-GPU strategies

### 7. **07_resource_management.py**
- Dynamic allocation
- Memory tuning (executor, driver, overhead)
- CPU core allocation

### 8. **08_shuffle_optimization.py**
- Minimize shuffling
- Partition keys for joins
- Broadcast variables

### 9. **09_cluster_monitoring.py**
- Spark UI metrics
- Track executor utilization
- Identify bottlenecks

### Real Cluster Deployments (NEW!)

### 10. **10_yarn_cluster_example.py** ⭐ NEW
- Complete YARN cluster configuration
- Dynamic allocation with shuffle service
- Queue management for multi-tenancy
- Real ETL pipeline on YARN
- Production spark-submit examples
- **Best for:** Enterprise Hadoop clusters, 100+ nodes

### 11. **11_kubernetes_cluster_example.py** ⭐ NEW
- Kubernetes-native Spark deployment
- Complete YAML manifests (namespace, RBAC, service accounts)
- Pod-based executor management
- Resource requests and limits
- Container image configuration
- **Best for:** Cloud deployments (EKS, GKE, AKS), 10-1000 nodes

### 12. **12_standalone_cluster_example.py** ⭐ NEW
- Spark Standalone cluster setup (simplest option)
- Master and worker configuration scripts
- No external dependencies (no Hadoop, no K8s)
- Complete setup/monitoring guide
- **Best for:** Small clusters, dev/test, on-premises, < 100 nodes

📖 **See also:** `CLUSTER_MANAGERS_GUIDE.md` for detailed comparison and decision matrix

### Architecture Fundamentals (Interview Essentials)

### 13. **13_driver_responsibilities.py** 🎯 NEW
- What the Spark Driver does
- SparkSession creation and configuration
- Logical plan building (lazy evaluation)
- Job/stage/task splitting
- Task scheduling with locality awareness
- Result collection (with OOM warnings)
- Broadcast variable management
- Executor monitoring and health tracking
- **Best for:** Understanding Spark architecture, interview prep

### 14. **14_executor_responsibilities.py** 🎯 NEW
- What Spark Executors do
- Execute tasks on data partitions
- Cache/persist partitions (memory/disk)
- Shuffle read/write operations
- Unified memory management
- Fault tolerance and recovery
- Task execution lifecycle
- **Best for:** Understanding distributed execution, debugging performance

### 15. **15_dag_lazy_evaluation.py** 🎯 NEW
- DAG (Directed Acyclic Graph) building
- Lazy evaluation explained
- Transformations vs actions
- Catalyst optimizer optimizations
- Logical vs physical plans
- Stage boundaries at shuffles
- DAG reuse with caching
- **Best for:** Query optimization, understanding execution flow

---

## 🚀 Quick Start

### Local Cluster Simulation (Testing)

```python
from pyspark.sql import SparkSession

# Simulate 3-node cluster locally
spark = SparkSession.builder \
    .appName("LocalClusterTest") \
    .master("local[3]")  # 3 cores = 3 "workers"
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# Your code here
df = spark.read.csv("large_file.csv")
result = df.groupBy("category").count()
result.show()
```

### Real Cluster (YARN/Kubernetes)

```bash
# Submit to YARN cluster
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 10 \
    --executor-cores 4 \
    --executor-memory 8g \
    --driver-memory 4g \
    your_script.py

# Submit to Kubernetes
spark-submit \
    --master k8s://https://kubernetes-api:443 \
    --deploy-mode cluster \
    --conf spark.executor.instances=10 \
    --conf spark.kubernetes.container.image=spark:3.5.0 \
    your_script.py
```

---

## 🔧 Cluster Configuration Cheat Sheet

### Memory Configuration

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g")           # Driver node memory
    .config("spark.executor.memory", "8g")         # Each executor memory
    .config("spark.executor.memoryOverhead", "2g") # Off-heap memory
    .config("spark.memory.fraction", "0.8")        # Heap for execution/storage
    .getOrCreate()
```

### Parallelism Configuration

```python
spark = SparkSession.builder \
    .config("spark.default.parallelism", "200")        # RDD operations
    .config("spark.sql.shuffle.partitions", "200")     # DataFrame shuffles
    .config("spark.executor.cores", "4")               # Cores per executor
    .config("spark.executor.instances", "10")          # Number of executors
    .getOrCreate()
```

### Network Configuration

```python
spark = SparkSession.builder \
    .config("spark.network.timeout", "800s")           # Network timeout
    .config("spark.shuffle.compress", "true")          # Compress shuffle data
    .config("spark.shuffle.file.buffer", "1m")         # Shuffle buffer size
    .config("spark.reducer.maxSizeInFlight", "96m")    # Reduce fetch size
    .getOrCreate()
```

---

## 📊 Performance Guidelines

### Data Size → Cluster Size

| <sub>Data Size</sub> | <sub>Executors</sub> | <sub>Cores/Executor</sub> | <sub>Memory/Executor</sub> | <sub>Total Cores</sub> |
|-----------|-----------|----------------|-----------------|-------------|
| <sub>10 GB</sub> | <sub>2-3</sub> | <sub>2-4</sub> | <sub>4-8 GB</sub> | <sub>4-12</sub> |
| <sub>100 GB</sub> | <sub>5-10</sub> | <sub>4-8</sub> | <sub>8-16 GB</sub> | <sub>20-80</sub> |
| <sub>1 TB</sub> | <sub>20-50</sub> | <sub>4-8</sub> | <sub>16-32 GB</sub> | <sub>80-400</sub> |
| <sub>10 TB</sub> | <sub>100-200</sub> | <sub>4-8</sub> | <sub>32-64 GB</sub> | <sub>400-1600</sub> |

### Partition Guidelines

```python
# Rule of thumb: 2-4 partitions per CPU core
num_partitions = num_executors * cores_per_executor * 2

# For 10 executors with 4 cores each:
# 10 * 4 * 2 = 80 partitions
df = df.repartition(80)
```

---

## 🎯 Best Practices

### ✅ DO

1. **Partition your data properly**
   ```python
   # Good: Balanced partitions
   df = df.repartition(num_partitions, "key_column")
   ```

2. **Use broadcast joins for small tables**
   ```python
   from pyspark.sql.functions import broadcast
   result = large_df.join(broadcast(small_df), "id")
   ```

3. **Cache frequently-used DataFrames**
   ```python
   df.cache()
   df.count()  # Materialize cache
   ```

4. **Enable dynamic allocation**
   ```python
   spark.conf.set("spark.dynamicAllocation.enabled", "true")
   spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
   spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
   ```

5. **Monitor and tune**
   - Check Spark UI (http://driver:4040)
   - Look for data skew
   - Identify slow stages

### ❌ DON'T

1. **Don't use too many small partitions** (overhead)
2. **Don't ignore data skew** (some partitions much larger)
3. **Don't cache everything** (limited memory)
4. **Don't forget to unpersist** when done
5. **Don't use collect() on large data** (OOM on driver)

---

## 🔍 Troubleshooting

### Problem: Out of Memory

**Symptoms**: `java.lang.OutOfMemoryError`

**Solutions**:
```python
# Increase executor memory
.config("spark.executor.memory", "16g")
.config("spark.executor.memoryOverhead", "4g")

# Increase partitions (smaller per-partition data)
df = df.repartition(400)

# Avoid wide transformations
df.cache()  # Cache before shuffle
```

### Problem: Slow Shuffles

**Symptoms**: Long shuffle read/write times in Spark UI

**Solutions**:
```python
# Use broadcast for small tables
broadcast(small_df)

# Increase shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Repartition on join key before join
df1 = df1.repartition("join_key")
df2 = df2.repartition("join_key")
result = df1.join(df2, "join_key")
```

### Problem: Data Skew

**Symptoms**: One task takes much longer than others

**Solutions**:
```python
# Salting technique: Add random suffix to skewed key
from pyspark.sql.functions import concat, lit, rand

df_salted = df.withColumn("salted_key", 
    concat(col("skewed_key"), lit("_"), (rand() * 10).cast("int")))
```

---

## 📈 Monitoring Commands

### Check Cluster Resources

```bash
# YARN cluster
yarn application -list
yarn application -status application_id

# Kubernetes cluster
kubectl get pods -n spark
kubectl logs spark-driver-pod
```

### Spark UI Metrics to Watch

- **Executor Tab**: Memory usage, active tasks
- **Stages Tab**: Shuffle read/write, task time
- **Storage Tab**: Cached RDDs/DataFrames
- **SQL Tab**: Query plans, physical plans

---

## 🎓 Interview Questions

### Q1: "How does PySpark distribute data across a cluster?"

**Answer**: PySpark partitions data into chunks and distributes them across executor processes on worker nodes. Each partition is processed independently in parallel. The number of partitions is controlled by `spark.default.parallelism` (RDDs) or `spark.sql.shuffle.partitions` (DataFrames).

### Q2: "What's the difference between repartition and coalesce?"

**Answer**: 
- `repartition(N)`: Can increase/decrease partitions, triggers full shuffle
- `coalesce(N)`: Only decreases partitions, avoids shuffle by merging adjacent partitions
- Use `repartition` to increase parallelism, `coalesce` to reduce output files

### Q3: "How do you optimize joins on large tables?"

**Answer**:
1. **Broadcast small tables**: `broadcast(small_df)` sends to all executors
2. **Partition on join key**: Pre-partition both tables on the join column
3. **Filter early**: Reduce data before join
4. **Bucketing**: Pre-bucket tables for repeated joins

### Q4: "Explain Spark's shuffle operation"

**Answer**: Shuffle redistributes data across partitions, typically for joins, groupBy, or repartition. It involves:
1. **Map side**: Each executor writes shuffle data to disk
2. **Network transfer**: Data sent to appropriate executors
3. **Reduce side**: Executors read and process received data

Shuffles are expensive (disk I/O, network, serialization). Minimize by using broadcast joins and reducing partition count.

---

## 📚 Resources

- [Apache Spark Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Spark Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
- [Tuning Spark Applications](https://spark.apache.org/docs/latest/tuning.html)

---

**Next Steps**: Run the examples to see cluster computing in action!