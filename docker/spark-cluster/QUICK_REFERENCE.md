# ⚡ Spark Cluster Quick Reference

## 🚀 Essential Commands

### Cluster Management
```bash
# Start cluster
docker compose up -d

# Stop cluster
docker compose down

# Restart cluster
docker compose restart

# View status
docker compose ps

# Scale workers (add more)
docker compose up -d --scale spark-worker=5
```

### Job Execution
```bash
# Submit job to cluster
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/example_job.py

# Submit with custom config
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --executor-memory 1G \
    --executor-cores 1 \
    /opt/spark-apps/your_job.py
```

### Monitoring
```bash
# View master logs
docker logs spark-master

# Follow master logs
docker logs -f spark-master

# View worker logs
docker logs spark-worker-1
docker logs spark-worker-2
docker logs spark-worker-3

# Check resource usage
docker stats
```

### Interactive Shell
```bash
# PySpark shell on master
docker exec -it spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077

# Bash shell on master
docker exec -it spark-master bash

# Bash shell on worker
docker exec -it spark-worker-1 bash
```

## 🌐 Web UIs

| Service | URL | Description |
|---------|-----|-------------|
| **Master UI** | http://localhost:9080 | Cluster status, workers, apps |
| **Worker 1** | http://localhost:8081 | Worker 1 executors |
| **Worker 2** | http://localhost:8082 | Worker 2 executors |
| **Worker 3** | http://localhost:8083 | Worker 3 executors |
| **Application** | http://localhost:4040 | Active job details (during run) |

## 📂 File Locations

### On Host
```
docker/spark-cluster/
├── apps/          → Your PySpark scripts
├── data/          → Input/output data
└── docker-compose.yml
```

### In Containers
```
/opt/spark-apps/   → Mounted from ./apps
/opt/spark-data/   → Mounted from ./data
/opt/spark/        → Spark installation
```

## 🐛 Troubleshooting

### Cluster won't start
```bash
# Check for port conflicts
sudo lsof -i :9080
sudo lsof -i :7077
sudo lsof -i :8081

# View startup logs
docker compose logs spark-master
```

### Workers not registering
```bash
# Check master logs
docker logs spark-master | grep "Registering worker"

# Check network
docker network inspect spark-cluster_spark-network

# Restart workers
docker compose restart spark-worker-1 spark-worker-2 spark-worker-3
```

### Job fails
```bash
# View detailed logs
docker logs spark-master 2>&1 | grep ERROR

# Check executor logs
docker exec spark-master cat /opt/spark/work/app-*/*/stderr

# Verify Python script syntax
docker exec spark-master python3 /opt/spark-apps/example_job.py
```

### Out of memory
```bash
# Increase worker memory in docker-compose.yml
environment:
  - SPARK_WORKER_MEMORY=4G  # Changed from 2G

# Restart cluster
docker compose down && docker compose up -d
```

## 📊 Performance Tuning

### Partition Count
```python
# Recommended: 2-3x number of cores
rdd = sc.parallelize(data, numSlices=18)  # 3 workers × 2 cores × 3

# For DataFrames
df = spark.range(1000000, numPartitions=18)
```

### Executor Configuration
```bash
# In spark-submit
--executor-memory 2G \
--executor-cores 2 \
--num-executors 3
```

### Shuffle Partitions
```python
# For joins and aggregations
spark.conf.set("spark.sql.shuffle.partitions", "18")
```

## 🔧 Configuration

### Master Configuration
```yaml
environment:
  - SPARK_MASTER_HOST=spark-master
  - SPARK_MASTER_PORT=7077
  - SPARK_MASTER_WEBUI_PORT=8080
```

### Worker Configuration
```yaml
environment:
  - SPARK_WORKER_MEMORY=2G
  - SPARK_WORKER_CORES=2
```

## 📈 Common Operations

### View Cluster Info
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
sc = spark.sparkContext

print(f"Master: {sc.master}")
print(f"App ID: {sc.applicationId}")
print(f"Default Parallelism: {sc.defaultParallelism}")
```

### Check Partition Distribution
```python
rdd = sc.parallelize(range(1000), numSlices=12)
print(f"Partitions: {rdd.getNumPartitions()}")
print(f"Partition sizes: {rdd.glom().map(len).collect()}")
```

### Force Repartition
```python
# Increase partitions
df_repartitioned = df.repartition(24)

# Decrease partitions (use coalesce)
df_coalesced = df.coalesce(6)
```

## 🎯 Best Practices

### ✅ Do's
- Use PROCESS_LOCAL tasks (check Spark UI)
- Monitor memory usage with `docker stats`
- Keep data partitions balanced
- Cache frequently used datasets
- Use persist() for iterative algorithms

### ❌ Don'ts
- Don't use `.collect()` on large datasets
- Avoid wide transformations unnecessarily
- Don't create too many small partitions
- Avoid UDFs when native functions exist
- Don't mix RDD and DataFrame APIs

## 🆘 Emergency Recovery

### Complete Reset
```bash
# Stop and remove everything
docker compose down -v

# Remove network
docker network rm spark-cluster_spark-network

# Clean restart
docker compose up -d
```

### Clean Temporary Files
```bash
# On master
docker exec spark-master rm -rf /tmp/spark-*

# On workers
docker exec spark-worker-1 rm -rf /tmp/spark-*
```

---

**Quick Access**: Keep this file open in a terminal or browser for instant reference! 📌
