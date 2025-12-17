# 🐳 Spark Cluster Simulation with Docker

Simulate a real Spark cluster locally with 1 Master + 3 Workers!

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      SPARK CLUSTER                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │              SPARK MASTER (Driver)                      │   │
│   │              spark-master:7077                          │   │
│   │              Web UI: localhost:8080                     │   │
│   └─────────────────────────────────────────────────────────┘   │
│                            │                                    │
│            ┌───────────────┼───────────────┐                    │
│            │               │               │                    │
│            ▼               ▼               ▼                    │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│   │  WORKER 1   │  │  WORKER 2   │  │  WORKER 3   │            │
│   │  2 cores    │  │  2 cores    │  │  2 cores    │            │
│   │  2GB RAM    │  │  2GB RAM    │  │  2GB RAM    │            │
│   │  :8081      │  │  :8082      │  │  :8083      │            │
│   └─────────────┘  └─────────────┘  └─────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

### 1. Start the Cluster
```bash
docker-compose up -d
```

### 2. Check Cluster Status
```bash
# View running containers
docker-compose ps

# View logs
docker-compose logs -f spark-master
```

### 3. Open Spark Web UI
- **Master UI**: http://localhost:8080
- **Worker 1**: http://localhost:8081
- **Worker 2**: http://localhost:8082
- **Worker 3**: http://localhost:8083

### 4. Submit a Job
```bash
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/example_job.py
```

### 5. Interactive PySpark Shell
```bash
docker exec -it spark-master pyspark \
    --master spark://spark-master:7077
```

### 6. Stop the Cluster
```bash
docker-compose down
```

## 📁 Directory Structure

```
spark-cluster/
├── docker-compose.yml     # Cluster configuration
├── README.md              # This file
├── apps/                  # Your Spark applications
│   └── example_job.py     # Sample distributed job
└── data/                  # Shared data directory
```

## 🔧 Configuration

### Adjust Worker Resources
Edit `docker-compose.yml`:
```yaml
environment:
  - SPARK_WORKER_MEMORY=4G    # Increase memory
  - SPARK_WORKER_CORES=4      # Increase cores
```

### Add More Workers
Copy a worker block and change:
- `container_name`
- `hostname`
- `ports` (use unique port like 8084:8081)

### Scale Workers Dynamically
```bash
docker-compose up -d --scale spark-worker-1=1 --scale spark-worker-2=1 --scale spark-worker-3=3
```

## 💡 What This Demonstrates

1. **Driver Node**: `spark-master` container runs the driver program
2. **Worker Nodes**: `spark-worker-1/2/3` execute tasks in parallel
3. **Cluster Manager**: Spark Standalone scheduler allocates resources
4. **Task Distribution**: Work is split across partitions on different workers
5. **Communication**: Containers communicate over `spark-network`

## 📊 Monitoring

### View Registered Workers
Open http://localhost:8080 → Shows all workers with:
- Cores available
- Memory allocated
- Running applications

### View Job Execution
After submitting a job:
1. Go to http://localhost:8080
2. Click on your application
3. View stages, tasks, and which executor ran each task

### View Task Distribution
In the Spark UI:
- **Stages** tab shows task distribution
- **Executors** tab shows per-worker metrics
- Each task shows which worker executed it

## 🧪 Example Output

When you run `example_job.py`, you'll see:
```
🖥️  WORKER NODE EXECUTION
──────────────────────────────────────────────────────────────────────
Partition Distribution Across Workers:
  spark-worker-1: 4 partitions, 333,333 records
  spark-worker-2: 4 partitions, 333,333 records
  spark-worker-3: 4 partitions, 333,334 records
```

This proves the work is distributed across all three workers!

## 🔗 Relates To

See `src/cluster_computing/quick_info.md` for conceptual overview of:
- Driver vs Worker nodes
- How jobs are scheduled
- How code is distributed
