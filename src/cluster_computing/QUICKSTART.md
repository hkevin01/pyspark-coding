# Cluster Computing - Quick Start 🚀

## Run Examples Locally

All examples work in local mode to simulate a cluster:

```bash
cd /home/kevin/Projects/pyspark-coding/src/cluster_computing

# 1. Cluster setup and configuration
python 01_cluster_setup.py

# 2. Data partitioning strategies
python 02_data_partitioning.py
```

## Key Concepts (30-Second Summary)

### What is Cluster Computing?
**Distributing computation across multiple computers to process data too large for one machine.**

### Core Components
- **Driver**: Your code, coordinates work
- **Executors**: Run on worker nodes, process partitions
- **Partitions**: Data chunks distributed across executors

### Critical Formula
```
num_partitions = num_executors × cores_per_executor × (2-4)
```

### Example
- 10 executors × 4 cores = 40 cores
- Recommended partitions: 80-160

## Interview Prep

### Q: "How does Spark distribute data?"
**A**: Data is split into partitions distributed across executor processes. Each partition is processed independently in parallel.

### Q: "repartition vs coalesce?"
**A**: 
- `repartition()`: Full shuffle, can increase/decrease
- `coalesce()`: No shuffle, only decreases by merging

### Q: "How to optimize joins?"
**A**: 
1. Broadcast small tables
2. Partition both tables on join key
3. Filter early to reduce data

## Quick Reference

| Data Size | Executors | Cores/Exec | Memory/Exec |
|-----------|-----------|------------|-------------|
| 10 GB     | 2-3       | 2-4        | 4-8 GB      |
| 100 GB    | 5-10      | 4-8        | 8-16 GB     |
| 1 TB      | 20-50     | 4-8        | 16-32 GB    |
| 10 TB     | 100-200   | 4-8        | 32-64 GB    |

**Partition Size**: 128-256 MB per partition (sweet spot)

