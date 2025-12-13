# Performance Optimization

Strategies for optimizing PySpark applications.

## Quick Wins

```python
# 1. Broadcast small tables
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "key")

# 2. Adjust shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# 3. Enable adaptive query execution
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 4. Cache reused DataFrames
df.cache()

# 5. Repartition before expensive ops
df.repartition(200, "key")

# 6. Coalesce before writing
df.coalesce(10).write.parquet("output/")
```

## Join Strategies

1. **Broadcast Join** - Small table (< 100 MB)
2. **Shuffle Hash Join** - Medium tables
3. **Sort Merge Join** - Large tables (default)

## Configuration

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.instances", "20") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

## Rules of Thumb

- **Executor memory**: 2-8 GB per executor
- **Executor cores**: 5 cores optimal
- **Shuffle partitions**: 2-3x number of cores
- **File size**: 128 MB - 1 GB per file

## See Also

- **PYSPARK_MASTER_CURRICULUM.md** - Complete optimization section
- **docs/gpu_vs_cpu_decision_matrix.md** - Hardware decisions
