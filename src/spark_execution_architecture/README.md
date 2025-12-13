# Spark Execution Architecture

Understanding how Spark executes distributed computations.

## Architecture Overview

```
Driver → DAG Scheduler → Task Scheduler → Executors
```

## Key Concepts

- **Driver**: Coordinates execution, creates DAG
- **Executors**: Run tasks, store data
- **DAG Scheduler**: Splits job into stages
- **Task Scheduler**: Assigns tasks to executors

## Transformations

- **Narrow**: map, filter (no shuffle)
- **Wide**: groupBy, join (shuffle required)

## Shuffle

Data redistribution across executors. Expensive!

```python
# Narrow (no shuffle)
df.filter(col("age") > 25)

# Wide (shuffle)
df.groupBy("city").count()
```

## See Also

- **PYSPARK_MASTER_CURRICULUM.md** - Complete architecture section
- **src/cluster_computing/** - Cluster examples
