# Spark Session

Entry point for PySpark applications.

## Quick Start

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Read data
df = spark.read.csv("data.csv", header=True)

# Transform
df = df.filter(col("age") > 25)

# Write
df.write.parquet("output/")

# Cleanup
spark.stop()
```

## Key Operations

- **Read**: csv, parquet, json, jdbc, hive
- **SQL**: `spark.sql("SELECT * FROM table")`
- **UDF**: Custom functions
- **Config**: Runtime configuration

## See Also

- **PYSPARK_MASTER_CURRICULUM.md** - Complete session section
- **notebooks/examples/** - Example notebooks
