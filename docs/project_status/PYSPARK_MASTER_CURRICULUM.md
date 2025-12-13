# PySpark Master Curriculum Guide
## Complete Reference for Big Data Engineering with PySpark

**Last Updated**: December 13, 2025  
**Coverage**: 100+ topics across 7 major areas  
**Level**: Beginner to Advanced

---

## 📚 Table of Contents

1. [RDD Operations](#1-rdd-operations) ✅ COMPLETE
2. [HDFS - Hadoop Distributed File System](#2-hdfs) ✅ COMPLETE
3. [PyCharm for PySpark Development](#3-pycharm)
4. [Spark Execution Architecture](#4-spark-execution-architecture)
5. [Spark Session](#5-spark-session)
6. [DataFrame ETL Operations](#6-dataframe-etl)
7. [Performance Optimization](#7-performance-optimization)

---

## 1. RDD Operations

**Status**: ✅ COMPLETE - See `src/rdd_operations/`

### Quick Reference
- **What**: Resilient Distributed Datasets - low-level Spark API
- **When**: Unstructured data, fine-grained control, legacy code
- **Files**: 6 Python examples + comprehensive guides
- **Topics**: Transformations, Actions, Shuffles, Partitioning

### Key Takeaways
```python
# Use DataFrames by default
df = spark.read.csv("data.csv")

# Drop to RDDs only when needed:
# 1. Unstructured/binary data
# 2. Custom partitioning logic
# 3. mapPartitions for expensive operations
rdd = sc.textFile("logs.txt")
```

---

## 2. HDFS

**Status**: ✅ COMPLETE - See `src/hdfs/`

### Quick Reference
- **What**: Distributed file system for big data storage
- **Architecture**: NameNode (master) + DataNodes (workers)
- **Block Size**: 128 MB default
- **Replication**: 3x default

### Essential Commands
```bash
# Upload
hdfs dfs -put /local/file.txt /hdfs/path/

# Download  
hdfs dfs -get /hdfs/path/file.txt /local/

# List
hdfs dfs -ls -R /hdfs/path/

# Delete
hdfs dfs -rm -r /hdfs/path/directory/

# Check status
hdfs dfsadmin -report
```

---

## 3. PyCharm

###What is PyCharm?

**PyCharm** is a professional IDE (Integrated Development Environment) for Python development, including PySpark applications.

### Why PyCharm for PySpark?

```
✅ Intelligent code completion
✅ Debugging support
✅ Git integration
✅ Database tools
✅ SSH/remote development
✅ Jupyter notebook support
✅ Professional testing tools
```

### Installation

#### Windows
```bash
# Download from jetbrains.com/pycharm
# Professional Edition (paid) or Community (free)

# Install via chocolatey
choco install pycharm-community
```

#### macOS
```bash
# Download from jetbrains.com/pycharm

# Install via Homebrew
brew install --cask pycharm-ce
```

#### Linux
```bash
# Download tar.gz from jetbrains.com

# Extract and run
tar -xzf pycharm-*.tar.gz
cd pycharm-*/bin
./pycharm.sh

# Or use snap
sudo snap install pycharm-community --classic
```

### PyCharm Basics

#### 1. **Create PySpark Project**
```
File → New Project
├─ Location: /path/to/pyspark-project
├─ Interpreter: Python 3.8+
└─ Create

# Install PySpark
pip install pyspark
```

#### 2. **Project Structure**
```
pyspark-project/
├─ src/
│   ├─ __init__.py
│   ├─ etl_pipeline.py
│   └─ transformations.py
├─ tests/
│   └─ test_transformations.py
├─ data/
│   ├─ input/
│   └─ output/
├─ notebooks/
│   └─ analysis.ipynb
├─ requirements.txt
└─ README.md
```

#### 3. **Configure Spark**
```python
# spark_config.py
from pyspark.sql import SparkSession

def create_spark_session(app_name="MyApp"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    return spark
```

### PyCharm Runtime Arguments

#### Running with Arguments
```python
# my_spark_job.py
import sys

def main(input_path, output_path, date):
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("ETL").getOrCreate()
    
    df = spark.read.parquet(input_path)
    df.filter(f"date = '{date}'").write.parquet(output_path)
    
if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    date = sys.argv[3]
    
    main(input_path, output_path, date)
```

#### PyCharm Run Configuration
```
Run → Edit Configurations
├─ Script path: /path/to/my_spark_job.py
├─ Parameters: /data/input /data/output 2024-12-13
├─ Environment variables: PYSPARK_PYTHON=python3
└─ Working directory: /path/to/project
```

#### Using argparse (Better)
```python
import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='PySpark ETL Job')
    parser.add_argument('--input', required=True, help='Input path')
    parser.add_argument('--output', required=True, help='Output path')
    parser.add_argument('--date', required=True, help='Date filter')
    parser.add_argument('--partitions', type=int, default=8)
    return parser.parse_args()

def main():
    args = parse_args()
    # Use args.input, args.output, args.date, args.partitions
    
if __name__ == "__main__":
    main()

# Run in PyCharm:
# Parameters: --input /data/input --output /data/output --date 2024-12-13
```

### PyCharm Tips for PySpark

#### 1. **Debug PySpark Locally**
```python
# Set breakpoint (click left margin)
df = spark.read.csv("data.csv")
df = df.filter(col("age") > 25)  # ← Set breakpoint here
df.show()

# Run with debugger: Shift+F9
# Step through execution, inspect variables
```

#### 2. **Interactive Console**
```python
# Tools → Python Console
# Access with Alt+P (Windows/Linux) or Cmd+P (Mac)

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
df = spark.range(100)
df.show()
```

#### 3. **Database Tools**
```
View → Tool Windows → Database
├─ Add Data Source → PostgreSQL/MySQL/Hive
├─ Test Connection
└─ Query data directly in PyCharm

# Use in PySpark
df = spark.read.jdbc(url, "table_name", properties)
```

#### 4. **Version Control (Git)**
```
VCS → Enable Version Control Integration → Git

# Commit: Ctrl+K (Cmd+K on Mac)
# Push: Ctrl+Shift+K
# Pull: Ctrl+T
# View History: Alt+9
```

#### 5. **Code Templates**
```
File → Settings → Editor → Live Templates

# Create template: "pyspark"
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("$APP_NAME$") \
    .master("local[*]") \
    .getOrCreate()

$END$

# Use: Type "pyspark" + Tab
```

---

## 4. Spark Execution Architecture

### Full Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Spark Application                           │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Driver Program                        │   │
│  │  ┌──────────────────────────────────────────────────┐  │   │
│  │  │          SparkContext / SparkSession             │  │   │
│  │  │  - Creates RDDs/DataFrames                       │  │   │
│  │  │  - Transforms → DAG                               │  │   │
│  │  │  - Schedules tasks                                │  │   │
│  │  └──────────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────────┐  │   │
│  │  │              DAG Scheduler                       │  │   │
│  │  │  - Builds execution DAG                          │  │   │
│  │  │  - Splits into stages (shuffle boundaries)       │  │   │
│  │  │  - Submits stages to Task Scheduler              │  │   │
│  │  └──────────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────────┐  │   │
│  │  │              Task Scheduler                      │  │   │
│  │  │  - Breaks stages into tasks                      │  │   │
│  │  │  - Assigns tasks to executors                    │  │   │
│  │  │  - Handles task failures/retries                 │  │   │
│  │  └──────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                     │
│                            │ (via Cluster Manager)               │
│                            ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                Cluster Manager (YARN/K8s/Standalone)    │   │
│  │  - Allocates resources                                  │   │
│  │  - Monitors executors                                   │   │
│  │  - Launches containers                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                     │
│                            ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Executors (Workers)                  │   │
│  │                                                           │   │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐          │   │
│  │  │Executor 1│    │Executor 2│    │Executor 3│   ...    │   │
│  │  │          │    │          │    │          │          │   │
│  │  │ Tasks:   │    │ Tasks:   │    │ Tasks:   │          │   │
│  │  │ [T1][T2] │    │ [T3][T4] │    │ [T5][T6] │          │   │
│  │  │ [T7][T8] │    │ [T9][T10]│    │ [T11][T12]          │   │
│  │  │          │    │          │    │          │          │   │
│  │  │ Cache    │    │ Cache    │    │ Cache    │          │   │
│  │  └──────────┘    └──────────┘    └──────────┘          │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### YARN As Spark Cluster Manager

**YARN (Yet Another Resource Negotiator)** - Hadoop's resource manager

```
┌─────────────────────────────────────────────────────────────────┐
│                      YARN Cluster                                │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │          ResourceManager (Master)                        │   │
│  │  - Global resource scheduler                             │   │
│  │  - Accepts application submissions                       │   │
│  │  - Allocates containers to applications                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                     │
│                            │ Submit Spark App                    │
│                            ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │        ApplicationMaster (Spark Driver)                  │   │
│  │  - Negotiates resources with RM                          │   │
│  │  - Launches executors in containers                      │   │
│  │  - Monitors task execution                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                            │                                     │
│                            │ Request containers                  │
│                            ↓                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              NodeManagers (Workers)                      │   │
│  │                                                           │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │   │
│  │  │ Container 1 │  │ Container 2 │  │ Container 3 │ ... │   │
│  │  │  Executor   │  │  Executor   │  │  Executor   │     │   │
│  │  │  (JVM)      │  │  (JVM)      │  │  (JVM)      │     │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘     │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘

Advantages:
✅ Multi-tenancy (multiple Spark apps share cluster)
✅ Resource isolation
✅ Dynamic allocation
✅ Fault tolerance
```

### JVMs Across Clusters

```
Each Executor = Separate JVM Process

┌──────────────────────────────────────┐
│          Node 1                      │
│  ┌──────────┐      ┌──────────┐    │
│  │  JVM 1   │      │  JVM 2   │    │
│  │Executor 1│      │Executor 2│    │
│  │ Heap: 4GB│      │ Heap: 4GB│    │
│  └──────────┘      └──────────┘    │
└──────────────────────────────────────┘

Benefits:
✅ Isolation: Failure in one doesn't affect others
✅ Parallelism: Multiple JVMs = multiple cores
✅ Memory: Each JVM has own heap

Challenges:
❌ GC pauses can affect performance
❌ Serialization overhead between JVMs
❌ Memory overhead (JVM per executor)
```

### Commonly Used Terms

```
┌─────────────────────────────────────────────────────────────┐
│ Application  → Complete Spark program (main())              │
│ Job          → Parallel computation triggered by action     │
│ Stage        → Set of tasks between shuffle boundaries      │
│ Task         → Unit of work sent to one executor            │
│ Partition    → Logical chunk of data (RDD/DataFrame)        │
│ Slot         → CPU core available for task execution        │
│ Shuffle      → Data redistribution across executors         │
│ DAG          → Directed Acyclic Graph of transformations    │
│ Lineage      → Chain of transformations for fault tolerance │
└─────────────────────────────────────────────────────────────┘

Example:
df.read.csv("data.csv")           ← Application starts
  .filter(col("age") > 25)        ← Transformation (lazy)
  .groupBy("city").count()        ← Transformation (lazy)
  .show()                         ← ACTION triggers Job!
    └─> Creates 2 Stages (shuffle at groupBy)
        └─> Each stage has N tasks (1 per partition)
```

### Narrow vs Wide Transformations

```
Narrow Transformations (No Shuffle):
────────────────────────────────────
map(), filter(), union(), coalesce()

Input Partition → Output Partition (1:1 mapping)

┌─────────┐      ┌─────────┐
│ Part 1  │─────>│ Part 1' │
└─────────┘      └─────────┘
┌─────────┐      ┌─────────┐
│ Part 2  │─────>│ Part 2' │
└─────────┘      └─────────┘

✅ Fast (no data movement)
✅ Can be pipelined
✅ Fault tolerance: recompute lost partition only


Wide Transformations (Shuffle Required):
────────────────────────────────────────
groupByKey(), reduceByKey(), join(), repartition()

Multiple input partitions → Multiple output partitions

┌─────────┐ \    / ┌─────────┐
│ Part 1  │─ \ /  ─│ Part 1' │
└─────────┘─  X   ─└─────────┘
┌─────────┐─ / \  ─┌─────────┐
│ Part 2  │─/   \ ─│ Part 2' │
└─────────┘      \ └─────────┘
                  SHUFFLE!

❌ Expensive (disk + network I/O)
❌ Shuffle boundary → new stage
❌ Fault tolerance: may need to recompute all
```

### DAG Scheduler

**Converts logical plan into physical execution plan**

#### Part 1: Logical to Physical

```python
# User code (Logical Plan)
df = spark.read.csv("data.csv")
filtered = df.filter(col("age") > 25)
grouped = filtered.groupBy("city").count()
sorted_result = grouped.orderBy("count", ascending=False)
sorted_result.show()

# DAG Scheduler creates:

Stage 1: Read + Filter (Narrow transformations)
  ├─ Task 1: Read partition 1, filter
  ├─ Task 2: Read partition 2, filter
  └─ Task 3: Read partition 3, filter
  
  ↓ SHUFFLE (groupBy boundary)
  
Stage 2: GroupBy + Count
  ├─ Task 1: Aggregate partition 1
  ├─ Task 2: Aggregate partition 2
  └─ Task 3: Aggregate partition 3
  
  ↓ SHUFFLE (orderBy boundary)
  
Stage 3: Sort + Collect
  └─ Task 1: Global sort, collect to driver
```

#### Part 2: Stage Dependencies

```
Types of Dependencies:

1. Narrow Dependency (PipelinedRDD):
   Parent RDD → Child RDD (1:1)
   Can execute in same stage
   
2. Wide Dependency (ShuffledRDD):
   Parent RDD → Child RDD (N:M)
   Requires new stage

Example:
rdd1.map(f)            # Narrow → Same stage
    .filter(g)         # Narrow → Same stage
    .reduceByKey(h)    # Wide → New stage!
    .map(i)            # Narrow → Same stage
```

#### Part 3: Stage Execution

```
DAG Scheduler workflow:

1. Receive Job (from action like collect(), show())
2. Build complete DAG of RDD dependencies
3. Identify shuffle boundaries
4. Split into stages
5. Determine stage dependencies
6. Submit stages in topological order
7. Wait for stage completion
8. Submit next stage
9. Repeat until job complete

Example execution order:
Stage 1 (no dependencies) → Execute first
  ↓ Complete
Stage 2 (depends on Stage 1) → Execute second
  ↓ Complete
Stage 3 (depends on Stage 2) → Execute third
```

### Task Scheduler

**Assigns tasks to executors and monitors execution**

```
Task Scheduler Workflow:

1. Receive Stage from DAG Scheduler
2. Create TaskSet (one task per partition)
3. For each task:
   - Find available executor
   - Consider data locality:
     * PROCESS_LOCAL (data in same JVM)
     * NODE_LOCAL (data on same node)
     * RACK_LOCAL (data in same rack)
     * ANY (data anywhere, must transfer)
   - Assign task to executor
4. Monitor task execution
5. Handle failures:
   - Retry failed tasks (up to 4 times)
   - Blacklist problematic executors
6. Report completion to DAG Scheduler

Task Locality Example:

Best:  Data in executor memory (PROCESS_LOCAL)
Good:  Data on same node (NODE_LOCAL)
OK:    Data in same rack (RACK_LOCAL)
Slow:  Data on different rack (ANY)

Spark waits 3s for better locality before giving up
```

---

## 5. Spark Session

### Spark Object and spark-submit

#### Understanding SparkSession

```python
from pyspark.sql import SparkSession

# SparkSession is the entry point (Spark 2.0+)
spark = SparkSession.builder \
    .appName("MyApplication") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .enableHiveSupport() \
    .getOrCreate()

# Unified interface for:
# - SparkContext (RDDs)
# - SQLContext (DataFrames)
# - HiveContext (Hive tables)
```

#### spark-submit Command

```bash
# Basic syntax
spark-submit \
  --master <master-url> \
  --deploy-mode <client|cluster> \
  --conf <key>=<value> \
  --driver-memory <memory> \
  --executor-memory <memory> \
  --executor-cores <num> \
  --num-executors <num> \
  <application-jar/py> \
  [application-arguments]

# Local mode
spark-submit \
  --master local[4] \
  --driver-memory 2g \
  my_app.py --input /data/input --output /data/output

# YARN cluster mode
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 10g \
  --driver-memory 4g \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  my_app.py

# With Python dependencies
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files dependencies.zip \
  --files config.json \
  my_app.py
```

### Version and Range

```python
# Check Spark version
print(spark.version)  # "3.5.0"

# Create sequential numbers
df = spark.range(1000)  # 0 to 999
df = spark.range(10, 100)  # 10 to 99
df = spark.range(0, 100, 5)  # 0, 5, 10, ..., 95

# With partitions
df = spark.range(0, 1000000, numPartitions=100)
print(df.rdd.getNumPartitions())  # 100

# Use for testing
df = spark.range(1000000).select(
    col("id"),
    (col("id") * 2).alias("doubled"),
    (col("id") % 10).alias("mod10")
)
```

### createDataFrame

```python
# From list of tuples
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# From list of Row objects
from pyspark.sql import Row
data = [
    Row(name="Alice", age=25, city="NY"),
    Row(name="Bob", age=30, city="LA")
]
df = spark.createDataFrame(data)

# With schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=False)
])

data = [("Alice", 25), ("Bob", 30)]
df = spark.createDataFrame(data, schema)

# From pandas DataFrame
import pandas as pd
pandas_df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [25, 30]})
spark_df = spark.createDataFrame(pandas_df)

# From RDD
rdd = spark.sparkContext.parallelize([(1, "a"), (2, "b")])
df = spark.createDataFrame(rdd, ["id", "value"])
```

### SQL Operations

```python
# Register DataFrame as temp view
df.createOrReplaceTempView("people")

# Query with SQL
result = spark.sql("""
    SELECT name, age
    FROM people
    WHERE age > 25
    ORDER BY age DESC
""")

# Global temp view (across sessions)
df.createOrReplaceGlobalTempView("global_people")
spark.sql("SELECT * FROM global_temp.global_people")

# SQL with joins
spark.sql("""
    SELECT p.name, o.total
    FROM people p
    JOIN orders o ON p.id = o.customer_id
    WHERE o.total > 1000
""")
```

### Table Operations

```python
# Save as table
df.write.saveAsTable("my_table")

# Read from table
df = spark.table("my_table")

# Drop table
spark.sql("DROP TABLE IF EXISTS my_table")

# Table metadata
spark.sql("DESCRIBE EXTENDED my_table").show()
```

### SparkContext

```python
# Access SparkContext from SparkSession
sc = spark.sparkContext

# Application info
print(sc.applicationId)
print(sc.master)
print(sc.defaultParallelism)

# Create RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd = sc.textFile("data.txt")

# Broadcast variable
broadcast_var = sc.broadcast({"key": "value"})
value = broadcast_var.value

# Accumulator
accum = sc.accumulator(0)
rdd.foreach(lambda x: accum.add(x))
print(accum.value)
```

### Configuration

```python
# Get config
conf = spark.sparkContext.getConf()
print(conf.get("spark.app.name"))
print(conf.get("spark.master"))

# Set config (before creating session)
from pyspark import SparkConf

conf = SparkConf() \
    .setAppName("MyApp") \
    .set("spark.sql.shuffle.partitions", "8") \
    .set("spark.sql.adaptive.enabled", "true")

spark = SparkSession.builder.config(conf=conf).getOrCreate()

# Runtime config (after session created)
spark.conf.set("spark.sql.shuffle.partitions", "16")
value = spark.conf.get("spark.sql.shuffle.partitions")
```

### UDF (User Defined Functions)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# Simple UDF
def upper_case(s):
    return s.upper() if s else ""

upper_udf = udf(upper_case, StringType())
df.select(upper_udf(col("name")).alias("upper_name"))

# With decorator
@udf(returnType=IntegerType())
def square(x):
    return x * x

df.select(square(col("value")))

# Register for SQL
spark.udf.register("square_udf", lambda x: x * x, IntegerType())
spark.sql("SELECT square_udf(value) FROM table")

# Pandas UDF (vectorized, faster!)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(IntegerType())
def pandas_square(s: pd.Series) -> pd.Series:
    return s * s

df.select(pandas_square(col("value")))
```

### Reading Data

```python
# CSV
df = spark.read.csv("data.csv", header=True, inferSchema=True)
df = spark.read.option("header", "true").option("sep", ",").csv("data.csv")

# Text
df = spark.read.text("file.txt")  # Single column: "value"

# ORC
df = spark.read.orc("data.orc")

# Parquet
df = spark.read.parquet("data.parquet")

# JSON
df = spark.read.json("data.json")
df = spark.read.json("multiline.json", multiLine=True)

# Avro (requires spark-avro package)
df = spark.read.format("avro").load("data.avro")

# Hive
df = spark.sql("SELECT * FROM hive_table")
df = spark.table("hive_table")

# JDBC
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/mydb",
    table="users",
    properties={"user": "admin", "password": "secret"}
)

# With partitioning
df = spark.read.jdbc(
    url=jdbc_url,
    table="large_table",
    column="id",
    lowerBound=0,
    upperBound=1000000,
    numPartitions=10,
    properties=props
)
```

### Catalog Operations

```python
# List databases
spark.catalog.listDatabases()

# List tables
spark.catalog.listTables()
spark.catalog.listTables("database_name")

# List columns
spark.catalog.listColumns("table_name")

# Check if table exists
spark.catalog.tableExists("my_table")

# Current database
spark.catalog.currentDatabase()

# Set database
spark.catalog.setCurrentDatabase("my_db")

# Cache table
spark.catalog.cacheTable("my_table")
spark.catalog.isCached("my_table")
spark.catalog.uncacheTable("my_table")

# Refresh table
spark.catalog.refreshTable("my_table")
```

### newSession and stop

```python
# Create new session (shares SparkContext)
new_spark = spark.newSession()

# Independent config
new_spark.conf.set("spark.sql.shuffle.partitions", "32")

# Stop session
spark.stop()

# Best practice in scripts
if __name__ == "__main__":
    spark = SparkSession.builder.appName("MyApp").getOrCreate()
    
    try:
        # Your code here
        df = spark.read.csv("data.csv")
        df.show()
    finally:
        spark.stop()  # Always cleanup
```

---

## 6. DataFrame ETL

### Selection Operations

```python
# Select columns
df.select("name", "age")
df.select(col("name"), col("age"))
df.select(df.name, df.age)

# Select with expressions
df.select(
    col("name"),
    (col("age") + 5).alias("age_plus_5"),
    (col("salary") * 1.1).alias("new_salary")
)

# Select all columns
df.select("*")

# Drop columns
df.drop("column1", "column2")

# Rename
df.withColumnRenamed("old_name", "new_name")

# Add column
df.withColumn("new_col", col("existing") * 2)
```

### Filter and Where

```python
# Filter (same as where)
df.filter(col("age") > 25)
df.where(col("age") > 25)

# Multiple conditions
df.filter((col("age") > 25) & (col("city") == "NY"))
df.filter((col("age") < 20) | (col("age") > 60))

# SQL expression
df.filter("age > 25 AND city = 'NY'")

# NULL handling
df.filter(col("name").isNotNull())
df.filter(col("age").isNull())

# String operations
df.filter(col("name").startswith("A"))
df.filter(col("email").endswith("@gmail.com"))
df.filter(col("name").contains("John"))
```

### Sorting

```python
# Order by (ascending)
df.orderBy("age")
df.orderBy(col("age").asc())

# Descending
df.orderBy(col("age").desc())

# Multiple columns
df.orderBy("city", col("age").desc())

# Sort (alias for orderBy)
df.sort("age")

# Null handling
df.orderBy(col("age").asc_nulls_first())
df.orderBy(col("age").asc_nulls_last())
```

### Set Operations

```python
# Union (keep duplicates)
df1.union(df2)

# Union by name (match columns by name, not position)
df1.unionByName(df2)

# Intersect (common rows)
df1.intersect(df2)

# Except (rows in df1 not in df2)
df1.except_(df2)  # Note: except is keyword, use except_()

# Distinct
df.distinct()

# Drop duplicates
df.dropDuplicates()
df.dropDuplicates(["col1", "col2"])  # Based on subset
```

### Joins

```python
# Inner join (default)
df1.join(df2, "key_column")
df1.join(df2, df1.id == df2.user_id)

# Left outer join
df1.join(df2, "key", "left")
df1.join(df2, "key", "left_outer")

# Right outer join
df1.join(df2, "key", "right")

# Full outer join
df1.join(df2, "key", "outer")
df1.join(df2, "key", "full_outer")

# Left anti join (rows in left not in right)
df1.join(df2, "key", "left_anti")

# Left semi join (rows in left that have match in right)
df1.join(df2, "key", "left_semi")

# Cross join (cartesian product)
df1.crossJoin(df2)

# Join with multiple conditions
df1.join(
    df2,
    (df1.id == df2.user_id) & (df1.date == df2.date),
    "inner"
)
```

### Aggregations

```python
from pyspark.sql.functions import *

# Simple aggregations
df.count()
df.select(sum("salary"), avg("age"), max("score"), min("score"))

# Group by
df.groupBy("city").count()
df.groupBy("city").agg(
    sum("salary").alias("total_salary"),
    avg("age").alias("avg_age"),
    count("*").alias("num_people")
)

# Multiple group columns
df.groupBy("city", "department").avg("salary")

# Having (filter after groupBy)
df.groupBy("city") \
    .agg(sum("salary").alias("total")) \
    .filter(col("total") > 100000)
```

### GroupBy Operations

```python
# Aggregate functions
df.groupBy("category").agg(
    count("*").alias("count"),
    sum("amount").alias("total"),
    avg("amount").alias("average"),
    max("amount").alias("maximum"),
    min("amount").alias("minimum"),
    stddev("amount").alias("std_dev"),
    variance("amount").alias("variance"),
    collect_list("product").alias("products"),
    collect_set("product").alias("unique_products")
)

# Pivot (wide format)
df.groupBy("year").pivot("quarter").sum("revenue")

# Result:
# year | Q1 | Q2 | Q3 | Q4
# 2023 | 100| 150| 200| 250
# 2024 | 120| 180| 220| 280
```

### Window Functions

```python
from pyspark.sql.window import Window

# Define window
window = Window.partitionBy("category").orderBy("date")

# Ranking
df.withColumn("rank", rank().over(window))
df.withColumn("dense_rank", dense_rank().over(window))
df.withColumn("row_number", row_number().over(window))

# Running aggregates
df.withColumn("running_total", sum("amount").over(window))
df.withColumn("running_avg", avg("amount").over(window))

# Lead and Lag
df.withColumn("next_value", lead("amount", 1).over(window))
df.withColumn("prev_value", lag("amount", 1).over(window))

# Window with rows/range
window_rows = Window.partitionBy("id").orderBy("date") \
    .rowsBetween(-2, 2)  # 2 rows before and after

df.withColumn("moving_avg", avg("value").over(window_rows))

# Unbounded window
window_unbounded = Window.partitionBy("category") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

### Sampling

```python
# Random sample with replacement
df.sample(withReplacement=True, fraction=0.1, seed=42)

# Without replacement
df.sample(withReplacement=False, fraction=0.1)

# Sample N rows
df.limit(100)

# Stratified sampling
df.sampleBy("category", fractions={"A": 0.1, "B": 0.2, "C": 0.3})
```

### Built-in Functions

#### String Functions
```python
# Length
df.select(length("name"))

# Upper/Lower
df.select(upper("name"), lower("name"))

# Trim
df.select(trim("name"), ltrim("name"), rtrim("name"))

# Substring
df.select(substring("name", 1, 3))  # First 3 chars

# Concat
df.select(concat("first_name", lit(" "), "last_name"))

# Split
df.select(split("full_name", " ").alias("name_parts"))

# Replace
df.select(regexp_replace("text", "\\d+", "NUM"))

# Extract
df.select(regexp_extract("email", "(.+)@(.+)", 1).alias("username"))
```

#### Date Functions
```python
# Current date/timestamp
df.select(current_date(), current_timestamp())

# Date arithmetic
df.select(date_add("date", 7))  # Add 7 days
df.select(date_sub("date", 30))  # Subtract 30 days

# Date diff
df.select(datediff("end_date", "start_date"))

# Format date
df.select(date_format("timestamp", "yyyy-MM-dd"))

# Extract parts
df.select(
    year("date"),
    month("date"),
    dayofmonth("date"),
    dayofweek("date"),
    hour("timestamp"),
    minute("timestamp")
)

# Parse string to date
df.select(to_date("date_string", "yyyy-MM-dd"))
df.select(to_timestamp("ts_string", "yyyy-MM-dd HH:mm:ss"))
```

#### NULL Functions
```python
# Check null
df.filter(col("name").isNull())
df.filter(col("name").isNotNull())

# Replace nulls
df.fillna(0)  # Fill all numeric nulls with 0
df.fillna({"age": 0, "name": "Unknown"})  # Specific columns

# Drop nulls
df.dropna()  # Drop rows with ANY null
df.dropna(how="all")  # Drop only if ALL columns are null
df.dropna(subset=["age", "name"])  # Drop if these columns have null

# Coalesce (first non-null)
df.select(coalesce("col1", "col2", "col3"))
```

#### Collection Functions
```python
# Array operations
df.select(array("col1", "col2", "col3").alias("array_col"))
df.select(size("array_col"))  # Array length
df.select(array_contains("array_col", "value"))
df.select(explode("array_col"))  # One row per array element

# Map operations
df.select(create_map("key_col", "value_col"))
df.select(map_keys("map_col"))
df.select(map_values("map_col"))

# Struct operations
df.select(struct("col1", "col2").alias("struct_col"))
df.select(col("struct_col.col1"))  # Access field
```

#### Math Functions
```python
# Basic math
df.select(
    abs("value"),
    ceil("value"),
    floor("value"),
    round("value", 2),
    sqrt("value"),
    pow("base", lit(2))
)

# Trigonometry
df.select(sin("angle"), cos("angle"), tan("angle"))

# Logarithms
df.select(log("value"), log10("value"), exp("value"))

# Random
df.select(rand(), randn())  # Uniform and normal distribution
```

#### JSON Functions
```python
# Parse JSON string
df.select(get_json_object("json_col", "$.field"))

# JSON to struct
df.select(from_json("json_col", schema))

# Struct to JSON
df.select(to_json(struct("col1", "col2")))

# Explode JSON array
df.select(explode(from_json("json_array", ArrayType(StringType()))))
```

### Repartition and Coalesce

```python
# Check current partitions
print(df.rdd.getNumPartitions())

# Repartition (can increase or decrease, causes shuffle)
df_repartitioned = df.repartition(100)
df_repartitioned = df.repartition("key_column")  # Hash partitioning

# Coalesce (decrease only, no shuffle if decreasing)
df_coalesced = df.coalesce(10)

# When to use:
# - Repartition: Before expensive operations (joins, groupBy)
# - Coalesce: Before writing to reduce number of output files

# Write with optimal partitions
df.coalesce(1).write.csv("output/")  # Single file
df.repartition(100).write.parquet("output/")  # 100 files
```

### DataFrame Extraction (Writing)

```python
# CSV
df.write.csv("output.csv", header=True, mode="overwrite")

# Parquet (default, most efficient)
df.write.parquet("output.parquet", mode="overwrite")

# ORC
df.write.orc("output.orc")

# JSON
df.write.json("output.json")

# Avro
df.write.format("avro").save("output.avro")

# Hive table
df.write.saveAsTable("my_table", mode="overwrite")

# JDBC
df.write.jdbc(
    url="jdbc:postgresql://localhost:5432/mydb",
    table="users",
    mode="append",
    properties={"user": "admin", "password": "secret"}
)

# Partitioned write
df.write.partitionBy("year", "month").parquet("output/")

# With compression
df.write.option("compression", "snappy").parquet("output/")

# Modes: "overwrite", "append", "ignore", "error"
df.write.mode("overwrite").parquet("output/")
```

---

## 7. Performance Optimization

### Join Strategies

#### 1. Broadcast Join
```python
# For small tables (< 10 MB default)
from pyspark.sql.functions import broadcast

large_df.join(broadcast(small_df), "key")

# Spark automatically broadcasts if under threshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB

# When to use:
✅ One table is small (< 100 MB)
✅ Joining large table with dimension table
✅ Want to avoid shuffle

# How it works:
# Small table sent to all executors
# Join happens locally on each executor
# No shuffle needed!
```

#### 2. Shuffle Hash Join
```python
# For medium-sized tables
# Automatically chosen if both tables too large for broadcast

# How it works:
# 1. Shuffle both tables by join key
# 2. Build hash table for smaller side
# 3. Probe with larger side

# Configuration
spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
```

#### 3. Sort Merge Join
```python
# Default for large tables
# Both tables sorted, then merged

# How it works:
# 1. Shuffle both tables by join key
# 2. Sort both sides
# 3. Merge sorted partitions

# Best for:
✅ Both tables are large
✅ Join keys already sorted
✅ Equi-joins (equality conditions)
```

#### 4. Cartesian Product Join
```python
# Cross join (every row with every row)
df1.crossJoin(df2)

# Warning: Very expensive!
# Result size = df1.count() × df2.count()

# Use only when necessary
```

#### 5. Broadcast Nested Loop Join
```python
# For non-equi joins or no join condition
df1.join(df2, df1.col1 < df2.col2)

# Slowest join strategy
# Avoid if possible
```

### Driver Configurations

```python
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.cores", "2") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

# spark.driver.memory
# - Amount of memory for driver JVM
# - Default: 1g
# - Increase if collecting large results

# spark.driver.cores
# - Number of cores for driver
# - Only matters in local mode

# spark.driver.maxResultSize
# - Max size of results sent to driver
# - Prevents OOM from large collect()
# - Default: 1g
```

### Executor Configurations

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.instances", "10") \
    .config("spark.executor.memoryOverhead", "1g") \
    .getOrCreate()

# spark.executor.memory
# - Heap memory per executor
# - Rule: 2-8 GB per executor

# spark.executor.cores  
# - Cores per executor
# - Rule: 5 cores optimal (diminishing returns after)

# spark.executor.instances
# - Number of executors
# - Rule: Total cores / executor.cores

# spark.executor.memoryOverhead
# - Off-heap memory (native, Python processes)
# - Default: 10% of executor.memory, min 384 MB
# - Increase for Python UDFs

# Example calculation:
# Cluster: 10 nodes × 16 cores × 64 GB RAM
# Config:
#   executor.cores = 5
#   executor.memory = 10g (leaves room for OS)
#   executor.instances = (10 × 16) / 5 = 32 executors
```

### Dynamic Allocation

```python
spark = SparkSession.builder \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "2") \
    .config("spark.dynamicAllocation.maxExecutors", "100") \
    .config("spark.dynamicAllocation.initialExecutors", "10") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.shuffle.service.enabled", "true") \
    .getOrCreate()

# Benefits:
✅ Scales executors based on workload
✅ Releases idle executors
✅ Efficient resource utilization

# Requirements:
# Must enable external shuffle service
```

### Parallelism Configurations

```python
spark = SparkSession.builder \
    .config("spark.default.parallelism", "200") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# spark.default.parallelism
# - Default partitions for RDDs
# - Default: Number of cores in cluster
# - Rule: 2-3x number of cores

# spark.sql.shuffle.partitions
# - Partitions for DataFrame shuffles
# - Default: 200
# - Adjust based on data size:
#   * Small data (< 1 GB): 8-16
#   * Medium (1-10 GB): 50-100
#   * Large (> 10 GB): 200-1000

# Adaptive Query Execution (auto-optimizes)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Memory Management

```python
# Memory fractions
spark.conf.set("spark.memory.fraction", "0.6")  # 60% for execution/storage
spark.conf.set("spark.memory.storageFraction", "0.5")  # 50% of above for storage

# Memory breakdown:
# Total Executor Memory = executor.memory + executor.memoryOverhead
# 
# executor.memory (Heap):
#   ├─ Reserved (300 MB)
#   ├─ Spark Memory (60% default)
#   │   ├─ Storage (50% = 30% total) - Cached data
#   │   └─ Execution (50% = 30% total) - Shuffles, joins
#   └─ User Memory (40%) - Your data structures
#
# executor.memoryOverhead (Off-heap):
#   └─ Native memory, Python processes

# Example with 10g executor.memory:
# Reserved: 300 MB
# Spark Memory: (10g - 300MB) × 0.6 = 5.8 GB
#   ├─ Storage: 5.8 × 0.5 = 2.9 GB
#   └─ Execution: 2.9 GB
# User: 3.9 GB
# Overhead: 1 GB (memoryOverhead)

# When to adjust:
# - More caching → Increase storageFraction
# - Complex shuffles → Increase memory.fraction
# - Python UDFs → Increase memoryOverhead
```

### Complete Example

```python
# Production-optimized Spark application
spark = SparkSession.builder \
    .appName("OptimizedETL") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "5") \
    .config("spark.executor.instances", "20") \
    .config("spark.executor.memoryOverhead", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "50") \
    .config("spark.shuffle.service.enabled", "true") \
    .getOrCreate()

# Read with optimized partitions
df = spark.read.parquet("large_dataset.parquet")

# Repartition before expensive operations
df = df.repartition(200, "key_column")

# Use broadcast for small table joins
small_df = spark.read.parquet("dimension_table.parquet")
result = df.join(broadcast(small_df), "key")

# Cache if reusing
result.cache()

# Write with optimal partitions
result.coalesce(100).write.parquet("output/")

spark.stop()
```

---

## Summary

### What You've Learned 🎯

1. **RDD Operations** - Low-level Spark API for unstructured data
2. **HDFS** - Distributed storage foundation
3. **PyCharm** - Professional IDE for PySpark development
4. **Spark Architecture** - How Spark executes distributed computations
5. **Spark Session** - Entry point and configuration
6. **DataFrame ETL** - 100+ operations for data transformation
7. **Optimization** - Performance tuning strategies

### Key Takeaways ✅

- **Use DataFrames** - Faster and easier than RDDs
- **HDFS for big data storage** - Cheap, scalable, fault-tolerant
- **Understand execution model** - Driver, executors, stages, tasks
- **Optimize joins** - Broadcast small tables
- **Tune parallelism** - Right number of partitions
- **Monitor and profile** - Use Spark UI

### Next Steps 🚀

1. ✅ Master fundamentals (YOU ARE HERE)
2. 🔨 Build projects (ETL pipelines, ML models)
3. 📊 Practice optimization (profiling, tuning)
4. 🎓 Get certified (Databricks, Cloudera)
5. 💼 Land big data engineer role

**You now have a complete PySpark curriculum! Start building! 🚀**

---

**Package Locations:**
- `src/rdd_operations/` - RDD examples and guides
- `src/hdfs/` - HDFS comprehensive guide
- `src/pycharm/` - IDE setup
- `src/spark_execution_architecture/` - Architecture details
- `src/spark_session/` - Session operations
- `src/dataframe_etl/` - ETL operations
- `src/optimization/` - Performance tuning

**Total Coverage**: 100+ topics, interview-ready, production-grade knowledge!
