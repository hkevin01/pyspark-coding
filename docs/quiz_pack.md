## Quiz Pack (20 questions) — with answers

1) **What does Spark’s DAG scheduler do?**  
Builds a *Directed Acyclic Graph* of transformations, splits a job into **stages** at shuffle boundaries, and schedules **tasks** to run on executors as efficiently as possible.

2) **Difference between transformations and actions.**  
- **Transformations** (e.g., `select`, `filter`, `groupBy`) are **lazy**: they build a plan/DAG and return a new DataFrame/RDD.  
- **Actions** (e.g., `count`, `collect`, `show`, `write`) **trigger execution** and return results or write output.

3) **Why is `collect()` dangerous?**  
It pulls *all data* to the **driver**. If the dataset is large, you can run out of driver memory or crash the application. Prefer `show()`, `take(n)`, `limit(n)`, or write to storage.

4) **When to use broadcast?**  
When joining a **large table** with a **small table** (small enough to fit in executor memory). Broadcasting avoids shuffle of the big table and speeds up joins.

5) **What is a wide transformation?**  
A transformation that requires data to move across partitions (a **shuffle**)—e.g., `groupBy`, `join`, `distinct`, `repartition`.

6) **What is shuffle?**  
A **network + disk intensive** redistribution of data across partitions/executors to satisfy operations like joins and aggregations. Shuffles are expensive and often the biggest performance cost.

7) **What is Tungsten?**  
Spark’s execution engine improvements focused on **memory management** and **CPU efficiency**, including off-heap memory, binary processing, and whole-stage code generation.

8) **How does Spark store DataFrames?**  
Internally as **Dataset[Row]** with a schema; execution often uses **optimized binary format** (UnsafeRow) and **columnar storage** for caching (depending on settings).

9) **What is lazy evaluation?**  
Spark does not execute transformations immediately. It waits until an **action** is called, then optimizes and executes the full plan.

10) **Name 3 optimizations for joins.**  
Examples:  
- **Broadcast join** for small dimension tables  
- **Repartition by join key** (or use `spark.sql.shuffle.partitions` tuning)  
- **Skew handling** (salting keys / AQE skew join)  
- Also: filter early, select only needed columns, use Parquet, avoid UDFs

11) **Explain Spark jobs → stages → tasks.**  
- An **action** triggers a **job**.  
- The job is split into **stages** separated by shuffles.  
- Each stage runs many **tasks** (usually one per partition) on executors.

12) **Difference between narrow and wide transformations.**  
- **Narrow**: each input partition maps to at most one output partition (no shuffle), e.g., `map`, `filter`, `select`.  
- **Wide**: output depends on many input partitions (shuffle), e.g., `groupBy`, `join`.

13) **What is data skew? Why does it hurt?**  
Skew means some keys/partitions have *much more data* than others. It creates **straggler tasks**, long stage times, and poor parallelism.

14) **How to mitigate skew?**  
Broadcast the small side, **salting** heavy keys, using AQE skew join handling, increasing parallelism, pre-aggregating, or custom partitioning.

15) **What is Catalyst optimizer?**  
Spark SQL’s optimizer that transforms logical plans into efficient physical plans using rule-based + cost-based optimizations (predicate pushdown, projection pruning, join reordering, etc.).

16) **What is predicate pushdown?**  
Filters are pushed down to the data source (like Parquet/ORC) so Spark reads **less data**.

17) **What is caching/persisting and when to use it?**  
Storing a DataFrame/RDD in memory (or memory+disk) to reuse across multiple actions. Use it when you reuse the same computed dataset multiple times.

18) **`repartition` vs `coalesce`**  
- `repartition(n)`: reshuffles to increase/decrease partitions (costly but balanced).  
- `coalesce(n)`: reduces partitions with minimal shuffle (cheaper; may be less balanced).

19) **Why is Parquet preferred in Spark?**  
Columnar + compressed + supports predicate pushdown and schema evolution → faster reads, less IO, better performance.

20) **How does Structured Streaming achieve “exactly-once” (practically)?**  
Through **checkpointing** + **write-ahead/offset tracking** (e.g., Kafka offsets) and **idempotent sinks/transactions** where supported. It provides end-to-end guarantees when sources/sinks support them.

---

## PPT deck (60 slides) — ready-to-copy into PowerPoint/Google Slides

You asked for “create a ppt” and “at least 60 pages.” I’m giving you **60 slide titles + bullet content**. If you want, I can also output this as **a single self-contained HTML slideshow** (offline) that you can print to PDF—tell me your preference.

---

### Slide 1 — Title
**PySpark Essentials (3-Day Interview Bootcamp)**  
- Distributed computing with Apache Spark + Python  
- Focus: interview-ready concepts + practical patterns

### Slide 2 — Why Spark?
- Big data beyond single-machine limits  
- Fast: in-memory + optimized execution  
- Unified engine: SQL, streaming, ML

### Slide 3 — Spark vs Hadoop MapReduce
- MapReduce: disk-heavy, batch-only  
- Spark: DAG execution, in-memory reuse, interactive

### Slide 4 — Spark Components
- Spark Core (RDD)  
- Spark SQL (DataFrames, Catalyst)  
- Structured Streaming  
- MLlib

### Slide 5 — Cluster Concepts
- Driver vs executors  
- Cluster manager: YARN / K8s / Standalone  
- Worker nodes + resources

### Slide 6 — Driver Responsibilities
- Builds plan (logical/physical)  
- Schedules jobs/stages/tasks  
- Holds SparkSession, collects small results

### Slide 7 — Executors Responsibilities
- Run tasks  
- Cache data partitions  
- Report results/metrics back to driver

### Slide 8 — Jobs, Stages, Tasks
- Action → Job  
- Shuffle boundaries → Stages  
- Partitions → Tasks

### Slide 9 — DAG + Lazy Evaluation
- Transformations build DAG  
- Actions trigger execution  
- Optimizer can reorder/pushdown

### Slide 10 — Narrow vs Wide Transformations
- Narrow: no shuffle (fast)  
- Wide: shuffle (expensive)  
- Examples of each

### Slide 11 — Installing / Running PySpark
- Local mode: `pip install pyspark`  
- Databricks/EMR/Dataproc options  
- When local is enough for learning

### Slide 12 — SparkSession Basics
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("prep").getOrCreate()
```
- Entry point for DataFrame + SQL

### Slide 13 — Config Basics
- `spark.sql.shuffle.partitions`  
- `spark.executor.memory`  
- `spark.sql.adaptive.enabled`

### Slide 14 — Your First DataFrame
```python
df = spark.createDataFrame([(1,"a"),(2,"b")], ["id","val"])
df.show()
```

### Slide 15 — RDD: What and Why
- Low-level distributed collection  
- More control, less optimization  
- Use when DataFrames don’t fit (unstructured)

### Slide 16 — RDD Transformations
- `map`, `flatMap`, `filter`  
- `distinct`, `union`, `sample`

### Slide 17 — RDD Actions
- `count`, `take`, `first`  
- `reduce`, `collect` (careful)

### Slide 18 — Persistence (RDD/DataFrame)
- `cache()` vs `persist()`  
- Storage levels: memory/disk  
- Use when reused

### Slide 19 — Partitions Basics
- Parallelism = number of partitions  
- Too few → underutilization  
- Too many → overhead

### Slide 20 — RDD → DataFrame
- Convert with schema for SQL benefits  
- Prefer DataFrames for ETL analytics

---

### Slide 21 — DataFrames: Why They Win
- Optimized by Catalyst + Tungsten  
- Safer: schema, types, column operations  
- Better IO and execution planning

### Slide 22 — Reading Data
```python
df = spark.read.option("header", True).csv("path")
```
- `inferSchema` trade-offs  
- explicit schemas for production

### Slide 23 — Inspecting Schema & Data
- `df.printSchema()`  
- `df.show(n, truncate=False)`  
- `df.describe().show()`

### Slide 24 — Selecting Columns
- `select("col")`, `selectExpr(...)`  
- `col()` from `pyspark.sql.functions`

### Slide 25 — Filtering Rows
- `where`, `filter`  
- boolean expressions; avoid Python UDF where possible

### Slide 26 — Adding / Transforming Columns
- `withColumn` + built-in functions  
- `when/otherwise`, `regexp_replace`, `to_date`

### Slide 27 — Aggregations
- `groupBy().agg(...)`  
- common metrics: count, sum, avg  
- watch for shuffle

### Slide 28 — Joins Overview
- inner, left, right, full, semi, anti  
- join keys and null behavior

### Slide 29 — Broadcast Join
```python
from pyspark.sql.functions import broadcast
df_big.join(broadcast(df_small), "key")
```
- when df_small fits memory  
- avoids big shuffle

### Slide 30 — Handling Duplicates
- `dropDuplicates()`  
- choose subset columns  
- deterministic dedupe via window functions

### Slide 31 — Window Functions Intro
- `row_number`, `rank`, `dense_rank`  
- partitionBy + orderBy

### Slide 32 — Window Example (Top-N per group)
- rank employees by salary per department  
- keep top 3

### Slide 33 — Null Handling
- `fillna`, `dropna`  
- `na.replace`  
- business logic for nulls

### Slide 34 — Type Casting & Dates
- `cast("int")`, `to_timestamp`  
- timezone considerations  
- parse failures → nulls

### Slide 35 — UDFs: When/Why Avoid
- Python UDF slower (serialization)  
- prefer built-in functions or pandas UDF if needed  
- measure impact

### Slide 36 — Spark SQL: Temp Views
```python
df.createOrReplaceTempView("t")
spark.sql("select ... from t").show()
```

### Slide 37 — SQL vs DataFrame API
- Same engine underneath  
- pick what’s readable for team  
- ensure optimization (avoid UDF)

### Slide 38 — Explain Plans
- `df.explain()`  
- logical vs physical plan  
- spotting shuffles and broadcasts

### Slide 39 — File Formats
- CSV/JSON: row-based, slower  
- Parquet/ORC: columnar, compressed, pushdown

### Slide 40 — Partitioning Strategy
- Partition by common filters (date, region)  
- avoid too many small files  
- understand partition pruning

---

### Slide 41 — MLlib Overview
- scalable ML on Spark  
- pipelines: transformers + estimators  
- vector-based features

### Slide 42 — Feature Engineering
- `StringIndexer`, `OneHotEncoder`  
- `VectorAssembler`  
- scaling/normalization options

### Slide 43 — Modeling Example: Logistic Regression
- fit/predict workflow  
- train/test split  
- evaluate accuracy/AUC

### Slide 44 — Pipelines
- reproducible steps  
- single `.fit()` call  
- consistent transform on new data

### Slide 45 — Model Evaluation
- `BinaryClassificationEvaluator`  
- cross-validation basics  
- avoid leakage

### Slide 46 — Performance: Common Bottlenecks
- shuffles, skew, wide ops  
- too many small files  
- Python UDF

### Slide 47 — Caching Strategy
- cache only reused datasets  
- uncache to free memory  
- prefer caching post-filter

### Slide 48 — Shuffle Tuning
- `spark.sql.shuffle.partitions` tuning  
- repartition by keys before join/agg  
- AQE to optimize at runtime

### Slide 49 — Skew Detection
- long tail tasks in Spark UI  
- uneven partition sizes  
- key frequency analysis

### Slide 50 — Skew Fix Techniques
- broadcast small side  
- salting keys  
- pre-aggregate  
- AQE skew join

### Slide 51 — Repartition vs Coalesce
- when increasing partitions: repartition  
- when decreasing for write: coalesce  
- avoid unnecessary reshuffles

### Slide 52 — Structured Streaming Basics
- streaming DataFrame  
- micro-batches or continuous mode (use cases)  
- sinks: console, files, Kafka, Delta

### Slide 53 — Streaming with Kafka (concept)
- read from Kafka topic  
- parse value as JSON  
- extract fields, aggregate

### Slide 54 — Watermarking & Late Data
- event-time vs processing-time  
- watermark to bound state  
- handle late events safely

### Slide 55 — Stateful Aggregations
- windowed counts/sums  
- checkpointing required  
- output modes: append/update/complete

### Slide 56 — End-to-End ETL Project (Interview Story)
- ingest raw data  
- clean + validate  
- transform + aggregate  
- write curated tables

### Slide 57 — End-to-End: Optimization Story
- partitioning choice  
- broadcast dimension  
- caching reused intermediate  
- measure before/after

### Slide 58 — Spark UI: What to Look For
- stages timeline  
- shuffle read/write  
- task time skew  
- storage tab for caching

### Slide 59 — Common Production Issues
- OOM driver/executor  
- too many small files  
- schema drift  
- corrupt records handling

### Slide 60 — Interview Lightning Round (What to memorize)
- lazy evaluation; stages/tasks  
- narrow vs wide; shuffle  
- broadcast join; skew fixes  
- partitioning + Parquet + explain plan

---

## Next: I can turn this into an actual “PPT”
PowerPoint files (`.pptx`) can’t be directly attached here, but I *can* generate one of these formats you can copy/export:

1) **Google Slides / PPTX copy-paste friendly** (what you see above; already done)  
2) **Self-contained HTML slideshow** (open in browser → print to PDF)  
3) **Marp Markdown deck** (you paste into Marp to export PPTX/PDF)

Tell me which format you want (2 or 3), and whether your interview is more **Data Engineering** (ETL/SQL/perf) or **Data Science** (MLlib), and I’ll tailor the deck + add **3 quizzes per day** with answers and coding drills.