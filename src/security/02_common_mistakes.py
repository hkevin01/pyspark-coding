#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PYSPARK COMMON MISTAKES - Anti-Patterns and How to Fix Them
================================================================================

MODULE OVERVIEW:
----------------
This module documents the most common mistakes developers make when writing
PySpark code, why they're problematic, and how to fix them. Learning from
these anti-patterns will save you hours of debugging and prevent production
issues.

Common mistake categories:
• Performance killers (shuffles, small files, collect())
• Memory issues (OOM errors, executor failures)
• Correctness bugs (caching mistakes, time zones, null handling)
• Design anti-patterns (UDF overuse, non-parallelizable code)
• Configuration errors (wrong resource allocation)

PURPOSE:
--------
Learn to identify and fix:
1. Performance anti-patterns that slow down jobs
2. Memory management mistakes causing OOM errors
3. Correctness issues leading to wrong results
4. Poor design patterns hurting maintainability
5. Configuration mistakes wasting resources

Each mistake includes:
• ❌ Wrong way (anti-pattern)
• Why it's problematic
• ✅ Right way (best practice)
• Performance impact
• Real-world examples

TARGET AUDIENCE:
----------------
• Developers new to PySpark
• Teams experiencing performance issues
• Anyone debugging slow Spark jobs
• Code reviewers enforcing best practices

================================================================================
MISTAKE #1: Using collect() on Large DataFrames
================================================================================

❌ WRONG - Driver OOM:
──────────────────────

# NEVER DO THIS with large data!
df = spark.read.parquet("s3://bucket/big_data/")  # 100 GB
all_data = df.collect()  # ⚠️  Tries to load 100 GB into driver memory!

for row in all_data:
    process(row)

WHY THIS IS BAD:
• collect() brings ALL data to driver (single machine)
• Driver runs out of memory (OOM error)
• No parallelism - processes one row at a time
• Defeats the purpose of distributed computing

ERROR YOU'LL SEE:
java.lang.OutOfMemoryError: Java heap space
    at org.apache.spark.sql.Dataset.collect()

✅ CORRECT - Distributed Processing:
────────────────────────────────────

# Process data on executors (distributed)
df = spark.read.parquet("s3://bucket/big_data/")

# Option 1: Use DataFrame operations (best)
result = df.filter(col("amount") > 1000) \
    .groupBy("category") \
    .agg(sum("amount"))

result.write.parquet("s3://bucket/output/")

# Option 2: Use foreach if you need custom logic
def process_partition(partition):
    for row in partition:
        # Process each row on executor
        send_to_api(row)

df.foreachPartition(process_partition)

# Option 3: If you MUST collect, use take() with limit
sample = df.take(100)  # Only first 100 rows to driver

PERFORMANCE IMPACT:
❌ collect(): 100 GB data → driver OOM → job fails
✅ Distributed: 100 GB processed across 100 executors → completes in minutes

WHEN collect() IS OK:
• Aggregated results (already small): df.groupBy(...).count().collect()
• Small dimension tables (< 100 MB)
• Final summary statistics

================================================================================
MISTAKE #2: Using Python UDFs Instead of Built-in Functions
================================================================================

❌ WRONG - Slow Python UDF:
───────────────────────────

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Python UDF (10-100x slower!)
@udf(StringType())
def extract_domain(email):
    if email:
        return email.split('@')[1]
    return None

df = df.withColumn("domain", extract_domain(col("email")))

WHY THIS IS BAD:
• Data serialized from JVM to Python (expensive)
• Python interpreter overhead (slow)
• Can't use Catalyst optimizer
• Loses vectorization benefits
• 10-100x slower than built-in functions

✅ CORRECT - Use Built-in Functions:
────────────────────────────────────

from pyspark.sql.functions import split, element_at

# Built-in functions (fast!)
df = df.withColumn("domain", 
    element_at(split(col("email"), "@"), 2))

# Or use regexp_extract
from pyspark.sql.functions import regexp_extract

df = df.withColumn("domain",
    regexp_extract(col("email"), r"@(.+)$", 1))

PERFORMANCE COMPARISON:
Test: Extract domain from 10M emails
❌ Python UDF: 120 seconds
✅ Built-in: 8 seconds (15x faster!)

WHEN UDFs ARE OK:
• Complex business logic not expressible with built-ins
• Third-party library integration (use pandas UDF)
• Last resort after exhausting built-in options

BETTER ALTERNATIVE - Pandas UDF (vectorized):
──────────────────────────────────────────────

from pyspark.sql.functions import pandas_udf
import pandas as pd

# Pandas UDF (vectorized, much faster than regular UDF)
@pandas_udf(StringType())
def extract_domain_vectorized(emails: pd.Series) -> pd.Series:
    return emails.str.split('@').str[1]

df = df.withColumn("domain", extract_domain_vectorized(col("email")))

# 5-10x faster than Python UDF (still slower than built-ins)

================================================================================
MISTAKE #3: Repeated Actions Without Caching
================================================================================

❌ WRONG - Recompute Everything:
────────────────────────────────

# Expensive transformation chain
df = spark.read.parquet("s3://bucket/data/")
df_filtered = df.filter(col("status") == "active")
df_enriched = df_filtered.join(dimension_table, "id")
df_processed = df_enriched.withColumn("score", complex_calculation(...))

# Action 1: Recomputes everything
count = df_processed.count()

# Action 2: Recomputes everything AGAIN!
df_processed.write.parquet("s3://output/")

# Action 3: Recomputes everything AGAIN!
df_processed.groupBy("category").sum("amount").show()

WHY THIS IS BAD:
• Same computation repeated multiple times
• Reads source data 3 times
• Wastes time and resources
• Lineage grows (longer recovery time)

✅ CORRECT - Cache Intermediate Results:
────────────────────────────────────────

# Expensive transformation chain
df = spark.read.parquet("s3://bucket/data/")
df_filtered = df.filter(col("status") == "active")
df_enriched = df_filtered.join(dimension_table, "id")
df_processed = df_enriched.withColumn("score", complex_calculation(...))

# Cache after expensive operations, before multiple actions
df_processed.cache()

# Action 1: Computes and caches
count = df_processed.count()

# Action 2: Uses cached data (fast!)
df_processed.write.parquet("s3://output/")

# Action 3: Uses cached data (fast!)
df_processed.groupBy("category").sum("amount").show()

# Unpersist when done
df_processed.unpersist()

PERFORMANCE IMPACT:
❌ Without cache: 45 min + 45 min + 45 min = 135 minutes
✅ With cache: 45 min + 2 min + 2 min = 49 minutes (2.7x faster!)

CACHING BEST PRACTICES:
✅ Cache after expensive operations (joins, aggregations)
✅ Cache before multiple actions
✅ Use MEMORY_AND_DISK if data doesn't fit in memory
✅ Unpersist() when done to free memory
❌ Don't cache if only one action
❌ Don't cache if data is small (overhead)

================================================================================
MISTAKE #4: Creating Too Many Small Files
================================================================================

❌ WRONG - Small File Problem:
──────────────────────────────

# Write with default partitioning
df.write.parquet("s3://bucket/output/")

# Result: 1000 partitions = 1000 files (each 5 MB)
# Problem: "Small file problem"

WHY THIS IS BAD:
• S3 list operations slow with many files
• Metadata overhead (each file has metadata)
• Inefficient reads (open/close overhead)
• Higher costs (S3 LIST/GET requests)

SYMPTOMS:
• Thousands of small files in output directory
• Slow subsequent reads
• High S3 API costs

✅ CORRECT - Repartition Before Writing:
────────────────────────────────────────

# Option 1: Repartition to fewer files
df.repartition(10).write.parquet("s3://bucket/output/")
# Result: 10 files (each 500 MB) ✅

# Option 2: Coalesce (more efficient than repartition)
df.coalesce(10).write.parquet("s3://bucket/output/")

# Option 3: Use maxRecordsPerFile
df.write.option("maxRecordsPerFile", 100000) \
    .parquet("s3://bucket/output/")

TARGET FILE SIZE:
• S3/HDFS: 128 MB - 1 GB per file
• Too small (< 10 MB): Overhead problems
• Too large (> 2 GB): Memory problems during reads

CALCULATE OPTIMAL PARTITIONS:
total_data_size = 10_000  # MB
target_file_size = 500  # MB
optimal_partitions = total_data_size // target_file_size  # 20 partitions

df.repartition(optimal_partitions).write.parquet("...")

================================================================================
MISTAKE #5: Not Handling Null Values
================================================================================

❌ WRONG - Ignoring Nulls:
──────────────────────────

# Division without null checking
df = df.withColumn("ratio", col("numerator") / col("denominator"))

# Result: NaN, Infinity, or errors

# String operations without null checking
df = df.withColumn("upper_name", col("name").upper())

# Result: Crashes on null

✅ CORRECT - Explicit Null Handling:
────────────────────────────────────

# Option 1: Filter nulls
df_clean = df.filter(col("denominator").isNotNull() & (col("denominator") != 0))
df_clean = df_clean.withColumn("ratio", col("numerator") / col("denominator"))

# Option 2: Replace nulls
df = df.withColumn("ratio",
    when(col("denominator").isNull() | (col("denominator") == 0), lit(0))
    .otherwise(col("numerator") / col("denominator"))
)

# Option 3: Use coalesce
df = df.withColumn("name_upper", 
    coalesce(col("name").upper(), lit("UNKNOWN")))

NULL-SAFE OPERATIONS:
• isNull() / isNotNull()
• na.drop() / na.fill()
• coalesce(col1, col2, default)
• when().otherwise()

================================================================================
MISTAKE #6: Wrong Shuffle Partition Configuration
================================================================================

❌ WRONG - Default 200 Partitions:
──────────────────────────────────

# Default spark.sql.shuffle.partitions = 200
df.groupBy("category").count().show()

PROBLEMS:
• Too many partitions for small data (overhead)
• Too few partitions for large data (skew)

SYMPTOMS:
• Small data: Tasks complete in < 1 second (too much overhead)
• Large data: Tasks take > 5 minutes (too much data per task)

✅ CORRECT - Configure for Data Size:
─────────────────────────────────────

# Small data (< 10 GB)
spark.conf.set("spark.sql.shuffle.partitions", "10")

# Medium data (10-100 GB)
spark.conf.set("spark.sql.shuffle.partitions", "50")

# Large data (> 100 GB)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Or use Adaptive Query Execution (Spark 3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

RULE OF THUMB:
partitions = data_size_gb * 2
# Example: 50 GB data → 100 partitions

TARGET PARTITION SIZE:
• 128 MB - 1 GB per partition
• Not too small (< 10 MB): overhead
• Not too large (> 2 GB): memory issues

================================================================================
MISTAKE #7: Using repartition() Instead of coalesce()
================================================================================

❌ WRONG - Full Shuffle When Reducing:
──────────────────────────────────────

# Reduce from 1000 to 10 partitions
df_large = df.repartition(1000)  # 1000 partitions
df_small = df_large.repartition(10)  # Full shuffle! ⚠️

WHY THIS IS BAD:
• repartition() always does full shuffle
• All data moves across network
• Expensive for simple reduction

✅ CORRECT - Use coalesce():
────────────────────────────

# Reduce from 1000 to 10 partitions
df_large = df.repartition(1000)
df_small = df_large.coalesce(10)  # No shuffle! ✅

# coalesce merges adjacent partitions (efficient)

WHEN TO USE EACH:
repartition(): 
  • Increase partitions (10 → 100)
  • Even distribution needed
  • Partition by specific columns

coalesce():
  • Decrease partitions (100 → 10)
  • Final output file count
  • No rebalancing needed

PERFORMANCE:
❌ repartition(10): 5 minutes (full shuffle)
✅ coalesce(10): 10 seconds (merge only)

================================================================================
MISTAKE #8: Cartesian Joins (Cross Joins)
================================================================================

❌ WRONG - Accidental Cartesian Product:
────────────────────────────────────────

# Missing join condition!
df1 = spark.range(1000)
df2 = spark.range(1000)

result = df1.crossJoin(df2)  # 1000 × 1000 = 1M rows!

# Or implicit cross join
result = df1.join(df2)  # No join condition → cross join!

WHY THIS IS BAD:
• Explodes data: N × M rows
• 1000 × 1000 = 1,000,000 rows
• 10K × 10K = 100,000,000 rows
• Usually a mistake

✅ CORRECT - Explicit Join Condition:
─────────────────────────────────────

# Option 1: Inner join on key
result = df1.join(df2, df1.id == df2.id, "inner")

# Option 2: Use join column name
result = df1.join(df2, "id", "inner")

# Option 3: If you REALLY need cross join, be explicit
spark.conf.set("spark.sql.crossJoin.enabled", "true")
result = df1.crossJoin(df2)  # Explicit cross join

SPARK WARNING:
"Detected implicit cartesian product for INNER join between logical plans"

================================================================================
MISTAKE #9: Not Using Broadcast Joins for Small Tables
================================================================================

❌ WRONG - Shuffle Both Tables:
───────────────────────────────

# Large table (100 GB)
orders = spark.read.parquet("orders")  # 100M rows

# Small table (10 MB)
products = spark.read.parquet("products")  # 1000 rows

# Regular join (shuffles BOTH tables)
result = orders.join(products, "product_id")

WHY THIS IS BAD:
• Shuffles large orders table (expensive)
• Shuffles small products table (unnecessary)

✅ CORRECT - Broadcast Small Table:
───────────────────────────────────

from pyspark.sql.functions import broadcast

# Broadcast small table to all executors
result = orders.join(broadcast(products), "product_id")

WHAT HAPPENS:
• products (10 MB) sent to all executors once
• orders stays partitioned (no shuffle)
• Massive speedup!

PERFORMANCE:
❌ Regular join: 45 minutes
✅ Broadcast join: 5 minutes (9x faster!)

AUTO-BROADCAST:
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB
# Spark auto-broadcasts tables < 10 MB

================================================================================
MISTAKE #10: Wrong Data Types
================================================================================

❌ WRONG - Everything as String:
────────────────────────────────

# Read CSV without schema
df = spark.read.csv("data.csv", header=True)

# All columns are strings!
df.select("age").show()  # age: string
df.select("amount").show()  # amount: string
df.select("date").show()  # date: string

WHY THIS IS BAD:
• No type safety (logic errors)
• Slower operations (string operations)
• More memory (strings larger than ints)
• Wrong aggregations (sum on strings?)

✅ CORRECT - Explicit Schema:
─────────────────────────────

from pyspark.sql.types import *

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("date", DateType(), True)
])

df = spark.read.csv("data.csv", header=True, schema=schema)

# Or cast after reading
df = df.withColumn("age", col("age").cast(IntegerType())) \
    .withColumn("amount", col("amount").cast(DoubleType()))

BENEFITS:
✅ Type safety (catch errors early)
✅ Faster operations (int operations vs string)
✅ Less memory (int 4 bytes vs string 10+ bytes)
✅ Correct aggregations

================================================================================
ADDITIONAL COMMON MISTAKES:
================================================================================

11. Writing to Single Partition
   ❌ df.repartition(1).write.parquet()  # Bottleneck!
   ✅ Configure appropriate parallelism

12. Not Using Partitioning for Large Tables
   ❌ df.write.parquet("output/")
   ✅ df.write.partitionBy("year", "month").parquet("output/")

13. Ignoring Data Skew
   ❌ groupBy on skewed keys (one partition gets 90% of data)
   ✅ Salting, broadcast joins, adaptive execution

14. Using DataFrame show() in Production
   ❌ df.show()  # Collects data to driver
   ✅ df.write() or df.count() for validation

15. Not Setting Appropriate Logging Level
   ❌ Default: INFO (too verbose)
   ✅ spark.sparkContext.setLogLevel("WARN")

16. Mixing Pandas and Spark Incorrectly
   ❌ pandas_df = df.toPandas()  # Collects all data
   ✅ Use pandas UDFs for distributed pandas operations

17. Not Handling Time Zones
   ❌ Assuming UTC everywhere
   ✅ Explicit timezone conversions

18. Using deprecated APIs
   ❌ df.registerTempTable()  # Deprecated
   ✅ df.createOrReplaceTempView()

19. Not Persisting Checkpoints
   ❌ Long lineage without checkpointing
   ✅ df.checkpoint() for iterative algorithms

20. Forgetting to Stop SparkSession
   ❌ Long-running notebooks with active sessions
   ✅ spark.stop() when done

================================================================================
DEBUGGING CHECKLIST:
================================================================================

WHEN YOUR JOB IS SLOW:
☐ Check for collect() calls
☐ Look for Python UDFs (use built-ins)
☐ Check shuffle partition count
☐ Verify caching strategy
☐ Look for cross joins
☐ Check for broadcast join opportunities
☐ Review Spark UI (stages, tasks, shuffle)

WHEN YOU GET OOM ERRORS:
☐ Reduce executor memory allocation
☐ Increase memory per executor
☐ Check for data skew
☐ Remove unnecessary columns early
☐ Use cache() wisely (not everywhere)
☐ Check for collect() or toPandas()

WHEN RESULTS ARE WRONG:
☐ Handle null values explicitly
☐ Check data types (schema)
☐ Verify time zone handling
☐ Check join conditions
☐ Look for truncation/rounding

================================================================================
RELATED RESOURCES:
================================================================================

Spark Performance Tuning:
  https://spark.apache.org/docs/latest/tuning.html

Common PySpark Pitfalls:
  https://sparkbyexamples.com/pyspark/pyspark-common-mistakes/

Databricks Best Practices:
  https://docs.databricks.com/optimizations/best-practices.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - Common Mistakes Guide
CREATED: 2024
================================================================================
"""

# This file is documentation-focused
# Run security/01_security_best_practices.py for executable examples

print("""
This module is documentation-focused.

For executable examples, see:
- 01_security_best_practices.py
- ../optimization/ directory
- ../cluster_computing/ directory
""")
