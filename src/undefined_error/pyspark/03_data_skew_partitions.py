"""
PySpark Undefined Behavior: Data Skew & Partition Issues

Demonstrates dangerous patterns related to unbalanced data distribution.

UNDEFINED BEHAVIORS COVERED:
============================
1. Data skew causing OOM errors
2. Single partition bottlenecks
3. Unbalanced joins (hot keys)
4. Repartition vs coalesce misuse
5. Too many/few partitions
6. Partition skew in aggregations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, rand, randn
import time


def dangerous_data_skew():
    """
    ⚠️ UNDEFINED BEHAVIOR: Severe data skew causes executor OOM.
    
    PROBLEM: 99% of data in one partition.
    RESULT: One executor OOMs, others idle.
    """
    spark = SparkSession.builder.appName("Skew UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: Severe data skew")
    
    # 99% of records have same key!
    df = spark.range(1000000).withColumn(
        "key",
        when(col("id") < 990000, lit("hot_key"))  # 99% have "hot_key"
        .otherwise(col("id").cast("string"))
    )
    
    # ❌ GroupBy on skewed key - one partition gets 990K rows!
    start = time.time()
    result = df.groupBy("key").count()
    result.show(10)
    elapsed = time.time() - start
    
    print(f"   Time: {elapsed:.2f}s (with skew)")
    print("   ⚠️ One partition/executor does 99% of work!")
    
    spark.stop()


def safe_salting():
    """
    ✅ SAFE: Use salting to distribute hot keys.
    """
    spark = SparkSession.builder.appName("Salting").master("local[*]").getOrCreate()
    
    print("✅ SAFE: Salting to distribute hot keys")
    
    # Add salt to hot keys
    df = spark.range(1000000).withColumn(
        "key",
        when(col("id") < 990000, lit("hot_key"))
        .otherwise(col("id").cast("string"))
    ).withColumn("salt", (rand() * 10).cast("int"))
    
    # Append salt to key
    df_salted = df.withColumn("salted_key", col("key").cast("string") + col("salt").cast("string"))
    
    start = time.time()
    result = df_salted.groupBy("salted_key").count()
    result.show(10)
    elapsed = time.time() - start
    
    print(f"   Time: {elapsed:.2f}s (with salting)")
    print("   ✅ Work distributed across partitions!")
    
    spark.stop()


def dangerous_single_partition():
    """
    ⚠️ UNDEFINED BEHAVIOR: Coalesce(1) creates bottleneck.
    
    PROBLEM: All data forced into single partition.
    RESULT: No parallelism, severe slowdown.
    """
    spark = SparkSession.builder.appName("Single Partition UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: Coalescing to 1 partition")
    
    df = spark.range(10000000)
    
    # ❌ Force all data into 1 partition
    start = time.time()
    single = df.coalesce(1).withColumn("squared", col("id") * col("id"))
    single.count()
    elapsed = time.time() - start
    
    print(f"   Time with 1 partition: {elapsed:.2f}s")
    print("   ⚠️ No parallelism, single-threaded execution!")
    
    spark.stop()


def safe_balanced_partitions():
    """
    ✅ SAFE: Use appropriate partition count.
    """
    spark = SparkSession.builder.appName("Balanced Partitions").master("local[*]").getOrCreate()
    
    print("✅ SAFE: Balanced partition count")
    
    df = spark.range(10000000)
    
    # ✅ Use multiple partitions (2-3x CPU cores)
    start = time.time()
    balanced = df.repartition(8).withColumn("squared", col("id") * col("id"))
    balanced.count()
    elapsed = time.time() - start
    
    print(f"   Time with 8 partitions: {elapsed:.2f}s")
    print("   ✅ Parallel execution across cores!")
    
    spark.stop()


def dangerous_too_many_partitions():
    """
    ⚠️ UNDEFINED BEHAVIOR: Too many small partitions.
    
    PROBLEM: Overhead from task scheduling dominates.
    RESULT: Slower than optimal partition count.
    """
    spark = SparkSession.builder.appName("Too Many Partitions UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: Too many tiny partitions")
    
    df = spark.range(100000)
    
    # ❌ 10,000 partitions for 100K rows (10 rows per partition!)
    start = time.time()
    over_partitioned = df.repartition(10000).withColumn("squared", col("id") * col("id"))
    over_partitioned.count()
    elapsed = time.time() - start
    
    print(f"   Time with 10,000 partitions: {elapsed:.2f}s")
    print("   ⚠️ Task scheduling overhead dominates!")
    
    spark.stop()


print("\n" + "=" * 80)
print("KEY TAKEAWAYS:")
print("=" * 80)
print("❌ Data skew causes executor OOM and poor performance")
print("❌ Single partition eliminates parallelism")
print("❌ Too many partitions increases scheduling overhead")
print("✅ Use salting for hot keys")
print("✅ Target 2-3x CPU cores for partition count")
print("✅ Aim for 128MB-1GB per partition")
print("=" * 80 + "\n")
