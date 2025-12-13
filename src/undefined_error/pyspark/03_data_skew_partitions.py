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

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, randn, when


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
        when(col("id") < 990000, lit("hot_key")).otherwise(  # 99% have "hot_key"
            col("id").cast("string")
        ),
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

    SALTING EXPLAINED:
    ------------------
    Data Skew Problem:
    • "hot_key" appears 990,000 times (99% of data)
    • Other keys appear once each (1% of data)
    • groupBy("key") puts 99% of work in ONE partition!
    • That partition takes 99x longer than others

    Salting Solution:
    • Add random suffix to split hot key into multiple keys
    • "hot_key" becomes "hot_key0", "hot_key1", ..., "hot_key9"
    • Each salted key processed in separate partition
    • Work distributed evenly across 10 partitions

    Why rand() Works:
    • rand() generates uniform random float in [0.0, 1.0)
    • Multiply by N: rand() * 10 gives [0.0, 10.0)
    • Cast to int: produces {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    • Each value has ~10% probability (uniform distribution)
    • Over 990K rows: ~99K rows get each salt value
    • Result: 10 balanced partitions of ~99K rows each!

    Performance Impact:
    • Before: 1 partition processes 990K rows (bottleneck)
    • After: 10 partitions each process ~99K rows (parallel)
    • Speedup: ~10x faster execution!
    """
    spark = SparkSession.builder.appName("Salting").master("local[*]").getOrCreate()

    print("✅ SAFE: Salting to distribute hot keys")

    # Add salt to hot keys
    # Step 1: Create skewed data (990K "hot_key", 10K unique keys)
    df = (
        spark.range(1000000)
        .withColumn(
            "key",
            when(col("id") < 990000, lit("hot_key")).otherwise(  # 99% of data!
                col("id").cast("string")
            ),  # 1% of data
        )
        .withColumn("salt", (rand() * 10).cast("int"))
    )  # Random 0-9
    # Now each row has: {id, key, salt}
    # "hot_key" rows get random salt: 0, 1, 2, ..., 9

    # Step 2: Append salt to key to create unique salted keys
    df_salted = df.withColumn(
        "salted_key", col("key").cast("string") + col("salt").cast("string")
    )
    # "hot_key" + "0" = "hot_key0"
    # "hot_key" + "1" = "hot_key1"
    # ...
    # "hot_key" + "9" = "hot_key9"
    # Now we have 10 different keys instead of 1 hot key!

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
    spark = (
        SparkSession.builder.appName("Single Partition UB")
        .master("local[*]")
        .getOrCreate()
    )

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
    spark = (
        SparkSession.builder.appName("Balanced Partitions")
        .master("local[*]")
        .getOrCreate()
    )

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
    spark = (
        SparkSession.builder.appName("Too Many Partitions UB")
        .master("local[*]")
        .getOrCreate()
    )

    print("❌ DANGEROUS: Too many tiny partitions")

    df = spark.range(100000)

    # ❌ 10,000 partitions for 100K rows (10 rows per partition!)
    start = time.time()
    over_partitioned = df.repartition(10000).withColumn(
        "squared", col("id") * col("id")
    )
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
