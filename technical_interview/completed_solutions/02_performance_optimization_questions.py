#!/usr/bin/env python3
"""
PySpark Technical Interview Questions - Performance & Optimization
===================================================================

Advanced questions focusing on optimization, performance tuning,
and best practices.
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    expr,
    lit,
    monotonically_increasing_id,
    when,
)
from pyspark.sql.functions import max as _max
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def create_spark_session():
    """Create Spark session for optimization practice."""
    return (
        SparkSession.builder.appName("PySpark_Interview_Optimization")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


# ============================================================================
# QUESTION 1: Broadcast Join vs Regular Join
# ============================================================================
def question_1_broadcast_join(spark):
    """
    When and how to use broadcast joins for performance.

    Key Concept: Broadcast small tables (<10MB) to avoid shuffles.
    """
    print("\n" + "=" * 70)
    print("QUESTION 1: Broadcast Join Optimization")
    print("=" * 70)

    # Large dataset
    large_data = [(i, f"user_{i}", i % 100) for i in range(10000)]
    large_df = spark.createDataFrame(large_data, ["id", "name", "region_id"])

    # Small lookup table
    small_data = [(i, f"Region_{i}") for i in range(100)]
    small_df = spark.createDataFrame(small_data, ["region_id", "region_name"])

    print("\nLarge DataFrame size:", large_df.count())
    print("Small DataFrame size:", small_df.count())

    # Regular Join (causes shuffle)
    print("\n--- Regular Join ---")
    start = time.time()
    regular_join = large_df.join(small_df, "region_id")
    regular_count = regular_join.count()
    regular_time = time.time() - start
    print(f"Regular join time: {regular_time:.3f}s")

    # Broadcast Join (no shuffle for large table)
    print("\n--- Broadcast Join ---")
    start = time.time()
    broadcast_join = large_df.join(broadcast(small_df), "region_id")
    broadcast_count = broadcast_join.count()
    broadcast_time = time.time() - start
    print(f"Broadcast join time: {broadcast_time:.3f}s")

    # Show execution plans
    print("\n--- Regular Join Plan ---")
    regular_join.explain()

    print("\n--- Broadcast Join Plan ---")
    broadcast_join.explain()

    print(f"\n✅ Speedup: {regular_time/broadcast_time:.2f}x faster with broadcast")

    return broadcast_join


# ============================================================================
# QUESTION 2: Avoid Multiple Passes - Cache/Persist
# ============================================================================
def question_2_caching_strategy(spark):
    """
    When and how to cache DataFrames effectively.

    Key Concept: Cache when DataFrame is used multiple times.
    """
    print("\n" + "=" * 70)
    print("QUESTION 2: Caching Strategy")
    print("=" * 70)

    # Create large dataset
    data = [(i, f"user_{i}", i * 2, i % 10) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "name", "value", "category"])

    # WITHOUT CACHING (multiple passes over data)
    print("\n--- WITHOUT CACHING ---")
    start = time.time()

    count1 = df.filter(col("category") == 5).count()
    count2 = df.filter(col("value") > 50000).count()
    count3 = df.groupBy("category").count().count()

    no_cache_time = time.time() - start
    print(f"Time without caching: {no_cache_time:.3f}s")
    print(f"Results: {count1}, {count2}, {count3}")

    # WITH CACHING
    print("\n--- WITH CACHING ---")
    df_cached = df.cache()
    df_cached.count()  # Materialize cache

    start = time.time()

    count1 = df_cached.filter(col("category") == 5).count()
    count2 = df_cached.filter(col("value") > 50000).count()
    count3 = df_cached.groupBy("category").count().count()

    cache_time = time.time() - start
    print(f"Time with caching: {cache_time:.3f}s")
    print(f"Results: {count1}, {count2}, {count3}")

    print(f"\n✅ Speedup: {no_cache_time/cache_time:.2f}x faster with caching")

    # Clean up
    df_cached.unpersist()

    return df_cached


# ============================================================================
# QUESTION 3: Partition Tuning
# ============================================================================
def question_3_partition_optimization(spark):
    """
    How to optimize partition size for performance.

    Key Concept: Aim for 128MB-200MB per partition.
    """
    print("\n" + "=" * 70)
    print("QUESTION 3: Partition Optimization")
    print("=" * 70)

    # Create dataset
    data = [(i, f"user_{i}", i % 100) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "name", "category"])

    print(f"\nOriginal partitions: {df.rdd.getNumPartitions()}")

    # Too many partitions (overhead)
    df_many = df.repartition(1000)
    print(f"Too many partitions: {df_many.rdd.getNumPartitions()}")
    print("❌ High overhead, slow performance")

    # Too few partitions (underutilized)
    df_few = df.coalesce(2)
    print(f"Too few partitions: {df_few.rdd.getNumPartitions()}")
    print("❌ Not using all cores, slow")

    # Optimal partitions
    optimal_partitions = spark.sparkContext.defaultParallelism * 2
    df_optimal = df.repartition(optimal_partitions)
    print(f"Optimal partitions: {df_optimal.rdd.getNumPartitions()}")
    print("✅ Balanced workload across executors")

    # Repartition by column for better data locality
    df_by_column = df.repartition(10, "category")
    print(f"\nRepartitioned by 'category': {df_by_column.rdd.getNumPartitions()}")
    print("✅ Related data in same partition - better for aggregations")

    return df_optimal


# ============================================================================
# QUESTION 4: Filter Pushdown
# ============================================================================
def question_4_filter_pushdown(spark):
    """
    Demonstrate filter pushdown optimization.

    Key Concept: Filter early to reduce data processed.
    """
    print("\n" + "=" * 70)
    print("QUESTION 4: Filter Pushdown Optimization")
    print("=" * 70)

    # Create large dataset
    data = [(i, f"user_{i}", i * 2, i % 100) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "name", "value", "region"])

    # BAD: Filter after expensive operations
    print("\n--- BAD: Filter After Aggregation ---")
    bad_query = (
        df.groupBy("region")
        .agg(_sum("value").alias("total_value"))
        .filter(col("region") == 5)
    )  # Filter AFTER aggregation

    start = time.time()
    bad_count = bad_query.count()
    bad_time = time.time() - start

    print(f"Time: {bad_time:.3f}s")
    bad_query.explain()

    # GOOD: Filter before expensive operations
    print("\n--- GOOD: Filter Before Aggregation ---")
    good_query = (
        df.filter(col("region") == 5)
        .groupBy("region")
        .agg(_sum("value").alias("total_value"))
    )  # Filter FIRST, then aggregate

    start = time.time()
    good_count = good_query.count()
    good_time = time.time() - start

    print(f"Time: {good_time:.3f}s")
    good_query.explain()

    print(f"\n✅ Speedup: {bad_time/good_time:.2f}x faster with early filtering")

    return good_query


# ============================================================================
# QUESTION 5: Avoiding Shuffles
# ============================================================================
def question_5_avoid_shuffles(spark):
    """
    Strategies to minimize expensive shuffle operations.

    Key Concept: Shuffles are expensive - minimize them.
    """
    print("\n" + "=" * 70)
    print("QUESTION 5: Avoiding Shuffle Operations")
    print("=" * 70)

    # Create datasets
    data = [(i, i % 100, i * 2) for i in range(10000)]
    df = spark.createDataFrame(data, ["id", "category", "value"])

    print("\n--- Operations that cause SHUFFLE ---")

    # 1. groupBy causes shuffle
    print("\n1. groupBy (causes shuffle):")
    grouped = df.groupBy("category").agg(_sum("value").alias("total"))
    grouped.explain()

    # 2. join causes shuffle
    print("\n2. Join (causes shuffle):")
    df2 = df.withColumn("category2", col("category") + 100)
    joined = df.join(df2, "id")
    joined.explain()

    # 3. repartition causes shuffle
    print("\n3. Repartition (causes shuffle):")
    repart = df.repartition(10)
    repart.explain()

    print("\n--- Operations that AVOID shuffle ---")

    # 1. coalesce (only reduces partitions, no shuffle)
    print("\n1. Coalesce (no shuffle):")
    coal = df.coalesce(2)
    coal.explain()

    # 2. filter (no shuffle)
    print("\n2. Filter (no shuffle):")
    filtered = df.filter(col("category") < 10)
    filtered.explain()

    # 3. select (no shuffle)
    print("\n3. Select (no shuffle):")
    selected = df.select("id", "value")
    selected.explain()

    print("\n✅ Minimize shuffles by:")
    print("  • Using broadcast joins for small tables")
    print("  • Filtering early")
    print("  • Using coalesce instead of repartition when reducing")
    print("  • Caching data before multiple shuffles")

    return grouped


# ============================================================================
# QUESTION 6: Column Pruning
# ============================================================================
def question_6_column_pruning(spark):
    """
    Select only necessary columns to reduce memory usage.

    Key Concept: Don't read columns you don't need.
    """
    print("\n" + "=" * 70)
    print("QUESTION 6: Column Pruning")
    print("=" * 70)

    # Create wide dataset
    data = [
        (i, f"name_{i}", f"email_{i}", i * 2, i * 3, i * 4, i * 5, i * 6, i * 7, i * 8)
        for i in range(10000)
    ]

    df = spark.createDataFrame(
        data,
        ["id", "name", "email", "col1", "col2", "col3", "col4", "col5", "col6", "col7"],
    )

    # BAD: Select all columns
    print("\n--- BAD: Using all columns ---")
    bad_query = df.groupBy("id").agg(count("*").alias("count"))
    start = time.time()
    bad_result = bad_query.count()
    bad_time = time.time() - start
    print(f"Time: {bad_time:.3f}s")

    # GOOD: Select only needed columns
    print("\n--- GOOD: Select only needed columns ---")
    good_query = df.select("id").groupBy("id").agg(count("*").alias("count"))
    start = time.time()
    good_result = good_query.count()
    good_time = time.time() - start
    print(f"Time: {good_time:.3f}s")

    print("\n✅ Always select only the columns you need!")

    return good_query


# ============================================================================
# QUESTION 7: Predicate Pushdown with Parquet
# ============================================================================
def question_7_predicate_pushdown(spark):
    """
    Demonstrate predicate pushdown with Parquet files.

    Key Concept: Filters pushed to storage layer.
    """
    print("\n" + "=" * 70)
    print("QUESTION 7: Predicate Pushdown with Parquet")
    print("=" * 70)

    import os
    import tempfile

    # Create and save data as parquet
    data = [(i, f"user_{i}", i % 100, i * 2) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "name", "category", "value"])

    temp_dir = tempfile.mkdtemp()
    parquet_path = os.path.join(temp_dir, "data.parquet")

    df.write.mode("overwrite").parquet(parquet_path)

    # Read with filter - predicate pushdown
    print("\nReading Parquet with filter (predicate pushdown):")
    filtered_df = spark.read.parquet(parquet_path).filter(col("category") == 5)

    print("\n✅ Parquet will skip reading data that doesn't match the filter!")
    filtered_df.explain()

    # Clean up
    import shutil

    shutil.rmtree(temp_dir)

    return filtered_df


# ============================================================================
# QUESTION 8: Avoiding UDFs
# ============================================================================
def question_8_avoid_udfs(spark):
    """
    Why to avoid UDFs and use built-in functions instead.

    Key Concept: UDFs break optimization, use native functions.
    """
    print("\n" + "=" * 70)
    print("QUESTION 8: Avoid UDFs - Use Built-in Functions")
    print("=" * 70)

    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType

    # Create dataset
    data = [(i, i * 2) for i in range(100000)]
    df = spark.createDataFrame(data, ["id", "value"])

    # BAD: Using UDF
    print("\n--- BAD: Using UDF ---")

    @udf(returnType=IntegerType())
    def multiply_by_10(x):
        return x * 10

    start = time.time()
    bad_result = df.withColumn("result", multiply_by_10(col("value"))).count()
    bad_time = time.time() - start
    print(f"UDF time: {bad_time:.3f}s")
    print("❌ UDF breaks Catalyst optimizer")
    print("❌ Serialization overhead")
    print("❌ Can't push down to Parquet/ORC")

    # GOOD: Using built-in function
    print("\n--- GOOD: Using built-in function ---")
    start = time.time()
    good_result = df.withColumn("result", col("value") * 10).count()
    good_time = time.time() - start
    print(f"Built-in time: {good_time:.3f}s")
    print("✅ Optimized by Catalyst")
    print("✅ No serialization overhead")
    print("✅ Predicate pushdown works")

    print(f"\n✅ Speedup: {bad_time/good_time:.2f}x faster without UDF")

    return df


# ============================================================================
# QUESTION 9: Data Skew Handling
# ============================================================================
def question_9_handle_data_skew(spark):
    """
    Handle data skew in joins and aggregations.

    Key Concept: Salting keys to distribute skewed data.
    """
    print("\n" + "=" * 70)
    print("QUESTION 9: Handling Data Skew")
    print("=" * 70)

    # Create skewed dataset (90% records have same key)
    skewed_data = [(1, f"user_{i}") for i in range(9000)]  # 90% with key=1
    skewed_data += [(i, f"user_{i}") for i in range(2, 1002)]  # 10% with other keys

    df_skewed = spark.createDataFrame(skewed_data, ["key", "value"])

    print("\nData distribution:")
    df_skewed.groupBy("key").count().orderBy(col("count").desc()).show(5)

    # SOLUTION: Add salt to distribute skewed key
    from pyspark.sql.functions import rand

    salt_factor = 10
    df_salted = df_skewed.withColumn(
        "salt", (rand() * salt_factor).cast("int")
    ).withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

    print("\nAfter salting:")
    df_salted.groupBy("salted_key").count().orderBy(col("count").desc()).show(10)

    print("\n✅ Salting distributes skewed data across partitions")
    print("✅ Better parallelism and performance")

    return df_salted


# ============================================================================
# QUESTION 10: Memory Management
# ============================================================================
def question_10_memory_management(spark):
    """
    Best practices for memory management in Spark.

    Key Concept: Balance execution and storage memory.
    """
    print("\n" + "=" * 70)
    print("QUESTION 10: Memory Management Best Practices")
    print("=" * 70)

    print("\n✅ Memory Management Tips:")
    print("\n1. Storage vs Execution Memory:")
    print("   - Storage: cached data (60% by default)")
    print("   - Execution: joins, aggregations (40% by default)")
    print("   - Set spark.memory.fraction (default 0.6)")

    print("\n2. Cache Levels:")
    print("   - MEMORY_ONLY: Fast but can cause OOM")
    print("   - MEMORY_AND_DISK: Safer, spills to disk")
    print("   - DISK_ONLY: Slow but won't cause OOM")

    # Demonstrate different cache levels
    data = [(i, f"user_{i}", i * 2) for i in range(10000)]
    df = spark.createDataFrame(data, ["id", "name", "value"])

    from pyspark import StorageLevel

    # Cache in memory only
    df_mem = df.cache()  # Same as persist(StorageLevel.MEMORY_ONLY)
    print("\n   MEMORY_ONLY cached")

    # Cache in memory and disk
    df_mem_disk = df.persist(StorageLevel.MEMORY_AND_DISK)
    print("   MEMORY_AND_DISK cached")

    print("\n3. Unpersist when done:")
    print("   - Always unpersist() DataFrames when no longer needed")
    print("   - Frees up memory for other operations")

    df_mem.unpersist()
    df_mem_disk.unpersist()

    print("\n4. Monitor memory usage:")
    print("   - Spark UI -> Storage tab")
    print("   - Check for spills to disk")
    print("   - Adjust spark.executor.memory if needed")

    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Run all optimization interview questions."""
    print("\n" + "=" * 70)
    print("PYSPARK TECHNICAL INTERVIEW - PERFORMANCE & OPTIMIZATION")
    print("=" * 70)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        question_1_broadcast_join(spark)
        question_2_caching_strategy(spark)
        question_3_partition_optimization(spark)
        question_4_filter_pushdown(spark)
        question_5_avoid_shuffles(spark)
        question_6_column_pruning(spark)
        question_7_predicate_pushdown(spark)
        question_8_avoid_udfs(spark)
        question_9_handle_data_skew(spark)
        question_10_memory_management(spark)

        print("\n" + "=" * 70)
        print("✅ All optimization questions completed!")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
