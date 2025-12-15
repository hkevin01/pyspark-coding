#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
ADVANCED PARQUET OPTIMIZATION - Performance Tuning
================================================================================

📖 OVERVIEW:
Advanced techniques for optimizing Parquet file operations including
vectorized reads, file sizing, partition strategies, and performance tuning.

🎯 TOPICS COVERED:
• Vectorized columnar processing
• File size optimization
• Z-ordering and data skipping
• Partition pruning strategies
• Performance monitoring

🚀 RUN:
spark-submit 02_advanced_parquet_optimization.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, count, avg, sum as spark_sum, 
    max as spark_max, min as spark_min, rand, lit
)
from pyspark.sql.types import *
import time


def create_spark_session():
    """Create Spark session with optimal Parquet settings."""
    print("=" * 80)
    print("🚀 CREATING OPTIMIZED SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("Advanced Parquet Optimization") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("✅ Vectorized reader: enabled")
    print("✅ Batch size: 4096 rows")
    print("✅ Max partition bytes: 128MB")
    print("✅ Adaptive query execution: enabled")
    print()
    
    return spark


def example_1_file_sizing_optimization(spark):
    """Example 1: Optimal file sizing for performance."""
    print("=" * 80)
    print("EXAMPLE 1: File Sizing Optimization")
    print("=" * 80)
    
    # Create dataset
    rows = 1000000
    df = spark.range(0, rows) \
        .withColumn("value", expr("id * 1.5")) \
        .withColumn("category", expr(f"'Category_' || (id % 10)")) \
        .withColumn("description", expr("'Description for record ' || id"))
    
    print(f"📊 Dataset: {df.count():,} rows\n")
    
    # Bad: Too many small files
    print("❌ BAD PRACTICE: Many small files (repartition 200)")
    start = time.time()
    output_path = "/tmp/parquet_examples/many_small_files"
    df.repartition(200).write.mode("overwrite").parquet(output_path)
    duration = time.time() - start
    
    import subprocess
    result = subprocess.run(['find', output_path, '-name', '*.parquet', '-type', 'f'], 
                          capture_output=True, text=True)
    file_count = len(result.stdout.strip().split('\n'))
    result = subprocess.run(['du', '-sh', output_path], capture_output=True, text=True)
    size = result.stdout.strip().split()[0]
    
    print(f"   Files: {file_count}")
    print(f"   Size: {size}")
    print(f"   Write time: {duration:.2f}s")
    
    # Good: Optimal file size (128MB-1GB per file)
    print("\n✅ GOOD PRACTICE: Optimal file size (coalesce to 4)")
    start = time.time()
    output_path = "/tmp/parquet_examples/optimal_files"
    df.coalesce(4).write.mode("overwrite").parquet(output_path)
    duration = time.time() - start
    
    result = subprocess.run(['find', output_path, '-name', '*.parquet', '-type', 'f'], 
                          capture_output=True, text=True)
    file_count = len(result.stdout.strip().split('\n'))
    result = subprocess.run(['du', '-sh', output_path], capture_output=True, text=True)
    size = result.stdout.strip().split()[0]
    
    print(f"   Files: {file_count}")
    print(f"   Size: {size}")
    print(f"   Write time: {duration:.2f}s")
    
    print("\n💡 File sizing guidelines:")
    print("   • Target: 128MB - 1GB per file")
    print("   • Avoid: 1000s of small files (overhead)")
    print("   • Avoid: Few very large files (poor parallelism)")
    print()


def example_2_partition_pruning(spark):
    """Example 2: Effective partition pruning strategies."""
    print("=" * 80)
    print("EXAMPLE 2: Partition Pruning Strategies")
    print("=" * 80)
    
    # Create dataset with date partitions
    data = []
    for year in [2023, 2024]:
        for month in range(1, 13):
            for day in range(1, 29):
                for i in range(100):
                    data.append((
                        year, month, day,
                        f"user_{i % 1000}",
                        f"event_{i % 10}",
                        i * 10.0
                    ))
    
    columns = ["year", "month", "day", "user_id", "event_type", "value"]
    df = spark.createDataFrame(data[:50000], columns)  # Limit for demo
    
    print(f"📊 Dataset: {df.count():,} rows")
    
    # Write with hierarchical partitioning
    output_path = "/tmp/parquet_examples/date_partitioned"
    df.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)
    
    print(f"✅ Written with year/month partitions")
    
    # Query with partition pruning
    print("\n📊 Query: Events in January 2024")
    start = time.time()
    result = spark.read.parquet(output_path) \
        .filter((col("year") == 2024) & (col("month") == 1)) \
        .count()
    duration = time.time() - start
    
    print(f"   Result: {result:,} rows")
    print(f"   Query time: {duration:.2f}s")
    print(f"   ✅ Only scans year=2024/month=1 partition")
    
    # Query without partition pruning
    print("\n📊 Query: Event type 'event_5' (no partition filter)")
    start = time.time()
    result = spark.read.parquet(output_path) \
        .filter(col("event_type") == "event_5") \
        .count()
    duration = time.time() - start
    
    print(f"   Result: {result:,} rows")
    print(f"   Query time: {duration:.2f}s")
    print(f"   ⚠️  Scans ALL partitions")
    
    print("\n💡 Partition pruning best practices:")
    print("   • Partition by columns used in WHERE clauses")
    print("   • Use low-cardinality columns (date, region, category)")
    print("   • Avoid over-partitioning (< 10,000 partitions ideal)")
    print("   • Each partition should be 1GB+ for optimal performance")
    print()


def example_3_vectorized_processing(spark):
    """Example 3: Vectorized columnar processing."""
    print("=" * 80)
    print("EXAMPLE 3: Vectorized Columnar Processing")
    print("=" * 80)
    
    # Create large dataset for performance comparison
    rows = 5000000
    df = spark.range(0, rows) \
        .withColumn("value1", expr("id * 1.5")) \
        .withColumn("value2", expr("id * 2.3")) \
        .withColumn("value3", expr("id * 0.8"))
    
    output_path = "/tmp/parquet_examples/vectorized_test"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"📊 Dataset: {rows:,} rows, 4 columns")
    
    # Read with vectorized reader (default)
    print("\n✅ WITH vectorized reader:")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    start = time.time()
    result = spark.read.parquet(output_path) \
        .select(
            avg("value1").alias("avg1"),
            avg("value2").alias("avg2"),
            avg("value3").alias("avg3")
        ).collect()
    duration_vectorized = time.time() - start
    print(f"   Query time: {duration_vectorized:.2f}s")
    
    # Read without vectorized reader
    print("\n❌ WITHOUT vectorized reader:")
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    start = time.time()
    result = spark.read.parquet(output_path) \
        .select(
            avg("value1").alias("avg1"),
            avg("value2").alias("avg2"),
            avg("value3").alias("avg3")
        ).collect()
    duration_non_vectorized = time.time() - start
    print(f"   Query time: {duration_non_vectorized:.2f}s")
    
    # Re-enable vectorized reader
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    
    speedup = duration_non_vectorized / duration_vectorized
    print(f"\n🚀 Speedup: {speedup:.2f}x faster with vectorized reader")
    
    print("\n💡 Vectorized processing benefits:")
    print("   • Processes data in batches (e.g., 4096 rows)")
    print("   • Reduces overhead of row-by-row processing")
    print("   • Leverages CPU SIMD instructions")
    print("   • Best for primitive types (numeric, dates)")
    print()


def example_4_column_pruning_benefits(spark):
    """Example 4: Demonstrate column pruning benefits."""
    print("=" * 80)
    print("EXAMPLE 4: Column Pruning Benefits")
    print("=" * 80)
    
    # Create wide dataset (many columns)
    df = spark.range(0, 1000000)
    for i in range(1, 51):  # 50 additional columns
        df = df.withColumn(f"col_{i}", expr(f"id * {i}"))
    
    output_path = "/tmp/parquet_examples/wide_table"
    df.write.mode("overwrite").parquet(output_path)
    
    print(f"📊 Dataset: 1,000,000 rows × {len(df.columns)} columns")
    
    # Read all columns
    print("\n❌ Reading ALL columns:")
    start = time.time()
    df_all = spark.read.parquet(output_path)
    result = df_all.count()
    duration_all = time.time() - start
    print(f"   Query time: {duration_all:.2f}s")
    
    # Read only 3 columns
    print("\n✅ Reading only 3 columns (id, col_1, col_2):")
    start = time.time()
    df_pruned = spark.read.parquet(output_path).select("id", "col_1", "col_2")
    result = df_pruned.count()
    duration_pruned = time.time() - start
    print(f"   Query time: {duration_pruned:.2f}s")
    
    speedup = duration_all / duration_pruned
    print(f"\n🚀 Speedup: {speedup:.2f}x faster with column pruning")
    
    print("\n💡 Column pruning best practices:")
    print("   • Always SELECT only needed columns")
    print("   • Avoid SELECT * in production queries")
    print("   • Columnar format makes this extremely efficient")
    print("   • Savings increase with more columns")
    print()


def example_5_statistics_and_filtering(spark):
    """Example 5: Use statistics for efficient filtering."""
    print("=" * 80)
    print("EXAMPLE 5: Statistics and Data Skipping")
    print("=" * 80)
    
    # Create dataset with clear ranges per partition
    df = spark.range(0, 1000000) \
        .withColumn("category", expr("CASE " +
            "WHEN id < 250000 THEN 'A' " +
            "WHEN id < 500000 THEN 'B' " +
            "WHEN id < 750000 THEN 'C' " +
            "ELSE 'D' END")) \
        .withColumn("value", expr("id * 2"))
    
    output_path = "/tmp/parquet_examples/with_stats"
    df.repartition(4, "category").write.mode("overwrite").parquet(output_path)
    
    print(f"📊 Dataset: {df.count():,} rows, 4 partitions")
    
    # Query with filter on range
    print("\n📊 Query: id BETWEEN 100000 AND 200000")
    start = time.time()
    result = spark.read.parquet(output_path) \
        .filter((col("id") >= 100000) & (col("id") <= 200000)) \
        .count()
    duration = time.time() - start
    
    print(f"   Result: {result:,} rows")
    print(f"   Query time: {duration:.2f}s")
    print(f"   ✅ Parquet statistics skip files outside range")
    
    print("\n💡 Statistics in Parquet:")
    print("   • Min/max values stored per row group")
    print("   • Enables data skipping (skip entire files)")
    print("   • Automatic - no configuration needed")
    print("   • Works best with sorted/clustered data")
    print()


def example_6_performance_monitoring(spark):
    """Example 6: Monitor Parquet query performance."""
    print("=" * 80)
    print("EXAMPLE 6: Performance Monitoring")
    print("=" * 80)
    
    # Create and query dataset
    df = spark.range(0, 1000000) \
        .withColumn("category", expr("'Category_' || (id % 100)")) \
        .withColumn("value", expr("id * rand()"))
    
    output_path = "/tmp/parquet_examples/perf_test"
    df.write.mode("overwrite").parquet(output_path)
    
    # Run query with metrics
    print("📊 Running aggregation query...")
    query = spark.read.parquet(output_path) \
        .groupBy("category") \
        .agg(
            count("*").alias("count"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value")
        )
    
    start = time.time()
    result = query.collect()
    duration = time.time() - start
    
    print(f"✅ Query completed in {duration:.2f}s")
    print(f"   Categories: {len(result)}")
    
    # Show query plan
    print("\n📋 Query plan (optimized):")
    query.explain()
    
    print("\n💡 Performance monitoring tips:")
    print("   • Use .explain() to see query plan")
    print("   • Check Spark UI for detailed metrics")
    print("   • Monitor: Scan time, shuffle size, GC time")
    print("   • Look for: ColumnarToRow, Filter, Project operations")
    print()


def main():
    """Main execution function."""
    print("\n" + "🔥 " * 40)
    print("ADVANCED PARQUET OPTIMIZATION")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    try:
        example_1_file_sizing_optimization(spark)
        example_2_partition_pruning(spark)
        example_3_vectorized_processing(spark)
        example_4_column_pruning_benefits(spark)
        example_5_statistics_and_filtering(spark)
        example_6_performance_monitoring(spark)
        
        print("=" * 80)
        print("✅ ALL OPTIMIZATION EXAMPLES COMPLETED")
        print("=" * 80)
        
        print("""
📋 PARQUET OPTIMIZATION CHECKLIST:

1. ✅ File Sizing
   [x] Target 128MB - 1GB per file
   [x] Use coalesce/repartition to control file count
   [x] Avoid thousands of tiny files

2. ✅ Partitioning
   [x] Partition by low-cardinality columns
   [x] Use columns in WHERE clauses
   [x] Keep partition count < 10,000
   [x] Each partition > 1GB

3. ✅ Query Optimization
   [x] SELECT only needed columns (column pruning)
   [x] Apply filters early (predicate pushdown)
   [x] Use partition columns in filters
   [x] Enable vectorized reader

4. ✅ Compression
   [x] Use snappy for general use
   [x] Use gzip for cold storage
   [x] Consider lz4 for read-heavy workloads

5. ✅ Monitoring
   [x] Check query plans with .explain()
   [x] Monitor Spark UI metrics
   [x] Track: scan time, shuffle size, memory
   [x] Identify bottlenecks

🎯 Performance Tips:
   • Vectorized reader: 2-10x speedup
   • Column pruning: Up to 50x for wide tables
   • Partition pruning: 10-100x for large datasets
   • Proper file sizing: 2-5x improvement
        """)
    
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()
        print("\n✅ Spark session stopped")


if __name__ == "__main__":
    main()
