#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
ADVANCED PARQUET TECHNIQUES - Performance Optimization & Best Practices
================================================================================

📖 OVERVIEW:
Advanced techniques for working with Parquet files including performance
tuning, statistics, metadata management, and optimization strategies.

🎯 TOPICS COVERED:
• Row group sizing
• Dictionary encoding
• Statistics and metadata
• Coalescing and repartitioning
• Performance benchmarking

🚀 RUN:
spark-submit 02_advanced_parquet_techniques.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import time
import random


def create_spark_session():
    """Create optimized Spark session."""
    spark = SparkSession.builder \
        .appName("Advanced Parquet Techniques") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.block.size", "134217728") \
        .config("spark.sql.parquet.page.size", "1048576") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.recordLevelFilter.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def example_1_row_group_optimization(spark):
    """
    Example 1: Row Group Size Optimization
    
    📝 Demonstrates:
    • Controlling row group size
    • Impact on query performance
    • Memory vs I/O tradeoffs
    """
    print("=" * 80)
    print("EXAMPLE 1: Row Group Size Optimization")
    print("=" * 80)
    
    # Generate large dataset
    print("📊 Generating 1 million records...")
    
    data = [(i, f"user_{i}", random.uniform(0, 1000), random.choice(['A', 'B', 'C']))
            for i in range(1000000)]
    
    df = spark.createDataFrame(data, ['id', 'username', 'value', 'category'])
    
    # Write with different row group sizes
    configs = [
        ("small", 64 * 1024 * 1024),   # 64 MB
        ("medium", 128 * 1024 * 1024),  # 128 MB (default)
        ("large", 256 * 1024 * 1024)    # 256 MB
    ]
    
    print("\n📝 Writing with different row group sizes...")
    print()
    
    for name, size in configs:
        output_path = f"/tmp/pyspark_examples/parquet/rowgroup_{name}"
        
        start = time.time()
        
        df.write \
            .mode("overwrite") \
            .option("parquet.block.size", size) \
            .parquet(output_path)
        
        write_time = time.time() - start
        
        # Get file size
        import os
        total_size = sum(os.path.getsize(os.path.join(root, f))
                        for root, _, files in os.walk(output_path)
                        for f in files if f.endswith('.parquet'))
        
        print(f"   {name.upper()}:")
        print(f"      Row group size: {size / (1024*1024):.0f} MB")
        print(f"      Write time: {write_time:.2f}s")
        print(f"      Total size: {total_size / (1024*1024):.2f} MB")
        print()
    
    print("💡 Row Group Size Guidelines:")
    print("   • Small (64MB): Better for selective queries")
    print("   • Medium (128MB): Good general-purpose default")
    print("   • Large (256MB): Better for full scans")
    print("   • Consider: memory available, query patterns, cluster size")
    print()


def example_2_dictionary_encoding(spark):
    """
    Example 2: Dictionary Encoding
    
    📝 Demonstrates:
    • Automatic dictionary encoding for low-cardinality columns
    • Size savings from encoding
    • When encoding is beneficial
    """
    print("=" * 80)
    print("EXAMPLE 2: Dictionary Encoding")
    print("=" * 80)
    
    # Create dataset with varying cardinality
    print("📊 Creating datasets with different cardinality levels...")
    
    size = 100000
    
    # Low cardinality (perfect for dictionary encoding)
    data_low = [(i, random.choice(['A', 'B', 'C', 'D', 'E'])) for i in range(size)]
    df_low = spark.createDataFrame(data_low, ['id', 'category'])
    
    # High cardinality (poor for dictionary encoding)
    data_high = [(i, f"unique_value_{i}") for i in range(size)]
    df_high = spark.createDataFrame(data_high, ['id', 'category'])
    
    # Write both
    path_low = "/tmp/pyspark_examples/parquet/dict_low_cardinality"
    path_high = "/tmp/pyspark_examples/parquet/dict_high_cardinality"
    
    df_low.write.mode("overwrite").parquet(path_low)
    df_high.write.mode("overwrite").parquet(path_high)
    
    # Compare sizes
    import os
    
    size_low = sum(os.path.getsize(os.path.join(root, f))
                   for root, _, files in os.walk(path_low)
                   for f in files if f.endswith('.parquet'))
    
    size_high = sum(os.path.getsize(os.path.join(root, f))
                    for root, _, files in os.walk(path_high)
                    for f in files if f.endswith('.parquet'))
    
    print(f"\n�� Comparison:")
    print(f"   Low cardinality (5 unique values):")
    print(f"      Size: {size_low / (1024*1024):.2f} MB")
    print(f"   High cardinality (100,000 unique values):")
    print(f"      Size: {size_high / (1024*1024):.2f} MB")
    print(f"   Ratio: {size_high / size_low:.2f}x larger")
    print()
    
    print("💡 Dictionary Encoding Best Practices:")
    print("   • Most effective for cardinality < 10,000")
    print("   • Automatically applied by Parquet writer")
    print("   • Great for: status codes, categories, countries")
    print("   • Poor for: IDs, timestamps, unique strings")
    print()


def example_3_statistics_and_metadata(spark):
    """
    Example 3: Parquet Statistics and Metadata
    
    📝 Demonstrates:
    • Column statistics (min, max, null count)
    • Using statistics for query optimization
    • Metadata inspection
    """
    print("=" * 80)
    print("EXAMPLE 3: Statistics and Metadata")
    print("=" * 80)
    
    # Create dataset with clear min/max values
    data = [
        (1, 'Alice', 25, 50000, '2024-01-15'),
        (2, 'Bob', 30, 75000, '2024-02-20'),
        (3, 'Charlie', 35, 100000, '2024-03-10'),
        (4, 'Diana', 28, 65000, '2024-04-05'),
        (5, 'Eve', 42, 120000, '2024-05-18')
    ]
    
    df = spark.createDataFrame(data, ['id', 'name', 'age', 'salary', 'hire_date'])
    
    path = "/tmp/pyspark_examples/parquet/with_statistics"
    
    print("📝 Writing data with statistics...")
    df.write.mode("overwrite").parquet(path)
    
    print("✅ Statistics automatically collected:")
    print("   • min/max values for each column")
    print("   • null counts")
    print("   • total row counts")
    print()
    
    print("🔍 Demonstrating statistics-based pruning...")
    print("   Query: salary > 150000")
    
    # This query should skip all row groups based on statistics
    df_filtered = spark.read.parquet(path).filter(col('salary') > 150000)
    
    count = df_filtered.count()
    print(f"   Result: {count} rows (Parquet skipped all row groups using stats)")
    print()
    
    print("�� Statistics Benefits:")
    print("   • Skip entire row groups without reading")
    print("   • Automatic for numeric, string, date columns")
    print("   • Enables min/max pruning")
    print("   • Crucial for large datasets")
    print()


def example_4_coalesce_optimization(spark):
    """
    Example 4: Coalesce and Repartition
    
    📝 Demonstrates:
    • Controlling output file count
    • coalesce() vs repartition()
    • Optimal file sizing
    """
    print("=" * 80)
    print("EXAMPLE 4: Coalesce and Repartition")
    print("=" * 80)
    
    # Create large dataset
    print("�� Creating 500,000 record dataset...")
    data = [(i, f"value_{i}", random.randint(1, 100)) for i in range(500000)]
    df = spark.createDataFrame(data, ['id', 'value', 'score'])
    
    print(f"   Initial partitions: {df.rdd.getNumPartitions()}")
    print()
    
    # Write without coalescing (many small files)
    path_many = "/tmp/pyspark_examples/parquet/many_files"
    print("📝 Writing without coalesce...")
    df.write.mode("overwrite").parquet(path_many)
    
    import os
    file_count_many = len([f for _, _, files in os.walk(path_many) 
                           for f in files if f.endswith('.parquet')])
    
    # Write with coalesce (fewer, larger files)
    path_coalesced = "/tmp/pyspark_examples/parquet/coalesced"
    print(f"📝 Writing with coalesce(4)...")
    df.coalesce(4).write.mode("overwrite").parquet(path_coalesced)
    
    file_count_coalesced = len([f for _, _, files in os.walk(path_coalesced)
                                for f in files if f.endswith('.parquet')])
    
    print(f"\n📊 Comparison:")
    print(f"   Without coalesce: {file_count_many} files")
    print(f"   With coalesce(4): {file_count_coalesced} files")
    print()
    
    print("�� File Sizing Guidelines:")
    print("   • Target: 128-512 MB per file")
    print("   • Too many small files: slow metadata operations")
    print("   • Too few large files: poor parallelism")
    print("   • Use coalesce() to reduce (no shuffle)")
    print("   • Use repartition() to increase (with shuffle)")
    print()
    
    # Calculate ideal partition count
    total_size_mb = sum(os.path.getsize(os.path.join(root, f))
                       for root, _, files in os.walk(path_many)
                       for f in files if f.endswith('.parquet')) / (1024*1024)
    
    target_file_size_mb = 256
    ideal_partitions = max(1, int(total_size_mb / target_file_size_mb))
    
    print(f"�� Calculation for this dataset:")
    print(f"   Total size: {total_size_mb:.2f} MB")
    print(f"   Target per file: {target_file_size_mb} MB")
    print(f"   Recommended partitions: {ideal_partitions}")
    print()


def example_5_performance_benchmark(spark):
    """
    Example 5: Performance Benchmarking
    
    📝 Demonstrates:
    • Comparing read/write performance
    • Impact of different configurations
    • Measuring query execution time
    """
    print("=" * 80)
    print("EXAMPLE 5: Performance Benchmarking")
    print("=" * 80)
    
    # Generate test dataset
    print("📊 Generating 1 million records for benchmark...")
    size = 1000000
    data = [(i, f"user_{i % 1000}", random.uniform(0, 1000), 
             random.choice(['A', 'B', 'C', 'D', 'E']),
             datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365)))
            for i in range(size)]
    
    df = spark.createDataFrame(data, ['id', 'user', 'amount', 'category', 'date'])
    
    configs = [
        ("snappy", "true", 4),
        ("gzip", "true", 4),
        ("snappy", "false", 4),
        ("snappy", "true", 8)
    ]
    
    print("\n📝 Running benchmarks...")
    print()
    
    results = []
    
    for compression, vectorized, partitions in configs:
        path = f"/tmp/pyspark_examples/parquet/bench_{compression}_{vectorized}_{partitions}"
        
        # Configure
        spark.conf.set("spark.sql.parquet.compression.codec", compression)
        spark.conf.set("spark.sql.parquet.enableVectorizedReader", vectorized)
        
        # Write benchmark
        start = time.time()
        df.coalesce(partitions).write.mode("overwrite").parquet(path)
        write_time = time.time() - start
        
        # Read benchmark
        start = time.time()
        count = spark.read.parquet(path).count()
        read_time = time.time() - start
        
        # Query benchmark (filter + aggregation)
        start = time.time()
        result = spark.read.parquet(path) \
            .filter(col('category') == 'A') \
            .groupBy('user') \
            .agg(sum('amount').alias('total')) \
            .count()
        query_time = time.time() - start
        
        # Get size
        import os
        size_mb = sum(os.path.getsize(os.path.join(root, f))
                     for root, _, files in os.walk(path)
                     for f in files if f.endswith('.parquet')) / (1024*1024)
        
        results.append({
            'compression': compression,
            'vectorized': vectorized,
            'partitions': partitions,
            'write_time': write_time,
            'read_time': read_time,
            'query_time': query_time,
            'size_mb': size_mb
        })
    
    print("📊 Benchmark Results:")
    print("-" * 100)
    print(f"{'Compression':<12} {'Vectorized':<12} {'Parts':<8} {'Write(s)':<12} {'Read(s)':<12} {'Query(s)':<12} {'Size(MB)'}")
    print("-" * 100)
    
    for r in results:
        print(f"{r['compression']:<12} {r['vectorized']:<12} {r['partitions']:<8} "
              f"{r['write_time']:<12.2f} {r['read_time']:<12.2f} {r['query_time']:<12.2f} {r['size_mb']:.2f}")
    
    print("-" * 100)
    print()
    
    print("💡 Performance Insights:")
    print("   • snappy: Best balance of speed and compression")
    print("   • gzip: Slower but better compression ratio")
    print("   • Vectorized reader: Significant read performance boost")
    print("   • More partitions: Better parallelism for large datasets")
    print()


def example_6_nested_data_structures(spark):
    """
    Example 6: Nested and Complex Data Types
    
    📝 Demonstrates:
    • Struct, Array, Map types
    • Efficient storage of nested data
    • Querying nested structures
    """
    print("=" * 80)
    print("EXAMPLE 6: Nested Data Structures")
    print("=" * 80)
    
    # Create nested data
    data = [
        {
            'id': 1,
            'user': {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'},
            'orders': [
                {'order_id': 'O1', 'amount': 100.0, 'items': ['laptop', 'mouse']},
                {'order_id': 'O2', 'amount': 50.0, 'items': ['keyboard']}
            ],
            'metadata': {'signup_date': '2024-01-01', 'premium': True}
        },
        {
            'id': 2,
            'user': {'name': 'Bob', 'age': 25, 'email': 'bob@example.com'},
            'orders': [
                {'order_id': 'O3', 'amount': 200.0, 'items': ['phone', 'case', 'charger']}
            ],
            'metadata': {'signup_date': '2024-02-15', 'premium': False}
        }
    ]
    
    df = spark.createDataFrame(data)
    
    print("📋 Schema with nested structures:")
    df.printSchema()
    
    path = "/tmp/pyspark_examples/parquet/nested_data"
    
    print("\n📝 Writing nested data to Parquet...")
    df.write.mode("overwrite").parquet(path)
    
    print("✅ Nested structures preserved in Parquet")
    print()
    
    print("📖 Reading and querying nested data...")
    df_read = spark.read.parquet(path)
    
    # Query nested fields
    df_read.select(
        col('id'),
        col('user.name').alias('user_name'),
        col('user.age').alias('user_age'),
        size(col('orders')).alias('order_count'),
        col('metadata.premium').alias('is_premium')
    ).show()
    
    print("💡 Nested Data Benefits:")
    print("   • Natural representation of complex objects")
    print("   • Preserves relationships without joins")
    print("   • Efficient storage (no duplication)")
    print("   • Column pruning works on nested fields")
    print()


def main():
    """Main execution function."""
    print("\n" + "🔥 " * 40)
    print("ADVANCED PARQUET TECHNIQUES")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    try:
        example_1_row_group_optimization(spark)
        example_2_dictionary_encoding(spark)
        example_3_statistics_and_metadata(spark)
        example_4_coalesce_optimization(spark)
        example_5_performance_benchmark(spark)
        example_6_nested_data_structures(spark)
        
        print("=" * 80)
        print("✅ ALL ADVANCED EXAMPLES COMPLETED")
        print("=" * 80)
        print()
        
        print("""
�� Advanced Parquet Optimization Summary:

1. Row Group Sizing:
   • Default: 128 MB
   • Tune based on query patterns and memory
   • Larger = better for scans, Smaller = better for selective queries

2. Dictionary Encoding:
   • Automatic for low-cardinality columns
   • Huge space savings (10x+ for categories)
   • Most effective for < 10,000 unique values

3. Statistics:
   • Automatically collected (min/max/null count)
   • Enable row group skipping
   • Critical for query performance

4. File Sizing:
   • Target: 128-512 MB per file
   • Use coalesce() to control file count
   • Balance: parallelism vs metadata overhead

5. Performance Tuning:
   • Enable vectorized reader
   • Use snappy compression (default)
   • Partition appropriately
   • Monitor and benchmark

6. Nested Data:
   • Efficiently stored in Parquet
   • Supports struct, array, map
   • Column pruning works on nested fields
        """)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
