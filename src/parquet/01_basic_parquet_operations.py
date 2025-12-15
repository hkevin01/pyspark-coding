#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PARQUET BASIC OPERATIONS - Reading and Writing Parquet Files
================================================================================

📖 OVERVIEW:
Apache Parquet is a columnar storage file format optimized for use with big data
processing frameworks. This script demonstrates basic read/write operations.

🎯 KEY FEATURES:
• Columnar storage (efficient for analytics)
• Compression (snappy, gzip, lzo)
• Schema evolution support
• Predicate pushdown
• Column pruning

🚀 RUN:
spark-submit 01_basic_parquet_operations.py

📦 BENEFITS:
• 10-100x smaller file sizes vs CSV
• 10-100x faster queries with column pruning
• Type safety with embedded schema
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random


def create_spark_session():
    """Create Spark session optimized for Parquet."""
    print("=" * 80)
    print("🚀 CREATING SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("Parquet Basic Operations") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.parquet.mergeSchema", "true") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    print(f"✅ Parquet compression: snappy")
    print(f"✅ Predicate pushdown: enabled")
    print(f"✅ Vectorized reader: enabled")
    print()
    
    return spark


def generate_sample_data(spark):
    """Generate sample dataset for demonstration."""
    print("=" * 80)
    print("📊 GENERATING SAMPLE DATA")
    print("=" * 80)
    
    # Generate 10,000 sample records
    num_records = 10000
    
    data = []
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
    countries = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'IN', 'BR', 'JP', 'CN']
    
    base_date = datetime(2024, 1, 1)
    
    for i in range(num_records):
        record = {
            'order_id': f'ORD{i+1:06d}',
            'customer_id': f'CUST{random.randint(1, 1000):04d}',
            'product_id': f'PROD{random.randint(1, 500):04d}',
            'category': random.choice(categories),
            'quantity': random.randint(1, 10),
            'unit_price': round(random.uniform(10.0, 500.0), 2),
            'country': random.choice(countries),
            'order_date': (base_date + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            'is_returned': random.random() < 0.05,
            'rating': random.randint(1, 5)
        }
        record['total_price'] = round(record['quantity'] * record['unit_price'], 2)
        data.append(record)
    
    df = spark.createDataFrame(data)
    
    print(f"✅ Generated {num_records:,} sample orders")
    print(f"✅ Columns: {', '.join(df.columns)}")
    print(f"✅ Date range: 2024-01-01 to 2024-12-31")
    print()
    
    return df


def example_1_write_parquet(spark, df):
    """
    Example 1: Write DataFrame to Parquet format
    
    📝 Demonstrates:
    • Basic write operation
    • Compression codec selection
    • Write modes (overwrite, append, error, ignore)
    """
    print("=" * 80)
    print("EXAMPLE 1: Write DataFrame to Parquet")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_examples/parquet/orders_basic"
    
    print("📝 Writing DataFrame to Parquet...")
    
    df.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    print(f"✅ Data written to: {output_path}")
    print(f"✅ Compression: snappy (default)")
    print(f"✅ Mode: overwrite")
    print()
    
    # Show file size comparison
    print("📊 File Statistics:")
    import os
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.endswith('.parquet'):
                file_path = os.path.join(root, file)
                size = os.path.getsize(file_path)
                total_size += size
                file_count += 1
    
    print(f"   Total size: {total_size / (1024*1024):.2f} MB")
    print(f"   Number of files: {file_count}")
    print(f"   Average file size: {total_size / (1024*1024*file_count):.2f} MB")
    print()


def example_2_read_parquet(spark):
    """
    Example 2: Read Parquet files
    
    📝 Demonstrates:
    • Basic read operation
    • Automatic schema inference
    • Reading from single file or directory
    """
    print("=" * 80)
    print("EXAMPLE 2: Read Parquet Files")
    print("=" * 80)
    
    input_path = "/tmp/pyspark_examples/parquet/orders_basic"
    
    print(f"📖 Reading Parquet from: {input_path}")
    
    df = spark.read.parquet(input_path)
    
    print("✅ Data loaded successfully")
    print()
    
    print("📋 Schema Information:")
    df.printSchema()
    
    print("📊 Sample Data (first 10 rows):")
    df.show(10, truncate=False)
    
    print(f"📈 Total records: {df.count():,}")
    print()


def example_3_column_pruning(spark):
    """
    Example 3: Column Pruning (Projection Pushdown)
    
    📝 Demonstrates:
    • Reading only specific columns
    • Significant performance improvement
    • Reduced memory usage
    """
    print("=" * 80)
    print("EXAMPLE 3: Column Pruning")
    print("=" * 80)
    
    input_path = "/tmp/pyspark_examples/parquet/orders_basic"
    
    print("🔍 Reading only 3 columns instead of all...")
    
    # Read only specific columns
    df = spark.read.parquet(input_path).select('order_id', 'category', 'total_price')
    
    print("✅ Column pruning applied")
    print()
    
    print("📋 Selected Columns:")
    df.printSchema()
    
    print("📊 Sample Data:")
    df.show(10)
    
    print("💡 Performance Benefit:")
    print("   • Only selected columns are read from disk")
    print("   • Reduces I/O by ~70% (3 of 11 columns)")
    print("   • Faster query execution")
    print("   • Lower memory usage")
    print()


def example_4_predicate_pushdown(spark):
    """
    Example 4: Predicate Pushdown (Filter Pushdown)
    
    📝 Demonstrates:
    • Applying filters at read time
    • Skip reading unnecessary row groups
    • Dramatic performance improvement
    """
    print("=" * 80)
    print("EXAMPLE 4: Predicate Pushdown")
    print("=" * 80)
    
    input_path = "/tmp/pyspark_examples/parquet/orders_basic"
    
    print("🔍 Applying filter: category = 'Electronics' AND total_price > 1000")
    
    # Predicate pushdown - filter is pushed to Parquet reader
    df = spark.read.parquet(input_path) \
        .filter((col('category') == 'Electronics') & (col('total_price') > 1000))
    
    print("✅ Predicate pushdown enabled")
    print()
    
    print("📊 Filtered Results:")
    df.show(20)
    
    print(f"📈 Matching records: {df.count():,}")
    print()
    
    print("💡 Performance Benefit:")
    print("   • Parquet reader skips row groups that don't match filter")
    print("   • Column statistics enable smart skipping")
    print("   • Can reduce data scanned by 90%+")
    print()


def example_5_compression_codecs(spark, df):
    """
    Example 5: Different Compression Codecs
    
    📝 Demonstrates:
    • snappy (fast, moderate compression)
    • gzip (slower, better compression)
    • uncompressed (fastest, largest size)
    """
    print("=" * 80)
    print("EXAMPLE 5: Compression Codecs Comparison")
    print("=" * 80)
    
    codecs = ['uncompressed', 'snappy', 'gzip']
    
    results = []
    
    for codec in codecs:
        output_path = f"/tmp/pyspark_examples/parquet/orders_{codec}"
        
        print(f"📝 Writing with {codec} compression...")
        
        import time
        start_time = time.time()
        
        df.write \
            .mode("overwrite") \
            .option("compression", codec) \
            .parquet(output_path)
        
        write_time = time.time() - start_time
        
        # Calculate size
        import os
        total_size = 0
        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.endswith('.parquet'):
                    total_size += os.path.getsize(os.path.join(root, file))
        
        results.append({
            'codec': codec,
            'size_mb': total_size / (1024*1024),
            'write_time': write_time
        })
        
        print(f"   ✓ Size: {total_size / (1024*1024):.2f} MB")
        print(f"   ✓ Write time: {write_time:.2f} seconds")
        print()
    
    print("📊 Compression Comparison:")
    print("-" * 60)
    print(f"{'Codec':<15} {'Size (MB)':<15} {'Write Time (s)':<15} {'Compression Ratio'}")
    print("-" * 60)
    
    base_size = results[0]['size_mb']
    for r in results:
        ratio = base_size / r['size_mb']
        print(f"{r['codec']:<15} {r['size_mb']:<15.2f} {r['write_time']:<15.2f} {ratio:.2f}x")
    
    print("-" * 60)
    print()
    
    print("💡 Recommendations:")
    print("   • snappy: Best for general use (fast + good compression)")
    print("   • gzip: Best for cold storage (slower but smallest)")
    print("   • uncompressed: Only for very fast I/O requirements")
    print()


def example_6_partitioned_writes(spark, df):
    """
    Example 6: Partitioned Parquet Files
    
    📝 Demonstrates:
    • Partitioning by column(s)
    • Partition pruning for faster queries
    • Organizing data for efficient access
    """
    print("=" * 80)
    print("EXAMPLE 6: Partitioned Writes")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_examples/parquet/orders_partitioned"
    
    print("📝 Writing partitioned by country and category...")
    
    df.write \
        .mode("overwrite") \
        .partitionBy("country", "category") \
        .parquet(output_path)
    
    print(f"✅ Data partitioned and written to: {output_path}")
    print()
    
    print("📂 Partition Structure:")
    import os
    partitions = []
    for root, dirs, files in os.walk(output_path):
        if any(f.endswith('.parquet') for f in files):
            rel_path = os.path.relpath(root, output_path)
            if rel_path != '.':
                partitions.append(rel_path)
    
    for i, partition in enumerate(sorted(partitions)[:10], 1):
        print(f"   {i}. {partition}")
    
    if len(partitions) > 10:
        print(f"   ... and {len(partitions) - 10} more partitions")
    
    print()
    
    print("💡 Partition Benefits:")
    print("   • Partition pruning: Only read relevant partitions")
    print("   • Data organization: Logical grouping")
    print("   • Query performance: Skip entire partitions")
    print()
    
    # Demonstrate partition pruning
    print("🔍 Reading data for US + Electronics only...")
    
    df_filtered = spark.read.parquet(output_path) \
        .filter((col('country') == 'US') & (col('category') == 'Electronics'))
    
    print(f"✅ Only scanned US/Electronics partition")
    print(f"📈 Records in this partition: {df_filtered.count():,}")
    print()


def example_7_schema_evolution(spark):
    """
    Example 7: Schema Evolution
    
    📝 Demonstrates:
    • Adding new columns
    • Merging schemas
    • Backward compatibility
    """
    print("=" * 80)
    print("EXAMPLE 7: Schema Evolution")
    print("=" * 80)
    
    # Original schema
    data_v1 = [
        {'id': 1, 'name': 'Product A', 'price': 100.0},
        {'id': 2, 'name': 'Product B', 'price': 200.0}
    ]
    
    df_v1 = spark.createDataFrame(data_v1)
    path = "/tmp/pyspark_examples/parquet/products"
    
    print("📝 Writing version 1 (3 columns)...")
    df_v1.write.mode("overwrite").parquet(path)
    df_v1.printSchema()
    
    # Evolved schema with new column
    data_v2 = [
        {'id': 3, 'name': 'Product C', 'price': 300.0, 'category': 'Electronics'},
        {'id': 4, 'name': 'Product D', 'price': 400.0, 'category': 'Books'}
    ]
    
    df_v2 = spark.createDataFrame(data_v2)
    
    print("📝 Appending version 2 (4 columns - added 'category')...")
    df_v2.write.mode("append").parquet(path)
    df_v2.printSchema()
    
    print("📖 Reading with schema merging...")
    
    df_merged = spark.read \
        .option("mergeSchema", "true") \
        .parquet(path)
    
    print("✅ Schemas merged successfully")
    print()
    
    print("📋 Merged Schema:")
    df_merged.printSchema()
    
    print("📊 All Data (note NULL values for old records):")
    df_merged.orderBy('id').show()
    
    print("💡 Schema Evolution Tips:")
    print("   • Set spark.sql.parquet.mergeSchema=true")
    print("   • Add columns safely (defaults to NULL for old data)")
    print("   • Cannot remove or rename columns directly")
    print("   • Plan schema changes carefully")
    print()


def main():
    """Main execution function."""
    print("\n" + "🔥 " * 40)
    print("PARQUET BASIC OPERATIONS - COMPREHENSIVE GUIDE")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    try:
        # Generate sample data
        df = generate_sample_data(spark)
        
        # Run examples
        example_1_write_parquet(spark, df)
        example_2_read_parquet(spark)
        example_3_column_pruning(spark)
        example_4_predicate_pushdown(spark)
        example_5_compression_codecs(spark, df)
        example_6_partitioned_writes(spark, df)
        example_7_schema_evolution(spark)
        
        print("=" * 80)
        print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print()
        
        print("""
📚 Summary - Parquet Best Practices:

1. ✅ Use column pruning (select only needed columns)
2. ✅ Enable predicate pushdown (filter early)
3. ✅ Choose appropriate compression (snappy recommended)
4. ✅ Partition large datasets logically
5. ✅ Use vectorized reader for better performance
6. ✅ Plan schema evolution carefully
7. ✅ Monitor file sizes (128-512 MB per file ideal)

🎯 When to Use Parquet:
   • Analytical queries (OLAP)
   • Column-heavy operations (aggregations, filtering)
   • Long-term storage
   • Data lake architectures
   • Machine learning datasets

�� When NOT to Use Parquet:
   • Need row-level updates
   • Primarily row-based access
   • Real-time streaming (use ORC or Avro instead)
   • Small datasets (<1 MB)
        """)
    
    finally:
        spark.stop()
        print("✅ Spark session stopped")


if __name__ == "__main__":
    main()
