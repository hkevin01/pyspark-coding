#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
ORC BASIC OPERATIONS - Reading and Writing ORC Files
================================================================================

📖 OVERVIEW:
Apache ORC (Optimized Row Columnar) is a columnar storage format optimized for
Hive and big data analytics. It provides excellent compression and performance.

🎯 KEY FEATURES:
• Columnar storage (like Parquet)
• Built-in indexes (row groups, bloom filters)
• ACID transaction support
• Superior compression
• Optimized for Hive ecosystem

🚀 RUN:
spark-submit 01_basic_orc_operations.py

📦 BENEFITS:
• 75% smaller than text formats
• 3-5x faster than Parquet for some workloads
• Best integration with Hive
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random


def create_spark_session():
    """Create Spark session optimized for ORC."""
    print("=" * 80)
    print("🚀 CREATING SPARK SESSION WITH ORC SUPPORT")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("ORC Basic Operations") \
        .config("spark.sql.orc.impl", "native") \
        .config("spark.sql.orc.enableVectorizedReader", "true") \
        .config("spark.sql.orc.filterPushdown", "true") \
        .config("spark.sql.orc.char.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created")
    print(f"✅ ORC implementation: native")
    print(f"✅ Vectorized reader: enabled")
    print(f"✅ Filter pushdown: enabled")
    print()
    
    return spark


def generate_sample_data(spark):
    """Generate sample dataset."""
    print("=" * 80)
    print("📊 GENERATING SAMPLE DATA")
    print("=" * 80)
    
    num_records = 10000
    
    data = []
    products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse']
    regions = ['North', 'South', 'East', 'West', 'Central']
    statuses = ['Completed', 'Pending', 'Cancelled', 'Shipped']
    
    for i in range(num_records):
        record = {
            'transaction_id': i + 1,
            'customer_id': random.randint(1000, 5000),
            'product': random.choice(products),
            'quantity': random.randint(1, 20),
            'price': round(random.uniform(50.0, 2000.0), 2),
            'region': random.choice(regions),
            'status': random.choice(statuses),
            'transaction_date': (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            'is_premium': random.random() > 0.7,
            'discount_rate': round(random.uniform(0.0, 0.3), 2)
        }
        record['total_amount'] = round(record['quantity'] * record['price'] * (1 - record['discount_rate']), 2)
        data.append(record)
    
    df = spark.createDataFrame(data)
    
    print(f"✅ Generated {num_records:,} transactions")
    print(f"✅ Columns: {', '.join(df.columns)}")
    print()
    
    return df


def example_1_write_orc(spark, df):
    """
    Example 1: Write DataFrame to ORC format
    
    📝 Demonstrates:
    • Basic write operation
    • Compression codecs
    • Write modes
    """
    print("=" * 80)
    print("EXAMPLE 1: Write DataFrame to ORC")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_examples/orc/transactions_basic"
    
    print("📝 Writing DataFrame to ORC...")
    
    df.write \
        .format("orc") \
        .mode("overwrite") \
        .save(output_path)
    
    print(f"✅ Data written to: {output_path}")
    print(f"✅ Format: ORC (columnar)")
    print(f"✅ Compression: zlib (default)")
    print()
    
    # Show file statistics
    import os
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.endswith('.orc') or file.startswith('part-'):
                file_path = os.path.join(root, file)
                size = os.path.getsize(file_path)
                total_size += size
                file_count += 1
    
    print(f"📊 File Statistics:")
    print(f"   Total size: {total_size / (1024*1024):.2f} MB")
    print(f"   Number of files: {file_count}")
    if file_count > 0:
        print(f"   Average file size: {total_size / (1024*1024*file_count):.2f} MB")
    print()


def example_2_read_orc(spark):
    """
    Example 2: Read ORC files
    
    📝 Demonstrates:
    • Basic read operation
    • Automatic schema inference
    • Built-in statistics
    """
    print("=" * 80)
    print("EXAMPLE 2: Read ORC Files")
    print("=" * 80)
    
    input_path = "/tmp/pyspark_examples/orc/transactions_basic"
    
    print(f"📖 Reading ORC from: {input_path}")
    
    df = spark.read \
        .format("orc") \
        .load(input_path)
    
    print("✅ Data loaded successfully")
    print()
    
    print("📋 Schema Information:")
    df.printSchema()
    
    print("📊 Sample Data (first 10 rows):")
    df.show(10)
    
    print(f"📈 Total records: {df.count():,}")
    print()


def example_3_compression_comparison(spark, df):
    """
    Example 3: Compression Codec Comparison
    
    📝 Demonstrates:
    • zlib (default, good compression)
    • snappy (fast)
    • lzo (fast, moderate compression)
    • none (uncompressed)
    """
    print("=" * 80)
    print("EXAMPLE 3: Compression Codecs")
    print("=" * 80)
    
    codecs = ['none', 'snappy', 'zlib']
    results = []
    
    import time
    
    for codec in codecs:
        output_path = f"/tmp/pyspark_examples/orc/transactions_{codec}"
        
        print(f"📝 Writing with {codec} compression...")
        
        start_time = time.time()
        
        df.write \
            .format("orc") \
            .mode("overwrite") \
            .option("compression", codec) \
            .save(output_path)
        
        write_time = time.time() - start_time
        
        # Calculate size
        import os
        total_size = 0
        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.startswith('part-'):
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
    print(f"{'Codec':<15} {'Size (MB)':<15} {'Write Time (s)':<15} {'Ratio'}")
    print("-" * 60)
    
    base_size = results[0]['size_mb']
    for r in results:
        ratio = base_size / r['size_mb']
        print(f"{r['codec']:<15} {r['size_mb']:<15.2f} {r['write_time']:<15.2f} {ratio:.2f}x")
    
    print("-" * 60)
    print()
    
    print("💡 Codec Recommendations:")
    print("   • zlib: Best compression (default, recommended)")
    print("   • snappy: Fast compression, good for hot data")
    print("   • lzo: Balance of speed and compression")
    print()


def example_4_predicate_pushdown(spark):
    """
    Example 4: Predicate Pushdown with ORC Statistics
    
    📝 Demonstrates:
    • Column statistics (min/max/count)
    • Predicate pushdown optimization
    • Stripe-level filtering
    """
    print("=" * 80)
    print("EXAMPLE 4: Predicate Pushdown")
    print("=" * 80)
    
    input_path = "/tmp/pyspark_examples/orc/transactions_basic"
    
    print("🔍 Applying filter: product = 'Laptop' AND total_amount > 1000")
    
    df = spark.read \
        .format("orc") \
        .load(input_path) \
        .filter((col('product') == 'Laptop') & (col('total_amount') > 1000))
    
    print("✅ Predicate pushdown enabled (ORC stripe-level filtering)")
    print()
    
    print("📊 Filtered Results:")
    df.show(20)
    
    print(f"📈 Matching records: {df.count():,}")
    print()
    
    print("💡 ORC Predicate Pushdown:")
    print("   • Min/max statistics for each stripe")
    print("   • Skip entire stripes that don't match")
    print("   • Bloom filters for precise filtering")
    print("   • Can skip 90%+ of data")
    print()


def example_5_partitioned_orc(spark, df):
    """
    Example 5: Partitioned ORC Files
    
    📝 Demonstrates:
    • Partitioning by columns
    • Partition pruning
    • Hive-style partitions
    """
    print("=" * 80)
    print("EXAMPLE 5: Partitioned ORC")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_examples/orc/transactions_partitioned"
    
    print("📝 Writing partitioned by region and status...")
    
    df.write \
        .format("orc") \
        .mode("overwrite") \
        .partitionBy("region", "status") \
        .save(output_path)
    
    print(f"✅ Data partitioned and written to: {output_path}")
    print()
    
    print("📂 Partition Structure (sample):")
    import os
    partitions = []
    for root, dirs, files in os.walk(output_path):
        if any(f.startswith('part-') for f in files):
            rel_path = os.path.relpath(root, output_path)
            if rel_path != '.':
                partitions.append(rel_path)
    
    for i, partition in enumerate(sorted(partitions)[:15], 1):
        print(f"   {i}. {partition}")
    
    if len(partitions) > 15:
        print(f"   ... and {len(partitions) - 15} more partitions")
    
    print()
    
    # Demonstrate partition pruning
    print("🔍 Reading only North region + Completed status...")
    
    df_filtered = spark.read \
        .format("orc") \
        .load(output_path) \
        .filter((col('region') == 'North') & (col('status') == 'Completed'))
    
    print(f"✅ Only scanned North/Completed partition")
    print(f"📈 Records: {df_filtered.count():,}")
    print()


def example_6_orc_with_indexes(spark):
    """
    Example 6: ORC Built-in Indexes
    
    📝 Demonstrates:
    • Row group indexes
    • Bloom filters
    • Column statistics
    """
    print("=" * 80)
    print("EXAMPLE 6: ORC Built-in Indexes")
    print("=" * 80)
    
    # Create dataset with patterns
    data = []
    for i in range(100000):
        data.append({
            'id': i,
            'category': f"cat_{i % 100}",
            'value': random.uniform(0, 10000),
            'status': random.choice(['A', 'B', 'C', 'D'])
        })
    
    df = spark.createDataFrame(data)
    
    path = "/tmp/pyspark_examples/orc/with_indexes"
    
    print("📝 Writing ORC with automatic indexes...")
    
    df.write \
        .format("orc") \
        .mode("overwrite") \
        .option("orc.bloom.filter.columns", "category,status") \
        .option("orc.bloom.filter.fpp", "0.05") \
        .save(path)
    
    print("✅ ORC written with bloom filters on 'category' and 'status'")
    print()
    
    print("🔍 Testing bloom filter efficiency...")
    print("   Query: category = 'cat_50'")
    
    import time
    start = time.time()
    
    result = spark.read \
        .format("orc") \
        .load(path) \
        .filter(col('category') == 'cat_50') \
        .count()
    
    query_time = time.time() - start
    
    print(f"   ✓ Found {result:,} records in {query_time:.2f}s")
    print()
    
    print("💡 ORC Indexes:")
    print("   • Row group indexes (automatic)")
    print("   • Bloom filters (configurable)")
    print("   • Column statistics (automatic)")
    print("   • Stripe-level metadata")
    print()


def example_7_acid_transactions(spark):
    """
    Example 7: ACID Transaction Support
    
    📝 Demonstrates:
    • ORC's ACID capabilities (when used with Hive)
    • Append mode
    • Overwrite mode
    """
    print("=" * 80)
    print("EXAMPLE 7: ACID Transaction Support")
    print("=" * 80)
    
    path = "/tmp/pyspark_examples/orc/acid_demo"
    
    # Initial data
    data_v1 = [
        {'id': 1, 'name': 'Alice', 'balance': 1000},
        {'id': 2, 'name': 'Bob', 'balance': 1500}
    ]
    
    df_v1 = spark.createDataFrame(data_v1)
    
    print("📝 Writing initial data...")
    df_v1.write.format("orc").mode("overwrite").save(path)
    df_v1.show()
    
    # Append more data
    data_v2 = [
        {'id': 3, 'name': 'Charlie', 'balance': 2000},
        {'id': 4, 'name': 'Diana', 'balance': 2500}
    ]
    
    df_v2 = spark.createDataFrame(data_v2)
    
    print("📝 Appending new data...")
    df_v2.write.format("orc").mode("append").save(path)
    
    # Read all
    df_all = spark.read.format("orc").load(path)
    
    print("✅ All data after append:")
    df_all.orderBy('id').show()
    
    print(f"📈 Total records: {df_all.count()}")
    print()
    
    print("💡 ACID in ORC:")
    print("   • Supports Atomicity, Consistency, Isolation, Durability")
    print("   • Best with Hive metastore")
    print("   • Enables INSERT, UPDATE, DELETE in Hive")
    print("   • ORC is only format supporting ACID in Hive")
    print()


def example_8_complex_types(spark):
    """
    Example 8: Complex and Nested Data Types
    
    📝 Demonstrates:
    • Struct, Array, Map types
    • Nested structures
    """
    print("=" * 80)
    print("EXAMPLE 8: Complex Data Types")
    print("=" * 80)
    
    data = [
        {
            'customer_id': 1,
            'profile': {'name': 'Alice', 'age': 30, 'city': 'NYC'},
            'purchases': [
                {'item': 'Laptop', 'price': 1200},
                {'item': 'Mouse', 'price': 25}
            ],
            'preferences': {'theme': 'dark', 'language': 'en'}
        },
        {
            'customer_id': 2,
            'profile': {'name': 'Bob', 'age': 25, 'city': 'LA'},
            'purchases': [
                {'item': 'Phone', 'price': 800}
            ],
            'preferences': {'theme': 'light', 'language': 'es'}
        }
    ]
    
    df = spark.createDataFrame(data)
    
    print("📋 Schema with complex types:")
    df.printSchema()
    
    path = "/tmp/pyspark_examples/orc/complex_types"
    
    print("\n📝 Writing complex nested data to ORC...")
    df.write.format("orc").mode("overwrite").save(path)
    
    print("✅ Complex types preserved")
    print()
    
    print("📖 Reading and querying nested data...")
    df_read = spark.read.format("orc").load(path)
    
    df_read.select(
        col('customer_id'),
        col('profile.name').alias('name'),
        col('profile.city').alias('city'),
        size(col('purchases')).alias('purchase_count'),
        col('preferences.theme').alias('theme')
    ).show()
    
    print("💡 Complex Types in ORC:")
    print("   • Full support for nested structures")
    print("   • Efficient columnar storage even for nested data")
    print("   • Good performance with complex schemas")
    print()


def main():
    """Main execution function."""
    print("\n" + "🔥 " * 40)
    print("ORC BASIC OPERATIONS - COMPREHENSIVE GUIDE")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    try:
        # Generate sample data
        df = generate_sample_data(spark)
        
        # Run examples
        example_1_write_orc(spark, df)
        example_2_read_orc(spark)
        example_3_compression_comparison(spark, df)
        example_4_predicate_pushdown(spark)
        example_5_partitioned_orc(spark, df)
        example_6_orc_with_indexes(spark)
        example_7_acid_transactions(spark)
        example_8_complex_types(spark)
        
        print("=" * 80)
        print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print()
        
        print("""
📚 Summary - ORC Best Practices:

1. ✅ Use zlib compression (default, best ratio)
2. ✅ Enable vectorized reader
3. ✅ Use bloom filters for high-cardinality columns
4. ✅ Partition large datasets
5. ✅ Leverage built-in statistics
6. ✅ Best for Hive ecosystem
7. ✅ ACID support when needed

🎯 When to Use ORC:
   • Hive data warehouse
   • ACID transactions required
   • Heavy read workloads
   • Analytical queries (OLAP)
   • When you need bloom filters
   • Best compression needed

🚫 When NOT to Use ORC:
   • Streaming workloads (use Avro)
   • Non-Hive environments (use Parquet)
   • Small datasets
   • Frequent schema changes
        """)
    
    finally:
        spark.stop()
        print("✅ Spark session stopped")


if __name__ == "__main__":
    main()
