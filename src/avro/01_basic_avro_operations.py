#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
AVRO BASIC OPERATIONS - Reading and Writing Avro Files
================================================================================

📖 OVERVIEW:
Apache Avro is a row-based data serialization system with rich data structures
and a compact binary format. Ideal for streaming and schema evolution.

🎯 KEY FEATURES:
• Row-based storage (efficient for streaming)
• Compact binary serialization
• Schema stored with data
• Rich data structures (records, enums, arrays, maps, unions)
• Strong schema evolution support

�� RUN:
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0 \
             01_basic_avro_operations.py

📦 REQUIRES:
spark-avro package
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro, to_avro
from datetime import datetime, timedelta
import random
import json


def create_spark_session():
    """Create Spark session with Avro support."""
    print("=" * 80)
    print("🚀 CREATING SPARK SESSION WITH AVRO SUPPORT")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("Avro Basic Operations") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("✅ Spark session created with Avro support")
    print()
    
    return spark


def generate_sample_data(spark):
    """Generate sample dataset."""
    print("=" * 80)
    print("📊 GENERATING SAMPLE DATA")
    print("=" * 80)
    
    num_records = 5000
    
    data = []
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance']
    cities = ['New York', 'London', 'Tokyo', 'Paris', 'Sydney']
    
    for i in range(num_records):
        record = {
            'employee_id': i + 1,
            'name': f'Employee_{i+1}',
            'email': f'emp{i+1}@company.com',
            'department': random.choice(departments),
            'salary': random.randint(30000, 150000),
            'hire_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime('%Y-%m-%d'),
            'city': random.choice(cities),
            'is_active': random.random() > 0.1,
            'performance_score': round(random.uniform(2.0, 5.0), 2)
        }
        data.append(record)
    
    df = spark.createDataFrame(data)
    
    print(f"✅ Generated {num_records:,} employee records")
    print(f"✅ Columns: {', '.join(df.columns)}")
    print()
    
    return df


def example_1_write_avro(spark, df):
    """
    Example 1: Write DataFrame to Avro format
    
    📝 Demonstrates:
    • Basic write operation
    • Compression codecs (snappy, deflate, uncompressed)
    • Write modes
    """
    print("=" * 80)
    print("EXAMPLE 1: Write DataFrame to Avro")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_examples/avro/employees_basic"
    
    print("📝 Writing DataFrame to Avro...")
    
    df.write \
        .format("avro") \
        .mode("overwrite") \
        .save(output_path)
    
    print(f"✅ Data written to: {output_path}")
    print(f"✅ Format: Avro (binary)")
    print(f"✅ Compression: snappy (default)")
    print()
    
    # Show file statistics
    import os
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(output_path):
        for file in files:
            if file.endswith('.avro'):
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


def example_2_read_avro(spark):
    """
    Example 2: Read Avro files
    
    📝 Demonstrates:
    • Basic read operation
    • Automatic schema inference from Avro metadata
    • Schema is embedded in the file
    """
    print("=" * 80)
    print("EXAMPLE 2: Read Avro Files")
    print("=" * 80)
    
    input_path = "/tmp/pyspark_examples/avro/employees_basic"
    
    print(f"📖 Reading Avro from: {input_path}")
    
    df = spark.read \
        .format("avro") \
        .load(input_path)
    
    print("✅ Data loaded successfully")
    print()
    
    print("📋 Schema (from Avro metadata):")
    df.printSchema()
    
    print("📊 Sample Data (first 10 rows):")
    df.show(10)
    
    print(f"📈 Total records: {df.count():,}")
    print()
    
    print("💡 Avro Advantages:")
    print("   • Schema is self-describing (embedded in file)")
    print("   • No need for external schema definition")
    print("   • Schema evolution supported")
    print()


def example_3_compression_codecs(spark, df):
    """
    Example 3: Different Compression Codecs
    
    📝 Demonstrates:
    • snappy (fast)
    • deflate (better compression)
    • uncompressed
    """
    print("=" * 80)
    print("EXAMPLE 3: Compression Codecs")
    print("=" * 80)
    
    codecs = ['uncompressed', 'snappy', 'deflate']
    results = []
    
    import time
    
    for codec in codecs:
        output_path = f"/tmp/pyspark_examples/avro/employees_{codec}"
        
        print(f"📝 Writing with {codec} compression...")
        
        start_time = time.time()
        
        df.write \
            .format("avro") \
            .mode("overwrite") \
            .option("compression", codec) \
            .save(output_path)
        
        write_time = time.time() - start_time
        
        # Calculate size
        import os
        total_size = 0
        for root, dirs, files in os.walk(output_path):
            for file in files:
                if file.endswith('.avro'):
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


def example_4_schema_evolution(spark):
    """
    Example 4: Schema Evolution
    
    📝 Demonstrates:
    • Adding new fields
    • Reading old and new data together
    • Default values for missing fields
    """
    print("=" * 80)
    print("EXAMPLE 4: Schema Evolution")
    print("=" * 80)
    
    path = "/tmp/pyspark_examples/avro/schema_evolution"
    
    # Version 1: Original schema
    data_v1 = [
        {'id': 1, 'name': 'Alice', 'age': 30},
        {'id': 2, 'name': 'Bob', 'age': 25}
    ]
    
    df_v1 = spark.createDataFrame(data_v1)
    
    print("📝 Writing version 1 (3 columns)...")
    df_v1.write.format("avro").mode("overwrite").save(path)
    df_v1.printSchema()
    
    # Version 2: Evolved schema (added columns)
    data_v2 = [
        {'id': 3, 'name': 'Charlie', 'age': 35, 'city': 'NYC', 'salary': 100000},
        {'id': 4, 'name': 'Diana', 'age': 28, 'city': 'London', 'salary': 90000}
    ]
    
    df_v2 = spark.createDataFrame(data_v2)
    
    print("📝 Appending version 2 (5 columns - added city, salary)...")
    df_v2.write.format("avro").mode("append").save(path)
    df_v2.printSchema()
    
    # Read all data
    print("📖 Reading all data (mixed schemas)...")
    
    df_read = spark.read.format("avro").load(path)
    
    print("✅ Data read successfully with evolved schema")
    print()
    
    print("📋 Result Schema:")
    df_read.printSchema()
    
    print("📊 All Data (NULL for missing fields):")
    df_read.orderBy('id').show()
    
    print("💡 Schema Evolution in Avro:")
    print("   • Forward compatible (new readers, old data)")
    print("   • Backward compatible (old readers, new data)")
    print("   • NULL for missing fields")
    print("   • Best practice: Add fields with defaults")
    print()


def example_5_partitioned_avro(spark, df):
    """
    Example 5: Partitioned Avro Files
    
    📝 Demonstrates:
    • Partitioning by column
    • Partition pruning
    • Directory structure
    """
    print("=" * 80)
    print("EXAMPLE 5: Partitioned Avro")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_examples/avro/employees_partitioned"
    
    print("📝 Writing partitioned by department and city...")
    
    df.write \
        .format("avro") \
        .mode("overwrite") \
        .partitionBy("department", "city") \
        .save(output_path)
    
    print(f"✅ Data partitioned and written to: {output_path}")
    print()
    
    print("📂 Partition Structure (sample):")
    import os
    partitions = []
    for root, dirs, files in os.walk(output_path):
        if any(f.endswith('.avro') for f in files):
            rel_path = os.path.relpath(root, output_path)
            if rel_path != '.':
                partitions.append(rel_path)
    
    for i, partition in enumerate(sorted(partitions)[:15], 1):
        print(f"   {i}. {partition}")
    
    if len(partitions) > 15:
        print(f"   ... and {len(partitions) - 15} more partitions")
    
    print()
    
    # Demonstrate partition pruning
    print("🔍 Reading only Engineering department in New York...")
    
    df_filtered = spark.read \
        .format("avro") \
        .load(output_path) \
        .filter((col('department') == 'Engineering') & (col('city') == 'New York'))
    
    print(f"✅ Only scanned Engineering/New York partition")
    print(f"📈 Records: {df_filtered.count():,}")
    print()


def example_6_complex_data_types(spark):
    """
    Example 6: Complex and Nested Data Types
    
    📝 Demonstrates:
    • Struct (nested records)
    • Array
    • Map
    • Combinations
    """
    print("=" * 80)
    print("EXAMPLE 6: Complex Data Types")
    print("=" * 80)
    
    # Create complex nested data
    data = [
        {
            'user_id': 1,
            'profile': {
                'name': 'Alice',
                'age': 30,
                'email': 'alice@example.com'
            },
            'skills': ['Python', 'Spark', 'SQL'],
            'projects': [
                {'name': 'Project A', 'hours': 120},
                {'name': 'Project B', 'hours': 80}
            ],
            'metadata': {'last_login': '2024-12-01', 'account_type': 'premium'}
        },
        {
            'user_id': 2,
            'profile': {
                'name': 'Bob',
                'age': 25,
                'email': 'bob@example.com'
            },
            'skills': ['Java', 'Kafka', 'MongoDB'],
            'projects': [
                {'name': 'Project C', 'hours': 200}
            ],
            'metadata': {'last_login': '2024-12-10', 'account_type': 'standard'}
        }
    ]
    
    df = spark.createDataFrame(data)
    
    print("📋 Schema with complex types:")
    df.printSchema()
    
    path = "/tmp/pyspark_examples/avro/complex_types"
    
    print("\n📝 Writing complex nested data to Avro...")
    df.write.format("avro").mode("overwrite").save(path)
    
    print("✅ Complex types preserved in Avro")
    print()
    
    print("📖 Reading complex data...")
    df_read = spark.read.format("avro").load(path)
    
    # Query nested fields
    df_read.select(
        col('user_id'),
        col('profile.name').alias('name'),
        col('profile.age').alias('age'),
        size(col('skills')).alias('skill_count'),
        col('skills')[0].alias('primary_skill'),
        col('metadata.account_type').alias('account_type')
    ).show(truncate=False)
    
    print("💡 Complex Types in Avro:")
    print("   • Full support for nested structures")
    print("   • Preserves schema in binary format")
    print("   • Efficient serialization")
    print("   • Ideal for event streams with rich data")
    print()


def example_7_streaming_use_case(spark):
    """
    Example 7: Avro for Streaming (Kafka Integration)
    
    📝 Demonstrates:
    • Why Avro is popular for streaming
    • to_avro and from_avro functions
    • Schema registry concept
    """
    print("=" * 80)
    print("EXAMPLE 7: Avro for Streaming")
    print("=" * 80)
    
    # Create sample streaming data
    data = [
        {'event_id': 1, 'user_id': 'user_1', 'event_type': 'click', 'timestamp': '2024-12-15T10:00:00Z'},
        {'event_id': 2, 'user_id': 'user_2', 'event_type': 'view', 'timestamp': '2024-12-15T10:01:00Z'},
        {'event_id': 3, 'user_id': 'user_3', 'event_type': 'purchase', 'timestamp': '2024-12-15T10:02:00Z'}
    ]
    
    df = spark.createDataFrame(data)
    
    print("📋 Original DataFrame:")
    df.show()
    
    # Define Avro schema (would typically come from schema registry)
    avro_schema = json.dumps({
        "type": "record",
        "name": "Event",
        "fields": [
            {"name": "event_id", "type": "int"},
            {"name": "user_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "timestamp", "type": "string"}
        ]
    })
    
    print("\n📝 Converting to Avro binary (like for Kafka)...")
    
    # Convert to Avro bytes (would send to Kafka)
    df_avro = df.select(
        to_avro(struct([col(c) for c in df.columns])).alias("value")
    )
    
    print("✅ Converted to Avro binary format")
    df_avro.printSchema()
    
    print("\n📖 Converting from Avro binary (like from Kafka)...")
    
    # Convert from Avro bytes (would receive from Kafka)
    df_decoded = df_avro.select(
        from_avro(col("value"), avro_schema).alias("data")
    ).select("data.*")
    
    print("✅ Decoded from Avro binary")
    df_decoded.show()
    
    print("💡 Avro for Streaming:")
    print("   • Compact binary format (efficient network transfer)")
    print("   • Self-describing schema (no schema drift)")
    print("   • Fast serialization/deserialization")
    print("   • Schema registry for version management")
    print("   • Popular with Kafka and event-driven systems")
    print()


def main():
    """Main execution function."""
    print("\n" + "🔥 " * 40)
    print("AVRO BASIC OPERATIONS - COMPREHENSIVE GUIDE")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    try:
        # Generate sample data
        df = generate_sample_data(spark)
        
        # Run examples
        example_1_write_avro(spark, df)
        example_2_read_avro(spark)
        example_3_compression_codecs(spark, df)
        example_4_schema_evolution(spark)
        example_5_partitioned_avro(spark, df)
        example_6_complex_data_types(spark)
        example_7_streaming_use_case(spark)
        
        print("=" * 80)
        print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print()
        
        print("""
📚 Summary - Avro Best Practices:

1. ✅ Use Avro for row-based access patterns
2. ✅ Ideal for streaming and event systems
3. ✅ Schema evolution friendly
4. ✅ Compact binary format
5. ✅ Use with Schema Registry (Confluent/AWS Glue)
6. ✅ Good compression with snappy
7. ✅ Self-describing (schema embedded)

🎯 When to Use Avro:
   • Streaming data (Kafka, Kinesis)
   • Event-driven architectures
   • Schema evolution required
   • Row-based access patterns
   • Message queues
   • Microservices communication

🚫 When NOT to Use Avro:
   • Analytical queries (use Parquet instead)
   • Column-heavy operations
   • Data warehouse storage
   • When you need predicate pushdown
        """)
    
    finally:
        spark.stop()
        print("✅ Spark session stopped")


if __name__ == "__main__":
    main()
