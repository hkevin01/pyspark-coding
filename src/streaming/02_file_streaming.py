"""
02_file_streaming.py
====================

File-based Structured Streaming

This example demonstrates:
- Reading from file sources (CSV, JSON, Parquet)
- Schema specification
- File sink output
- Checkpoint management

Setup:
Create input directory: mkdir -p /tmp/streaming_input
Create checkpoint directory: mkdir -p /tmp/streaming_checkpoint
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp
import os


def create_spark_session():
    """Create SparkSession for file streaming."""
    return SparkSession.builder \
        .appName("FileStreaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()


def csv_file_streaming(spark):
    """
    Read CSV files as they arrive in a directory.
    
    Test:
    1. Run this script
    2. Copy CSV files to /tmp/streaming_input/
    3. See results in /tmp/streaming_output/
    """
    print("\n" + "=" * 80)
    print("CSV FILE STREAMING")
    print("=" * 80)
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", DoubleType(), True)
    ])
    
    # Read CSV stream
    df = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .load("/tmp/streaming_input/csv/")
    
    # Add processing timestamp
    df_with_ts = df.withColumn("processed_time", current_timestamp())
    
    # Write to parquet
    query = df_with_ts.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "/tmp/streaming_output/csv_to_parquet/") \
        .option("checkpointLocation", "/tmp/streaming_checkpoint/csv/") \
        .trigger(processingTime="5 seconds") \
        .start()
    
    print("✅ CSV streaming started!")
    print("   Input: /tmp/streaming_input/csv/")
    print("   Output: /tmp/streaming_output/csv_to_parquet/")
    print("   Checkpoint: /tmp/streaming_checkpoint/csv/")
    print("\n📝 To test:")
    print("   1. Create: /tmp/streaming_input/csv/")
    print("   2. Add CSV files with columns: id, name, age, salary")
    print("   3. Watch output directory for results\n")
    
    try:
        query.awaitTermination(timeout=60)
    except KeyboardInterrupt:
        query.stop()
    
    return query


def json_file_streaming(spark):
    """
    Read JSON files as they arrive.
    """
    print("\n" + "=" * 80)
    print("JSON FILE STREAMING")
    print("=" * 80)
    
    # Define schema
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    # Read JSON stream
    df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 2) \
        .load("/tmp/streaming_input/json/")
    
    # Filter and transform
    filtered_df = df.filter(col("value") > 100)
    
    # Write to console and file
    query1 = filtered_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .start()
    
    query2 = filtered_df.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", "/tmp/streaming_output/filtered_json/") \
        .option("checkpointLocation", "/tmp/streaming_checkpoint/json/") \
        .start()
    
    print("✅ JSON streaming started!")
    print("   Input: /tmp/streaming_input/json/")
    print("   Output: Console + /tmp/streaming_output/filtered_json/")
    
    try:
        query1.awaitTermination(timeout=60)
    except KeyboardInterrupt:
        query1.stop()
        query2.stop()
    
    return [query1, query2]


def parquet_file_streaming(spark):
    """
    Read Parquet files with automatic schema inference.
    """
    print("\n" + "=" * 80)
    print("PARQUET FILE STREAMING")
    print("=" * 80)
    
    # Read Parquet stream (schema inferred)
    df = spark.readStream \
        .format("parquet") \
        .option("maxFilesPerTrigger", 1) \
        .load("/tmp/streaming_input/parquet/")
    
    # Show schema
    print("\n📋 Inferred Schema:")
    df.printSchema()
    
    # Write to memory sink for testing
    query = df.writeStream \
        .format("memory") \
        .queryName("parquet_stream") \
        .outputMode("append") \
        .start()
    
    print("✅ Parquet streaming started!")
    print("   Query name: parquet_stream")
    print("   You can query with: spark.sql('SELECT * FROM parquet_stream')")
    
    return query


def multi_format_streaming(spark):
    """
    Read multiple formats and combine.
    """
    print("\n" + "=" * 80)
    print("MULTI-FORMAT STREAMING")
    print("=" * 80)
    
    # CSV schema
    csv_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    
    # Read CSV
    csv_df = spark.readStream \
        .format("csv") \
        .schema(csv_schema) \
        .option("header", "true") \
        .load("/tmp/streaming_input/multi/csv/") \
        .withColumn("source", col("name").cast("string"))
    
    # Read JSON
    json_df = spark.readStream \
        .format("json") \
        .schema(csv_schema) \
        .load("/tmp/streaming_input/multi/json/") \
        .withColumn("source", col("name").cast("string"))
    
    # Union both streams
    combined_df = csv_df.union(json_df)
    
    # Write combined output
    query = combined_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "/tmp/streaming_output/combined/") \
        .option("checkpointLocation", "/tmp/streaming_checkpoint/combined/") \
        .start()
    
    print("✅ Multi-format streaming started!")
    print("   Reading from CSV and JSON")
    print("   Output: /tmp/streaming_output/combined/")
    
    return query


def create_sample_data():
    """
    Create sample data files for testing.
    """
    import pandas as pd
    import json
    from datetime import datetime
    
    print("\n" + "=" * 80)
    print("CREATING SAMPLE DATA")
    print("=" * 80)
    
    # Create directories
    os.makedirs("/tmp/streaming_input/csv", exist_ok=True)
    os.makedirs("/tmp/streaming_input/json", exist_ok=True)
    os.makedirs("/tmp/streaming_input/parquet", exist_ok=True)
    
    # Sample CSV data
    csv_data = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "salary": [50000, 60000, 75000]
    })
    csv_data.to_csv("/tmp/streaming_input/csv/sample_1.csv", index=False)
    
    # Sample JSON data
    json_data = [
        {"user_id": 1, "action": "click", "timestamp": datetime.now().isoformat(), "value": 150.0},
        {"user_id": 2, "action": "purchase", "timestamp": datetime.now().isoformat(), "value": 200.0}
    ]
    with open("/tmp/streaming_input/json/sample_1.json", "w") as f:
        for record in json_data:
            f.write(json.dumps(record) + "\n")
    
    # Sample Parquet data
    parquet_data = pd.DataFrame({
        "id": [10, 20, 30],
        "product": ["Laptop", "Mouse", "Keyboard"],
        "price": [999.99, 29.99, 79.99]
    })
    parquet_data.to_parquet("/tmp/streaming_input/parquet/sample_1.parquet")
    
    print("✅ Sample data created!")
    print("   CSV: /tmp/streaming_input/csv/sample_1.csv")
    print("   JSON: /tmp/streaming_input/json/sample_1.json")
    print("   Parquet: /tmp/streaming_input/parquet/sample_1.parquet")


def main():
    """Main execution."""
    print("\n" + "📁" * 40)
    print("PYSPARK FILE STREAMING EXAMPLES")
    print("📁" * 40)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Create sample data
    create_sample_data()
    
    print("\n📝 Example Options:")
    print("   1. CSV file streaming")
    print("   2. JSON file streaming")
    print("   3. Parquet file streaming")
    print("   4. Multi-format streaming")
    print("\nRunning Example 1: CSV File Streaming\n")
    
    # Run CSV streaming example
    query = csv_file_streaming(spark)
    
    # Uncomment to run other examples:
    # json_file_streaming(spark)
    # parquet_file_streaming(spark)
    # multi_format_streaming(spark)
    
    print("\n✅ File streaming example completed!")
    print("\n💡 Tips:")
    print("   - Use checkpoints for fault tolerance")
    print("   - Set maxFilesPerTrigger to control processing rate")
    print("   - Define schema explicitly for better performance")
    print("   - Monitor checkpoint location for state recovery")
    
    spark.stop()


if __name__ == "__main__":
    main()
