"""
================================================================================
BEGINNER 02: Reading and Writing Files
================================================================================

PURPOSE:
--------
Learn how to read data from files (CSV, JSON, Parquet) and write results back
to disk - essential skills for real-world data pipelines.

WHAT YOU'LL LEARN:
------------------
- Reading CSV files with headers
- Reading JSON files
- Reading Parquet files (columnar format)
- Writing DataFrames to different formats
- Handling file options (delimiter, schema, etc.)

WHY THIS MATTERS:
-----------------
99% of real PySpark jobs involve reading from files or databases. Master file I/O
to work with real datasets instead of just toy examples.

FILE FORMATS COMPARISON:
------------------------
CSV:    ✅ Human-readable  ❌ Slow, no schema
JSON:   ✅ Flexible schema ❌ Slow, verbose
Parquet: ✅ Fast, compressed, schema ❌ Not human-readable (binary)

REAL-WORLD USE CASE:
--------------------
**Data Pipeline**: Read raw CSV data from sales system, clean it, and save
as Parquet for fast analytics queries.

TIME TO COMPLETE: 20 minutes
DIFFICULTY: ⭐⭐☆☆☆ (2/5)

================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def create_spark_session():
    """Create SparkSession with CSV/JSON support."""
    return (
        SparkSession.builder
        .appName("Beginner02_Files")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def setup_sample_data(spark):
    """
    Create sample data files for demonstration.
    
    WHAT: Generate CSV and JSON files with sample data
    WHY: Learn with realistic data structures
    HOW: Use PySpark to write sample files
    """
    print("\n" + "=" * 80)
    print("SETUP: Creating Sample Data Files")
    print("=" * 80)
    
    # Create data directory
    data_dir = "/tmp/pyspark_beginner"
    os.makedirs(data_dir, exist_ok=True)
    
    # Sample product data
    products = [
        (1, "Laptop", "Electronics", 999.99, 50),
        (2, "Mouse", "Electronics", 29.99, 200),
        (3, "Desk", "Furniture", 299.99, 30),
        (4, "Chair", "Furniture", 199.99, 45),
        (5, "Keyboard", "Electronics", 79.99, 150),
    ]
    
    df = spark.createDataFrame(
        products,
        ["product_id", "product_name", "category", "price", "stock"]
    )
    
    # Write as CSV
    csv_path = f"{data_dir}/products.csv"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    
    # Write as JSON
    json_path = f"{data_dir}/products.json"
    df.coalesce(1).write.mode("overwrite").json(json_path)
    
    # Write as Parquet
    parquet_path = f"{data_dir}/products.parquet"
    df.write.mode("overwrite").parquet(parquet_path)
    
    print(f"\n✅ Sample files created in: {data_dir}")
    print(f"   - CSV:     {csv_path}")
    print(f"   - JSON:    {json_path}")
    print(f"   - Parquet: {parquet_path}")
    
    return data_dir


def example_1_read_csv(spark, data_dir):
    """
    Example 1: Read CSV files.
    
    WHAT: Load comma-separated values files into DataFrame
    WHY: CSV is the most common data exchange format
    HOW: Use spark.read.csv() with options
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: READ CSV FILES")
    print("=" * 80)
    
    csv_path = f"{data_dir}/products.csv"
    
    # Read CSV with automatic schema inference
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
    
    print("\n✅ CSV file loaded!")
    print(f"   Path: {csv_path}")
    print(f"   Rows: {df.count()}")
    
    print("\n📊 Data:")
    df.show()
    
    print("\n📋 Inferred Schema:")
    df.printSchema()
    
    return df


def example_2_read_with_schema(spark, data_dir):
    """
    Example 2: Read CSV with explicit schema.
    
    WHAT: Define exact data types instead of inferring
    WHY: Faster loading, prevents type inference errors
    HOW: Create StructType schema and pass to read
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: READ CSV WITH EXPLICIT SCHEMA")
    print("=" * 80)
    
    # Define schema explicitly
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("stock", IntegerType(), False),
    ])
    
    csv_path = f"{data_dir}/products.csv"
    df = spark.read.option("header", "true").schema(schema).csv(csv_path)
    
    print("\n✅ CSV loaded with explicit schema!")
    print("\n📋 Schema (notice exact types):")
    df.printSchema()
    
    print("\n💡 TIP: Explicit schemas are 2-5x faster than inferSchema!")
    
    return df


def example_3_read_json(spark, data_dir):
    """
    Example 3: Read JSON files.
    
    WHAT: Load JSON (JavaScript Object Notation) files
    WHY: JSON is common for APIs and nested data
    HOW: Use spark.read.json() - schema inferred automatically
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: READ JSON FILES")
    print("=" * 80)
    
    json_path = f"{data_dir}/products.json"
    df = spark.read.json(json_path)
    
    print("\n✅ JSON file loaded!")
    print(f"   Path: {json_path}")
    
    print("\n📊 Data:")
    df.show()
    
    print("\n💡 JSON Advantages:")
    print("   ✅ Automatic schema inference")
    print("   ✅ Supports nested structures")
    print("   ✅ Self-describing format")
    
    return df


def example_4_read_parquet(spark, data_dir):
    """
    Example 4: Read Parquet files (recommended format).
    
    WHAT: Load columnar Parquet files
    WHY: Parquet is 10-100x faster than CSV, includes schema
    HOW: Use spark.read.parquet()
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: READ PARQUET FILES (BEST PRACTICE)")
    print("=" * 80)
    
    parquet_path = f"{data_dir}/products.parquet"
    df = spark.read.parquet(parquet_path)
    
    print("\n✅ Parquet file loaded!")
    print(f"   Path: {parquet_path}")
    
    print("\n�� Data:")
    df.show()
    
    print("\n🚀 Parquet Performance:")
    print("   ✅ 10-100x faster than CSV")
    print("   ✅ Built-in compression (smaller files)")
    print("   ✅ Schema included (no inference needed)")
    print("   ✅ Columnar format (read only needed columns)")
    
    print("\n💡 RECOMMENDATION: Always use Parquet for production!")
    
    return df


def example_5_write_files(spark, data_dir):
    """
    Example 5: Write DataFrames to files.
    
    WHAT: Save processed data to disk
    WHY: Persist results for later use or sharing
    HOW: Use df.write with mode and format
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: WRITE FILES")
    print("=" * 80)
    
    # Create sample data with transformations
    df = spark.read.parquet(f"{data_dir}/products.parquet")
    
    # Add calculated columns
    df_enriched = df.withColumn(
        "total_value", col("price") * col("stock")
    ).withColumn(
        "stock_status",
        when(col("stock") < 50, "Low Stock")
        .when(col("stock") < 100, "Medium Stock")
        .otherwise("High Stock")
    )
    
    print("\n📊 Enriched data (with calculated columns):")
    df_enriched.show()
    
    # Write to different formats
    output_dir = f"{data_dir}/output"
    
    # 1. Write as Parquet (recommended)
    parquet_out = f"{output_dir}/enriched.parquet"
    df_enriched.write.mode("overwrite").parquet(parquet_out)
    print(f"\n✅ Saved as Parquet: {parquet_out}")
    
    # 2. Write as CSV with header
    csv_out = f"{output_dir}/enriched.csv"
    df_enriched.write.mode("overwrite").option("header", "true").csv(csv_out)
    print(f"✅ Saved as CSV: {csv_out}")
    
    # 3. Write as JSON
    json_out = f"{output_dir}/enriched.json"
    df_enriched.write.mode("overwrite").json(json_out)
    print(f"✅ Saved as JSON: {json_out}")
    
    # 4. Write with partitioning (for large datasets)
    partitioned_out = f"{output_dir}/partitioned"
    df_enriched.write.mode("overwrite").partitionBy("category").parquet(partitioned_out)
    print(f"✅ Saved partitioned by category: {partitioned_out}")
    
    print("\n💡 Write Modes:")
    print("   - overwrite: Replace existing data")
    print("   - append:    Add to existing data")
    print("   - ignore:    Skip if exists")
    print("   - error:     Fail if exists (default)")
    
    return df_enriched


def main():
    """Main execution."""
    print("\n" + "📁" * 40)
    print("BEGINNER LESSON 2: READING AND WRITING FILES")
    print("📁" * 40)
    
    spark = create_spark_session()
    
    try:
        # Setup sample data
        data_dir = setup_sample_data(spark)
        
        # Run examples
        example_1_read_csv(spark, data_dir)
        example_2_read_with_schema(spark, data_dir)
        example_3_read_json(spark, data_dir)
        example_4_read_parquet(spark, data_dir)
        example_5_write_files(spark, data_dir)
        
        print("\n" + "=" * 80)
        print("🎉 CONGRATULATIONS! LESSON 2 COMPLETE!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Read CSV files (with and without schema)")
        print("   2. Read JSON files")
        print("   3. Read Parquet files (BEST PRACTICE)")
        print("   4. Write DataFrames to multiple formats")
        print("   5. Use write modes and partitioning")
        
        print("\n🏆 KEY TAKEAWAY:")
        print("   Always use Parquet for production - it's 10-100x faster!")
        
        print("\n➡️  NEXT: Try beginner/03_data_cleaning.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
