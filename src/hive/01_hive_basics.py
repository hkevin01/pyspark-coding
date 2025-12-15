#!/usr/bin/env python3
"""
================================================================================
HIVE 01: Hive Basics with PySpark
================================================================================

PURPOSE:
--------
Learn how to use Hive with PySpark for SQL-based data processing and 
table management.

WHAT YOU'LL LEARN:
------------------
- Enable Hive support in SparkSession
- Create Hive databases and tables
- Run HiveQL queries
- Understand managed vs external tables
- Work with Hive partitions
- Use Hive UDFs

WHY USE HIVE WITH SPARK:
------------------------
- SQL interface for big data processing
- Metadata management (table schemas, partitions)
- Integration with existing Hive data warehouses
- Support for complex queries and aggregations

PREREQUISITES:
--------------
- PySpark installed
- Basic SQL knowledge
- Understanding of DataFrames

REAL-WORLD USE CASE:
--------------------
**Data Warehouse**: Build a data warehouse with:
- Multiple databases for different business units
- Partitioned tables for efficient querying
- External tables pointing to existing data
- Managed tables for processed data

TIME TO COMPLETE: 25 minutes
DIFFICULTY: ⭐⭐⭐☆☆ (3/5)

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import os


def create_spark_session_with_hive():
    """
    Create SparkSession with Hive support enabled.
    
    WHAT: Enable Hive integration in PySpark
    WHY: Access Hive metastore and use HiveQL
    HOW: Use enableHiveSupport() method
    """
    print("\n" + "=" * 80)
    print("CREATING SPARK SESSION WITH HIVE SUPPORT")
    print("=" * 80)
    
    # Get the hive-site.xml path
    hive_conf_dir = os.path.dirname(os.path.abspath(__file__))
    
    spark = SparkSession.builder \
        .appName("HiveBasics") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.metastore.warehouse.dir", "/tmp/spark-warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("\n✅ SparkSession with Hive support created!")
    print(f"📁 Warehouse directory: /tmp/spark-warehouse")
    print(f"📁 Metastore directory: /tmp/metastore_db")
    
    return spark


def example_1_create_database(spark):
    """
    Example 1: Create and use Hive databases.
    
    WHAT: Organize tables into logical databases
    WHY: Separate different projects/business units
    HOW: Use CREATE DATABASE and USE commands
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: CREATE HIVE DATABASE")
    print("=" * 80)
    
    # Create a new database
    spark.sql("CREATE DATABASE IF NOT EXISTS sales_analytics")
    
    print("\n✅ Created database: sales_analytics")
    
    # Show all databases
    print("\n📊 Available databases:")
    spark.sql("SHOW DATABASES").show()
    
    # Use the database
    spark.sql("USE sales_analytics")
    print("\n✅ Now using database: sales_analytics")


def example_2_create_managed_table(spark):
    """
    Example 2: Create a managed (internal) table.
    
    WHAT: Hive manages both data and metadata
    WHY: Let Hive handle data lifecycle
    HOW: CREATE TABLE without EXTERNAL keyword
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: CREATE MANAGED TABLE")
    print("=" * 80)
    
    # Create sample sales data
    sales_data = [
        (1, "Laptop", 1200.00, "Electronics", "2024-01-15"),
        (2, "Mouse", 25.99, "Electronics", "2024-01-16"),
        (3, "Desk", 350.00, "Furniture", "2024-01-17"),
        (4, "Chair", 150.00, "Furniture", "2024-01-18"),
        (5, "Monitor", 300.00, "Electronics", "2024-01-19"),
    ]
    
    columns = ["product_id", "product_name", "price", "category", "sale_date"]
    df = spark.createDataFrame(sales_data, columns)
    
    # Create managed table from DataFrame
    df.write.mode("overwrite").saveAsTable("sales")
    
    print("\n✅ Created managed table: sales")
    
    # Show table details
    print("\n📋 Table schema:")
    spark.sql("DESCRIBE sales").show()
    
    print("\n📊 Table data:")
    spark.sql("SELECT * FROM sales").show()
    
    # Show table location (managed by Hive)
    print("\n📁 Table location:")
    spark.sql("DESCRIBE EXTENDED sales").filter(
        col("col_name") == "Location"
    ).show(truncate=False)


def example_3_hiveql_queries(spark):
    """
    Example 3: Run HiveQL queries.
    
    WHAT: Use SQL to query Hive tables
    WHY: Familiar SQL interface for data analysis
    HOW: Use spark.sql() with HiveQL syntax
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: HIVEQL QUERIES")
    print("=" * 80)
    
    # Simple SELECT
    print("\n📊 All sales:")
    spark.sql("""
        SELECT product_name, price, category
        FROM sales
        ORDER BY price DESC
    """).show()
    
    # Aggregation
    print("\n📊 Sales by category:")
    spark.sql("""
        SELECT 
            category,
            COUNT(*) as num_products,
            SUM(price) as total_revenue,
            AVG(price) as avg_price
        FROM sales
        GROUP BY category
        ORDER BY total_revenue DESC
    """).show()
    
    # Filtering
    print("\n📊 High-value products (> $200):")
    spark.sql("""
        SELECT product_name, price, category
        FROM sales
        WHERE price > 200
        ORDER BY price DESC
    """).show()
    
    # Subquery
    print("\n📊 Products above average price:")
    spark.sql("""
        SELECT product_name, price, category
        FROM sales
        WHERE price > (SELECT AVG(price) FROM sales)
    """).show()


def example_4_partitioned_tables(spark):
    """
    Example 4: Create partitioned tables for performance.
    
    WHAT: Organize data by partition columns
    WHY: Faster queries on specific partitions
    HOW: Use PARTITIONED BY clause
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: PARTITIONED TABLES")
    print("=" * 80)
    
    # Create larger dataset with dates
    import random
    from datetime import datetime, timedelta
    
    start_date = datetime(2024, 1, 1)
    sales_data_large = []
    
    for i in range(50):
        date = start_date + timedelta(days=random.randint(0, 90))
        product = random.choice(["Laptop", "Mouse", "Monitor", "Keyboard", "Desk"])
        category = "Electronics" if product in ["Laptop", "Mouse", "Monitor", "Keyboard"] else "Furniture"
        price = random.uniform(20, 1500)
        
        sales_data_large.append((
            i + 1,
            product,
            round(price, 2),
            category,
            date.strftime("%Y-%m-%d"),
            date.year,
            date.month
        ))
    
    columns = ["product_id", "product_name", "price", "category", "sale_date", "year", "month"]
    df = spark.createDataFrame(sales_data_large, columns)
    
    # Create partitioned table
    df.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .saveAsTable("sales_partitioned")
    
    print("\n✅ Created partitioned table: sales_partitioned")
    
    # Show partitions
    print("\n📁 Table partitions:")
    spark.sql("SHOW PARTITIONS sales_partitioned").show(10)
    
    # Query specific partition (faster!)
    print("\n📊 Sales in January 2024 (partition pruning):")
    spark.sql("""
        SELECT product_name, price, sale_date
        FROM sales_partitioned
        WHERE year = 2024 AND month = 1
        ORDER BY sale_date
    """).show(5)
    
    print("\n💡 Partition pruning = Scan only relevant partitions, not entire table!")


def example_5_external_tables(spark):
    """
    Example 5: Create external tables.
    
    WHAT: Point to existing data without copying
    WHY: Data managed outside Hive, flexibility
    HOW: CREATE EXTERNAL TABLE with LOCATION
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: EXTERNAL TABLES")
    print("=" * 80)
    
    # Create data in external location
    external_path = "/tmp/external_data/products"
    
    products_data = [
        (1, "Laptop", "Electronics", 1200.00),
        (2, "Desk", "Furniture", 350.00),
        (3, "Monitor", "Electronics", 300.00),
    ]
    
    df = spark.createDataFrame(
        products_data,
        ["product_id", "product_name", "category", "price"]
    )
    
    # Save to external location
    df.write.mode("overwrite").parquet(external_path)
    
    print(f"\n✅ Saved data to: {external_path}")
    
    # Create external table pointing to this data
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS products_external (
            product_id INT,
            product_name STRING,
            category STRING,
            price DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{external_path}'
    """)
    
    print("\n✅ Created external table: products_external")
    
    print("\n📊 External table data:")
    spark.sql("SELECT * FROM products_external").show()
    
    print("\n💡 Dropping external table won't delete the data files!")


def example_6_insert_operations(spark):
    """
    Example 6: Insert data into Hive tables.
    
    WHAT: Add new data to existing tables
    WHY: Incremental data loading
    HOW: INSERT INTO and INSERT OVERWRITE
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 6: INSERT OPERATIONS")
    print("=" * 80)
    
    # Insert new rows
    spark.sql("""
        INSERT INTO sales
        VALUES 
            (6, 'Keyboard', 75.00, 'Electronics', '2024-01-20'),
            (7, 'Lamp', 45.00, 'Furniture', '2024-01-21')
    """)
    
    print("\n✅ Inserted 2 new rows into sales table")
    
    print("\n📊 Updated table:")
    spark.sql("SELECT * FROM sales ORDER BY product_id").show()
    
    # Insert from SELECT
    print("\n✅ Inserting Electronics products into new table...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS electronics_only AS
        SELECT * FROM sales WHERE category = 'Electronics'
    """)
    
    spark.sql("SELECT * FROM electronics_only").show()


def example_7_table_management(spark):
    """
    Example 7: Manage Hive tables.
    
    WHAT: View, describe, and manage tables
    WHY: Understand table structure and metadata
    HOW: SHOW, DESCRIBE, DROP commands
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 7: TABLE MANAGEMENT")
    print("=" * 80)
    
    # Show all tables
    print("\n📋 All tables in current database:")
    spark.sql("SHOW TABLES").show()
    
    # Show table creation DDL
    print("\n📋 Create statement for sales table:")
    spark.sql("SHOW CREATE TABLE sales").show(truncate=False)
    
    # Detailed table info
    print("\n📋 Detailed table information:")
    spark.sql("DESCRIBE FORMATTED sales").show(50, truncate=False)
    
    # Table statistics
    print("\n📊 Table statistics:")
    spark.sql("ANALYZE TABLE sales COMPUTE STATISTICS")
    print("✅ Statistics computed")


def main():
    """Main execution - run all examples."""
    print("\n" + "🎓" * 40)
    print("PYSPARK + HIVE: COMPREHENSIVE GUIDE")
    print("🎓" * 40)
    
    # Create Spark session with Hive
    spark = create_spark_session_with_hive()
    
    try:
        # Run all examples
        example_1_create_database(spark)
        example_2_create_managed_table(spark)
        example_3_hiveql_queries(spark)
        example_4_partitioned_tables(spark)
        example_5_external_tables(spark)
        example_6_insert_operations(spark)
        example_7_table_management(spark)
        
        print("\n" + "=" * 80)
        print("🎉 CONGRATULATIONS! YOU'VE MASTERED HIVE WITH PYSPARK!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Enable Hive support in SparkSession")
        print("   2. Create databases and managed tables")
        print("   3. Run HiveQL queries")
        print("   4. Use partitioned tables for performance")
        print("   5. Create external tables")
        print("   6. Insert and update data")
        print("   7. Manage tables and metadata")
        
        print("\n➡️  NEXT STEP: Try hive/02_advanced_hive.py")
        print("=" * 80 + "\n")
        
    finally:
        # Cleanup (optional)
        print("\n🧹 Cleanup:")
        print("To keep tables: Do nothing")
        print("To clean up: spark.sql('DROP DATABASE sales_analytics CASCADE')")
        
        spark.stop()


if __name__ == "__main__":
    main()
