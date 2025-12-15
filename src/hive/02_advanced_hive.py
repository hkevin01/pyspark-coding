#!/usr/bin/env python3
"""
================================================================================
HIVE 02: Advanced Hive Operations with PySpark
================================================================================

PURPOSE:
--------
Master advanced Hive features including complex queries, window functions,
file formats, and performance optimization.

WHAT YOU'LL LEARN:
------------------
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- Complex joins and subqueries
- File formats (ORC, Parquet, Avro)
- Bucketing for performance
- Views and CTEs
- Dynamic partitioning
- Hive UDFs with PySpark

REAL-WORLD USE CASE:
--------------------
**Advanced Analytics**: 
- Customer segmentation with window functions
- Time-series analysis with LAG/LEAD
- Optimized data storage with ORC/Parquet
- Performance tuning with bucketing

TIME TO COMPLETE: 35 minutes
DIFFICULTY: ⭐⭐⭐⭐☆ (4/5)

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta


def create_spark_session():
    """Create SparkSession with Hive and optimization configs."""
    return SparkSession.builder \
        .appName("AdvancedHive") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport() \
        .getOrCreate()


def example_1_window_functions(spark):
    """
    Example 1: Window functions for advanced analytics.
    
    WHAT: Perform calculations across rows related to current row
    WHY: Ranking, running totals, moving averages
    HOW: Use OVER() with PARTITION BY and ORDER BY
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: WINDOW FUNCTIONS")
    print("=" * 80)
    
    # Create sales data with multiple transactions per customer
    sales_data = []
    customers = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    
    for i in range(20):
        sales_data.append((
            i + 1,
            random.choice(customers),
            random.randint(50, 500),
            (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
        ))
    
    df = spark.createDataFrame(
        sales_data,
        ["order_id", "customer", "amount", "order_date"]
    )
    
    df.write.mode("overwrite").saveAsTable("customer_orders")
    
    # ROW_NUMBER: Assign sequential number
    print("\n📊 Orders with row numbers per customer:")
    spark.sql("""
        SELECT 
            customer,
            order_date,
            amount,
            ROW_NUMBER() OVER (
                PARTITION BY customer 
                ORDER BY order_date
            ) as order_number
        FROM customer_orders
        ORDER BY customer, order_number
    """).show(10)
    
    # RANK: Rank by amount
    print("\n📊 Top orders by customer (ranked by amount):")
    spark.sql("""
        SELECT 
            customer,
            order_date,
            amount,
            RANK() OVER (
                PARTITION BY customer 
                ORDER BY amount DESC
            ) as rank
        FROM customer_orders
    """).show(10)
    
    # Running total
    print("\n📊 Running total per customer:")
    spark.sql("""
        SELECT 
            customer,
            order_date,
            amount,
            SUM(amount) OVER (
                PARTITION BY customer 
                ORDER BY order_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as running_total
        FROM customer_orders
        ORDER BY customer, order_date
    """).show(10)
    
    # LAG and LEAD
    print("\n📊 Previous and next order amounts:")
    spark.sql("""
        SELECT 
            customer,
            order_date,
            amount,
            LAG(amount) OVER (
                PARTITION BY customer 
                ORDER BY order_date
            ) as prev_amount,
            LEAD(amount) OVER (
                PARTITION BY customer 
                ORDER BY order_date
            ) as next_amount
        FROM customer_orders
        ORDER BY customer, order_date
    """).show(10)


def example_2_file_formats(spark):
    """
    Example 2: Different file formats (ORC, Parquet, Avro).
    
    WHAT: Store data in optimized columnar formats
    WHY: Better compression, faster queries
    HOW: STORED AS ORC/PARQUET/AVRO
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: FILE FORMATS")
    print("=" * 80)
    
    # Create sample data
    data = [(i, f"Product_{i}", random.randint(10, 1000)) 
            for i in range(1000)]
    df = spark.createDataFrame(data, ["id", "name", "value"])
    
    # Save as ORC
    df.write.mode("overwrite").format("orc").saveAsTable("products_orc")
    print("✅ Created ORC table: products_orc")
    
    # Save as Parquet
    df.write.mode("overwrite").format("parquet").saveAsTable("products_parquet")
    print("✅ Created Parquet table: products_parquet")
    
    # Show storage details
    print("\n📁 ORC table details:")
    spark.sql("DESCRIBE FORMATTED products_orc") \
        .filter(col("col_name").isin(["Location", "SerDe Library", "InputFormat"])) \
        .show(truncate=False)
    
    print("\n📁 Parquet table details:")
    spark.sql("DESCRIBE FORMATTED products_parquet") \
        .filter(col("col_name").isin(["Location", "SerDe Library", "InputFormat"])) \
        .show(truncate=False)
    
    print("\n💡 ORC: Optimized for Hive, excellent compression")
    print("💡 Parquet: Cross-platform, great for analytics")


def example_3_bucketing(spark):
    """
    Example 3: Bucketing for optimized joins.
    
    WHAT: Distribute data into fixed number of buckets
    WHY: Faster joins on bucketed columns
    HOW: CLUSTERED BY with number of buckets
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: BUCKETING")
    print("=" * 80)
    
    # Enable bucketing
    spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
    
    # Create bucketed table
    customers_data = [
        (i, f"Customer_{i}", f"customer{i}@email.com", random.choice(["US", "UK", "DE", "FR"]))
        for i in range(100)
    ]
    
    df = spark.createDataFrame(
        customers_data,
        ["customer_id", "name", "email", "country"]
    )
    
    # Write with bucketing
    df.write.mode("overwrite") \
        .bucketBy(10, "customer_id") \
        .sortBy("customer_id") \
        .saveAsTable("customers_bucketed")
    
    print("\n✅ Created bucketed table: customers_bucketed")
    print("   • Bucketed by: customer_id")
    print("   • Number of buckets: 10")
    print("   • Sorted by: customer_id")
    
    # Show table properties
    print("\n📋 Bucketing info:")
    spark.sql("DESCRIBE FORMATTED customers_bucketed") \
        .filter(col("col_name").contains("Bucket")) \
        .show(truncate=False)
    
    print("\n💡 Bucketing helps with:")
    print("   • Faster joins on bucketed columns")
    print("   • Efficient sampling")
    print("   • Better data distribution")


def example_4_views_and_ctes(spark):
    """
    Example 4: Views and Common Table Expressions (CTEs).
    
    WHAT: Create virtual tables and structured queries
    WHY: Code reusability, readability
    HOW: CREATE VIEW and WITH clauses
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: VIEWS AND CTEs")
    print("=" * 80)
    
    # Create a view
    spark.sql("""
        CREATE OR REPLACE VIEW high_value_customers AS
        SELECT 
            customer,
            COUNT(*) as order_count,
            SUM(amount) as total_spent,
            AVG(amount) as avg_order
        FROM customer_orders
        GROUP BY customer
        HAVING SUM(amount) > 500
    """)
    
    print("\n✅ Created view: high_value_customers")
    
    print("\n📊 View data:")
    spark.sql("SELECT * FROM high_value_customers ORDER BY total_spent DESC").show()
    
    # Use CTE (Common Table Expression)
    print("\n📊 Query with CTE:")
    spark.sql("""
        WITH monthly_sales AS (
            SELECT 
                SUBSTR(order_date, 1, 7) as month,
                SUM(amount) as monthly_total
            FROM customer_orders
            GROUP BY SUBSTR(order_date, 1, 7)
        ),
        avg_monthly AS (
            SELECT AVG(monthly_total) as avg_amount
            FROM monthly_sales
        )
        SELECT 
            ms.month,
            ms.monthly_total,
            am.avg_amount,
            CASE 
                WHEN ms.monthly_total > am.avg_amount THEN 'Above Average'
                ELSE 'Below Average'
            END as performance
        FROM monthly_sales ms
        CROSS JOIN avg_monthly am
        ORDER BY ms.month
    """).show()


def example_5_dynamic_partitioning(spark):
    """
    Example 5: Dynamic partitioning.
    
    WHAT: Automatically create partitions based on data
    WHY: No need to specify partitions manually
    HOW: Enable dynamic partition mode
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: DYNAMIC PARTITIONING")
    print("=" * 80)
    
    # Create data with multiple partitions
    events_data = []
    for i in range(100):
        date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 60))
        events_data.append((
            i + 1,
            random.choice(["click", "view", "purchase", "search"]),
            random.choice(["mobile", "desktop", "tablet"]),
            date.year,
            date.month,
            date.day
        ))
    
    df = spark.createDataFrame(
        events_data,
        ["event_id", "event_type", "device", "year", "month", "day"]
    )
    
    # Write with dynamic partitioning
    df.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .saveAsTable("events_dynamic")
    
    print("\n✅ Created dynamically partitioned table: events_dynamic")
    
    # Show partitions
    print("\n📁 Auto-created partitions:")
    spark.sql("SHOW PARTITIONS events_dynamic").show(15)
    
    print("\n💡 Dynamic partitioning benefits:")
    print("   • No manual partition specification")
    print("   • Automatic partition creation")
    print("   • Handles varying partition values")


def example_6_complex_joins(spark):
    """
    Example 6: Complex joins and subqueries.
    
    WHAT: Advanced join patterns
    WHY: Real-world data often requires complex relationships
    HOW: Multiple joins, subqueries, join hints
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 6: COMPLEX JOINS")
    print("=" * 80)
    
    # Create related tables
    products = [(1, "Laptop", "Electronics"), (2, "Desk", "Furniture"), (3, "Mouse", "Electronics")]
    categories = [("Electronics", "Tech"), ("Furniture", "Home")]
    sales = [(1, 1, 1200), (2, 2, 350), (3, 1, 1300), (4, 3, 25)]
    
    spark.createDataFrame(products, ["product_id", "name", "category"]).write.mode("overwrite").saveAsTable("products")
    spark.createDataFrame(categories, ["category", "department"]).write.mode("overwrite").saveAsTable("categories")
    spark.createDataFrame(sales, ["sale_id", "product_id", "amount"]).write.mode("overwrite").saveAsTable("sales")
    
    # Complex join query
    print("\n📊 Sales with product and department info:")
    spark.sql("""
        SELECT 
            s.sale_id,
            p.name as product_name,
            p.category,
            c.department,
            s.amount
        FROM sales s
        INNER JOIN products p ON s.product_id = p.product_id
        INNER JOIN categories c ON p.category = c.category
        ORDER BY s.amount DESC
    """).show()
    
    # Subquery example
    print("\n📊 Products with above-average sales:")
    spark.sql("""
        SELECT 
            p.name,
            s.amount
        FROM products p
        INNER JOIN sales s ON p.product_id = s.product_id
        WHERE s.amount > (SELECT AVG(amount) FROM sales)
    """).show()


def example_7_performance_tips(spark):
    """
    Example 7: Performance optimization tips.
    
    WHAT: Best practices for fast queries
    WHY: Production workloads need optimization
    HOW: Various tuning techniques
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 7: PERFORMANCE OPTIMIZATION")
    print("=" * 80)
    
    tips = [
        ("1. Use columnar formats", "ORC/Parquet for better compression and faster reads"),
        ("2. Partition wisely", "Don't over-partition (< 1GB per partition ideal)"),
        ("3. Use bucketing", "For large tables with frequent joins"),
        ("4. Collect statistics", "ANALYZE TABLE for query optimization"),
        ("5. Predicate pushdown", "Filter early in WHERE clauses"),
        ("6. Use appropriate joins", "Broadcast small tables"),
        ("7. Compress output", "Enable output compression for saves"),
        ("8. Cache frequently used", "CACHE TABLE for repeated queries"),
    ]
    
    for tip, desc in tips:
        print(f"\n💡 {tip}")
        print(f"   {desc}")
    
    # Example: Analyze table
    spark.sql("ANALYZE TABLE customer_orders COMPUTE STATISTICS")
    print("\n✅ Computed statistics for customer_orders")


def main():
    """Main execution."""
    print("\n" + "🎓" * 40)
    print("ADVANCED HIVE WITH PYSPARK")
    print("🎓" * 40)
    
    spark = create_spark_session()
    
    try:
        spark.sql("USE sales_analytics")
        
        example_1_window_functions(spark)
        example_2_file_formats(spark)
        example_3_bucketing(spark)
        example_4_views_and_ctes(spark)
        example_5_dynamic_partitioning(spark)
        example_6_complex_joins(spark)
        example_7_performance_tips(spark)
        
        print("\n" + "=" * 80)
        print("🎉 ADVANCED HIVE MASTERY COMPLETE!")
        print("=" * 80)
        print("\n✅ Advanced topics covered:")
        print("   1. Window functions (ROW_NUMBER, RANK, LAG, LEAD)")
        print("   2. File formats (ORC, Parquet)")
        print("   3. Bucketing for performance")
        print("   4. Views and CTEs")
        print("   5. Dynamic partitioning")
        print("   6. Complex joins")
        print("   7. Performance optimization")
        
        print("\n➡️  You're now ready for production Hive workloads!")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
