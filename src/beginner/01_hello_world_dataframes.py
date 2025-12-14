"""
================================================================================
BEGINNER 01: Hello World and DataFrame Basics
================================================================================

PURPOSE:
--------
Your first PySpark program! Learn how to create a Spark session, build DataFrames,
and perform basic operations.

WHAT YOU'LL LEARN:
------------------
- Creating a SparkSession (entry point to PySpark)
- Building DataFrames from Python lists
- Viewing data with show()
- Basic DataFrame operations (select, filter)
- Simple aggregations (count, sum, avg)

WHY START HERE:
---------------
DataFrames are the foundation of PySpark. Master these basics before moving on
to advanced topics. Think of DataFrames like Excel spreadsheets - but for big data!

PREREQUISITES:
--------------
- Python basics (lists, dictionaries)
- Basic SQL knowledge helpful but not required
- PySpark installed (pip install pyspark)

REAL-WORLD USE CASE:
--------------------
**E-commerce Analytics**: Analyze customer orders to find:
- Total number of orders
- Average order value
- Orders over $100
- Most active customers

TIME TO COMPLETE: 15 minutes
DIFFICULTY: ⭐☆☆☆☆ (1/5)

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum as spark_sum


def create_spark_session():
    """
    WHAT: Create a SparkSession - the entry point for PySpark
    WHY: You need a SparkSession to work with DataFrames
    HOW: Use SparkSession.builder with an app name
    """
    return SparkSession.builder.appName("Beginner01_HelloWorld").getOrCreate()


def example_1_create_dataframe(spark):
    """
    Example 1: Create your first DataFrame from a Python list.
    
    WHAT: Convert Python data into a distributed DataFrame
    WHY: Real data often starts in Python before scaling to PySpark
    HOW: Use spark.createDataFrame() with schema names
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: CREATE YOUR FIRST DATAFRAME")
    print("=" * 80)
    
    # Sample data: Customer orders
    data = [
        (1, "Alice", 150.50, "2024-01-15"),
        (2, "Bob", 89.99, "2024-01-16"),
        (3, "Alice", 200.00, "2024-01-17"),
        (4, "Charlie", 45.75, "2024-01-18"),
        (5, "Bob", 120.00, "2024-01-19"),
    ]
    
    # Define column names
    columns = ["order_id", "customer_name", "amount", "order_date"]
    
    # Create DataFrame
    df = spark.createDataFrame(data, columns)
    
    print("\n✅ DataFrame created successfully!")
    print("\n📊 Your data:")
    df.show()
    
    print("\n📋 Schema (data types):")
    df.printSchema()
    
    return df


def example_2_select_columns(df):
    """
    Example 2: Select specific columns.
    
    WHAT: Choose which columns to display (like SQL SELECT)
    WHY: Focus on relevant data, reduce data transfer
    HOW: Use select() method with column names
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: SELECT COLUMNS")
    print("=" * 80)
    
    # Select just customer name and amount
    result = df.select("customer_name", "amount")
    
    print("\n✅ Selected columns: customer_name, amount")
    result.show()
    
    return result


def example_3_filter_rows(df):
    """
    Example 3: Filter rows based on conditions.
    
    WHAT: Keep only rows that meet certain criteria (like SQL WHERE)
    WHY: Focus on relevant data, find specific patterns
    HOW: Use filter() or where() with conditions
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: FILTER ROWS")
    print("=" * 80)
    
    # Find orders over $100
    high_value_orders = df.filter(col("amount") > 100)
    
    print("\n✅ Orders over $100:")
    high_value_orders.show()
    
    # Multiple conditions: Alice's orders over $100
    alice_big_orders = df.filter(
        (col("customer_name") == "Alice") & (col("amount") > 100)
    )
    
    print("\n✅ Alice's orders over $100:")
    alice_big_orders.show()
    
    return high_value_orders


def example_4_aggregations(df):
    """
    Example 4: Calculate summary statistics.
    
    WHAT: Count rows, sum values, calculate averages
    WHY: Understand your data at a glance
    HOW: Use agg() with functions like count(), sum(), avg()
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: AGGREGATIONS")
    print("=" * 80)
    
    # Total number of orders
    total_orders = df.count()
    print(f"\n📊 Total Orders: {total_orders}")
    
    # Calculate statistics
    stats = df.agg(
        count("order_id").alias("total_orders"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value")
    )
    
    print("\n✅ Summary Statistics:")
    stats.show()
    
    return stats


def example_5_group_by(df):
    """
    Example 5: Group data and aggregate by category.
    
    WHAT: Group rows by a column and calculate per-group statistics
    WHY: Find patterns per customer, product, region, etc.
    HOW: Use groupBy() followed by agg()
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: GROUP BY")
    print("=" * 80)
    
    # Orders per customer
    customer_stats = df.groupBy("customer_name").agg(
        count("order_id").alias("num_orders"),
        spark_sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value")
    )
    
    print("\n✅ Statistics per customer:")
    customer_stats.show()
    
    # Sort by total spent (descending)
    top_customers = customer_stats.orderBy(col("total_spent").desc())
    
    print("\n🏆 Top customers by spending:")
    top_customers.show()
    
    return top_customers


def main():
    """Main execution - run all examples."""
    print("\n" + "🎓" * 40)
    print("WELCOME TO PYSPARK - BEGINNER LESSON 1")
    print("🎓" * 40)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Run all examples
        df = example_1_create_dataframe(spark)
        example_2_select_columns(df)
        example_3_filter_rows(df)
        example_4_aggregations(df)
        example_5_group_by(df)
        
        print("\n" + "=" * 80)
        print("🎉 CONGRATULATIONS! YOU'VE COMPLETED BEGINNER LESSON 1!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Create a SparkSession")
        print("   2. Build DataFrames from Python data")
        print("   3. Select specific columns")
        print("   4. Filter rows with conditions")
        print("   5. Calculate aggregations (count, sum, avg)")
        print("   6. Group data by categories")
        
        print("\n➡️  NEXT STEP: Try beginner/02_reading_files.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
