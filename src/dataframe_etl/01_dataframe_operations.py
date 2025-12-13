"""
01_dataframe_operations.py
===========================

DataFrame ETL Operations - Complete Examples

Covers all major DataFrame operations:
- Selection, Filter, Sorting
- Joins
- Aggregations & GroupBy
- Window Functions
- Built-in Functions
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, when, expr,
    sum as _sum, avg, count, max as _max, min as _min,
    row_number, rank, dense_rank, lag, lead,
    concat, upper, lower, substring, regexp_replace,
    to_date, year, month, dayofmonth, date_add, datediff,
    array, explode, size, array_contains,
    collect_list, collect_set,
    from_json, to_json, get_json_object
)
from pyspark.sql.types import *


def create_spark():
    return SparkSession.builder \
        .appName("DataFrameETL") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def create_sample_data(spark):
    """
    Create comprehensive sample dataset for demonstrations.
    """
    print("=" * 80)
    print("CREATING SAMPLE DATA")
    print("=" * 80)
    
    # Sales data
    sales_data = [
        (1, "2024-01-15", "Alice", "Electronics", "Laptop", 1200, 1),
        (2, "2024-01-16", "Bob", "Electronics", "Phone", 800, 2),
        (3, "2024-01-17", "Charlie", "Clothing", "Shirt", 50, 3),
        (4, "2024-01-18", "Alice", "Clothing", "Pants", 80, 2),
        (5, "2024-01-19", "David", "Electronics", "Tablet", 600, 1),
        (6, "2024-01-20", "Bob", "Electronics", "Laptop", 1200, 1),
        (7, "2024-01-21", "Eve", "Furniture", "Chair", 150, 4),
        (8, "2024-01-22", "Alice", "Furniture", "Desk", 400, 1),
        (9, "2024-01-23", "Charlie", "Electronics", "Mouse", 25, 5),
        (10, "2024-01-24", "David", "Clothing", "Jacket", 120, 1)
    ]
    
    sales_df = spark.createDataFrame(
        sales_data,
        ["id", "date", "customer", "category", "product", "price", "quantity"]
    )
    
    print("✅ Created sales dataset:")
    sales_df.show(5)
    
    return sales_df


def demonstrate_selection_filter(df):
    """
    Selection and filtering operations.
    """
    print("\n" + "=" * 80)
    print("SELECTION & FILTER OPERATIONS")
    print("=" * 80)
    
    # Select specific columns
    print("\n1️⃣  Select columns:")
    df.select("customer", "product", "price").show(5)
    
    # Select with expressions
    print("2️⃣  Select with calculations:")
    df.select(
        col("customer"),
        col("product"),
        (col("price") * col("quantity")).alias("total")
    ).show(5)
    
    # Filter operations
    print("3️⃣  Filter: price > 100:")
    df.filter(col("price") > 100).show(5)
    
    print("4️⃣  Filter: Electronics category:")
    df.where(col("category") == "Electronics").show(5)
    
    # Complex filters
    print("5️⃣  Complex filter (AND/OR):")
    df.filter(
        (col("price") > 100) & (col("category") == "Electronics")
    ).show(5)


def demonstrate_sorting(df):
    """
    Sorting operations.
    """
    print("\n" + "=" * 80)
    print("SORTING OPERATIONS")
    print("=" * 80)
    
    # Sort ascending
    print("\n1️⃣  Sort by price (ascending):")
    df.orderBy("price").show(5)
    
    # Sort descending
    print("2️⃣  Sort by price (descending):")
    df.orderBy(col("price").desc()).show(5)
    
    # Multiple columns
    print("3️⃣  Sort by category, then price:")
    df.orderBy("category", col("price").desc()).show(10)


def demonstrate_aggregations(df):
    """
    Aggregation operations.
    """
    print("\n" + "=" * 80)
    print("AGGREGATION OPERATIONS")
    print("=" * 80)
    
    # Simple aggregations
    print("\n1️⃣  Overall statistics:")
    df.select(
        count("*").alias("total_orders"),
        _sum("price").alias("total_revenue"),
        avg("price").alias("avg_price"),
        _max("price").alias("max_price"),
        _min("price").alias("min_price")
    ).show()
    
    # GroupBy
    print("2️⃣  GroupBy category:")
    df.groupBy("category").agg(
        count("*").alias("num_orders"),
        _sum(col("price") * col("quantity")).alias("revenue"),
        avg("price").alias("avg_price")
    ).orderBy(col("revenue").desc()).show()
    
    # Multiple grouping columns
    print("3️⃣  GroupBy customer and category:")
    df.groupBy("customer", "category").agg(
        count("*").alias("orders"),
        _sum(col("price") * col("quantity")).alias("total_spent")
    ).orderBy(col("total_spent").desc()).show()


def demonstrate_joins(spark, df):
    """
    Join operations.
    """
    print("\n" + "=" * 80)
    print("JOIN OPERATIONS")
    print("=" * 80)
    
    # Create customer info dataset
    customer_data = [
        ("Alice", "New York", "Premium"),
        ("Bob", "San Francisco", "Standard"),
        ("Charlie", "Los Angeles", "Premium"),
        ("David", "Chicago", "Standard"),
        ("Frank", "Boston", "Premium")  # No orders
    ]
    
    customers_df = spark.createDataFrame(
        customer_data,
        ["customer", "city", "tier"]
    )
    
    print("\nCustomer data:")
    customers_df.show()
    
    # Inner join
    print("\n1️⃣  Inner Join:")
    inner_result = df.join(customers_df, "customer", "inner")
    inner_result.select("customer", "city", "tier", "product", "price").show(5)
    
    # Left join
    print("2️⃣  Left Join (all sales):")
    left_result = df.join(customers_df, "customer", "left")
    print(f"   Sales records: {df.count()}, After left join: {left_result.count()}")
    
    # Right join
    print("3️⃣  Right Join (all customers):")
    right_result = df.join(customers_df, "customer", "right")
    print(f"   Customer records: {customers_df.count()}, After right join: {right_result.count()}")
    right_result.select("customer", "city", "product").show()


def demonstrate_window_functions(df):
    """
    Window function operations.
    """
    print("\n" + "=" * 80)
    print("WINDOW FUNCTIONS")
    print("=" * 80)
    
    # Window spec
    window_spec = Window.partitionBy("category").orderBy(col("price").desc())
    
    # Ranking functions
    print("\n1️⃣  Ranking within category:")
    df.withColumn("rank", rank().over(window_spec)) \
      .withColumn("row_num", row_number().over(window_spec)) \
      .withColumn("dense_rank", dense_rank().over(window_spec)) \
      .select("category", "product", "price", "rank", "row_num", "dense_rank") \
      .show(10)
    
    # Running totals
    print("2️⃣  Running total by customer:")
    customer_window = Window.partitionBy("customer").orderBy("id")
    df.withColumn(
        "running_total",
        _sum(col("price") * col("quantity")).over(customer_window)
    ).select("customer", "product", "price", "quantity", "running_total").show()
    
    # Lag/Lead
    print("3️⃣  Previous and next order:")
    df.withColumn("prev_product", lag("product", 1).over(Window.orderBy("id"))) \
      .withColumn("next_product", lead("product", 1).over(Window.orderBy("id"))) \
      .select("id", "prev_product", "product", "next_product") \
      .show(10)


def demonstrate_builtin_functions(spark):
    """
    Built-in functions examples.
    """
    print("\n" + "=" * 80)
    print("BUILT-IN FUNCTIONS")
    print("=" * 80)
    
    # String functions
    print("\n1️⃣  String functions:")
    data = [("john doe", "john.doe@email.com", "123-456-7890")]
    df = spark.createDataFrame(data, ["name", "email", "phone"])
    
    df.select(
        upper(col("name")).alias("upper_name"),
        lower(col("email")).alias("lower_email"),
        concat(col("name"), lit(" - "), col("email")).alias("combined"),
        substring(col("phone"), 1, 3).alias("area_code"),
        regexp_replace(col("phone"), "-", "").alias("clean_phone")
    ).show(truncate=False)
    
    # Date functions
    print("2️⃣  Date functions:")
    date_data = [("2024-01-15",), ("2024-06-20",), ("2024-12-25",)]
    date_df = spark.createDataFrame(date_data, ["date_str"])
    
    date_df.select(
        to_date(col("date_str")).alias("date"),
        year(to_date(col("date_str"))).alias("year"),
        month(to_date(col("date_str"))).alias("month"),
        dayofmonth(to_date(col("date_str"))).alias("day"),
        date_add(to_date(col("date_str")), 30).alias("plus_30_days")
    ).show()
    
    # Array functions
    print("3️⃣  Array functions:")
    array_data = [(["apple", "banana", "orange"],), (["grape", "melon"],)]
    array_df = spark.createDataFrame(array_data, ["fruits"])
    
    array_df.select(
        col("fruits"),
        size(col("fruits")).alias("count"),
        array_contains(col("fruits"), "apple").alias("has_apple"),
        explode(col("fruits")).alias("fruit")
    ).show()


def demonstrate_repartition_coalesce(df):
    """
    Repartition and coalesce operations.
    """
    print("\n" + "=" * 80)
    print("REPARTITION & COALESCE")
    print("=" * 80)
    
    print(f"\nOriginal partitions: {df.rdd.getNumPartitions()}")
    
    # Repartition
    repartitioned = df.repartition(4)
    print(f"After repartition(4): {repartitioned.rdd.getNumPartitions()}")
    
    # Repartition by column
    repartitioned_col = df.repartition(4, "category")
    print(f"After repartition(4, 'category'): {repartitioned_col.rdd.getNumPartitions()}")
    
    # Coalesce
    coalesced = df.coalesce(2)
    print(f"After coalesce(2): {coalesced.rdd.getNumPartitions()}")


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("DATAFRAME ETL - COMPREHENSIVE EXAMPLES")
    print("🎯" * 40)
    
    spark = create_spark()
    
    # Create and demonstrate
    df = create_sample_data(spark)
    demonstrate_selection_filter(df)
    demonstrate_sorting(df)
    demonstrate_aggregations(df)
    demonstrate_joins(spark, df)
    demonstrate_window_functions(df)
    demonstrate_builtin_functions(spark)
    demonstrate_repartition_coalesce(df)
    
    print("\n" + "=" * 80)
    print("✅ DATAFRAME ETL COMPLETE")
    print("=" * 80)
    print("\n�� Operations Covered:")
    print("   ✅ Selection & Filtering")
    print("   ✅ Sorting")
    print("   ✅ Aggregations & GroupBy")
    print("   ✅ Joins (inner, left, right)")
    print("   ✅ Window Functions")
    print("   ✅ Built-in Functions")
    print("   ✅ Repartition & Coalesce")
    
    spark.stop()


if __name__ == "__main__":
    main()
