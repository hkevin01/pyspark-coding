"""
================================================================================
BEGINNER 04: Joining DataFrames
================================================================================

PURPOSE: Learn how to combine multiple DataFrames using joins - the foundation
of relational data processing.

WHAT YOU'LL LEARN:
- Inner joins (matching rows only)
- Left joins (keep all left rows)
- Right joins (keep all right rows)
- Full outer joins (keep all rows)
- Join conditions and multiple keys

WHY: Real data is split across multiple tables. Joins let you combine them.

REAL-WORLD: Combine customers, orders, and products tables for sales analysis.

TIME: 30 minutes | DIFFICULTY: ⭐⭐⭐☆☆ (3/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum


def create_spark_session():
    return SparkSession.builder.appName("Beginner04_Joins").getOrCreate()


def create_sample_data(spark):
    """Create sample tables for join examples."""
    print("\n" + "=" * 80)
    print("CREATING SAMPLE DATA")
    print("=" * 80)
    
    # Customers table
    customers = [
        (1, "Alice", "alice@email.com", "NY"),
        (2, "Bob", "bob@email.com", "CA"),
        (3, "Charlie", "charlie@email.com", "TX"),
        (4, "Diana", "diana@email.com", "FL"),
    ]
    df_customers = spark.createDataFrame(customers, ["customer_id", "name", "email", "state"])
    
    # Orders table (some customers have no orders, some orders have invalid customer_id)
    orders = [
        (101, 1, 150.00, "2024-01-15"),
        (102, 2, 200.00, "2024-01-16"),
        (103, 1, 75.00, "2024-01-17"),
        (104, 3, 300.00, "2024-01-18"),
        (105, 5, 50.00, "2024-01-19"),  # customer_id=5 doesn't exist!
    ]
    df_orders = spark.createDataFrame(orders, ["order_id", "customer_id", "amount", "order_date"])
    
    print("\n👥 CUSTOMERS TABLE:")
    df_customers.show()
    
    print("\n🛒 ORDERS TABLE:")
    df_orders.show()
    
    print("\n💡 NOTE: Customer 4 (Diana) has no orders")
    print("   NOTE: Order 105 references non-existent customer 5")
    
    return df_customers, df_orders


def example_1_inner_join(df_customers, df_orders):
    """Inner join - keep only matching rows."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: INNER JOIN (Most Common)")
    print("=" * 80)
    
    # Join customers and orders
    df_result = df_customers.join(
        df_orders,
        df_customers.customer_id == df_orders.customer_id,
        "inner"
    )
    
    print("\n✅ INNER JOIN Result:")
    print("   Only rows where customer_id matches in BOTH tables")
    df_result.select(
        df_customers.customer_id,
        "name",
        "order_id",
        "amount",
        "order_date"
    ).show()
    
    print(f"\n�� Rows: {df_result.count()}")
    print("   ❌ Diana excluded (no orders)")
    print("   ❌ Order 105 excluded (invalid customer)")
    
    return df_result


def example_2_left_join(df_customers, df_orders):
    """Left join - keep all left table rows."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: LEFT JOIN")
    print("=" * 80)
    
    df_result = df_customers.join(
        df_orders,
        "customer_id",  # Shorthand when column names match
        "left"
    )
    
    print("\n✅ LEFT JOIN Result:")
    print("   All customers + their orders (null if no orders)")
    df_result.select(
        "customer_id",
        "name",
        "order_id",
        "amount"
    ).show()
    
    print("\n📊 Notice:")
    print("   ✅ Diana included (order_id = null)")
    print("   ❌ Order 105 excluded (not in customers table)")
    
    return df_result


def example_3_right_join(df_customers, df_orders):
    """Right join - keep all right table rows."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: RIGHT JOIN")
    print("=" * 80)
    
    df_result = df_customers.join(
        df_orders,
        "customer_id",
        "right"
    )
    
    print("\n✅ RIGHT JOIN Result:")
    print("   All orders + customer info (null if customer not found)")
    df_result.select(
        "customer_id",
        "name",
        "order_id",
        "amount"
    ).show()
    
    print("\n📊 Notice:")
    print("   ✅ Order 105 included (name = null)")
    print("   ❌ Diana excluded (no orders)")
    
    return df_result


def example_4_full_outer_join(df_customers, df_orders):
    """Full outer join - keep all rows from both tables."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: FULL OUTER JOIN")
    print("=" * 80)
    
    df_result = df_customers.join(
        df_orders,
        "customer_id",
        "outer"
    )
    
    print("\n✅ FULL OUTER JOIN Result:")
    print("   All customers AND all orders (nulls where no match)")
    df_result.select(
        "customer_id",
        "name",
        "order_id",
        "amount"
    ).show()
    
    print("\n📊 Notice:")
    print("   ✅ Diana included (no orders)")
    print("   ✅ Order 105 included (no customer)")
    print("   ✅ Everything included!")
    
    return df_result


def example_5_practical_analysis(df_customers, df_orders):
    """Practical example: Customer lifetime value."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: PRACTICAL ANALYSIS")
    print("=" * 80)
    
    # Calculate customer lifetime value
    df_result = df_customers.join(
        df_orders,
        "customer_id",
        "left"  # Include customers with no orders
    ).groupBy("customer_id", "name", "state").agg(
        count("order_id").alias("num_orders"),
        spark_sum("amount").alias("total_spent")
    ).fillna({"num_orders": 0, "total_spent": 0.0}).orderBy(col("total_spent").desc())
    
    print("\n✅ Customer Lifetime Value:")
    df_result.show()
    
    print("\n💡 Business Insights:")
    print("   - Who are top customers?")
    print("   - Who hasn't ordered yet?")
    print("   - Which states generate most revenue?")
    
    return df_result


def main():
    """Run all join examples."""
    print("\n" + "🔗" * 40)
    print("BEGINNER LESSON 4: JOINING DATAFRAMES")
    print("🔗" * 40)
    
    spark = create_spark_session()
    
    try:
        df_customers, df_orders = create_sample_data(spark)
        
        example_1_inner_join(df_customers, df_orders)
        example_2_left_join(df_customers, df_orders)
        example_3_right_join(df_customers, df_orders)
        example_4_full_outer_join(df_customers, df_orders)
        example_5_practical_analysis(df_customers, df_orders)
        
        print("\n" + "=" * 80)
        print("🎉 JOINS MASTERED!")
        print("=" * 80)
        print("\n✅ Join Types:")
        print("   - INNER:  Only matching rows")
        print("   - LEFT:   All left + matching right")
        print("   - RIGHT:  All right + matching left")
        print("   - OUTER:  All rows from both")
        
        print("\n🏆 WHEN TO USE:")
        print("   - INNER: Most common, only valid matches")
        print("   - LEFT:  Keep all customers, add order info")
        print("   - RIGHT: Keep all orders, add customer info")
        print("   - OUTER: Comprehensive analysis, find gaps")
        
        print("\n➡️  NEXT: Try beginner/05_simple_etl_pipeline.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
