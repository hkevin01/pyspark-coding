"""
================================================================================
INTERMEDIATE 04: Advanced Aggregation Patterns
================================================================================

PURPOSE: Master complex aggregation techniques - pivot, rollup, cube, and
multiple aggregations.

WHAT YOU'LL LEARN:
- Multiple group by levels
- Pivot tables
- Rollup (subtotals)
- Cube (all combinations)
- Custom aggregations

WHY: Real analytics requires multi-dimensional aggregations.

REAL-WORLD: Sales reports, pivot tables, OLAP-style analytics.

TIME: 35 minutes | DIFFICULTY: ⭐⭐⭐⭐☆ (4/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, round as spark_round, collect_list, concat_ws
)


def create_spark_session():
    return SparkSession.builder.appName("Intermediate04_Aggregation").getOrCreate()


def create_sales_data(spark):
    """Create sample sales data."""
    data = [
        ("2024-01", "Electronics", "East", "Laptop", 1000, 5),
        ("2024-01", "Electronics", "East", "Mouse", 50, 20),
        ("2024-01", "Electronics", "West", "Laptop", 1000, 3),
        ("2024-01", "Furniture", "East", "Desk", 500, 4),
        ("2024-02", "Electronics", "East", "Laptop", 1000, 6),
        ("2024-02", "Electronics", "West", "Mouse", 50, 15),
        ("2024-02", "Furniture", "East", "Desk", 500, 5),
        ("2024-02", "Furniture", "West", "Chair", 200, 10),
    ]
    
    df = spark.createDataFrame(
        data,
        ["month", "category", "region", "product", "unit_price", "quantity"]
    ).withColumn("revenue", col("unit_price") * col("quantity"))
    
    return df


def example_1_multiple_groupby(df):
    """Multiple aggregations with groupBy."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: MULTIPLE AGGREGATIONS")
    print("=" * 80)
    
    # Multiple aggregations at once
    df_agg = df.groupBy("category", "region").agg(
        count("*").alias("num_transactions"),
        spark_sum("revenue").alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_revenue"),
        spark_min("revenue").alias("min_revenue"),
        spark_max("revenue").alias("max_revenue"),
        collect_list("product").alias("products")
    ).orderBy("category", "region")
    
    print("\n✅ Multi-Dimensional Aggregation:")
    df_agg.show(truncate=False)
    
    print("\n💡 One groupBy, many aggregations!")
    
    return df_agg


def example_2_pivot_tables(df):
    """Create pivot tables."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: PIVOT TABLES")
    print("=" * 80)
    
    # Pivot: Months as rows, Regions as columns
    df_pivot = df.groupBy("month", "category").pivot("region").agg(
        spark_sum("revenue")
    ).orderBy("month", "category")
    
    print("\n✅ Pivot Table (Region Sales by Month):")
    df_pivot.show()
    
    # Another pivot: Categories as columns
    df_pivot2 = df.groupBy("month").pivot("category").agg(
        spark_sum("revenue")
    ).orderBy("month")
    
    print("\n✅ Pivot Table (Category Sales by Month):")
    df_pivot2.show()
    
    print("\n💡 Pivot converts rows to columns!")
    
    return df_pivot


def example_3_rollup(df):
    """Rollup for subtotals."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: ROLLUP (Subtotals)")
    print("=" * 80)
    
    # Rollup: Hierarchical subtotals
    df_rollup = df.rollup("category", "region").agg(
        spark_sum("revenue").alias("total_revenue")
    ).orderBy("category", "region")
    
    print("\n✅ Rollup (with NULL for subtotals):")
    df_rollup.show()
    
    print("\n💡 Rollup creates hierarchical subtotals:")
    print("   1. (Category, Region) - detail")
    print("   2. (Category, NULL)   - category subtotal")
    print("   3. (NULL, NULL)       - grand total")
    
    return df_rollup


def example_4_cube(df):
    """Cube for all combinations."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: CUBE (All Combinations)")
    print("=" * 80)
    
    # Cube: All dimension combinations
    df_cube = df.cube("category", "region").agg(
        spark_sum("revenue").alias("total_revenue"),
        count("*").alias("num_sales")
    ).orderBy("category", "region")
    
    print("\n✅ Cube (all combinations):")
    df_cube.show()
    
    print("\n💡 Cube creates ALL combinations:")
    print("   1. (Category, Region) - detail")
    print("   2. (Category, NULL)   - by category")
    print("   3. (NULL, Region)     - by region")
    print("   4. (NULL, NULL)       - grand total")
    
    return df_cube


def example_5_practical_report(df):
    """Build a comprehensive sales report."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: COMPREHENSIVE SALES REPORT")
    print("=" * 80)
    
    # Create executive summary
    from pyspark.sql.functions import when, lit
    
    df_report = df.groupBy("month", "category").agg(
        spark_sum("revenue").alias("revenue"),
        count("*").alias("num_orders"),
        spark_round(avg("revenue"), 2).alias("avg_order_value"),
        collect_list("product").alias("products_sold")
    ).withColumn("performance",
        when(col("revenue") >= 5000, "🚀 Excellent")
        .when(col("revenue") >= 2000, "💪 Good")
        .otherwise("⚠️  Needs Attention")
    ).orderBy(col("revenue").desc())
    
    print("\n✅ Executive Sales Report:")
    df_report.select(
        "month", "category", "revenue", "num_orders", 
        "avg_order_value", "performance"
    ).show(truncate=False)
    
    # Summary stats
    print("\n📊 SUMMARY STATISTICS:")
    total_revenue = df.agg(spark_sum("revenue")).collect()[0][0]
    total_orders = df.count()
    print(f"   Total Revenue: ${total_revenue:,.2f}")
    print(f"   Total Orders:  {total_orders}")
    print(f"   Avg Order:     ${total_revenue/total_orders:,.2f}")
    
    return df_report


def main():
    """Run all aggregation examples."""
    print("\n" + "📊" * 40)
    print("INTERMEDIATE LESSON 4: AGGREGATION PATTERNS")
    print("📊" * 40)
    
    spark = create_spark_session()
    
    try:
        df = create_sales_data(spark)
        print("\n📊 Sales Data:")
        df.show()
        
        example_1_multiple_groupby(df)
        example_2_pivot_tables(df)
        example_3_rollup(df)
        example_4_cube(df)
        example_5_practical_report(df)
        
        print("\n" + "=" * 80)
        print("🎉 AGGREGATION PATTERNS MASTERED!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Multiple aggregations (count, sum, avg, min, max)")
        print("   2. Pivot tables (rows to columns)")
        print("   3. Rollup (hierarchical subtotals)")
        print("   4. Cube (all combinations)")
        print("   5. Building comprehensive reports")
        
        print("\n🏆 KEY PATTERNS:")
        print("   - groupBy().agg() for multiple metrics")
        print("   - pivot() for crosstab reports")
        print("   - rollup() for hierarchical totals")
        print("   - cube() for OLAP-style analytics")
        
        print("\n➡️  NEXT: Try intermediate/05_data_quality_framework.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
