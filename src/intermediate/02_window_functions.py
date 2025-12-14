"""
================================================================================
INTERMEDIATE 02: Window Functions
================================================================================

PURPOSE: Master window functions - the most powerful feature for analytics.
Perform calculations across rows without grouping.

WHAT YOU'LL LEARN:
- Ranking functions (row_number, rank, dense_rank)
- Lag and lead (previous/next values)
- Running totals and cumulative sums
- Moving averages
- Partition and ordering

WHY: Window functions solve complex problems that groupBy can't handle.

REAL-WORLD: Sales leaderboards, time series analysis, customer cohorts.

TIME: 40 minutes | DIFFICULTY: ⭐⭐⭐⭐☆ (4/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, lag, lead,
    sum as spark_sum, avg, round as spark_round, count
)


def create_spark_session():
    return SparkSession.builder.appName("Intermediate02_Windows").getOrCreate()


def create_sales_data(spark):
    """Create sample sales data."""
    data = [
        ("Alice", "2024-01", "Electronics", 1000),
        ("Alice", "2024-02", "Electronics", 1500),
        ("Alice", "2024-03", "Electronics", 1200),
        ("Bob", "2024-01", "Electronics", 800),
        ("Bob", "2024-02", "Electronics", 900),
        ("Bob", "2024-03", "Electronics", 1100),
        ("Charlie", "2024-01", "Furniture", 500),
        ("Charlie", "2024-02", "Furniture", 600),
        ("Charlie", "2024-03", "Furniture", 700),
        ("Diana", "2024-01", "Furniture", 450),
        ("Diana", "2024-02", "Furniture", 550),
        ("Diana", "2024-03", "Furniture", 650),
    ]
    
    df = spark.createDataFrame(data, ["salesperson", "month", "category", "sales"])
    return df


def example_1_ranking(df):
    """Ranking functions."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: RANKING FUNCTIONS")
    print("=" * 80)
    
    # Window: partition by category, order by sales desc
    window_spec = Window.partitionBy("category").orderBy(col("sales").desc())
    
    df_ranked = df.withColumn("row_num", row_number().over(window_spec)) \
                  .withColumn("rank", rank().over(window_spec)) \
                  .withColumn("dense_rank", dense_rank().over(window_spec))
    
    print("\n✅ Rankings by Category:")
    df_ranked.orderBy("category", "rank").show()
    
    print("\n💡 Differences:")
    print("   - row_number: Unique (1,2,3,4...)")
    print("   - rank: Gaps after ties (1,2,2,4...)")
    print("   - dense_rank: No gaps (1,2,2,3...)")
    
    return df_ranked


def example_2_lag_lead(df):
    """Previous/next values."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: LAG AND LEAD")
    print("=" * 80)
    
    # Window: partition by salesperson, order by month
    window_spec = Window.partitionBy("salesperson").orderBy("month")
    
    df_with_prev_next = df.withColumn("prev_month_sales", lag("sales", 1).over(window_spec)) \
                          .withColumn("next_month_sales", lead("sales", 1).over(window_spec)) \
                          .withColumn("month_over_month_change",
                                     col("sales") - lag("sales", 1).over(window_spec))
    
    print("\n✅ Month-over-Month Comparison:")
    df_with_prev_next.orderBy("salesperson", "month").show()
    
    print("\n💡 Use Cases:")
    print("   - Compare to previous period")
    print("   - Calculate growth rates")
    print("   - Identify trends")
    
    return df_with_prev_next


def example_3_running_totals(df):
    """Cumulative sums."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: RUNNING TOTALS")
    print("=" * 80)
    
    # Window: partition by salesperson, order by month, unbounded preceding
    window_spec = Window.partitionBy("salesperson").orderBy("month") \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    df_running = df.withColumn("running_total", spark_sum("sales").over(window_spec)) \
                   .withColumn("running_avg", spark_round(avg("sales").over(window_spec), 2))
    
    print("\n✅ Running Totals by Salesperson:")
    df_running.orderBy("salesperson", "month").show()
    
    print("\n💡 Pattern: rowsBetween(unboundedPreceding, currentRow)")
    
    return df_running


def example_4_moving_averages(df):
    """Rolling/moving averages."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: MOVING AVERAGES")
    print("=" * 80)
    
    # 2-month moving average
    window_spec = Window.partitionBy("salesperson").orderBy("month") \
                        .rowsBetween(-1, 0)  # Previous + current row
    
    df_moving = df.withColumn("moving_avg_2mo", 
                             spark_round(avg("sales").over(window_spec), 2))
    
    # 3-month moving average
    window_spec_3 = Window.partitionBy("salesperson").orderBy("month") \
                          .rowsBetween(-2, 0)
    
    df_moving = df_moving.withColumn("moving_avg_3mo",
                                     spark_round(avg("sales").over(window_spec_3), 2))
    
    print("\n✅ Moving Averages:")
    df_moving.orderBy("salesperson", "month").show()
    
    print("\n💡 Pattern: rowsBetween(-N, 0) for N-period average")
    
    return df_moving


def example_5_complex_analytics(df):
    """Combine multiple window functions."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: COMPLEX ANALYTICS")
    print("=" * 80)
    
    # Multiple windows
    window_category = Window.partitionBy("category").orderBy(col("sales").desc())
    window_person = Window.partitionBy("salesperson").orderBy("month")
    window_running = Window.partitionBy("salesperson").orderBy("month") \
                           .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    df_analytics = df \
        .withColumn("category_rank", row_number().over(window_category)) \
        .withColumn("mom_change", col("sales") - lag("sales", 1).over(window_person)) \
        .withColumn("ytd_total", spark_sum("sales").over(window_running)) \
        .withColumn("is_top_performer",
                   when(col("category_rank") == 1, "🏆 #1").otherwise(""))
    
    print("\n✅ Complete Sales Dashboard:")
    df_analytics.orderBy("salesperson", "month").show(truncate=False)
    
    print("\n💡 Real-world use: Executive dashboards!")
    
    return df_analytics


def main():
    """Run all window function examples."""
    print("\n" + "🪟" * 40)
    print("INTERMEDIATE LESSON 2: WINDOW FUNCTIONS")
    print("🪟" * 40)
    
    spark = create_spark_session()
    
    try:
        df = create_sales_data(spark)
        print("\n📊 Sales Data:")
        df.orderBy("salesperson", "month").show()
        
        example_1_ranking(df)
        example_2_lag_lead(df)
        example_3_running_totals(df)
        example_4_moving_averages(df)
        example_5_complex_analytics(df)
        
        print("\n" + "=" * 80)
        print("🎉 WINDOW FUNCTIONS MASTERED!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Ranking: row_number, rank, dense_rank")
        print("   2. Lag/Lead: Compare to previous/next rows")
        print("   3. Running totals: Cumulative sums")
        print("   4. Moving averages: N-period rolling calcs")
        print("   5. Complex analytics: Combine multiple windows")
        
        print("\n🏆 POWER USER TIP:")
        print("   Window functions are 10x more powerful than groupBy!")
        
        print("\n➡️  NEXT: Try intermediate/03_user_defined_functions.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
