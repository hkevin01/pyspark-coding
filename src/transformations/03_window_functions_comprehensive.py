#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PYSPARK WINDOW FUNCTIONS - COMPREHENSIVE GUIDE
================================================================================

MODULE OVERVIEW:
----------------
Window functions in PySpark allow you to perform calculations across a set of
rows that are related to the current row, WITHOUT collapsing rows like groupBy().

This module covers:
• What Windows are and why they matter
• Partitioning and ordering
• Frame specifications (rowsBetween, rangeBetween)
• Ranking functions (row_number, rank, dense_rank)
• Analytical functions (lead, lag, first, last)
• Aggregate functions over windows (sum, avg, min, max)
• Real-world use cases (time-series, rankings, cumulative metrics)

WHAT IS A WINDOW?
-----------------

A Window is a specification that defines:
1. **Partitioning**: How to group rows (like groupBy, but keeps all rows)
2. **Ordering**: How to order rows within each partition
3. **Frame**: Which rows to include relative to current row

Think of it like looking through a sliding window on your data:

```
Without Window (groupBy):                With Window:
────────────────────────────              ────────────────────────────
dept   salary                             dept   salary   running_total
Sales  5000   ─┐                          Sales  5000     5000
Sales  6000    │ → Collapses to:          Sales  6000     11000
Sales  7000   ─┘    Sales: 3 rows         Sales  7000     18000
                                                           ↑
                                          Each row preserved!
```

use GROUP BY when you want summary tables, and WINDOW functions when you want contextual metrics

WINDOW COMPONENTS:
------------------

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, row_number

# 1. Partitioning: Group rows (like GROUP BY)
windowSpec = Window.partitionBy("department")

# 2. Ordering: Sort within each partition
windowSpec = Window.partitionBy("department").orderBy("salary")

# 3. Frame: Define range of rows to include
windowSpec = Window.partitionBy("department") \
                   .orderBy("salary") \
                   .rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Apply window function
df.withColumn("running_total", sum("salary").over(windowSpec))
```

FRAME SPECIFICATIONS:
---------------------

Frame defines which rows to include relative to current row:

```
rowsBetween(start, end):
• Window.unboundedPreceding: From first row in partition
• Window.currentRow: Current row
• Window.unboundedFollowing: To last row in partition
• -N: N rows before current
• N: N rows after current

Examples:
─────────
1. Running total (all previous rows + current):
   .rowsBetween(Window.unboundedPreceding, Window.currentRow)
   
   Row 1: [Row 1]
   Row 2: [Row 1, Row 2]
   Row 3: [Row 1, Row 2, Row 3]

2. Moving average (3-row window):
   .rowsBetween(-1, 1)
   
   Row 1: [Row 1, Row 2]           (can't go before first)
   Row 2: [Row 1, Row 2, Row 3]
   Row 3: [Row 2, Row 3, Row 4]

3. All rows in partition:
   .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
   
   All rows: [Row 1, Row 2, ..., Row N]
```

TYPES OF WINDOW FUNCTIONS:
---------------------------

┌──────────────────────────────────────────────────────────────────┐
│ 1. RANKING FUNCTIONS                                             │
├──────────────────────────────────────────────────────────────────┤
│ row_number()    Unique sequential number (1, 2, 3, ...)         │
│ rank()          Rank with gaps (1, 2, 2, 4, ...)                │
│ dense_rank()    Rank without gaps (1, 2, 2, 3, ...)             │
│ percent_rank()  Percentile rank                                  │
│ ntile(n)        Divide into n buckets                            │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│ 2. ANALYTICAL FUNCTIONS                                          │
├──────────────────────────────────────────────────────────────────┤
│ lag(col, n)     Value from n rows before                        │
│ lead(col, n)    Value from n rows after                         │
│ first(col)      First value in window                           │
│ last(col)       Last value in window                            │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│ 3. AGGREGATE FUNCTIONS (over window)                             │
├──────────────────────────────────────────────────────────────────┤
│ sum(col)        Sum over window                                  │
│ avg(col)        Average over window                              │
│ min(col)        Minimum over window                              │
│ max(col)        Maximum over window                              │
│ count(col)      Count over window                                │
│ stddev(col)     Standard deviation over window                   │
└──────────────────────────────────────────────────────────────────┘

WHY WINDOWS MATTER:
-------------------

✅ Advantages over groupBy():
• Preserve row granularity (don't collapse)
• Add computed columns without losing data
• Complex analytics (running totals, moving averages)
• Time-series analysis
• Ranking within groups
• Compare current row to neighbors

🎯 Use Cases:
• Time-series: Moving averages, cumulative sums
• Rankings: Top N per category
• Trend analysis: Compare to previous period
• Anomaly detection: Compare to window statistics
• Financial: Running balances, YTD totals
• Web analytics: Session analysis, conversion funnels

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - Window Functions Guide
UPDATED: December 2024
================================================================================
"""

import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    asc,
    avg,
    col,
    concat,
    count,
    datediff,
    dense_rank,
    desc,
    first,
    lag,
    last,
    lead,
    lit,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import ntile, percent_rank, rank
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import row_number, stddev
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_date, when
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_spark_session():
    """Create SparkSession for window functions examples."""
    return (
        SparkSession.builder.appName("WindowFunctionsComprehensive")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def example_1_ranking_functions(spark: SparkSession):
    """
    EXAMPLE 1: Ranking Functions

    Use Case: Rank employees by salary within each department.

    Ranking Functions Compared:
    • row_number(): Unique sequential (1, 2, 3, 4, ...)
    • rank(): Gaps for ties (1, 2, 2, 4, ...)
    • dense_rank(): No gaps (1, 2, 2, 3, ...)
    """
    print("=" * 80)
    print("EXAMPLE 1: RANKING FUNCTIONS")
    print("=" * 80)

    # Sample data: Employees with salaries
    data = [
        ("Sales", "Alice", 5000),
        ("Sales", "Bob", 6000),
        ("Sales", "Charlie", 6000),  # Tie!
        ("Sales", "Diana", 7000),
        ("Engineering", "Eve", 8000),
        ("Engineering", "Frank", 9000),
        ("Engineering", "Grace", 9000),  # Tie!
        ("Engineering", "Henry", 10000),
        ("HR", "Ivy", 4000),
        ("HR", "Jack", 4500),
    ]

    schema = StructType(
        [
            StructField("department", StringType(), False),
            StructField("name", StringType(), False),
            StructField("salary", IntegerType(), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    print("\n📊 Employee Data:")
    df.show()

    # Define window: Partition by department, order by salary descending
    windowSpec = Window.partitionBy("department").orderBy(desc("salary"))

    print("\n🪟 Window Specification:")
    print("   • Partition by: department (separate ranking per dept)")
    print("   • Order by: salary DESC (highest salary = rank 1)")
    print("   • Frame: Not specified (default for ranking)")

    # Apply all ranking functions
    df_ranked = (
        df.withColumn("row_number", row_number().over(windowSpec))
        .withColumn("rank", rank().over(windowSpec))
        .withColumn("dense_rank", dense_rank().over(windowSpec))
    )

    print("\n✅ Results with Ranking Functions:")
    df_ranked.orderBy("department", "row_number").show(truncate=False)

    print("\n💡 Key Differences:")
    print("   • row_number: Always unique (even for ties)")
    print("     - Engineering: Grace and Frank both $9000 → 2, 3")
    print("   • rank: Gaps for ties")
    print("     - Engineering: Grace and Frank both $9000 → 2, 2, then 4")
    print("   • dense_rank: No gaps")
    print("     - Engineering: Grace and Frank both $9000 → 2, 2, then 3")

    print("\n🎯 Use Cases:")
    print("   • row_number: Unique IDs, pagination")
    print("   • rank: Olympic-style rankings (ties share rank)")
    print("   • dense_rank: Consecutive ranks (no gaps)")

    return df_ranked


def example_2_analytical_functions(spark: SparkSession):
    """
    EXAMPLE 2: Analytical Functions (lag, lead, first, last)

    Use Case: Compare current month sales to previous/next month.
    Analyze trends and changes over time.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: ANALYTICAL FUNCTIONS (lag, lead, first, last)")
    print("=" * 80)

    # Sample data: Monthly sales by product
    data = [
        ("Product_A", "2024-01", 1000),
        ("Product_A", "2024-02", 1200),
        ("Product_A", "2024-03", 1100),
        ("Product_A", "2024-04", 1400),
        ("Product_A", "2024-05", 1600),
        ("Product_B", "2024-01", 500),
        ("Product_B", "2024-02", 550),
        ("Product_B", "2024-03", 600),
        ("Product_B", "2024-04", 580),
        ("Product_B", "2024-05", 620),
    ]

    schema = StructType(
        [
            StructField("product", StringType(), False),
            StructField("month", StringType(), False),
            StructField("sales", IntegerType(), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    print("\n📊 Monthly Sales Data:")
    df.show()

    # Define window: Partition by product, order by month
    windowSpec = Window.partitionBy("product").orderBy("month")

    print("\n🪟 Window Specification:")
    print("   • Partition by: product (separate timeline per product)")
    print("   • Order by: month (chronological order)")

    # Apply analytical functions
    df_analysis = (
        df.withColumn("prev_month_sales", lag("sales", 1).over(windowSpec))
        .withColumn("next_month_sales", lead("sales", 1).over(windowSpec))
        .withColumn("first_month_sales", first("sales").over(windowSpec))
        .withColumn("last_month_sales", last("sales").over(windowSpec))
        .withColumn("sales_change", col("sales") - col("prev_month_sales"))
        .withColumn(
            "sales_change_pct",
            spark_round(
                (col("sales") - col("prev_month_sales"))
                / col("prev_month_sales")
                * 100,
                2,
            ),
        )
    )

    print("\n✅ Results with Analytical Functions:")
    df_analysis.orderBy("product", "month").show(truncate=False)

    print("\n💡 Insights:")
    print("   • lag(sales, 1): Previous month's sales")
    print("     - Product_A Feb: prev = 1000 (Jan's value)")
    print("   • lead(sales, 1): Next month's sales")
    print("     - Product_A Feb: next = 1100 (Mar's value)")
    print("   • first(sales): First month in partition")
    print("     - All Product_A rows: first = 1000 (Jan)")
    print("   • last(sales): Last month in partition")
    print("     - All Product_A rows: last = 1600 (May)")
    print("   • sales_change: Current - Previous")
    print("     - Product_A Feb: 1200 - 1000 = 200 (20% growth)")

    print("\n🎯 Use Cases:")
    print("   • lag/lead: Time-series comparison, trend analysis")
    print("   • first/last: Compare to baseline or final value")
    print("   • Calculated changes: Growth rates, deltas")

    return df_analysis


def example_3_aggregate_functions_running_totals(spark: SparkSession):
    """
    EXAMPLE 3: Aggregate Functions with Frame Specification

    Use Case: Calculate running totals, moving averages.
    Essential for financial reporting, dashboards.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: AGGREGATE FUNCTIONS - RUNNING TOTALS & MOVING AVERAGES")
    print("=" * 80)

    # Sample data: Daily transactions
    data = [
        ("2024-01-01", "Store_A", 1000),
        ("2024-01-02", "Store_A", 1200),
        ("2024-01-03", "Store_A", 900),
        ("2024-01-04", "Store_A", 1500),
        ("2024-01-05", "Store_A", 1100),
        ("2024-01-06", "Store_A", 1300),
        ("2024-01-07", "Store_A", 1400),
        ("2024-01-01", "Store_B", 500),
        ("2024-01-02", "Store_B", 600),
        ("2024-01-03", "Store_B", 550),
        ("2024-01-04", "Store_B", 700),
        ("2024-01-05", "Store_B", 650),
        ("2024-01-06", "Store_B", 720),
        ("2024-01-07", "Store_B", 680),
    ]

    schema = StructType(
        [
            StructField("date", StringType(), False),
            StructField("store", StringType(), False),
            StructField("revenue", IntegerType(), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    print("\n📊 Daily Revenue Data:")
    df.show(7)

    # Window 1: Running total (unbounded preceding to current)
    windowSpec_running = (
        Window.partitionBy("store")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    print("\n🪟 Window Specification 1: Running Total")
    print("   • Partition by: store")
    print("   • Order by: date")
    print("   • Frame: unboundedPreceding to currentRow")
    print("     (All rows from start to current)")

    # Window 2: Moving average (3-day window)
    windowSpec_moving = Window.partitionBy("store").orderBy("date").rowsBetween(-1, 1)

    print("\n🪟 Window Specification 2: Moving Average (3-day)")
    print("   • Partition by: store")
    print("   • Order by: date")
    print("   • Frame: -1 to 1")
    print("     (Previous row, current row, next row)")

    # Window 3: Week total (all rows in partition)
    windowSpec_total = Window.partitionBy("store")

    print("\n🪟 Window Specification 3: Total")
    print("   • Partition by: store")
    print("   • No ordering needed")
    print("   • Frame: All rows in partition")

    # Apply window functions
    df_windowed = (
        df.withColumn("running_total", spark_sum("revenue").over(windowSpec_running))
        .withColumn(
            "moving_avg_3day", spark_round(avg("revenue").over(windowSpec_moving), 2)
        )
        .withColumn("week_total", spark_sum("revenue").over(windowSpec_total))
        .withColumn(
            "pct_of_week", spark_round(col("revenue") / col("week_total") * 100, 2)
        )
    )

    print("\n✅ Results with Window Aggregations:")
    df_windowed.filter(col("store") == "Store_A").orderBy("date").show(truncate=False)

    print("\n💡 Interpretation (Store_A):")
    print("   • Jan 1: running_total = 1000 (only day 1)")
    print("   • Jan 2: running_total = 2200 (day 1 + day 2)")
    print("   • Jan 3: running_total = 3100 (day 1 + day 2 + day 3)")
    print()
    print("   • Jan 1: moving_avg = 1100 (can't go before, so [1000, 1200]/2)")
    print("   • Jan 2: moving_avg = 1033.33 ([1000, 1200, 900]/3)")
    print("   • Jan 3: moving_avg = 1200 ([1200, 900, 1500]/3)")
    print()
    print("   • All days: week_total = 8400 (sum of all 7 days)")
    print("   • Jan 1: pct_of_week = 11.90% (1000 / 8400)")

    print("\n🎯 Use Cases:")
    print("   • Running totals: YTD revenue, cumulative metrics")
    print("   • Moving averages: Smoothing, trend detection")
    print("   • Percentage of total: Contribution analysis")

    return df_windowed


def example_4_advanced_time_series(spark: SparkSession):
    """
    EXAMPLE 4: Advanced Time-Series Analysis

    Use Case: Stock price analysis with multiple window operations.
    Demonstrates complex real-world analytics.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: ADVANCED TIME-SERIES ANALYSIS (Stock Prices)")
    print("=" * 80)

    # Sample data: Daily stock prices
    data = [
        ("AAPL", "2024-01-01", 180.0, 182.0, 179.0, 181.5, 1000000),
        ("AAPL", "2024-01-02", 181.5, 184.0, 180.5, 183.0, 1200000),
        ("AAPL", "2024-01-03", 183.0, 185.0, 182.0, 184.5, 1100000),
        ("AAPL", "2024-01-04", 184.5, 186.0, 183.0, 185.0, 1300000),
        ("AAPL", "2024-01-05", 185.0, 187.0, 184.0, 186.5, 1250000),
        ("GOOGL", "2024-01-01", 140.0, 142.0, 139.0, 141.0, 800000),
        ("GOOGL", "2024-01-02", 141.0, 143.0, 140.0, 142.5, 850000),
        ("GOOGL", "2024-01-03", 142.5, 144.0, 141.5, 143.0, 900000),
        ("GOOGL", "2024-01-04", 143.0, 145.0, 142.0, 144.0, 950000),
        ("GOOGL", "2024-01-05", 144.0, 146.0, 143.0, 145.5, 920000),
    ]

    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("date", StringType(), False),
            StructField("open", DoubleType(), False),
            StructField("high", DoubleType(), False),
            StructField("low", DoubleType(), False),
            StructField("close", DoubleType(), False),
            StructField("volume", IntegerType(), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    print("\n📊 Stock Price Data:")
    df.filter(col("symbol") == "AAPL").show()

    # Multiple window specifications
    windowSpec_date = Window.partitionBy("symbol").orderBy("date")

    windowSpec_moving_avg = (
        Window.partitionBy("symbol").orderBy("date").rowsBetween(-2, 0)
    )  # 3-day MA (2 prev + current)

    # Complex analysis
    df_analysis = (
        df.withColumn("prev_close", lag("close", 1).over(windowSpec_date))
        .withColumn(
            "daily_return",
            spark_round(
                (col("close") - col("prev_close")) / col("prev_close") * 100, 2
            ),
        )
        .withColumn("ma_3day", spark_round(avg("close").over(windowSpec_moving_avg), 2))
        .withColumn("volatility", spark_round(col("high") - col("low"), 2))
        .withColumn(
            "signal",
            when(col("close") > col("ma_3day"), "BUY")
            .when(col("close") < col("ma_3day"), "SELL")
            .otherwise("HOLD"),
        )
    )

    print("\n✅ Advanced Stock Analysis:")
    df_analysis.filter(col("symbol") == "AAPL").orderBy("date").show(truncate=False)

    print("\n💡 Metrics Explained:")
    print("   • prev_close: Previous day's closing price")
    print("   • daily_return: % change from previous close")
    print("   • ma_3day: 3-day moving average (smoothing)")
    print("   • volatility: Daily price range (high - low)")
    print("   • signal: Trading signal (close vs moving average)")

    print("\n🎯 Trading Strategy:")
    print("   • BUY: When price > 3-day moving average")
    print("   • SELL: When price < 3-day moving average")
    print("   • HOLD: When price = moving average")

    return df_analysis


def example_5_ntile_percentiles(spark: SparkSession):
    """
    EXAMPLE 5: Ntile and Percentiles

    Use Case: Divide customers into segments (quartiles, deciles).
    Useful for cohort analysis and segmentation.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: NTILE AND PERCENTILES (Customer Segmentation)")
    print("=" * 80)

    # Sample data: Customer lifetime value
    data = [
        ("C001", "Electronics", 12000),
        ("C002", "Electronics", 8500),
        ("C003", "Electronics", 15000),
        ("C004", "Electronics", 5000),
        ("C005", "Electronics", 20000),
        ("C006", "Electronics", 9500),
        ("C007", "Electronics", 11000),
        ("C008", "Electronics", 6500),
        ("C009", "Books", 2000),
        ("C010", "Books", 3500),
        ("C011", "Books", 1500),
        ("C012", "Books", 4500),
        ("C013", "Books", 2800),
        ("C014", "Books", 3200),
    ]

    schema = StructType(
        [
            StructField("customer_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("lifetime_value", IntegerType(), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    print("\n📊 Customer Lifetime Value:")
    df.show()

    # Window specification
    windowSpec = Window.partitionBy("category").orderBy(desc("lifetime_value"))

    # Apply ntile (quartiles)
    df_segmented = (
        df.withColumn("quartile", ntile(4).over(windowSpec))
        .withColumn("decile", ntile(10).over(windowSpec))
        .withColumn(
            "segment",
            when(col("quartile") == 1, "VIP")
            .when(col("quartile") == 2, "High Value")
            .when(col("quartile") == 3, "Medium Value")
            .otherwise("Low Value"),
        )
    )

    print("\n✅ Customer Segmentation:")
    df_segmented.orderBy("category", desc("lifetime_value")).show(truncate=False)

    print("\n💡 Segmentation Explained:")
    print("   • ntile(4) = Quartiles (Q1, Q2, Q3, Q4)")
    print("     - Electronics: 8 customers → 2 per quartile")
    print("     - Books: 6 customers → ~1.5 per quartile")
    print()
    print("   • Quartile 1 = Top 25% (VIP customers)")
    print("     - Electronics: $20,000 and $15,000")
    print("   • Quartile 4 = Bottom 25% (Low Value)")
    print("     - Electronics: $5,000 and $6,500")

    # Summary by segment
    print("\n�� Segment Summary:")
    df_segmented.groupBy("category", "segment").agg(
        count("*").alias("customer_count"),
        spark_round(avg("lifetime_value"), 2).alias("avg_value"),
        spark_sum("lifetime_value").alias("total_value"),
    ).orderBy("category", desc("avg_value")).show(truncate=False)

    print("\n🎯 Use Cases:")
    print("   • Customer segmentation (VIP, high, medium, low)")
    print("   • Cohort analysis (divide users into groups)")
    print("   • Performance ranking (top 10%, bottom 10%)")
    print("   • Resource allocation (focus on top quartile)")

    return df_segmented


def example_6_real_world_e_commerce(spark: SparkSession):
    """
    EXAMPLE 6: Real-World E-Commerce Analytics

    Comprehensive example combining multiple window functions.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 6: REAL-WORLD E-COMMERCE ANALYTICS")
    print("=" * 80)

    # Sample data: E-commerce transactions
    data = [
        ("U001", "2024-01-01", "P001", "Electronics", 500, 1),
        ("U001", "2024-01-05", "P002", "Books", 30, 2),
        ("U001", "2024-01-10", "P003", "Electronics", 800, 1),
        ("U001", "2024-01-15", "P004", "Clothing", 60, 3),
        ("U002", "2024-01-02", "P001", "Electronics", 500, 1),
        ("U002", "2024-01-08", "P005", "Books", 25, 1),
        ("U002", "2024-01-12", "P003", "Electronics", 800, 1),
        ("U003", "2024-01-03", "P002", "Books", 30, 1),
        ("U003", "2024-01-06", "P004", "Clothing", 60, 2),
        ("U003", "2024-01-09", "P001", "Electronics", 500, 1),
    ]

    schema = StructType(
        [
            StructField("user_id", StringType(), False),
            StructField("date", StringType(), False),
            StructField("product_id", StringType(), False),
            StructField("category", StringType(), False),
            StructField("amount", IntegerType(), False),
            StructField("quantity", IntegerType(), False),
        ]
    )

    df = spark.createDataFrame(data, schema=schema)

    print("\n📊 Transaction Data:")
    df.show()

    # Multiple windows for comprehensive analysis
    windowSpec_user = Window.partitionBy("user_id").orderBy("date")

    windowSpec_running = (
        Window.partitionBy("user_id")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Comprehensive analytics
    df_analytics = (
        df.withColumn("transaction_number", row_number().over(windowSpec_user))
        .withColumn("cumulative_spent", spark_sum("amount").over(windowSpec_running))
        .withColumn(
            "avg_order_value", spark_round(avg("amount").over(windowSpec_running), 2)
        )
        .withColumn(
            "days_since_last_purchase",
            datediff(col("date"), lag("date", 1).over(windowSpec_user)),
        )
        .withColumn(
            "category_change",
            when(
                col("category") != lag("category", 1).over(windowSpec_user), "Yes"
            ).otherwise("No"),
        )
        .withColumn(
            "customer_segment",
            when(col("cumulative_spent") >= 1000, "High Value")
            .when(col("cumulative_spent") >= 500, "Medium Value")
            .otherwise("Low Value"),
        )
    )

    print("\n✅ Comprehensive E-Commerce Analytics:")
    df_analytics.orderBy("user_id", "date").show(truncate=False)

    print("\n💡 Business Insights:")
    print("   • transaction_number: Sequential purchase count")
    print("   • cumulative_spent: Lifetime value (running total)")
    print("   • avg_order_value: Average spend per transaction")
    print("   • days_since_last_purchase: Purchase frequency")
    print("   • category_change: Cross-category shopping behavior")
    print("   • customer_segment: Dynamic segmentation by spend")

    print("\n🎯 Actionable Insights:")
    print("   • U001 becomes 'High Value' after 3rd purchase ($1330)")
    print("   • Purchase frequency varies: 4-5 days between orders")
    print("   • Category changes indicate cross-selling opportunities")

    return df_analytics


def main():
    """Main execution function."""
    print("\n" + "🔷 " * 40)
    print("PYSPARK WINDOW FUNCTIONS - COMPREHENSIVE GUIDE")
    print("🔷 " * 40)

    spark = create_spark_session()

    # Run all examples
    example_1_ranking_functions(spark)
    example_2_analytical_functions(spark)
    example_3_aggregate_functions_running_totals(spark)
    example_4_advanced_time_series(spark)
    example_5_ntile_percentiles(spark)
    example_6_real_world_e_commerce(spark)

    print("\n" + "=" * 80)
    print("✅ WINDOW FUNCTIONS GUIDE COMPLETE")
    print("=" * 80)

    print(
        """
📚 Key Takeaways:

1. WHAT: Window functions operate on a subset of rows related to current row
2. WHY: Preserve row granularity while adding computed columns
3. WHEN: Time-series, rankings, cumulative metrics, trend analysis

🪟 Window Components:
   • partitionBy(): Group rows (like GROUP BY but keeps all rows)
   • orderBy(): Sort within each partition
   • rowsBetween(): Define frame (which rows to include)

📊 Function Types:
   • Ranking: row_number(), rank(), dense_rank(), ntile()
   • Analytical: lag(), lead(), first(), last()
   • Aggregate: sum(), avg(), min(), max(), count()

🎯 Common Use Cases:
   ✅ Running totals (YTD revenue, cumulative metrics)
   ✅ Moving averages (3-day MA, smoothing)
   ✅ Rankings (top N per category)
   ✅ Trend analysis (compare to previous period)
   ✅ Customer segmentation (quartiles, deciles)
   ✅ Time-series analytics (stock prices, sensors)

💡 Windows vs GroupBy:
   • groupBy(): Collapses rows (aggregation)
   • Windows: Preserves rows (adds computed columns)
   
   Use groupBy when: You want summary statistics
   Use Windows when: You need row-level details + analytics

🚀 Performance Tips:
   • Order by indexed columns when possible
   • Use appropriate frame specification
   • Partition data to reduce window size
   • Consider caching if multiple window operations

🔗 Related Files:
   • src/transformations/01_basic_transformations.py
   • src/dataframe_etl/01_filtering_sorting.py
   • src/optimization/02_best_practices_comprehensive.py
    """
    )

    spark.stop()


if __name__ == "__main__":
    main()
