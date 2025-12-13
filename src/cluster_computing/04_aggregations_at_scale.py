#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
AGGREGATIONS AT SCALE - Distributed Analytics Across Billions of Rows
================================================================================

MODULE OVERVIEW:
----------------
Aggregations are fundamental to data analytics but can be expensive in
distributed systems because they require shuffling data across the network to
group rows by keys. Processing billions of rows requires understanding how
aggregations work under the hood and applying optimization techniques to
minimize shuffle and memory usage.

This module provides a comprehensive guide to:
1. Basic and advanced aggregation patterns
2. Window functions for analytics (ranking, moving averages)
3. Approximate aggregations for massive scale (HyperLogLog)
4. Multi-level and complex aggregations
5. Performance optimization strategies
6. Handling data skew in aggregations

PURPOSE:
--------
Learn to:
• Optimize group-by operations for large datasets
• Use window functions efficiently
• Apply approximate algorithms for billion-row datasets
• Handle data skew in aggregations
• Choose appropriate aggregation strategies
• Minimize shuffle and memory pressure

AGGREGATION FUNDAMENTALS:
-------------------------

What Happens During Aggregation:
┌─────────────────────────────────────────────────────────────────┐
│                   AGGREGATION WORKFLOW                          │
├─────────────────────────────────────────────────────────────────┤
│  STEP 1: MAP (Partial Aggregation on Each Partition)           │
│  ┌────────────┬────────────┬────────────┬────────────┐         │
│  │Partition 1 │Partition 2 │Partition 3 │Partition 4 │         │
│  │A: 100      │A: 150      │B: 200      │B: 250      │         │
│  │B: 120      │C: 130      │C: 180      │A: 170      │         │
│  │C: 110      │A: 140      │A: 160      │C: 190      │         │
│  └─────│──────┴─────│──────┴─────│──────┴─────│──────┘         │
│        │            │            │            │                 │
│  Partial sums:                                                  │
│  A: 100      A: 150+140  B: 200     B: 250                      │
│  B: 120      C: 130      C: 180     A: 170                      │
│  C: 110                  A: 160     C: 190                      │
│        │            │            │            │                 │
│  ══════════════════════════════════════════════════             │
│        SHUFFLE (Group by key)                                   │
│  ══════════════════════════════════════════════════             │
│        ↓            ↓            ↓                               │
│  STEP 2: REDUCE (Final Aggregation)                            │
│  ┌─────────────┬─────────────┬─────────────┐                   │
│  │  Key A      │  Key B      │  Key C      │                   │
│  │100+150+140  │120+200+250  │110+130+180  │                   │
│  │   +160+170  │             │   +190      │                   │
│  │   = 720     │   = 570     │   = 610     │                   │
│  └─────────────┴─────────────┴─────────────┘                   │
│                                                                 │
│  Result: A=720, B=570, C=610                                    │
└─────────────────────────────────────────────────────────────────┘

Aggregation Types by Shuffle:
┌──────────────────────────┬───────────────┬──────────────────────┐
│ Aggregation Type         │ Shuffle?      │ Performance          │
├──────────────────────────┼───────────────┼──────────────────────┤
│ Global (no groupBy)      │ ❌ No         │ ⭐⭐⭐⭐⭐ Fast     │
│   df.agg(sum("amount"))  │               │                      │
├──────────────────────────┼───────────────┼──────────────────────┤
│ GroupBy single key       │ ✅ Yes        │ ⭐⭐⭐⭐ Good       │
│   df.groupBy("category") │               │                      │
├──────────────────────────┼───────────────┼──────────────────────┤
│ GroupBy multiple keys    │ ✅ Yes        │ ⭐⭐⭐ Moderate     │
│   df.groupBy("cat","id") │               │                      │
├──────────────────────────┼───────────────┼──────────────────────┤
│ Window functions         │ ✅ Yes        │ ⭐⭐ Expensive      │
│   (with partitionBy)     │               │                      │
└──────────────────────────┴───────────────┴──────────────────────┘

AGGREGATION FUNCTIONS:
----------------------

Basic Aggregate Functions:
• count(*): Count rows
• sum(column): Sum values
• avg(column): Average
• min(column): Minimum value
• max(column): Maximum value
• stddev(column): Standard deviation
• variance(column): Variance

Collection Functions:
• collect_list(column): Array of all values (including duplicates)
• collect_set(column): Array of unique values
• ⚠️  Warning: Can cause OOM if too many values per group

Statistical Functions:
• countDistinct(column): Exact unique count (expensive)
• approx_count_distinct(column, rsd): Approximate unique count (HyperLogLog)
• percentile_approx(column, percentile): Approximate percentile

Advanced Functions:
• first(column): First value in group
• last(column): Last value in group
• corr(col1, col2): Correlation
• covar_pop(col1, col2): Covariance

WINDOW FUNCTIONS:
-----------------

Window Specification Components:
┌─────────────────────────────────────────────────────────────────┐
│  Window = partitionBy + orderBy + frame                        │
├─────────────────────────────────────────────────────────────────┤
│  from pyspark.sql import Window                                 │
│                                                                 │
│  window_spec = Window \\                                        │
│      .partitionBy("category")  ← Divide data into groups       │
│      .orderBy("date")          ← Order within each group       │
│      .rowsBetween(-6, 0)       ← Frame: last 7 rows            │
└─────────────────────────────────────────────────────────────────┘

Window Frame Types:
1. ROWS BETWEEN:
   - Physical rows (count-based)
   - Example: .rowsBetween(-6, 0)  # Last 7 rows
   
2. RANGE BETWEEN:
   - Logical range (value-based)
   - Example: .rangeBetween(-86400, 0)  # Last day in seconds

Frame Boundaries:
• Window.unboundedPreceding: Start of partition
• Window.unboundedFollowing: End of partition
• Window.currentRow: Current row
• Negative numbers: Rows before current
• Positive numbers: Rows after current

Window Function Categories:

1. Ranking Functions:
   ┌──────────────────┬───────────────────────────────────────┐
   │ Function         │ Behavior                              │
   ├──────────────────┼───────────────────────────────────────┤
   │ row_number()     │ 1, 2, 3, 4... (unique rank)           │
   │ rank()           │ 1, 2, 2, 4... (gaps for ties)         │
   │ dense_rank()     │ 1, 2, 2, 3... (no gaps)               │
   │ ntile(n)         │ Divide into n buckets                 │
   └──────────────────┴───────────────────────────────────────┘
   
   Example:
   Sales: [100, 90, 90, 70]
   row_number: [1, 2, 3, 4]
   rank: [1, 2, 2, 4]
   dense_rank: [1, 2, 2, 3]

2. Analytic Functions:
   • lag(column, offset): Previous row value
   • lead(column, offset): Next row value
   • first(column): First value in window
   • last(column): Last value in window
   
   Example (7-day moving average):
   window = Window.partitionBy("product") \\
       .orderBy("date") \\
       .rowsBetween(-6, 0)
   
   df.withColumn("ma_7day", avg("sales").over(window))

3. Aggregate Functions Over Windows:
   • sum(column).over(window): Running total
   • avg(column).over(window): Moving average
   • max(column).over(window): Running max
   • count(*).over(window): Running count

APPROXIMATE AGGREGATIONS:
-------------------------

Why Approximate?
┌─────────────────────────────────────────────────────────────────┐
│  Exact vs Approximate for 1 Billion Rows:                      │
├─────────────────────────────────────────────────────────────────┤
│  Exact countDistinct:                                           │
│  • Requires: Full shuffle of all data                           │
│  • Memory: Stores all unique values                             │
│  • Time: 10-30 minutes                                          │
│  • Accuracy: 100%                                               │
│                                                                 │
│  Approximate approx_count_distinct:                             │
│  • Requires: Minimal shuffle (small sketch)                     │
│  • Memory: ~12 KB per group (HyperLogLog)                       │
│  • Time: 30-60 seconds (10-30x faster!)                         │
│  • Accuracy: 99-99.9% (configurable)                            │
│                                                                 │
│  Trade-off: Slight accuracy loss for massive speedup            │
└─────────────────────────────────────────────────────────────────┘

HyperLogLog Algorithm:
approx_count_distinct(column, rsd=0.05)
• rsd: Relative Standard Deviation (error rate)
• rsd=0.05 → ±5% error
• rsd=0.01 → ±1% error (more accurate, slower)

Use Cases for Approximate Aggregations:
✅ Do use when:
• Dataset > 1 billion rows
• Exact count not critical (analytics, reporting)
• Performance more important than precision
• Interactive queries (dashboards)

❌ Don't use when:
• Financial calculations (need exact)
• Compliance requirements (auditing)
• Small datasets (< 1 million rows - exact is fast enough)
• Legal/regulatory reporting

Other Approximate Functions:
• percentile_approx(column, [0.25, 0.5, 0.75]): Approximate quartiles
• approxQuantile(column, [0.5, 0.95], 0.05): Approximate percentiles
• sampling: df.sample(fraction=0.01) for exploratory analysis

OPTIMIZATION TECHNIQUES:
------------------------

Technique 1: Pre-filter Data
❌ Bad (aggregate then filter):
df.groupBy("category").agg(sum("amount")).filter(col("sum(amount)") > 1000)

✅ Good (filter then aggregate):
df.filter(col("amount") > 0).groupBy("category").agg(sum("amount"))

Benefit: Less data to shuffle

Technique 2: Partition by Aggregation Key
❌ Bad (random partitions):
df.groupBy("customer_id").agg(sum("amount"))

✅ Good (pre-partition):
df.repartition("customer_id").groupBy("customer_id").agg(sum("amount"))

Benefit: Co-located data reduces shuffle

Technique 3: Use Combiners (Automatic in Spark SQL)
• Spark automatically does partial aggregation on each partition
• Reduces shuffle data size
• No action needed (handled by optimizer)

Technique 4: Cache Before Multiple Aggregations
✅ Best practice:
df_cached = df.cache()
df_cached.count()  # Materialize

agg1 = df_cached.groupBy("category").agg(sum("amount"))
agg2 = df_cached.groupBy("product").agg(avg("price"))

Technique 5: Adjust Shuffle Partitions
# Default: 200 partitions
spark.conf.set("spark.sql.shuffle.partitions", "100")

Rule: Set to 2-4x number of executor cores

DATA SKEW IN AGGREGATIONS:
--------------------------

Problem: Unbalanced Key Distribution
┌─────────────────────────────────────────────────────────────────┐
│              SKEWED AGGREGATION                                 │
├─────────────────────────────────────────────────────────────────┤
│  groupBy("user_id"):                                            │
│                                                                 │
│  ┌──┬──┬──┬────────────────────────────────────────────────┐   │
│  │P0│P1│P2│  Partition 3 (user_id="popular_user")         │   │
│  │  │  │  │  90% of data concentrated here                │   │
│  │  │  │  │  One executor overloaded                      │   │
│  └──┴──┴──┴────────────────────────────────────────────────┘   │
│  2m  2m  2m               1 hour (bottleneck!)                  │
│                                                                 │
│  Symptoms:                                                      │
│  • One task takes much longer than others                      │
│  • Executor OOM on specific partition                          │
│  • Shuffle read skew in Spark UI                               │
└─────────────────────────────────────────────────────────────────┘

Skew Mitigation: Salting
# SALTING: Two-stage aggregation to handle skewed keys
# =====================================================
# Problem: user_id="whale_user" has 10M transactions (90% of data)
#          Other users have 1-100 transactions each
#          Single partition gets overloaded with whale_user data
#
# Solution: Split whale_user into 10 sub-keys for parallel processing

from pyspark.sql.functions import concat, lit, rand

# STAGE 1: Add random salt to create sub-keys
df_salted = df.withColumn("salt", (rand() * 10).cast("int")) \\
    .withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))
# Example transformation:
#   user_id="whale_user" → "whale_user_0", "whale_user_1", ..., "whale_user_9"
#   Each sub-key gets ~1M transactions (10M / 10)
#   Now 10 partitions can process whale_user in parallel!

# STAGE 2: Partial aggregation (happens in parallel across 10 partitions)
partial = df_salted.groupBy("salted_key") \\
    .agg(sum("amount").alias("partial_sum"))
# Result: 10 partial sums for whale_user:
#   {"whale_user_0": 500K, "whale_user_1": 480K, ..., "whale_user_9": 510K}

# STAGE 3: Remove salt and final aggregation (combines partial sums)
final = partial.withColumn("user_id", 
    split(col("salted_key"), "_")[0]) \\
    .groupBy("user_id") \\
    .agg(sum("partial_sum").alias("total_amount"))

Skew Mitigation: Adaptive Execution (Spark 3+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Spark automatically handles skew

PERFORMANCE BENCHMARKS:
-----------------------

Typical Performance (1 Billion Rows):
┌──────────────────────────┬──────────────┬──────────────┬───────────┐
│ Operation                │ Naive        │ Optimized    │ Speedup   │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ GroupBy + Count          │ 15 min       │ 3 min        │ 5x        │
│ (with pre-filter)        │              │              │           │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ CountDistinct            │ 30 min       │ 2 min        │ 15x       │
│ (approx vs exact)        │              │              │           │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ Window Function          │ 45 min       │ 10 min       │ 4.5x      │
│ (with partition)         │              │              │           │
├──────────────────────────┼──────────────┼──────────────┼───────────┤
│ Skewed Aggregation       │ 2 hours      │ 20 min       │ 6x        │
│ (with salting)           │              │              │           │
└──────────────────────────┴──────────────┴──────────────┴───────────┘

BEST PRACTICES:
---------------

✅ DO:
1. Filter data before aggregation
2. Use approximate functions for billion-row datasets
3. Pre-partition on aggregation keys
4. Cache before multiple aggregations
5. Use salting for skewed keys
6. Monitor shuffle size in Spark UI
7. Use window functions instead of self-joins
8. Set appropriate spark.sql.shuffle.partitions

❌ DON'T:
1. Use collect_list/collect_set on high-cardinality keys
2. Use exact countDistinct on billion-row datasets unnecessarily
3. Create complex window frames without understanding cost
4. Ignore data skew
5. Use default 200 shuffle partitions for all data sizes
6. Use window functions when simple groupBy suffices

MONITORING:
-----------

Spark UI Metrics:
• SQL Tab: Check "shuffle read" and "shuffle write" sizes
• Stages Tab: Look for task duration skew
• Storage Tab: Check cache hit rate

Key Questions to Ask:
1. Is shuffle size reasonable for my data?
2. Are any tasks taking much longer (skew)?
3. Is memory pressure causing spills to disk?
4. Am I using the right number of partitions?

TARGET AUDIENCE:
----------------
• Data analysts building reports and dashboards
• Data engineers optimizing slow aggregations
• Anyone processing large-scale analytics workloads
• Teams experiencing aggregation bottlenecks

RELATED RESOURCES:
------------------
• cluster_computing/02_data_partitioning.py (partitioning strategies)
• cluster_computing/03_distributed_joins.py (join optimization)
• security/02_common_mistakes.py (#6 Wrong Shuffle Partitions)

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 2.0.0 - Comprehensive Aggregations Guide
UPDATED: 2024
================================================================================
"""

import time

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    collect_list,
    collect_set,
    count,
    countDistinct,
    dense_rank,
    first,
    lag,
    last,
    lead,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import rank, row_number
from pyspark.sql.functions import sum as spark_sum


def create_spark():
    return (
        SparkSession.builder.appName("AggregationsAtScale")
        .master("local[4]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def demonstrate_basic_aggregations(spark):
    """Basic aggregation patterns."""
    print("=" * 70)
    print("1. BASIC AGGREGATIONS")
    print("=" * 70)

    # Create sales dataset
    sales = (
        spark.range(1, 1000001)
        .toDF("transaction_id")
        .withColumn("product_id", (col("transaction_id") % 100).cast("int"))
        .withColumn("category", (col("product_id") % 10).cast("int"))
        .withColumn("amount", (col("transaction_id") % 1000).cast("double"))
        .withColumn("quantity", (col("transaction_id") % 10 + 1).cast("int"))
    )

    print(f"📊 Dataset: {sales.count():,} transactions")

    # Simple aggregations
    print("\n📈 Simple aggregations:")
    start = time.time()
    result = sales.agg(
        count("*").alias("total_transactions"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction"),
        spark_max("amount").alias("max_transaction"),
        spark_min("amount").alias("min_transaction"),
    )
    result.show()
    simple_time = time.time() - start
    print(f"   Time: {simple_time:.3f}s")

    # Group by aggregations
    print("\n📊 Group by category:")
    start = time.time()
    category_stats = (
        sales.groupBy("category")
        .agg(
            count("*").alias("transactions"),
            spark_sum("amount").alias("revenue"),
            avg("amount").alias("avg_amount"),
            spark_sum("quantity").alias("total_quantity"),
        )
        .orderBy(col("revenue").desc())
    )

    category_stats.show(10)
    groupby_time = time.time() - start
    print(f"   Time: {groupby_time:.3f}s")
    print(f"   ⚠️  Shuffle: Data grouped by category across nodes")


def demonstrate_window_functions(spark):
    """Window functions for advanced analytics."""
    print("\n" + "=" * 70)
    print("2. WINDOW FUNCTIONS")
    print("=" * 70)

    # Create sales data with dates
    from pyspark.sql.functions import current_date, date_add, lit

    sales = (
        spark.range(1, 10001)
        .toDF("transaction_id")
        .withColumn("product_id", (col("transaction_id") % 20).cast("int"))
        .withColumn("category", (col("product_id") % 5).cast("int"))
        .withColumn("amount", (col("transaction_id") % 1000 + 100).cast("double"))
        .withColumn(
            "date", date_add(current_date(), -(col("transaction_id") % 365).cast("int"))
        )
    )

    print(f"📊 Dataset: {sales.count():,} transactions")

    # Ranking within groups
    print("\n🏆 Ranking: Top products by revenue in each category")
    window_spec = Window.partitionBy("category").orderBy(col("revenue").desc())

    product_revenue = sales.groupBy("category", "product_id").agg(
        spark_sum("amount").alias("revenue")
    )

    ranked = product_revenue.withColumn("rank", row_number().over(window_spec)).filter(
        col("rank") <= 3
    )

    ranked.orderBy("category", "rank").show(15)

    # Running totals
    print("\n📈 Running totals: Cumulative revenue by date")
    window_running = (
        Window.partitionBy("category")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    daily_sales = (
        sales.groupBy("category", "date")
        .agg(spark_sum("amount").alias("daily_revenue"))
        .withColumn(
            "cumulative_revenue", spark_sum("daily_revenue").over(window_running)
        )
    )

    daily_sales.orderBy("category", "date").show(10)

    # Moving averages
    print("\n📊 Moving average: 7-day rolling average")
    window_moving = (
        Window.partitionBy("category").orderBy("date").rowsBetween(-6, 0)
    )  # Last 7 days including current

    with_ma = daily_sales.withColumn(
        "ma_7day", avg("daily_revenue").over(window_moving)
    )

    with_ma.filter(col("category") == 0).orderBy("date").show(10)

    print("\n💡 Window Function Types:")
    print("   - Ranking: row_number, rank, dense_rank")
    print("   - Analytics: lag, lead, first, last")
    print("   - Aggregates: sum, avg, max, min over window")
    print("   - Frame: ROWS/RANGE BETWEEN ... AND ...")


def demonstrate_approximate_aggregations(spark):
    """Use approximate aggregations for massive scale."""
    print("\n" + "=" * 70)
    print("3. APPROXIMATE AGGREGATIONS (For Massive Scale)")
    print("=" * 70)

    # Create large dataset
    large_data = (
        spark.range(1, 10000001)
        .toDF("user_id")
        .withColumn("page_id", (col("user_id") % 1000000).cast("int"))
    )

    print(f"📊 Dataset: {large_data.count():,} events")

    # Exact distinct count (expensive)
    print("\n🔍 Exact distinct count:")
    start = time.time()
    exact_count = large_data.select(countDistinct("page_id")).collect()[0][0]
    exact_time = time.time() - start
    print(f"   Distinct pages: {exact_count:,}")
    print(f"   Time: {exact_time:.3f}s")
    print(f"   ⚠️  Expensive: All data shuffled to compute exact count")

    # Approximate distinct count (fast)
    print("\n⚡ Approximate distinct count:")
    start = time.time()
    approx_count = large_data.select(
        approx_count_distinct("page_id", rsd=0.05)  # 5% error
    ).collect()[0][0]
    approx_time = time.time() - start
    print(f"   Distinct pages: {approx_count:,}")
    print(f"   Time: {approx_time:.3f}s")
    print(f"   Error: {abs(exact_count - approx_count) / exact_count * 100:.2f}%")
    print(f"   Speedup: {exact_time / approx_time:.2f}x")
    print(f"   ✅ Uses HyperLogLog algorithm")

    # Approximate quantiles
    print("\n📊 Approximate quantiles (percentiles):")
    start = time.time()
    quantiles = large_data.approxQuantile("user_id", [0.25, 0.5, 0.75, 0.95], 0.05)
    quantiles_time = time.time() - start
    print(f"   25th percentile: {quantiles[0]:,.0f}")
    print(f"   Median (50th): {quantiles[1]:,.0f}")
    print(f"   75th percentile: {quantiles[2]:,.0f}")
    print(f"   95th percentile: {quantiles[3]:,.0f}")
    print(f"   Time: {quantiles_time:.3f}s")


def demonstrate_complex_aggregations(spark):
    """Complex multi-level aggregations."""
    print("\n" + "=" * 70)
    print("4. COMPLEX MULTI-LEVEL AGGREGATIONS")
    print("=" * 70)

    # Create e-commerce dataset
    orders = (
        spark.range(1, 100001)
        .toDF("order_id")
        .withColumn("customer_id", (col("order_id") % 1000).cast("int"))
        .withColumn("product_id", (col("order_id") % 50).cast("int"))
        .withColumn("category", (col("product_id") % 10).cast("int"))
        .withColumn("amount", (col("order_id") % 500 + 10).cast("double"))
        .withColumn("quantity", (col("order_id") % 5 + 1).cast("int"))
    )

    print(f"📊 Dataset: {orders.count():,} orders")

    # Multiple aggregations
    print("\n📊 Customer lifetime value analysis:")
    customer_metrics = orders.groupBy("customer_id").agg(
        count("*").alias("total_orders"),
        spark_sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        spark_max("amount").alias("max_order"),
        countDistinct("product_id").alias("unique_products"),
        collect_set("category").alias("categories_purchased"),
    )

    customer_metrics.orderBy(col("total_spent").desc()).show(10, truncate=False)

    # Multi-level aggregation
    print("\n📈 Category performance by customer segment:")

    # Step 1: Customer segments
    customer_segments = customer_metrics.withColumn(
        "segment",
        when(col("total_spent") > 10000, "VIP")
        .when(col("total_spent") > 5000, "Gold")
        .when(col("total_spent") > 1000, "Silver")
        .otherwise("Bronze"),
    )

    # Step 2: Join back to orders
    from pyspark.sql.functions import when

    orders_with_segment = orders.join(
        customer_segments.select("customer_id", "segment"), "customer_id"
    )

    # Step 3: Aggregate by segment and category
    segment_category = (
        orders_with_segment.groupBy("segment", "category")
        .agg(
            count("*").alias("orders"),
            spark_sum("amount").alias("revenue"),
            avg("amount").alias("avg_order"),
        )
        .orderBy("segment", col("revenue").desc())
    )

    segment_category.show(20)


def demonstrate_optimization_techniques(spark):
    """Optimization techniques for aggregations."""
    print("\n" + "=" * 70)
    print("5. AGGREGATION OPTIMIZATION TECHNIQUES")
    print("=" * 70)

    # Create large dataset
    data = (
        spark.range(1, 5000001)
        .toDF("id")
        .withColumn("category", (col("id") % 100).cast("int"))
        .withColumn("value", (col("id") % 1000).cast("double"))
    )

    print(f"📊 Dataset: {data.count():,} rows")

    # Without pre-partitioning
    print("\n❌ Without pre-partitioning:")
    start = time.time()
    result1 = data.groupBy("category").agg(
        count("*").alias("count"), spark_sum("value").alias("total")
    )
    result1.write.mode("overwrite").format("noop").save()
    time1 = time.time() - start
    print(f"   Time: {time1:.3f}s")
    print(f"   Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

    # With optimized partition count
    print("\n✅ With optimized partition count:")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    start = time.time()
    result2 = data.groupBy("category").agg(
        count("*").alias("count"), spark_sum("value").alias("total")
    )
    result2.write.mode("overwrite").format("noop").save()
    time2 = time.time() - start
    print(f"   Time: {time2:.3f}s")
    print(f"   Speedup: {time1 / time2:.2f}x")
    print(f"   Shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

    # With pre-partitioning and caching
    print("\n✅ With pre-partitioning and caching:")
    start = time.time()
    data_partitioned = data.repartition(16, "category").cache()
    data_partitioned.count()  # Materialize cache

    result3 = data_partitioned.groupBy("category").agg(
        count("*").alias("count"), spark_sum("value").alias("total")
    )
    result3.write.mode("overwrite").format("noop").save()

    # Second aggregation reuses cache
    result4 = data_partitioned.groupBy("category").agg(
        avg("value").alias("average"), spark_max("value").alias("maximum")
    )
    result4.write.mode("overwrite").format("noop").save()

    time3 = time.time() - start
    print(f"   Time (2 aggregations): {time3:.3f}s")
    print(f"   ✅ Cached: Second aggregation reuses partitioned data")

    data_partitioned.unpersist()


def demonstrate_best_practices(spark):
    """Best practices summary."""
    print("\n" + "=" * 70)
    print("6. AGGREGATION BEST PRACTICES")
    print("=" * 70)

    print(
        """
🎯 Performance Optimization:

1. ✅ Adjust shuffle partitions
   spark.conf.set("spark.sql.shuffle.partitions", "200")
   Rule: ~128MB per partition after aggregation

2. ✅ Pre-partition on group-by key
   df = df.repartition(200, "key")
   df.cache()  # If multiple aggregations

3. ✅ Use approximate aggregations for massive scale
   approx_count_distinct(col, rsd=0.05)  # 5% error, much faster

4. ✅ Filter before aggregation
   df.filter(col("date") > "2024-01-01").groupBy(...)

5. ✅ Avoid collect_list/collect_set on large groups
   These collect all values into memory per group

6. ✅ Use window functions instead of self-joins
   Window functions are more efficient

⚠️  Common Mistakes:

1. ❌ Too many shuffle partitions (overhead)
   Default 200 may be too high for small data

2. ❌ Too few shuffle partitions (memory issues)
   Large groups won't fit in memory

3. ❌ Not caching for multiple aggregations
   Same shuffle repeated multiple times

4. ❌ Using exact distinct count unnecessarily
   approx_count_distinct is 10-100x faster

5. ❌ Collecting large groups to driver
   collect_list on skewed data causes OOM

📊 Shuffle Partition Sizing:

Data Size     Shuffle Partitions
----------    ------------------
< 1 GB        10-50
1-10 GB       50-200
10-100 GB     200-500
100GB-1TB     500-2000
> 1 TB        2000+

Formula: data_size_MB / 128
    """
    )


def main():
    spark = create_spark()

    print("📊 AGGREGATIONS AT SCALE")
    print("=" * 70)

    demonstrate_basic_aggregations(spark)
    demonstrate_window_functions(spark)
    demonstrate_approximate_aggregations(spark)
    demonstrate_complex_aggregations(spark)
    demonstrate_optimization_techniques(spark)
    demonstrate_best_practices(spark)

    print("\n" + "=" * 70)
    print("✅ AGGREGATIONS DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Adjust shuffle partitions based on data size")
    print("   2. Use approx_count_distinct for massive scale (10-100x faster)")
    print("   3. Pre-partition and cache for multiple aggregations")
    print("   4. Window functions avoid expensive self-joins")
    print("   5. Filter before aggregation to reduce shuffle")
    print("   6. Target ~128MB per partition after shuffle")

    spark.stop()


if __name__ == "__main__":
    main()
