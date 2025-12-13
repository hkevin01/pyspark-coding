"""
04_aggregations_at_scale.py
============================

Master aggregations across billions of rows in distributed clusters.

Aggregations require shuffling data to group by keys. Learn how to
optimize group-by, window functions, and complex aggregations.
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    row_number, rank, dense_rank, lag, lead, first, last,
    collect_list, collect_set, countDistinct, approx_count_distinct
)
import time


def create_spark():
    return SparkSession.builder \
        .appName("AggregationsAtScale") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def demonstrate_basic_aggregations(spark):
    """Basic aggregation patterns."""
    print("=" * 70)
    print("1. BASIC AGGREGATIONS")
    print("=" * 70)
    
    # Create sales dataset
    sales = spark.range(1, 1000001).toDF("transaction_id") \
        .withColumn("product_id", (col("transaction_id") % 100).cast("int")) \
        .withColumn("category", (col("product_id") % 10).cast("int")) \
        .withColumn("amount", (col("transaction_id") % 1000).cast("double")) \
        .withColumn("quantity", (col("transaction_id") % 10 + 1).cast("int"))
    
    print(f"📊 Dataset: {sales.count():,} transactions")
    
    # Simple aggregations
    print("\n📈 Simple aggregations:")
    start = time.time()
    result = sales.agg(
        count("*").alias("total_transactions"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction"),
        spark_max("amount").alias("max_transaction"),
        spark_min("amount").alias("min_transaction")
    )
    result.show()
    simple_time = time.time() - start
    print(f"   Time: {simple_time:.3f}s")
    
    # Group by aggregations
    print("\n📊 Group by category:")
    start = time.time()
    category_stats = sales.groupBy("category").agg(
        count("*").alias("transactions"),
        spark_sum("amount").alias("revenue"),
        avg("amount").alias("avg_amount"),
        spark_sum("quantity").alias("total_quantity")
    ).orderBy(col("revenue").desc())
    
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
    from pyspark.sql.functions import lit, date_add, current_date
    
    sales = spark.range(1, 10001).toDF("transaction_id") \
        .withColumn("product_id", (col("transaction_id") % 20).cast("int")) \
        .withColumn("category", (col("product_id") % 5).cast("int")) \
        .withColumn("amount", (col("transaction_id") % 1000 + 100).cast("double")) \
        .withColumn("date", date_add(current_date(), -(col("transaction_id") % 365).cast("int")))
    
    print(f"📊 Dataset: {sales.count():,} transactions")
    
    # Ranking within groups
    print("\n🏆 Ranking: Top products by revenue in each category")
    window_spec = Window.partitionBy("category").orderBy(col("revenue").desc())
    
    product_revenue = sales.groupBy("category", "product_id").agg(
        spark_sum("amount").alias("revenue")
    )
    
    ranked = product_revenue.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(col("rank") <= 3)
    
    ranked.orderBy("category", "rank").show(15)
    
    # Running totals
    print("\n📈 Running totals: Cumulative revenue by date")
    window_running = Window.partitionBy("category").orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    daily_sales = sales.groupBy("category", "date").agg(
        spark_sum("amount").alias("daily_revenue")
    ).withColumn(
        "cumulative_revenue",
        spark_sum("daily_revenue").over(window_running)
    )
    
    daily_sales.orderBy("category", "date").show(10)
    
    # Moving averages
    print("\n📊 Moving average: 7-day rolling average")
    window_moving = Window.partitionBy("category").orderBy("date") \
        .rowsBetween(-6, 0)  # Last 7 days including current
    
    with_ma = daily_sales.withColumn(
        "ma_7day",
        avg("daily_revenue").over(window_moving)
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
    large_data = spark.range(1, 10000001).toDF("user_id") \
        .withColumn("page_id", (col("user_id") % 1000000).cast("int"))
    
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
    orders = spark.range(1, 100001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("product_id", (col("order_id") % 50).cast("int")) \
        .withColumn("category", (col("product_id") % 10).cast("int")) \
        .withColumn("amount", (col("order_id") % 500 + 10).cast("double")) \
        .withColumn("quantity", (col("order_id") % 5 + 1).cast("int"))
    
    print(f"📊 Dataset: {orders.count():,} orders")
    
    # Multiple aggregations
    print("\n📊 Customer lifetime value analysis:")
    customer_metrics = orders.groupBy("customer_id").agg(
        count("*").alias("total_orders"),
        spark_sum("amount").alias("total_spent"),
        avg("amount").alias("avg_order_value"),
        spark_max("amount").alias("max_order"),
        countDistinct("product_id").alias("unique_products"),
        collect_set("category").alias("categories_purchased")
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
        .otherwise("Bronze")
    )
    
    # Step 2: Join back to orders
    from pyspark.sql.functions import when
    orders_with_segment = orders.join(
        customer_segments.select("customer_id", "segment"),
        "customer_id"
    )
    
    # Step 3: Aggregate by segment and category
    segment_category = orders_with_segment.groupBy("segment", "category").agg(
        count("*").alias("orders"),
        spark_sum("amount").alias("revenue"),
        avg("amount").alias("avg_order")
    ).orderBy("segment", col("revenue").desc())
    
    segment_category.show(20)


def demonstrate_optimization_techniques(spark):
    """Optimization techniques for aggregations."""
    print("\n" + "=" * 70)
    print("5. AGGREGATION OPTIMIZATION TECHNIQUES")
    print("=" * 70)
    
    # Create large dataset
    data = spark.range(1, 5000001).toDF("id") \
        .withColumn("category", (col("id") % 100).cast("int")) \
        .withColumn("value", (col("id") % 1000).cast("double"))
    
    print(f"📊 Dataset: {data.count():,} rows")
    
    # Without pre-partitioning
    print("\n❌ Without pre-partitioning:")
    start = time.time()
    result1 = data.groupBy("category").agg(
        count("*").alias("count"),
        spark_sum("value").alias("total")
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
        count("*").alias("count"),
        spark_sum("value").alias("total")
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
        count("*").alias("count"),
        spark_sum("value").alias("total")
    )
    result3.write.mode("overwrite").format("noop").save()
    
    # Second aggregation reuses cache
    result4 = data_partitioned.groupBy("category").agg(
        avg("value").alias("average"),
        spark_max("value").alias("maximum")
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
    
    print("""
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
    """)


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
