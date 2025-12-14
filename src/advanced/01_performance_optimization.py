"""
================================================================================
ADVANCED 01: Performance Optimization
================================================================================

PURPOSE: Master Spark performance tuning - partitioning, caching, broadcast,
and Adaptive Query Execution (AQE).

WHAT YOU'LL LEARN:
- Partitioning strategies
- Caching and persistence
- Broadcast variables
- Adaptive Query Execution
- Performance benchmarking

WHY: Production Spark jobs must be optimized for speed and cost.

REAL-WORLD: Optimize ETL pipelines running on terabytes of data.

TIME: 45 minutes | DIFFICULTY: ⭐⭐⭐⭐⭐ (5/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, broadcast
import time


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Advanced01_Performance")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
        .getOrCreate()
    )


def create_large_dataset(spark, num_rows=100000):
    """Create large dataset for testing."""
    from pyspark.sql.functions import rand, randn
    
    df = spark.range(num_rows).select(
        col("id").alias("customer_id"),
        (rand() * 1000).cast("int").alias("order_value"),
        (rand() * 100).cast("int").alias("product_id"),
        (rand() * 10).cast("int").alias("region_id")
    )
    
    return df


def technique_1_partitioning(spark):
    """Optimize partitioning."""
    print("\n" + "=" * 80)
    print("TECHNIQUE 1: PARTITIONING")
    print("=" * 80)
    
    # Create data
    df = create_large_dataset(spark, 100000)
    
    print(f"\n📊 Default partitions: {df.rdd.getNumPartitions()}")
    
    # Bad: Too few partitions (underutilized)
    df_bad = df.coalesce(1)
    print(f"❌ Bad (1 partition):    {df_bad.rdd.getNumPartitions()}")
    
    # Bad: Too many partitions (overhead)
    df_bad2 = df.repartition(1000)
    print(f"❌ Bad (1000 partitions): {df_bad2.rdd.getNumPartitions()}")
    
    # Good: Right-sized (2-4x number of cores)
    df_good = df.repartition(8)
    print(f"✅ Good (8 partitions):   {df_good.rdd.getNumPartitions()}")
    
    # Best: Partition by key for joins
    df_best = df.repartition(8, "region_id")
    print(f"✅ Best (8, by region):   {df_best.rdd.getNumPartitions()}")
    
    print("\n💡 RULES:")
    print("   - 2-4x partitions per CPU core")
    print("   - Partition by join/group keys")
    print("   - Use coalesce() to reduce, repartition() to increase")
    print("   - Target 128MB-1GB per partition")


def technique_2_caching(spark):
    """Strategic caching."""
    print("\n" + "=" * 80)
    print("TECHNIQUE 2: CACHING & PERSISTENCE")
    print("=" * 80)
    
    df = create_large_dataset(spark, 50000)
    
    # Without caching
    start = time.time()
    count1 = df.filter(col("order_value") > 500).count()
    count2 = df.filter(col("order_value") > 700).count()
    time_no_cache = time.time() - start
    
    print(f"\n⏱️  Without cache: {time_no_cache:.2f}s (computes DF twice)")
    
    # With caching
    df_cached = df.cache()
    start = time.time()
    count1 = df_cached.filter(col("order_value") > 500).count()
    count2 = df_cached.filter(col("order_value") > 700).count()
    time_cached = time.time() - start
    
    print(f"⚡ With cache:    {time_cached:.2f}s (computes once)")
    print(f"🚀 Speedup:       {time_no_cache/time_cached:.1f}x")
    
    # Clean up
    df_cached.unpersist()
    
    print("\n💡 WHEN TO CACHE:")
    print("   ✅ Reused multiple times in job")
    print("   ✅ Expensive to recompute")
    print("   ✅ Fits in memory")
    print("   ❌ Used only once")
    print("   ❌ Larger than cluster memory")


def technique_3_broadcast_joins(spark):
    """Broadcast small tables."""
    print("\n" + "=" * 80)
    print("TECHNIQUE 3: BROADCAST JOINS")
    print("=" * 80)
    
    # Large table
    df_large = create_large_dataset(spark, 100000)
    
    # Small lookup table (regions)
    df_small = spark.createDataFrame([
        (0, "North"),
        (1, "South"),
        (2, "East"),
        (3, "West"),
        (4, "Central")
    ], ["region_id", "region_name"])
    
    # Regular join (shuffle both sides)
    start = time.time()
    df_regular = df_large.join(df_small, "region_id")
    result1 = df_regular.count()
    time_regular = time.time() - start
    
    print(f"\n⏱️  Regular join:    {time_regular:.2f}s (shuffle both sides)")
    
    # Broadcast join (no shuffle!)
    start = time.time()
    df_broadcast = df_large.join(broadcast(df_small), "region_id")
    result2 = df_broadcast.count()
    time_broadcast = time.time() - start
    
    print(f"⚡ Broadcast join:  {time_broadcast:.2f}s (no shuffle!)")
    print(f"🚀 Speedup:         {time_regular/time_broadcast:.1f}x")
    
    print("\n💡 BROADCAST WHEN:")
    print("   ✅ Small table < 10MB (configurable)")
    print("   ✅ Joining to large table")
    print("   ✅ Lookup/dimension tables")
    print("   ❌ Both tables are large")


def technique_4_adaptive_query_execution(spark):
    """Leverage AQE."""
    print("\n" + "=" * 80)
    print("TECHNIQUE 4: ADAPTIVE QUERY EXECUTION (AQE)")
    print("=" * 80)
    
    print("\n✅ AQE Features (enabled in this session):")
    print("   1. Dynamically coalesces partitions")
    print("   2. Dynamically switches join strategies")
    print("   3. Dynamically optimizes skew joins")
    
    df = create_large_dataset(spark, 100000)
    
    # AQE automatically optimizes this
    result = df.groupBy("region_id").agg(
        count("*").alias("num_orders"),
        spark_sum("order_value").alias("total_value")
    )
    
    print(f"\n📊 Before optimization: ~{df.rdd.getNumPartitions()} partitions")
    print(f"📊 After AQE:           ~{result.rdd.getNumPartitions()} partitions (coalesced!)")
    
    result.show()
    
    print("\n💡 AQE CONFIG:")
    print("   spark.sql.adaptive.enabled = true")
    print("   spark.sql.adaptive.coalescePartitions.enabled = true")
    print("   spark.sql.adaptive.skewJoin.enabled = true")


def technique_5_best_practices():
    """Performance best practices."""
    print("\n" + "=" * 80)
    print("TECHNIQUE 5: PERFORMANCE BEST PRACTICES")
    print("=" * 80)
    
    print("\n🏆 TOP 10 PERFORMANCE TIPS:")
    print("\n1. PARTITIONING:")
    print("   - Aim for 2-4x partitions per core")
    print("   - Partition by join/groupBy keys")
    print("   - Target 128MB-1GB per partition")
    
    print("\n2. CACHING:")
    print("   - Cache data used 2+ times")
    print("   - Use cache() for memory-only")
    print("   - Use persist(MEMORY_AND_DISK) for safety")
    print("   - Always unpersist() when done")
    
    print("\n3. BROADCAST:")
    print("   - Broadcast tables < 10MB")
    print("   - Use broadcast() hint explicitly")
    print("   - Avoid broadcasting large tables")
    
    print("\n4. DATA FORMATS:")
    print("   - Use Parquet (10-100x faster than CSV)")
    print("   - Use compression (snappy for speed, gzip for size)")
    print("   - Use partitioned writes for large datasets")
    
    print("\n5. OPERATIONS:")
    print("   - Filter early, before joins")
    print("   - Use built-in functions (not UDFs)")
    print("   - Avoid collect() on large data")
    print("   - Use coalesce() before write")
    
    print("\n6. JOINS:")
    print("   - Broadcast small tables")
    print("   - Partition on join keys before joining")
    print("   - Use appropriate join type")
    
    print("\n7. AGGREGATIONS:")
    print("   - Partition before groupBy on skewed keys")
    print("   - Use approx functions for estimates")
    print("   - Avoid distinct() on high-cardinality columns")
    
    print("\n8. MEMORY:")
    print("   - Set spark.executor.memory appropriately")
    print("   - Monitor GC time (should be < 10%)")
    print("   - Use --executor-memory wisely")
    
    print("\n9. MONITORING:")
    print("   - Check Spark UI (localhost:4040)")
    print("   - Watch for data skew")
    print("   - Monitor shuffle read/write")
    
    print("\n10. ADAPTIVE EXECUTION:")
    print("   - Enable AQE (Spark 3.0+)")
    print("   - Let Spark optimize dynamically")


def main():
    """Run all optimization techniques."""
    print("\n" + "⚡" * 40)
    print("ADVANCED LESSON 1: PERFORMANCE OPTIMIZATION")
    print("⚡" * 40)
    
    spark = create_spark_session()
    
    try:
        technique_1_partitioning(spark)
        technique_2_caching(spark)
        technique_3_broadcast_joins(spark)
        technique_4_adaptive_query_execution(spark)
        technique_5_best_practices()
        
        print("\n" + "=" * 80)
        print("🎉 PERFORMANCE OPTIMIZATION MASTERED!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Partitioning strategies")
        print("   2. Caching and persistence")
        print("   3. Broadcast joins")
        print("   4. Adaptive Query Execution")
        print("   5. Production best practices")
        
        print("\n�� KEY RULE: Measure, optimize, verify!")
        print("\n➡️  NEXT: Try advanced/02_streaming_introduction.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
