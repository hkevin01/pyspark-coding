"""
02_data_partitioning.py
=======================

Master data partitioning strategies for cluster computing.

Partitioning determines how data is distributed across cluster nodes.
Good partitioning = balanced workload = fast processing.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, spark_partition_id, count
import time


def create_spark():
    return SparkSession.builder \
        .appName("DataPartitioning") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


def demonstrate_default_partitioning(spark):
    """Show how Spark partitions data by default."""
    print("=" * 70)
    print("1. DEFAULT PARTITIONING")
    print("=" * 70)
    
    # Create large dataset
    df = spark.range(1, 1000001)  # 1 million rows
    
    print(f"✅ Created DataFrame with {df.count():,} rows")
    print(f"📦 Default partitions: {df.rdd.getNumPartitions()}")
    
    # Show partition distribution
    partition_df = df.withColumn("partition_id", spark_partition_id()) \
        .groupBy("partition_id") \
        .agg(count("*").alias("row_count"))
    
    print("\n📊 Rows per partition:")
    partition_df.orderBy("partition_id").show()
    
    # Rule of thumb
    num_cores = spark.sparkContext.defaultParallelism
    print(f"\n💡 Default parallelism: {num_cores} cores")
    print(f"💡 Recommended partitions: {num_cores * 2} to {num_cores * 4}")


def demonstrate_repartition_vs_coalesce(spark):
    """Compare repartition() vs coalesce()."""
    print("\n" + "=" * 70)
    print("2. REPARTITION VS COALESCE")
    print("=" * 70)
    
    # Start with 8 partitions
    df = spark.range(1, 100001).repartition(8)
    print(f"📦 Initial partitions: {df.rdd.getNumPartitions()}")
    
    # REPARTITION: Increase partitions (full shuffle)
    print("\n🔄 Using repartition(16) - FULL SHUFFLE:")
    start = time.time()
    df_repartitioned = df.repartition(16)
    df_repartitioned.write.mode("overwrite").format("noop").save()
    duration = time.time() - start
    print(f"   New partitions: {df_repartitioned.rdd.getNumPartitions()}")
    print(f"   Time: {duration:.3f}s")
    print(f"   ⚠️  Full shuffle: Data moved across all nodes")
    
    # COALESCE: Decrease partitions (optimized, no shuffle)
    print("\n🔄 Using coalesce(4) - NO SHUFFLE:")
    start = time.time()
    df_coalesced = df.coalesce(4)
    df_coalesced.write.mode("overwrite").format("noop").save()
    duration = time.time() - start
    print(f"   New partitions: {df_coalesced.rdd.getNumPartitions()}")
    print(f"   Time: {duration:.3f}s")
    print(f"   ✅ Optimized: Merges adjacent partitions, no shuffle")
    
    # When to use each
    print("\n📝 When to use:")
    print("   repartition(N): Increase parallelism, balance skewed data")
    print("   coalesce(N): Reduce output files, final stage optimization")


def demonstrate_partitioning_strategies(spark):
    """Show different partitioning strategies."""
    print("\n" + "=" * 70)
    print("3. PARTITIONING STRATEGIES")
    print("=" * 70)
    
    # Create dataset with categories
    data = [(i, f"category_{i % 10}", i * 100) 
            for i in range(1, 10001)]
    df = spark.createDataFrame(data, ["id", "category", "amount"])
    
    # Strategy 1: Random partitioning
    print("\n📊 Strategy 1: Random Partitioning")
    df_random = df.repartition(4)
    print(f"   Partitions: {df_random.rdd.getNumPartitions()}")
    print("   ✅ Good for: General balanced distribution")
    print("   ❌ Bad for: Operations on specific keys")
    
    # Strategy 2: Hash partitioning by key
    print("\n�� Strategy 2: Hash Partitioning by Key")
    df_hash = df.repartition(4, "category")
    print(f"   Partitions: {df_hash.rdd.getNumPartitions()}")
    print("   ✅ Good for: Group-by, joins on same key")
    print("   ✅ Same category always goes to same partition")
    
    # Show distribution
    partition_dist = df_hash.withColumn("partition_id", spark_partition_id()) \
        .groupBy("partition_id", "category") \
        .agg(count("*").alias("count"))
    
    print("\n   Distribution by partition and category:")
    partition_dist.orderBy("partition_id", "category").show(20)
    
    # Strategy 3: Range partitioning
    print("\n📊 Strategy 3: Range Partitioning")
    df_range = df.repartitionByRange(4, "amount")
    print(f"   Partitions: {df_range.rdd.getNumPartitions()}")
    print("   ✅ Good for: Sorting, range queries")
    print("   ✅ Sorted data distribution")


def demonstrate_partition_best_practices(spark):
    """Best practices for production."""
    print("\n" + "=" * 70)
    print("4. PARTITION BEST PRACTICES")
    print("=" * 70)
    
    # Create large dataset
    df = spark.range(1, 1000001)
    
    # Practice 1: Right-size partitions
    print("\n💡 Practice 1: Right-Size Partitions")
    print("   Rule: 128MB - 256MB per partition")
    print("   Formula: num_partitions = data_size_MB / 256")
    print("   Example: 10GB data → 10,000MB / 256 ≈ 40 partitions")
    
    # Practice 2: Partition before expensive operations
    print("\n💡 Practice 2: Partition Before Expensive Operations")
    
    # Bad: Join without partitioning
    df1 = spark.range(1, 100001).withColumnRenamed("id", "key")
    df2 = spark.range(1, 50001).withColumnRenamed("id", "key")
    
    print("\n   ❌ Bad: Direct join (unbalanced):")
    start = time.time()
    result_bad = df1.join(df2, "key")
    result_bad.count()
    bad_time = time.time() - start
    print(f"      Time: {bad_time:.3f}s")
    
    # Good: Partition before join
    print("\n   ✅ Good: Partition before join:")
    start = time.time()
    df1_partitioned = df1.repartition(4, "key")
    df2_partitioned = df2.repartition(4, "key")
    result_good = df1_partitioned.join(df2_partitioned, "key")
    result_good.count()
    good_time = time.time() - start
    print(f"      Time: {good_time:.3f}s")
    print(f"      Speedup: {bad_time / good_time:.2f}x")
    
    # Practice 3: Cache after repartitioning
    print("\n💡 Practice 3: Cache After Repartitioning")
    print("   If using DataFrame multiple times:")
    df_partitioned = df.repartition(8)
    df_partitioned.cache()
    df_partitioned.count()  # Materialize cache
    print("   ✅ Cached partitioned data")
    print("   ✅ Future operations use cached partitions")


def demonstrate_skew_handling(spark):
    """Handle data skew with salting."""
    print("\n" + "=" * 70)
    print("5. HANDLING DATA SKEW")
    print("=" * 70)
    
    # Create skewed dataset (90% of data in one category)
    skewed_data = []
    for i in range(1, 10001):
        if i < 9000:
            category = "popular"  # 90% of data
        else:
            category = f"cat_{i % 10}"
        skewed_data.append((i, category, i * 100))
    
    df_skewed = spark.createDataFrame(skewed_data, ["id", "category", "amount"])
    
    print("📊 Skewed Dataset:")
    df_skewed.groupBy("category").count().orderBy(col("count").desc()).show(5)
    
    # Problem: One partition gets 90% of data
    print("\n❌ Problem: Unbalanced partitions")
    df_skewed_partitioned = df_skewed.repartition(4, "category")
    partition_sizes = df_skewed_partitioned.withColumn("partition_id", spark_partition_id()) \
        .groupBy("partition_id").count()
    print("   Partition sizes:")
    partition_sizes.orderBy("partition_id").show()
    
    # Solution: Salting
    print("\n✅ Solution: Salting Technique")
    from pyspark.sql.functions import concat, lit, rand
    
    df_salted = df_skewed.withColumn(
        "salted_category",
        concat(col("category"), lit("_"), (rand() * 4).cast("int"))
    )
    
    df_salted_partitioned = df_salted.repartition(4, "salted_category")
    salted_partition_sizes = df_salted_partitioned.withColumn("partition_id", spark_partition_id()) \
        .groupBy("partition_id").count()
    
    print("   Salted partition sizes:")
    salted_partition_sizes.orderBy("partition_id").show()
    print("   ✅ More balanced distribution!")


def main():
    spark = create_spark()
    
    print("🔧 DATA PARTITIONING STRATEGIES")
    print("=" * 70)
    
    demonstrate_default_partitioning(spark)
    demonstrate_repartition_vs_coalesce(spark)
    demonstrate_partitioning_strategies(spark)
    demonstrate_partition_best_practices(spark)
    demonstrate_skew_handling(spark)
    
    print("\n" + "=" * 70)
    print("✅ PARTITIONING DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. repartition(): Full shuffle, increases/decreases partitions")
    print("   2. coalesce(): No shuffle, only decreases partitions")
    print("   3. Partition on join key for better performance")
    print("   4. Target 128-256MB per partition")
    print("   5. Use salting to handle data skew")
    print("   6. Cache after expensive repartitioning")
    
    spark.stop()


if __name__ == "__main__":
    main()
