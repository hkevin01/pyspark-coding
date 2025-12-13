"""
03_distributed_joins.py
=======================

Master distributed joins across multiple cluster nodes.

Joins are expensive in distributed systems because they often require
shuffling data across the network. This example shows optimization techniques.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, count, lit
import time


def create_spark():
    return SparkSession.builder \
        .appName("DistributedJoins") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB \
        .getOrCreate()


def demonstrate_naive_join(spark):
    """Show the problem with naive joins."""
    print("=" * 70)
    print("1. NAIVE JOIN (Unoptimized)")
    print("=" * 70)
    
    # Create large datasets
    orders = spark.range(1, 100001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("amount", (col("order_id") * 10).cast("int"))
    
    customers = spark.range(1, 1001).toDF("customer_id") \
        .withColumn("name", lit("Customer"))
    
    print(f"📊 Orders: {orders.count():,} rows")
    print(f"📊 Customers: {customers.count():,} rows")
    
    # Naive join
    print("\n⚠️  Naive join (no optimization):")
    start = time.time()
    result = orders.join(customers, "customer_id")
    result.write.mode("overwrite").format("noop").save()
    naive_time = time.time() - start
    
    print(f"   Rows: {result.count():,}")
    print(f"   Time: {naive_time:.3f}s")
    print(f"   ⚠️  Full shuffle: Both tables shuffled across network")
    
    return naive_time


def demonstrate_broadcast_join(spark):
    """Optimize with broadcast join for small tables."""
    print("\n" + "=" * 70)
    print("2. BROADCAST JOIN (Optimized for Small Tables)")
    print("=" * 70)
    
    # Same datasets
    orders = spark.range(1, 100001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 1000).cast("int")) \
        .withColumn("amount", (col("order_id") * 10).cast("int"))
    
    customers = spark.range(1, 1001).toDF("customer_id") \
        .withColumn("name", lit("Customer"))
    
    # Broadcast join
    print("\n✅ Broadcast join (small table sent to all nodes):")
    start = time.time()
    result = orders.join(broadcast(customers), "customer_id")
    result.write.mode("overwrite").format("noop").save()
    broadcast_time = time.time() - start
    
    print(f"   Rows: {result.count():,}")
    print(f"   Time: {broadcast_time:.3f}s")
    print(f"   ✅ No shuffle: Customers broadcast to all executors")
    print(f"   ✅ Only orders partitioned")
    
    print("\n💡 When to use broadcast join:")
    print("   - Small table < 10MB (default threshold)")
    print("   - Small table fits in executor memory")
    print("   - Avoid shuffle for large table")
    
    return broadcast_time


def demonstrate_partition_join(spark):
    """Optimize with pre-partitioning on join key."""
    print("\n" + "=" * 70)
    print("3. PARTITIONED JOIN (Optimized for Large Tables)")
    print("=" * 70)
    
    # Larger datasets where broadcast won't work
    orders = spark.range(1, 500001).toDF("order_id") \
        .withColumn("customer_id", (col("order_id") % 5000).cast("int")) \
        .withColumn("amount", (col("order_id") * 10).cast("int"))
    
    customers = spark.range(1, 5001).toDF("customer_id") \
        .withColumn("name", lit("Customer")) \
        .withColumn("city", lit("City"))
    
    print(f"📊 Orders: {orders.count():,} rows")
    print(f"📊 Customers: {customers.count():,} rows")
    print("   (Too large for broadcast)")
    
    # Without pre-partitioning
    print("\n❌ Without pre-partitioning:")
    start = time.time()
    result_bad = orders.join(customers, "customer_id")
    result_bad.write.mode("overwrite").format("noop").save()
    bad_time = time.time() - start
    print(f"   Time: {bad_time:.3f}s")
    
    # With pre-partitioning
    print("\n✅ With pre-partitioning on join key:")
    start = time.time()
    orders_partitioned = orders.repartition(8, "customer_id")
    customers_partitioned = customers.repartition(8, "customer_id")
    result_good = orders_partitioned.join(customers_partitioned, "customer_id")
    result_good.write.mode("overwrite").format("noop").save()
    good_time = time.time() - start
    
    print(f"   Time: {good_time:.3f}s")
    print(f"   Speedup: {bad_time / good_time:.2f}x")
    print(f"   ✅ Co-located: Same keys on same nodes")
    print(f"   ✅ Reduced shuffle: Data already partitioned correctly")


def demonstrate_join_types(spark):
    """Compare different join types and their shuffle behavior."""
    print("\n" + "=" * 70)
    print("4. JOIN TYPES & SHUFFLE BEHAVIOR")
    print("=" * 70)
    
    # Create datasets
    df1 = spark.range(1, 10001).toDF("id") \
        .withColumn("value1", col("id") * 10)
    
    df2 = spark.range(5000, 15001).toDF("id") \
        .withColumn("value2", col("id") * 20)
    
    print(f"📊 DataFrame 1: {df1.count():,} rows (1-10,000)")
    print(f"📊 DataFrame 2: {df2.count():,} rows (5,000-15,000)")
    print(f"   Overlap: 5,000 rows (5,000-10,000)")
    
    # Inner join
    print("\n🔗 INNER JOIN:")
    inner_result = df1.join(df2, "id", "inner")
    inner_count = inner_result.count()
    print(f"   Result: {inner_count:,} rows (only overlapping)")
    print(f"   Shuffle: Both sides")
    
    # Left outer join
    print("\n🔗 LEFT OUTER JOIN:")
    left_result = df1.join(df2, "id", "left")
    left_count = left_result.count()
    print(f"   Result: {left_count:,} rows (all from left)")
    print(f"   Shuffle: Both sides")
    
    # Right outer join
    print("\n🔗 RIGHT OUTER JOIN:")
    right_result = df1.join(df2, "id", "right")
    right_count = right_result.count()
    print(f"   Result: {right_count:,} rows (all from right)")
    print(f"   Shuffle: Both sides")
    
    # Full outer join
    print("\n🔗 FULL OUTER JOIN:")
    full_result = df1.join(df2, "id", "full")
    full_count = full_result.count()
    print(f"   Result: {full_count:,} rows (all from both)")
    print(f"   Shuffle: Both sides (most expensive)")
    
    # Left semi join (filtering)
    print("\n🔗 LEFT SEMI JOIN:")
    semi_result = df1.join(df2, "id", "left_semi")
    semi_count = semi_result.count()
    print(f"   Result: {semi_count:,} rows (left rows that match)")
    print(f"   Columns: Only from left table")
    print(f"   Use case: Filtering without duplicating data")


def demonstrate_skewed_join(spark):
    """Handle data skew in joins."""
    print("\n" + "=" * 70)
    print("5. HANDLING SKEWED JOINS")
    print("=" * 70)
    
    # Create skewed dataset (90% have same key)
    skewed_data = []
    for i in range(1, 10001):
        if i < 9000:
            key = 1  # 90% of data
        else:
            key = i % 100
        skewed_data.append((i, key, i * 100))
    
    orders_skewed = spark.createDataFrame(skewed_data, ["order_id", "customer_id", "amount"])
    customers = spark.range(1, 101).toDF("customer_id") \
        .withColumn("name", lit("Customer"))
    
    print("📊 Skewed dataset:")
    orders_skewed.groupBy("customer_id").count() \
        .orderBy(col("count").desc()).show(5)
    
    # Problem: Skewed join
    print("\n❌ Problem: Skewed join (one partition overloaded):")
    start = time.time()
    result_skewed = orders_skewed.join(customers, "customer_id")
    result_skewed.write.mode("overwrite").format("noop").save()
    skewed_time = time.time() - start
    print(f"   Time: {skewed_time:.3f}s")
    print(f"   ⚠️  One partition processes 90% of data")
    
    # Solution: Salting + broadcast
    print("\n✅ Solution 1: Broadcast join (if customers small):")
    start = time.time()
    result_broadcast = orders_skewed.join(broadcast(customers), "customer_id")
    result_broadcast.write.mode("overwrite").format("noop").save()
    broadcast_time = time.time() - start
    print(f"   Time: {broadcast_time:.3f}s")
    print(f"   Speedup: {skewed_time / broadcast_time:.2f}x")
    print(f"   ✅ No shuffle, no skew issue")
    
    # Solution: Salting (for large dimension tables)
    print("\n✅ Solution 2: Salting (if both tables large):")
    from pyspark.sql.functions import concat, rand, lit as spark_lit, explode, array
    
    # Add salt to fact table
    orders_salted = orders_skewed.withColumn(
        "salt",
        (rand() * 10).cast("int")
    ).withColumn(
        "salted_key",
        concat(col("customer_id").cast("string"), spark_lit("_"), col("salt").cast("string"))
    )
    
    # Explode dimension table with all salt values
    customers_exploded = customers.withColumn(
        "salt",
        explode(array([lit(i) for i in range(10)]))
    ).withColumn(
        "salted_key",
        concat(col("customer_id").cast("string"), spark_lit("_"), col("salt").cast("string"))
    )
    
    start = time.time()
    result_salted = orders_salted.join(customers_exploded, "salted_key")
    result_salted.write.mode("overwrite").format("noop").save()
    salted_time = time.time() - start
    
    print(f"   Time: {salted_time:.3f}s")
    print(f"   ✅ Distributed: Skewed key split across 10 partitions")
    print(f"   ✅ Balanced: Each partition processes ~10% of data")


def demonstrate_join_best_practices(spark):
    """Summary of join best practices."""
    print("\n" + "=" * 70)
    print("6. JOIN BEST PRACTICES")
    print("=" * 70)
    
    print("""
📋 Decision Tree for Joins:

┌─ Small table (< 10MB)?
│  └─ YES → Use broadcast join ✅
│     from pyspark.sql.functions import broadcast
│     result = large.join(broadcast(small), "key")
│
└─ Both tables large?
   │
   ├─ Same partition count & key?
   │  └─ YES → Already optimized ✅
   │
   └─ Different partitions?
      └─ Repartition both on join key
         df1 = df1.repartition(N, "key")
         df2 = df2.repartition(N, "key")
         result = df1.join(df2, "key")

🎯 Optimization Checklist:

1. ✅ Filter before join (reduce data size)
2. ✅ Use broadcast for small tables (< 10MB)
3. ✅ Partition both tables on join key
4. ✅ Use same partition count for both tables
5. ✅ Cache tables if joining multiple times
6. ✅ Use left semi join for filtering
7. ✅ Handle skew with salting or broadcast
8. ✅ Monitor Spark UI for shuffle size

⚠️  Common Mistakes:

1. ❌ Joining without filtering first
2. ❌ Not broadcasting small tables
3. ❌ Different partition counts for join tables
4. ❌ Ignoring data skew
5. ❌ Using full outer join when not needed
6. ❌ Multiple joins without caching
    """)


def main():
    spark = create_spark()
    
    print("🔗 DISTRIBUTED JOINS IN PYSPARK")
    print("=" * 70)
    
    # 1. Naive join
    naive_time = demonstrate_naive_join(spark)
    
    # 2. Broadcast join
    broadcast_time = demonstrate_broadcast_join(spark)
    
    # 3. Partitioned join
    demonstrate_partition_join(spark)
    
    # 4. Join types
    demonstrate_join_types(spark)
    
    # 5. Skewed joins
    demonstrate_skewed_join(spark)
    
    # 6. Best practices
    demonstrate_join_best_practices(spark)
    
    print("\n" + "=" * 70)
    print("✅ DISTRIBUTED JOINS DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print(f"   1. Broadcast join: {naive_time / broadcast_time:.2f}x faster for small tables")
    print("   2. Partition on join key for large-large joins")
    print("   3. Handle skew with salting or broadcast")
    print("   4. Filter before join to reduce shuffle")
    print("   5. Monitor Spark UI shuffle metrics")
    print("   6. Cache if joining same tables multiple times")
    
    spark.stop()


if __name__ == "__main__":
    main()
