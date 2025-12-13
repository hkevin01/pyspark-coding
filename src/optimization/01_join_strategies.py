"""
01_join_strategies.py
=====================

Spark Join Optimization Strategies

Demonstrates:
- 5 join strategies in Spark
- When to use each strategy
- Performance comparisons
- Optimization techniques
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, lit, concat
import time


def create_spark():
    """Create Spark session with optimization configs."""
    return SparkSession.builder \
        .appName("JoinOptimization") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", 10485760) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def create_sample_data(spark):
    """Create sample datasets for join demonstrations."""
    # Large fact table - Sales data
    sales_data = [(i, i % 100, i * 10, f"2024-01-{(i % 28) + 1:02d}") 
                  for i in range(100000)]
    sales = spark.createDataFrame(sales_data, ["sale_id", "product_id", "amount", "date"])
    
    # Small dimension table - Products
    product_data = [(i, f"Product_{i}", f"Category_{i % 5}") 
                    for i in range(100)]
    products = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])
    
    # Medium table - Customers
    customer_data = [(i, f"Customer_{i}", f"City_{i % 20}") 
                     for i in range(1000)]
    customers = spark.createDataFrame(customer_data, ["customer_id", "name", "city"])
    
    return sales, products, customers


def demonstrate_broadcast_hash_join(spark):
    """
    Strategy 1: Broadcast Hash Join
    Best for: Small table (<10MB) joining with large table
    """
    print("\n" + "=" * 80)
    print("STRATEGY 1: BROADCAST HASH JOIN")
    print("=" * 80)
    
    sales, products, _ = create_sample_data(spark)
    
    print("\n📊 Scenario: Large sales (100K rows) JOIN Small products (100 rows)")
    
    # WITHOUT broadcast (regular shuffle join)
    print("\n❌ WITHOUT BROADCAST (Shuffle Hash/Sort Merge Join):")
    start = time.time()
    regular_join = sales.join(products, "product_id")
    regular_join.explain()
    count1 = regular_join.count()
    time1 = time.time() - start
    print(f"   Time: {time1:.3f}s")
    print(f"   Result: {count1} rows")
    print("   Note: Both tables shuffled across network")
    
    # WITH broadcast
    print("\n✅ WITH BROADCAST:")
    start = time.time()
    broadcast_join = sales.join(broadcast(products), "product_id")
    broadcast_join.explain()
    count2 = broadcast_join.count()
    time2 = time.time() - start
    print(f"   Time: {time2:.3f}s")
    print(f"   Result: {count2} rows")
    print(f"   Speedup: {time1/time2:.2f}x faster!")
    print("   Note: Only products table copied to executors, no shuffle of sales")
    
    print("\n🎯 When to use:")
    print("   ✅ Small table (<10MB, configurable)")
    print("   ✅ Joining dimension table with fact table")
    print("   ✅ Repeated joins with same small table")
    
    print("\n⚠️  When NOT to use:")
    print("   ❌ Both tables are large")
    print("   ❌ Small table exceeds broadcast threshold")
    print("   ❌ Executors have limited memory")


def demonstrate_sort_merge_join(spark):
    """
    Strategy 2: Sort Merge Join
    Best for: Large table joining with large table
    """
    print("\n" + "=" * 80)
    print("STRATEGY 2: SORT MERGE JOIN")
    print("=" * 80)
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable broadcast
    
    # Create two large tables
    large1 = spark.range(0, 100000).toDF("id") \
        .withColumn("value1", col("id") * 2)
    
    large2 = spark.range(0, 100000).toDF("id") \
        .withColumn("value2", col("id") * 3)
    
    print("\n📊 Scenario: Large (100K) JOIN Large (100K)")
    
    start = time.time()
    result = large1.join(large2, "id")
    result.explain()
    count = result.count()
    exec_time = time.time() - start
    
    print(f"   Time: {exec_time:.3f}s")
    print(f"   Result: {count} rows")
    print("   Note: Both tables shuffled and sorted")
    
    print("\n🔍 How it works:")
    print("   1. Shuffle both tables by join key")
    print("   2. Sort both sides by join key")
    print("   3. Merge sorted partitions")
    
    print("\n🎯 When to use:")
    print("   ✅ Both tables are large")
    print("   ✅ Equi-joins (equality condition)")
    print("   ✅ Data already partitioned by join key")
    
    print("\n⚠️  Characteristics:")
    print("   📊 Two shuffles (one per table)")
    print("   📊 Sort operation on both sides")
    print("   📊 Default strategy for large-large joins")


def demonstrate_shuffle_hash_join(spark):
    """
    Strategy 3: Shuffle Hash Join
    Best for: When one side is smaller and sort isn't needed
    """
    print("\n" + "=" * 80)
    print("STRATEGY 3: SHUFFLE HASH JOIN")
    print("=" * 80)
    
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    large = spark.range(0, 50000).toDF("id").withColumn("value", col("id") * 2)
    medium = spark.range(0, 5000).toDF("id").withColumn("category", col("id") % 10)
    
    print("\n📊 Scenario: Large (50K) JOIN Medium (5K)")
    
    start = time.time()
    result = large.join(medium, "id")
    result.explain()
    count = result.count()
    exec_time = time.time() - start
    
    print(f"   Time: {exec_time:.3f}s")
    print(f"   Result: {count} rows")
    
    print("\n🔍 How it works:")
    print("   1. Shuffle both tables by join key")
    print("   2. Build hash table from smaller side")
    print("   3. Probe hash table with larger side")
    print("   4. No sort required")
    
    print("\n🎯 When to use:")
    print("   ✅ One side significantly smaller")
    print("   ✅ But too large to broadcast")
    print("   ✅ Want to avoid sort overhead")
    
    # Reset configs
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")


def demonstrate_broadcast_nested_loop_join(spark):
    """
    Strategy 4: Broadcast Nested Loop Join
    Used for: Non-equi joins, cross joins
    """
    print("\n" + "=" * 80)
    print("STRATEGY 4: BROADCAST NESTED LOOP JOIN")
    print("=" * 80)
    
    table1 = spark.range(0, 100).toDF("id").withColumn("value", col("id") * 2)
    table2 = spark.range(0, 50).toDF("id2").withColumn("threshold", col("id2") * 3)
    
    print("\n📊 Scenario: Non-equi join (value > threshold)")
    
    # Non-equi join (can't use hash-based strategies)
    start = time.time()
    result = table1.join(broadcast(table2), col("value") > col("threshold"))
    result.explain()
    count = result.count()
    exec_time = time.time() - start
    
    print(f"   Time: {exec_time:.3f}s")
    print(f"   Result: {count} rows")
    
    print("\n�� How it works:")
    print("   1. Broadcast smaller table")
    print("   2. Nested loop through both tables")
    print("   3. Check join condition for each pair")
    
    print("\n🎯 When used:")
    print("   ⚠️  Non-equi joins (>, <, !=)")
    print("   ⚠️  Cross joins")
    print("   ⚠️  Joins with complex conditions")
    
    print("\n⚠️  Performance:")
    print("   ❌ O(n*m) complexity")
    print("   ❌ Avoid for large tables")
    print("   ❌ Last resort when other strategies can't be used")


def demonstrate_cartesian_join(spark):
    """
    Strategy 5: Cartesian Join (Cross Join)
    Warning: Very expensive!
    """
    print("\n" + "=" * 80)
    print("STRATEGY 5: CARTESIAN JOIN (CROSS JOIN)")
    print("=" * 80)
    
    table1 = spark.range(0, 100).toDF("id1")
    table2 = spark.range(0, 100).toDF("id2")
    
    print("\n📊 Scenario: Cross join (every row with every row)")
    
    start = time.time()
    result = table1.crossJoin(table2)
    result.explain()
    count = result.count()
    exec_time = time.time() - start
    
    print(f"   Time: {exec_time:.3f}s")
    print(f"   Result: {count} rows (100 × 100 = 10,000)")
    print(f"   Warning: Result grows exponentially!")
    
    print("\n🔍 How it works:")
    print("   1. Pairs every row from table1 with every row from table2")
    print("   2. Result size = rows(table1) × rows(table2)")
    
    print("\n⚠️  DANGER:")
    print("   ❌ 1000 × 1000 = 1,000,000 rows")
    print("   ❌ 10K × 10K = 100,000,000 rows")
    print("   ❌ 100K × 100K = 10,000,000,000 rows (OOM!)")
    
    print("\n🎯 When used:")
    print("   ⚠️  Rarely intentional")
    print("   ⚠️  Often a mistake (missing join condition)")
    print("   ⚠️  Some ML algorithms (feature combinations)")


def demonstrate_join_optimization_tips(spark):
    """
    Best practices for join optimization.
    """
    print("\n" + "=" * 80)
    print("JOIN OPTIMIZATION BEST PRACTICES")
    print("=" * 80)
    
    sales, products, customers = create_sample_data(spark)
    
    print("\n1️⃣  FILTER BEFORE JOIN (Reduce data size)")
    print("   ❌ BAD:")
    print("      sales.join(products).filter(col('category') == 'Electronics')")
    print("   ✅ GOOD:")
    print("      filtered_products = products.filter(col('category') == 'Electronics')")
    print("      sales.join(broadcast(filtered_products))")
    
    # Demonstrate
    filtered_products = products.filter(col("category") == "Category_0")
    result = sales.join(broadcast(filtered_products), "product_id")
    print(f"      Result: {result.count()} rows (reduced dataset)")
    
    print("\n2️⃣  SELECT ONLY NEEDED COLUMNS")
    print("   ❌ BAD:")
    print("      sales.join(products).select('sale_id', 'product_name')")
    print("   ✅ GOOD:")
    print("      sales.select('sale_id', 'product_id')")
    print("           .join(products.select('product_id', 'product_name'))")
    
    print("\n3️⃣  REPARTITION BY JOIN KEY (For large-large joins)")
    print("   sales_repart = sales.repartition('product_id')")
    print("   products_repart = products.repartition('product_id')")
    print("   result = sales_repart.join(products_repart, 'product_id')")
    
    print("\n4️⃣  USE BROADCAST FOR DIMENSION TABLES")
    print("   Always broadcast small dimension tables (<10MB)")
    
    print("\n5️⃣  ENABLE ADAPTIVE QUERY EXECUTION (AQE)")
    print("   spark.conf.set('spark.sql.adaptive.enabled', 'true')")
    print("   spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')")
    
    print("\n6️⃣  TUNE SHUFFLE PARTITIONS")
    print("   Too many: overhead from small tasks")
    print("   Too few: not enough parallelism")
    print("   Rule of thumb: 2-3x number of cores")
    
    print("\n7️⃣  HANDLE DATA SKEW")
    print("   Use salting for skewed keys")
    print("   Enable AQE skew join optimization")
    print("   Consider splitting skewed keys separately")


def demonstrate_join_strategy_comparison(spark):
    """
    Compare all strategies side-by-side.
    """
    print("\n" + "=" * 80)
    print("JOIN STRATEGY COMPARISON")
    print("=" * 80)
    
    print("""
┌─────────────────────────┬───────────────┬───────────────┬─────────────────────────┐
│ Strategy                │ Use Case      │ Shuffle?      │ Performance             │
├─────────────────────────┼───────────────┼───────────────┼─────────────────────────┤
│ Broadcast Hash Join     │ Small + Large │ NO            │ ⭐⭐⭐⭐⭐ (Fastest)      │
│                         │ (<10MB)       │               │ No shuffle overhead     │
├─────────────────────────┼───────────────┼───────────────┼─────────────────────────┤
│ Sort Merge Join         │ Large + Large │ YES (both)    │ ⭐⭐⭐ (Good)            │
│                         │ Equi-join     │               │ Default for large joins │
├─────────────────────────┼───────────────┼───────────────┼─────────────────────────┤
│ Shuffle Hash Join       │ Medium + Large│ YES (both)    │ ⭐⭐⭐⭐ (Better)         │
│                         │ No sort needed│               │ Faster than Sort Merge  │
├─────────────────────────┼───────────────┼───────────────┼─────────────────────────┤
│ Broadcast Nested Loop   │ Non-equi join │ NO            │ ⭐⭐ (Slow)              │
│                         │ Small tables  │               │ O(n*m) complexity       │
├─────────────────────────┼───────────────┼───────────────┼─────────────────────────┤
│ Cartesian Join          │ Cross join    │ YES           │ ⭐ (Very Slow)           │
│                         │ (avoid!)      │               │ Result explodes         │
└─────────────────────────┴───────────────┴───────────────┴─────────────────────────┘
    """)
    
    print("\n🎯 DECISION FLOWCHART:")
    print("""
    Is one table < 10MB?
    ├─ YES → Broadcast Hash Join ✅
    └─ NO
       ├─ Is it an equi-join (=)?
       │  ├─ YES
       │  │  ├─ Both tables large? → Sort Merge Join
       │  │  └─ One medium? → Shuffle Hash Join
       │  └─ NO → Broadcast Nested Loop Join (careful!)
       └─ Is it a cross join? → Cartesian Join (avoid!)
    """)


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("SPARK JOIN OPTIMIZATION STRATEGIES")
    print("🎯" * 40)
    
    spark = create_spark()
    
    demonstrate_broadcast_hash_join(spark)
    demonstrate_sort_merge_join(spark)
    demonstrate_shuffle_hash_join(spark)
    demonstrate_broadcast_nested_loop_join(spark)
    demonstrate_cartesian_join(spark)
    demonstrate_join_optimization_tips(spark)
    demonstrate_join_strategy_comparison(spark)
    
    print("\n" + "=" * 80)
    print("✅ JOIN STRATEGIES COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. Broadcast small tables (<10MB)")
    print("   2. Sort Merge for large-large equi-joins")
    print("   3. Filter and select columns before join")
    print("   4. Use .explain() to verify join strategy")
    print("   5. Enable AQE for automatic optimization")
    print("   6. Monitor Spark UI for shuffle metrics")
    
    spark.stop()


if __name__ == "__main__":
    main()
