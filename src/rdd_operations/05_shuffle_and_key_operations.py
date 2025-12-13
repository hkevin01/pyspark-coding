"""
05_shuffle_and_key_operations.py
==================================

Shuffle, Combiner, and Key Aggregations

Topics covered:
- What is shuffle and why it's expensive
- Combiner optimization
- reduceByKey(): Aggregate by key with combiner
- groupByKey(): Group values by key (no combiner)
- aggregateByKey(): Flexible key aggregation
- combineByKey(): Most flexible key combiner
- foldByKey(): Fold with zero value per key
- countByKey(): Count occurrences per key
"""

from pyspark.sql import SparkSession


def explain_shuffle():
    """Explain shuffle operation."""
    print("=" * 80)
    print("UNDERSTANDING SHUFFLE")
    print("=" * 80)
    
    print("""
🔄 What is Shuffle?
   Shuffle is data movement across partitions/nodes in the cluster.
   
   Example: groupByKey() on key-value pairs
   
   BEFORE SHUFFLE (distributed across nodes):
   Node 1: [("a", 1), ("b", 2), ("a", 3)]
   Node 2: [("b", 4), ("c", 5), ("a", 6)]
   Node 3: [("c", 7), ("b", 8), ("a", 9)]
   
   AFTER SHUFFLE (grouped by key):
   Node 1: [("a", [1, 3, 6, 9])]
   Node 2: [("b", [2, 4, 8])]
   Node 3: [("c", [5, 7])]

⚠️  Why is Shuffle Expensive?
   1. Disk I/O: Write shuffle data to disk
   2. Network I/O: Transfer data between nodes
   3. Serialization/Deserialization
   4. Memory pressure
   
💡 Operations that cause shuffle:
   - groupByKey, reduceByKey, aggregateByKey
   - join, cogroup
   - distinct
   - repartition, coalesce (increase partitions)
   - sortByKey

⚡ Optimization: Use operations with COMBINERS
   - reduceByKey > groupByKey (has combiner!)
   - aggregateByKey, combineByKey (custom combiners)
   """)


def demonstrate_combiner():
    """Show combiner optimization."""
    print("\n" + "=" * 80)
    print("COMBINER OPTIMIZATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("CombinerDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Create word count data
    words = sc.parallelize([
        "spark", "hadoop", "spark", "spark", 
        "hadoop", "spark", "flink", "spark"
    ], 2)  # 2 partitions
    
    pairs = words.map(lambda x: (x, 1))
    
    print("\n❌ groupByKey (NO combiner):")
    print("   Partition 1: [(spark,1), (hadoop,1), (spark,1), (spark,1)]")
    print("   Partition 2: [(hadoop,1), (spark,1), (flink,1), (spark,1)]")
    print("   ")
    print("   SHUFFLE (transfers all pairs):")
    print("   → spark: [1,1,1,1,1]  (5 values transferred!)")
    print("   → hadoop: [1,1]       (2 values transferred)")
    print("   → flink: [1]          (1 value transferred)")
    print("   Total data shuffled: 8 values")
    
    result_group = pairs.groupByKey().mapValues(sum)
    print(f"\n   Result: {dict(result_group.collect())}")
    
    print("\n✅ reduceByKey (WITH combiner):")
    print("   Partition 1 (local combine): spark=3, hadoop=1")
    print("   Partition 2 (local combine): hadoop=1, spark=2, flink=1")
    print("   ")
    print("   SHUFFLE (transfers pre-aggregated):")
    print("   → spark: [3,2]     (2 values instead of 5!)")
    print("   → hadoop: [1,1]    (2 values)")
    print("   → flink: [1]       (1 value)")
    print("   Total data shuffled: 5 values (37.5% reduction!)")
    
    result_reduce = pairs.reduceByKey(lambda a, b: a + b)
    print(f"\n   Result: {dict(result_reduce.collect())}")
    
    print("\n💡 Always use reduceByKey over groupByKey when possible!")
    
    spark.stop()


def demonstrate_reduce_by_key():
    """reduceByKey: Merge values with combiner."""
    print("\n" + "=" * 80)
    print("REDUCEBYKEY TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("ReduceByKeyDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Word count
    words = sc.parallelize(["apple", "banana", "apple", "cherry", "banana", "apple"])
    word_pairs = words.map(lambda x: (x, 1))
    word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
    
    print("\n📝 Word counts:")
    for word, count in sorted(word_counts.collect()):
        print(f"   {word}: {count}")
    
    # Example 2: Sum by category
    sales = sc.parallelize([
        ("Electronics", 100),
        ("Clothing", 50),
        ("Electronics", 150),
        ("Books", 30),
        ("Clothing", 75),
        ("Electronics", 200)
    ])
    
    category_totals = sales.reduceByKey(lambda a, b: a + b)
    print("\n💰 Sales by category:")
    for category, total in sorted(category_totals.collect()):
        print(f"   {category}: ${total}")
    
    # Example 3: Find max by key
    scores = sc.parallelize([
        ("Alice", 85),
        ("Bob", 92),
        ("Alice", 90),
        ("Bob", 88),
        ("Alice", 95)
    ])
    
    max_scores = scores.reduceByKey(lambda a, b: max(a, b))
    print("\n🏆 Maximum scores:")
    for name, score in sorted(max_scores.collect()):
        print(f"   {name}: {score}")
    
    spark.stop()


def demonstrate_group_by_key():
    """groupByKey: Group all values by key."""
    print("\n" + "=" * 80)
    print("GROUPBYKEY TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("GroupByKeyDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example: Group transactions by customer
    transactions = sc.parallelize([
        ("Alice", 100),
        ("Bob", 50),
        ("Alice", 150),
        ("Charlie", 200),
        ("Bob", 75),
        ("Alice", 80)
    ])
    
    grouped = transactions.groupByKey()
    
    print("\n💳 Transactions by customer:")
    for customer, amounts in grouped.collect():
        amounts_list = list(amounts)
        total = sum(amounts_list)
        print(f"   {customer}: {amounts_list} → Total: ${total}")
    
    print("\n⚠️  Note: groupByKey transfers ALL values (expensive!)")
    print("💡 Better: Use reduceByKey, aggregateByKey, or combineByKey")
    
    spark.stop()


def demonstrate_aggregate_by_key():
    """aggregateByKey: Flexible aggregation with different types."""
    print("\n" + "=" * 80)
    print("AGGREGATEBYKEY TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("AggregateByKeyDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example: Calculate average by key
    scores = sc.parallelize([
        ("Math", 85),
        ("Science", 90),
        ("Math", 95),
        ("Science", 88),
        ("Math", 90)
    ])
    
    # Zero value: (sum, count)
    zero_value = (0, 0)
    
    # Seq function: combine value into accumulator
    def seq_func(acc, value):
        return (acc[0] + value, acc[1] + 1)
    
    # Comb function: combine accumulators
    def comb_func(acc1, acc2):
        return (acc1[0] + acc2[0], acc1[1] + acc2[1])
    
    aggregated = scores.aggregateByKey(zero_value, seq_func, comb_func)
    
    # Calculate averages
    averages = aggregated.mapValues(lambda x: x[0] / x[1])
    
    print("\n📊 Average scores:")
    for subject, avg in sorted(averages.collect()):
        print(f"   {subject}: {avg:.1f}")
    
    spark.stop()


def demonstrate_combine_by_key():
    """combineByKey: Most flexible key aggregation."""
    print("\n" + "=" * 80)
    print("COMBINEBYKEY TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("CombineByKeyDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example: Calculate average per key
    data = sc.parallelize([
        ("A", 10),
        ("B", 20),
        ("A", 15),
        ("B", 25),
        ("A", 20)
    ])
    
    # Three functions needed:
    # 1. createCombiner: Create accumulator for first value
    # 2. mergeValue: Add value to accumulator
    # 3. mergeCombiner: Merge two accumulators
    
    sum_count = data.combineByKey(
        lambda value: (value, 1),              # createCombiner
        lambda acc, value: (acc[0] + value, acc[1] + 1),  # mergeValue
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])  # mergeCombiner
    )
    
    averages = sum_count.mapValues(lambda x: x[0] / x[1])
    
    print("\n📊 Averages using combineByKey:")
    for key, avg in sorted(averages.collect()):
        print(f"   {key}: {avg:.1f}")
    
    spark.stop()


def demonstrate_count_by_key():
    """countByKey: Count occurrences of each key."""
    print("\n" + "=" * 80)
    print("COUNTBYKEY ACTION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("CountByKeyDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example: Count events by type
    events = sc.parallelize([
        ("login", "user1"),
        ("click", "user1"),
        ("login", "user2"),
        ("logout", "user1"),
        ("click", "user2"),
        ("login", "user3"),
        ("click", "user1")
    ])
    
    # countByKey returns a dictionary
    counts = events.countByKey()
    
    print("\n📊 Event counts:")
    for event_type, count in sorted(counts.items()):
        print(f"   {event_type}: {count}")
    
    print("\n💡 Note: countByKey returns dict to driver (use for small results)")
    
    spark.stop()


def main():
    """Run all demonstrations."""
    print("\n" + "🔑" * 40)
    print("SHUFFLE AND KEY AGGREGATIONS")
    print("🔑" * 40)
    
    explain_shuffle()
    demonstrate_combiner()
    demonstrate_reduce_by_key()
    demonstrate_group_by_key()
    demonstrate_aggregate_by_key()
    demonstrate_combine_by_key()
    demonstrate_count_by_key()
    
    print("\n" + "=" * 80)
    print("✅ KEY OPERATIONS COMPLETE")
    print("=" * 80)
    
    print("\n📚 Summary:")
    print("   • Shuffle is expensive (disk + network I/O)")
    print("   • Combiners reduce shuffle data (local pre-aggregation)")
    print("   • reduceByKey > groupByKey (always!)")
    print("   • aggregateByKey: When you need (sum, count) pattern")
    print("   • combineByKey: Most flexible, handle any aggregation")
    print("   • countByKey: Quick counts (returns to driver)")


if __name__ == "__main__":
    main()
