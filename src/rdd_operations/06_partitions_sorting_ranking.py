"""
06_partitions_sorting_ranking.py
==================================

Partitioning, Sorting, Ranking, Set Operations, and Sampling

Topics:
- What is a Partition
- repartition(): Change partition count (shuffle)
- coalesce(): Reduce partitions (avoid shuffle)
- repartitionAndSortWithinPartitions()
- sortByKey(), sortBy()
- top(), takeOrdered()
- Set operations: union, intersection, subtract
- sample(), takeSample()
"""

from pyspark.sql import SparkSession


def explain_partitions():
    """Explain partitions."""
    print("=" * 80)
    print("UNDERSTANDING PARTITIONS")
    print("=" * 80)
    
    print("""
📦 What is a Partition?
   A partition is a logical division of data that can be processed independently.
   
   Example: RDD with 100 elements, 4 partitions
   
   ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
   │ Partition 0 │ │ Partition 1 │ │ Partition 2 │ │ Partition 3 │
   │ Elements    │ │ Elements    │ │ Elements    │ │ Elements    │
   │ 0-24        │ │ 25-49       │ │ 50-74       │ │ 75-99       │
   └─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘
   
🔹 Why Partitions Matter:
   1. Parallelism: Each partition = 1 task
   2. Memory: Partition must fit in executor memory
   3. Shuffle: Partitioning determines data movement
   
💡 Rules of Thumb:
   - Partitions = 2-4x number of cores
   - Each partition: 100MB - 200MB ideal
   - Too few: Underutilization
   - Too many: Scheduling overhead
   
🎯 Default Partitioning:
   - Input files: One partition per HDFS block (128MB)
   - parallelize(): Number of cores in cluster
   - After shuffle: spark.sql.shuffle.partitions (default: 200)
    """)


def demonstrate_partitions():
    """Show partition operations."""
    print("\n" + "=" * 80)
    print("PARTITION OPERATIONS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("PartitionsDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Create RDD with specified partitions
    data = sc.parallelize(range(100), 4)
    
    print(f"\n📊 RDD with 100 elements")
    print(f"📊 Number of partitions: {data.getNumPartitions()}")
    
    # View partition contents
    def show_partition(index, iterator):
        elements = list(iterator)
        print(f"   Partition {index}: {len(elements)} elements, first={elements[0] if elements else 'empty'}, last={elements[-1] if elements else 'empty'}")
        return elements
    
    print("\n📦 Partition distribution:")
    data.mapPartitionsWithIndex(show_partition).collect()
    
    # glom(): Convert each partition to array
    partitions_as_lists = data.glom().collect()
    print(f"\n📦 First 3 elements of each partition:")
    for i, partition in enumerate(partitions_as_lists):
        print(f"   Partition {i}: {partition[:3]}...")
    
    spark.stop()


def demonstrate_repartition_vs_coalesce():
    """repartition vs coalesce."""
    print("\n" + "=" * 80)
    print("REPARTITION VS COALESCE")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("RepartitionDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Start with 8 partitions
    data = sc.parallelize(range(100), 8)
    print(f"\n📊 Original: {data.getNumPartitions()} partitions")
    
    # repartition: Always causes full shuffle
    print("\n🔄 repartition(4) - FULL SHUFFLE:")
    repartitioned = data.repartition(4)
    print(f"   Partitions: {repartitioned.getNumPartitions()}")
    print("   ⚠️  Causes full shuffle (expensive!)")
    
    # coalesce: Avoids shuffle when reducing
    print("\n🔄 coalesce(4) - NO SHUFFLE (when reducing):")
    coalesced = data.coalesce(4)
    print(f"   Partitions: {coalesced.getNumPartitions()}")
    print("   ✅ No shuffle, just merges partitions")
    
    # coalesce with shuffle
    print("\n🔄 coalesce(4, shuffle=True) - WITH SHUFFLE:")
    coalesced_shuffle = data.coalesce(4, shuffle=True)
    print(f"   Partitions: {coalesced_shuffle.getNumPartitions()}")
    
    # Increasing partitions
    print("\n🔄 coalesce(16) - INCREASING (requires shuffle):")
    increased = data.coalesce(16, shuffle=True)
    print(f"   Partitions: {increased.getNumPartitions()}")
    print("   ⚠️  Must use shuffle=True to increase")
    
    print("\n📚 When to use what:")
    print("   • repartition(): Increase partitions or rebalance")
    print("   • coalesce(n): Reduce partitions (no shuffle)")
    print("   • coalesce(n, shuffle=True): Increase partitions")
    
    spark.stop()


def demonstrate_repartition_and_sort():
    """repartitionAndSortWithinPartitions: Efficient sorting."""
    print("\n" + "=" * 80)
    print("REPARTITION AND SORT WITHIN PARTITIONS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("RepartitionSortDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Create data with keys
    data = sc.parallelize([
        ("B", 3), ("A", 1), ("C", 2),
        ("B", 1), ("A", 2), ("C", 3),
        ("A", 3), ("B", 2), ("C", 1)
    ])
    
    print("\n📊 Original data:")
    for item in data.collect():
        print(f"   {item}")
    
    # repartitionAndSortWithinPartitions: Efficient!
    # Combines repartition + sort in single pass
    sorted_data = data.repartitionAndSortWithinPartitions(3)
    
    print(f"\n🔄 After repartitionAndSortWithinPartitions({3}):")
    print(f"   Partitions: {sorted_data.getNumPartitions()}")
    
    # View sorted partitions
    partitions = sorted_data.glom().collect()
    for i, partition in enumerate(partitions):
        print(f"\n   Partition {i} (sorted):")
        for item in partition:
            print(f"      {item}")
    
    print("\n💡 More efficient than: repartition().sortByKey()")
    print("   Sorts DURING shuffle instead of after")
    
    spark.stop()


def demonstrate_sorting():
    """sortBy and sortByKey."""
    print("\n" + "=" * 80)
    print("SORTING OPERATIONS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("SortingDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: sortByKey
    pairs = sc.parallelize([
        ("banana", 2),
        ("apple", 5),
        ("cherry", 3),
        ("date", 1)
    ])
    
    sorted_asc = pairs.sortByKey()
    sorted_desc = pairs.sortByKey(ascending=False)
    
    print("\n🔤 sortByKey():")
    print(f"   Ascending: {sorted_asc.collect()}")
    print(f"   Descending: {sorted_desc.collect()}")
    
    # Example 2: sortBy with custom key
    numbers = sc.parallelize([5, 2, 8, 1, 9, 3])
    
    # Sort by value itself
    sorted_numbers = numbers.sortBy(lambda x: x)
    print(f"\n🔢 sortBy(value): {sorted_numbers.collect()}")
    
    # Sort by absolute value
    mixed = sc.parallelize([-5, 2, -8, 1, 9, -3])
    sorted_abs = mixed.sortBy(lambda x: abs(x))
    print(f"🔢 sortBy(abs): {sorted_abs.collect()}")
    
    # Sort complex objects
    students = sc.parallelize([
        ("Alice", 85),
        ("Bob", 92),
        ("Charlie", 78),
        ("David", 95)
    ])
    
    sorted_by_score = students.sortBy(lambda x: x[1], ascending=False)
    print(f"\n🎓 Top students:")
    for name, score in sorted_by_score.collect():
        print(f"   {name}: {score}")
    
    spark.stop()


def demonstrate_ranking():
    """top and takeOrdered for ranking."""
    print("\n" + "=" * 80)
    print("RANKING OPERATIONS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("RankingDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    numbers = sc.parallelize([45, 12, 78, 23, 89, 34, 67, 56])
    
    # top(n): Get top n elements (largest)
    top_3 = numbers.top(3)
    print(f"\n🏆 top(3): {top_3}")
    
    # takeOrdered(n): Get smallest n elements
    bottom_3 = numbers.takeOrdered(3)
    print(f"🏆 takeOrdered(3): {bottom_3}")
    
    # takeOrdered with custom key
    students = sc.parallelize([
        ("Alice", 85),
        ("Bob", 92),
        ("Charlie", 78),
        ("David", 95),
        ("Eve", 88)
    ])
    
    # Top 3 by score
    top_students = students.takeOrdered(3, key=lambda x: -x[1])  # Negative for descending
    print(f"\n🎓 Top 3 students:")
    for name, score in top_students:
        print(f"   {name}: {score}")
    
    # Bottom 3 by score
    bottom_students = students.takeOrdered(3, key=lambda x: x[1])
    print(f"\n📚 Students needing help:")
    for name, score in bottom_students:
        print(f"   {name}: {score}")
    
    spark.stop()


def demonstrate_set_operations():
    """union, intersection, subtract, distinct."""
    print("\n" + "=" * 80)
    print("SET OPERATIONS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("SetDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    set_a = sc.parallelize([1, 2, 3, 4, 5])
    set_b = sc.parallelize([4, 5, 6, 7, 8])
    
    print(f"\n📊 Set A: {set_a.collect()}")
    print(f"📊 Set B: {set_b.collect()}")
    
    # Union: A ∪ B (all elements, keeps duplicates)
    union_result = set_a.union(set_b)
    print(f"\n∪ Union: {union_result.collect()}")
    print(f"∪ Union distinct: {union_result.distinct().collect()}")
    
    # Intersection: A ∩ B (common elements)
    intersection_result = set_a.intersection(set_b)
    print(f"\n∩ Intersection: {intersection_result.collect()}")
    
    # Subtract: A - B (in A but not in B)
    subtract_result = set_a.subtract(set_b)
    print(f"\n− Subtract (A-B): {subtract_result.collect()}")
    
    # Cartesian: A × B (all pairs)
    cartesian_result = sc.parallelize([1, 2]).cartesian(sc.parallelize(['a', 'b']))
    print(f"\n× Cartesian (small example): {cartesian_result.collect()}")
    
    spark.stop()


def demonstrate_sampling():
    """sample and takeSample."""
    print("\n" + "=" * 80)
    print("SAMPLING OPERATIONS")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("SamplingDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    data = sc.parallelize(range(1, 101))  # 1 to 100
    
    # sample(withReplacement, fraction, seed): Random sample (lazy)
    print("\n🎲 sample(withReplacement=False, fraction=0.1):")
    sample_10pct = data.sample(False, 0.1, seed=42)
    print(f"   Sample size: {sample_10pct.count()}")
    print(f"   Sample: {sorted(sample_10pct.collect())}")
    
    # With replacement
    print("\n🎲 sample(withReplacement=True, fraction=0.1):")
    sample_replacement = data.sample(True, 0.1, seed=42)
    print(f"   Sample (may have duplicates): {sorted(sample_replacement.collect())}")
    
    # takeSample(withReplacement, num, seed): Exact count (action)
    print("\n🎲 takeSample(withReplacement=False, num=10):")
    exact_sample = data.takeSample(False, 10, seed=42)
    print(f"   Exact 10 elements: {sorted(exact_sample)}")
    
    # Stratified sampling (for key-value pairs)
    categories = sc.parallelize([
        ("A", 1), ("B", 2), ("A", 3), ("C", 4),
        ("B", 5), ("A", 6), ("C", 7), ("B", 8)
    ])
    
    # Sample different fractions per key
    fractions = {"A": 0.5, "B": 0.3, "C": 1.0}  # 50% A, 30% B, 100% C
    stratified = categories.sampleByKey(False, fractions, seed=42)
    
    print("\n🎯 Stratified sampling by key:")
    for key, value in sorted(stratified.collect()):
        print(f"   {key}: {value}")
    
    spark.stop()


def main():
    """Run all demonstrations."""
    print("\n" + "📦" * 40)
    print("PARTITIONS, SORTING, RANKING, SETS & SAMPLING")
    print("📦" * 40)
    
    explain_partitions()
    demonstrate_partitions()
    demonstrate_repartition_vs_coalesce()
    demonstrate_repartition_and_sort()
    demonstrate_sorting()
    demonstrate_ranking()
    demonstrate_set_operations()
    demonstrate_sampling()
    
    print("\n" + "=" * 80)
    print("✅ ALL OPERATIONS COMPLETE")
    print("=" * 80)
    
    print("\n📚 Quick Reference:")
    print("\n   Partitioning:")
    print("      • repartition(n): Change partitions (shuffle)")
    print("      • coalesce(n): Reduce partitions (no shuffle)")
    print("      • repartitionAndSortWithinPartitions: Efficient sorted repartition")
    print("\n   Sorting:")
    print("      • sortByKey(): Sort key-value pairs")
    print("      • sortBy(func): Sort by custom key")
    print("\n   Ranking:")
    print("      • top(n): Largest n elements")
    print("      • takeOrdered(n): Smallest n elements")
    print("\n   Sets:")
    print("      • union, intersection, subtract, distinct")
    print("\n   Sampling:")
    print("      • sample(fraction): Random sample (lazy)")
    print("      • takeSample(num): Exact count (action)")


if __name__ == "__main__":
    main()
