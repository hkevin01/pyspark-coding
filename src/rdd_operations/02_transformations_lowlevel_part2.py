"""
02_transformations_lowlevel_part2.py
=====================================

RDD Transformations - Low Level Part 2

Additional core transformations:
- distinct(): Remove duplicates
- union(): Combine two RDDs
- intersection(): Common elements
- subtract(): Elements in first but not second
- cartesian(): Cartesian product
- pipe(): Pipe elements through external process
"""

from pyspark.sql import SparkSession


def demonstrate_distinct():
    """Remove duplicate elements."""
    print("=" * 80)
    print("DISTINCT TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("DistinctDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Remove number duplicates
    numbers = sc.parallelize([1, 2, 2, 3, 3, 3, 4, 4, 4, 4])
    unique = numbers.distinct()
    
    print(f"\n📊 Original ({numbers.count()} elements):", numbers.collect())
    print(f"📊 Unique ({unique.count()} elements):", unique.collect())
    
    # Example 2: Distinct words
    words = sc.parallelize(["apple", "banana", "apple", "cherry", "banana", "apple"])
    unique_words = words.distinct()
    
    print(f"\n📝 All words:", words.collect())
    print(f"📝 Unique words:", unique_words.collect())
    
    # Example 3: Complex objects
    pairs = sc.parallelize([("a", 1), ("b", 2), ("a", 1), ("c", 3), ("b", 2)])
    unique_pairs = pairs.distinct()
    
    print(f"\n🔷 All pairs:", pairs.collect())
    print(f"🔷 Unique pairs:", unique_pairs.collect())
    
    print("\n⚠️  Note: distinct() causes shuffle - expensive for large datasets!")
    
    spark.stop()


def demonstrate_union():
    """Combine two RDDs (allows duplicates)."""
    print("\n" + "=" * 80)
    print("UNION TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("UnionDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Combine number sets
    rdd1 = sc.parallelize([1, 2, 3, 4])
    rdd2 = sc.parallelize([3, 4, 5, 6])
    combined = rdd1.union(rdd2)
    
    print("\n🔢 RDD1:", rdd1.collect())
    print("🔢 RDD2:", rdd2.collect())
    print("🔢 Union (with duplicates):", combined.collect())
    print("🔢 Union distinct:", combined.distinct().collect())
    
    # Example 2: Combine logs from different sources
    log1 = sc.parallelize(["ERROR: Failed", "INFO: Started", "WARN: Slow"])
    log2 = sc.parallelize(["INFO: Completed", "ERROR: Timeout", "INFO: Started"])
    
    all_logs = log1.union(log2)
    print("\n📋 Combined logs:")
    for log in all_logs.collect():
        print(f"   {log}")
    
    spark.stop()


def demonstrate_intersection():
    """Get common elements between two RDDs."""
    print("\n" + "=" * 80)
    print("INTERSECTION TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("IntersectionDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Common numbers
    rdd1 = sc.parallelize([1, 2, 3, 4, 5])
    rdd2 = sc.parallelize([3, 4, 5, 6, 7])
    common = rdd1.intersection(rdd2)
    
    print("\n🔢 RDD1:", rdd1.collect())
    print("🔢 RDD2:", rdd2.collect())
    print("🔢 Common elements:", common.collect())
    
    # Example 2: Common customers
    customers_store1 = sc.parallelize(["Alice", "Bob", "Charlie", "David"])
    customers_store2 = sc.parallelize(["Bob", "Charlie", "Eve", "Frank"])
    
    common_customers = customers_store1.intersection(customers_store2)
    print("\n👥 Customers at both stores:", common_customers.collect())
    
    spark.stop()


def demonstrate_subtract():
    """Elements in first RDD but not in second."""
    print("\n" + "=" * 80)
    print("SUBTRACT TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("SubtractDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Set difference
    all_items = sc.parallelize([1, 2, 3, 4, 5])
    sold_items = sc.parallelize([2, 4])
    remaining = all_items.subtract(sold_items)
    
    print("\n📦 All items:", all_items.collect())
    print("📦 Sold items:", sold_items.collect())
    print("📦 Remaining:", remaining.collect())
    
    # Example 2: New users
    all_users = sc.parallelize(["Alice", "Bob", "Charlie", "David", "Eve"])
    existing_users = sc.parallelize(["Alice", "Charlie"])
    
    new_users = all_users.subtract(existing_users)
    print("\n👤 New users:", new_users.collect())
    
    spark.stop()


def demonstrate_cartesian():
    """Cartesian product of two RDDs."""
    print("\n" + "=" * 80)
    print("CARTESIAN TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("CartesianDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Simple cartesian product
    numbers = sc.parallelize([1, 2, 3])
    letters = sc.parallelize(['a', 'b'])
    
    product = numbers.cartesian(letters)
    
    print("\n🔢 Numbers:", numbers.collect())
    print("🔤 Letters:", letters.collect())
    print("🔷 Cartesian product:")
    for pair in product.collect():
        print(f"   {pair}")
    
    # Example 2: Generate test combinations
    sizes = sc.parallelize(['S', 'M', 'L'])
    colors = sc.parallelize(['Red', 'Blue'])
    
    combinations = sizes.cartesian(colors)
    print("\n👕 Product variations:")
    for size, color in combinations.collect():
        print(f"   Size: {size}, Color: {color}")
    
    print("\n⚠️  Warning: Cartesian product size = n1 * n2 (can be huge!)")
    
    spark.stop()


def main():
    """Run all demonstrations."""
    print("\n" + "🔷" * 40)
    print("RDD TRANSFORMATIONS - LOW LEVEL PART 2")
    print("🔷" * 40)
    
    demonstrate_distinct()
    demonstrate_union()
    demonstrate_intersection()
    demonstrate_subtract()
    demonstrate_cartesian()
    
    print("\n" + "=" * 80)
    print("✅ LOW-LEVEL TRANSFORMATIONS PART 2 COMPLETE")
    print("=" * 80)
    
    print("\n📚 Summary:")
    print("   - distinct(): Remove duplicates (causes shuffle)")
    print("   - union(): Combine RDDs (keeps duplicates)")
    print("   - intersection(): Common elements (expensive)")
    print("   - subtract(): A - B (elements in A not in B)")
    print("   - cartesian(): All pairs (very expensive!)")


if __name__ == "__main__":
    main()
