"""
01_transformations_lowlevel_part1.py
=====================================

RDD Transformations - Low Level Part 1

Core transformation operations:
- map(): Transform each element
- flatMap(): Transform and flatten
- filter(): Select elements based on condition
- mapPartitions(): Transform entire partitions
- mapPartitionsWithIndex(): Transform with partition index

These are LAZY operations - they build a DAG without executing.
"""

from pyspark.sql import SparkSession


def demonstrate_map():
    """
    map(func): Apply function to each element.
    Returns: RDD with same number of elements.
    """
    print("=" * 80)
    print("MAP TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("MapDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Simple transformation
    numbers = sc.parallelize([1, 2, 3, 4, 5])
    squared = numbers.map(lambda x: x ** 2)
    
    print("\n📊 Original:", numbers.collect())
    print("📊 Squared:", squared.collect())
    
    # Example 2: String transformation
    names = sc.parallelize(["alice", "bob", "charlie"])
    uppercase = names.map(lambda x: x.upper())
    
    print("\n📝 Original:", names.collect())
    print("📝 Uppercase:", uppercase.collect())
    
    # Example 3: Complex object transformation
    employees = sc.parallelize([
        ("Alice", 50000),
        ("Bob", 60000),
        ("Charlie", 55000)
    ])
    with_bonus = employees.map(lambda x: (x[0], x[1], x[1] * 0.1))
    
    print("\n💰 Employees with bonus:")
    for emp in with_bonus.collect():
        print(f"   {emp[0]}: Salary=${emp[1]}, Bonus=${emp[2]:.2f}")
    
    spark.stop()


def demonstrate_flatmap():
    """
    flatMap(func): Apply function and flatten results.
    Returns: RDD with potentially different number of elements.
    """
    print("\n" + "=" * 80)
    print("FLATMAP TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("FlatMapDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Split sentences into words
    sentences = sc.parallelize([
        "Hello world",
        "PySpark is awesome",
        "RDD operations are powerful"
    ])
    
    # map returns: [['Hello', 'world'], ['PySpark', 'is', 'awesome'], ...]
    words_map = sentences.map(lambda x: x.split())
    print("\n📊 With map():", words_map.collect())
    
    # flatMap returns: ['Hello', 'world', 'PySpark', 'is', 'awesome', ...]
    words_flatmap = sentences.flatMap(lambda x: x.split())
    print("📊 With flatMap():", words_flatmap.collect())
    
    # Example 2: Generate ranges
    numbers = sc.parallelize([1, 2, 3])
    
    # Each number generates a range
    ranges = numbers.flatMap(lambda x: range(1, x + 1))
    print("\n🔢 Numbers:", numbers.collect())
    print("🔢 Flattened ranges:", ranges.collect())
    # Output: [1, 1, 2, 1, 2, 3]
    
    # Example 3: Extract multiple fields
    data = sc.parallelize([
        "user1:action1,action2,action3",
        "user2:action4,action5"
    ])
    
    actions = data.flatMap(lambda x: [
        (x.split(':')[0], action) 
        for action in x.split(':')[1].split(',')
    ])
    
    print("\n👤 User actions:")
    for user, action in actions.collect():
        print(f"   {user}: {action}")
    
    spark.stop()


def demonstrate_filter():
    """
    filter(func): Select elements where func returns True.
    """
    print("\n" + "=" * 80)
    print("FILTER TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("FilterDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Example 1: Filter even numbers
    numbers = sc.parallelize(range(1, 11))
    evens = numbers.filter(lambda x: x % 2 == 0)
    
    print("\n🔢 All numbers:", numbers.collect())
    print("🔢 Even numbers:", evens.collect())
    
    # Example 2: Filter by string condition
    words = sc.parallelize(["apple", "banana", "avocado", "cherry", "apricot"])
    a_words = words.filter(lambda x: x.startswith('a'))
    
    print("\n🍎 All words:", words.collect())
    print("🍎 Words starting with 'a':", a_words.collect())
    
    # Example 3: Complex filtering
    employees = sc.parallelize([
        ("Alice", "Engineering", 80000),
        ("Bob", "Sales", 60000),
        ("Charlie", "Engineering", 75000),
        ("David", "Sales", 55000),
        ("Eve", "Engineering", 90000)
    ])
    
    # Filter engineering employees with salary > 75k
    high_paid_engineers = employees.filter(
        lambda x: x[1] == "Engineering" and x[2] > 75000
    )
    
    print("\n💼 High-paid engineers:")
    for emp in high_paid_engineers.collect():
        print(f"   {emp[0]}: ${emp[2]:,}")
    
    spark.stop()


def demonstrate_map_partitions():
    """
    mapPartitions(func): Transform entire partitions at once.
    More efficient than map for expensive operations.
    """
    print("\n" + "=" * 80)
    print("MAPPARTITIONS TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("MapPartitionsDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    # Create RDD with explicit partitioning
    numbers = sc.parallelize(range(1, 21), 4)  # 4 partitions
    
    print(f"\n📊 Total elements: 20")
    print(f"📊 Partitions: {numbers.getNumPartitions()}")
    
    # Example 1: Process each partition
    def process_partition(iterator):
        """
        Iterator contains all elements in the partition.
        Return an iterator of transformed elements.
        """
        # Convert to list to see partition contents
        partition_data = list(iterator)
        print(f"   Processing partition with {len(partition_data)} elements: {partition_data}")
        
        # Transform each element
        return [x * 2 for x in partition_data]
    
    doubled = numbers.mapPartitions(process_partition)
    print("\n🔄 Processing partitions...")
    result = doubled.collect()
    print(f"✅ Result: {result}")
    
    # Example 2: Aggregate within partition (more efficient)
    def sum_partition(iterator):
        """Sum all elements in partition - only one output per partition."""
        total = sum(iterator)
        return [total]  # Return iterator with single value
    
    partition_sums = numbers.mapPartitions(sum_partition)
    print(f"\n📊 Sum of each partition: {partition_sums.collect()}")
    print(f"📊 Total sum: {sum(partition_sums.collect())}")
    
    # Example 3: Database connections (efficiency gain)
    def process_with_connection(iterator):
        """
        Expensive setup (e.g., DB connection) done ONCE per partition
        instead of once per element.
        """
        # Simulated expensive setup
        print("   🔌 Opening database connection...")
        connection = "DB_CONNECTION"
        
        results = []
        for item in iterator:
            # Use connection for each item
            results.append(f"Processed {item} with {connection}")
        
        print("   🔌 Closing database connection...")
        return results
    
    small_rdd = sc.parallelize(range(1, 7), 2)
    processed = small_rdd.mapPartitions(process_with_connection)
    
    print("\n💾 Processing with connections:")
    for item in processed.collect():
        print(f"   {item}")
    
    spark.stop()


def demonstrate_map_partitions_with_index():
    """
    mapPartitionsWithIndex(func): Like mapPartitions but includes partition index.
    Useful for debugging and partition-specific logic.
    """
    print("\n" + "=" * 80)
    print("MAPPARTITIONSWITHINDEX TRANSFORMATION")
    print("=" * 80)
    
    spark = SparkSession.builder.appName("MapPartitionsIndexDemo").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    
    numbers = sc.parallelize(range(1, 21), 4)
    
    # Example 1: Show partition index
    def show_partition_info(index, iterator):
        """
        index: Partition number (0, 1, 2, ...)
        iterator: Elements in that partition
        """
        elements = list(iterator)
        print(f"   Partition {index}: {elements}")
        return [(index, x) for x in elements]
    
    indexed = numbers.mapPartitionsWithIndex(show_partition_info)
    print("\n📊 Elements with partition index:")
    for partition_id, value in indexed.collect()[:10]:  # Show first 10
        print(f"   Partition {partition_id}: value={value}")
    
    # Example 2: Partition-specific transformation
    def transform_by_partition(index, iterator):
        """Different transformation based on partition index."""
        elements = list(iterator)
        
        if index == 0:
            return [x * 2 for x in elements]  # Double first partition
        elif index == 1:
            return [x * 3 for x in elements]  # Triple second partition
        else:
            return elements  # Keep others unchanged
    
    transformed = numbers.mapPartitionsWithIndex(transform_by_partition)
    print("\n🔄 Partition-specific transformations:")
    print(f"   Result: {transformed.collect()}")
    
    # Example 3: Add partition-based prefixes
    words = sc.parallelize(["apple", "banana", "cherry", "date", "elderberry", "fig"], 3)
    
    def add_prefix(index, iterator):
        """Add partition number as prefix."""
        return [f"P{index}_{word}" for word in iterator]
    
    prefixed = words.mapPartitionsWithIndex(add_prefix)
    print("\n📝 Words with partition prefix:")
    for word in prefixed.collect():
        print(f"   {word}")
    
    spark.stop()


def main():
    """
    Run all low-level transformation demonstrations.
    """
    print("\n" + "🔷" * 40)
    print("RDD TRANSFORMATIONS - LOW LEVEL PART 1")
    print("🔷" * 40)
    
    demonstrate_map()
    demonstrate_flatmap()
    demonstrate_filter()
    demonstrate_map_partitions()
    demonstrate_map_partitions_with_index()
    
    print("\n" + "=" * 80)
    print("✅ LOW-LEVEL TRANSFORMATIONS PART 1 COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. map(): 1-to-1 transformation (same # of elements)")
    print("   2. flatMap(): 1-to-N transformation (flatten results)")
    print("   3. filter(): Select elements by condition")
    print("   4. mapPartitions(): Transform entire partitions (more efficient)")
    print("   5. mapPartitionsWithIndex(): Partition transformation with index")
    
    print("\n💡 When to use what:")
    print("   - map(): Simple element-wise transformations")
    print("   - flatMap(): When each element produces multiple outputs")
    print("   - filter(): Remove unwanted elements")
    print("   - mapPartitions(): Expensive setup (DB connections, ML models)")
    print("   - mapPartitionsWithIndex(): Debugging, partition-aware logic")
    
    print("\n⚡ Performance tips:")
    print("   - Use mapPartitions for expensive operations (DB, HTTP calls)")
    print("   - Avoid collect() on large RDDs")
    print("   - Filter early to reduce data size")
    print("   - Chain transformations - they're lazy until action")


if __name__ == "__main__":
    main()
