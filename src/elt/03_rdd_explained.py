"""
RDD (Resilient Distributed Dataset) Explained

WHAT IS RDD?
============
RDD = Resilient Distributed Dataset

Spark's original data structure for:
• Distributed data across cluster
• Fault tolerance (resilient)
• Parallel processing

DataFrames largely replaced RDDs because they're faster and easier to use.

WHY RDD?
========
• Low-level API for custom logic
• Control over partitioning and caching
• Necessary for some advanced operations

WHY DataFrames are better:
• Optimized execution (Catalyst optimizer)
• SQL support
• Schema enforcement
• Easier to use

Author: PySpark Learning Series
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session for RDD examples."""
    print("\n" + "=" * 70)
    print("CREATING SPARK SESSION FOR RDD EXAMPLES")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("RDD Explained") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    print("✅ Spark session created")
    return spark


def example_1_rdd_basics(spark):
    """
    RDD basics: Creation and basic operations.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: RDD BASICS")
    print("=" * 70)
    
    print("""
    WHAT IS RDD?
    ===========
    RDD = Resilient Distributed Dataset
    
    • RESILIENT: Fault-tolerant, can recover from failures
    • DISTRIBUTED: Data spread across cluster
    • DATASET: Collection of data
    
    KEY CHARACTERISTICS:
    ===================
    1. Immutable: Once created, cannot be changed
    2. Lazy evaluation: Transformations not executed until action
    3. Partitioned: Data split across machines
    4. Fault-tolerant: Lineage graph allows recomputation
    
    TWO TYPES OF OPERATIONS:
    =======================
    • Transformations: Create new RDD (map, filter, flatMap)
    • Actions: Return value to driver (collect, count, reduce)
    """)
    
    # Create RDD from list
    print("\n1️⃣ CREATING RDD FROM LIST:")
    data = [1, 2, 3, 4, 5]
    rdd = spark.sparkContext.parallelize(data)
    
    print(f"Data: {data}")
    print(f"RDD: {rdd}")
    print(f"Type: {type(rdd)}")
    print(f"Partitions: {rdd.getNumPartitions()}")
    
    # Transformations (lazy)
    print("\n2️⃣ TRANSFORMATIONS (Lazy - not executed yet):")
    mapped_rdd = rdd.map(lambda x: x * 2)
    filtered_rdd = mapped_rdd.filter(lambda x: x > 5)
    
    print("• map(x * 2)")
    print("• filter(x > 5)")
    print("❗ Nothing computed yet (lazy evaluation)")
    
    # Actions (trigger execution)
    print("\n3️⃣ ACTIONS (Trigger execution):")
    result = filtered_rdd.collect()
    
    print(f"collect() result: {result}")
    print(f"count(): {filtered_rdd.count()}")
    print(f"first(): {filtered_rdd.first()}")
    
    print("\n" + "=" * 70)
    print("✅ RDD BASICS COMPLETE")
    print("=" * 70)


def example_2_rdd_transformations(spark):
    """
    Common RDD transformations.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: RDD TRANSFORMATIONS")
    print("=" * 70)
    
    print("""
    COMMON TRANSFORMATIONS:
    ======================
    1. map(func): Apply function to each element
    2. filter(func): Keep elements that pass condition
    3. flatMap(func): Map then flatten (1 element → 0 or more)
    4. distinct(): Remove duplicates
    5. union(other): Combine two RDDs
    6. intersection(other): Common elements
    7. subtract(other): Elements in first but not second
    """)
    
    # Sample data
    data = [1, 2, 3, 4, 5, 2, 3]
    rdd = spark.sparkContext.parallelize(data)
    
    print(f"\n📊 INPUT RDD: {rdd.collect()}")
    
    # map
    print("\n1️⃣ map(x * 2):")
    mapped = rdd.map(lambda x: x * 2)
    print(f"Result: {mapped.collect()}")
    
    # filter
    print("\n2️⃣ filter(x > 2):")
    filtered = rdd.filter(lambda x: x > 2)
    print(f"Result: {filtered.collect()}")
    
    # flatMap
    print("\n3️⃣ flatMap(range(x)):")
    flat_mapped = rdd.flatMap(lambda x: range(x))
    print(f"Result: {flat_mapped.collect()}")
    print("Explanation: 1→[0], 2→[0,1], 3→[0,1,2], etc.")
    
    # distinct
    print("\n4️⃣ distinct():")
    distinct_rdd = rdd.distinct()
    print(f"Result: {sorted(distinct_rdd.collect())}")
    
    # union
    print("\n5️⃣ union([10, 20]):")
    other_rdd = spark.sparkContext.parallelize([10, 20])
    union_rdd = rdd.union(other_rdd)
    print(f"Result: {union_rdd.collect()}")
    
    print("\n" + "=" * 70)
    print("✅ RDD TRANSFORMATIONS COMPLETE")
    print("=" * 70)


def example_3_key_value_rdd(spark):
    """
    Key-value pair RDD operations.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: KEY-VALUE PAIR RDD")
    print("=" * 70)
    
    print("""
    KEY-VALUE RDD OPERATIONS:
    ========================
    When RDD contains (key, value) tuples:
    
    1. reduceByKey(func): Reduce values for each key
    2. groupByKey(): Group values for each key
    3. sortByKey(): Sort by key
    4. mapValues(func): Transform values (keep keys same)
    5. keys(): Get all keys
    6. values(): Get all values
    7. join(other): Inner join on keys
    """)
    
    # Sample key-value data
    data = [
        ("apple", 2),
        ("banana", 3),
        ("apple", 5),
        ("banana", 1),
        ("orange", 4),
    ]
    
    rdd = spark.sparkContext.parallelize(data)
    
    print(f"\n📊 INPUT KEY-VALUE RDD:")
    for item in rdd.collect():
        print(f"  {item}")
    
    # reduceByKey
    print("\n1️⃣ reduceByKey(lambda a,b: a+b) - Sum by key:")
    reduced = rdd.reduceByKey(lambda a, b: a + b)
    print(f"Result: {reduced.collect()}")
    
    # groupByKey
    print("\n2️⃣ groupByKey() - Group values by key:")
    grouped = rdd.groupByKey()
    result = grouped.mapValues(list).collect()
    for k, v in result:
        print(f"  {k}: {v}")
    
    # sortByKey
    print("\n3️⃣ sortByKey() - Sort by key:")
    sorted_rdd = rdd.sortByKey()
    print(f"Result: {sorted_rdd.collect()}")
    
    # mapValues
    print("\n4️⃣ mapValues(lambda x: x*2) - Double values:")
    mapped_values = rdd.mapValues(lambda x: x * 2)
    print(f"Result: {mapped_values.collect()}")
    
    # keys and values
    print("\n5️⃣ keys() and values():")
    print(f"Keys: {rdd.keys().distinct().collect()}")
    print(f"Values: {rdd.values().collect()}")
    
    print("\n" + "=" * 70)
    print("✅ KEY-VALUE RDD COMPLETE")
    print("=" * 70)


def example_4_rdd_vs_dataframe(spark):
    """
    Comparison: RDD vs DataFrame
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: RDD vs DATAFRAME")
    print("=" * 70)
    
    print("""
    RDD (Original Spark API):
    ========================
    • Low-level API
    • Manual optimization required
    • No schema
    • Functional programming style
    • Slower (no Catalyst optimizer)
    
    DataFrame (Modern Spark API):
    ============================
    • High-level API
    • Automatic optimization (Catalyst)
    • Schema enforcement
    • SQL-like operations
    • Faster (10-100x in some cases)
    
    WHEN TO USE RDD:
    ===============
    ❌ Don't use unless necessary
    ✅ Custom partitioning logic
    ✅ Low-level control needed
    ✅ No schema available
    
    WHEN TO USE DATAFRAME:
    =====================
    ✅ Always use if possible
    ✅ SQL operations
    ✅ Structured data
    ✅ Better performance
    """)
    
    # Same operation with RDD vs DataFrame
    data = [("Alice", 30), ("Bob", 35), ("Charlie", 25)]
    
    # With RDD
    print("\n1️⃣ WITH RDD (Manual):")
    rdd = spark.sparkContext.parallelize(data)
    filtered_rdd = rdd.filter(lambda x: x[1] > 28)
    result_rdd = filtered_rdd.map(lambda x: (x[0], x[1] * 2))
    
    print("Code:")
    print("  rdd.filter(lambda x: x[1] > 28)")
    print("     .map(lambda x: (x[0], x[1] * 2))")
    print(f"Result: {result_rdd.collect()}")
    
    # With DataFrame
    print("\n2️⃣ WITH DATAFRAME (Optimized):")
    df = spark.createDataFrame(data, ["name", "age"])
    result_df = df.filter(col("age") > 28).withColumn("age", col("age") * 2)
    
    print("Code:")
    print("  df.filter(col('age') > 28)")
    print("    .withColumn('age', col('age') * 2)")
    result_df.show()
    
    print("\n💡 DataFrame benefits:")
    print("  • Automatic query optimization")
    print("  • Predicate pushdown")
    print("  • Column pruning")
    print("  • Code generation")
    print("  • Result: 10-100x faster!")
    
    print("\n" + "=" * 70)
    print("✅ RDD vs DATAFRAME COMPLETE")
    print("=" * 70)


def example_5_rdd_partitioning(spark):
    """
    RDD partitioning and caching.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: RDD PARTITIONING & CACHING")
    print("=" * 70)
    
    print("""
    PARTITIONING:
    ============
    • RDD data split across partitions
    • Each partition processed in parallel
    • More partitions = more parallelism
    
    CACHING:
    =======
    • Store RDD in memory for reuse
    • Avoid recomputation
    • Important for iterative algorithms
    """)
    
    # Create RDD with specific partitions
    data = range(1, 101)
    rdd = spark.sparkContext.parallelize(data, numSlices=4)
    
    print(f"\n📊 RDD with {rdd.getNumPartitions()} partitions")
    print(f"Data: 1 to 100")
    
    # Show partitioning
    print("\n1️⃣ VIEW PARTITIONS:")
    def show_partition(index, iterator):
        yield f"Partition {index}: {list(iterator)[:5]}..."
    
    partitions = rdd.mapPartitionsWithIndex(show_partition).collect()
    for p in partitions:
        print(f"  {p}")
    
    # Repartition
    print("\n2️⃣ REPARTITION (change number of partitions):")
    repartitioned = rdd.repartition(8)
    print(f"Old partitions: {rdd.getNumPartitions()}")
    print(f"New partitions: {repartitioned.getNumPartitions()}")
    
    # Caching
    print("\n3️⃣ CACHING (persist in memory):")
    cached_rdd = rdd.cache()
    
    print("First action (compute + cache):")
    count1 = cached_rdd.count()
    print(f"  count() = {count1}")
    
    print("Second action (use cache):")
    count2 = cached_rdd.count()
    print(f"  count() = {count2} (faster, from cache)")
    
    # Unpersist
    cached_rdd.unpersist()
    print("\n✅ Cache cleared with unpersist()")
    
    print("\n" + "=" * 70)
    print("✅ RDD PARTITIONING & CACHING COMPLETE")
    print("=" * 70)


def main():
    """Run all RDD examples."""
    spark = create_spark_session()
    
    try:
        example_1_rdd_basics(spark)
        example_2_rdd_transformations(spark)
        example_3_key_value_rdd(spark)
        example_4_rdd_vs_dataframe(spark)
        example_5_rdd_partitioning(spark)
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS:")
        print("=" * 70)
        print("""
        1. RDD = Resilient Distributed Dataset
           • Spark's original data structure
           • Fault-tolerant and distributed
        
        2. Two types of operations:
           • Transformations: lazy (map, filter, flatMap)
           • Actions: trigger execution (collect, count, reduce)
        
        3. Key-value RDD operations:
           • reduceByKey: aggregate by key
           • groupByKey: group values by key
           • join: combine RDDs by key
        
        4. RDD vs DataFrame:
           • RDD: Low-level, manual optimization
           • DataFrame: High-level, automatic optimization
           • Use DataFrames whenever possible (10-100x faster)
        
        5. Advanced features:
           • Partitioning: control data distribution
           • Caching: store in memory for reuse
        
        6. Modern Spark:
           • DataFrames replaced RDDs for most use cases
           • Use RDD only when low-level control needed
        """)
        
    finally:
        spark.stop()
        print("\n✅ Spark session stopped")


if __name__ == "__main__":
    main()
