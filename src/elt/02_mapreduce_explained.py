"""
MapReduce Programming Model Explained

WHAT IS MAPREDUCE?
==================
MapReduce is a programming model with two main steps:

1. MAP: Break the job into many small tasks processed in parallel
2. REDUCE: Combine all those results into one final output

Example: Count words
• MAP: Break text into chunks and count locally
• REDUCE: Sum all counts together

MapReduce was originally developed by Google (2004) for processing large datasets
across distributed clusters. It's the foundation of Hadoop and inspired Spark.

Author: PySpark Learning Series
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session for MapReduce examples."""
    print("\n" + "=" * 70)
    print("CREATING SPARK SESSION FOR MAPREDUCE EXAMPLES")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("MapReduce Explained") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    print("✅ Spark session created")
    return spark


def example_1_word_count_mapreduce(spark):
    """
    Classic MapReduce example: Word count
    
    Problem: Count how many times each word appears in a large text.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: WORD COUNT (Classic MapReduce)")
    print("=" * 70)
    
    print("""
    MAPREDUCE FLOW FOR WORD COUNT:
    ==============================
    
    INPUT TEXT:
    "hello world hello spark spark is fast"
    
    STEP 1: MAP (Split and emit key-value pairs)
    ============================================
    Mapper 1: "hello world"
      → (hello, 1)
      → (world, 1)
    
    Mapper 2: "hello spark"
      → (hello, 1)
      → (spark, 1)
    
    Mapper 3: "spark is fast"
      → (spark, 1)
      → (is, 1)
      → (fast, 1)
    
    STEP 2: SHUFFLE & SORT (Group by key)
    =====================================
    hello: [1, 1]
    world: [1]
    spark: [1, 1]
    is: [1]
    fast: [1]
    
    STEP 3: REDUCE (Sum the values)
    ===============================
    Reducer 1: hello → [1, 1] → 2
    Reducer 2: world → [1] → 1
    Reducer 3: spark → [1, 1] → 2
    Reducer 4: is → [1] → 1
    Reducer 5: fast → [1] → 1
    
    FINAL OUTPUT:
    (hello, 2)
    (world, 1)
    (spark, 2)
    (is, 1)
    (fast, 1)
    """)
    
    # Sample data
    text_data = [
        ("hello world hello spark"),
        ("spark is fast"),
        ("hello pyspark"),
        ("spark spark pyspark")
    ]
    
    df = spark.createDataFrame(text_data, ["text"])
    
    print("\n📊 INPUT DATA:")
    df.show(truncate=False)
    
    # MapReduce with PySpark
    print("\n🔧 APPLYING MAPREDUCE:")
    
    # MAP: Split text into words and emit (word, 1)
    words_df = df.select(explode(split(col("text"), " ")).alias("word"))
    mapped_df = words_df.withColumn("count", lit(1))
    
    print("\n1️⃣ MAP PHASE (word, 1):")
    mapped_df.show(truncate=False)
    
    # REDUCE: Group by word and sum counts
    result_df = mapped_df.groupBy("word").agg(sum("count").alias("total_count"))
    
    print("\n2️⃣ REDUCE PHASE (Group and Sum):")
    result_df.orderBy(desc("total_count")).show(truncate=False)
    
    print("\n" + "=" * 70)
    print("✅ MAPREDUCE COMPLETE")
    print("=" * 70)


def example_2_mapreduce_with_rdd(spark):
    """
    Word count using RDD (lower-level MapReduce API).
    
    RDD = Resilient Distributed Dataset
    This is how Spark originally implemented MapReduce before DataFrames.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: MAPREDUCE WITH RDD (Lower-Level)")
    print("=" * 70)
    
    print("""
    RDD API: More explicit MapReduce operations
    ==========================================
    
    1. map(): Transform each element
    2. flatMap(): Transform each element to 0 or more elements
    3. reduceByKey(): Reduce values for each key
    4. groupByKey(): Group values for each key
    5. sortByKey(): Sort by key
    """)
    
    # Create RDD
    text_data = [
        "hello world hello spark",
        "spark is fast",
        "hello pyspark",
        "spark spark pyspark"
    ]
    
    rdd = spark.sparkContext.parallelize(text_data)
    
    print("\n📊 INPUT RDD:")
    print(rdd.collect())
    
    # MAP: Split into words
    print("\n1️⃣ MAP (flatMap to split words):")
    words_rdd = rdd.flatMap(lambda line: line.split(" "))
    print(words_rdd.collect())
    
    # MAP: Emit (word, 1) pairs
    print("\n2️⃣ MAP (Create key-value pairs):")
    pairs_rdd = words_rdd.map(lambda word: (word, 1))
    print(pairs_rdd.collect())
    
    # REDUCE: Sum counts by word
    print("\n3️⃣ REDUCE (reduceByKey - sum counts):")
    counts_rdd = pairs_rdd.reduceByKey(lambda a, b: a + b)
    print(counts_rdd.collect())
    
    # Sort by count
    print("\n4️⃣ SORT (Sort by count descending):")
    sorted_rdd = counts_rdd.sortBy(lambda x: x[1], ascending=False)
    result = sorted_rdd.collect()
    
    for word, count in result:
        print(f"  {word}: {count}")
    
    print("\n" + "=" * 70)
    print("✅ RDD MAPREDUCE COMPLETE")
    print("=" * 70)


def example_3_sales_aggregation_mapreduce(spark):
    """
    Real-world example: Sales aggregation using MapReduce pattern.
    
    Problem: Calculate total sales per category.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: SALES AGGREGATION (Real-World MapReduce)")
    print("=" * 70)
    
    print("""
    PROBLEM: Calculate total sales per category
    ===========================================
    
    INPUT: Sales transactions
    (product, category, amount)
    
    MAP PHASE:
    • Emit (category, amount) for each transaction
    
    SHUFFLE:
    • Group all amounts by category
    
    REDUCE PHASE:
    • Sum all amounts for each category
    
    OUTPUT: (category, total_sales)
    """)
    
    # Sample sales data
    sales_data = [
        ("iPhone", "Electronics", 1200),
        ("MacBook", "Electronics", 2500),
        ("Shirt", "Clothing", 45),
        ("TV", "Electronics", 800),
        ("Pants", "Clothing", 60),
        ("Shoes", "Clothing", 80),
        ("iPad", "Electronics", 900),
    ]
    
    df = spark.createDataFrame(sales_data, ["product", "category", "amount"])
    
    print("\n📊 INPUT DATA (Sales transactions):")
    df.show(truncate=False)
    
    # MAP: Emit (category, amount)
    print("\n1️⃣ MAP PHASE (Emit category-amount pairs):")
    mapped_df = df.select("category", "amount")
    mapped_df.show(truncate=False)
    
    # REDUCE: Group by category and sum
    print("\n2️⃣ REDUCE PHASE (Group and sum by category):")
    result_df = mapped_df.groupBy("category").agg(
        sum("amount").alias("total_sales"),
        count("*").alias("num_products"),
        avg("amount").alias("avg_price")
    ).orderBy(desc("total_sales"))
    
    result_df.show(truncate=False)
    
    print("\n" + "=" * 70)
    print("✅ SALES AGGREGATION COMPLETE")
    print("=" * 70)


def example_4_distributed_processing_visual(spark):
    """
    Visualize how MapReduce distributes work across cluster.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: DISTRIBUTED PROCESSING (Visual)")
    print("=" * 70)
    
    print("""
    MAPREDUCE ON A DISTRIBUTED CLUSTER:
    ===================================
    
    DATA: 1 TB of log files
    CLUSTER: 100 machines
    
    ┌──────────────────────────────────────────────────┐
    │              INPUT DATA (HDFS)                   │
    │          1 TB split into 1000 blocks            │
    │         (each block = 1 GB = 128 MB)            │
    └────────────┬─────────────────────────────────────┘
                 │
                 ▼ DISTRIBUTE TO WORKERS
    ┌─────────────────────────────────────────────────┐
    │              MAP PHASE (Parallel)               │
    ├─────────────┬─────────────┬─────────────────────┤
    │  Worker 1   │  Worker 2   │  ...  │  Worker 100 │
    │  Process    │  Process    │       │  Process    │
    │  Blocks     │  Blocks     │       │  Blocks     │
    │  1-10       │  11-20      │       │  991-1000   │
    │             │             │       │             │
    │  Emit       │  Emit       │       │  Emit       │
    │  (key,val)  │  (key,val)  │       │  (key,val)  │
    └─────────────┴─────────────┴─────────────────────┘
                 │
                 ▼ SHUFFLE & SORT (Network Transfer)
    ┌─────────────────────────────────────────────────┐
    │           GROUP BY KEY (Shuffle)                │
    │  • key1: [val1, val2, val3, ...]               │
    │  • key2: [val1, val2, val3, ...]               │
    │  • key3: [val1, val2, val3, ...]               │
    └─────────────┬───────────────────────────────────┘
                 │
                 ▼ DISTRIBUTE TO REDUCERS
    ┌─────────────────────────────────────────────────┐
    │            REDUCE PHASE (Parallel)              │
    ├─────────────┬─────────────┬─────────────────────┤
    │  Reducer 1  │  Reducer 2  │  ...  │  Reducer 100│
    │  Process    │  Process    │       │  Process    │
    │  key1       │  key2       │       │  key1000    │
    │             │             │       │             │
    │  Aggregate  │  Aggregate  │       │  Aggregate  │
    │  Results    │  Results    │       │  Results    │
    └─────────────┴─────────────┴─────────────────────┘
                 │
                 ▼
    ┌─────────────────────────────────────────────────┐
    │              FINAL OUTPUT (HDFS)                │
    │           (key1, result1)                       │
    │           (key2, result2)                       │
    │           ...                                   │
    └─────────────────────────────────────────────────┘
    
    KEY BENEFITS:
    • Parallel processing: 100 machines work simultaneously
    • Data locality: Process data where it's stored
    • Fault tolerance: Re-run failed tasks automatically
    • Scalability: Add more machines = faster processing
    
    EXAMPLE: Word count on 1 TB of text
    • Without MapReduce: 1 machine, 10 hours
    • With MapReduce (100 machines): ~6 minutes!
    """)
    
    print("\n" + "=" * 70)
    print("✅ DISTRIBUTED PROCESSING EXPLAINED")
    print("=" * 70)


def example_5_mapreduce_vs_spark(spark):
    """
    Comparison: Traditional MapReduce vs Spark.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: MAPREDUCE vs SPARK")
    print("=" * 70)
    
    print("""
    HADOOP MAPREDUCE (Traditional):
    ==============================
    • Write intermediate results to disk after each stage
    • Slower for iterative algorithms (machine learning)
    • Batch-only processing
    • Java API primarily
    
    APACHE SPARK (Modern):
    =====================
    • Keep intermediate results in memory (RDDs)
    • 10-100x faster for iterative algorithms
    • Supports batch, streaming, SQL, ML, graph processing
    • Python, Scala, Java, R APIs
    
    EXAMPLE: Iterative Algorithm (PageRank)
    =======================================
    
    Hadoop MapReduce:
    Iteration 1: Read HDFS → Map → Reduce → Write HDFS
    Iteration 2: Read HDFS → Map → Reduce → Write HDFS
    Iteration 3: Read HDFS → Map → Reduce → Write HDFS
    ... (many disk I/O operations)
    
    Apache Spark:
    Load data → Cache in memory
    Iteration 1: Map → Reduce (in memory)
    Iteration 2: Map → Reduce (in memory)
    Iteration 3: Map → Reduce (in memory)
    ... (no disk I/O)
    Write final result → HDFS
    
    RESULT: Spark is 10-100x faster!
    """)
    
    comparison_data = [
        ("Storage", "Disk (HDFS)", "Memory (RAM)"),
        ("Speed", "Slow", "Fast (10-100x)"),
        ("Iteration", "Disk I/O each time", "In-memory"),
        ("API", "Java", "Python, Scala, Java, R"),
        ("Use Case", "Batch only", "Batch, Streaming, SQL, ML"),
        ("Ease", "Complex", "Simple (SQL, DataFrames)"),
    ]
    
    comparison_df = spark.createDataFrame(comparison_data, 
        ["Feature", "Hadoop MapReduce", "Apache Spark"])
    
    print("\n📊 COMPARISON TABLE:")
    comparison_df.show(truncate=False)
    
    print("\n" + "=" * 70)
    print("✅ SPARK IS THE MODERN MAPREDUCE")
    print("=" * 70)


def main():
    """Run all MapReduce examples."""
    spark = create_spark_session()
    
    try:
        example_1_word_count_mapreduce(spark)
        example_2_mapreduce_with_rdd(spark)
        example_3_sales_aggregation_mapreduce(spark)
        example_4_distributed_processing_visual(spark)
        example_5_mapreduce_vs_spark(spark)
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS:")
        print("=" * 70)
        print("""
        1. MapReduce = MAP (split work) + REDUCE (combine results)
        
        2. MAP phase:
           • Break job into small parallel tasks
           • Emit key-value pairs
        
        3. SHUFFLE phase:
           • Group values by key
           • Network transfer
        
        4. REDUCE phase:
           • Aggregate values for each key
           • Produce final output
        
        5. Benefits:
           • Parallel processing across cluster
           • Fault tolerance
           • Scalability
        
        6. Spark improves MapReduce:
           • In-memory processing (faster)
           • High-level APIs (DataFrames, SQL)
           • Unified engine (batch, streaming, ML)
        """)
        
    finally:
        spark.stop()
        print("\n✅ Spark session stopped")


if __name__ == "__main__":
    main()
