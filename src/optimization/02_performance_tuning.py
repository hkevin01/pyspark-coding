"""
02_performance_tuning.py
=========================

Spark Performance Tuning and Configuration

Demonstrates:
- Memory configuration
- Parallelism tuning
- Shuffle optimization
- Caching strategies
- Adaptive Query Execution (AQE)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count
import time


def create_optimized_spark():
    """
    Create Spark session with optimized configuration.
    """
    return SparkSession.builder \
        .appName("PerformanceTuning") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", 10485760) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def demonstrate_memory_tuning():
    """
    Memory configuration and tuning.
    """
    print("\n" + "=" * 80)
    print("MEMORY CONFIGURATION AND TUNING")
    print("=" * 80)
    
    print("\n📊 SPARK MEMORY MODEL:")
    print("""
    ┌────────────────────────────────────────────────────────┐
    │              EXECUTOR MEMORY (e.g., 4GB)               │
    ├────────────────────────────────────────────────────────┤
    │                                                        │
    │  ┌─────────────────────────────────────────────────┐  │
    │  │      RESERVED MEMORY (300MB fixed)             │  │
    │  └─────────────────────────────────────────────────┘  │
    │                                                        │
    │  ┌─────────────────────────────────────────────────┐  │
    │  │      USABLE MEMORY (3.7GB)                      │  │
    │  ├─────────────────────────────────────────────────┤  │
    │  │                                                 │  │
    │  │  ┌──────────────────────────────────────────┐  │  │
    │  │  │  SPARK MEMORY (60% = 2.22GB)            │  │  │
    │  │  │  (spark.memory.fraction = 0.6)          │  │  │
    │  │  ├──────────────────────────────────────────┤  │  │
    │  │  │                                          │  │  │
    │  │  │  ┌────────────────────────────────────┐ │  │  │
    │  │  │  │ STORAGE MEMORY (50% = 1.11GB)     │ │  │  │
    │  │  │  │ (spark.memory.storageFraction)    │ │  │  │
    │  │  │  │ • Cache                           │ │  │  │
    │  │  │  │ • Broadcast variables             │ │  │  │
    │  │  │  └────────────────────────────────────┘ │  │  │
    │  │  │                                          │  │  │
    │  │  │  ┌────────────────────────────────────┐ │  │  │
    │  │  │  │ EXECUTION MEMORY (50% = 1.11GB)   │ │  │  │
    │  │  │  │ • Shuffles                        │ │  │  │
    │  │  │  │ • Joins                           │ │  │  │
    │  │  │  │ • Sorts                           │ │  │  │
    │  │  │  │ • Aggregations                    │ │  │  │
    │  │  │  └────────────────────────────────────┘ │  │  │
    │  │  │                                          │  │  │
    │  │  └──────────────────────────────────────────┘  │  │
    │  │                                                 │  │
    │  │  ┌──────────────────────────────────────────┐  │  │
    │  │  │  USER MEMORY (40% = 1.48GB)             │  │  │
    │  │  │  • User data structures                 │  │  │
    │  │  │  • UDFs                                  │  │  │
    │  │  │  • Python objects                        │  │  │
    │  │  └──────────────────────────────────────────┘  │  │
    │  │                                                 │  │
    │  └─────────────────────────────────────────────────┘  │
    │                                                        │
    └────────────────────────────────────────────────────────┘
    """)
    
    print("\n⚙️  KEY CONFIGURATION PARAMETERS:")
    print("""
    # Executor Memory
    --executor-memory 4g
    
    # Spark Memory Fraction (default: 0.6)
    --conf spark.memory.fraction=0.6
    
    # Storage vs Execution split (default: 0.5)
    --conf spark.memory.storageFraction=0.5
    
    # Driver Memory
    --driver-memory 2g
    """)
    
    print("\n🎯 TUNING GUIDELINES:")
    print("""
    1. Executor Memory:
       • Too small → frequent spills to disk
       • Too large → GC overhead
       • Recommended: 8-32GB per executor
    
    2. Storage Fraction:
       • High (0.7) → more caching, less execution memory
       • Low (0.3) → less caching, more shuffle memory
       • Default (0.5) → balanced
    
    3. Memory Fraction:
       • Default (0.6) works for most cases
       • Increase if lots of UDFs or Python objects
    """)
    
    print("\n💡 COMMON MEMORY ISSUES:")
    print("""
    ❌ OutOfMemoryError:
       → Increase executor memory
       → Reduce cache usage
       → Increase shuffle partitions
    
    ❌ GC Overhead:
       → Reduce executor memory size
       → Increase number of executors
       → Use off-heap memory
    
    ❌ Shuffle Spill:
       → Increase execution memory
       → Increase shuffle partitions
       → Enable compression
    """)


def demonstrate_parallelism_tuning(spark):
    """
    Parallelism and partition tuning.
    """
    print("\n" + "=" * 80)
    print("PARALLELISM AND PARTITION TUNING")
    print("=" * 80)
    
    # Create sample data
    df = spark.range(0, 1000000)
    
    print("\n📊 PARTITION METRICS:")
    print(f"   Default parallelism: {spark.sparkContext.defaultParallelism}")
    print(f"   Initial partitions: {df.rdd.getNumPartitions()}")
    
    print("\n⚙️  KEY PARAMETERS:")
    print("""
    1. spark.default.parallelism
       • For RDD operations
       • Default: number of cores in cluster
       • Set to: 2-3x available cores
    
    2. spark.sql.shuffle.partitions
       • For DataFrame shuffle operations
       • Default: 200 (often too high!)
       • Set based on data size:
         - Small data (<1GB): 8-16
         - Medium data (1-10GB): 50-100
         - Large data (>10GB): 200-500
    """)
    
    # Demonstrate partition tuning
    print("\n🧪 PARTITION TUNING EXAMPLE:")
    
    # Too many partitions (overhead)
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    start = time.time()
    result1 = df.groupBy((col("id") % 10).alias("key")).count()
    result1.show()
    time_200 = time.time() - start
    print(f"   With 200 partitions: {time_200:.3f}s")
    print(f"   Partitions after shuffle: {result1.rdd.getNumPartitions()}")
    
    # Optimized partitions
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    start = time.time()
    result2 = df.groupBy((col("id") % 10).alias("key")).count()
    result2.show()
    time_8 = time.time() - start
    print(f"   With 8 partitions: {time_8:.3f}s")
    print(f"   Partitions after shuffle: {result2.rdd.getNumPartitions()}")
    print(f"   Speedup: {time_200/time_8:.2f}x")
    
    print("\n🎯 PARTITION SIZE GUIDELINES:")
    print("""
    Ideal partition size: 100-200MB
    
    Too many partitions:
    ❌ Small tasks → scheduling overhead
    ❌ Too many small files on write
    
    Too few partitions:
    ❌ Large tasks → memory pressure
    ❌ Poor parallelism
    ❌ Stragglers
    
    Calculate optimal partitions:
    partitions = data_size_MB / target_partition_size_MB
    partitions = 10GB / 128MB = ~80 partitions
    """)


def demonstrate_shuffle_optimization(spark):
    """
    Shuffle operation optimization.
    """
    print("\n" + "=" * 80)
    print("SHUFFLE OPTIMIZATION")
    print("=" * 80)
    
    df = spark.range(0, 100000)
    
    print("\n⚙️  SHUFFLE CONFIGURATION:")
    print("""
    # Shuffle partitions (most important!)
    --conf spark.sql.shuffle.partitions=100
    
    # Shuffle compression (default: true)
    --conf spark.sql.shuffle.compress=true
    
    # Shuffle compression codec (default: lz4)
    --conf spark.io.compression.codec=lz4
    # Options: lz4 (fast), snappy (balanced), gzip (high compression)
    
    # Shuffle file buffer (default: 32k)
    --conf spark.shuffle.file.buffer=32k
    
    # Shuffle spill compression (default: true)
    --conf spark.shuffle.spill.compress=true
    
    # Reducer memory fraction for sorting (default: 0.2)
    --conf spark.shuffle.memoryFraction=0.2
    """)
    
    print("\n🎯 SHUFFLE OPTIMIZATION TECHNIQUES:")
    print("""
    1. Filter Before Shuffle:
       ❌ df.groupBy("key").count().filter(col("count") > 10)
       ✅ df.filter(col("value") > 100).groupBy("key").count()
    
    2. Repartition by Join Key:
       df1.repartition("key").join(df2.repartition("key"), "key")
    
    3. Coalesce After Filter:
       df.filter(col("value") > 1000).coalesce(8)
    
    4. Use Broadcast for Small Tables:
       large_df.join(broadcast(small_df), "key")
    
    5. Enable Sort-Based Shuffle:
       --conf spark.shuffle.sort.bypassMergeThreshold=200
    """)
    
    # Demonstrate filter before shuffle
    print("\n🧪 EXAMPLE: Filter Before Shuffle")
    
    # Bad: filter after shuffle
    print("   ❌ Filter AFTER shuffle:")
    start = time.time()
    bad = df.groupBy((col("id") % 100).alias("key")).count() \
        .filter(col("count") > 900)
    bad.show(5)
    time_bad = time.time() - start
    print(f"      Time: {time_bad:.3f}s")
    
    # Good: filter before shuffle
    print("   ✅ Filter BEFORE shuffle:")
    start = time.time()
    good = df.filter(col("id") > 10000) \
        .groupBy((col("id") % 100).alias("key")).count()
    good.show(5)
    time_good = time.time() - start
    print(f"      Time: {time_good:.3f}s")


def demonstrate_caching_strategies(spark):
    """
    Effective caching strategies.
    """
    print("\n" + "=" * 80)
    print("CACHING STRATEGIES")
    print("=" * 80)
    
    df = spark.range(0, 1000000) \
        .withColumn("value", col("id") * 2)
    
    print("\n📊 STORAGE LEVELS:")
    print("""
    ┌──────────────────────────┬──────────┬──────────┬─────────────┐
    │ Storage Level            │ Memory   │ Disk     │ Serialized  │
    ├──────────────────────────┼──────────┼──────────┼─────────────┤
    │ MEMORY_ONLY (default)    │ Yes      │ No       │ No          │
    │ MEMORY_AND_DISK          │ Yes      │ Yes      │ No          │
    │ MEMORY_ONLY_SER          │ Yes      │ No       │ Yes         │
    │ MEMORY_AND_DISK_SER      │ Yes      │ Yes      │ Yes         │
    │ DISK_ONLY                │ No       │ Yes      │ Yes         │
    │ MEMORY_ONLY_2            │ Yes (2x) │ No       │ No          │
    │ MEMORY_AND_DISK_2        │ Yes (2x) │ Yes      │ No          │
    └──────────────────────────┴──────────┴──────────┴─────────────┘
    
    2x = Replicated (fault tolerance)
    SER = Serialized (less memory, more CPU)
    """)
    
    print("\n🎯 WHEN TO CACHE:")
    print("""
    ✅ DataFrame used multiple times
    ✅ Expensive transformations (joins, aggregations)
    ✅ Iterative algorithms (ML training)
    ✅ Interactive queries
    
    ❌ Used only once
    ❌ Before filtering (cache after filter!)
    ❌ Very large datasets (OOM risk)
    ❌ Frequent updates
    """)
    
    print("\n🧪 CACHING DEMO:")
    
    # Without cache
    print("   Without cache (multiple actions):")
    start = time.time()
    df.count()
    df.filter(col("value") > 1000).count()
    df.groupBy((col("id") % 10).alias("key")).count().show(5, False)
    time_no_cache = time.time() - start
    print(f"      Total time: {time_no_cache:.3f}s")
    
    # With cache
    print("   With cache:")
    cached_df = df.cache()
    start = time.time()
    cached_df.count()  # Materializes cache
    cached_df.filter(col("value") > 1000).count()
    cached_df.groupBy((col("id") % 10).alias("key")).count().show(5, False)
    time_cache = time.time() - start
    print(f"      Total time: {time_cache:.3f}s")
    print(f"      Speedup: {time_no_cache/time_cache:.2f}x")
    
    # Cleanup
    cached_df.unpersist()
    
    print("\n💡 CACHE MANAGEMENT:")
    print("""
    # Cache DataFrame
    df.cache()  # or df.persist()
    
    # Materialize cache
    df.count()  # or any action
    
    # Check cache status
    # Spark UI → Storage tab
    
    # Remove from cache
    df.unpersist()
    
    # Clear all cache
    spark.catalog.clearCache()
    """)


def demonstrate_aqe(spark):
    """
    Adaptive Query Execution (AQE) features.
    """
    print("\n" + "=" * 80)
    print("ADAPTIVE QUERY EXECUTION (AQE)")
    print("=" * 80)
    
    print("\n⚙️  AQE CONFIGURATION:")
    print("""
    # Enable AQE (Spark 3.0+)
    --conf spark.sql.adaptive.enabled=true
    
    # Coalesce partitions (combine small partitions)
    --conf spark.sql.adaptive.coalescePartitions.enabled=true
    --conf spark.sql.adaptive.coalescePartitions.minPartitionNum=1
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB
    
    # Dynamic join strategy
    --conf spark.sql.adaptive.localShuffleReader.enabled=true
    
    # Skew join optimization
    --conf spark.sql.adaptive.skewJoin.enabled=true
    --conf spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
    --conf spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
    """)
    
    print("\n🎯 AQE FEATURES:")
    print("""
    1. Dynamic Partition Coalescing:
       • Reduces shuffle partitions at runtime
       • Combines small partitions after shuffle
       • Saves overhead from empty/small tasks
    
    2. Dynamic Join Strategy Switching:
       • Switches to broadcast join if table is small
       • Decision made at runtime (not planning time)
       • Based on actual data size, not estimates
    
    3. Dynamic Skew Join Optimization:
       • Detects skewed partitions
       • Splits large partitions into smaller chunks
       • Improves parallelism for skewed data
    """)
    
    df = spark.range(0, 10000)
    
    # Demonstrate AQE partition coalescing
    print("\n🧪 AQE DEMO: Partition Coalescing")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    result = df.groupBy((col("id") % 10).alias("key")).count()
    result.explain()
    result.show()
    
    print("   With AQE enabled:")
    print("   • Started with 200 shuffle partitions")
    print("   • AQE coalesced to ~10 partitions (based on data size)")
    print("   • Check explain() for 'AQE' annotations")


def demonstrate_configuration_checklist():
    """
    Complete configuration checklist.
    """
    print("\n" + "=" * 80)
    print("PERFORMANCE TUNING CHECKLIST")
    print("=" * 80)
    
    print("""
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                 SPARK PERFORMANCE TUNING CHECKLIST                   ║
    ╚══════════════════════════════════════════════════════════════════════╝
    
    📋 MEMORY CONFIGURATION
    ═══════════════════════════════════════════════════════════════════════
    □ Set appropriate executor memory (8-32GB)
    □ Configure driver memory based on collect size
    □ Adjust memory.fraction if using many UDFs (0.5-0.7)
    □ Tune storageFraction based on cache vs shuffle (0.3-0.7)
    
    📋 PARALLELISM
    ═══════════════════════════════════════════════════════════════════════
    □ Set spark.default.parallelism = 2-3x cores
    □ Tune spark.sql.shuffle.partitions based on data size:
      • Small (<1GB): 8-16
      • Medium (1-10GB): 50-100
      • Large (>10GB): 200-500
    □ Target partition size: 100-200MB
    
    📋 SHUFFLE OPTIMIZATION
    ═══════════════════════════════════════════════════════════════════════
    □ Enable shuffle compression (default: true)
    □ Choose codec: lz4 (fast), snappy (balanced)
    □ Filter before shuffle operations
    □ Use broadcast for small tables (<10MB)
    □ Repartition by join key for large-large joins
    
    📋 CACHING
    ═══════════════════════════════════════════════════════════════════════
    □ Cache frequently accessed DataFrames
    □ Use appropriate storage level (MEMORY_AND_DISK safe default)
    □ Cache AFTER filtering, not before
    □ Unpersist when done
    □ Monitor cache usage in Spark UI
    
    📋 ADAPTIVE QUERY EXECUTION (AQE)
    ═══════════════════════════════════════════════════════════════════════
    □ Enable AQE (Spark 3.0+)
    □ Enable partition coalescing
    □ Enable skew join optimization
    □ Let AQE switch join strategies dynamically
    
    📋 SERIALIZATION
    ═══════════════════════════════════════════════════════════════════════
    □ Use KryoSerializer (faster than Java)
    □ Register classes with Kryo for better performance
    
    📋 DATA FORMAT
    ═══════════════════════════════════════════════════════════════════════
    □ Use Parquet (columnar, compressed, predicate pushdown)
    □ Partition data by frequently filtered columns
    □ Use appropriate compression (snappy default)
    
    📋 CODE OPTIMIZATION
    ═══════════════════════════════════════════════════════════════════════
    □ Filter early (predicate pushdown)
    □ Select only needed columns (projection pruning)
    □ Avoid UDFs (use built-in functions)
    □ Use DataFrame API (not RDD) for Catalyst optimization
    □ Check explain() plans
    
    📋 MONITORING
    ═══════════════════════════════════════════════════════════════════════
    □ Monitor Spark UI (http://localhost:4040)
    □ Check for data skew (task duration variance)
    □ Monitor shuffle read/write sizes
    □ Watch for spill to disk
    □ Track GC time (should be <10% of task time)
    """)


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("SPARK PERFORMANCE TUNING")
    print("🎯" * 40)
    
    spark = create_optimized_spark()
    
    demonstrate_memory_tuning()
    demonstrate_parallelism_tuning(spark)
    demonstrate_shuffle_optimization(spark)
    demonstrate_caching_strategies(spark)
    demonstrate_aqe(spark)
    demonstrate_configuration_checklist()
    
    print("\n" + "=" * 80)
    print("✅ PERFORMANCE TUNING COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. Tune shuffle partitions based on data size")
    print("   2. Filter before shuffle operations")
    print("   3. Cache strategically (after filters)")
    print("   4. Enable AQE for automatic optimization")
    print("   5. Use Parquet with partitioning")
    print("   6. Monitor Spark UI continuously")
    print("   7. Target 100-200MB partition size")
    
    spark.stop()


if __name__ == "__main__":
    main()
