"""
14_executor_responsibilities.py
================================

Comprehensive example demonstrating Spark Executor responsibilities.

Executors are worker processes that run on cluster nodes.
They execute tasks and store data for your application.

Executor Responsibilities:
1. Execute tasks on data partitions
2. Cache/persist partitions in memory or disk
3. Shuffle read/write data during wide operations
4. Report results and metrics back to Driver
5. Manage local storage for intermediate data

This example shows what happens inside Executors during job execution.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, expr
from pyspark import StorageLevel
import time


def demonstrate_executor_setup():
    """
    Show how executors are initialized and configured.
    """
    print("=" * 80)
    print("EXECUTOR SETUP & INITIALIZATION")
    print("=" * 80)
    
    # Create Spark session with executor configuration
    spark = SparkSession.builder \
        .appName("ExecutorDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.instances", "2") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    print("\n📊 Executor Configuration:")
    print(f"   Executor Memory: 2g")
    print(f"   Executor Cores: 2 (can run 2 tasks in parallel)")
    print(f"   Memory Fraction: 60% for execution + storage")
    print(f"   Storage Fraction: 50% of memory fraction for caching")
    
    print("\n💾 Executor Memory Breakdown (2GB total):")
    print("   ┌─────────────────────────────────┐")
    print("   │ Reserved: 300MB (15%)           │")
    print("   ├─────────────────────────────────┤")
    print("   │ Execution + Storage: 1.2GB (60%)│")
    print("   │   ├─ Execution: 600MB (50%)     │")
    print("   │   └─ Storage: 600MB (50%)       │")
    print("   ├─────────────────────────────────┤")
    print("   │ User Memory: 500MB (25%)        │")
    print("   └─────────────────────────────────┘")
    
    return spark


def demonstrate_task_execution(spark):
    """
    Show how executors execute tasks on partitions.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR RESPONSIBILITY 1: EXECUTE TASKS ON PARTITIONS")
    print("=" * 80)
    
    # Create data with explicit partitioning
    from pyspark.sql.functions import rand
    
    print("\n📊 Creating dataset with 8 partitions...")
    df = spark.range(0, 1_000_000, numPartitions=8) \
        .withColumn("value", (rand() * 100).cast("int"))
    
    print(f"   Total records: 1,000,000")
    print(f"   Partitions: {df.rdd.getNumPartitions()}")
    print(f"   Records per partition: {1_000_000 // 8:,}")
    
    print("\n🔄 Executors process partitions in parallel:")
    print("   Executor 1 (2 cores):")
    print("      Task 1: Process partition 0 (125K rows)")
    print("      Task 2: Process partition 1 (125K rows)")
    print("   ")
    print("   Executor 2 (2 cores):")
    print("      Task 3: Process partition 2 (125K rows)")
    print("      Task 4: Process partition 3 (125K rows)")
    print("   ")
    print("   (Continues for partitions 4-7)")
    
    print("\n⚡ Executing transformation...")
    start = time.time()
    result_df = df.filter(col("value") > 50)
    count = result_df.count()
    elapsed = time.time() - start
    
    print(f"\n✅ Filtered {count:,} records in {elapsed:.2f}s")
    print(f"   Each executor processed its partitions independently")
    print(f"   No data exchange between executors (narrow transformation)")


def demonstrate_caching_persistence(spark):
    """
    Show how executors cache/persist partitions.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR RESPONSIBILITY 2: CACHE/PERSIST PARTITIONS")
    print("=" * 80)
    
    # Create expensive computation
    from pyspark.sql.functions import rand
    
    df = spark.range(0, 500_000) \
        .withColumn("v1", rand()) \
        .withColumn("v2", rand()) \
        .withColumn("v3", rand()) \
        .withColumn("result", col("v1") + col("v2") + col("v3"))
    
    print("\n📊 Created DataFrame with expensive computation")
    print("   500K records, multiple random columns")
    
    # First run without caching
    print("\n❌ WITHOUT CACHING:")
    start = time.time()
    count1 = df.count()
    elapsed1 = time.time() - start
    
    start = time.time()
    count2 = df.count()
    elapsed2 = time.time() - start
    
    print(f"   First count: {elapsed1:.3f}s")
    print(f"   Second count: {elapsed2:.3f}s")
    print(f"   Total: {elapsed1 + elapsed2:.3f}s")
    print("   ⚠️  Recomputed everything both times!")
    
    # Now with caching
    print("\n✅ WITH CACHING:")
    df_cached = df.cache()
    
    start = time.time()
    count1 = df_cached.count()
    elapsed1 = time.time() - start
    
    print(f"   First count: {elapsed1:.3f}s (computed + cached)")
    print("   ")
    print("   💾 Executors stored partitions in memory:")
    print("      Executor 1: Cached partitions 0-3")
    print("      Executor 2: Cached partitions 4-7")
    
    start = time.time()
    count2 = df_cached.count()
    elapsed2 = time.time() - start
    
    print(f"\n   Second count: {elapsed2:.3f}s (read from cache)")
    print(f"   Total: {elapsed1 + elapsed2:.3f}s")
    print(f"   ⚡ Speedup: {(elapsed1 + elapsed2) / (elapsed1 + elapsed2):.1f}x faster on reuse!")
    
    # Show storage levels
    print("\n📦 Storage Levels:")
    print("   MEMORY_ONLY: Fast, but OOM if doesn't fit")
    print("   MEMORY_AND_DISK: Spill to disk if needed (safer)")
    print("   DISK_ONLY: Slower, but always works")
    print("   MEMORY_ONLY_SER: Serialized (less memory, more CPU)")
    
    # Demonstrate different storage levels
    df_disk = spark.range(0, 100_000).persist(StorageLevel.MEMORY_AND_DISK)
    df_disk.count()
    
    print("\n💾 Cached with MEMORY_AND_DISK:")
    print("   Executor tries memory first")
    print("   If memory full, spills to local disk")
    print("   Executor manages cache eviction (LRU)")
    
    # Cleanup
    df_cached.unpersist()
    df_disk.unpersist()


def demonstrate_shuffle_operations(spark):
    """
    Show how executors handle shuffle read/write during wide operations.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR RESPONSIBILITY 3: SHUFFLE READ/WRITE")
    print("=" * 80)
    
    print("\n📊 Shuffle is needed for wide transformations:")
    print("   - groupBy / reduceByKey")
    print("   - join")
    print("   - repartition")
    print("   - distinct")
    print("   - sortBy")
    
    # Create data for groupBy (causes shuffle)
    from pyspark.sql.functions import rand
    
    df = spark.range(0, 1_000_000) \
        .withColumn("category", (rand() * 10).cast("int")) \
        .withColumn("value", (rand() * 100).cast("int"))
    
    print(f"\n📊 Dataset: 1M records, 10 categories")
    print(f"   Initial partitions: {df.rdd.getNumPartitions()}")
    
    # Perform groupBy (triggers shuffle)
    print("\n🔄 Executing groupBy (wide transformation)...")
    print("\n   SHUFFLE WRITE Phase:")
    print("   ─────────────────────────────────────")
    print("   Executor 1:")
    print("      Task 1: Read partition 0")
    print("              Group by category locally")
    print("              Write shuffle files for each category")
    print("              Creates shuffle blocks on local disk")
    print("   ")
    print("   Executor 2:")
    print("      Task 2: Read partition 1")
    print("              Group by category locally")
    print("              Write shuffle files")
    print("   (Continues for all partitions...)")
    
    start = time.time()
    result = df.groupBy("category").agg(
        count("*").alias("count"),
        _sum("value").alias("total"),
        avg("value").alias("average")
    )
    
    print("\n   SHUFFLE READ Phase:")
    print("   ─────────────────────────────────────")
    print("   Executor 1:")
    print("      Task 1: Fetch category 0 data from all executors")
    print("              Read shuffle blocks over network")
    print("              Perform final aggregation")
    print("   ")
    print("   Executor 2:")
    print("      Task 2: Fetch category 1 data from all executors")
    print("              Aggregate locally")
    print("   (Continues for all categories...)")
    
    result_count = result.count()
    elapsed = time.time() - start
    
    print(f"\n✅ Shuffle complete in {elapsed:.2f}s")
    print(f"   Result: {result_count} categories")
    print(f"   Output partitions: {result.rdd.getNumPartitions()}")
    
    print("\n💾 Executor shuffle storage:")
    print("   - Shuffle write: Local disk (spark.local.dir)")
    print("   - Shuffle read: Network fetch from other executors")
    print("   - Shuffle cleanup: Automatic after job completes")


def demonstrate_shuffle_metrics(spark):
    """
    Show detailed shuffle metrics that executors track.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR SHUFFLE METRICS")
    print("=" * 80)
    
    # Create scenario with significant shuffle
    df = spark.range(0, 1_000_000).repartition(4)
    
    print("\n📊 Repartitioning 1M records from 8 → 4 partitions")
    print("   This causes a full shuffle")
    
    print("\n📈 Executor metrics during shuffle:")
    print("   ")
    print("   WRITE metrics (per executor):")
    print("      - Shuffle bytes written")
    print("      - Shuffle records written")
    print("      - Shuffle write time")
    print("   ")
    print("   READ metrics (per executor):")
    print("      - Shuffle bytes read")
    print("      - Shuffle records read")
    print("      - Shuffle read time")
    print("      - Shuffle remote reads (network)")
    print("      - Shuffle local reads (same executor)")
    
    start = time.time()
    count = df.count()
    elapsed = time.time() - start
    
    print(f"\n✅ Shuffle complete: {count:,} records in {elapsed:.2f}s")
    
    print("\n🎯 Executor shuffle optimization:")
    print("   - Combine records locally before shuffle")
    print("   - Compress shuffle data (spark.shuffle.compress)")
    print("   - Use fast serialization (Kryo)")
    print("   - Minimize shuffle data size")


def demonstrate_executor_failures():
    """
    Show how executors handle failures and recovery.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR FAILURE HANDLING")
    print("=" * 80)
    
    print("\n⚠️  Executor failure scenarios:")
    print("   1. Out of Memory (OOM)")
    print("   2. Disk full (shuffle writes)")
    print("   3. Network timeout")
    print("   4. Task failures (bad data)")
    print("   5. Node hardware failure")
    
    print("\n🔄 Recovery mechanisms:")
    print("   ")
    print("   1. Task Retry:")
    print("      - Driver detects task failure")
    print("      - Reschedules task on another executor")
    print("      - Up to spark.task.maxFailures (default: 4)")
    print("   ")
    print("   2. Executor Replacement:")
    print("      - Driver detects executor lost")
    print("      - Requests new executor from cluster manager")
    print("      - Recomputes lost partitions from lineage")
    print("   ")
    print("   3. Lineage Reconstruction:")
    print("      - Spark remembers computation graph (DAG)")
    print("      - Can recompute any lost partition")
    print("      - No need to recompute entire job")
    
    print("\n💾 Checkpointing for fault tolerance:")
    print("   - Break lineage for long pipelines")
    print("   - Save intermediate results to reliable storage")
    print("   - Avoid recomputing from beginning")


def demonstrate_executor_memory_management(spark):
    """
    Show how executors manage memory for execution and storage.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR MEMORY MANAGEMENT")
    print("=" * 80)
    
    print("\n💾 Unified Memory Manager (Spark 1.6+):")
    print("   ")
    print("   Execution Memory (shuffles, joins, sorts):")
    print("      - Used for intermediate results")
    print("      - Can borrow from storage if available")
    print("      - Evicted when storage needs memory")
    print("   ")
    print("   Storage Memory (caching):")
    print("      - Used for cached/persisted data")
    print("      - Can borrow from execution if available")
    print("      - Evicted when execution needs memory (LRU)")
    
    # Create scenario showing memory usage
    from pyspark.sql.functions import rand
    
    df1 = spark.range(0, 500_000).withColumn("v1", rand())
    df2 = spark.range(0, 500_000).withColumn("v2", rand())
    
    print("\n📊 Memory usage example:")
    print("   ")
    print("   1. Cache df1 (uses storage memory)")
    df1.cache().count()
    print("      Executor: 500K records cached")
    
    print("   ")
    print("   2. Join df1 with df2 (uses execution memory)")
    result = df1.join(df2, df1.id == df2.id)
    print("      Executor: Execution memory for join operation")
    
    print("   ")
    print("   3. If memory pressure occurs:")
    print("      - Evict least recently used cached partitions")
    print("      - Spill execution data to disk if needed")
    print("      - Balance between execution and storage dynamically")
    
    df1.unpersist()


def main():
    """
    Main function demonstrating all Executor responsibilities.
    """
    print("\n" + "⚙️ " * 40)
    print("SPARK EXECUTOR RESPONSIBILITIES - COMPLETE GUIDE")
    print("⚙️ " * 40)
    
    # Setup
    spark = demonstrate_executor_setup()
    
    # 1. Task execution
    demonstrate_task_execution(spark)
    
    # 2. Caching and persistence
    demonstrate_caching_persistence(spark)
    
    # 3. Shuffle operations
    demonstrate_shuffle_operations(spark)
    
    # 4. Shuffle metrics
    demonstrate_shuffle_metrics(spark)
    
    # 5. Failure handling
    demonstrate_executor_failures()
    
    # 6. Memory management
    demonstrate_executor_memory_management(spark)
    
    print("\n" + "=" * 80)
    print("✅ EXECUTOR RESPONSIBILITIES COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. Executors execute tasks on data partitions in parallel")
    print("   2. Each executor core can run one task at a time")
    print("   3. Executors cache partitions in memory/disk for reuse")
    print("   4. Shuffle write: Executors write data to local disk")
    print("   5. Shuffle read: Executors fetch data over network")
    print("   6. Unified memory: Dynamic balance execution/storage")
    print("   7. LRU eviction: Remove least recently used cached data")
    print("   8. Fault tolerance: Recompute lost partitions from lineage")
    
    print("\n💡 Executor optimization tips:")
    print("   - More cores = more parallel tasks")
    print("   - More memory = more caching + less spill")
    print("   - Cache frequently reused data")
    print("   - Minimize shuffle data size")
    print("   - Use broadcast for small lookup tables")
    print("   - Monitor executor memory usage in Spark UI")
    
    print("\n⚠️  Common executor issues:")
    print("   - OOM: Increase executor memory or reduce partition size")
    print("   - Slow tasks: Data skew, increase parallelism")
    print("   - Shuffle spill: Increase memory or reduce shuffle data")
    print("   - Lost executors: Check node health, increase timeouts")
    
    spark.stop()


if __name__ == "__main__":
    main()
