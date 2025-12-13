"""
05_fault_tolerance.py
=====================

Master fault tolerance, checkpointing, and recovery in distributed clusters.

When executor nodes fail or computations are interrupted, Spark uses
lineage to recompute lost data. Learn checkpointing to truncate lineage
and optimize recovery.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when
import time
import os


def create_spark(checkpoint_dir=None):
    """Create Spark session with optional checkpoint directory."""
    builder = SparkSession.builder \
        .appName("FaultTolerance") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "8")
    
    if checkpoint_dir:
        builder = builder.config("spark.sql.streaming.checkpointLocation", checkpoint_dir)
    
    spark = builder.getOrCreate()
    
    if checkpoint_dir:
        spark.sparkContext.setCheckpointDir(checkpoint_dir)
        print(f"✅ Checkpoint directory: {checkpoint_dir}")
    
    return spark


def demonstrate_lineage_basics(spark):
    """Understand RDD lineage and DAG."""
    print("=" * 70)
    print("1. LINEAGE BASICS")
    print("=" * 70)
    
    # Create transformation chain
    df1 = spark.range(1, 1000001).toDF("id")
    df2 = df1.withColumn("value", col("id") * 2)
    df3 = df2.filter(col("value") > 100)
    df4 = df3.withColumn("squared", col("value") * col("value"))
    df5 = df4.groupBy((col("id") % 10).alias("bucket")).count()
    
    print("\n📊 Transformation chain:")
    print("   1. spark.range(1000000) - Create DataFrame")
    print("   2. withColumn('value') - Transform")
    print("   3. filter(value > 100) - Filter")
    print("   4. withColumn('squared') - Transform")
    print("   5. groupBy('bucket').count() - Aggregate")
    
    # Explain plan
    print("\n🔍 Execution plan:")
    df5.explain(extended=False)
    
    print("\n💡 Lineage Concept:")
    print("   - Spark tracks transformation chain (lineage)")
    print("   - If data is lost, recompute from source")
    print("   - Long lineage = expensive recovery")
    print("   - Checkpointing truncates lineage")


def demonstrate_persistence_levels(spark):
    """Compare different persistence strategies."""
    print("\n" + "=" * 70)
    print("2. PERSISTENCE LEVELS")
    print("=" * 70)
    
    from pyspark import StorageLevel
    
    # Create expensive computation
    data = spark.range(1, 5000001).toDF("id") \
        .withColumn("value1", rand() * 1000) \
        .withColumn("value2", rand() * 1000)
    
    print("📊 Testing persistence levels:")
    print(f"   Dataset: {data.count():,} rows")
    
    # No caching
    print("\n❌ No caching (recompute every time):")
    start = time.time()
    data.filter(col("value1") > 500).count()
    time1 = time.time() - start
    data.filter(col("value2") > 500).count()
    time2 = time.time() - start - time1
    print(f"   First query: {time1:.3f}s")
    print(f"   Second query: {time2:.3f}s")
    print(f"   Total: {time1 + time2:.3f}s")
    
    # Memory only
    print("\n✅ MEMORY_ONLY:")
    data_mem = data.persist(StorageLevel.MEMORY_ONLY)
    data_mem.count()  # Materialize
    start = time.time()
    data_mem.filter(col("value1") > 500).count()
    time3 = time.time() - start
    data_mem.filter(col("value2") > 500).count()
    time4 = time.time() - start - time3
    print(f"   First query: {time3:.3f}s")
    print(f"   Second query: {time4:.3f}s")
    print(f"   Total: {time3 + time4:.3f}s")
    print(f"   Speedup: {(time1 + time2) / (time3 + time4):.2f}x")
    print(f"   ⚠️  Risk: If memory full, partitions evicted")
    data_mem.unpersist()
    
    # Memory and disk
    print("\n✅ MEMORY_AND_DISK:")
    data_disk = data.persist(StorageLevel.MEMORY_AND_DISK)
    data_disk.count()  # Materialize
    start = time.time()
    data_disk.filter(col("value1") > 500).count()
    time5 = time.time() - start
    data_disk.filter(col("value2") > 500).count()
    time6 = time.time() - start - time5
    print(f"   First query: {time5:.3f}s")
    print(f"   Second query: {time6:.3f}s")
    print(f"   Total: {time5 + time6:.3f}s")
    print(f"   ✅ Safer: Spills to disk if memory full")
    data_disk.unpersist()
    
    print("\n📊 Persistence Levels:")
    print("""
    MEMORY_ONLY          - Fastest, risky if memory full
    MEMORY_AND_DISK      - Safe fallback to disk
    MEMORY_ONLY_SER      - Serialized (saves memory, slower)
    MEMORY_AND_DISK_SER  - Serialized with disk fallback
    DISK_ONLY            - Only disk (slowest)
    OFF_HEAP             - Use off-heap memory (advanced)
    """)


def demonstrate_checkpointing(spark):
    """Demonstrate checkpointing to truncate lineage."""
    print("\n" + "=" * 70)
    print("3. CHECKPOINTING (Lineage Truncation)")
    print("=" * 70)
    
    # Setup checkpoint directory
    checkpoint_dir = "/tmp/spark-checkpoint"
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)
    
    print(f"📁 Checkpoint directory: {checkpoint_dir}")
    
    # Create long lineage
    print("\n📊 Building long lineage (20 transformations):")
    df = spark.range(1, 1000001).toDF("id")
    
    for i in range(20):
        df = df.withColumn(f"col_{i}", col("id") * (i + 1))
        if i % 5 == 0:
            print(f"   Transformation {i + 1}/20...")
    
    print("\n❌ Without checkpointing:")
    start = time.time()
    result1 = df.count()
    time1 = time.time() - start
    print(f"   Count: {result1:,}")
    print(f"   Time: {time1:.3f}s")
    print(f"   ⚠️  Long lineage: If failure, recompute all 20 steps")
    
    # With checkpointing
    print("\n✅ With checkpointing after 10 transformations:")
    df = spark.range(1, 1000001).toDF("id")
    
    for i in range(10):
        df = df.withColumn(f"col_{i}", col("id") * (i + 1))
    
    # Checkpoint here (truncates lineage)
    print("   💾 Checkpointing at step 10...")
    df = df.checkpoint()  # Triggers computation and saves
    print("   ✅ Lineage truncated! Starting from checkpoint.")
    
    for i in range(10, 20):
        df = df.withColumn(f"col_{i}", col("id") * (i + 1))
    
    start = time.time()
    result2 = df.count()
    time2 = time.time() - start
    print(f"   Count: {result2:,}")
    print(f"   Time: {time2:.3f}s")
    print(f"   ✅ Recovery: Only recompute steps 11-20 (not 1-10)")
    
    print("\n💡 When to Checkpoint:")
    print("   1. After expensive shuffles/joins")
    print("   2. Before iterative algorithms (ML)")
    print("   3. After 10+ transformations")
    print("   4. When lineage becomes complex")


def demonstrate_fault_recovery(spark):
    """Simulate fault recovery scenarios."""
    print("\n" + "=" * 70)
    print("4. FAULT RECOVERY SIMULATION")
    print("=" * 70)
    
    print("""
📊 Scenario: Processing 1TB dataset with 100 workers

WITHOUT CHECKPOINTING:
----------------------
1. ❌ Worker 50 fails at 80% progress
2. ⚠️  Recompute ALL partitions from source (1TB)
3. ❌ Restart from beginning
4. ⏱️  Total time: 2x original time

WITH CHECKPOINTING (every 25%):
--------------------------------
1. ✅ Worker 50 fails at 80% progress
2. ✅ Last checkpoint at 75%
3. ✅ Recompute only 75% → 80% (250GB)
4. ⏱️  Total time: 1.05x original time (5% overhead)

Recovery Time Comparison:
-------------------------
No Checkpoint:    100% recomputation
Checkpoint 50%:   50% recomputation (avg)
Checkpoint 25%:   25% recomputation (avg)
Checkpoint 10%:   10% recomputation (avg)

⚠️  Trade-off: More checkpoints = more disk I/O overhead
    """)
    
    # Simulate recovery
    print("\n🔄 Simulating recovery:")
    data = spark.range(1, 100001).toDF("id") \
        .withColumn("value", rand() * 1000)
    
    # Stage 1: Load and transform
    print("   Stage 1: Load and transform (25%)")
    stage1 = data.withColumn("transformed", col("value") * 2)
    
    # Stage 2: Filter and aggregate
    print("   Stage 2: Filter and aggregate (50%)")
    stage2 = stage1.filter(col("transformed") > 100) \
                   .groupBy((col("id") % 10).alias("bucket")) \
                   .count()
    
    # Checkpoint
    print("   💾 Checkpoint at 50% (truncate lineage)")
    stage2_checkpoint = stage2.checkpoint()
    
    # Stage 3: More transformations
    print("   Stage 3: Additional transformations (75%)")
    stage3 = stage2_checkpoint.withColumn("doubled", col("count") * 2)
    
    # Stage 4: Final aggregation
    print("   Stage 4: Final aggregation (100%)")
    result = stage3.agg({"doubled": "sum"}).collect()[0][0]
    
    print(f"\n   ✅ Result: {result:,.0f}")
    print("   ✅ If failure after checkpoint, only recompute stages 3-4")


def demonstrate_storage_strategies(spark):
    """Compare storage strategies for fault tolerance."""
    print("\n" + "=" * 70)
    print("5. STORAGE STRATEGIES")
    print("=" * 70)
    
    print("""
🗄️  Storage Options for Checkpoints:

1. Local Disk (Development):
   ✅ Fast for testing
   ❌ Lost if node fails
   Path: file:///tmp/spark-checkpoint

2. HDFS (Hadoop Clusters):
   ✅ Replicated across nodes (3x default)
   ✅ Survives node failures
   ✅ High throughput
   Path: hdfs://namenode:8020/checkpoint

3. Amazon S3 (Cloud):
   ✅ Durable (11 9's)
   ✅ No cluster dependency
   ⚠️  Higher latency
   Path: s3a://bucket/checkpoint

4. Azure Blob Storage:
   ✅ Durable cloud storage
   ✅ Integrated with Azure
   Path: wasbs://container@account.blob.core.windows.net/checkpoint

5. Google Cloud Storage:
   ✅ Durable cloud storage
   ✅ Integrated with GCP
   Path: gs://bucket/checkpoint

📊 Performance Comparison:

Storage          Write Speed   Read Speed   Durability
--------         -----------   ----------   ----------
Local Disk       Fastest       Fastest      Low (single node)
HDFS             Fast          Fast         High (3x replication)
S3               Medium        Medium       Very High (11 9's)
Azure Blob       Medium        Medium       Very High
GCS              Medium        Medium       Very High

💡 Recommendation:
   - Development: Local disk
   - Production (on-prem): HDFS
   - Production (cloud): S3/Azure/GCS
    """)


def demonstrate_best_practices(spark):
    """Best practices for fault tolerance."""
    print("\n" + "=" * 70)
    print("6. FAULT TOLERANCE BEST PRACTICES")
    print("=" * 70)
    
    print("""
🎯 Checkpointing Strategy:

1. ✅ Checkpoint after expensive operations
   - Large shuffles (join, groupBy)
   - Complex aggregations
   - 10+ sequential transformations

2. ✅ Choose right checkpoint frequency
   Checkpoint every:    Recovery cost:
   ------------------   ---------------
   Never                100% recompute
   End only             50% recompute (avg)
   2 checkpoints        33% recompute (avg)
   4 checkpoints        20% recompute (avg)

3. ✅ Use durable storage in production
   # Development
   sc.setCheckpointDir("file:///tmp/checkpoint")
   
   # Production (HDFS)
   sc.setCheckpointDir("hdfs://namenode:8020/checkpoint")
   
   # Production (S3)
   sc.setCheckpointDir("s3a://bucket/checkpoint")

4. ✅ Clean up old checkpoints
   # Streaming: automatic cleanup
   spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
   
   # Batch: manual cleanup with retention
   # Keep last 7 days of checkpoints

5. ✅ Combine caching and checkpointing
   df.cache()       # Fast repeated access
   df.checkpoint()  # Truncate lineage, durable storage

⚠️  Common Mistakes:

1. ❌ Over-checkpointing
   Too frequent = excessive disk I/O overhead
   
2. ❌ No checkpointing for iterative algorithms
   ML training (100+ iterations) needs checkpoints
   
3. ❌ Using local disk in production
   Node failure loses checkpoint data
   
4. ❌ Not cleaning up checkpoints
   Fills up disk over time
   
5. ❌ Checkpointing small datasets
   Overhead > benefit for < 1GB data

📊 Decision Tree:

                    Start
                      |
           Is lineage complex (>10 steps)?
          /                                \\
        No                                 Yes
        |                                   |
   Skip checkpoint              Is data > 1GB?
                              /                \\
                            No                 Yes
                            |                   |
                       Use cache()        Use checkpoint()
                                               |
                                    Which storage?
                                    /      |      \\
                                 Dev    HDFS    Cloud
                                  |       |       |
                              Local   Hadoop   S3/Azure

🔧 Configuration:

# Enable checkpoint cleanup
spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")

# Checkpoint interval for streaming
spark.conf.set("spark.streaming.checkpoint.interval", "10s")

# Reliable checkpointing (write twice for durability)
spark.conf.set("spark.checkpoint.compress", "true")
    """)


def main():
    # Create checkpoint directory
    checkpoint_dir = "/tmp/spark-checkpoint-demo"
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    spark = create_spark(checkpoint_dir)
    
    print("💾 FAULT TOLERANCE & CHECKPOINTING")
    print("=" * 70)
    
    demonstrate_lineage_basics(spark)
    demonstrate_persistence_levels(spark)
    demonstrate_checkpointing(spark)
    demonstrate_fault_recovery(spark)
    demonstrate_storage_strategies(spark)
    demonstrate_best_practices(spark)
    
    print("\n" + "=" * 70)
    print("✅ FAULT TOLERANCE DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Checkpointing truncates lineage for faster recovery")
    print("   2. Checkpoint after expensive shuffles (>10 transformations)")
    print("   3. Use HDFS/S3 for production (not local disk)")
    print("   4. Balance checkpoint frequency vs overhead")
    print("   5. Cache for speed, checkpoint for fault tolerance")
    print("   6. Enable automatic checkpoint cleanup")
    print(f"\n📁 Checkpoint directory: {checkpoint_dir}")
    
    spark.stop()


if __name__ == "__main__":
    main()
