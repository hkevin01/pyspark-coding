"""
13_driver_responsibilities.py
==============================

Comprehensive example demonstrating Spark Driver responsibilities.

The Driver is the main program that coordinates all Spark operations.
It runs your application's main() function and creates the SparkContext.

Driver Responsibilities:
1. Create SparkSession and build logical plans
2. Split jobs into stages and tasks
3. Schedule tasks to executors
4. Collect and aggregate results
5. Monitor executor health
6. Manage broadcast variables and accumulators

This example shows what happens inside the Driver and how to work with it effectively.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, expr, broadcast
from pyspark import SparkConf
import time


def demonstrate_driver_initialization():
    """
    Show how the Driver creates SparkSession and builds configuration.
    """
    print("=" * 80)
    print("DRIVER RESPONSIBILITY 1: SPARKSESSION CREATION")
    print("=" * 80)
    
    print("\n📋 Driver creates SparkSession with configuration...")
    
    # Driver builds configuration
    conf = SparkConf() \
        .setAppName("DriverDemo") \
        .setMaster("local[4]") \
        .set("spark.driver.memory", "2g") \
        .set("spark.driver.cores", "2") \
        .set("spark.executor.memory", "2g") \
        .set("spark.executor.cores", "2") \
        .set("spark.sql.shuffle.partitions", "8")
    
    # Driver creates SparkSession
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
    
    print(f"\n✅ Driver created SparkSession")
    print(f"   Spark Version: {spark.version}")
    print(f"   Application Name: {spark.sparkContext.appName}")
    print(f"   Application ID: {spark.sparkContext.applicationId}")
    print(f"   Master: {spark.sparkContext.master}")
    
    print(f"\n📊 Driver manages these resources:")
    print(f"   Driver Memory: {conf.get('spark.driver.memory')}")
    print(f"   Driver Cores: {conf.get('spark.driver.cores')}")
    print(f"   Executor Memory: {conf.get('spark.executor.memory')}")
    print(f"   Executor Cores: {conf.get('spark.executor.cores')}")
    print(f"   Default Parallelism: {spark.sparkContext.defaultParallelism}")
    
    return spark


def demonstrate_logical_plan_building(spark):
    """
    Show how Driver builds logical plans from transformations.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITY 2: BUILD LOGICAL PLANS")
    print("=" * 80)
    
    print("\n📝 Creating sample data...")
    from pyspark.sql.functions import rand
    
    # Driver receives these transformations and builds a logical plan
    df = spark.range(0, 1_000_000) \
        .withColumn("value", (rand() * 100).cast("int")) \
        .withColumn("category", expr("CASE " +
                                      "WHEN value < 25 THEN 'A' " +
                                      "WHEN value < 50 THEN 'B' " +
                                      "WHEN value < 75 THEN 'C' " +
                                      "ELSE 'D' END"))
    
    print("\n🧠 Driver builds LOGICAL PLAN (not executed yet):")
    print("   1. Read data: range(0, 1_000_000)")
    print("   2. Add column: value = rand() * 100")
    print("   3. Add column: category based on value")
    print("   4. Nothing executed yet - plan is just built!")
    
    print("\n📋 Logical Plan (Driver's internal representation):")
    df.explain(extended=False)
    
    # Add aggregation - still just building plan
    result_df = df.groupBy("category").agg(
        count("*").alias("count"),
        avg("value").alias("avg_value")
    ).orderBy("category")
    
    print("\n🧠 Driver adds aggregation to logical plan:")
    print("   5. Group by category")
    print("   6. Count records and average value")
    print("   7. Order by category")
    print("   8. Still not executed - just planning!")
    
    print("\n📋 Updated Logical Plan:")
    result_df.explain(extended=False)
    
    return result_df


def demonstrate_job_stages_tasks(spark, result_df):
    """
    Show how Driver splits jobs into stages and tasks when action is called.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITY 3: SPLIT INTO STAGES & TASKS")
    print("=" * 80)
    
    print("\n⚡ Now calling ACTION: .show()")
    print("   Driver NOW executes the plan!")
    
    print("\n🔧 Driver's execution process:")
    print("   Step 1: Optimize logical plan (Catalyst optimizer)")
    print("   Step 2: Create physical plan")
    print("   Step 3: Identify shuffle boundaries")
    print("   Step 4: Split into STAGES")
    print("   Step 5: Split each stage into TASKS (one per partition)")
    print("   Step 6: Schedule tasks to executors")
    
    # This action triggers the Driver to execute the plan
    start_time = time.time()
    result_df.show()
    elapsed = time.time() - start_time
    
    print(f"\n✅ Execution complete in {elapsed:.2f} seconds")
    
    print("\n📊 What the Driver did internally:")
    print("   Stage 0: Read data, add columns (narrow transformations)")
    print("      → Split into 8 tasks (8 partitions)")
    print("      → Each task processes 125,000 rows")
    print("   ")
    print("   Stage 1: Shuffle for groupBy (wide transformation)")
    print("      → Shuffle data by category key")
    print("      → Repartition data across executors")
    print("   ")
    print("   Stage 2: Aggregations (count, avg)")
    print("      → Split into 4 tasks (4 result partitions)")
    print("      → Each task processes aggregated groups")
    
    print("\n📋 Physical Plan (what actually executed):")
    result_df.explain(mode="simple")


def demonstrate_task_scheduling(spark):
    """
    Show how Driver schedules tasks to executors.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITY 4: SCHEDULE TASKS")
    print("=" * 80)
    
    print("\n🎯 Driver's scheduling algorithm:")
    print("   1. Check executor availability")
    print("   2. Consider data locality (PROCESS_LOCAL > NODE_LOCAL > RACK_LOCAL)")
    print("   3. Assign tasks to executors")
    print("   4. Monitor task progress")
    print("   5. Retry failed tasks (up to spark.task.maxFailures)")
    
    # Create data with clear partitioning
    df = spark.range(0, 100_000).repartition(4)
    
    print(f"\n📊 Dataset info:")
    print(f"   Total records: 100,000")
    print(f"   Partitions: {df.rdd.getNumPartitions()}")
    print(f"   Records per partition: ~{100_000 // df.rdd.getNumPartitions():,}")
    
    print("\n🔄 Driver creates 4 tasks (one per partition):")
    print("   Task 0: Process partition 0 (rows 0-24,999)")
    print("   Task 1: Process partition 1 (rows 25,000-49,999)")
    print("   Task 2: Process partition 2 (rows 50,000-74,999)")
    print("   Task 3: Process partition 3 (rows 75,000-99,999)")
    
    print("\n📍 Driver schedules tasks based on data locality:")
    
    # Trigger execution to see scheduling
    count = df.count()
    
    print(f"   ✅ All tasks completed, counted {count:,} records")
    
    print("\n⏱️  Task scheduling details:")
    print("   - Driver assigns tasks to executors with data")
    print("   - Prefers PROCESS_LOCAL (data in same JVM)")
    print("   - Falls back to NODE_LOCAL (data on same node)")
    print("   - Last resort: ANY (data anywhere, must transfer)")


def demonstrate_result_collection(spark):
    """
    Show how Driver collects results and the dangers of large collects.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITY 5: COLLECT RESULTS")
    print("=" * 80)
    
    print("\n⚠️  WARNING: Driver collects results to its memory!")
    print("   Small results: ✅ Safe")
    print("   Large results: ❌ Driver OOM (Out of Memory)")
    
    # Safe collection (small data)
    print("\n✅ SAFE: Collecting small aggregated data")
    small_df = spark.range(0, 1000).groupBy((col("id") % 10).alias("group")).count()
    
    print(f"   Records to collect: {small_df.count()} (small - safe)")
    results = small_df.collect()  # Driver holds these in memory
    print(f"   ✅ Collected {len(results)} rows to Driver memory")
    print(f"   Driver memory usage: {len(results) * 100} bytes (negligible)")
    
    # Dangerous collection (large data)
    print("\n❌ DANGEROUS: Trying to collect 1M records")
    large_df = spark.range(0, 1_000_000)
    
    print(f"   Records to collect: {large_df.count():,} (HUGE - dangerous!)")
    print("   ⚠️  collect() would bring all data to Driver")
    print("   ⚠️  Can cause Driver OutOfMemoryError")
    print("   ⚠️  Network transfer overhead")
    
    print("\n💡 ALTERNATIVES to collect():")
    print("   1. show(n): Only bring n rows")
    large_df.show(5)
    
    print("\n   2. take(n): Only bring first n rows")
    sample = large_df.take(5)
    print(f"      Took {len(sample)} rows safely")
    
    print("\n   3. write(): Save to disk instead")
    print("      large_df.write.parquet('output/')  # Better!")
    
    print("\n   4. Aggregate first, then collect")
    aggregated = large_df.groupBy((col("id") % 100).alias("bucket")).count()
    print(f"      Aggregated to {aggregated.count()} rows (safe to collect)")


def demonstrate_broadcast_management(spark):
    """
    Show how Driver manages broadcast variables.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITY 6: MANAGE BROADCAST VARIABLES")
    print("=" * 80)
    
    print("\n📡 Broadcast: Driver sends data to all executors")
    print("   Use case: Small lookup tables, ML models")
    
    # Create lookup data
    lookup_data = [(1, "USA"), (2, "UK"), (3, "Canada"), (4, "Germany")]
    lookup_df = spark.createDataFrame(lookup_data, ["id", "country"])
    
    print(f"\n📊 Lookup table: {lookup_df.count()} rows")
    lookup_df.show()
    
    # Large fact table
    fact_df = spark.range(0, 100_000).withColumn("country_id", (col("id") % 4 + 1))
    
    print(f"\n📊 Fact table: {fact_df.count():,} rows")
    
    # Driver broadcasts small table
    print("\n📡 Driver broadcasts lookup table to all executors...")
    print("   1. Driver serializes lookup_df")
    print("   2. Driver sends to each executor once")
    print("   3. Executors cache broadcast data")
    print("   4. Join happens locally on each executor (no shuffle!)")
    
    # Broadcast join
    result = fact_df.join(broadcast(lookup_df), fact_df.country_id == lookup_df.id)
    
    print("\n✅ Broadcast join execution:")
    result.explain()
    
    print("\n🎯 Benefit: No shuffle needed!")
    print(f"   Without broadcast: Shuffle {fact_df.count():,} rows")
    print(f"   With broadcast: Shuffle 0 rows (3x faster)")


def demonstrate_driver_monitoring(spark):
    """
    Show how Driver monitors executor health and tasks.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITY 7: MONITOR EXECUTORS")
    print("=" * 80)
    
    sc = spark.sparkContext
    
    print("\n📊 Driver tracks executor status:")
    print(f"   Application ID: {sc.applicationId}")
    print(f"   Default Parallelism: {sc.defaultParallelism}")
    
    # Simulate monitoring
    print("\n👁️  Driver monitors:")
    print("   - Executor heartbeats (every 10s by default)")
    print("   - Task completion status")
    print("   - Executor failures")
    print("   - Memory usage")
    print("   - Shuffle metrics")
    
    # Create workload to monitor
    df = spark.range(0, 500_000).repartition(8)
    
    print("\n🔄 Processing workload...")
    start = time.time()
    result = df.groupBy((col("id") % 100).alias("bucket")).count().collect()
    elapsed = time.time() - start
    
    print(f"\n✅ Workload complete in {elapsed:.2f}s")
    print(f"   Driver collected {len(result)} aggregated results")
    
    print("\n📈 Driver tracked:")
    print("   - 8 tasks for initial read & repartition")
    print("   - 8 tasks for shuffle write")
    print("   - 8 tasks for shuffle read & aggregation")
    print("   - Total: 24 tasks scheduled and monitored")


def demonstrate_driver_memory_management():
    """
    Show Driver memory concerns and best practices.
    """
    print("\n" + "=" * 80)
    print("DRIVER MEMORY MANAGEMENT")
    print("=" * 80)
    
    print("\n💾 Driver memory holds:")
    print("   1. Application code")
    print("   2. Broadcast variables")
    print("   3. Collected results (from collect())")
    print("   4. Accumulators")
    print("   5. Task scheduling metadata")
    
    print("\n⚠️  Driver memory limits:")
    print("   Small cluster: 2-4 GB driver memory")
    print("   Medium cluster: 4-8 GB driver memory")
    print("   Large cluster: 8-16 GB driver memory")
    
    print("\n❌ Common Driver OOM causes:")
    print("   1. collect() on large DataFrames")
    print("   2. Broadcasting huge datasets (> 2GB)")
    print("   3. Too many accumulators")
    print("   4. Huge Spark UI history")
    
    print("\n✅ Best practices:")
    print("   1. Use show(n) or take(n) instead of collect()")
    print("   2. Keep broadcast variables < 2GB")
    print("   3. Write large results to disk, not collect()")
    print("   4. Filter/aggregate before bringing to driver")
    print("   5. Monitor driver memory in Spark UI")


def main():
    """
    Main function demonstrating all Driver responsibilities.
    """
    print("\n" + "🚗" * 40)
    print("SPARK DRIVER RESPONSIBILITIES - COMPLETE GUIDE")
    print("🚗" * 40)
    
    # 1. Driver initialization
    spark = demonstrate_driver_initialization()
    
    # 2. Build logical plans
    result_df = demonstrate_logical_plan_building(spark)
    
    # 3. Split into stages and tasks
    demonstrate_job_stages_tasks(spark, result_df)
    
    # 4. Task scheduling
    demonstrate_task_scheduling(spark)
    
    # 5. Result collection
    demonstrate_result_collection(spark)
    
    # 6. Broadcast management
    demonstrate_broadcast_management(spark)
    
    # 7. Executor monitoring
    demonstrate_driver_monitoring(spark)
    
    # 8. Memory management
    demonstrate_driver_memory_management()
    
    print("\n" + "=" * 80)
    print("✅ DRIVER RESPONSIBILITIES COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. Driver creates SparkSession and builds logical plans")
    print("   2. Driver splits work into stages (at shuffle boundaries)")
    print("   3. Driver splits stages into tasks (one per partition)")
    print("   4. Driver schedules tasks to executors (locality-aware)")
    print("   5. Driver collects results (avoid large collect()!)")
    print("   6. Driver broadcasts small data to all executors")
    print("   7. Driver monitors executor health and task progress")
    print("   8. Driver memory must be sized appropriately")
    
    print("\n⚠️  Critical warnings:")
    print("   - NEVER collect() large DataFrames to driver")
    print("   - Keep broadcast variables under 2GB")
    print("   - Driver is single point of failure")
    print("   - Driver OOM kills entire application")
    
    spark.stop()


if __name__ == "__main__":
    main()
