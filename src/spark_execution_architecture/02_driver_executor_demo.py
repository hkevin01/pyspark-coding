"""
02_driver_executor_demo.py
===========================

Driver and Executor Responsibilities Demo

Demonstrates:
- Driver responsibilities (planning, scheduling, coordination)
- Executor responsibilities (task execution, data storage)
- Communication patterns
- Resource allocation
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, sum as _sum
from pyspark.sql.types import StringType
import time


def create_spark():
    """Create Spark session with explicit configuration."""
    return SparkSession.builder \
        .appName("DriverExecutorDemo") \
        .master("local[4]") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def demonstrate_driver_responsibilities():
    """
    Show what the driver does.
    """
    print("\n" + "=" * 80)
    print("DRIVER RESPONSIBILITIES")
    print("=" * 80)
    
    spark = create_spark()
    
    print("\n🎯 DRIVER TASKS:")
    print("=" * 80)
    
    # 1. Maintains SparkSession
    print("\n1️⃣  Maintains SparkSession")
    print(f"   SparkSession created: {spark}")
    print(f"   App Name: {spark.sparkContext.appName}")
    print(f"   Master: {spark.sparkContext.master}")
    
    # 2. Builds execution plans
    print("\n2️⃣  Builds Execution Plans (Logical → Physical)")
    df = spark.range(0, 1000).toDF("id")
    result = df.filter(col("id") > 500).select("id")
    
    print("   Logical Plan built by driver:")
    result.explain(extended=True)
    
    # 3. Schedules jobs/stages/tasks
    print("\n3️⃣  Schedules Jobs, Stages, and Tasks")
    print("   Driver breaks query into:")
    print("   • Jobs (one per action)")
    print("   • Stages (split at shuffles)")
    print("   • Tasks (one per partition)")
    
    # 4. Tracks executor status
    print("\n4️⃣  Tracks Executor Status")
    print(f"   Active executors: {spark.sparkContext.defaultParallelism}")
    print(f"   Default parallelism: {spark.sparkContext.defaultParallelism}")
    
    # 5. Collects results
    print("\n5️⃣  Collects Results from Executors")
    count = result.count()
    print(f"   Driver received: {count} rows from executors")
    
    # 6. Manages broadcasts
    print("\n6️⃣  Manages Broadcast Variables")
    broadcast_var = spark.sparkContext.broadcast({"key": "value"})
    print(f"   Broadcast variable created: {broadcast_var}")
    print("   Driver sends broadcast to all executors")
    
    print("\n📊 DRIVER MEMORY USAGE:")
    print("   • SparkSession metadata")
    print("   • Execution plans (DAGs)")
    print("   • Job/stage/task tracking")
    print("   • Broadcast variables")
    print("   • Small results from collect()")


def demonstrate_executor_responsibilities():
    """
    Show what executors do.
    """
    print("\n" + "=" * 80)
    print("EXECUTOR RESPONSIBILITIES")
    print("=" * 80)
    
    spark = create_spark()
    
    print("\n🔧 EXECUTOR TASKS:")
    print("=" * 80)
    
    # 1. Run tasks
    print("\n1️⃣  Execute Tasks on Partitions")
    df = spark.range(0, 10000).repartition(4)
    
    # UDF to show which executor is running
    @udf(StringType())
    def get_partition_info(value):
        import os
        return f"PID:{os.getpid()}"
    
    result = df.withColumn("executor_pid", get_partition_info(col("id")))
    
    print("   Tasks distributed across executors:")
    result.select("executor_pid").distinct().show()
    
    # 2. Store data partitions
    print("\n2️⃣  Store Data Partitions in Memory")
    cached_df = df.cache()
    cached_df.count()  # Materialize cache
    
    print("   Data cached on executors")
    print("   Check Storage tab in Spark UI")
    
    # 3. Execute computations
    print("\n3️⃣  Execute Transformations")
    
    start = time.time()
    computed = df \
        .filter(col("id") > 1000) \
        .withColumn("squared", col("id") * col("id")) \
        .filter(col("squared") < 50000000)
    
    result_count = computed.count()
    exec_time = time.time() - start
    
    print(f"   Executors processed {result_count} rows")
    print(f"   Execution time: {exec_time:.4f}s")
    print("   All computation done on executors, not driver")
    
    # 4. Shuffle read/write
    print("\n4️⃣  Handle Shuffle Operations")
    grouped = df.groupBy((col("id") % 10).alias("bucket")).count()
    grouped.show()
    
    print("   Executors wrote shuffle files")
    print("   Executors read shuffle files")
    print("   Check Stages tab for shuffle metrics")
    
    # 5. Report metrics
    print("\n5️⃣  Report Metrics to Driver")
    print("   • Task completion status")
    print("   • Shuffle bytes written/read")
    print("   • Records processed")
    print("   • Execution time")
    print("   • Memory usage")
    
    print("\n📊 EXECUTOR MEMORY USAGE:")
    print("   • Cached data partitions")
    print("   • Task execution memory")
    print("   • Shuffle buffers")
    print("   • Broadcast variable copies")
    
    cached_df.unpersist()


def demonstrate_driver_executor_communication():
    """
    Show communication patterns between driver and executors.
    """
    print("\n" + "=" * 80)
    print("DRIVER ↔ EXECUTOR COMMUNICATION")
    print("=" * 80)
    
    spark = create_spark()
    
    print("\n📡 COMMUNICATION FLOW:")
    print("=" * 80)
    
    df = spark.range(0, 10000).repartition(4)
    
    print("\n1️⃣  JOB SUBMISSION:")
    print("   Driver → Executors: 'Execute these tasks'")
    print("   ┌─────────┐         ┌──────────┐")
    print("   │ Driver  │ ------> │ Executor │")
    print("   └─────────┘         └──────────┘")
    print("              Task 0-3 (by partition)")
    
    print("\n2️⃣  TASK EXECUTION:")
    result = df.filter(col("id") > 5000).count()
    
    print("   Executors: Running tasks in parallel")
    print("   ┌──────────┐")
    print("   │ Executor │ Task 0: Process partition 0")
    print("   │ Executor │ Task 1: Process partition 1")
    print("   │ Executor │ Task 2: Process partition 2")
    print("   │ Executor │ Task 3: Process partition 3")
    print("   └──────────┘")
    
    print("\n3️⃣  RESULT COLLECTION:")
    print("   Executors → Driver: 'Task complete, here are results'")
    print("   ┌──────────┐         ┌─────────┐")
    print("   │ Executor │ ------> │ Driver  │")
    print("   └──────────┘         └─────────┘")
    print("           Task results: counts from each partition")
    
    print(f"\n   Driver aggregates: {result} rows total")
    
    print("\n4️⃣  SHUFFLE COMMUNICATION:")
    grouped = df.groupBy((col("id") % 3).alias("key")).count()
    grouped.show()
    
    print("   Executors ↔ Executors: Shuffle data exchange")
    print("   ┌──────────┐         ┌──────────┐")
    print("   │ Exec 1   │ <-----> │ Exec 2   │")
    print("   └──────────┘         └──────────┘")
    print("        ↕                    ↕")
    print("   ┌──────────┐         ┌──────────┐")
    print("   │ Exec 3   │ <-----> │ Exec 4   │")
    print("   └──────────┘         └──────────┘")
    print("   (via shuffle service or executor)")


def demonstrate_failure_handling():
    """
    Show how driver handles executor failures.
    """
    print("\n" + "=" * 80)
    print("FAILURE HANDLING")
    print("=" * 80)
    
    spark = create_spark()
    
    print("\n🔄 FAULT TOLERANCE:")
    print("=" * 80)
    
    print("\n1️⃣  Task Failure:")
    print("   • Executor fails during task")
    print("   • Driver detects failure")
    print("   • Driver reschedules task on different executor")
    print("   • Uses lineage (DAG) to recompute lost data")
    
    print("\n2️⃣  Executor Failure:")
    print("   • Executor process crashes")
    print("   • Driver marks executor as lost")
    print("   • Driver requests new executor from cluster manager")
    print("   • Reschedules all tasks from failed executor")
    
    print("\n3️⃣  Driver Failure:")
    print("   • ⚠️  Driver crash = entire application fails")
    print("   • No automatic recovery (single point of failure)")
    print("   • Solution: Use cluster mode + checkpointing")
    
    print("\n4️⃣  Shuffle Data Persistence:")
    df = spark.range(0, 10000).repartition(4)
    grouped = df.groupBy((col("id") % 5).alias("key")).count()
    grouped.show()
    
    print("   • Shuffle files written to disk")
    print("   • Survive executor failures")
    print("   • External shuffle service keeps data")


def demonstrate_resource_allocation():
    """
    Show resource allocation between driver and executors.
    """
    print("\n" + "=" * 80)
    print("RESOURCE ALLOCATION")
    print("=" * 80)
    
    spark = create_spark()
    
    print("\n💾 TYPICAL CLUSTER CONFIGURATION:")
    print("=" * 80)
    
    print("""
    ┌─────────────────────────────────────────────────────────┐
    │                    CLUSTER RESOURCES                     │
    ├─────────────────────────────────────────────────────────┤
    │                                                          │
    │  ┌──────────────┐                                       │
    │  │   DRIVER     │  (1 node)                             │
    │  │              │                                        │
    │  │  Memory: 2GB │  • Plans execution                    │
    │  │  Cores: 2    │  • Schedules tasks                    │
    │  │              │  • Collects results                   │
    │  └──────────────┘                                       │
    │         ↓                                                │
    │  ┌──────────────────────────────────────────────────┐  │
    │  │            EXECUTORS (Worker Nodes)              │  │
    │  ├──────────────────────────────────────────────────┤  │
    │  │  ┌────────────┐  ┌────────────┐  ┌────────────┐ │  │
    │  │  │ Executor 1 │  │ Executor 2 │  │ Executor 3 │ │  │
    │  │  │            │  │            │  │            │ │  │
    │  │  │ Memory: 8GB│  │ Memory: 8GB│  │ Memory: 8GB│ │  │
    │  │  │ Cores: 4   │  │ Cores: 4   │  │ Cores: 4   │ │  │
    │  │  │            │  │            │  │            │ │  │
    │  │  │ • Run tasks│  │ • Run tasks│  │ • Run tasks│ │  │
    │  │  │ • Store data│ │ • Store data│ │ • Store data│ │  │
    │  │  │ • Shuffle  │  │ • Shuffle  │  │ • Shuffle  │ │  │
    │  │  └────────────┘  └────────────┘  └────────────┘ │  │
    │  └──────────────────────────────────────────────────┘  │
    └─────────────────────────────────────────────────────────┘
    
    Total Cluster:
    • Driver: 1 node × 2GB = 2GB
    • Executors: 3 nodes × 8GB = 24GB
    • Total: 26GB cluster memory
    • Parallelism: 3 executors × 4 cores = 12 concurrent tasks
    """)
    
    print("\n⚙️  CONFIGURATION PARAMETERS:")
    print("   --driver-memory 2g")
    print("   --executor-memory 8g")
    print("   --executor-cores 4")
    print("   --num-executors 3")


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("DRIVER AND EXECUTOR RESPONSIBILITIES")
    print("🎯" * 40)
    
    demonstrate_driver_responsibilities()
    demonstrate_executor_responsibilities()
    demonstrate_driver_executor_communication()
    demonstrate_failure_handling()
    demonstrate_resource_allocation()
    
    print("\n" + "=" * 80)
    print("✅ DRIVER/EXECUTOR DEMO COMPLETE")
    print("=" * 80)
    
    print("\n📚 Summary:")
    print("   DRIVER:")
    print("   ✅ Maintains SparkSession")
    print("   ✅ Builds execution plans")
    print("   ✅ Schedules jobs/stages/tasks")
    print("   ✅ Collects results")
    print("   ✅ Manages broadcasts")
    
    print("\n   EXECUTORS:")
    print("   ✅ Execute tasks on partitions")
    print("   ✅ Store cached data")
    print("   ✅ Perform computations")
    print("   ✅ Handle shuffles")
    print("   ✅ Report metrics")
    
    spark = SparkSession.builder.getOrCreate()
    spark.stop()


if __name__ == "__main__":
    main()
