"""
01_dag_visualization.py
=======================

Spark Execution Architecture - DAG, Stages, and Tasks

Demonstrates:
- DAG (Directed Acyclic Graph) creation and visualization
- Stage boundaries at shuffles
- Task creation per partition
- Narrow vs Wide transformations in execution plan
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, lit

def create_spark():
    """Create Spark session with visible UI."""
    return SparkSession.builder \
        .appName("DAGVisualization") \
        .master("local[4]") \
        .config("spark.ui.port", "4040") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def demonstrate_simple_dag():
    """
    Simple DAG with no shuffles (single stage).
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: SIMPLE DAG - No Shuffles (Single Stage)")
    print("=" * 80)
    
    spark = create_spark()
    
    # Create data
    df = spark.range(0, 1000).toDF("id")
    
    # Chain of narrow transformations
    result = df \
        .filter(col("id") > 100) \
        .withColumn("doubled", col("id") * 2) \
        .filter(col("doubled") < 1500) \
        .select("id", "doubled")
    
    print("\n📋 Logical Plan:")
    result.explain(extended=False)
    
    print("\n📊 Physical Plan (with all details):")
    result.explain(mode="formatted")
    
    print("\n🎯 What happens:")
    print("   Stage 0: Read → filter → withColumn → filter → select")
    print("   All operations pipelined (no shuffle)")
    print("   Number of tasks = number of partitions")
    
    # Execute to see in Spark UI
    result.show(5)
    
    print(f"\n✅ Partitions: {result.rdd.getNumPartitions()}")
    print(f"✅ Total rows: {result.count()}")
    print(f"✅ Check Spark UI: http://localhost:4040")
    print(f"   → Jobs tab: See 1 job (from count)")
    print(f"   → Stages tab: See 1 stage")
    print(f"   → DAG Visualization: No shuffle boundaries")


def demonstrate_multi_stage_dag():
    """
    Complex DAG with multiple stages (shuffles).
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: MULTI-STAGE DAG - With Shuffles")
    print("=" * 80)
    
    spark = create_spark()
    
    # Create sales data
    sales = spark.createDataFrame([
        (1, "Electronics", "Laptop", 1200, "2024-01-01"),
        (2, "Electronics", "Phone", 800, "2024-01-01"),
        (3, "Clothing", "Shirt", 50, "2024-01-02"),
        (4, "Clothing", "Pants", 80, "2024-01-02"),
        (5, "Electronics", "Tablet", 600, "2024-01-03"),
        (6, "Furniture", "Chair", 150, "2024-01-03"),
    ] * 1000, ["id", "category", "product", "price", "date"])
    
    # Complex query with multiple stages
    result = sales \
        .filter(col("price") > 50) \
        .groupBy("category") \
        .agg(
            count("*").alias("num_products"),
            _sum("price").alias("total_revenue"),
            avg("price").alias("avg_price")
        ) \
        .filter(col("total_revenue") > 10000) \
        .orderBy(col("total_revenue").desc())
    
    print("\n📋 Execution Plan:")
    result.explain(mode="formatted")
    
    print("\n🎯 DAG Breakdown:")
    print("""
    Stage 0 (ShuffleMapStage):
      ├─ Read data from memory
      ├─ Filter: price > 50 (NARROW)
      ├─ Map: (category, price) pairs
      └─ Write shuffle files by category hash
      
    Stage 1 (ShuffleMapStage):
      ├─ Read shuffle data from Stage 0
      ├─ GroupBy category + Aggregations (sum, count, avg)
      ├─ Filter: total_revenue > 10000 (NARROW)
      ├─ Map: prepare for sort
      └─ Write shuffle files for global sort
      
    Stage 2 (ResultStage):
      ├─ Read shuffle data from Stage 1
      ├─ Global sort by total_revenue desc
      └─ Collect results to driver (show/count)
    """)
    
    # Execute
    result.show()
    
    print("\n✅ Check Spark UI: http://localhost:4040")
    print("   → SQL tab: See full query execution")
    print("   → Look for 'Exchange' nodes (shuffles)")
    print("   → 3 stages total")


def demonstrate_job_stage_task_hierarchy():
    """
    Show the relationship: Job → Stages → Tasks.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: JOB → STAGES → TASKS HIERARCHY")
    print("=" * 80)
    
    spark = create_spark()
    
    # Create data with 4 partitions
    df = spark.range(0, 1000).repartition(4)
    
    print(f"📊 Dataset has {df.rdd.getNumPartitions()} partitions")
    
    # Action 1: Simple count (1 job, 1 stage, 4 tasks)
    print("\n🎬 ACTION 1: count()")
    count = df.count()
    print(f"   Result: {count} rows")
    print(f"   Job: 1 job created")
    print(f"   Stages: 1 stage")
    print(f"   Tasks: 4 tasks (1 per partition)")
    
    # Action 2: GroupBy (1 job, 2 stages, multiple tasks)
    print("\n🎬 ACTION 2: groupBy + count")
    grouped = df.groupBy((col("id") % 10).alias("bucket")).count()
    result = grouped.collect()
    print(f"   Result: {len(result)} groups")
    print(f"   Job: 1 job created")
    print(f"   Stages: 2 stages (shuffle boundary)")
    print(f"   Stage 0: 4 tasks (read + map)")
    print(f"   Stage 1: 8 tasks (configurable via spark.sql.shuffle.partitions)")
    
    # Action 3: Multiple actions on same DataFrame
    print("\n🎬 ACTION 3: Multiple actions (count + show)")
    grouped.count()  # Job 3
    grouped.show(5)  # Job 4
    print(f"   Jobs: 2 separate jobs created")
    print(f"   Each action triggers new job")
    print(f"   TIP: Use .cache() to avoid recomputation")
    
    print("\n📐 HIERARCHY:")
    print("""
    Application
    └─ SparkContext
       └─ Jobs (one per action)
          └─ Stages (split at shuffles)
             └─ Tasks (one per partition in stage)
    
    Example:
    df.groupBy("key").count()  # One action
    └─ Job 0
       ├─ Stage 0: 4 tasks (if 4 input partitions)
       └─ Stage 1: 8 tasks (if 8 shuffle partitions)
    """)


def demonstrate_dag_optimization():
    """
    Show how Catalyst optimizer changes the DAG.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: DAG OPTIMIZATION BY CATALYST")
    print("=" * 80)
    
    spark = create_spark()
    
    df = spark.range(0, 10000).toDF("id") \
        .withColumn("value", col("id") * 2) \
        .withColumn("category", (col("id") % 10))
    
    # Unoptimized query (as written)
    print("\n📝 Query as written:")
    print("""
    df.select("id", "value", "category", "extra_col")
      .filter(col("value") > 1000)
      .filter(col("category") == 5)
      .select("id", "value")
    """)
    
    # Add extra column that won't be used
    df_with_extra = df.withColumn("extra_col", lit("unused"))
    
    result = df_with_extra \
        .select("id", "value", "category", "extra_col") \
        .filter(col("value") > 1000) \
        .filter(col("category") == 5) \
        .select("id", "value")
    
    print("\n🔧 Optimized Plan (by Catalyst):")
    result.explain(mode="formatted")
    
    print("\n✨ Optimizations applied:")
    print("   1. ✅ Column Pruning: 'extra_col' removed (not needed)")
    print("   2. ✅ Predicate Pushdown: Filters applied early")
    print("   3. ✅ Filter Combination: (value > 1000) AND (category == 5)")
    print("   4. ✅ Projection Pruning: Only reads id, value, category")
    
    result.show(5)


def demonstrate_lazy_evaluation_dag():
    """
    Show how DAG is built lazily and executed on action.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: LAZY EVALUATION - DAG Building")
    print("=" * 80)
    
    spark = create_spark()
    
    import time
    
    df = spark.range(0, 1000000)
    
    print("🔨 Building DAG (transformations)...")
    start = time.time()
    
    # All transformations (instant - just building DAG)
    step1 = df.filter(col("id") > 100)
    print(f"   ✅ step1 = filter: {time.time() - start:.4f}s")
    
    step2 = step1.withColumn("squared", col("id") * col("id"))
    print(f"   ✅ step2 = withColumn: {time.time() - start:.4f}s")
    
    step3 = step2.filter(col("squared") < 500000)
    print(f"   ✅ step3 = filter: {time.time() - start:.4f}s")
    
    step4 = step3.select("id", "squared")
    print(f"   ✅ step4 = select: {time.time() - start:.4f}s")
    
    print(f"\n⚡ Total transformation time: {time.time() - start:.4f}s")
    print("   (Nearly instant - just building DAG)")
    
    print("\n🚀 Executing DAG (action)...")
    action_start = time.time()
    result = step4.count()
    action_time = time.time() - action_start
    
    print(f"   ✅ Action (count): {action_time:.4f}s")
    print(f"   Result: {result} rows")
    print(f"\n   Transformation time: ~0.001s (building DAG)")
    print(f"   Execution time: {action_time:.4f}s (actual work)")


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("SPARK EXECUTION ARCHITECTURE - DAG VISUALIZATION")
    print("🎯" * 40)
    
    demonstrate_simple_dag()
    demonstrate_multi_stage_dag()
    demonstrate_job_stage_task_hierarchy()
    demonstrate_dag_optimization()
    demonstrate_lazy_evaluation_dag()
    
    print("\n" + "=" * 80)
    print("✅ DAG VISUALIZATION COMPLETE")
    print("=" * 80)
    print("\n📚 Key Concepts Demonstrated:")
    print("   ✅ DAG creation and execution")
    print("   ✅ Stage boundaries at shuffles")
    print("   ✅ Task parallelism per partition")
    print("   ✅ Job → Stages → Tasks hierarchy")
    print("   ✅ Catalyst optimizer transformations")
    print("   ✅ Lazy evaluation timing")
    
    print("\n🔍 Next Steps:")
    print("   1. Open Spark UI: http://localhost:4040")
    print("   2. Explore Jobs, Stages, and SQL tabs")
    print("   3. Click on DAG visualizations")
    print("   4. Compare plans with .explain()")
    
    spark = SparkSession.builder.getOrCreate()
    input("\n⏸️  Press Enter to stop Spark and close UI...")
    spark.stop()


if __name__ == "__main__":
    main()
