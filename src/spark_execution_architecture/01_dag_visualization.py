#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
Spark Execution Architecture: DAG Visualization & Query Plans
================================================================================

MODULE OVERVIEW:
----------------
Demonstrates how Spark's Catalyst optimizer and execution engine work together
to transform your high-level DataFrame operations into efficient distributed
execution plans. Learn to use .explain() effectively for performance tuning.

PURPOSE:
--------
Master the art of reading and understanding Spark execution plans to:
- Identify performance bottlenecks (shuffles, skew, etc.)
- Understand how transformations map to stages and tasks
- Verify optimizer decisions
- Debug query performance issues

TARGET AUDIENCE:
----------------
- Data engineers optimizing Spark jobs
- Developers debugging performance issues
- Anyone wanting to understand Spark internals
- Interview candidates learning execution architecture

KEY CONCEPTS COVERED:
====================
1. DAG (Directed Acyclic Graph) construction
2. Stage boundaries at shuffle operations
3. Task creation (1 task per partition per stage)
4. Narrow vs Wide transformations
5. Catalyst optimizer transformations
6. Physical vs Logical plans
7. .explain() method and its modes

EXPLAIN() METHOD - COMPREHENSIVE GUIDE:
========================================

WHAT IS .explain()?
-------------------
The .explain() method is your window into Spark's execution engine. It shows
you EXACTLY how Spark will execute your query, including:
- What operations will be performed
- Where data will be shuffled across the network
- How many partitions will be created
- What optimizations have been applied

WHEN TO USE .explain():
-----------------------
✅ Before running expensive queries (to catch issues early)
✅ When query performance is slow (to identify bottlenecks)
✅ After adding filters/aggregations (to verify pushdown worked)
✅ When debugging data skew issues (look for uneven partitions)
✅ To understand shuffle operations (Exchange nodes)

EXPLAIN() MODES:
================

1. explain() or explain(extended=False) - DEFAULT MODE
   ----------------------------------------------------
   Shows: Physical plan only (what Spark will execute)

   Output includes:
   - Operator tree (bottom-up execution)
   - Partition counts at each step
   - Exchange nodes (shuffles)

   Example output:
   ```
   == Physical Plan ==
   *(2) Sort [revenue#123 DESC], true, 0
   +- Exchange rangepartitioning(revenue#123 DESC, 200)
      +- *(1) HashAggregate(keys=[category#45], functions=[sum(price#67)])
         +- Exchange hashpartitioning(category#45, 200)
            +- *(1) Filter (price#67 > 100)
   ```

2. explain(extended=True) - EXTENDED MODE
   ----------------------------------------
   Shows: ALL optimization phases

   Output includes:
   - Parsed Logical Plan (initial parse tree)
   - Analyzed Logical Plan (after name resolution)
   - Optimized Logical Plan (after Catalyst optimizer)
   - Physical Plan (final execution plan)

   Use when:
   - Debugging optimizer issues
   - Understanding why a query is slow
   - Verifying predicate pushdown
   - Checking column pruning

3. explain(mode="simple") - SIMPLE MODE (Spark 3.0+)
   --------------------------------------------------
   Same as explain(extended=False)
   Shows physical plan only

4. explain(mode="extended") - EXTENDED MODE (Spark 3.0+)
   -------------------------------------------------------
   Same as explain(extended=True)
   Shows all optimization phases

5. explain(mode="formatted") - FORMATTED MODE (RECOMMENDED)
   ---------------------------------------------------------
   Shows: Physical plan in readable tree format

   Features:
   - Indented tree structure
   - Clear parent-child relationships
   - Highlighted shuffle operations
   - Data size estimates
   - Partition counts at each operator

   Example output:
   ```
   * Sort (2)
   +- Exchange rangepartitioning
      +- * HashAggregate (1)
         +- Exchange hashpartitioning
            +- * Filter
   ```

6. explain(mode="codegen") - CODE GENERATION MODE
   ------------------------------------------------
   Shows: Generated Java code for whole-stage codegen

   Output includes:
   - Actual Java bytecode Spark generates
   - Function calls that will be executed
   - Variable declarations

   Use when:
   - Investigating codegen issues
   - Understanding performance at CPU level
   - Verifying operator fusion

7. explain(mode="cost") - COST-BASED MODE
   ----------------------------------------
   Shows: Statistics used by cost-based optimizer

   Output includes:
   - Row count estimates
   - Data size estimates
   - Column statistics

   Use when:
   - Debugging join strategy selection
   - Understanding broadcast decisions
   - Verifying statistics are accurate

READING THE PLAN - KEY PATTERNS:
=================================

SHUFFLE INDICATORS (expensive operations):
-------------------------------------------
- "Exchange hashpartitioning" = Shuffle for groupBy/join
- "Exchange rangepartitioning" = Shuffle for sort/window
- "Exchange" = Stage boundary (data moved across network)

EXECUTION ORDER:
----------------
Plans are shown BOTTOM-UP but execute TOP-DOWN:
```
Step 3: Sort           ← Last operation
   ↑
Step 2: Aggregate      ← Middle operation
   ↑
Step 1: Filter         ← First operation (reads data)
```

OPERATOR SYMBOLS:
-----------------
- * or +- = Physical operators
- Numbers in () = Stage IDs
- [column#123] = Internal column references
- true/false = Boolean flags

PERFORMANCE TIPS FROM .explain():
=================================
❌ BAD: Many Exchange nodes → Excessive shuffles
✅ GOOD: Filters before Exchange → Less data shuffled
❌ BAD: Exchange after join → Large shuffle
✅ GOOD: BroadcastExchange for small tables → No shuffle
❌ BAD: No * before Sort → Codegen disabled
✅ GOOD: * everywhere → Whole-stage codegen active

USAGE:
------
Run this file directly to see all examples:
    $ python3 01_dag_visualization.py

Or import specific functions:
    from dag_visualization import demonstrate_simple_dag
    demonstrate_simple_dag()

Then open Spark UI:
    http://localhost:4040

RELATED RESOURCES:
------------------
- Spark SQL Tuning Guide: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Catalyst Optimizer: https://databricks.com/glossary/catalyst-optimizer
- Query Plans: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0
CREATED: 2025
LAST MODIFIED: December 13, 2025

================================================================================
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
# (none required)

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit
from pyspark.sql.functions import sum as _sum


def create_spark():
    """Create Spark session with visible UI."""
    return (
        SparkSession.builder.appName("DAGVisualization")
        .master("local[4]")
        .config("spark.ui.port", "4040")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


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
    result = (
        df.filter(col("id") > 100)
        .withColumn("doubled", col("id") * 2)
        .filter(col("doubled") < 1500)
        .select("id", "doubled")
    )

    # ========================================================================
    # EXPLAIN() - Display Query Execution Plans
    # ========================================================================
    # The .explain() method shows how Spark will execute your query
    # It's crucial for understanding performance and debugging issues

    print("\n📋 Logical Plan:")
    # ========================================================================
    # explain(extended=False) - Shows PHYSICAL plan only (default)
    # ========================================================================
    # WHAT IT DOES:
    # - Displays the optimized physical execution plan
    # - Shows actual operators Spark will execute
    # - Does NOT show logical plan or analysis steps
    #
    # WHEN TO USE:
    # - Quick check of execution strategy
    # - See if operations are pipelined or cause shuffles
    # - Verify partition counts
    result.explain(extended=False)

    print("\n📊 Physical Plan (with all details):")
    # ========================================================================
    # explain(mode="formatted") - Pretty-printed tree format (Spark 3.0+)
    # ========================================================================
    # WHAT IT DOES:
    # - Shows physical plan in readable tree structure
    # - Includes metrics: data size estimates, partition counts
    # - Highlights Exchange nodes (shuffles) clearly
    # - Better visual hierarchy than default output
    #
    # MODE OPTIONS:
    # - "simple": Same as explain(extended=False) - physical plan only
    # - "extended": Shows parsed, analyzed, optimized, and physical plans
    # - "codegen": Shows generated Java code for whole-stage codegen
    # - "cost": Shows cost-based optimizer statistics
    # - "formatted": Tree format with clear structure (RECOMMENDED)
    #
    # WHEN TO USE:
    # - Debugging complex queries
    # - Understanding optimizer decisions
    # - Finding performance bottlenecks
    # - Seeing data size estimates
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
    sales = spark.createDataFrame(
        [
            (1, "Electronics", "Laptop", 1200, "2024-01-01"),
            (2, "Electronics", "Phone", 800, "2024-01-01"),
            (3, "Clothing", "Shirt", 50, "2024-01-02"),
            (4, "Clothing", "Pants", 80, "2024-01-02"),
            (5, "Electronics", "Tablet", 600, "2024-01-03"),
            (6, "Furniture", "Chair", 150, "2024-01-03"),
        ]
        * 1000,
        ["id", "category", "product", "price", "date"],
    )

    # Complex query with multiple stages
    result = (
        sales.filter(col("price") > 50)
        .groupBy("category")
        .agg(
            count("*").alias("num_products"),
            _sum("price").alias("total_revenue"),
            avg("price").alias("avg_price"),
        )
        .filter(col("total_revenue") > 10000)
        .orderBy(col("total_revenue").desc())
    )

    # ========================================================================
    # EXPLAIN() with formatted mode - See multi-stage execution
    # ========================================================================
    print("\n📋 Execution Plan:")
    # WHAT HAPPENS HERE:
    # 1. Spark analyzes the entire query (filter, groupBy, agg, orderBy)
    # 2. Catalyst optimizer creates optimized logical plan
    # 3. Physical planner converts to executable stages
    # 4. Identifies shuffle boundaries (Exchange nodes)
    # 5. Plans 3 stages:
    #    - Stage 0: Read + filter + map for shuffle
    #    - Stage 1: Aggregate + filter + map for sort shuffle
    #    - Stage 2: Final sort + collect results
    #
    # WHAT TO LOOK FOR:
    # - "Exchange" nodes = shuffle boundaries = stage boundaries
    # - "hashpartitioning" = data redistributed by hash (for groupBy)
    # - "rangepartitioning" = data sorted globally (for orderBy)
    # - Numbers in parentheses = partition counts at each step
    result.explain(mode="formatted")

    print("\n🎯 DAG Breakdown:")
    print(
        """
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
    """
    )

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
    print(
        """
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
    """
    )


def demonstrate_dag_optimization():
    """
    Show how Catalyst optimizer changes the DAG.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: DAG OPTIMIZATION BY CATALYST")
    print("=" * 80)

    spark = create_spark()

    df = (
        spark.range(0, 10000)
        .toDF("id")
        .withColumn("value", col("id") * 2)
        .withColumn("category", (col("id") % 10))
    )

    # Unoptimized query (as written)
    print("\n📝 Query as written:")
    print(
        """
    df.select("id", "value", "category", "extra_col")
      .filter(col("value") > 1000)
      .filter(col("category") == 5)
      .select("id", "value")
    """
    )

    # Add extra column that won't be used
    df_with_extra = df.withColumn("extra_col", lit("unused"))

    result = (
        df_with_extra.select("id", "value", "category", "extra_col")
        .filter(col("value") > 1000)
        .filter(col("category") == 5)
        .select("id", "value")
    )

    # ========================================================================
    # EXPLAIN() showing Catalyst optimizer's work
    # ========================================================================
    print("\n🔧 Optimized Plan (by Catalyst):")
    # WHAT HAPPENS WHEN explain() IS CALLED:
    # ========================================================================
    # Step 1: PARSING
    #   - Converts your Python/SQL code into Abstract Syntax Tree (AST)
    #   - Validates syntax
    #
    # Step 2: ANALYSIS
    #   - Resolves column names against schema
    #   - Type checking (ensure operations are valid)
    #   - Validates table/column existence
    #
    # Step 3: LOGICAL OPTIMIZATION (Catalyst Optimizer)
    #   - Predicate pushdown: Move filters closer to data source
    #   - Column pruning: Remove unused columns early
    #   - Constant folding: Evaluate constant expressions (2+3 → 5)
    #   - Filter combination: Merge multiple filters
    #   - Projection collapse: Remove redundant select operations
    #
    # Step 4: PHYSICAL PLANNING
    #   - Choose join strategies (broadcast vs shuffle join)
    #   - Decide partition counts for shuffles
    #   - Select sort vs hash aggregation
    #   - Determine operator fusion (whole-stage codegen)
    #
    # Step 5: CODE GENERATION
    #   - Generate optimized Java bytecode
    #   - Combine multiple operators into single function
    #   - Avoid virtual function calls for performance
    #
    # THE OUTPUT YOU SEE:
    # - Final optimized physical plan
    # - NOT the original query structure
    # - Shows what Spark will ACTUALLY execute
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
