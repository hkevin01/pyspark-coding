"""
15_dag_lazy_evaluation.py
==========================

Comprehensive example demonstrating DAG (Directed Acyclic Graph) 
and Lazy Evaluation in Spark.

Key Concepts:
1. Transformations are LAZY - they build a DAG without executing
2. Actions TRIGGER execution of the entire DAG
3. Catalyst Optimizer optimizes the DAG before execution
4. Logical Plan → Optimized Logical Plan → Physical Plan
5. Stage boundaries occur at shuffle operations
6. Spark can optimize across multiple transformations

This example shows how Spark builds, optimizes, and executes DAGs.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, expr, lit, when
import time


def demonstrate_lazy_evaluation(spark):
    """
    Show that transformations are lazy and don't execute immediately.
    """
    print("=" * 80)
    print("LAZY EVALUATION - TRANSFORMATIONS DON'T EXECUTE")
    print("=" * 80)
    
    print("\n📊 Creating DataFrame...")
    df = spark.range(0, 1_000_000)
    print("   ✅ DataFrame created (no computation yet!)")
    
    print("\n🔄 Adding transformations (all lazy)...")
    
    print("\n   1. Adding filter transformation...")
    start = time.time()
    df2 = df.filter(col("id") > 100)
    elapsed = time.time() - start
    print(f"      Time: {elapsed:.6f}s (instant! no actual filtering)")
    
    print("\n   2. Adding another filter...")
    start = time.time()
    df3 = df2.filter(col("id") < 900_000)
    elapsed = time.time() - start
    print(f"      Time: {elapsed:.6f}s (instant! still no execution)")
    
    print("\n   3. Adding map transformation...")
    start = time.time()
    df4 = df3.withColumn("squared", col("id") * col("id"))
    elapsed = time.time() - start
    print(f"      Time: {elapsed:.6f}s (instant! just building DAG)")
    
    print("\n   4. Adding aggregation...")
    start = time.time()
    df5 = df4.groupBy((col("id") % 10).alias("group")).agg(
        count("*").alias("count"),
        _sum("squared").alias("sum_squared")
    )
    elapsed = time.time() - start
    print(f"      Time: {elapsed:.6f}s (instant! DAG keeps growing)")
    
    print("\n⏱️  Total time for 4 transformations: ~0.0s")
    print("   NO DATA PROCESSED YET!")
    
    print("\n🚀 Now triggering execution with ACTION (count)...")
    start = time.time()
    result_count = df5.count()
    elapsed = time.time() - start
    
    print(f"\n✅ Execution complete: {result_count} rows in {elapsed:.3f}s")
    print("   All transformations executed together in one optimized plan!")


def demonstrate_dag_building(spark):
    """
    Show how Spark builds a DAG of transformations.
    """
    print("\n" + "=" * 80)
    print("DAG BUILDING - HOW SPARK TRACKS TRANSFORMATIONS")
    print("=" * 80)
    
    print("\n📊 Building a complex transformation pipeline...")
    
    # Start with data
    df = spark.range(0, 1_000_000)
    print("\n   Step 1: spark.range(1M)")
    print("   ├─ Creates RangeExec node in DAG")
    
    # Filter
    df2 = df.filter(col("id") % 2 == 0)
    print("\n   Step 2: .filter(id % 2 == 0)")
    print("   ├─ Adds Filter node to DAG")
    print("   └─ Parent: RangeExec")
    
    # Map
    df3 = df2.withColumn("category", (col("id") % 10).cast("string"))
    print("\n   Step 3: .withColumn(category)")
    print("   ├─ Adds Project node to DAG")
    print("   └─ Parent: Filter")
    
    # GroupBy (causes shuffle)
    df4 = df3.groupBy("category").agg(count("*").alias("total"))
    print("\n   Step 4: .groupBy(category).agg(count)")
    print("   ├─ Adds Aggregate node to DAG")
    print("   ├─ Causes SHUFFLE (stage boundary)")
    print("   └─ Parent: Project")
    
    # Sort
    df5 = df4.orderBy(col("total").desc())
    print("\n   Step 5: .orderBy(total)")
    print("   ├─ Adds Sort node to DAG")
    print("   ├─ Causes SHUFFLE (stage boundary)")
    print("   └─ Parent: Aggregate")
    
    print("\n📈 Final DAG structure:")
    print("   ")
    print("   RangeExec")
    print("      ↓")
    print("   Filter (id % 2 == 0)")
    print("      ↓")
    print("   Project (add category)")
    print("      ↓")
    print("   ═══════════════════════ SHUFFLE (Stage 0 → Stage 1)")
    print("      ↓")
    print("   Aggregate (count by category)")
    print("      ↓")
    print("   ═══════════════════════ SHUFFLE (Stage 1 → Stage 2)")
    print("      ↓")
    print("   Sort (by total desc)")
    print("   ")
    
    print("\n🚀 Executing with action...")
    results = df5.collect()
    print(f"✅ Complete: {len(results)} categories")


def demonstrate_logical_vs_physical_plan(spark):
    """
    Show the difference between logical and physical plans.
    """
    print("\n" + "=" * 80)
    print("LOGICAL vs PHYSICAL PLANS")
    print("=" * 80)
    
    df = spark.range(0, 1_000_000) \
        .filter(col("id") > 100) \
        .filter(col("id") < 900_000) \
        .withColumn("doubled", col("id") * 2)
    
    print("\n📋 LOGICAL PLAN (what user wrote):")
    print("   1. Range(0, 1000000)")
    print("   2. Filter(id > 100)")
    print("   3. Filter(id < 900000)")
    print("   4. Project(id, doubled = id * 2)")
    
    # Show actual logical plan
    print("\n🔍 Spark's Logical Plan:")
    df.explain(extended=False)
    
    print("\n⚡ OPTIMIZED LOGICAL PLAN (after Catalyst optimizer):")
    print("   Optimizations applied:")
    print("   - Predicate pushdown: Move filters closer to source")
    print("   - Column pruning: Only read needed columns")
    print("   - Constant folding: Evaluate constants once")
    print("   - Filter combining: Merge multiple filters")
    
    print("\n🎯 PHYSICAL PLAN (how Spark executes):")
    print("   - Chooses specific execution operators")
    print("   - Determines join strategies")
    print("   - Plans shuffle operations")
    print("   - Decides on whole-stage code generation")
    
    # Trigger execution
    count = df.count()
    print(f"\n✅ Executed: {count:,} records")


def demonstrate_catalyst_optimizations(spark):
    """
    Show specific Catalyst optimizer optimizations.
    """
    print("\n" + "=" * 80)
    print("CATALYST OPTIMIZER - MAKING QUERIES FASTER")
    print("=" * 80)
    
    print("\n1️⃣  PREDICATE PUSHDOWN")
    print("   " + "-" * 60)
    
    # Without optimization (conceptual)
    print("\n   ❌ Without pushdown:")
    print("      1. Read entire table (1M rows)")
    print("      2. Join with another table (expensive!)")
    print("      3. Filter result")
    
    print("\n   ✅ With pushdown:")
    print("      1. Read table with filter applied (10K rows)")
    print("      2. Join smaller dataset (much faster!)")
    
    df1 = spark.range(0, 1_000_000)
    df2 = spark.range(0, 100)
    
    # Catalyst will push the filter down before join
    result = df1.join(df2, df1.id == df2.id) \
        .filter(col("id") < 50)
    
    print("\n   🎯 Catalyst automatically pushes filter BEFORE join")
    
    print("\n\n2️⃣  COLUMN PRUNING")
    print("   " + "-" * 60)
    
    df = spark.range(0, 100_000) \
        .withColumn("a", col("id")) \
        .withColumn("b", col("id") * 2) \
        .withColumn("c", col("id") * 3) \
        .withColumn("d", col("id") * 4)
    
    print("\n   DataFrame has columns: id, a, b, c, d")
    
    # Only select one column
    result = df.select("b")
    
    print("\n   ✅ Catalyst optimization:")
    print("      - Only computes column 'b'")
    print("      - Doesn't compute unused columns a, c, d")
    print("      - Saves CPU and memory")
    
    print("\n\n3️⃣  CONSTANT FOLDING")
    print("   " + "-" * 60)
    
    # Expression with constants
    df = spark.range(0, 100_000) \
        .withColumn("result", col("id") + lit(10) * lit(5))
    
    print("\n   Original: id + (10 * 5)")
    print("   ✅ Optimized: id + 50")
    print("      - Evaluates (10 * 5) once at compile time")
    print("      - Not recomputed for each row")
    
    print("\n\n4️⃣  FILTER COMBINING")
    print("   " + "-" * 60)
    
    df = spark.range(0, 100_000) \
        .filter(col("id") > 100) \
        .filter(col("id") < 50_000) \
        .filter(col("id") % 2 == 0)
    
    print("\n   Three separate filters:")
    print("      .filter(id > 100)")
    print("      .filter(id < 50000)")
    print("      .filter(id % 2 == 0)")
    
    print("\n   ✅ Optimized to single filter:")
    print("      .filter(id > 100 AND id < 50000 AND id % 2 == 0)")
    print("      - One pass instead of three")
    
    result = df.count()
    print(f"\n   Processed {result:,} records efficiently")


def demonstrate_stages_and_shuffles(spark):
    """
    Show how DAG is divided into stages at shuffle boundaries.
    """
    print("\n" + "=" * 80)
    print("STAGES & SHUFFLES - DIVIDING THE DAG")
    print("=" * 80)
    
    print("\n📊 Creating pipeline with multiple shuffles...")
    
    df = spark.range(0, 1_000_000) \
        .withColumn("category", (col("id") % 10).cast("string")) \
        .withColumn("value", (col("id") % 100).cast("int"))
    
    # First shuffle: groupBy
    df2 = df.groupBy("category").agg(
        count("*").alias("count"),
        _sum("value").alias("total")
    )
    
    # Second shuffle: join
    df3 = df2.join(
        df2.withColumnRenamed("category", "cat2"),
        df2["count"] == df2["total"]
    )
    
    # Third shuffle: repartition
    df4 = df3.repartition(4)
    
    print("\n🎯 Spark divides this into stages:")
    print("   ")
    print("   STAGE 0: (No shuffle - Narrow transformations)")
    print("      - range(1M)")
    print("      - withColumn(category)")
    print("      - withColumn(value)")
    print("   ")
    print("   ╔═══════════════════════════════════════════╗")
    print("   ║  SHUFFLE 1: Exchange (hash partitioning) ║")
    print("   ╚═══════════════════════════════════════════╝")
    print("   ")
    print("   STAGE 1: (After first shuffle)")
    print("      - groupBy aggregation")
    print("      - Build join hash table")
    print("   ")
    print("   ╔═══════════════════════════════════════════╗")
    print("   ║  SHUFFLE 2: Exchange (join)              ║")
    print("   ╚═══════════════════════════════════════════╝")
    print("   ")
    print("   STAGE 2: (After second shuffle)")
    print("      - Perform join")
    print("   ")
    print("   ╔═══════════════════════════════════════════╗")
    print("   ║  SHUFFLE 3: Exchange (repartition)       ║")
    print("   ╚═══════════════════════════════════════════╝")
    print("   ")
    print("   STAGE 3: (Final stage)")
    print("      - Write results")
    
    print("\n🚀 Executing...")
    count = df4.count()
    print(f"✅ Complete: {count} rows")
    
    print("\n💡 Key insight:")
    print("   - Stages can run in parallel if independent")
    print("   - Within a stage, tasks process partitions in parallel")
    print("   - Shuffles are synchronization points (barrier)")


def demonstrate_dag_reuse(spark):
    """
    Show how Spark can reuse parts of the DAG when caching.
    """
    print("\n" + "=" * 80)
    print("DAG REUSE WITH CACHING")
    print("=" * 80)
    
    print("\n📊 Scenario: Multiple queries on same base data")
    
    # Expensive base computation
    base_df = spark.range(0, 500_000) \
        .withColumn("value1", col("id") * 2) \
        .withColumn("value2", col("id") * 3)
    
    print("\n❌ WITHOUT CACHING:")
    print("   Query 1: base_df.filter(value1 > 1000).count()")
    print("      ├─ Compute range")
    print("      ├─ Compute value1")
    print("      ├─ Compute value2")
    print("      └─ Filter & count")
    
    start = time.time()
    count1 = base_df.filter(col("value1") > 1000).count()
    time1 = time.time() - start
    
    print(f"\n   Result: {count1:,} rows in {time1:.3f}s")
    
    print("\n   Query 2: base_df.filter(value2 < 50000).count()")
    print("      ├─ Compute range AGAIN")
    print("      ├─ Compute value1 AGAIN")
    print("      ├─ Compute value2 AGAIN")
    print("      └─ Filter & count")
    
    start = time.time()
    count2 = base_df.filter(col("value2") < 50000).count()
    time2 = time.time() - start
    
    print(f"\n   Result: {count2:,} rows in {time2:.3f}s")
    print(f"   Total time: {time1 + time2:.3f}s")
    
    # Now with caching
    print("\n\n✅ WITH CACHING:")
    cached_df = base_df.cache()
    
    print("   Query 1: cached_df.filter(value1 > 1000).count()")
    print("      ├─ Compute range")
    print("      ├─ Compute value1")
    print("      ├─ Compute value2")
    print("      ├─ CACHE result")
    print("      └─ Filter & count")
    
    start = time.time()
    count1 = cached_df.filter(col("value1") > 1000).count()
    time1 = time.time() - start
    
    print(f"\n   Result: {count1:,} rows in {time1:.3f}s")
    
    print("\n   Query 2: cached_df.filter(value2 < 50000).count()")
    print("      ├─ Read from CACHE")
    print("      └─ Filter & count (fast!)")
    
    start = time.time()
    count2 = cached_df.filter(col("value2") < 50000).count()
    time2 = time.time() - start
    
    print(f"\n   Result: {count2:,} rows in {time2:.3f}s")
    print(f"   Total time: {time1 + time2:.3f}s")
    print(f"   💡 Second query much faster - reads from cache!")
    
    cached_df.unpersist()


def main():
    """
    Main function demonstrating DAG and Lazy Evaluation.
    """
    print("\n" + "📊 " * 40)
    print("SPARK DAG & LAZY EVALUATION - COMPLETE GUIDE")
    print("📊 " * 40)
    
    spark = SparkSession.builder \
        .appName("DAG_LazyEvaluation_Demo") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    # 1. Lazy evaluation
    demonstrate_lazy_evaluation(spark)
    
    # 2. DAG building
    demonstrate_dag_building(spark)
    
    # 3. Logical vs Physical plans
    demonstrate_logical_vs_physical_plan(spark)
    
    # 4. Catalyst optimizations
    demonstrate_catalyst_optimizations(spark)
    
    # 5. Stages and shuffles
    demonstrate_stages_and_shuffles(spark)
    
    # 6. DAG reuse with caching
    demonstrate_dag_reuse(spark)
    
    print("\n" + "=" * 80)
    print("✅ DAG & LAZY EVALUATION COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. Transformations are LAZY - build DAG without executing")
    print("   2. Actions TRIGGER execution of entire DAG")
    print("   3. Catalyst optimizer rewrites queries for better performance")
    print("   4. Logical plan → Optimized logical → Physical plan")
    print("   5. DAG divided into stages at shuffle boundaries")
    print("   6. Stages contain tasks that process partitions in parallel")
    print("   7. Caching breaks DAG to avoid recomputation")
    print("   8. Spark can execute independent stages in parallel")
    
    print("\n💡 Optimization strategies:")
    print("   - Let Catalyst optimize (don't micro-optimize)")
    print("   - Cache expensive computations used multiple times")
    print("   - Minimize shuffles (expensive!)")
    print("   - Use DataFrame API (more optimizable than RDD)")
    print("   - Check execution plan with .explain()")
    print("   - Monitor stages in Spark UI")
    
    print("\n🎯 Common transformations by type:")
    print("   ")
    print("   Narrow (no shuffle):")
    print("      - map, filter, flatMap")
    print("      - mapPartitions")
    print("      - union")
    print("   ")
    print("   Wide (causes shuffle):")
    print("      - groupBy, reduceByKey")
    print("      - join, cogroup")
    print("      - distinct")
    print("      - repartition, coalesce(increase)")
    print("      - sortBy")
    
    print("\n⚠️  Debugging tips:")
    print("   - Use .explain() to see execution plan")
    print("   - Use .explain(True) for detailed plan")
    print("   - Check Spark UI for stage breakdown")
    print("   - Look for skipped stages (cached data)")
    print("   - Identify expensive shuffles")
    
    spark.stop()


if __name__ == "__main__":
    main()
