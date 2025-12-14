"""
Execution Plans in Spark Explained

WHAT IS AN EXECUTION PLAN?
==========================
A "plan" in Spark/SQL means the EXECUTION PLAN - how the system will run your query.

Spark shows this in two forms:
• Logical Plan: What you want to do (high-level)
• Physical Plan: How Spark will actually do it (low-level)

Used for:
• Understanding performance
• Identifying bottlenecks
• Optimizing queries

Author: PySpark Learning Series
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Create Spark session for execution plan examples."""
    print("\n" + "=" * 70)
    print("CREATING SPARK SESSION FOR EXECUTION PLANS")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("Execution Plans Explained") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("✅ Spark session created")
    return spark


def example_1_what_is_execution_plan(spark):
    """
    Introduction to execution plans.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: WHAT IS AN EXECUTION PLAN?")
    print("=" * 70)
    
    print("""
    EXECUTION PLAN = How Spark runs your query
    ==========================================
    
    When you write:
      df.filter(col("age") > 25).select("name")
    
    Spark creates a PLAN:
    
    ┌─────────────────────────────────────────┐
    │       LOGICAL PLAN (What)               │
    │  • Filter (age > 25)                    │
    │  • Project (select name)                │
    │  • Read from DataFrame                  │
    └─────────────────────────────────────────┘
                    ↓
            Catalyst Optimizer
                    ↓
    ┌─────────────────────────────────────────┐
    │       OPTIMIZED LOGICAL PLAN            │
    │  • Push filter down to scan             │
    │  • Only read needed columns             │
    └─────────────────────────────────────────┘
                    ↓
            Code Generation
                    ↓
    ┌─────────────────────────────────────────┐
    │       PHYSICAL PLAN (How)               │
    │  • Scan parquet [name, age]             │
    │  • Filter (age > 25)                    │
    │  • Project [name]                       │
    └─────────────────────────────────────────┘
    
    THREE TYPES OF PLANS:
    ====================
    1. Parsed Logical Plan: Your code translated to plan
    2. Optimized Logical Plan: After Catalyst optimization
    3. Physical Plan: Actual execution steps
    """)
    
    # Create sample data
    data = [
        ("Alice", 30, "Engineering"),
        ("Bob", 25, "Sales"),
        ("Charlie", 35, "Engineering"),
        ("Diana", 28, "Marketing"),
    ]
    
    df = spark.createDataFrame(data, ["name", "age", "department"])
    
    print("\n📊 SAMPLE DATA:")
    df.show()
    
    # Simple query
    result = df.filter(col("age") > 25).select("name", "department")
    
    print("\n📋 QUERY:")
    print("df.filter(col('age') > 25).select('name', 'department')")
    
    print("\n🔍 EXECUTION PLAN:")
    result.explain(extended=False)
    
    print("\n" + "=" * 70)
    print("✅ EXECUTION PLAN INTRO COMPLETE")
    print("=" * 70)


def example_2_explain_methods(spark):
    """
    Different explain() methods to view plans.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: EXPLAIN METHODS")
    print("=" * 70)
    
    print("""
    EXPLAIN METHODS IN SPARK:
    ========================
    
    1. df.explain()
       • Shows physical plan only (default)
    
    2. df.explain(extended=True)
       • Shows all plans:
         - Parsed Logical Plan
         - Analyzed Logical Plan
         - Optimized Logical Plan
         - Physical Plan
    
    3. df.explain(mode="simple")
       • Physical plan (same as explain())
    
    4. df.explain(mode="extended")
       • All plans (same as extended=True)
    
    5. df.explain(mode="cost")
       • Shows cost estimates
    
    6. df.explain(mode="formatted")
       • Pretty-printed physical plan
    """)
    
    # Sample data
    data = [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35),
    ]
    
    df = spark.createDataFrame(data, ["name", "age"])
    filtered = df.filter(col("age") > 25)
    
    print("\n1️⃣ df.explain() - Physical Plan Only:")
    print("=" * 50)
    filtered.explain()
    
    print("\n2️⃣ df.explain(extended=True) - All Plans:")
    print("=" * 50)
    filtered.explain(extended=True)
    
    print("\n" + "=" * 70)
    print("✅ EXPLAIN METHODS COMPLETE")
    print("=" * 70)


def example_3_catalyst_optimizer(spark):
    """
    Demonstrate Catalyst optimizer optimizations.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: CATALYST OPTIMIZER")
    print("=" * 70)
    
    print("""
    CATALYST OPTIMIZER:
    ==================
    Spark's query optimizer that transforms your code into efficient execution plan.
    
    KEY OPTIMIZATIONS:
    =================
    
    1. PREDICATE PUSHDOWN:
       • Push filters down to data source
       • Read less data from disk
       
    2. COLUMN PRUNING:
       • Only read columns you need
       • Reduce I/O and memory
    
    3. CONSTANT FOLDING:
       • Evaluate constants at compile time
       • Example: 2 + 3 → 5
    
    4. FILTER ORDERING:
       • Apply most selective filters first
       • Reduce data early
    
    5. JOIN REORDERING:
       • Optimal join order
       • Smaller intermediate results
    """)
    
    # Sample data
    employees = [
        (1, "Alice", 30, "Engineering", 100000),
        (2, "Bob", 25, "Sales", 80000),
        (3, "Charlie", 35, "Engineering", 120000),
        (4, "Diana", 28, "Marketing", 90000),
        (5, "Eve", 32, "Sales", 85000),
    ]
    
    df = spark.createDataFrame(employees, ["id", "name", "age", "dept", "salary"])
    df.createOrReplaceTempView("employees")
    
    print("\n📊 EMPLOYEES TABLE:")
    df.show()
    
    # Example: Predicate pushdown
    print("\n1️⃣ PREDICATE PUSHDOWN:")
    print("=" * 50)
    print("Query: SELECT name FROM employees WHERE age > 28")
    
    query = spark.sql("SELECT name FROM employees WHERE age > 28")
    
    print("\nOptimization:")
    print("• Filter (age > 28) pushed to scan")
    print("• Only 'name' and 'age' columns read (column pruning)")
    print("• Not reading 'id', 'dept', 'salary'")
    
    print("\nPhysical Plan:")
    query.explain()
    
    # Example: Column pruning
    print("\n2️⃣ COLUMN PRUNING:")
    print("=" * 50)
    
    # Inefficient: Read all columns then select
    print("Code: df.select('name')")
    result = df.select("name")
    
    print("\nOptimization:")
    print("• Catalyst only reads 'name' column from source")
    print("• Other columns not read from disk")
    
    print("\nPhysical Plan:")
    result.explain()
    
    print("\n" + "=" * 70)
    print("✅ CATALYST OPTIMIZER COMPLETE")
    print("=" * 70)


def example_4_reading_execution_plans(spark):
    """
    How to read and understand execution plans.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: READING EXECUTION PLANS")
    print("=" * 70)
    
    print("""
    HOW TO READ PHYSICAL PLANS:
    ==========================
    
    Plans read from BOTTOM to TOP (like a stack):
    
    Example Plan:
    
    == Physical Plan ==
    *(1) Project [name#123]              ← Step 3: Project name
    +- *(1) Filter (age#124 > 25)        ← Step 2: Filter age
       +- *(1) Scan ...                  ← Step 1: Read data
    
    KEY SYMBOLS:
    ===========
    • *(...) : WholeStageCodegen (optimized code generation)
    • +- : Tree structure (child operation)
    • [column#id] : Column references
    • ... : Additional details
    
    COMMON OPERATIONS:
    =================
    • Scan: Read data from source
    • Filter: Apply WHERE condition
    • Project: SELECT columns
    • Aggregate: GROUP BY operations
    • HashAggregate: Hash-based aggregation
    • SortMergeJoin: Join using sort-merge
    • BroadcastHashJoin: Broadcast smaller table
    • Exchange: Shuffle data across partitions
    """)
    
    # Sample data
    sales = [
        ("Electronics", 1200),
        ("Clothing", 450),
        ("Electronics", 800),
        ("Furniture", 1500),
        ("Clothing", 600),
    ]
    
    df = spark.createDataFrame(sales, ["category", "amount"])
    
    print("\n📊 SALES DATA:")
    df.show()
    
    # Simple aggregation
    print("\n1️⃣ SIMPLE QUERY:")
    print("df.groupBy('category').agg(sum('amount'))")
    
    result = df.groupBy("category").agg(sum("amount").alias("total"))
    
    print("\n🔍 EXECUTION PLAN (read bottom to top):")
    result.explain()
    
    print("""
    READING THE PLAN:
    ================
    1. Scan: Read data from DataFrame
    2. HashAggregate: Group by category and sum
    3. Exchange: Shuffle data (if needed)
    4. HashAggregate: Final aggregation
    5. Project: Output result
    """)
    
    # Complex query
    print("\n2️⃣ COMPLEX QUERY:")
    print("Filter → GroupBy → OrderBy")
    
    complex = df.filter(col("amount") > 500) \
        .groupBy("category") \
        .agg(sum("amount").alias("total"),
             count("*").alias("count")) \
        .orderBy(desc("total"))
    
    print("\n🔍 EXECUTION PLAN:")
    complex.explain()
    
    print("\n" + "=" * 70)
    print("✅ READING PLANS COMPLETE")
    print("=" * 70)


def example_5_shuffle_operations(spark):
    """
    Understanding shuffle operations in plans.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: SHUFFLE OPERATIONS")
    print("=" * 70)
    
    print("""
    SHUFFLE = EXPENSIVE OPERATION
    ============================
    
    What is Shuffle?
    • Moving data across network between partitions
    • Needed for groupBy, join, repartition, etc.
    
    Why Expensive?
    • Network I/O
    • Disk I/O (spill to disk if memory full)
    • Serialization/deserialization
    
    In Execution Plan:
    • Look for "Exchange" operation
    • Indicates data movement across network
    
    OPERATIONS THAT CAUSE SHUFFLE:
    =============================
    • groupBy() / GROUP BY
    • join() / JOIN
    • distinct() / DISTINCT
    • repartition()
    • sortBy() / ORDER BY (global sort)
    • window functions with PARTITION BY
    
    HOW TO AVOID SHUFFLE:
    ====================
    • Use broadcast join for small tables
    • Pre-partition data appropriately
    • Use repartition() wisely
    • Cache intermediate results
    """)
    
    # Create two datasets
    employees = [
        (1, "Alice", "Engineering"),
        (2, "Bob", "Sales"),
        (3, "Charlie", "Engineering"),
    ]
    
    departments = [
        ("Engineering", "Building A"),
        ("Sales", "Building B"),
        ("Marketing", "Building C"),
    ]
    
    emp_df = spark.createDataFrame(employees, ["id", "name", "dept"])
    dept_df = spark.createDataFrame(departments, ["dept", "building"])
    
    print("\n📊 EMPLOYEES:")
    emp_df.show()
    
    print("📊 DEPARTMENTS:")
    dept_df.show()
    
    # Join (causes shuffle)
    print("\n1️⃣ JOIN (with shuffle):")
    joined = emp_df.join(dept_df, "dept")
    
    print("🔍 EXECUTION PLAN (look for 'Exchange'):")
    joined.explain()
    
    print("\n⚠️  'Exchange' indicates shuffle (network transfer)")
    
    # GroupBy (causes shuffle)
    print("\n2️⃣ GROUPBY (with shuffle):")
    grouped = emp_df.groupBy("dept").count()
    
    print("🔍 EXECUTION PLAN:")
    grouped.explain()
    
    print("\n⚠️  'Exchange' for groupBy shuffle")
    
    print("\n" + "=" * 70)
    print("✅ SHUFFLE OPERATIONS COMPLETE")
    print("=" * 70)


def example_6_query_optimization_tips(spark):
    """
    Tips for query optimization based on execution plans.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 6: QUERY OPTIMIZATION TIPS")
    print("=" * 70)
    
    print("""
    HOW TO USE EXECUTION PLANS FOR OPTIMIZATION:
    ===========================================
    
    1. CHECK FOR SHUFFLES (Exchange):
       ❌ Bad: Multiple shuffles
       ✅ Good: Minimize shuffles
       Fix: Use broadcast joins, pre-partition data
    
    2. CHECK FOR FULL TABLE SCANS:
       ❌ Bad: Reading entire table
       ✅ Good: Filters pushed down
       Fix: Add WHERE clauses, use partitioned tables
    
    3. CHECK FOR UNNECESSARY COLUMNS:
       ❌ Bad: Reading all columns
       ✅ Good: Only needed columns
       Fix: Select specific columns early
    
    4. CHECK JOIN STRATEGY:
       ❌ Bad: SortMergeJoin on small table
       ✅ Good: BroadcastHashJoin for small tables
       Fix: Use broadcast() hint
    
    5. CHECK FOR SORT OPERATIONS:
       ❌ Bad: Expensive global sorts
       ✅ Good: Avoid unnecessary ORDER BY
       Fix: Only sort when needed
    
    OPTIMIZATION WORKFLOW:
    =====================
    1. Write query
    2. Run explain()
    3. Look for:
       • Multiple Exchange (shuffles)
       • Full table scans
       • Inefficient joins
    4. Optimize code
    5. Re-run explain()
    6. Compare performance
    """)
    
    # Sample optimization example
    sales = [
        ("Electronics", "USA", 1200),
        ("Clothing", "USA", 450),
        ("Electronics", "Canada", 800),
        ("Furniture", "USA", 1500),
    ] * 100  # Repeat for larger dataset
    
    df = spark.createDataFrame(sales, ["category", "country", "amount"])
    
    print("\n📊 BEFORE OPTIMIZATION:")
    print("Query: Filter after aggregation")
    
    # Inefficient: Aggregate then filter
    bad_query = df.groupBy("category", "country") \
        .agg(sum("amount").alias("total")) \
        .filter(col("country") == "USA")
    
    print("\n🔍 PLAN (Inefficient):")
    bad_query.explain()
    
    print("\n📊 AFTER OPTIMIZATION:")
    print("Query: Filter before aggregation")
    
    # Efficient: Filter then aggregate
    good_query = df.filter(col("country") == "USA") \
        .groupBy("category", "country") \
        .agg(sum("amount").alias("total"))
    
    print("\n🔍 PLAN (Optimized):")
    good_query.explain()
    
    print("""
    
    ✅ OPTIMIZATION RESULT:
    • Filter applied before aggregation
    • Less data to aggregate
    • Faster execution
    • Same result!
    """)
    
    print("\n" + "=" * 70)
    print("✅ OPTIMIZATION TIPS COMPLETE")
    print("=" * 70)


def main():
    """Run all execution plan examples."""
    spark = create_spark_session()
    
    try:
        example_1_what_is_execution_plan(spark)
        example_2_explain_methods(spark)
        example_3_catalyst_optimizer(spark)
        example_4_reading_execution_plans(spark)
        example_5_shuffle_operations(spark)
        example_6_query_optimization_tips(spark)
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS:")
        print("=" * 70)
        print("""
        1. EXECUTION PLAN = How Spark runs your query
           • Logical Plan: What you want
           • Physical Plan: How it's done
        
        2. View plans with explain():
           • df.explain() - Physical plan
           • df.explain(extended=True) - All plans
        
        3. Catalyst Optimizer:
           • Predicate pushdown (filter at source)
           • Column pruning (read only needed columns)
           • Constant folding, join reordering
        
        4. Read plans BOTTOM to TOP:
           • Scan → Filter → Aggregate → Project
        
        5. Watch for SHUFFLES (Exchange):
           • Expensive network operations
           • Minimize with broadcast joins
           • Pre-partition data
        
        6. Use plans for optimization:
           • Identify bottlenecks
           • Minimize shuffles
           • Push filters down
           • Select columns early
        
        7. Common operations:
           • Scan: Read data
           • Filter: WHERE clause
           • Aggregate: GROUP BY
           • Exchange: Shuffle data
           • Join: Combine datasets
        """)
        
    finally:
        spark.stop()
        print("\n✅ Spark session stopped")


if __name__ == "__main__":
    main()
