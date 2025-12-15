#!/usr/bin/env python3
"""
================================================================================
EXAMPLE 1: SPARK UI BASICS - UNDERSTANDING THE WEB INTERFACE
================================================================================

PURPOSE:
    Learn how to use Spark's built-in Web UI for monitoring and debugging

WHAT YOU'LL LEARN:
    • Access and navigate the Spark UI
    • Understand Jobs, Stages, and Tasks
    • Monitor job execution in real-time
    • View SQL query plans
    • Analyze performance bottlenecks

SPARK UI TABS:
    • Jobs - List of all Spark jobs
    • Stages - Breakdown of job stages
    • Storage - Cached RDDs and DataFrames
    • Environment - Spark configuration
    • Executors - Executor statistics
    • SQL - SQL query execution plans

TIME: 15 minutes | DIFFICULTY: ⭐⭐☆☆☆
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, sum as spark_sum, count, avg
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create SparkSession with UI enabled."""
    logger.info("Creating SparkSession with Web UI enabled...")
    
    spark = SparkSession.builder \
        .appName("SparkUI_Basics_Demo") \
        .master("local[4]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.retainedJobs", "100") \
        .config("spark.ui.retainedStages", "100") \
        .config("spark.ui.retainedTasks", "1000") \
        .config("spark.sql.ui.retainedExecutions", "100") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ SparkSession created")
    logger.info(f"🌐 Spark UI available at: http://localhost:4040")
    logger.info(f"   Application ID: {spark.sparkContext.applicationId}")
    
    return spark


def job_1_simple_transformation(spark):
    """
    Job 1: Simple transformation to demonstrate basic Spark UI features.
    
    What to observe in UI:
    - Jobs tab: New job created
    - Stages tab: Single stage with tasks
    - SQL tab: Physical plan
    """
    print("\n" + "="*80)
    print("JOB 1: Simple Transformation")
    print("="*80)
    
    logger.info("🚀 Starting Job 1...")
    
    # Create simple DataFrame
    df = spark.range(0, 1000000).toDF("id")
    df = df.withColumn("value", (col("id") * rand()).cast("int"))
    
    # Trigger action
    count_result = df.count()
    
    logger.info(f"✅ Job 1 completed: {count_result:,} records")
    
    print(f"\n📊 UI Navigation:")
    print(f"   1. Open: http://localhost:4040")
    print(f"   2. Click 'Jobs' tab - See this job listed")
    print(f"   3. Click on job description to see details")
    print(f"   4. View 'Event Timeline' - See task execution")
    
    time.sleep(2)


def job_2_multiple_stages(spark):
    """
    Job 2: Multiple stages to show stage dependencies.
    
    What to observe in UI:
    - Multiple stages (shuffle creates new stage)
    - Stage DAG visualization
    - Shuffle read/write metrics
    """
    print("\n" + "="*80)
    print("JOB 2: Multiple Stages (with Shuffle)")
    print("="*80)
    
    logger.info("🚀 Starting Job 2...")
    
    # Create DataFrame with groupBy (causes shuffle)
    df = spark.range(0, 1000000).toDF("id")
    df = df.withColumn("category", (col("id") % 10).cast("int"))
    df = df.withColumn("value", (rand() * 100).cast("int"))
    
    # GroupBy causes shuffle - creates multiple stages
    result = df.groupBy("category").agg(
        spark_sum("value").alias("total"),
        count("id").alias("count"),
        avg("value").alias("avg")
    )
    
    # Trigger action
    result.show(10)
    
    logger.info("✅ Job 2 completed")
    
    print(f"\n📊 UI Analysis:")
    print(f"   1. Jobs tab - See 2 stages (pre-shuffle and post-shuffle)")
    print(f"   2. Click on job - View DAG visualization")
    print(f"   3. Stages tab - Examine each stage individually")
    print(f"   4. Look at 'Shuffle Read' and 'Shuffle Write' metrics")
    
    time.sleep(2)


def job_3_sql_query(spark):
    """
    Job 3: SQL query to demonstrate SQL tab features.
    
    What to observe in UI:
    - SQL tab: Query execution plan
    - Physical plan vs logical plan
    - Query execution timeline
    """
    print("\n" + "="*80)
    print("JOB 3: SQL Query Execution")
    print("="*80)
    
    logger.info("🚀 Starting Job 3...")
    
    # Create sample data
    employees_df = spark.range(0, 10000).toDF("emp_id")
    employees_df = employees_df.withColumn("name", col("emp_id").cast("string"))
    employees_df = employees_df.withColumn("department", (col("emp_id") % 5).cast("int"))
    employees_df = employees_df.withColumn("salary", (rand() * 50000 + 50000).cast("double"))
    
    # Register as temp view
    employees_df.createOrReplaceTempView("employees")
    
    # Execute SQL query
    logger.info("📊 Running SQL query...")
    result = spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary
        FROM employees
        WHERE salary > 60000
        GROUP BY department
        ORDER BY avg_salary DESC
    """)
    
    result.show()
    
    logger.info("✅ Job 3 completed")
    
    print(f"\n📊 SQL Tab Features:")
    print(f"   1. Click 'SQL' tab in UI")
    print(f"   2. See the query we just executed")
    print(f"   3. Click 'Details' - View execution plan")
    print(f"   4. Examine: Scan, Filter, Aggregate, Sort operations")
    print(f"   5. Check 'Duration' column for slow operations")
    
    time.sleep(2)


def job_4_caching_demo(spark):
    """
    Job 4: DataFrame caching to demonstrate Storage tab.
    
    What to observe in UI:
    - Storage tab: Cached DataFrames
    - Memory usage
    - Partition information
    """
    print("\n" + "="*80)
    print("JOB 4: Caching and Storage")
    print("="*80)
    
    logger.info("🚀 Starting Job 4...")
    
    # Create large DataFrame
    df = spark.range(0, 5000000).toDF("id")
    df = df.withColumn("value", (rand() * 1000).cast("int"))
    df = df.withColumn("category", (col("id") % 100).cast("int"))
    
    # Cache the DataFrame
    logger.info("💾 Caching DataFrame...")
    df.cache()
    
    # Trigger action to materialize cache
    count1 = df.count()
    logger.info(f"   First count: {count1:,} records (loaded into cache)")
    
    # Use cached data
    count2 = df.filter(col("value") > 500).count()
    logger.info(f"   Filtered count: {count2:,} records (from cache)")
    
    logger.info("✅ Job 4 completed")
    
    print(f"\n📊 Storage Analysis:")
    print(f"   1. Click 'Storage' tab in UI")
    print(f"   2. See cached DataFrame listed")
    print(f"   3. View memory used and partition count")
    print(f"   4. Click on RDD name for detailed partition info")
    print(f"   5. Notice second action was faster (using cache)")
    
    # Keep cache for viewing
    time.sleep(3)
    
    # Unpersist
    df.unpersist()
    logger.info("🗑️  Cache cleared")


def job_5_executor_metrics(spark):
    """
    Job 5: Heavy computation to show executor metrics.
    
    What to observe in UI:
    - Executors tab: CPU, memory usage
    - Task metrics per executor
    - GC time
    """
    print("\n" + "="*80)
    print("JOB 5: Executor Metrics")
    print("="*80)
    
    logger.info("🚀 Starting Job 5...")
    
    # Create computationally intensive job
    df = spark.range(0, 2000000).toDF("id")
    
    # Multiple transformations
    for i in range(5):
        df = df.withColumn(f"col_{i}", (rand() * col("id")).cast("int"))
    
    # Aggregation
    result = df.selectExpr("sum(col_0)", "avg(col_1)", "max(col_2)", "min(col_3)")
    result.show()
    
    logger.info("✅ Job 5 completed")
    
    print(f"\n📊 Executor Metrics:")
    print(f"   1. Click 'Executors' tab in UI")
    print(f"   2. See executor statistics")
    print(f"   3. Check 'Storage Memory' used")
    print(f"   4. View 'Tasks' completed per executor")
    print(f"   5. Examine 'GC Time' - high GC can indicate memory pressure")
    
    time.sleep(2)


def job_6_slow_task_simulation(spark):
    """
    Job 6: Simulate slow tasks to demonstrate task analysis.
    
    What to observe in UI:
    - Task duration distribution
    - Slow tasks identification
    - Skewed data processing
    """
    print("\n" + "="*80)
    print("JOB 6: Task Performance Analysis")
    print("="*80)
    
    logger.info("🚀 Starting Job 6...")
    
    # Create DataFrame with skewed data
    df = spark.range(0, 100000).toDF("id")
    df = df.withColumn("key", (col("id") % 10).cast("int"))
    df = df.withColumn("value", rand())
    
    # This will cause some tasks to process more data
    result = df.groupBy("key").agg(
        count("id").alias("count"),
        spark_sum("value").alias("total")
    )
    
    result.show()
    
    logger.info("✅ Job 6 completed")
    
    print(f"\n📊 Task Analysis:")
    print(f"   1. Go to Jobs tab, click on this job")
    print(f"   2. Click on a stage")
    print(f"   3. Scroll to 'Tasks' section")
    print(f"   4. Sort by 'Duration' - identify slow tasks")
    print(f"   5. Look at 'Event Timeline' for visual representation")
    print(f"   6. Check 'Shuffle Read Size' - uneven distribution indicates skew")
    
    time.sleep(2)


def demo_environment_tab(spark):
    """Display information about Environment tab."""
    print("\n" + "="*80)
    print("ENVIRONMENT TAB OVERVIEW")
    print("="*80)
    
    print(f"\n📊 What's in the Environment Tab:")
    print(f"   • Spark Properties - All configuration settings")
    print(f"   • System Properties - JVM settings")
    print(f"   • Classpath Entries - JAR files loaded")
    print(f"   • Runtime Information - Scala version, Java version")
    
    print(f"\n🔧 Useful for:")
    print(f"   • Verifying configuration is correct")
    print(f"   • Debugging classpath issues")
    print(f"   • Checking memory settings")
    print(f"   • Confirming Spark version")


def main():
    """Run all Spark UI demonstration jobs."""
    
    print("\n" + "="*80)
    print("🎯 SPARK UI BASICS - INTERACTIVE DEMO")
    print("="*80)
    
    print(f"\n📋 What We'll Demonstrate:")
    print(f"   1. Simple transformations")
    print(f"   2. Multiple stages with shuffle")
    print(f"   3. SQL query execution plans")
    print(f"   4. DataFrame caching (Storage tab)")
    print(f"   5. Executor metrics")
    print(f"   6. Task performance analysis")
    
    spark = create_spark_session()
    
    print(f"\n" + "="*80)
    print(f"🌐 OPEN SPARK UI NOW: http://localhost:4040")
    print(f"="*80)
    print(f"\n⏸️  Keep the UI open in your browser while running examples...")
    
    input("\n👉 Press Enter when UI is open and you're ready to continue...")
    
    try:
        # Run all jobs with pauses
        job_1_simple_transformation(spark)
        input("\n⏸️  Review Job 1 in UI, then press Enter to continue...")
        
        job_2_multiple_stages(spark)
        input("\n⏸️  Review Job 2 stages in UI, then press Enter...")
        
        job_3_sql_query(spark)
        input("\n⏸️  Check SQL tab, then press Enter...")
        
        job_4_caching_demo(spark)
        input("\n⏸️  Review Storage tab, then press Enter...")
        
        job_5_executor_metrics(spark)
        input("\n⏸️  Check Executors tab, then press Enter...")
        
        job_6_slow_task_simulation(spark)
        input("\n⏸️  Analyze task metrics, then press Enter...")
        
        demo_environment_tab(spark)
        
        # Final summary
        print("\n" + "="*80)
        print("✅ ALL DEMONSTRATIONS COMPLETED")
        print("="*80)
        
        print(f"\n📊 Spark UI Tabs Summary:")
        print(f"   • Jobs - All Spark jobs executed")
        print(f"   • Stages - Stage-level details and DAG")
        print(f"   • Storage - Cached DataFrames/RDDs")
        print(f"   • Environment - Configuration settings")
        print(f"   • Executors - Resource usage statistics")
        print(f"   • SQL - Query execution plans")
        
        print(f"\n💡 Key Takeaways:")
        print(f"   ✓ Use Jobs tab to see overall job status")
        print(f"   ✓ Stages tab shows detailed execution breakdown")
        print(f"   ✓ SQL tab visualizes query execution plans")
        print(f"   ✓ Storage tab monitors cached data")
        print(f"   ✓ Executors tab tracks resource usage")
        print(f"   ✓ Task metrics help identify bottlenecks")
        
        print(f"\n🎯 Next Steps:")
        print(f"   1. Run your own Spark jobs and monitor in UI")
        print(f"   2. Practice identifying performance issues")
        print(f"   3. Learn to read execution plans")
        print(f"   4. Check out 02_spark_history_server.py for archived jobs")
        
        print(f"\n⏸️  UI will remain available for 60 seconds...")
        time.sleep(60)
        
    except KeyboardInterrupt:
        logger.info("\n👋 Interrupted by user")
    
    finally:
        logger.info("🛑 Stopping Spark session...")
        spark.stop()
        logger.info("✅ Done! UI is now closed.")


if __name__ == "__main__":
    main()
