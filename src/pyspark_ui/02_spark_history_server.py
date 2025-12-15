#!/usr/bin/env python3
"""
================================================================================
EXAMPLE 2: SPARK HISTORY SERVER - ANALYZING PAST JOBS
================================================================================

PURPOSE:
    Learn to use Spark History Server for analyzing completed applications

WHAT YOU'LL LEARN:
    • Enable event logging
    • Start Spark History Server
    • View historical job data
    • Compare multiple runs
    • Analyze trends over time

WHY HISTORY SERVER:
    - Regular Spark UI dies when application stops
    - History Server persists job data
    - Compare performance across runs
    - Debug issues after application completes

TIME: 20 minutes | DIFFICULTY: ⭐⭐⭐☆☆
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, sum as spark_sum, count
import time
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Event log directory
EVENT_LOG_DIR = "/tmp/spark-events"


def setup_event_logging():
    """Create event log directory."""
    os.makedirs(EVENT_LOG_DIR, exist_ok=True)
    logger.info(f"📁 Event log directory: {EVENT_LOG_DIR}")


def create_spark_session(app_name):
    """Create SparkSession with event logging enabled."""
    logger.info(f"Creating SparkSession: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[4]") \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", f"file://{EVENT_LOG_DIR}") \
        .config("spark.history.fs.logDirectory", f"file://{EVENT_LOG_DIR}") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✅ Session created: {spark.sparkContext.applicationId}")
    return spark


def run_etl_job_v1(spark):
    """Run ETL job - Version 1 (Unoptimized)."""
    logger.info("🚀 Running ETL Job v1 (Unoptimized)...")
    
    start_time = time.time()
    
    # Read data
    df = spark.range(0, 10000000).toDF("id")
    df = df.withColumn("value", (rand() * 1000).cast("int"))
    df = df.withColumn("category", (col("id") % 100).cast("int"))
    
    # Unoptimized: Multiple actions
    total = df.count()
    filtered = df.filter(col("value") > 500).count()
    grouped = df.groupBy("category").count().count()
    
    duration = time.time() - start_time
    
    logger.info(f"✅ Job v1 completed in {duration:.2f}s")
    logger.info(f"   Total: {total:,}, Filtered: {filtered:,}, Groups: {grouped}")
    
    return duration


def run_etl_job_v2(spark):
    """Run ETL job - Version 2 (Optimized with caching)."""
    logger.info("🚀 Running ETL Job v2 (Optimized)...")
    
    start_time = time.time()
    
    # Read data
    df = spark.range(0, 10000000).toDF("id")
    df = df.withColumn("value", (rand() * 1000).cast("int"))
    df = df.withColumn("category", (col("id") % 100).cast("int"))
    
    # Optimized: Cache and reuse
    df.cache()
    
    total = df.count()
    filtered = df.filter(col("value") > 500).count()
    grouped = df.groupBy("category").count().count()
    
    df.unpersist()
    
    duration = time.time() - start_time
    
    logger.info(f"✅ Job v2 completed in {duration:.2f}s")
    logger.info(f"   Total: {total:,}, Filtered: {filtered:,}, Groups: {grouped}")
    
    return duration


def run_batch_processing_job(spark, batch_num):
    """Simulate a batch processing job."""
    logger.info(f"📊 Processing batch {batch_num}...")
    
    # Generate batch data
    df = spark.range(0, 1000000).toDF("record_id")
    df = df.withColumn("value", (rand() * 100).cast("double"))
    df = df.withColumn("timestamp", col("record_id").cast("timestamp"))
    
    # Process
    result = df.groupBy("value").agg(count("record_id").alias("count"))
    result_count = result.count()
    
    logger.info(f"   ✓ Batch {batch_num}: {result_count} unique values")


def demonstrate_history_server():
    """Provide instructions for starting History Server."""
    print("\n" + "="*80)
    print("🌐 STARTING SPARK HISTORY SERVER")
    print("="*80)
    
    print(f"\n📋 Event logs location: {EVENT_LOG_DIR}")
    
    print(f"\n🚀 How to Start History Server:")
    print(f"\n   Option 1: Using spark-submit")
    print(f"   ─────────────────────────────")
    print(f"   $SPARK_HOME/sbin/start-history-server.sh \\")
    print(f"     --properties-file $SPARK_HOME/conf/spark-defaults.conf")
    
    print(f"\n   Option 2: Manual command")
    print(f"   ────────────────────────")
    print(f"   spark-submit \\")
    print(f"     --class org.apache.spark.deploy.history.HistoryServer \\")
    print(f"     --conf spark.history.fs.logDirectory=file://{EVENT_LOG_DIR} \\")
    print(f"     --conf spark.history.ui.port=18080")
    
    print(f"\n   Option 3: Python script")
    print(f"   ───────────────────────")
    print(f"   # Create a simple launcher")
    
    print(f"\n📊 After starting, access:")
    print(f"   URL: http://localhost:18080")
    
    print(f"\n💡 What you can do:")
    print(f"   • View all completed applications")
    print(f"   • Compare performance across runs")
    print(f"   • Analyze historical trends")
    print(f"   • Debug past failures")
    print(f"   • Export metrics for reporting")


def main():
    """Run History Server demonstration."""
    
    print("\n" + "="*80)
    print("🎯 SPARK HISTORY SERVER DEMO")
    print("="*80)
    
    print(f"\n📋 What We'll Do:")
    print(f"   1. Enable event logging")
    print(f"   2. Run multiple Spark jobs")
    print(f"   3. Generate event logs")
    print(f"   4. Show how to start History Server")
    print(f"   5. Analyze jobs in History Server UI")
    
    # Setup
    setup_event_logging()
    
    print(f"\n" + "="*80)
    print("RUNNING SAMPLE JOBS (Generating Event Logs)")
    print("="*80)
    
    # Job 1: Unoptimized ETL
    print(f"\n📊 Job 1: ETL Pipeline (Unoptimized)")
    spark1 = create_spark_session("ETL_Pipeline_v1_Unoptimized")
    try:
        duration1 = run_etl_job_v1(spark1)
    finally:
        spark1.stop()
    
    time.sleep(2)
    
    # Job 2: Optimized ETL
    print(f"\n📊 Job 2: ETL Pipeline (Optimized)")
    spark2 = create_spark_session("ETL_Pipeline_v2_Optimized")
    try:
        duration2 = run_etl_job_v2(spark2)
    finally:
        spark2.stop()
    
    time.sleep(2)
    
    # Job 3: Batch processing
    print(f"\n📊 Job 3: Batch Processing (Multiple Batches)")
    spark3 = create_spark_session("Batch_Processing_Job")
    try:
        for i in range(1, 4):
            run_batch_processing_job(spark3, i)
    finally:
        spark3.stop()
    
    # Summary
    print("\n" + "="*80)
    print("✅ ALL JOBS COMPLETED - EVENT LOGS GENERATED")
    print("="*80)
    
    print(f"\n📊 Performance Comparison:")
    print(f"   ETL v1 (Unoptimized): {duration1:.2f}s")
    print(f"   ETL v2 (Optimized):   {duration2:.2f}s")
    print(f"   Improvement: {((duration1-duration2)/duration1*100):.1f}%")
    
    # List event logs
    print(f"\n📁 Event Logs Created:")
    try:
        logs = os.listdir(EVENT_LOG_DIR)
        for i, log in enumerate(logs, 1):
            print(f"   {i}. {log}")
    except Exception as e:
        logger.error(f"Error listing logs: {e}")
    
    # History Server instructions
    demonstrate_history_server()
    
    print("\n" + "="*80)
    print("🎓 HISTORY SERVER ANALYSIS GUIDE")
    print("="*80)
    
    print(f"\n📊 What to Look For:")
    print(f"\n   1. Application List")
    print(f"      • Compare durations across runs")
    print(f"      • Identify failed applications")
    print(f"      • Track resource usage trends")
    
    print(f"\n   2. Job Comparison")
    print(f"      • Compare v1 vs v2 ETL jobs")
    print(f"      • See impact of caching")
    print(f"      • Analyze stage performance")
    
    print(f"\n   3. Timeline View")
    print(f"      • Visualize job execution order")
    print(f"      • Identify idle time")
    print(f"      • Spot resource bottlenecks")
    
    print(f"\n   4. Storage Tab")
    print(f"      • See cached data in v2 job")
    print(f"      • Memory usage patterns")
    
    print(f"\n   5. Environment")
    print(f"      • Verify configurations")
    print(f"      • Compare settings across runs")
    
    print(f"\n💡 Pro Tips:")
    print(f"   • Keep event logs for trend analysis")
    print(f"   • Set retention policy to manage disk space")
    print(f"   • Export metrics for dashboards")
    print(f"   • Use History Server for post-mortem debugging")
    
    print(f"\n🔧 Event Log Configuration:")
    print(f"   spark.eventLog.enabled=true")
    print(f"   spark.eventLog.dir=file://{EVENT_LOG_DIR}")
    print(f"   spark.history.fs.logDirectory=file://{EVENT_LOG_DIR}")
    print(f"   spark.history.ui.port=18080")
    
    print(f"\n📖 Next Steps:")
    print(f"   1. Start History Server")
    print(f"   2. Open http://localhost:18080")
    print(f"   3. Compare the 3 applications we just ran")
    print(f"   4. Analyze performance differences")
    print(f"   5. Try 03_ui_programmatic_access.py for API access")
    
    logger.info(f"\n✅ Demo complete!")


if __name__ == "__main__":
    main()
