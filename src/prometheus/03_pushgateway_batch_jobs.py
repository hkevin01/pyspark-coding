#!/usr/bin/env python3
"""
================================================================================
EXAMPLE 3: PROMETHEUS PUSHGATEWAY FOR BATCH JOBS
================================================================================

PURPOSE:
    Use Prometheus Pushgateway to collect metrics from batch/scheduled jobs

WHAT YOU'LL LEARN:
    • Push metrics to Pushgateway
    • Track batch job completion
    • Monitor job success/failure
    • Handle transient jobs
    • Aggregate metrics across multiple runs

WHY PUSHGATEWAY:
    - Regular Prometheus scraping doesn't work for short-lived jobs
    - Batch jobs finish before Prometheus can scrape them
    - Pushgateway stores metrics until Prometheus scrapes it

USE CASES:
    • Scheduled ETL jobs
    • Nightly batch processing
    • Data migrations
    • One-time data imports

TIME: 20 minutes | DIFFICULTY: ⭐⭐⭐⭐☆
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, current_timestamp, lit
from prometheus_client import (
    CollectorRegistry, Counter, Gauge, Histogram, 
    push_to_gateway, delete_from_gateway
)
import time
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════════
# PUSHGATEWAY CONFIGURATION
# ════════════════════════════════════════════════════════════════════════════════

# Pushgateway URL (update if running Pushgateway locally)
PUSHGATEWAY_URL = 'localhost:9091'

# Job identifier (appears as job label in Prometheus)
JOB_NAME = 'pyspark_batch_etl'


# ════════════════════════════════════════════════════════════════════════════════
# BATCH JOB METRICS
# ════════════════════════════════════════════════════════════════════════════════

def create_batch_job_registry():
    """Create a separate registry for batch job metrics."""
    registry = CollectorRegistry()
    
    # Job completion metrics
    job_last_success_timestamp = Gauge(
        'batch_job_last_success_timestamp_seconds',
        'Timestamp of last successful job completion',
        ['job_name'],
        registry=registry
    )
    
    job_duration = Gauge(
        'batch_job_duration_seconds',
        'Duration of the batch job in seconds',
        ['job_name'],
        registry=registry
    )
    
    job_records_processed = Gauge(
        'batch_job_records_processed_total',
        'Total records processed in batch job',
        ['job_name'],
        registry=registry
    )
    
    job_status = Gauge(
        'batch_job_status',
        'Status of the job (1=success, 0=failure)',
        ['job_name'],
        registry=registry
    )
    
    job_errors = Gauge(
        'batch_job_errors_total',
        'Total errors encountered in batch job',
        ['job_name', 'error_type'],
        registry=registry
    )
    
    return registry, {
        'success_timestamp': job_last_success_timestamp,
        'duration': job_duration,
        'records_processed': job_records_processed,
        'status': job_status,
        'errors': job_errors
    }


# ════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════════

def create_spark_session():
    """Create SparkSession."""
    spark = SparkSession.builder \
        .appName("PushgatewayBatch_Example") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def push_metrics(registry, job_name, instance=''):
    """Push metrics to Pushgateway."""
    try:
        logger.info(f"📤 Pushing metrics to Pushgateway: {PUSHGATEWAY_URL}")
        push_to_gateway(
            gateway=PUSHGATEWAY_URL,
            job=job_name,
            registry=registry,
            grouping_key={'instance': instance} if instance else {}
        )
        logger.info("✅ Metrics pushed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to push metrics: {e}")
        logger.warning(f"💡 Make sure Pushgateway is running: docker run -d -p 9091:9091 prom/pushgateway")
        return False


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 1: SIMPLE BATCH JOB
# ════════════════════════════════════════════════════════════════════════════════

def example_1_simple_batch_job(spark):
    """
    Simple batch ETL job with metrics pushed to Pushgateway.
    """
    print("\n" + "="*80)
    print("EXAMPLE 1: Simple Batch Job with Pushgateway")
    print("="*80)
    
    job_name = "daily_sales_etl"
    
    # Create metrics registry
    registry, metrics = create_batch_job_registry()
    
    start_time = time.time()
    error_count = 0
    
    try:
        logger.info(f"🚀 Starting batch job: {job_name}")
        
        # Simulate ETL work
        logger.info("📊 Extracting data...")
        df = spark.range(0, 100000).toDF("sale_id")
        df = df.withColumn("amount", (rand() * 500).cast("double"))
        df = df.withColumn("timestamp", current_timestamp())
        
        logger.info("🔄 Transforming data...")
        df_transformed = df.filter(col("amount") > 50) \
                          .withColumn("category", when(col("amount") > 250, "high").otherwise("medium"))
        
        logger.info("💾 Loading data...")
        records_processed = df_transformed.count()
        
        # Job completed successfully
        duration = time.time() - start_time
        
        # Update metrics
        metrics['success_timestamp'].labels(job_name=job_name).set(time.time())
        metrics['duration'].labels(job_name=job_name).set(duration)
        metrics['records_processed'].labels(job_name=job_name).set(records_processed)
        metrics['status'].labels(job_name=job_name).set(1)  # Success
        metrics['errors'].labels(job_name=job_name, error_type="none").set(0)
        
        print(f"\n✅ Job completed successfully:")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Records processed: {records_processed:,}")
        print(f"   Errors: {error_count}")
        
    except Exception as e:
        # Job failed
        duration = time.time() - start_time
        error_count = 1
        
        metrics['duration'].labels(job_name=job_name).set(duration)
        metrics['status'].labels(job_name=job_name).set(0)  # Failure
        metrics['errors'].labels(job_name=job_name, error_type="execution_error").set(1)
        
        logger.error(f"❌ Job failed: {e}")
    
    # Push metrics to Pushgateway
    push_metrics(registry, job_name)
    
    print(f"\n📊 Metrics pushed to Pushgateway")
    print(f"   View at: http://{PUSHGATEWAY_URL}/metrics")


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 2: MULTIPLE BATCH RUNS
# ════════════════════════════════════════════════════════════════════════════════

def example_2_multiple_batch_runs(spark):
    """
    Simulate multiple runs of a batch job with different outcomes.
    """
    print("\n" + "="*80)
    print("EXAMPLE 2: Multiple Batch Runs")
    print("="*80)
    
    job_name = "hourly_aggregation"
    num_runs = 5
    
    print(f"\n🔄 Running {num_runs} batch iterations...\n")
    
    for run_num in range(1, num_runs + 1):
        instance_id = f"run_{run_num}"
        
        # Create separate registry for each run
        registry, metrics = create_batch_job_registry()
        
        start_time = time.time()
        
        try:
            logger.info(f"📊 Run {run_num}/{num_runs} - Instance: {instance_id}")
            
            # Variable dataset size
            size = run_num * 20000
            df = spark.range(0, size).toDF("record_id")
            df = df.withColumn("value", (rand() * 100).cast("int"))
            
            # Process data
            result = df.groupBy("value").count()
            records = result.count()
            
            # Simulate occasional failure
            if run_num == 3:
                raise Exception("Simulated processing error")
            
            duration = time.time() - start_time
            
            # Update metrics
            metrics['success_timestamp'].labels(job_name=job_name).set(time.time())
            metrics['duration'].labels(job_name=job_name).set(duration)
            metrics['records_processed'].labels(job_name=job_name).set(size)
            metrics['status'].labels(job_name=job_name).set(1)
            
            print(f"   ✅ Success | {size:,} records | {duration:.2f}s")
            
        except Exception as e:
            duration = time.time() - start_time
            
            metrics['duration'].labels(job_name=job_name).set(duration)
            metrics['status'].labels(job_name=job_name).set(0)
            metrics['errors'].labels(job_name=job_name, error_type="processing").set(1)
            
            print(f"   ❌ Failed | {str(e)} | {duration:.2f}s")
        
        # Push metrics for this run
        push_metrics(registry, job_name, instance=instance_id)
        
        time.sleep(1)
    
    print(f"\n📊 All runs completed")
    print(f"   View aggregated metrics at: http://{PUSHGATEWAY_URL}/metrics")


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 3: CLEANUP AND MONITORING
# ════════════════════════════════════════════════════════════════════════════════

def example_3_cleanup_monitoring(spark):
    """
    Demonstrate metric cleanup and monitoring best practices.
    """
    print("\n" + "="*80)
    print("EXAMPLE 3: Cleanup and Monitoring")
    print("="*80)
    
    job_name = "nightly_backup"
    
    print(f"\n🧹 Cleaning up old metrics for job: {job_name}")
    
    try:
        # Delete old metrics from Pushgateway
        delete_from_gateway(
            gateway=PUSHGATEWAY_URL,
            job=job_name
        )
        logger.info(f"✅ Old metrics deleted")
    except Exception as e:
        logger.warning(f"⚠️  Could not delete metrics: {e}")
    
    # Run fresh job
    print(f"\n🚀 Running fresh job...")
    
    registry, metrics = create_batch_job_registry()
    
    start_time = time.time()
    
    # Simulate backup job
    df = spark.range(0, 50000).toDF("id")
    df = df.withColumn("data", col("id").cast("string"))
    
    records = df.count()
    duration = time.time() - start_time
    
    # Update metrics
    metrics['success_timestamp'].labels(job_name=job_name).set(time.time())
    metrics['duration'].labels(job_name=job_name).set(duration)
    metrics['records_processed'].labels(job_name=job_name).set(records)
    metrics['status'].labels(job_name=job_name).set(1)
    
    print(f"\n✅ Backup completed:")
    print(f"   Records: {records:,}")
    print(f"   Duration: {duration:.2f}s")
    print(f"   Timestamp: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Push fresh metrics
    push_metrics(registry, job_name)
    
    print(f"\n💡 Monitoring Tips:")
    print(f"   1. Check last success: batch_job_last_success_timestamp_seconds")
    print(f"   2. Alert if no recent success: time() - batch_job_last_success_timestamp_seconds > 86400")
    print(f"   3. Monitor failure rate: batch_job_status == 0")
    print(f"   4. Track duration trends: batch_job_duration_seconds")


# ════════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

def main():
    """Run all Pushgateway examples."""
    
    print("\n" + "="*80)
    print("🚀 PROMETHEUS PUSHGATEWAY INTEGRATION")
    print("="*80)
    
    print(f"\n⚙️  Configuration:")
    print(f"   Pushgateway URL: {PUSHGATEWAY_URL}")
    print(f"   Job name: {JOB_NAME}")
    
    print(f"\n📝 Prerequisites:")
    print(f"   1. Pushgateway running: docker run -d -p 9091:9091 prom/pushgateway")
    print(f"   2. View metrics: http://localhost:9091")
    
    print(f"\n💡 NOTE: If Pushgateway is not running, examples will show errors")
    print(f"   but will continue to demonstrate the code patterns.\n")
    
    spark = create_spark_session()
    
    try:
        example_1_simple_batch_job(spark)
        example_2_multiple_batch_runs(spark)
        example_3_cleanup_monitoring(spark)
        
        # Summary
        print("\n" + "="*80)
        print("✅ ALL EXAMPLES COMPLETED")
        print("="*80)
        
        print(f"\n📊 What We Did:")
        print(f"   • Pushed metrics from batch jobs to Pushgateway")
        print(f"   • Tracked job success/failure rates")
        print(f"   • Monitored processing duration and volume")
        print(f"   • Demonstrated cleanup of old metrics")
        
        print(f"\n🔍 Prometheus Queries:")
        print(f"   # Jobs that haven't run in 24 hours:")
        print(f"   time() - batch_job_last_success_timestamp_seconds > 86400")
        print(f"\n   # Current job success rate:")
        print(f"   avg_over_time(batch_job_status[1h])")
        print(f"\n   # Average job duration:")
        print(f"   avg(batch_job_duration_seconds)")
        
        print(f"\n🌐 Access Points:")
        print(f"   Pushgateway UI: http://{PUSHGATEWAY_URL}")
        print(f"   Metrics endpoint: http://{PUSHGATEWAY_URL}/metrics")
        
    except KeyboardInterrupt:
        logger.info("\n👋 Interrupted")
    finally:
        spark.stop()
        logger.info("✅ Done!")


if __name__ == "__main__":
    main()
