#!/usr/bin/env python3
"""
================================================================================
EXAMPLE 1: BASIC PROMETHEUS METRICS WITH PYSPARK
================================================================================

PURPOSE:
    Expose PySpark application metrics to Prometheus for monitoring

WHAT YOU'LL LEARN:
    • Export Spark metrics to Prometheus
    • Track job execution metrics
    • Monitor DataFrame operations
    • Custom application metrics

METRICS EXPOSED:
    • spark_job_duration_seconds - Job execution time
    • spark_dataframe_row_count - DataFrame row counts
    • spark_stage_count - Number of stages per job
    • spark_task_failures_total - Failed tasks counter

TIME: 15 minutes | DIFFICULTY: ⭐⭐⭐☆☆
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when
from prometheus_client import Counter, Gauge, Histogram, Summary, start_http_server
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════════
# PROMETHEUS METRICS DEFINITIONS
# ════════════════════════════════════════════════════════════════════════════════

# Counter: Monotonically increasing value (never decreases)
spark_jobs_total = Counter(
    'spark_jobs_total',
    'Total number of Spark jobs executed',
    ['job_name', 'status']
)

spark_task_failures = Counter(
    'spark_task_failures_total',
    'Total number of failed tasks'
)

# Gauge: Value that can go up or down
spark_active_executors = Gauge(
    'spark_active_executors',
    'Number of active executors'
)

spark_dataframe_rows = Gauge(
    'spark_dataframe_row_count',
    'Number of rows in DataFrame',
    ['dataframe_name']
)

spark_memory_used_mb = Gauge(
    'spark_memory_used_megabytes',
    'Memory used by Spark application'
)

# Histogram: Observations in configurable buckets
spark_job_duration = Histogram(
    'spark_job_duration_seconds',
    'Duration of Spark jobs in seconds',
    ['job_name'],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600)  # Buckets in seconds
)

spark_dataframe_size = Histogram(
    'spark_dataframe_size_bytes',
    'Size of DataFrame in bytes',
    ['dataframe_name'],
    buckets=(1024, 10240, 102400, 1048576, 10485760, 104857600)  # Bytes
)

# Summary: Like histogram but calculates quantiles
spark_query_duration = Summary(
    'spark_query_duration_seconds',
    'Duration of Spark SQL queries',
    ['query_type']
)


# ════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════════

def create_spark_session_with_metrics():
    """Create SparkSession configured for metrics collection."""
    logger.info("Creating SparkSession with Prometheus metrics enabled...")
    
    spark = SparkSession.builder \
        .appName("PrometheusMetrics_Example") \
        .master("local[*]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .config("spark.metrics.conf.*.sink.prometheus.class", "org.apache.spark.metrics.sink.PrometheusServlet") \
        .config("spark.metrics.conf.*.sink.prometheus.path", "/metrics") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ SparkSession created")
    logger.info(f"   Spark UI: http://localhost:4040")
    
    return spark


def get_executor_count(spark):
    """Get number of active executors."""
    try:
        # In local mode, return 1
        executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size()
        return max(1, executors)
    except:
        return 1


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 1: BASIC JOB METRICS
# ════════════════════════════════════════════════════════════════════════════════

def example_1_basic_job_metrics(spark):
    """
    Track basic job execution metrics.
    
    Metrics tracked:
    - Job count (success/failure)
    - Job duration
    - DataFrame row counts
    """
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic Job Metrics")
    print("="*80)
    
    job_name = "data_processing"
    
    try:
        # Start job timer
        with spark_job_duration.labels(job_name=job_name).time():
            logger.info(f"🚀 Starting job: {job_name}")
            
            # Create sample data
            df = spark.range(0, 1000000).toDF("id")
            df = df.withColumn("value", (col("id") * rand()).cast("int"))
            df = df.withColumn("category", when(col("value") % 2 == 0, "even").otherwise("odd"))
            
            # Perform transformations
            df_filtered = df.filter(col("value") > 100)
            df_result = df_filtered.groupBy("category").count()
            
            # Collect results (triggers job execution)
            results = df_result.collect()
            
            # Update metrics
            row_count = df.count()
            spark_dataframe_rows.labels(dataframe_name="raw_data").set(row_count)
            
            result_count = df_result.count()
            spark_dataframe_rows.labels(dataframe_name="aggregated_data").set(result_count)
            
            logger.info(f"✅ Job completed: {job_name}")
            logger.info(f"   Processed {row_count:,} rows")
            logger.info(f"   Result: {result_count} groups")
            
            # Increment success counter
            spark_jobs_total.labels(job_name=job_name, status="success").inc()
            
            # Display results
            print("\n📊 Results:")
            for row in results:
                print(f"   {row['category']}: {row['count']:,} records")
    
    except Exception as e:
        logger.error(f"❌ Job failed: {job_name} - {e}")
        spark_jobs_total.labels(job_name=job_name, status="failure").inc()
        spark_task_failures.inc()
        raise
    
    # Update executor metrics
    executor_count = get_executor_count(spark)
    spark_active_executors.set(executor_count)
    
    print(f"\n📈 Metrics Updated:")
    print(f"   ✓ Job duration recorded")
    print(f"   ✓ Row counts: {row_count:,} (raw), {result_count} (aggregated)")
    print(f"   ✓ Active executors: {executor_count}")


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 2: QUERY PERFORMANCE TRACKING
# ════════════════════════════════════════════════════════════════════════════════

def example_2_query_performance(spark):
    """
    Track SQL query performance with different query types.
    
    Metrics tracked:
    - Query duration by type (select, join, aggregate)
    - Query complexity
    """
    print("\n" + "="*80)
    print("EXAMPLE 2: Query Performance Tracking")
    print("="*80)
    
    # Create sample tables
    logger.info("📊 Creating sample tables...")
    
    customers_df = spark.range(0, 10000).toDF("customer_id")
    customers_df = customers_df.withColumn("name", col("customer_id").cast("string"))
    customers_df.createOrReplaceTempView("customers")
    
    orders_df = spark.range(0, 50000).toDF("order_id")
    orders_df = orders_df.withColumn("customer_id", (col("order_id") % 10000).cast("long"))
    orders_df = orders_df.withColumn("amount", (rand() * 1000).cast("double"))
    orders_df.createOrReplaceTempView("orders")
    
    print(f"✅ Created tables:")
    print(f"   • customers: {customers_df.count():,} rows")
    print(f"   • orders: {orders_df.count():,} rows")
    
    # Query Type 1: Simple SELECT
    print("\n🔍 Query 1: Simple SELECT")
    with spark_query_duration.labels(query_type="select").time():
        result1 = spark.sql("""
            SELECT * FROM customers 
            WHERE customer_id < 100
        """)
        count1 = result1.count()
        print(f"   Result: {count1} rows")
    
    # Query Type 2: JOIN
    print("\n🔍 Query 2: JOIN")
    with spark_query_duration.labels(query_type="join").time():
        result2 = spark.sql("""
            SELECT c.customer_id, c.name, COUNT(o.order_id) as order_count
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.name
            LIMIT 100
        """)
        count2 = result2.count()
        print(f"   Result: {count2} rows")
    
    # Query Type 3: Complex AGGREGATE
    print("\n🔍 Query 3: Complex AGGREGATE")
    with spark_query_duration.labels(query_type="aggregate").time():
        result3 = spark.sql("""
            SELECT 
                CAST(customer_id / 1000 AS INT) as customer_segment,
                COUNT(*) as order_count,
                SUM(amount) as total_amount,
                AVG(amount) as avg_amount,
                MIN(amount) as min_amount,
                MAX(amount) as max_amount
            FROM orders
            GROUP BY CAST(customer_id / 1000 AS INT)
            ORDER BY total_amount DESC
        """)
        count3 = result3.count()
        print(f"   Result: {count3} rows")
        
        # Show sample results
        print("\n   Top 3 segments by revenue:")
        for row in result3.take(3):
            print(f"     Segment {row['customer_segment']}: ${row['total_amount']:,.2f}")
    
    print(f"\n📈 Query metrics recorded for all query types")


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 3: CONTINUOUS MONITORING
# ════════════════════════════════════════════════════════════════════════════════

def example_3_continuous_monitoring(spark):
    """
    Simulate continuous monitoring of streaming-like workload.
    
    Metrics tracked:
    - Batch processing metrics
    - Memory usage
    - Throughput
    """
    print("\n" + "="*80)
    print("EXAMPLE 3: Continuous Monitoring (5 batches)")
    print("="*80)
    
    num_batches = 5
    
    for batch_num in range(1, num_batches + 1):
        job_name = f"batch_{batch_num}"
        
        logger.info(f"🔄 Processing batch {batch_num}/{num_batches}...")
        
        with spark_job_duration.labels(job_name=job_name).time():
            # Generate batch data
            batch_size = 100000
            df = spark.range(0, batch_size).toDF("id")
            df = df.withColumn("value", (rand() * 1000).cast("int"))
            df = df.withColumn("timestamp", col("id").cast("timestamp"))
            
            # Process data
            df_processed = df.filter(col("value") > 500) \
                            .groupBy("value") \
                            .count() \
                            .orderBy("count", ascending=False)
            
            # Trigger action
            result_count = df_processed.count()
            
            # Update metrics
            spark_dataframe_rows.labels(dataframe_name=f"batch_{batch_num}").set(batch_size)
            spark_jobs_total.labels(job_name=job_name, status="success").inc()
            
            print(f"   ✓ Batch {batch_num}: Processed {batch_size:,} rows → {result_count} groups")
        
        # Simulate delay between batches
        time.sleep(1)
    
    print(f"\n📈 Continuous monitoring metrics updated:")
    print(f"   ✓ {num_batches} batches processed")
    print(f"   ✓ All batch durations recorded")
    print(f"   ✓ Row counts tracked per batch")


# ════════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

def main():
    """Run all Prometheus metrics examples."""
    
    print("\n" + "="*80)
    print("🔥 PROMETHEUS + PYSPARK METRICS INTEGRATION")
    print("="*80)
    
    # Start Prometheus HTTP server
    prometheus_port = 8000
    logger.info(f"🌐 Starting Prometheus metrics server on port {prometheus_port}...")
    start_http_server(prometheus_port)
    
    print(f"\n📊 Prometheus Metrics Available:")
    print(f"   URL: http://localhost:{prometheus_port}/metrics")
    print(f"   Visit this URL to see all metrics in Prometheus format\n")
    
    spark = create_spark_session_with_metrics()
    
    try:
        # Run examples
        example_1_basic_job_metrics(spark)
        example_2_query_performance(spark)
        example_3_continuous_monitoring(spark)
        
        # Final summary
        print("\n" + "="*80)
        print("✅ ALL EXAMPLES COMPLETED")
        print("="*80)
        
        print(f"\n📊 Metrics Summary:")
        print(f"   • Total jobs executed: Check spark_jobs_total metric")
        print(f"   • Job durations: Check spark_job_duration_seconds histogram")
        print(f"   • Query performance: Check spark_query_duration_seconds summary")
        print(f"   • DataFrame sizes: Check spark_dataframe_row_count gauge")
        
        print(f"\n🌐 Access Metrics:")
        print(f"   Prometheus: http://localhost:{prometheus_port}/metrics")
        print(f"   Spark UI:   http://localhost:4040")
        
        print(f"\n💡 Next Steps:")
        print(f"   1. Open http://localhost:{prometheus_port}/metrics in browser")
        print(f"   2. Configure Prometheus to scrape this endpoint")
        print(f"   3. Create Grafana dashboards for visualization")
        print(f"   4. Set up alerts based on metric thresholds")
        
        # Keep server running for a bit
        print(f"\n⏳ Keeping metrics server alive for 30 seconds...")
        print(f"   (Press Ctrl+C to exit)")
        time.sleep(30)
        
    except KeyboardInterrupt:
        logger.info("\n�� Interrupted by user")
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
    
    finally:
        logger.info("🛑 Stopping Spark session...")
        spark.stop()
        logger.info("✅ Done!")


if __name__ == "__main__":
    main()
