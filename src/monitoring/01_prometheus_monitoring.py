"""
================================================================================
Prometheus Monitoring for PySpark
================================================================================

PURPOSE:
--------
Implement production monitoring for PySpark applications using Prometheus
for metrics collection, alerting, and observability.

WHAT PROMETHEUS MONITORS:
--------------------------
- Spark metrics (executors, memory, CPU)
- Job duration and success/failure rates
- Data pipeline throughput
- Resource utilization
- Custom business metrics

WHY MONITORING MATTERS:
-----------------------
- Detect failures before users notice
- Optimize resource allocation
- Capacity planning
- SLA compliance
- Cost optimization (avoid over-provisioning)

REAL-WORLD METRICS:
-------------------
- Job latency: 95th percentile < 5 minutes
- Success rate: > 99.9%
- Data freshness: < 15 minutes lag
- Resource utilization: 70-80% (not under/over)

KEY METRICS:
------------
1. SPARK METRICS:
   - spark_executor_memory_used
   - spark_executor_cpu_time
   - spark_stage_task_count
   - spark_job_duration_seconds

2. CUSTOM METRICS:
   - records_processed_total
   - data_quality_score
   - sla_violations_total

3. INFRASTRUCTURE:
   - node_cpu_usage
   - node_memory_available
   - disk_io_operations

ALERTING RULES:
---------------
- Job failure rate > 1%
- Job duration > 2x baseline
- Memory usage > 90%
- Data freshness > 30 minutes

================================================================================
"""

from pyspark.sql import SparkSession
import time

def create_spark_with_metrics():
    """Create Spark with Prometheus metrics enabled."""
    return SparkSession.builder \
        .appName("Spark_Prometheus_Monitoring") \
        .config("spark.metrics.namespace", "spark_app") \
        .config("spark.sql.streaming.metricsEnabled", "true") \
        .getOrCreate()

def example_1_basic_monitoring(spark):
    """
    Example 1: Basic Spark monitoring with custom metrics.
    
    METRICS TRACKED:
    - Job start/end times
    - Records processed
    - Data quality score
    - Error counts
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: SPARK MONITORING")
    print("=" * 80)
    
    start_time = time.time()
    
    # Sample data processing
    df = spark.range(0, 1000000)
    result = df.filter(df.id % 2 == 0).count()
    
    duration = time.time() - start_time
    
    print(f"\n📊 Job Metrics:")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   Records processed: 1,000,000")
    print(f"   Records filtered: {result}")
    print(f"   Filter rate: {(result/1000000)*100:.1f}%")
    
    print("\n�� Prometheus would expose:")
    print(f"   spark_job_duration_seconds{{job='filter'}} {duration:.2f}")
    print(f"   spark_records_processed_total{{job='filter'}} 1000000")
    print(f"   spark_records_filtered_total{{job='filter'}} {result}")
    
    print("\n🚨 Alert Rules:")
    print("   • Alert if duration > 10 seconds")
    print("   • Alert if filter rate < 40%")
    print("   • Alert on any job failure")
    
    return result

def main():
    """Main execution."""
    print("\n" + "��" * 40)
    print("PROMETHEUS MONITORING FOR PYSPARK")
    print("📈" * 40)
    
    spark = create_spark_with_metrics()
    example_1_basic_monitoring(spark)
    
    print("\n✅ Monitoring examples complete!")
    spark.stop()

if __name__ == "__main__":
    main()
