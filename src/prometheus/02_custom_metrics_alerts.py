#!/usr/bin/env python3
"""
================================================================================
EXAMPLE 2: CUSTOM METRICS AND ALERT THRESHOLDS
================================================================================

PURPOSE:
    Create custom business metrics and implement alert thresholds

WHAT YOU'LL LEARN:
    • Define custom business metrics
    • Set alert thresholds
    • Monitor data quality metrics
    • Track SLA compliance
    • Export custom application-specific metrics

BUSINESS METRICS:
    • Revenue tracking
    • Error rates
    • Data quality scores
    • Processing latency
    • SLA violations

TIME: 20 minutes | DIFFICULTY: ⭐⭐⭐⭐☆
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, current_timestamp, lit
from prometheus_client import (
    Counter, Gauge, Histogram, Info, Enum,
    start_http_server, generate_latest, REGISTRY
)
import time
import random
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ════════════════════════════════════════════════════════════════════════════════
# CUSTOM BUSINESS METRICS
# ════════════════════════════════════════════════════════════════════════════════

# Revenue metrics
revenue_total = Counter(
    'business_revenue_dollars_total',
    'Total revenue processed',
    ['product_category', 'region']
)

revenue_per_minute = Gauge(
    'business_revenue_per_minute_dollars',
    'Revenue per minute'
)

# Data quality metrics
data_quality_score = Gauge(
    'data_quality_score_percentage',
    'Overall data quality score (0-100)',
    ['dataset']
)

null_record_ratio = Gauge(
    'data_null_record_ratio',
    'Ratio of records with null values',
    ['table']
)

duplicate_records = Counter(
    'data_duplicate_records_total',
    'Total duplicate records detected',
    ['table']
)

# SLA metrics
sla_compliance_rate = Gauge(
    'sla_compliance_rate_percentage',
    'SLA compliance rate',
    ['service']
)

sla_violations = Counter(
    'sla_violations_total',
    'Total SLA violations',
    ['service', 'severity']
)

processing_latency = Histogram(
    'processing_latency_seconds',
    'End-to-end processing latency',
    ['pipeline'],
    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60)
)

# Error tracking
error_rate = Gauge(
    'application_error_rate_per_minute',
    'Errors per minute'
)

errors_by_type = Counter(
    'application_errors_total',
    'Total errors by type',
    ['error_type', 'severity']
)

# System health
pipeline_health_status = Enum(
    'pipeline_health_status',
    'Current health status of the pipeline',
    states=['healthy', 'degraded', 'critical', 'down']
)

active_users = Gauge(
    'active_users_count',
    'Number of active users'
)

# Application info
app_info = Info(
    'application_info',
    'Application version and environment info'
)


# ════════════════════════════════════════════════════════════════════════════════
# ALERT THRESHOLDS
# ════════════════════════════════════════════════════════════════════════════════

class AlertThresholds:
    """Define alert thresholds for monitoring."""
    
    # Data quality
    MIN_DATA_QUALITY_SCORE = 85.0  # Alert if below 85%
    MAX_NULL_RATIO = 0.05          # Alert if > 5% nulls
    
    # SLA
    MIN_SLA_COMPLIANCE = 99.0      # Alert if below 99%
    MAX_LATENCY_SECONDS = 5.0      # Alert if > 5 seconds
    
    # Error rates
    MAX_ERROR_RATE_PER_MIN = 10    # Alert if > 10 errors/min
    
    # Business
    MIN_REVENUE_PER_MIN = 1000.0   # Alert if < $1000/min


# ════════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ════════════════════════════════════════════════════════════════════════════════

def create_spark_session():
    """Create SparkSession."""
    spark = SparkSession.builder \
        .appName("CustomMetrics_Example") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def check_alert(metric_name, value, threshold, comparison="<"):
    """Check if metric violates threshold and log alert."""
    violated = False
    
    if comparison == "<" and value < threshold:
        violated = True
    elif comparison == ">" and value > threshold:
        violated = True
    
    if violated:
        logger.warning(f"🚨 ALERT: {metric_name} = {value:.2f} (threshold: {comparison}{threshold})")
        return True
    else:
        logger.info(f"✅ OK: {metric_name} = {value:.2f}")
        return False


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 1: DATA QUALITY MONITORING
# ════════════════════════════════════════════════════════════════════════════════

def example_1_data_quality_monitoring(spark):
    """
    Monitor data quality metrics and detect issues.
    """
    print("\n" + "="*80)
    print("EXAMPLE 1: Data Quality Monitoring")
    print("="*80)
    
    # Create dataset with quality issues
    logger.info("📊 Creating test dataset with quality issues...")
    
    df = spark.range(0, 10000).toDF("id")
    df = df.withColumn("name", when(col("id") % 100 == 0, None).otherwise(col("id").cast("string")))
    df = df.withColumn("email", when(col("id") % 50 == 0, None).otherwise(lit("user@example.com")))
    df = df.withColumn("amount", when(col("id") % 75 == 0, None).otherwise((rand() * 1000).cast("double")))
    
    total_records = df.count()
    
    # Check for null values
    null_counts = {
        'name': df.filter(col("name").isNull()).count(),
        'email': df.filter(col("email").isNull()).count(),
        'amount': df.filter(col("amount").isNull()).count()
    }
    
    print(f"\n📋 Dataset Statistics:")
    print(f"   Total records: {total_records:,}")
    print(f"   Null values:")
    for field, count in null_counts.items():
        ratio = count / total_records
        print(f"     • {field}: {count:,} ({ratio*100:.2f}%)")
        
        # Update metrics
        null_record_ratio.labels(table=f"users_{field}").set(ratio)
        
        # Check alert
        check_alert(f"null_ratio_{field}", ratio, AlertThresholds.MAX_NULL_RATIO, ">")
    
    # Check for duplicates
    duplicate_count = total_records - df.dropDuplicates(["id"]).count()
    print(f"\n   Duplicate records: {duplicate_count}")
    duplicate_records.labels(table="users").inc(duplicate_count)
    
    # Calculate overall quality score
    null_penalty = sum(null_counts.values()) / (total_records * len(null_counts)) * 100
    duplicate_penalty = (duplicate_count / total_records) * 100
    quality_score = 100 - null_penalty - duplicate_penalty
    
    data_quality_score.labels(dataset="users").set(quality_score)
    
    print(f"\n📊 Data Quality Score: {quality_score:.2f}%")
    
    # Check alert
    alert_triggered = check_alert(
        "data_quality_score", 
        quality_score, 
        AlertThresholds.MIN_DATA_QUALITY_SCORE,
        "<"
    )
    
    if alert_triggered:
        pipeline_health_status.state('degraded')
    else:
        pipeline_health_status.state('healthy')
    
    print(f"   Status: {pipeline_health_status._value.get()}")


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 2: SLA MONITORING
# ════════════════════════════════════════════════════════════════════════════════

def example_2_sla_monitoring(spark):
    """
    Track SLA compliance and latency metrics.
    """
    print("\n" + "="*80)
    print("EXAMPLE 2: SLA Monitoring")
    print("="*80)
    
    service_name = "etl_pipeline"
    
    # Simulate processing multiple batches
    successful_batches = 0
    total_batches = 20
    
    print(f"\n🔄 Processing {total_batches} batches...")
    
    for batch_num in range(1, total_batches + 1):
        # Simulate processing with variable latency
        latency = random.uniform(0.5, 8.0)
        
        with processing_latency.labels(pipeline=service_name).time():
            time.sleep(latency / 10)  # Simulate work
        
        # Check if SLA met (< 5 seconds)
        if latency <= AlertThresholds.MAX_LATENCY_SECONDS:
            successful_batches += 1
            status = "✓"
        else:
            status = "✗"
            sla_violations.labels(
                service=service_name,
                severity="high" if latency > 7 else "medium"
            ).inc()
            logger.warning(f"   Batch {batch_num}: {latency:.2f}s {status} SLA VIOLATION")
            continue
        
        if batch_num % 5 == 0:
            print(f"   Batch {batch_num}: {latency:.2f}s {status}")
    
    # Calculate SLA compliance
    compliance_rate = (successful_batches / total_batches) * 100
    sla_compliance_rate.labels(service=service_name).set(compliance_rate)
    
    print(f"\n📊 SLA Summary:")
    print(f"   Successful batches: {successful_batches}/{total_batches}")
    print(f"   Compliance rate: {compliance_rate:.2f}%")
    
    # Check alert
    check_alert(
        "sla_compliance_rate",
        compliance_rate,
        AlertThresholds.MIN_SLA_COMPLIANCE,
        "<"
    )


# ════════════════════════════════════════════════════════════════════════════════
# EXAMPLE 3: BUSINESS METRICS TRACKING
# ════════════════════════════════════════════════════════════════════════════════

def example_3_business_metrics(spark):
    """
    Track business KPIs and revenue metrics.
    """
    print("\n" + "="*80)
    print("EXAMPLE 3: Business Metrics Tracking")
    print("="*80)
    
    # Generate sales data
    logger.info("💰 Generating sales transactions...")
    
    categories = ['electronics', 'clothing', 'food', 'books']
    regions = ['north', 'south', 'east', 'west']
    
    df = spark.range(0, 5000).toDF("transaction_id")
    df = df.withColumn("category", lit(random.choice(categories)))
    df = df.withColumn("region", lit(random.choice(regions)))
    df = df.withColumn("amount", (rand() * 500 + 10).cast("double"))
    df = df.withColumn("timestamp", current_timestamp())
    
    # Calculate revenue by category and region
    revenue_df = df.groupBy("category", "region").sum("amount")
    
    print(f"\n�� Revenue by Category & Region:")
    total_revenue = 0
    
    for row in revenue_df.collect():
        revenue = row['sum(amount)']
        total_revenue += revenue
        
        # Update Prometheus metric
        revenue_total.labels(
            product_category=row['category'],
            region=row['region']
        ).inc(revenue)
        
        print(f"   {row['category']:12} | {row['region']:6} | ${revenue:,.2f}")
    
    print(f"\n   {'TOTAL':12} | {'ALL':6} | ${total_revenue:,.2f}")
    
    # Calculate revenue per minute (simulated)
    minutes_elapsed = 1.0
    rpm = total_revenue / minutes_elapsed
    revenue_per_minute.set(rpm)
    
    print(f"\n📈 Revenue per minute: ${rpm:,.2f}/min")
    
    # Check alert
    check_alert(
        "revenue_per_minute",
        rpm,
        AlertThresholds.MIN_REVENUE_PER_MIN,
        "<"
    )
    
    # Simulate some errors
    error_count = random.randint(0, 15)
    errors_by_type.labels(error_type="payment_failed", severity="medium").inc(error_count)
    error_rate.set(error_count)
    
    print(f"\n⚠️  Error rate: {error_count} errors/min")
    check_alert(
        "error_rate",
        error_count,
        AlertThresholds.MAX_ERROR_RATE_PER_MIN,
        ">"
    )
    
    # Update active users
    users = random.randint(100, 500)
    active_users.set(users)
    print(f"👥 Active users: {users}")


# ════════════════════════════════════════════════════════════════════════════════
# MAIN EXECUTION
# ════════════════════════════════════════════════════════════════════════════════

def main():
    """Run all custom metrics examples."""
    
    print("\n" + "="*80)
    print("🎯 CUSTOM METRICS & ALERT MONITORING")
    print("="*80)
    
    # Set application info
    app_info.info({
        'version': '2.0.0',
        'environment': 'production',
        'deployed_by': 'devops_team'
    })
    
    # Start Prometheus server
    port = 8001
    logger.info(f"🌐 Starting metrics server on port {port}...")
    start_http_server(port)
    
    print(f"\n📊 Metrics endpoint: http://localhost:{port}/metrics")
    print(f"\n🚨 Alert Thresholds:")
    print(f"   • Data quality: >= {AlertThresholds.MIN_DATA_QUALITY_SCORE}%")
    print(f"   • Null ratio: <= {AlertThresholds.MAX_NULL_RATIO*100}%")
    print(f"   • SLA compliance: >= {AlertThresholds.MIN_SLA_COMPLIANCE}%")
    print(f"   • Latency: <= {AlertThresholds.MAX_LATENCY_SECONDS}s")
    print(f"   • Error rate: <= {AlertThresholds.MAX_ERROR_RATE_PER_MIN}/min")
    print(f"   • Revenue: >= ${AlertThresholds.MIN_REVENUE_PER_MIN}/min")
    
    spark = create_spark_session()
    
    try:
        example_1_data_quality_monitoring(spark)
        example_2_sla_monitoring(spark)
        example_3_business_metrics(spark)
        
        # Summary
        print("\n" + "="*80)
        print("✅ ALL EXAMPLES COMPLETED")
        print("="*80)
        
        print(f"\n📊 Metrics Available:")
        print(f"   • Data quality scores")
        print(f"   • SLA compliance rates")
        print(f"   • Business revenue metrics")
        print(f"   • Error rates and types")
        print(f"   • Processing latency")
        
        print(f"\n💡 Prometheus Query Examples:")
        print(f"   # Data quality alert:")
        print(f"   data_quality_score_percentage < {AlertThresholds.MIN_DATA_QUALITY_SCORE}")
        print(f"\n   # SLA violations:")
        print(f"   rate(sla_violations_total[5m]) > 0")
        print(f"\n   # Revenue per minute:")
        print(f"   business_revenue_per_minute_dollars")
        
        print(f"\n⏳ Server running for 30 seconds...")
        time.sleep(30)
        
    except KeyboardInterrupt:
        logger.info("\n👋 Shutting down...")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
