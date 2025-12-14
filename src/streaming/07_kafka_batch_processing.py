"""
Spark Streaming - Kafka Batch Processing
========================================

Demonstrates using Kafka as a batch data source.
Kafka is primarily for streaming, but can also be queried in batch mode.

Real-world use cases:
- Historical data reprocessing
- Backfilling analytics
- Data lake migration
- FBI CJIS: Audit log analysis and compliance reporting

Key concepts:
- Batch vs streaming reads
- Offset range specification
- Partition-aware processing
- Performance optimization
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, expr,
    window, count, avg, min, max,
    date_format, hour, dayofweek
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType, DoubleType
)
import json


def demo_batch_read_basic(spark):
    """
    Basic batch read from Kafka.
    
    Unlike streaming, batch reads:
    - Read a fixed range of data
    - Return a regular DataFrame (not streaming)
    - Can be cached, counted, aggregated freely
    - No trigger or checkpoint needed
    """
    print("\n" + "=" * 70)
    print("1. BASIC BATCH READ FROM KAFKA")
    print("=" * 70)
    
    print("\n📖 Reading Kafka topic in batch mode...")
    
    # Read entire topic as batch
    batch_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "audit-logs") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    print("\n📊 Kafka batch DataFrame schema:")
    batch_df.printSchema()
    
    print("\n💡 Key Differences from Streaming:")
    print("   - Returns regular DataFrame (not streaming)")
    print("   - Can use .count(), .show(), .collect()")
    print("   - No checkpoint or trigger needed")
    print("   - Reads fixed range of offsets")
    
    print("\n📈 Dataset statistics:")
    print(f"   Total messages: {batch_df.count()}")
    print(f"   Partitions: {batch_df.select('partition').distinct().count()}")
    print(f"   Topics: {batch_df.select('topic').distinct().count()}")
    
    return batch_df


def demo_offset_ranges(spark):
    """
    Specify exact offset ranges for processing.
    
    Use cases:
    - Reprocess specific time window
    - Fix data quality issues
    - Backfill analytics
    """
    print("\n" + "=" * 70)
    print("2. OFFSET RANGE SPECIFICATION")
    print("=" * 70)
    
    print("\n🎯 Reading specific offset ranges...")
    
    # Option 1: Earliest to Latest (all data)
    print("\n📝 Option 1: All data (earliest to latest)")
    print("""
    df = spark.read.format("kafka") \\
        .option("startingOffsets", "earliest") \\
        .option("endingOffsets", "latest") \\
        .load()
    """)
    
    # Option 2: Specific offsets per partition
    print("\n📝 Option 2: Specific offsets per partition")
    offset_json = {
        "audit-logs": {
            "0": 100,   # Partition 0: start at offset 100
            "1": 250,   # Partition 1: start at offset 250
            "2": 50     # Partition 2: start at offset 50
        }
    }
    print(f"   startingOffsets JSON: {json.dumps(offset_json, indent=2)}")
    print("""
    df = spark.read.format("kafka") \\
        .option("startingOffsets", json.dumps(offset_json)) \\
        .option("endingOffsets", "latest") \\
        .load()
    """)
    
    # Option 3: Timestamp-based offsets
    print("\n📝 Option 3: Timestamp-based (Kafka 0.10.1+)")
    print("   Note: Requires querying Kafka directly for timestamp->offset mapping")
    print("   Use kafka-consumer-groups.sh or KafkaConsumer API")
    
    print("\n💡 Common Patterns:")
    print("   • Last 24 hours: Query timestamp, convert to offsets")
    print("   • Specific date range: Calculate offset ranges")
    print("   • Reprocess failures: Track failed offsets, replay")


def demo_partition_aware_processing(spark):
    """
    Process Kafka partitions in parallel.
    
    Benefits:
    - Better parallelism
    - Partition-local processing
    - Efficient resource utilization
    """
    print("\n" + "=" * 70)
    print("3. PARTITION-AWARE PROCESSING")
    print("=" * 70)
    
    print("\n⚙️  Leveraging Kafka partitions for parallelism...")
    
    print("""
    # Read with partition information
    df = spark.read.format("kafka").load()
    
    # Process per partition
    result = df.groupBy("partition") \\
        .agg(
            count("*").alias("message_count"),
            min("offset").alias("min_offset"),
            max("offset").alias("max_offset")
        )
    
    result.show()
    """)
    
    print("\n📊 Partition Statistics Example:")
    print("   ┌─────────┬──────────────┬────────────┬────────────┐")
    print("   │partition│message_count │ min_offset │ max_offset │")
    print("   ├─────────┼──────────────┼────────────┼────────────┤")
    print("   │    0    │    10,523    │      0     │   10,522   │")
    print("   │    1    │    12,108    │      0     │   12,107   │")
    print("   │    2    │     9,845    │      0     │    9,844   │")
    print("   └─────────┴──────────────┴────────────┴────────────┘")
    
    print("\n💡 Optimization Tips:")
    print("   ✓ Spark tasks = Kafka partitions (1:1 mapping)")
    print("   ✓ Increase Kafka partitions for more parallelism")
    print("   ✓ Use coalesce() if partitions >> cores")
    print("   ✓ Repartition for downstream operations")


def demo_backfill_analytics(spark):
    """
    Backfill analytics from historical Kafka data.
    
    Scenario: Generate hourly aggregations for last 30 days
    """
    print("\n" + "=" * 70)
    print("4. BACKFILL ANALYTICS USE CASE")
    print("=" * 70)
    
    print("\n📊 Scenario: Backfill hourly incident statistics")
    print("   Time range: Last 30 days")
    print("   Aggregation: Incidents per hour by severity")
    
    # Define schema for incident data
    incident_schema = StructType([
        StructField("incident_id", StringType(), False),
        StructField("timestamp", TimestampType(), True),
        StructField("severity", IntegerType(), True),
        StructField("incident_type", StringType(), True),
        StructField("location", StringType(), True)
    ])
    
    print("\n📋 Incident schema:")
    print("   - incident_id: STRING")
    print("   - timestamp: TIMESTAMP")
    print("   - severity: INTEGER (1-5)")
    print("   - incident_type: STRING")
    print("   - location: STRING")
    
    print("\n🔧 Processing steps:")
    print("""
    # 1. Read Kafka in batch mode
    kafka_df = spark.read.format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("subscribe", "incidents") \\
        .option("startingOffsets", "earliest") \\
        .option("endingOffsets", "latest") \\
        .load()
    
    # 2. Parse JSON from value column
    incidents = kafka_df.select(
        from_json(col("value").cast("string"), incident_schema).alias("data")
    ).select("data.*")
    
    # 3. Aggregate by hour and severity
    hourly_stats = incidents \\
        .withColumn("hour", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00")) \\
        .groupBy("hour", "severity") \\
        .agg(
            count("*").alias("incident_count"),
            count("incident_type").alias("distinct_types")
        ) \\
        .orderBy("hour", "severity")
    
    # 4. Write to data lake (Parquet)
    hourly_stats.write \\
        .mode("overwrite") \\
        .partitionBy("hour") \\
        .parquet("/data/analytics/hourly_incidents")
    """)
    
    print("\n📈 Expected Output:")
    print("   ┌──────────────────────┬──────────┬───────────────┐")
    print("   │        hour          │ severity │incident_count │")
    print("   ├──────────────────────┼──────────┼───────────────┤")
    print("   │ 2024-12-01 00:00:00  │    1     │      45       │")
    print("   │ 2024-12-01 00:00:00  │    2     │      32       │")
    print("   │ 2024-12-01 00:00:00  │    3     │      18       │")
    print("   │ 2024-12-01 01:00:00  │    1     │      52       │")
    print("   └──────────────────────┴──────────┴───────────────┘")


def demo_data_quality_analysis(spark):
    """
    Analyze data quality in Kafka topics.
    
    Checks:
    - Null values
    - Duplicate records
    - Schema violations
    - Timestamp gaps
    """
    print("\n" + "=" * 70)
    print("5. DATA QUALITY ANALYSIS")
    print("=" * 70)
    
    print("\n🔍 Analyzing data quality in Kafka topic...")
    
    print("\n📝 Quality Checks:")
    print("""
    # Read all messages
    df = spark.read.format("kafka") \\
        .option("subscribe", "audit-logs") \\
        .load()
    
    # Parse JSON
    parsed = df.select(
        col("key").cast("string"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("key", "data.*", "kafka_timestamp")
    
    # Quality report
    quality_report = parsed.select(
        # 1. Null counts
        count(when(col("key").isNull(), 1)).alias("null_keys"),
        count(when(col("data").isNull(), 1)).alias("null_data"),
        
        # 2. Duplicates (based on key)
        (count("key") - countDistinct("key")).alias("duplicate_keys"),
        
        # 3. Timestamp analysis
        min("kafka_timestamp").alias("earliest_message"),
        max("kafka_timestamp").alias("latest_message"),
        
        # 4. Total records
        count("*").alias("total_records")
    )
    
    quality_report.show()
    """)
    
    print("\n📊 Sample Quality Report:")
    print("   ┌───────────┬───────────┬────────────────┬─────────────────┬─────────────────┬──────────────┐")
    print("   │ null_keys │ null_data │ duplicate_keys │ earliest_message│ latest_message  │total_records │")
    print("   ├───────────┼───────────┼────────────────┼─────────────────┼─────────────────┼──────────────┤")
    print("   │     0     │     12    │       45       │ 2024-12-01 ...  │ 2024-12-13 ...  │   125,890    │")
    print("   └───────────┴───────────┴────────────────┴─────────────────┴─────────────────┴──────────────┘")
    
    print("\n⚠️  Quality Issues Detected:")
    print("   • 12 messages with null data (0.01%)")
    print("   • 45 duplicate keys (0.04%)")
    print("   • Action: Filter nulls, deduplicate by key + timestamp")


def demo_migration_to_data_lake(spark):
    """
    Migrate Kafka data to data lake.
    
    Pattern: Kafka → Parquet/Delta Lake
    """
    print("\n" + "=" * 70)
    print("6. KAFKA TO DATA LAKE MIGRATION")
    print("=" * 70)
    
    print("\n🚀 Migration Strategy:")
    print("   Source: Kafka topic (streaming data)")
    print("   Target: Delta Lake (queryable storage)")
    print("   Frequency: Daily batch job")
    
    print("\n📝 Migration Code:")
    print("""
    # 1. Read yesterday's data from Kafka
    # (In production, track processed offsets)
    kafka_df = spark.read.format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("subscribe", "production-events") \\
        .option("startingOffsets", yesterday_start_offset) \\
        .option("endingOffsets", yesterday_end_offset) \\
        .load()
    
    # 2. Parse and transform
    events = kafka_df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), event_schema).alias("event")
    ).select("kafka_timestamp", "partition", "offset", "event.*")
    
    # 3. Add partitioning columns
    partitioned = events \\
        .withColumn("year", year(col("event_time"))) \\
        .withColumn("month", month(col("event_time"))) \\
        .withColumn("day", dayofmonth(col("event_time")))
    
    # 4. Write to Delta Lake
    partitioned.write \\
        .format("delta") \\
        .mode("append") \\
        .partitionBy("year", "month", "day") \\
        .save("/data/lake/events")
    
    # 5. Track processed offsets (for next run)
    processed_offsets = events.groupBy("partition") \\
        .agg(max("offset").alias("last_offset"))
    
    processed_offsets.write \\
        .mode("overwrite") \\
        .parquet("/data/checkpoints/kafka_offsets")
    """)
    
    print("\n✅ Benefits:")
    print("   ✓ Long-term retention (Kafka retains days/weeks)")
    print("   ✓ Efficient querying (Parquet/Delta)")
    print("   ✓ Schema evolution support")
    print("   ✓ Time travel (Delta Lake)")
    print("   ✓ Cost optimization (cold storage)")


def demo_performance_tuning(spark):
    """
    Performance tuning for batch Kafka reads.
    """
    print("\n" + "=" * 70)
    print("7. PERFORMANCE TUNING")
    print("=" * 70)
    
    print("\n⚡ Performance Optimization Tips:")
    
    print("\n1️⃣  Parallelism:")
    print("   • Spark partitions = Kafka partitions (default)")
    print("   • Increase Kafka partitions for more parallelism")
    print("   • Use spark.sql.shuffle.partitions for aggregations")
    print("""
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    """)
    
    print("\n2️⃣  Batch Size:")
    print("   • Control messages per batch")
    print("   • Balance between throughput and memory")
    print("""
    df = spark.read.format("kafka") \\
        .option("maxOffsetsPerTrigger", "1000000") \\  # For streaming
        .option("minPartitions", "10") \\              # For batch
        .load()
    """)
    
    print("\n3️⃣  Predicate Pushdown:")
    print("   • Filter early (after parsing)")
    print("   • Reduce data volume for downstream operations")
    print("""
    # Good: Filter early
    df.filter(col("severity") >= 3).groupBy(...).count()
    
    # Bad: Filter late
    df.groupBy(...).count().filter(col("count") > 100)
    """)
    
    print("\n4️⃣  Caching:")
    print("   • Cache if reading multiple times")
    print("   • Use appropriate storage level")
    print("""
    # Cache parsed DataFrame
    parsed_df.cache()
    
    # Run multiple queries
    parsed_df.filter(...).count()
    parsed_df.groupBy(...).agg(...)
    
    # Unpersist when done
    parsed_df.unpersist()
    """)
    
    print("\n5️⃣  Compression:")
    print("   • Enable compression for output")
    print("   • Reduce storage and I/O")
    print("""
    df.write \\
        .option("compression", "snappy") \\  # or "gzip", "lz4", "zstd"
        .parquet("/output/path")
    """)
    
    print("\n📊 Performance Benchmarks (1M messages):")
    print("   ┌─────────────────────┬──────────┬────────────┐")
    print("   │ Configuration       │ Time     │ Throughput │")
    print("   ├─────────────────────┼──────────┼────────────┤")
    print("   │ Default (4 cores)   │  45 sec  │  22K/sec   │")
    print("   │ Optimized (4 cores) │  18 sec  │  55K/sec   │")
    print("   │ Optimized (16 cores)│   7 sec  │ 140K/sec   │")
    print("   └─────────────────────┴──────────┴────────────┘")


def main():
    """
    Main demo function.
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("KafkaBatchProcessingDemo") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("╔" + "=" * 68 + "╗")
        print("║" + " " * 10 + "KAFKA BATCH PROCESSING DEMO" + " " * 31 + "║")
        print("╚" + "=" * 68 + "╝")
        
        # Demo 1: Basic batch read
        # demo_batch_read_basic(spark)  # Requires Kafka
        
        # Demo 2: Offset ranges
        demo_offset_ranges(spark)
        
        # Demo 3: Partition-aware processing
        demo_partition_aware_processing(spark)
        
        # Demo 4: Backfill analytics
        demo_backfill_analytics(spark)
        
        # Demo 5: Data quality analysis
        demo_data_quality_analysis(spark)
        
        # Demo 6: Migration to data lake
        demo_migration_to_data_lake(spark)
        
        # Demo 7: Performance tuning
        demo_performance_tuning(spark)
        
        print("\n" + "=" * 70)
        print("BATCH VS STREAMING COMPARISON")
        print("=" * 70)
        
        print("\n📊 When to Use Batch:")
        print("   ✓ Historical data reprocessing")
        print("   ✓ One-time analytics")
        print("   ✓ Data migration")
        print("   ✓ Compliance audits")
        print("   ✓ Backfilling metrics")
        
        print("\n📊 When to Use Streaming:")
        print("   ✓ Real-time dashboards")
        print("   ✓ Continuous ETL")
        print("   ✓ Alerting systems")
        print("   ✓ Event-driven processing")
        print("   ✓ Low-latency requirements")
        
        print("\n" + "=" * 70)
        print("COMMON USE CASES")
        print("=" * 70)
        
        print("\n🎯 Use Case 1: Audit Log Analysis")
        print("   Scenario: Analyze 90 days of audit logs")
        print("   Approach: Batch read, aggregate by day/user/action")
        print("   Output: Compliance report")
        
        print("\n🎯 Use Case 2: Incident Backfill")
        print("   Scenario: Generate missing hourly stats")
        print("   Approach: Read specific time range, aggregate, write")
        print("   Output: Updated analytics dashboard")
        
        print("\n🎯 Use Case 3: Data Lake Migration")
        print("   Scenario: Move Kafka data to S3/Delta Lake")
        print("   Approach: Daily batch job, partitioned writes")
        print("   Output: Queryable data lake")
        
        print("\n🎯 Use Case 4: Schema Validation")
        print("   Scenario: Check message schema compliance")
        print("   Approach: Read all messages, validate against schema")
        print("   Output: Quality report")
        
        print("\n" + "=" * 70)
        print("PRODUCTION BEST PRACTICES")
        print("=" * 70)
        
        print("\n✅ DO:")
        print("   ✓ Track processed offsets for incremental processing")
        print("   ✓ Use partitioned writes (year/month/day)")
        print("   ✓ Implement idempotent processing")
        print("   ✓ Monitor job duration and throughput")
        print("   ✓ Set appropriate batch sizes")
        print("   ✓ Cache intermediate results")
        print("   ✓ Use compression for output")
        
        print("\n❌ DON'T:")
        print("   ✗ Read entire topic repeatedly")
        print("   ✗ Skip offset tracking")
        print("   ✗ Use streaming when batch is sufficient")
        print("   ✗ Ignore partition skew")
        print("   ✗ Forget to clean up old data")
        
        print("\n�� EXAMPLE PRODUCTION JOB:")
        print("""
    # Daily batch job to process yesterday's Kafka data
    
    from datetime import datetime, timedelta
    
    # 1. Get offset range for yesterday
    yesterday = datetime.now() - timedelta(days=1)
    start_offset = get_offset_for_timestamp(yesterday.replace(hour=0))
    end_offset = get_offset_for_timestamp(yesterday.replace(hour=23))
    
    # 2. Read data
    df = spark.read.format("kafka") \\
        .option("subscribe", "events") \\
        .option("startingOffsets", json.dumps(start_offset)) \\
        .option("endingOffsets", json.dumps(end_offset)) \\
        .load()
    
    # 3. Process
    processed = df.select(...).filter(...).groupBy(...)
    
    # 4. Write
    processed.write \\
        .mode("append") \\
        .partitionBy("date") \\
        .parquet(f"/data/events/{yesterday.strftime('%Y-%m-%d')}")
    
    # 5. Update checkpoint
    save_checkpoint(end_offset)
        """)
        
        print("\n" + "=" * 70)
        print("✅ KAFKA BATCH PROCESSING DEMO COMPLETE")
        print("=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
