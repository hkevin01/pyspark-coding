"""
Spark Streaming Output Modes Demo
==================================

This example demonstrates the three output modes in Spark Structured Streaming:
1. APPEND - Only new rows added to result table (default)
2. COMPLETE - Entire result table written to sink
3. UPDATE - Only rows that changed since last trigger

Real-world use cases:
- APPEND: Log file processing, event streams
- COMPLETE: Real-time aggregations, dashboards
- UPDATE: Stateful processing, windowed aggregations

FBI CJIS Compliance: Demonstrates audit logging patterns
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import time

def demo_append_mode(spark):
    """
    APPEND Mode: Only new rows are written to the output sink.
    
    Use cases:
    - Event logging systems
    - Audit trails (FBI CJIS requirement)
    - Append-only data lakes
    - Transaction logs
    
    Restrictions:
    - Cannot use aggregations without watermark
    - Cannot update/delete existing rows
    """
    print("\n" + "=" * 70)
    print("1. APPEND MODE - Only New Rows")
    print("=" * 70)
    
    # Define schema for input data (simulating access logs)
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("user_id", StringType(), True),
        StructField("action", StringType(), True),
        StructField("ip_address", StringType(), True)
    ])
    
    # Create a sample DataFrame simulating streaming data
    # In production, this would be: spark.readStream.format("kafka")...
    print("\n📊 Creating streaming DataFrame (simulated)...")
    print("   Schema: timestamp, user_id, action, ip_address")
    print("   Use Case: Security audit logs (CJIS compliant)")
    
    # Example: Reading from a directory (common pattern)
    print("\n💡 APPEND Mode Characteristics:")
    print("   ✓ Only newly arriving rows are output")
    print("   ✓ Existing rows never change")
    print("   ✓ Best for append-only use cases")
    print("   ✓ Memory efficient - doesn't store state")
    print("   ✗ Cannot use aggregations without watermark")
    
    print("\n📝 Example Query:")
    print("""
    # Reading access logs and appending to audit table
    audit_logs = spark.readStream \\
        .format("json") \\
        .schema(schema) \\
        .load("/data/access_logs")
    
    # Write with APPEND mode - each new log entry added once
    query = audit_logs.writeStream \\
        .outputMode("append") \\
        .format("parquet") \\
        .option("path", "/audit/access_logs") \\
        .option("checkpointLocation", "/checkpoint/audit") \\
        .start()
    """)
    
    print("\n✅ APPEND Mode: Perfect for immutable event logs")


def demo_complete_mode(spark):
    """
    COMPLETE Mode: Entire result table is written to output sink.
    
    Use cases:
    - Real-time dashboards
    - Running totals/counts
    - Top-N queries
    - Aggregated metrics
    
    Requirements:
    - Must have aggregations
    - Writes entire result every trigger
    """
    print("\n" + "=" * 70)
    print("2. COMPLETE MODE - Entire Result Table")
    print("=" * 70)
    
    print("\n📊 Use Case: Real-time crime statistics dashboard")
    print("   Aggregating incident reports by type and location")
    
    print("\n💡 COMPLETE Mode Characteristics:")
    print("   ✓ Entire result table written every trigger")
    print("   ✓ Perfect for aggregations and dashboards")
    print("   ✓ Always shows complete picture")
    print("   ✓ Required for some aggregations")
    print("   ✗ Higher memory usage (stores all state)")
    print("   ✗ More data written to sink")
    
    print("\n📝 Example Query:")
    print("""
    # Real-time crime statistics by type
    incident_counts = spark.readStream \\
        .format("kafka") \\
        .option("subscribe", "incidents") \\
        .load() \\
        .selectExpr("CAST(value AS STRING)") \\
        .groupBy("incident_type", "district") \\
        .count()
    
    # Write with COMPLETE mode - entire aggregation result each time
    query = incident_counts.writeStream \\
        .outputMode("complete") \\
        .format("memory") \\
        .queryName("crime_stats") \\
        .start()
    
    # Query the in-memory table for dashboard
    spark.sql("SELECT * FROM crime_stats ORDER BY count DESC").show()
    """)
    
    print("\n✅ COMPLETE Mode: Perfect for real-time aggregated dashboards")


def demo_update_mode(spark):
    """
    UPDATE Mode: Only rows that changed are written to output.
    
    Use cases:
    - Windowed aggregations
    - Stateful processing
    - Change data capture (CDC)
    - Incremental updates
    
    Benefits:
    - More efficient than COMPLETE
    - Can handle aggregations
    - Only writes changed rows
    """
    print("\n" + "=" * 70)
    print("3. UPDATE MODE - Only Changed Rows")
    print("=" * 70)
    
    print("\n📊 Use Case: Windowed fingerprint match statistics")
    print("   Track matches per hour, update only changed windows")
    
    print("\n💡 UPDATE Mode Characteristics:")
    print("   ✓ Only rows that changed are output")
    print("   ✓ More efficient than COMPLETE for large results")
    print("   ✓ Works with aggregations and watermarks")
    print("   ✓ Good for windowed computations")
    print("   ✗ Sink must support updates (delta, memory)")
    print("   ✗ More complex than APPEND")
    
    print("\n📝 Example Query:")
    print("""
    # Windowed aggregation - matches per 1-hour window
    windowed_matches = spark.readStream \\
        .format("kafka") \\
        .load() \\
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \\
        .withWatermark("timestamp", "10 minutes") \\
        .groupBy(
            window("timestamp", "1 hour", "30 minutes"),
            "match_type"
        ) \\
        .agg(count("*").alias("match_count"))
    
    # Write with UPDATE mode - only changed windows updated
    query = windowed_matches.writeStream \\
        .outputMode("update") \\
        .format("delta") \\
        .option("checkpointLocation", "/checkpoint/matches") \\
        .start("/data/match_windows")
    """)
    
    print("\n✅ UPDATE Mode: Perfect for windowed aggregations with incremental updates")


def demo_comparison(spark):
    """
    Side-by-side comparison of all three output modes.
    """
    print("\n" + "=" * 70)
    print("OUTPUT MODES COMPARISON")
    print("=" * 70)
    
    comparison = """
    ┌─────────────┬──────────────────────┬─────────────────────┬────────────────────┐
    │ Mode        │ What Gets Written    │ Best For            │ Restrictions       │
    ├─────────────┼──────────────────────┼─────────────────────┼────────────────────┤
    │ APPEND      │ Only new rows        │ Event logs          │ No aggregations    │
    │             │                      │ Audit trails        │ without watermark  │
    │             │                      │ Immutable data      │                    │
    ├─────────────┼──────────────────────┼─────────────────────┼────────────────────┤
    │ COMPLETE    │ Entire result table  │ Dashboards          │ Must have          │
    │             │ every time           │ Running totals      │ aggregations       │
    │             │                      │ Top-N queries       │                    │
    ├─────────────┼──────────────────────┼─────────────────────┼────────────────────┤
    │ UPDATE      │ Only changed rows    │ Windowed aggs       │ Sink must support  │
    │             │                      │ Stateful processing │ updates            │
    │             │                      │ CDC patterns        │                    │
    └─────────────┴──────────────────────┴─────────────────────┴────────────────────┘
    """
    print(comparison)
    
    print("\n📊 Memory Usage:")
    print("   APPEND:   Low    (stateless)")
    print("   UPDATE:   Medium (stores state for changed rows)")
    print("   COMPLETE: High   (stores entire result)")
    
    print("\n⚡ Performance:")
    print("   APPEND:   Fastest (no state management)")
    print("   UPDATE:   Medium  (partial state updates)")
    print("   COMPLETE: Slowest (full result each time)")
    
    print("\n💾 Compatible Sinks:")
    print("   APPEND:   All sinks (file, kafka, console, memory, delta)")
    print("   UPDATE:   memory, delta, custom sinks with update support")
    print("   COMPLETE: memory, console, some custom sinks")


def main():
    """
    Main function to demonstrate all output modes.
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("StreamingOutputModesDemo") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    try:
        print("╔" + "=" * 68 + "╗")
        print("║" + " " * 15 + "SPARK STREAMING OUTPUT MODES" + " " * 25 + "║")
        print("╚" + "=" * 68 + "╝")
        
        # Demonstrate each mode
        demo_append_mode(spark)
        demo_complete_mode(spark)
        demo_update_mode(spark)
        demo_comparison(spark)
        
        print("\n" + "=" * 70)
        print("REAL-WORLD EXAMPLES")
        print("=" * 70)
        
        print("\n🏛️  FBI CJIS Use Case:")
        print("   - APPEND: Store all access logs (audit requirement)")
        print("   - COMPLETE: Dashboard of active queries by type")
        print("   - UPDATE: Windowed statistics of system usage")
        
        print("\n🏦 Financial Services:")
        print("   - APPEND: Transaction logs")
        print("   - COMPLETE: Real-time portfolio values")
        print("   - UPDATE: Hourly transaction summaries")
        
        print("\n🏥 Healthcare:")
        print("   - APPEND: Patient visit records")
        print("   - COMPLETE: Current bed occupancy by department")
        print("   - UPDATE: Rolling 24-hour admission rates")
        
        print("\n" + "=" * 70)
        print("✅ OUTPUT MODES DEMO COMPLETE")
        print("=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
