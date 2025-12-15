#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
WINDOWED AGGREGATIONS - Time-Based Analytics on Kafka Streams
================================================================================

📖 OVERVIEW:
Demonstrates time-window based aggregations on Kafka streaming data:
• Tumbling windows (non-overlapping)
• Sliding windows (overlapping)
• Session windows (activity-based)
• Watermarks for late data handling

🎯 USE CASES:
• Real-time dashboards (metrics per minute/hour)
• Alert systems (threshold detection in time windows)
• Trend analysis (comparing window to window)
• Session analytics (user activity sessions)

🚀 RUN:
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  03_windowed_aggregations.py
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as _sum, avg, max as _max, min as _min,
    current_timestamp, expr, session_window
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def create_spark_session():
    """Create SparkSession for windowed streaming."""
    print("=" * 80)
    print("🚀 WINDOWED AGGREGATIONS - SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("KafkaWindowedAggregations") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/windowed_checkpoint") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark {spark.version} ready for windowed streaming")
    print()
    
    return spark


def example_1_tumbling_windows(spark):
    """
    Example 1: Tumbling Windows - Non-overlapping time buckets.
    
    Use Case: Page views per 5-minute intervals
    """
    print("=" * 80)
    print("📊 EXAMPLE 1: TUMBLING WINDOWS")
    print("=" * 80)
    
    print("""
🕐 Tumbling Windows (Non-Overlapping):

Timeline:
├──── 10:00 ────┼──── 10:05 ────┼──── 10:10 ────┼──── 10:15 ────┤
│  Window 1     │  Window 2     │  Window 3     │  Window 4     │
│  (5 min)      │  (5 min)      │  (5 min)      │  (5 min)      │
└───────────────┴───────────────┴───────────────┴───────────────┘

Each event belongs to exactly ONE window.
Perfect for: Metrics dashboards, hourly/daily reports

Example: Count page views per 5-minute bucket
    """)
    
    # Define user event schema
    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("country", StringType(), True)
    ])
    
    try:
        # Read from Kafka
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON
        events = kafka_stream.select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("data.*")
        
        # Apply watermark and tumbling window
        windowed_counts = events \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),  # Tumbling 5-min windows
                col("country")
            ) \
            .agg(
                count("*").alias("event_count"),
                count(expr("CASE WHEN event_type = 'page_view' THEN 1 END")).alias("page_views"),
                count(expr("CASE WHEN event_type = 'button_click' THEN 1 END")).alias("clicks")
            )
        
        print("""
✅ Tumbling Window Configuration:
   • Window size: 5 minutes
   • Watermark: 10 minutes (handle late data up to 10 min)
   • Group by: country
   • Metrics: Total events, page views, clicks per window

📊 Output Schema:
   window: {start: timestamp, end: timestamp}
   country: string
   event_count: long
   page_views: long
   clicks: long

💡 Watermark Explanation:
   "10 minutes" = Keep state for events up to 10 minutes late
   
   Example timeline:
   Current time: 10:15
   Processing window: 10:05-10:10
   Accept events timestamped >= 10:05 (10:15 - 10min watermark)
   Drop events timestamped < 10:05 (too late)
        """)
        
        print("\n✅ Example 1 setup complete (ready to start query)")
        
    except Exception as e:
        print(f"⚠️  Note: Requires running Kafka. Error: {e}")


def example_2_sliding_windows(spark):
    """
    Example 2: Sliding Windows - Overlapping time buckets.
    
    Use Case: Moving average of sensor readings
    """
    print("\n" + "=" * 80)
    print("�� EXAMPLE 2: SLIDING WINDOWS")
    print("=" * 80)
    
    print("""
🕐 Sliding Windows (Overlapping):

Timeline:
├──────────────┤        Window 1 (10:00 - 10:10)
    ├──────────────┤    Window 2 (10:05 - 10:15)
        ├──────────────┤    Window 3 (10:10 - 10:20)
10:00   10:05   10:10   10:15   10:20

Window: 10 minutes, Slide: 5 minutes
Each event may belong to MULTIPLE windows (overlap).
Perfect for: Moving averages, trend detection

Example: Average temperature per 10-min window, sliding every 5 min
    """)
    
    # IoT sensor schema
    sensor_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True)
    ])
    
    try:
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "iot-sensors") \
            .option("startingOffsets", "latest") \
            .load()
        
        sensors = kafka_stream.select(
            from_json(col("value").cast("string"), sensor_schema).alias("data")
        ).select("data.*")
        
        # Sliding window: 10-min window, 5-min slide
        sliding_avg = sensors \
            .filter(col("sensor_type") == "temperature") \
            .withWatermark("timestamp", "15 minutes") \
            .groupBy(
                window(col("timestamp"), "10 minutes", "5 minutes"),  # Sliding window
                col("location")
            ) \
            .agg(
                avg("value").alias("avg_temperature"),
                _min("value").alias("min_temperature"),
                _max("value").alias("max_temperature"),
                count("*").alias("reading_count")
            )
        
        print("""
✅ Sliding Window Configuration:
   • Window duration: 10 minutes
   • Slide duration: 5 minutes
   • Watermark: 15 minutes
   • Metrics: avg, min, max temperature per location

📊 Example Output (Temperature readings):
   
   Window: [2024-12-15 10:00:00, 2024-12-15 10:10:00]
   Location: warehouse_a
   Avg Temperature: 22.5°C, Min: 20.1°C, Max: 24.3°C
   
   Window: [2024-12-15 10:05:00, 2024-12-15 10:15:00]
   Location: warehouse_a
   Avg Temperature: 23.1°C, Min: 21.2°C, Max: 25.0°C

💡 When to Use Sliding Windows:
   ✓ Smoothing noisy data (moving average)
   ✓ Detecting gradual trends
   ✓ Real-time monitoring with overlap for context
   ✓ Alert systems (threshold breaches in rolling window)
        """)
        
        print("\n✅ Example 2 setup complete")
        
    except Exception as e:
        print(f"⚠️  Note: Requires running Kafka. Error: {e}")


def example_3_session_windows(spark):
    """
    Example 3: Session Windows - Activity-based windows.
    
    Use Case: User session analytics (gap-based grouping)
    """
    print("\n" + "=" * 80)
    print("👤 EXAMPLE 3: SESSION WINDOWS")
    print("=" * 80)
    
    print("""
🕐 Session Windows (Gap-Based):

User Activity Timeline:
Event1  Event2  Event3  ─── GAP (>5 min) ───  Event4  Event5
├───┼───┼───┤                                  ├───┼───┤
│ Session 1  │                                 │Session 2│
└────────────┘                                 └─────────┘

Gap threshold: 5 minutes of inactivity = end session
Perfect for: User behavior analysis, session metrics

Example: Session duration and event count per user
    """)
    
    event_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("page_url", StringType(), True)
    ])
    
    try:
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "user-events") \
            .option("startingOffsets", "latest") \
            .load()
        
        events = kafka_stream.select(
            from_json(col("value").cast("string"), event_schema).alias("data")
        ).select("data.*")
        
        # Session window: 5-minute gap threshold
        user_sessions = events \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                col("user_id"),
                session_window(col("timestamp"), "5 minutes")  # Session window
            ) \
            .agg(
                count("*").alias("events_in_session"),
                _min("timestamp").alias("session_start"),
                _max("timestamp").alias("session_end"),
                expr("(unix_timestamp(max(timestamp)) - unix_timestamp(min(timestamp))) / 60").alias("session_duration_minutes"),
                count(expr("CASE WHEN event_type = 'purchase' THEN 1 END")).alias("conversions")
            )
        
        print("""
✅ Session Window Configuration:
   • Gap threshold: 5 minutes
   • Group by: user_id
   • Watermark: 10 minutes
   • Metrics: event count, session duration, conversions

📊 Example Output (User Sessions):
   
   User: user_1234
   Session: [2024-12-15 10:00:00, 2024-12-15 10:12:00]
   Duration: 12 minutes
   Events: 8 (7 page views, 1 purchase)
   Conversions: 1

💡 Use Cases for Session Windows:
   ✓ User engagement metrics (session length, events per session)
   ✓ Conversion funnels (events leading to purchase)
   ✓ Bounce rate (single-event sessions)
   ✓ Session timeout detection (inactive users)
   ✓ Web analytics (time on site, pages per session)
        """)
        
        print("\n✅ Example 3 setup complete")
        
    except Exception as e:
        print(f"⚠️  Note: Requires running Kafka. Error: {e}")


def example_4_advanced_patterns(spark):
    """
    Example 4: Advanced patterns - Combining windows with other operations.
    """
    print("\n" + "=" * 80)
    print("🎯 EXAMPLE 4: ADVANCED WINDOWING PATTERNS")
    print("=" * 80)
    
    print("""
📚 Advanced Windowing Techniques:

1. Multiple Aggregations in Same Window:
   └─ Group by window + multiple dimensions
   └─ Compute various metrics simultaneously

2. Window-to-Window Comparison:
   └─ Compare current window to previous
   └─ Detect anomalies or trends

3. Nested Windows:
   └─ Minute windows aggregated into hour windows
   └─ Multi-level time hierarchies

4. Conditional Aggregations:
   └─ Different logic per event type
   └─ Filter within aggregation

Example: E-commerce metrics per 10-minute window
    """)
    
    purchase_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    try:
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "payment-events") \
            .option("startingOffsets", "latest") \
            .load()
        
        purchases = kafka_stream.select(
            from_json(col("value").cast("string"), purchase_schema).alias("data")
        ).select("data.*")
        
        # Complex aggregations
        ecommerce_metrics = purchases \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
                window(col("timestamp"), "10 minutes"),
                col("category")
            ) \
            .agg(
                # Revenue metrics
                _sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                _max("total_amount").alias("largest_order"),
                
                # Volume metrics
                count("*").alias("order_count"),
                expr("count(distinct user_id)").alias("unique_customers"),
                
                # Payment method breakdown
                count(expr("CASE WHEN payment_method = 'credit_card' THEN 1 END")).alias("cc_orders"),
                count(expr("CASE WHEN payment_method = 'paypal' THEN 1 END")).alias("paypal_orders"),
                
                # Performance indicators
                expr("sum(total_amount) / count(*)").alias("revenue_per_order"),
                expr("sum(total_amount) / count(distinct user_id)").alias("revenue_per_customer")
            )
        
        print("""
✅ Advanced Metrics Computed:
   Revenue: total, average, largest order
   Volume: orders, unique customers
   Payment: credit card vs PayPal orders
   KPIs: revenue per order, revenue per customer

📊 Sample Dashboard Output:

   Window: 10:00 - 10:10 | Category: Electronics
   ─────────────────────────────────────────────
   Total Revenue:        $12,450.00
   Orders:               45
   Unique Customers:     38
   Avg Order Value:      $276.67
   Largest Order:        $999.99
   Revenue/Customer:     $327.63
   Payment Mix:          30 CC, 15 PayPal

💡 Best Practices:
   ✓ Combine multiple aggregations in one query (efficient)
   ✓ Use CASE WHEN for conditional counting
   ✓ Calculate ratios with expressions
   ✓ Group by window + dimensions for drill-down
   ✓ Monitor state size (watermark is critical!)
        """)
        
        print("\n✅ Example 4 setup complete")
        
    except Exception as e:
        print(f"⚠️  Note: Requires running Kafka. Error: {e}")


def main():
    """Main execution function."""
    print("\n" + "🔥 " * 40)
    print("WINDOWED AGGREGATIONS ON KAFKA STREAMS")
    print("🔥 " * 40)
    print()
    
    spark = create_spark_session()
    
    example_1_tumbling_windows(spark)
    example_2_sliding_windows(spark)
    example_3_session_windows(spark)
    example_4_advanced_patterns(spark)
    
    print("\n" + "=" * 80)
    print("✅ ALL WINDOWING EXAMPLES COMPLETE")
    print("=" * 80)
    
    print("""
📚 Summary - Windowing Strategies:

1. Tumbling Windows:
   ✓ Non-overlapping time buckets
   ✓ Use: Hourly/daily reports, simple dashboards
   ✓ Example: .groupBy(window(col("timestamp"), "1 hour"))

2. Sliding Windows:
   ✓ Overlapping time buckets
   ✓ Use: Moving averages, trend detection
   ✓ Example: .groupBy(window(col("timestamp"), "10 minutes", "5 minutes"))

3. Session Windows:
   ✓ Activity-based (gap threshold)
   ✓ Use: User sessions, behavior analysis
   ✓ Example: .groupBy(session_window(col("timestamp"), "5 minutes"))

4. Watermarks (CRITICAL):
   ✓ Define "how late is too late"
   ✓ Enable state cleanup (prevent memory issues)
   ✓ Example: .withWatermark("timestamp", "10 minutes")

🎯 Choosing the Right Window:

Question: "How many events per hour?"
→ Tumbling window (1 hour)

Question: "What's the moving average over last 30 minutes?"
→ Sliding window (30 minutes, 5-minute slide)

Question: "How long do users stay active?"
→ Session window (5-10 minute gap)

⚠️  Common Pitfalls:
   ✗ Forgetting watermark = unbounded state growth
   ✗ Watermark too short = valid late data dropped
   ✗ Watermark too long = excessive memory usage
   ✗ Window too small = too many small windows
   ✗ Window too large = delayed insights

�� Related Files:
   • 01_basic_kafka_consumer.py - Reading from Kafka
   • 04_stream_joins.py - Joining windowed streams
   • ../streaming/03_kafka_streaming.py - Complete guide
    """)
    
    spark.stop()


if __name__ == "__main__":
    main()
