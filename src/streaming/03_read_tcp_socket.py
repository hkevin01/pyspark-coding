"""
Spark Streaming - Read Data From TCP Socket
===========================================

Demonstrates reading streaming data from a TCP socket connection.
Perfect for testing and debugging streaming applications.

Real-world use cases:
- Real-time log ingestion from servers
- Network monitoring and packet analysis
- IoT sensor data streams
- FBI CJIS: Real-time dispatch system monitoring

Key concepts:
- Socket source configuration
- Word count example (classic streaming demo)
- Stateful streaming with aggregations
- Watermarking for late data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    explode, split, window, col, count, 
    current_timestamp, length, lower, trim
)
import subprocess
import time
import threading
import socket


def start_netcat_server(port=9999):
    """
    Start a netcat server to send test data.
    
    In production, the TCP source would be:
    - Log aggregation servers (Fluentd, Logstash)
    - Network monitoring tools
    - Custom data streaming applications
    
    Note: Requires 'nc' (netcat) to be installed on the system
    """
    print(f"\n🌐 Starting netcat server on port {port}")
    print(f"   Send data using: echo 'your text' | nc localhost {port}")
    print("   Or: nc localhost 9999 (interactive mode)")
    
    # Instructions for manual testing
    print("\n📝 Test data suggestions:")
    print("   - 'fingerprint match found for subject 12345'")
    print("   - 'background check completed for applicant 67890'")
    print("   - 'database query executed successfully'")


def demo_basic_socket_stream(spark, host="localhost", port=9999):
    """
    Basic example: Read text lines from TCP socket.
    
    The socket source:
    - Reads UTF-8 text lines
    - One record per line (delimited by newline)
    - No schema (just a single 'value' column)
    """
    print("\n" + "=" * 70)
    print("1. BASIC TCP SOCKET STREAMING")
    print("=" * 70)
    
    print(f"\n🔌 Connecting to TCP socket...")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print("   Format: Plain text (one line per record)")
    
    # Read from socket
    # Returns DataFrame with single column: 'value' (string)
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()
    
    print("\n📊 Schema:")
    lines_df.printSchema()
    
    print("\n💡 Key Points:")
    print("   - Single column named 'value' (StringType)")
    print("   - Each line from socket = one row")
    print("   - Connection must be established before streaming starts")
    print("   - No built-in retry or fault tolerance")
    
    return lines_df


def demo_word_count(spark, host="localhost", port=9999):
    """
    Classic streaming example: Word count on streaming data.
    
    Demonstrates:
    - Splitting text into words
    - Stateful aggregation
    - Complete output mode (entire result table)
    """
    print("\n" + "=" * 70)
    print("2. STREAMING WORD COUNT")
    print("=" * 70)
    
    print("\n📖 Reading and counting words from socket...")
    
    # Read lines from socket
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()
    
    # Split lines into words and count
    # explode() converts array of words into separate rows
    words_df = lines_df \
        .select(explode(split(col("value"), " ")).alias("word"))
    
    # Count occurrences of each word
    word_counts = words_df \
        .groupBy("word") \
        .count() \
        .orderBy(col("count").desc())
    
    print("\n⚙️  Transformations applied:")
    print("   1. Split each line on spaces")
    print("   2. Explode array into individual words")
    print("   3. Group by word and count")
    print("   4. Order by count (descending)")
    
    print("\n📊 Output schema:")
    word_counts.printSchema()
    
    return word_counts


def demo_windowed_word_count(spark, host="localhost", port=9999):
    """
    Advanced example: Word count with time windows.
    
    Real-world pattern:
    - Count events in 10-minute windows
    - Slide window every 5 minutes
    - Handle late-arriving data
    - FBI CJIS: Monitor keyword frequency in dispatch logs
    """
    print("\n" + "=" * 70)
    print("3. WINDOWED WORD COUNT")
    print("=" * 70)
    
    print("\n⏱️  Windowed aggregation configuration:")
    print("   Window size: 10 minutes")
    print("   Slide interval: 5 minutes")
    print("   Watermark: 10 minutes (for late data)")
    
    # Read lines with timestamp
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()
    
    # Add timestamp for windowing
    lines_with_timestamp = lines_df \
        .select(col("value"), current_timestamp().alias("timestamp"))
    
    # Split into words
    words_df = lines_with_timestamp \
        .select(
            col("timestamp"),
            explode(split(col("value"), " ")).alias("word")
        )
    
    # Windowed aggregation with watermark
    windowed_counts = words_df \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "10 minutes", "5 minutes"),
            col("word")
        ) \
        .count() \
        .orderBy(col("window").desc(), col("count").desc())
    
    print("\n📊 Output schema with windows:")
    windowed_counts.printSchema()
    
    print("\n💡 Window mechanics:")
    print("   - Each event falls into multiple overlapping windows")
    print("   - Watermark drops data older than 10 minutes")
    print("   - Results updated as new data arrives")
    
    return windowed_counts


def demo_filtered_stream(spark, host="localhost", port=9999):
    """
    Filtered stream: Extract and process specific patterns.
    
    Use case:
    - Monitor for security keywords
    - Alert on suspicious patterns
    - Extract structured data from logs
    """
    print("\n" + "=" * 70)
    print("4. FILTERED STREAM (SECURITY MONITORING)")
    print("=" * 70)
    
    print("\n🔍 Filtering for security-related keywords...")
    print("   Keywords: fingerprint, match, alert, suspect, search")
    
    # Read lines
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()
    
    # Add processing timestamp and normalize text
    processed_df = lines_df \
        .withColumn("timestamp", current_timestamp()) \
        .withColumn("text_length", length(col("value"))) \
        .withColumn("text_lower", lower(trim(col("value"))))
    
    # Filter for security keywords
    security_keywords = ["fingerprint", "match", "alert", "suspect", "search"]
    filter_condition = None
    
    for keyword in security_keywords:
        if filter_condition is None:
            filter_condition = col("text_lower").contains(keyword)
        else:
            filter_condition = filter_condition | col("text_lower").contains(keyword)
    
    filtered_df = processed_df.filter(filter_condition)
    
    print("\n📊 Enhanced schema:")
    filtered_df.printSchema()
    
    print("\n⚠️  Alert conditions:")
    for i, keyword in enumerate(security_keywords, 1):
        print(f"   {i}. Text contains '{keyword}'")
    
    return filtered_df


def demo_write_to_console(streaming_df, output_mode="complete"):
    """
    Write streaming results to console.
    
    Output modes:
    - complete: Full result table (for aggregations)
    - append: Only new rows (for non-aggregated data)
    - update: Only changed rows (for aggregations)
    """
    print("\n" + "=" * 70)
    print("5. CONSOLE OUTPUT")
    print("=" * 70)
    
    print(f"\n📺 Starting console output...")
    print(f"   Output mode: {output_mode.upper()}")
    print("   Truncate: False (show full text)")
    
    query = streaming_df.writeStream \
        .format("console") \
        .outputMode(output_mode) \
        .option("truncate", False) \
        .start()
    
    return query


def main():
    """
    Main demo function.
    
    NOTE: This example requires a TCP server to be running.
    You can use netcat to test:
    
    Terminal 1: nc -lk 9999
    Terminal 2: python 03_read_tcp_socket.py
    Terminal 1: Type messages and press enter
    """
    # Create Spark session
    spark = SparkSession.builder \
        .appName("TCPSocketStreamingDemo") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("╔" + "=" * 68 + "╗")
        print("║" + " " * 14 + "TCP SOCKET STREAMING DEMO" + " " * 29 + "║")
        print("╚" + "=" * 68 + "╝")
        
        # Connection details
        host = "localhost"
        port = 9999
        
        # Instructions for setup
        start_netcat_server(port)
        
        print("\n" + "=" * 70)
        print("SETUP REQUIRED")
        print("=" * 70)
        print("\n⚠️  Before running, start a TCP server:")
        print(f"\n   Option 1 - Netcat (simple):")
        print(f"      nc -lk {port}")
        print(f"\n   Option 2 - Netcat (Linux alternative):")
        print(f"      nc -l {port}")
        print(f"\n   Option 3 - Python socket server:")
        print("      python -c \"import socket; s=socket.socket(); s.bind(('',9999)); s.listen(1); c,a=s.accept(); [c.send(input().encode()+'\\n'.encode()) for _ in iter(int,1)]\"")
        
        print("\n" + "=" * 70)
        print("EXAMPLE USAGE")
        print("=" * 70)
        
        # Example 1: Basic stream (commented out - requires active socket)
        print("\n1️⃣  Basic Socket Stream:")
        print("""
    lines_df = demo_basic_socket_stream(spark, host, port)
    query = demo_write_to_console(lines_df, "append")
    query.awaitTermination(30)  # Run for 30 seconds
    query.stop()
        """)
        
        # Example 2: Word count (commented out - requires active socket)
        print("\n2️⃣  Word Count:")
        print("""
    word_counts = demo_word_count(spark, host, port)
    query = demo_write_to_console(word_counts, "complete")
    query.awaitTermination(30)
    query.stop()
        """)
        
        # Example 3: Windowed word count (commented out - requires active socket)
        print("\n3️⃣  Windowed Word Count:")
        print("""
    windowed = demo_windowed_word_count(spark, host, port)
    query = demo_write_to_console(windowed, "complete")
    query.awaitTermination(30)
    query.stop()
        """)
        
        # Example 4: Filtered stream (commented out - requires active socket)
        print("\n4️⃣  Filtered Security Stream:")
        print("""
    filtered = demo_filtered_stream(spark, host, port)
    query = demo_write_to_console(filtered, "append")
    query.awaitTermination(30)
    query.stop()
        """)
        
        print("\n" + "=" * 70)
        print("PRODUCTION BEST PRACTICES")
        print("=" * 70)
        
        print("\n✅ DO:")
        print("   ✓ Use sockets for testing/debugging only")
        print("   ✓ Implement reconnection logic in production")
        print("   ✓ Add error handling for connection failures")
        print("   ✓ Use watermarks for time-based operations")
        print("   ✓ Set appropriate checkpointing")
        
        print("\n❌ DON'T:")
        print("   ✗ Use sockets for production streaming")
        print("   ✗ Rely on socket source for fault tolerance")
        print("   ✗ Process unbounded streams without watermarks")
        print("   ✗ Skip checkpointing in stateful operations")
        
        print("\n🔄 ALTERNATIVES FOR PRODUCTION:")
        print("   → Kafka: Distributed, fault-tolerant, scalable")
        print("   → Kinesis: AWS managed streaming")
        print("   → Event Hubs: Azure managed streaming")
        print("   → File sources: Simple, reliable, audit-friendly")
        
        print("\n" + "=" * 70)
        print("TESTING INSTRUCTIONS")
        print("=" * 70)
        
        print("\n📝 To test this code:")
        print("   1. Open two terminals")
        print(f"   2. Terminal 1: nc -lk {port}")
        print("   3. Terminal 2: python 03_read_tcp_socket.py")
        print("   4. Terminal 1: Type messages and press Enter")
        print("   5. Terminal 2: Watch the streaming output")
        
        print("\n📤 Sample test messages:")
        print("   • fingerprint match found for subject 12345")
        print("   • background check completed successfully")
        print("   • alert suspicious activity detected")
        print("   • database search initiated")
        print("   • fingerprint search match confirmed")
        
        print("\n" + "=" * 70)
        print("✅ TCP SOCKET STREAMING DEMO COMPLETE")
        print("=" * 70)
        
        print("\n💡 To actually run streaming queries, uncomment the")
        print("   example code above and ensure a TCP server is running.")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
