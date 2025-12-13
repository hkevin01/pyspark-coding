"""
01_socket_streaming.py
======================

Basic PySpark Structured Streaming with Socket Source

This example demonstrates:
- Reading from a socket stream
- Word count on streaming data
- Console sink output
- Query management

To test:
1. Start netcat server: nc -lk 9999
2. Run this script
3. Type words in netcat terminal
4. See word counts in console
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def create_spark_session():
    """Create SparkSession for streaming."""
    return SparkSession.builder \
        .appName("SocketStreaming") \
        .master("local[*]") \
        .getOrCreate()


def socket_word_count(spark):
    """
    Read text from socket and count words.
    
    Setup:
    1. Open terminal: nc -lk 9999
    2. Run this script
    3. Type text in netcat terminal
    """
    print("\n" + "=" * 80)
    print("SOCKET WORD COUNT STREAMING")
    print("=" * 80)
    print("\n📡 Instructions:")
    print("   1. Open another terminal")
    print("   2. Run: nc -lk 9999")
    print("   3. Type some text and press Enter")
    print("   4. See word counts below\n")
    
    # Read from socket
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    
    # Split lines into words
    words = lines.select(
        explode(split(col("value"), " ")).alias("word")
    )
    
    # Count words
    word_counts = words.groupBy("word").count()
    
    # Write to console
    query = word_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    print("✅ Streaming query started!")
    print("   Waiting for data from socket (localhost:9999)...")
    print("   Press Ctrl+C to stop\n")
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n⚠️  Stopping streaming query...")
        query.stop()
        print("✅ Query stopped successfully!")


def socket_with_timestamp(spark):
    """
    Read from socket and add timestamp to each line.
    """
    from pyspark.sql.functions import current_timestamp
    
    print("\n" + "=" * 80)
    print("SOCKET STREAMING WITH TIMESTAMPS")
    print("=" * 80)
    
    # Read from socket
    lines = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()
    
    # Add timestamp
    lines_with_timestamp = lines.select(
        col("value"),
        current_timestamp().alias("timestamp")
    )
    
    # Write to console
    query = lines_with_timestamp.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    print("✅ Streaming with timestamps started!")
    
    try:
        query.awaitTermination(timeout=30)  # Run for 30 seconds
    except KeyboardInterrupt:
        query.stop()


def main():
    """Main execution."""
    print("\n" + "🌊" * 40)
    print("PYSPARK SOCKET STREAMING EXAMPLES")
    print("🌊" * 40)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n📝 Example Options:")
    print("   1. Basic word count")
    print("   2. With timestamps")
    print("\nRunning Example 1: Basic Word Count\n")
    
    # Run basic example
    socket_word_count(spark)
    
    # Uncomment to run other examples
    # socket_with_timestamp(spark)
    
    spark.stop()


if __name__ == "__main__":
    main()
