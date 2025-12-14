"""
ADVANCED 02: Streaming Introduction
Learn structured streaming basics - readStream, writeStream, triggers, watermarks.
Real-time processing of event data.
TIME: 40 minutes | DIFFICULTY: ⭐⭐⭐⭐⭐ (5/5)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os

def create_spark_session():
    return SparkSession.builder.appName("Advanced02_Streaming").getOrCreate()

def example_1_basic_stream():
    """Basic streaming example."""
    spark = create_spark_session()
    
    # Create sample streaming data source (rate source)
    stream_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
    
    print("\n✅ Basic Stream Schema:")
    stream_df.printSchema()
    
    # Write to console
    query = stream_df.writeStream.format("console").outputMode("append").start()
    query.processAllAvailable()
    query.stop()
    
    spark.stop()

def main():
    print("\n🌊" * 40)
    print("ADVANCED LESSON 2: STREAMING INTRODUCTION")
    print("🌊" * 40)
    
    example_1_basic_stream()
    
    print("\n🎉 STREAMING BASICS COMPLETE!")
    print("\n➡️  NEXT: Try advanced/03_ml_pipelines.py")

if __name__ == "__main__":
    main()
