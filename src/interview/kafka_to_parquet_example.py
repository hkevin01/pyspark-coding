"""
================================================================================
KAFKA ETL TO PARQUET - COMPLETE EXAMPLE
================================================================================

PURPOSE: Production-ready example of reading from Kafka and writing to Parquet.
This demonstrates real-time streaming data ingestion into a data lake.

SCENARIO: E-commerce order processing
- Read orders from Kafka topic in real-time
- Parse and validate JSON data
- Apply transformations and enrichments
- Write to partitioned Parquet files
- Handle late data and checkpointing

DIFFICULTY: Advanced | TIME: Production Ready 🚀
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, sum as spark_sum,
    count, avg, current_timestamp, year, month, day, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType
)
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """
    Create SparkSession with Kafka dependencies.
    
    NOTE: Ensure spark-sql-kafka package is available:
    - Add to spark-submit: --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
    - Or configure in spark-defaults.conf
    """
    logger.info("Creating SparkSession with Kafka support...")
    
    spark = SparkSession.builder \
        .appName("Kafka_to_Parquet_ETL") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_checkpoint") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created successfully")
    return spark


def define_schema():
    """
    Define schema for incoming Kafka messages.
    
    Expected JSON format:
    {
        "order_id": "ORD-12345",
        "customer_id": "CUST-67890",
        "product_id": "PROD-111",
        "product_name": "Widget Pro",
        "quantity": 2,
        "price": 29.99,
        "amount": 59.98,
        "order_timestamp": "2025-12-15T10:30:00Z",
        "status": "completed"
    }
    """
    return StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("amount", DoubleType(), False),
        StructField("order_timestamp", StringType(), False),
        StructField("status", StringType(), True)
    ])


def read_from_kafka(spark, bootstrap_servers, topic, starting_offset="latest"):
    """
    Read streaming data from Kafka topic.
    
    Args:
        spark: SparkSession
        bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        topic: Kafka topic name
        starting_offset: Where to start reading ("earliest" or "latest")
    
    Returns:
        Streaming DataFrame with raw Kafka data
    """
    logger.info(f"Reading from Kafka topic: {topic}")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info(f"Starting offset: {starting_offset}")
    
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offset) \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("Kafka stream configured successfully")
    return df_kafka


def parse_kafka_messages(df_kafka, schema):
    """
    Parse Kafka messages from binary to structured data.
    
    Kafka provides:
    - key: binary
    - value: binary (contains our JSON)
    - topic: string
    - partition: int
    - offset: long
    - timestamp: timestamp
    
    We'll parse the 'value' column as JSON.
    """
    logger.info("Parsing Kafka messages...")
    
    df_parsed = df_kafka.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        "kafka_key",
        "data.*",
        "topic",
        "partition", 
        "offset",
        "kafka_timestamp"
    )
    
    logger.info("Messages parsed successfully")
    return df_parsed


def transform_data(df_parsed):
    """
    Apply transformations and data quality checks.
    
    Transformations:
    - Convert timestamp string to timestamp type
    - Add processing timestamp
    - Calculate derived fields
    - Add partition columns for efficient querying
    - Filter out invalid records
    """
    logger.info("Applying transformations...")
    
    df_transformed = df_parsed \
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp"))) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("revenue", col("quantity") * col("price")) \
        .withColumn("year", year(col("order_timestamp"))) \
        .withColumn("month", month(col("order_timestamp"))) \
        .withColumn("day", day(col("order_timestamp"))) \
        .withColumn("hour", hour(col("order_timestamp"))) \
        .filter(col("amount") > 0) \
        .filter(col("order_id").isNotNull()) \
        .filter(col("customer_id").isNotNull())
    
    logger.info("Transformations applied successfully")
    return df_transformed


def write_to_parquet_batch(df, output_path, checkpoint_path, trigger_interval="30 seconds"):
    """
    Write streaming data to Parquet files using batch mode.
    
    Features:
    - Partitioned by year/month/day for efficient querying
    - Checkpointing for fault tolerance
    - Append mode for continuous ingestion
    - Trigger interval controls micro-batch size
    
    Args:
        df: Transformed streaming DataFrame
        output_path: Path to write Parquet files
        checkpoint_path: Path for checkpoint data
        trigger_interval: How often to trigger micro-batches
    """
    logger.info(f"Writing to Parquet: {output_path}")
    logger.info(f"Checkpoint location: {checkpoint_path}")
    logger.info(f"Trigger interval: {trigger_interval}")
    
    query = df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime=trigger_interval) \
        .start()
    
    logger.info("Streaming query started successfully")
    return query


def write_to_parquet_with_foreachBatch(df, output_path, checkpoint_path):
    """
    Alternative: Write using foreachBatch for more control.
    
    This approach allows you to:
    - Perform additional operations per batch
    - Write to multiple sinks
    - Apply custom logic before writing
    - Handle errors per batch
    """
    logger.info(f"Writing to Parquet with foreachBatch: {output_path}")
    
    def process_batch(batch_df, batch_id):
        """Process each micro-batch."""
        if batch_df.count() > 0:
            logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            # Write to Parquet with partitioning
            batch_df.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(output_path)
            
            # You can add additional operations here:
            # - Write to another sink
            # - Update metrics
            # - Send notifications
            
            logger.info(f"Batch {batch_id} written successfully")
        else:
            logger.info(f"Batch {batch_id} is empty, skipping")
    
    query = df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("Streaming query with foreachBatch started")
    return query


def create_aggregated_view(df_transformed, output_path, checkpoint_path):
    """
    Create an aggregated view with windowed metrics.
    
    This writes summary statistics in addition to raw data:
    - Hourly aggregations
    - Customer-level metrics
    - Product performance
    """
    logger.info("Creating aggregated view...")
    
    # Add watermark for handling late data (10 minutes)
    df_watermarked = df_transformed.withWatermark("order_timestamp", "10 minutes")
    
    # Aggregate by customer per hour window
    df_aggregated = df_watermarked.groupBy(
        window(col("order_timestamp"), "1 hour"),
        col("customer_id")
    ).agg(
        spark_sum("amount").alias("total_sales"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value"),
        spark_sum("quantity").alias("total_quantity")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("customer_id"),
        col("total_sales"),
        col("order_count"),
        col("avg_order_value"),
        col("total_quantity")
    )
    
    # Write aggregated data
    query = df_aggregated.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", f"{output_path}_aggregated") \
        .option("checkpointLocation", f"{checkpoint_path}_aggregated") \
        .trigger(processingTime="1 minute") \
        .start()
    
    logger.info("Aggregated view query started")
    return query


def main():
    """
    Main ETL pipeline execution.
    
    Pipeline steps:
    1. Create SparkSession with Kafka support
    2. Define schema for incoming data
    3. Read from Kafka topic
    4. Parse JSON messages
    5. Transform and enrich data
    6. Write to Parquet (raw data)
    7. Write aggregated metrics (optional)
    8. Monitor streaming queries
    """
    logger.info("="*80)
    logger.info("KAFKA TO PARQUET ETL PIPELINE STARTED")
    logger.info("="*80)
    
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "orders"
    OUTPUT_PATH = "/tmp/data_lake/orders"
    CHECKPOINT_PATH = "/tmp/kafka_checkpoint/orders"
    
    try:
        # Step 1: Create Spark session
        spark = create_spark_session()
        
        # Step 2: Define schema
        schema = define_schema()
        
        # Step 3: Read from Kafka
        df_kafka = read_from_kafka(
            spark, 
            KAFKA_BOOTSTRAP_SERVERS, 
            KAFKA_TOPIC,
            starting_offset="latest"
        )
        
        # Step 4: Parse messages
        df_parsed = parse_kafka_messages(df_kafka, schema)
        
        # Step 5: Transform data
        df_transformed = transform_data(df_parsed)
        
        # Step 6: Write to Parquet (raw data)
        query_raw = write_to_parquet_batch(
            df_transformed,
            OUTPUT_PATH,
            CHECKPOINT_PATH,
            trigger_interval="30 seconds"
        )
        
        # Step 7: Create aggregated view (optional)
        query_agg = create_aggregated_view(
            df_transformed,
            OUTPUT_PATH,
            CHECKPOINT_PATH
        )
        
        logger.info("="*80)
        logger.info("PIPELINE RUNNING - Processing streaming data...")
        logger.info("="*80)
        logger.info(f"Raw data output: {OUTPUT_PATH}")
        logger.info(f"Aggregated output: {OUTPUT_PATH}_aggregated")
        logger.info("Press Ctrl+C to stop")
        logger.info("="*80)
        
        # Wait for termination (or run for specific duration)
        query_raw.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("\nStopping pipeline gracefully...")
        logger.info("Streaming queries stopped")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
        
    finally:
        logger.info("="*80)
        logger.info("KAFKA TO PARQUET ETL PIPELINE STOPPED")
        logger.info("="*80)


if __name__ == "__main__":
    main()
