"""
================================================================================
Spark Streaming - Kafka with JSON Messages
================================================================================

PURPOSE:
--------
Demonstrates production-ready Kafka integration with JSON message processing,
the most common pattern for real-time data pipelines in enterprise systems.

WHAT THIS DOES:
---------------
- Produces JSON messages to Kafka topics
- Consumes JSON messages from Kafka in real-time
- Parses JSON into structured Spark DataFrames
- Performs transformations and aggregations
- Writes results back to Kafka or other sinks

WHY KAFKA + JSON:
-----------------
- INDUSTRY STANDARD: Most common streaming pattern
- HUMAN READABLE: JSON easy to debug and inspect
- SCHEMA FLEXIBILITY: Easy to add/remove fields
- ECOSYSTEM: All tools support JSON
- INTEROPERABILITY: Works with any language/platform

HOW IT WORKS:
-------------
1. Producer serializes objects to JSON strings
2. JSON messages written to Kafka topic
3. Spark reads messages from Kafka (key, value, metadata)
4. from_json() parses JSON string into structured columns
5. Transform data using Spark SQL
6. Write results (Kafka, Parquet, Database, etc.)

KAFKA MESSAGE STRUCTURE:
-------------------------
Each Kafka record contains:
- key: Message key (optional, for partitioning)
- value: Message payload (JSON string)
- topic: Topic name
- partition: Partition number
- offset: Position in partition
- timestamp: Message timestamp

REAL-WORLD USE CASES:
---------------------
- EVENT-DRIVEN MICROSERVICES:
  Example: Order placed → Kafka → Spark → Update inventory

- REAL-TIME ANALYTICS:
  Example: User clicks → Kafka → Spark → Real-time dashboards

- LOG AGGREGATION:
  Example: Application logs → Kafka → Spark → Elasticsearch

- FBI CJIS: Incident reporting and case management
  Example: Field reports → Kafka → Spark → Case database

KEY CONCEPTS:
-------------

1. KAFKA CONSUMER CONFIGURATION:
   - bootstrap.servers: Broker addresses
   - subscribe: Topic name(s) to consume
   - startingOffsets: "earliest" (replay) or "latest" (new only)
   - failOnDataLoss: false (handle missing data gracefully)
   - kafka.security.protocol: SSL/SASL for encryption

2. JSON SERIALIZATION/DESERIALIZATION:
   - to_json(): Struct → JSON string for writing
   - from_json(): JSON string → Struct for reading
   - Must specify schema (or infer from sample)

3. OFFSET MANAGEMENT:
   - Kafka tracks consumer position per partition
   - Checkpointing enables exactly-once semantics
   - Can resume from last checkpoint after failure

4. CHECKPOINTING:
   - Saves streaming state and offsets
   - Enables fault tolerance and exactly-once
   - Store in reliable location (HDFS, S3, etc.)
   - Required for production streaming jobs

EXACTLY-ONCE SEMANTICS:
------------------------
Achieved through:
1. Kafka offset management (idempotent reads)
2. Checkpointing (state recovery)
3. Idempotent writes (duplicate elimination)
4. Transactional writes to Kafka

PERFORMANCE CONSIDERATIONS:
---------------------------
- BATCH SIZE: Tune maxOffsetsPerTrigger for throughput
- PARTITIONING: More partitions = more parallelism
- COMPRESSION: Enable Kafka compression (snappy, lz4)
- SCHEMA: Define explicit schema (faster than inference)

FAULT TOLERANCE:
----------------
- CHECKPOINTING: Automatic recovery from failures
- REPLICATION: Kafka replicates data across brokers
- EXACTLY-ONCE: No duplicates or data loss
- MONITORING: Track consumer lag and throughput

WHEN TO USE JSON:
-----------------
✅ Human-readable format needed
✅ Schema flexibility important
✅ Interoperability with non-JVM systems
✅ Easy debugging required
❌ Maximum performance critical (use Avro instead)
❌ Strict schema enforcement (use Avro with schema registry)

JSON VS AVRO:
-------------
JSON:
- Human-readable, easy to debug
- Larger size (no compression)
- Schema embedded in data
- Good for: General use, flexibility

AVRO:
- Binary format (efficient)
- Smaller size (20-40% less)
- Schema stored separately (registry)
- Good for: High throughput, strict schemas

================================================================================
"""

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    expr,
    from_json,
    struct,
    to_json,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_kafka_connection_config():
    """
    Kafka connection configuration.

    Production considerations:
    - Use environment variables for sensitive data
    - Configure SSL/SASL for security
    - Set appropriate consumer group ID
    - Configure replication factor
    """
    config = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "incidents",  # Topic name
        "startingOffsets": "earliest",  # Or "latest" for new data only
        "failOnDataLoss": "false",  # Handle missing data gracefully
    }

    print("\n⚙️  Kafka Configuration:")
    print(f"   Bootstrap servers: {config['kafka.bootstrap.servers']}")
    print(f"   Topic: {config['subscribe']}")
    print(f"   Starting offsets: {config['startingOffsets']}")
    print(f"   Fail on data loss: {config['failOnDataLoss']}")

    return config


def define_incident_schema():
    """
    Define schema for incident records.

    FBI CJIS incident structure:
    - incident_id: Unique identifier
    - timestamp: When incident occurred
    - incident_type: Classification (assault, theft, etc.)
    - severity: Priority level (1-5)
    - location: Coordinates or address
    - reporting_officer: Officer ID
    """
    schema = StructType(
        [
            StructField("incident_id", StringType(), False),
            StructField("timestamp", StringType(), True),
            StructField("incident_type", StringType(), True),
            StructField("severity", IntegerType(), True),
            StructField("location", StringType(), True),
            StructField("reporting_officer", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )

    print("\n�� Incident Schema:")
    print("   - incident_id: STRING (required)")
    print("   - timestamp: STRING")
    print("   - incident_type: STRING")
    print("   - severity: INTEGER (1-5)")
    print("   - location: STRING")
    print("   - reporting_officer: STRING")
    print("   - description: STRING")

    return schema


def demo_kafka_consumer(spark):
    """
    Read JSON messages from Kafka.

    Kafka message structure:
    - key: Optional message key (binary)
    - value: Message payload (binary)
    - topic: Topic name
    - partition: Partition number
    - offset: Message offset
    - timestamp: Kafka timestamp
    """
    print("\n" + "=" * 70)
    print("1. KAFKA CONSUMER - READ JSON MESSAGES")
    print("=" * 70)

    # Get connection config
    config = get_kafka_connection_config()

    # Read from Kafka
    print("\n📥 Consuming messages from Kafka...")
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config["kafka.bootstrap.servers"])
        .option("subscribe", config["subscribe"])
        .option("startingOffsets", config["startingOffsets"])
        .option("failOnDataLoss", config["failOnDataLoss"])
        .load()
    )

    print("\n📊 Raw Kafka schema:")
    kafka_df.printSchema()

    print("\n💡 Key columns:")
    print("   - key: Message key (binary)")
    print("   - value: Message payload (binary) - THIS IS YOUR JSON")
    print("   - topic: Topic name")
    print("   - partition: Partition number")
    print("   - offset: Message offset")
    print("   - timestamp: Kafka timestamp")
    print("   - timestampType: Type of timestamp (0=CreateTime, 1=LogAppendTime)")

    return kafka_df


def demo_parse_json(spark, kafka_df, schema):
    """
    Parse JSON from Kafka value column.

    Steps:
    1. Cast binary 'value' to string
    2. Parse string as JSON using schema
    3. Extract nested fields
    """
    print("\n" + "=" * 70)
    print("2. PARSE JSON FROM KAFKA")
    print("=" * 70)

    print("\n⚙️  Parsing JSON messages...")

    # Cast binary value to string and parse as JSON
    parsed_df = (
        kafka_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .select(col("key"), from_json(col("value"), schema).alias("data"))
        .select("key", "data.*")
    )

    print("\n📊 Parsed schema:")
    parsed_df.printSchema()

    print("\n✅ Successfully parsed JSON into structured columns")

    return parsed_df


def demo_transformations(parsed_df):
    """
    Apply transformations to parsed data.

    Common patterns:
    - Filter high-severity incidents
    - Add processing timestamp
    - Calculate derived fields
    - Enrich with reference data
    """
    print("\n" + "=" * 70)
    print("3. APPLY TRANSFORMATIONS")
    print("=" * 70)

    print("\n⚙️  Transformations:")
    print("   1. Filter: severity >= 3 (high priority)")
    print("   2. Add: processing_time column")
    print("   3. Add: priority_level (HIGH/MEDIUM/LOW)")
    print("   4. Clean: trim and uppercase incident_type")

    # Apply transformations
    transformed_df = (
        parsed_df.filter(col("severity") >= 3)
        .withColumn("processing_time", current_timestamp())
        .withColumn(
            "priority_level",
            expr(
                """
                CASE 
                    WHEN severity >= 4 THEN 'HIGH'
                    WHEN severity = 3 THEN 'MEDIUM'
                    ELSE 'LOW'
                END
            """
            ),
        )
        .withColumn("incident_type", expr("UPPER(TRIM(incident_type))"))
    )

    print("\n📊 Transformed schema:")
    transformed_df.printSchema()

    return transformed_df


def demo_aggregations(parsed_df):
    """
    Perform windowed aggregations.

    Real-world analytics:
    - Incidents per hour
    - Average severity by type
    - Hot spot detection
    """
    print("\n" + "=" * 70)
    print("4. WINDOWED AGGREGATIONS")
    print("=" * 70)

    print("\n📊 Aggregation configuration:")
    print("   Window: 1 hour")
    print("   Slide: 15 minutes")
    print("   Watermark: 30 minutes")

    # Convert timestamp string to TimestampType
    df_with_timestamp = parsed_df.withColumn(
        "event_time", expr("CAST(timestamp AS TIMESTAMP)")
    )

    # Windowed aggregations
    aggregated_df = (
        df_with_timestamp.withWatermark("event_time", "30 minutes")
        .groupBy(
            window(col("event_time"), "1 hour", "15 minutes"), col("incident_type")
        )
        .agg(count("*").alias("incident_count"), avg("severity").alias("avg_severity"))
    )

    print("\n📊 Aggregated schema:")
    aggregated_df.printSchema()

    return aggregated_df


def demo_kafka_producer(spark, processed_df):
    """
    Write results back to Kafka.

    Producing to Kafka:
    - Convert DataFrame to key-value format
    - Serialize value as JSON
    - Write to different topic
    """
    print("\n" + "=" * 70)
    print("5. KAFKA PRODUCER - WRITE JSON MESSAGES")
    print("=" * 70)

    print("\n📤 Writing to Kafka...")
    print("   Output topic: processed_incidents")
    print("   Format: JSON")
    print("   Key: incident_id")

    # Prepare for Kafka output
    # Kafka requires columns: key (optional), value (required)
    kafka_output = processed_df.select(
        col("incident_id").alias("key"), to_json(struct("*")).alias("value")
    )

    print("\n📊 Kafka output schema:")
    kafka_output.printSchema()

    print("\n💡 Output format:")
    print("   - key: incident_id (string)")
    print("   - value: Full record as JSON (string)")

    return kafka_output


def demo_write_to_console(streaming_df):
    """
    Write to console for debugging.
    """
    print("\n" + "=" * 70)
    print("6. CONSOLE OUTPUT (DEBUGGING)")
    print("=" * 70)

    print("\n📺 Writing to console...")

    query = (
        streaming_df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    return query


def demo_write_to_kafka(kafka_output_df, checkpoint_path):
    """
    Write to Kafka with exactly-once semantics.
    """
    print("\n" + "=" * 70)
    print("7. WRITE TO KAFKA (PRODUCTION)")
    print("=" * 70)

    print("\n📤 Writing to Kafka...")
    print(f"   Checkpoint: {checkpoint_path}")
    print("   Output mode: APPEND")
    print("   Exactly-once: Enabled (via checkpointing)")

    query = (
        kafka_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("topic", "processed_incidents")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )

    return query


def main():
    """
    Main demo function.

    Prerequisites:
    1. Kafka broker running on localhost:9092
    2. Topic 'incidents' created
    3. Spark with Kafka package:
       spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
    """
    # Create Spark session with Kafka support
    spark = (
        SparkSession.builder.appName("KafkaJSONDemo")
        .config(
            "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("╔" + "=" * 68 + "╗")
        print("║" + " " * 12 + "KAFKA JSON STREAMING DEMO" + " " * 31 + "║")
        print("╚" + "=" * 68 + "╝")

        # Define schema
        incident_schema = define_incident_schema()

        # Demo examples (conceptual - requires running Kafka)
        print("\n" + "=" * 70)
        print("KAFKA SETUP REQUIRED")
        print("=" * 70)

        print("\n⚠️  Prerequisites:")
        print("   1. Start Kafka:")
        print("      docker run -d -p 9092:9092 apache/kafka:latest")
        print("\n   2. Create topic:")
        print("      kafka-topics.sh --create --topic incidents \\")
        print("        --bootstrap-server localhost:9092")
        print("\n   3. Start Spark with Kafka package:")
        print(
            "      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\"
        )
        print("        04_kafka_json_messages.py")

        print("\n" + "=" * 70)
        print("EXAMPLE WORKFLOW")
        print("=" * 70)

        print("\n1️⃣  Consume from Kafka:")
        print(
            """
    kafka_df = demo_kafka_consumer(spark)
        """
        )

        print("\n2️⃣  Parse JSON:")
        print(
            """
    schema = define_incident_schema()
    parsed_df = demo_parse_json(spark, kafka_df, schema)
        """
        )

        print("\n3️⃣  Transform data:")
        print(
            """
    transformed_df = demo_transformations(parsed_df)
        """
        )

        print("\n4️⃣  Aggregate:")
        print(
            """
    aggregated_df = demo_aggregations(parsed_df)
        """
        )

        print("\n5️⃣  Write back to Kafka:")
        print(
            """
    kafka_output = demo_kafka_producer(spark, transformed_df)
    query = demo_write_to_kafka(kafka_output, "/tmp/checkpoint")
    query.awaitTermination(60)
    query.stop()
        """
        )

        print("\n" + "=" * 70)
        print("SAMPLE KAFKA PRODUCER (Python)")
        print("=" * 70)

        print(
            """
# Install: pip install kafka-python

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send sample incident
incident = {
    "incident_id": "INC-2024-001",
    "timestamp": "2024-12-13T10:00:00",
    "incident_type": "Assault",
    "severity": 4,
    "location": "123 Main St",
    "reporting_officer": "OFF-456",
    "description": "Armed assault reported"
}

producer.send('incidents', value=incident)
producer.flush()
        """
        )

        print("\n" + "=" * 70)
        print("PRODUCTION BEST PRACTICES")
        print("=" * 70)

        print("\n✅ DO:")
        print("   ✓ Use explicit schemas for JSON parsing")
        print("   ✓ Enable checkpointing for exactly-once semantics")
        print("   ✓ Set appropriate consumer group IDs")
        print("   ✓ Configure proper retention policies")
        print("   ✓ Use SSL/SASL for production")
        print("   ✓ Monitor consumer lag")
        print("   ✓ Partition topics appropriately")
        print("   ✓ Handle schema evolution")

        print("\n❌ DON'T:")
        print("   ✗ Skip checkpointing in production")
        print("   ✗ Use 'earliest' offset in production (risk of reprocessing)")
        print("   ✗ Ignore consumer lag metrics")
        print("   ✗ Run without authentication in production")
        print("   ✗ Use small partition counts for high-throughput topics")

        print("\n📊 KAFKA CONFIGURATION TUNING:")
        print("   • max.poll.records: Records per fetch (default: 500)")
        print("   • fetch.max.bytes: Max bytes per fetch (default: 50MB)")
        print("   • session.timeout.ms: Consumer heartbeat (default: 10s)")
        print("   • enable.auto.commit: Auto offset commit (default: true)")

        print("\n" + "=" * 70)
        print("✅ KAFKA JSON STREAMING DEMO COMPLETE")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
