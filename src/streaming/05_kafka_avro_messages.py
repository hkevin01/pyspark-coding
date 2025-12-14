"""
Spark Streaming - Kafka with Avro Messages
==========================================

Demonstrates producing and consuming Avro-encoded messages with Kafka.
Avro provides efficient binary serialization with schema evolution support.

Real-world use cases:
- High-throughput data pipelines (50%+ smaller than JSON)
- Schema evolution and compatibility
- Data lake ingestion
- FBI CJIS: Biometric data transmission (fingerprints, photos)

Key concepts:
- Avro schema definition
- Schema Registry integration
- Binary serialization/deserialization
- Schema compatibility modes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_avro, to_avro, struct,
    current_timestamp, expr
)
from pyspark.sql.avro.functions import from_avro as spark_from_avro
from pyspark.sql.avro.functions import to_avro as spark_to_avro
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, BinaryType
)
import json


def define_avro_schema():
    """
    Define Avro schema for biometric records.
    
    Avro schema components:
    - type: record, enum, array, map, union, fixed, etc.
    - name: Schema name (required for records)
    - namespace: Fully qualified namespace
    - fields: Array of field definitions
    
    Benefits over JSON:
    - Compact binary format (50-70% smaller)
    - Schema included in data
    - Strong typing
    - Schema evolution support
    """
    avro_schema = {
        "type": "record",
        "name": "BiometricRecord",
        "namespace": "gov.fbi.cjis",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "timestamp", "type": "long"},  # Unix epoch milliseconds
            {"name": "subject_id", "type": "string"},
            {"name": "biometric_type", "type": {
                "type": "enum",
                "name": "BiometricType",
                "symbols": ["FINGERPRINT", "FACE", "IRIS", "DNA"]
            }},
            {"name": "data_size", "type": "int"},
            {"name": "match_score", "type": "double"},
            {"name": "metadata", "type": ["null", "string"], "default": None}
        ]
    }
    
    # Convert to JSON string for Spark
    avro_schema_json = json.dumps(avro_schema)
    
    print("\n📋 Avro Schema Definition:")
    print(json.dumps(avro_schema, indent=2))
    
    print("\n💡 Avro Schema Features:")
    print("   ✓ Enum type for biometric_type (type safety)")
    print("   ✓ Nullable metadata field (union type)")
    print("   ✓ Long type for timestamp (millisecond precision)")
    print("   ✓ Namespace for schema organization")
    
    return avro_schema_json


def get_schema_registry_config():
    """
    Schema Registry configuration.
    
    Confluent Schema Registry:
    - Centralized schema management
    - Schema versioning
    - Compatibility checking
    - Schema evolution governance
    
    Compatibility modes:
    - BACKWARD: New schema can read old data
    - FORWARD: Old schema can read new data
    - FULL: Both backward and forward compatible
    - NONE: No compatibility checking
    """
    config = {
        "schema.registry.url": "http://localhost:8081",
        "schema.registry.subject": "biometric-records-value",
        "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer": "io.confluent.kafka.serializers.KafkaAvroSerializer"
    }
    
    print("\n⚙️  Schema Registry Configuration:")
    print(f"   URL: {config['schema.registry.url']}")
    print(f"   Subject: {config['schema.registry.subject']}")
    print("   Compatibility: BACKWARD (default)")
    
    print("\n🔄 Schema Evolution Rules (BACKWARD):")
    print("   ✓ Can add optional fields")
    print("   ✓ Can delete fields")
    print("   ✗ Cannot add required fields without default")
    print("   ✗ Cannot change field types")
    
    return config


def demo_kafka_avro_consumer(spark, avro_schema_json):
    """
    Read Avro messages from Kafka.
    
    Two approaches:
    1. With Schema Registry (recommended)
    2. With embedded schema (simpler but less flexible)
    """
    print("\n" + "=" * 70)
    print("1. KAFKA AVRO CONSUMER")
    print("=" * 70)
    
    print("\n📥 Consuming Avro messages from Kafka...")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "biometric-records") \
        .option("startingOffsets", "earliest") \
        .load()
    
    print("\n📊 Raw Kafka schema (binary data):")
    kafka_df.printSchema()
    
    # Approach 1: Deserialize with Schema Registry
    print("\n🔧 Approach 1: With Schema Registry")
    print("   Requires: Confluent Schema Registry running")
    print("   Benefit: Automatic schema evolution")
    print("""
    deserialized_df = kafka_df.select(
        col("key").cast("string"),
        from_avro(
            col("value"),
            options={
                "mode": "PERMISSIVE",
                "schema.registry.url": "http://localhost:8081"
            }
        ).alias("data")
    ).select("key", "data.*")
    """)
    
    # Approach 2: Deserialize with explicit schema
    print("\n🔧 Approach 2: With Explicit Schema")
    print("   Requires: Avro schema JSON string")
    print("   Benefit: No external dependencies")
    
    deserialized_df = kafka_df.select(
        col("key").cast("string"),
        from_avro(col("value"), avro_schema_json).alias("data")
    ).select("key", "data.*")
    
    print("\n📊 Deserialized schema:")
    deserialized_df.printSchema()
    
    return deserialized_df


def demo_avro_transformations(avro_df):
    """
    Apply transformations to Avro data.
    
    Common patterns:
    - Filter by biometric type
    - Convert Unix timestamp to readable format
    - Calculate data quality metrics
    """
    print("\n" + "=" * 70)
    print("2. AVRO DATA TRANSFORMATIONS")
    print("=" * 70)
    
    print("\n⚙️  Transformations:")
    print("   1. Filter: FINGERPRINT records only")
    print("   2. Convert: Unix timestamp to readable datetime")
    print("   3. Add: Quality flag (match_score >= 0.8)")
    print("   4. Add: Processing timestamp")
    
    transformed_df = avro_df \
        .filter(col("biometric_type") == "FINGERPRINT") \
        .withColumn(
            "event_time",
            expr("CAST(timestamp / 1000 AS TIMESTAMP)")  # Convert milliseconds
        ) \
        .withColumn(
            "high_quality",
            col("match_score") >= 0.8
        ) \
        .withColumn("processing_time", current_timestamp())
    
    print("\n📊 Transformed schema:")
    transformed_df.printSchema()
    
    return transformed_df


def demo_avro_producer(spark, processed_df, avro_schema_json):
    """
    Write data to Kafka with Avro encoding.
    
    Benefits:
    - 50-70% smaller than JSON
    - Faster serialization/deserialization
    - Built-in schema validation
    - Schema evolution support
    """
    print("\n" + "=" * 70)
    print("3. KAFKA AVRO PRODUCER")
    print("=" * 70)
    
    print("\n📤 Encoding to Avro and writing to Kafka...")
    
    # Prepare data for Avro encoding
    # Must match the Avro schema structure
    kafka_output = processed_df \
        .select(
            col("record_id").alias("key"),
            to_avro(struct(
                col("record_id"),
                col("timestamp"),
                col("subject_id"),
                col("biometric_type"),
                col("data_size"),
                col("match_score"),
                col("metadata")
            ), avro_schema_json).alias("value")
        )
    
    print("\n📊 Kafka output schema (Avro-encoded):")
    kafka_output.printSchema()
    
    print("\n💾 Size comparison (estimated):")
    print("   JSON:     ~500 bytes per record")
    print("   Avro:     ~200 bytes per record")
    print("   Savings:  ~60% reduction")
    
    return kafka_output


def demo_schema_evolution():
    """
    Demonstrate schema evolution scenarios.
    
    Common evolution patterns:
    - Adding optional fields
    - Removing fields
    - Changing defaults
    - Field renaming (with aliases)
    """
    print("\n" + "=" * 70)
    print("4. SCHEMA EVOLUTION EXAMPLES")
    print("=" * 70)
    
    print("\n📝 Scenario 1: Add Optional Field")
    print("   OLD: {record_id, timestamp, subject_id, ...}")
    print("   NEW: {record_id, timestamp, subject_id, ..., quality_score}")
    print("   ✅ COMPATIBLE (BACKWARD): Old readers can ignore new field")
    
    evolved_schema_v2 = {
        "type": "record",
        "name": "BiometricRecord",
        "namespace": "gov.fbi.cjis",
        "fields": [
            {"name": "record_id", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "subject_id", "type": "string"},
            {"name": "biometric_type", "type": {
                "type": "enum",
                "name": "BiometricType",
                "symbols": ["FINGERPRINT", "FACE", "IRIS", "DNA"]
            }},
            {"name": "data_size", "type": "int"},
            {"name": "match_score", "type": "double"},
            {"name": "metadata", "type": ["null", "string"], "default": None},
            # NEW FIELD
            {"name": "quality_score", "type": ["null", "double"], "default": None}
        ]
    }
    
    print(f"\n   Schema v2: {json.dumps(evolved_schema_v2, indent=2)}")
    
    print("\n📝 Scenario 2: Remove Field")
    print("   OLD: {..., metadata, quality_score}")
    print("   NEW: {..., metadata}")
    print("   ✅ COMPATIBLE (BACKWARD): New readers ignore missing field")
    
    print("\n📝 Scenario 3: Change Field Type")
    print("   OLD: {match_score: double}")
    print("   NEW: {match_score: string}")
    print("   ❌ NOT COMPATIBLE: Type mismatch")
    
    print("\n📝 Scenario 4: Add Required Field")
    print("   OLD: {record_id, timestamp}")
    print("   NEW: {record_id, timestamp, required_field}")
    print("   ❌ NOT COMPATIBLE (without default): Old data missing field")
    
    print("\n💡 Best Practices:")
    print("   ✓ Always use nullable types (union with null)")
    print("   ✓ Provide default values for new fields")
    print("   ✓ Never change field types")
    print("   ✓ Test compatibility before deployment")
    print("   ✓ Version your schemas (v1, v2, etc.)")


def main():
    """
    Main demo function.
    
    Prerequisites:
    1. Kafka broker: localhost:9092
    2. Schema Registry: localhost:8081 (optional but recommended)
    3. Spark with Avro package:
       spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0
    """
    # Create Spark session with Avro support
    spark = SparkSession.builder \
        .appName("KafkaAvroDemo") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-avro_2.12:3.5.0,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("╔" + "=" * 68 + "╗")
        print("║" + " " * 11 + "KAFKA AVRO STREAMING DEMO" + " " * 32 + "║")
        print("╚" + "=" * 68 + "╝")
        
        # Define Avro schema
        avro_schema_json = define_avro_schema()
        
        # Get Schema Registry config
        schema_registry_config = get_schema_registry_config()
        
        # Demo schema evolution
        demo_schema_evolution()
        
        print("\n" + "=" * 70)
        print("SETUP REQUIRED")
        print("=" * 70)
        
        print("\n⚠️  Prerequisites:")
        print("\n1. Start Kafka:")
        print("   docker run -d --name kafka -p 9092:9092 apache/kafka:latest")
        
        print("\n2. Start Schema Registry (optional):")
        print("   docker run -d --name schema-registry \\")
        print("     -p 8081:8081 \\")
        print("     -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092 \\")
        print("     confluentinc/cp-schema-registry:latest")
        
        print("\n3. Create topic:")
        print("   kafka-topics.sh --create --topic biometric-records \\")
        print("     --bootstrap-server localhost:9092")
        
        print("\n4. Install Python dependencies:")
        print("   pip install avro-python3 confluent-kafka")
        
        print("\n" + "=" * 70)
        print("EXAMPLE WORKFLOW")
        print("=" * 70)
        
        print("\n1️⃣  Define Avro schema:")
        print("""
    avro_schema = define_avro_schema()
        """)
        
        print("\n2️⃣  Consume Avro from Kafka:")
        print("""
    avro_df = demo_kafka_avro_consumer(spark, avro_schema)
        """)
        
        print("\n3️⃣  Transform data:")
        print("""
    transformed_df = demo_avro_transformations(avro_df)
        """)
        
        print("\n4️⃣  Produce Avro to Kafka:")
        print("""
    kafka_output = demo_avro_producer(spark, transformed_df, avro_schema)
    query = kafka_output.writeStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("topic", "processed-biometrics") \\
        .option("checkpointLocation", "/tmp/checkpoint") \\
        .start()
    query.awaitTermination(60)
    query.stop()
        """)
        
        print("\n" + "=" * 70)
        print("AVRO PRODUCER EXAMPLE (Python)")
        print("=" * 70)
        
        print("""
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Define schema
value_schema = avro.loads(avro_schema_json)

# Create producer
producer = AvroProducer({
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081'
}, default_value_schema=value_schema)

# Send record
record = {
    "record_id": "REC-001",
    "timestamp": 1702461600000,  # Unix milliseconds
    "subject_id": "SUB-12345",
    "biometric_type": "FINGERPRINT",
    "data_size": 65536,
    "match_score": 0.95,
    "metadata": "Left thumb impression"
}

producer.produce(topic='biometric-records', value=record)
producer.flush()
        """)
        
        print("\n" + "=" * 70)
        print("AVRO VS JSON COMPARISON")
        print("=" * 70)
        
        print("\n📊 JSON Advantages:")
        print("   ✓ Human-readable")
        print("   ✓ Widely supported")
        print("   ✓ Easy debugging")
        print("   ✓ No schema required")
        
        print("\n📊 Avro Advantages:")
        print("   ✓ 50-70% smaller size")
        print("   ✓ Faster serialization")
        print("   ✓ Schema evolution support")
        print("   ✓ Strong typing")
        print("   ✓ Built-in validation")
        print("   ✓ Better for high-throughput pipelines")
        
        print("\n🎯 When to use Avro:")
        print("   → High-volume data pipelines (>1M msgs/sec)")
        print("   → Long-term data storage (schema evolution needed)")
        print("   → Network bandwidth constraints")
        print("   → Binary data (images, audio, biometrics)")
        print("   → Data lake ingestion")
        
        print("\n🎯 When to use JSON:")
        print("   → Low-volume data")
        print("   → Quick prototyping")
        print("   → Human readability important")
        print("   → Simple debugging required")
        print("   → External system compatibility")
        
        print("\n" + "=" * 70)
        print("PRODUCTION BEST PRACTICES")
        print("=" * 70)
        
        print("\n✅ DO:")
        print("   ✓ Use Schema Registry for production")
        print("   ✓ Version your schemas (namespace + name)")
        print("   ✓ Test schema compatibility before deployment")
        print("   ✓ Use nullable types for flexibility")
        print("   ✓ Provide default values for new fields")
        print("   ✓ Document schema changes")
        print("   ✓ Monitor schema evolution metrics")
        
        print("\n❌ DON'T:")
        print("   ✗ Change field types")
        print("   ✗ Remove required fields without defaults")
        print("   ✗ Deploy breaking schema changes")
        print("   ✗ Skip compatibility testing")
        print("   ✗ Use Avro without Schema Registry in production")
        
        print("\n" + "=" * 70)
        print("✅ KAFKA AVRO STREAMING DEMO COMPLETE")
        print("=" * 70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
