# 🎵 Apache Avro Examples

Comprehensive examples for working with Apache Avro file format in PySpark.

## 📖 What is Avro?

Apache Avro is a **row-based data serialization system** with a compact binary format and rich data structures. It's widely used for streaming, messaging, and data exchange.

### Key Features

- ✅ **Row-Based Storage**: Efficient for streaming and full-row reads
- ✅ **Compact Binary Format**: Smaller than JSON, faster than text
- ✅ **Self-Describing**: Schema embedded with data
- ✅ **Schema Evolution**: Add/remove fields safely
- ✅ **Language Agnostic**: Interoperable across platforms
- ✅ **Rich Data Types**: Records, arrays, maps, unions, enums

## 📂 Files in This Directory

### 01_basic_avro_operations.py
Comprehensive introduction to Avro in PySpark

**Topics**:
- Reading and writing Avro files
- Compression codecs (snappy, deflate)
- Schema evolution
- Partitioned writes
- Complex/nested data types
- Streaming use cases (to_avro/from_avro)

```bash
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0 \
             01_basic_avro_operations.py
```

## 🚀 Quick Start

### Basic Write
```python
df.write.format("avro").mode("overwrite").save("/path/to/output")
```

### Basic Read
```python
df = spark.read.format("avro").load("/path/to/input")
```

### With Compression
```python
df.write \
    .format("avro") \
    .option("compression", "snappy") \
    .save("/path/to/output")
```

### Streaming (Kafka Integration)
```python
from pyspark.sql.avro.functions import to_avro, from_avro

# Serialize to Avro
df_avro = df.select(to_avro(struct(df.columns)).alias("value"))

# Deserialize from Avro
df_decoded = df.select(from_avro(col("value"), schema).alias("data"))
```

## ⚙️ Installation

Avro requires the spark-avro package:

```bash
# With spark-submit
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0 your_script.py

# With pyspark shell
pyspark --packages org.apache.spark:spark-avro_2.12:3.5.0

# In SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
    .getOrCreate()
```

## 📊 Avro vs Other Formats

| <sub>Feature</sub> | <sub>Avro</sub> | <sub>Parquet</sub> | <sub>ORC</sub> | <sub>JSON</sub> |
|---------|------|---------|-----|------|
| <sub>**Storage**</sub> | <sub>Row-based</sub> | <sub>Columnar</sub> | <sub>Columnar</sub> | <sub>Row-based</sub> |
| <sub>**Compression**</sub> | <sub>Good</sub> | <sub>Excellent</sub> | <sub>Excellent</sub> | <sub>Poor</sub> |
| <sub>**Streaming**</sub> | <sub>✅ Excellent</sub> | <sub>❌ Poor</sub> | <sub>❌ Poor</sub> | <sub>✅ Good</sub> |
| <sub>**Analytics**</sub> | <sub>❌ Poor</sub> | <sub>✅ Excellent</sub> | <sub>✅ Excellent</sub> | <sub>❌ Poor</sub> |
| <sub>**Schema Evolution**</sub> | <sub>✅ Excellent</sub> | <sub>✅ Good</sub> | <sub>✅ Good</sub> | <sub>❌ None</sub> |
| <sub>**Human Readable**</sub> | <sub>❌ Binary</sub> | <sub>❌ Binary</sub> | <sub>❌ Binary</sub> | <sub>✅ Yes</sub> |
| <sub>**Size**</sub> | <sub>Medium</sub> | <sub>Small</sub> | <sub>Small</sub> | <sub>Large</sub> |

## 🎯 When to Use Avro

### ✅ Ideal Use Cases

1. **Streaming Data**
   - Kafka producers/consumers
   - Kinesis streams
   - Event-driven architectures

2. **Data Exchange**
   - Microservices communication
   - API payloads
   - Message queues (RabbitMQ, SQS)

3. **Schema Evolution**
   - Frequently changing schemas
   - Multiple versions of data
   - Backward/forward compatibility

4. **Row-Based Access**
   - Full record reads/writes
   - Transactional systems
   - OLTP workloads

### ❌ Not Ideal For

1. **Analytical Queries**
   - Use Parquet or ORC instead
   - Column-heavy aggregations
   - Data warehouse storage

2. **Human Debugging**
   - Binary format not readable
   - Use JSON for debugging

3. **Small Simple Data**
   - Overhead not worth it for tiny datasets
   - Use CSV or JSON

## 📈 Performance Tips

### 1. Use Appropriate Compression
```python
# snappy (default) - fast, good compression
df.write.format("avro").option("compression", "snappy").save(path)

# deflate - slower, better compression
df.write.format("avro").option("compression", "deflate").save(path)
```

### 2. Schema Registry
For production streaming, use a schema registry:

```python
# Confluent Schema Registry
from confluent_kafka.schema_registry import SchemaRegistryClient

schema_registry = SchemaRegistryClient({'url': 'http://localhost:8081'})
schema_id = schema_registry.get_latest_version('my-topic-value')
```

### 3. Partitioning
Partition large datasets:

```python
df.write \
    .format("avro") \
    .partitionBy("year", "month", "day") \
    .save(path)
```

### 4. Coalescing
Control file count:

```python
df.coalesce(10).write.format("avro").save(path)
```

## 🔧 Common Patterns

### Kafka + Avro
```python
# Write to Kafka in Avro format
df.select(to_avro(struct(df.columns)).alias("value")) \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "my-topic") \
    .save()

# Read from Kafka in Avro format
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my-topic") \
    .load()

df_decoded = df.select(
    from_avro(col("value"), avro_schema).alias("data")
).select("data.*")
```

### Schema Evolution Example
```python
# Version 1
df_v1 = spark.createDataFrame([
    {'id': 1, 'name': 'Alice'}
])
df_v1.write.format("avro").save("data")

# Version 2 (added field)
df_v2 = spark.createDataFrame([
    {'id': 2, 'name': 'Bob', 'age': 30}
])
df_v2.write.format("avro").mode("append").save("data")

# Read all (schema merged automatically)
df = spark.read.format("avro").load("data")
# id:1 will have age=NULL
```

### Complex Nested Data
```python
# Avro handles nested structures natively
data = [{
    'user_id': 1,
    'profile': {'name': 'Alice', 'age': 30},
    'orders': [
        {'id': 'O1', 'amount': 100},
        {'id': 'O2', 'amount': 200}
    ]
}]

df = spark.createDataFrame(data)
df.write.format("avro").save("nested_data")
```

## 🐛 Troubleshooting

### Issue: ClassNotFoundException: AvroFileFormat

**Solution**: Add spark-avro package
```bash
spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0 script.py
```

### Issue: Schema incompatibility

**Solution**: Enable schema evolution or use union types
```python
# Read with schema evolution
df = spark.read \
    .format("avro") \
    .option("avroSchema", json_schema_string) \
    .load(path)
```

### Issue: Large file sizes

**Solution**: Use compression and partitioning
```python
df.coalesce(10) \
    .write \
    .format("avro") \
    .option("compression", "deflate") \
    .partitionBy("date") \
    .save(path)
```

## 📚 Best Practices

- [ ] Use spark-avro package (required)
- [ ] Enable compression (snappy default)
- [ ] Use schema registry for streaming
- [ ] Partition large datasets
- [ ] Plan schema evolution (add fields only)
- [ ] Use appropriate data types
- [ ] Coalesce before writing
- [ ] Monitor file sizes
- [ ] Version your schemas
- [ ] Test schema compatibility

## 🔗 Resources

- [Apache Avro Documentation](https://avro.apache.org/docs/)
- [Spark Avro Guide](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)
- [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
- [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)

---

**Perfect for**: Streaming • Kafka • Event-driven systems • Schema evolution • Data exchange