## Quick Decision Matrix

```
Data Size Decision:
├─ < 10 GB       → Pandas
├─ 10 GB - 100 GB → Pandas (if RAM allows) or PySpark
├─ 100 GB - 10 TB → PySpark
└─ > 10 TB       → PySpark definitely

Processing Type:
├─ Batch         → PySpark, Hadoop MapReduce
├─ Interactive   → PySpark, Presto/Trino
├─ Streaming     → PySpark (micro-batch), Flink (real-time)
├─ ML/Analytics  → PySpark
└─ Transactions  → PostgreSQL, MySQL

Latency Requirements:
├─ Minutes-Hours → PySpark, Hadoop
├─ Seconds       → PySpark Streaming
├─ Milliseconds  → Apache Flink, Kafka Streams
└─ Microseconds  → In-memory DB, Redis
```
### 📊 Enhanced Decision Tree

```
Need to process data?
│
├─ Data < 10GB?
│  ├─ YES → Use Pandas ✅
│  │  ├─ Need SQL queries?
│  │  │  └─ YES → Use Pandas + SQLite
│  │  ├─ Need ML inference?
│  │  │  └─ YES → Use Pandas + PyTorch/sklearn
│  │  └─ Need visualizations?
│  │     └─ YES → Add Matplotlib/Plotly
│  
└─ Data > 10GB?
   └─ YES → Use PySpark ✅
      │
      ├─── Processing Mode?
      │    │
      │    ├─ Batch Processing?
      │    │  └─ YES → PySpark DataFrame API
      │    │     ├─ Complex transformations? → Use UDFs
      │    │     ├─ SQL-heavy logic? → Use Spark SQL
      │    │     └─ Need checkpointing? → Enable `.checkpoint()`
      │    │
      │    └─ Stream Processing?
      │       └─ YES → PySpark Structured Streaming ✅
      │          │
      │          ├─── Data Source?
      │          │    ├─ Kafka → Use `.format("kafka")`
      │          │    ├─ File system → Use `.format("csv/json/parquet")`
      │          │    ├─ Socket → Use `.format("socket")`
      │          │    └─ Rate source (testing) → Use `.format("rate")`
      │          │
      │          ├─── Processing Requirements?
      │          │    ├─ Stateless transformations → Use `.select()`, `.filter()`
      │          │    ├─ Stateful operations → Use `.groupBy().agg()`
      │          │    ├─ Windowed aggregations → Use `.window()` or `.groupBy(window())`
      │          │    ├─ Watermarking needed? → Use `.withWatermark()`
      │          │    └─ Join streams? → Use `.join()` with watermarks
      │          │
      │          ├─── Output Sink?
      │          │    ├─ Console (debug) → `.writeStream.format("console")`
      │          │    ├─ File system → `.format("parquet/csv/json")`
      │          │    ├─ Kafka → `.format("kafka")`
      │          │    ├─ Database → `.foreach()` or `.foreachBatch()`
      │          │    └─ Memory (testing) → `.format("memory")`
      │          │
      │          └─── Delivery Semantics?
      │               ├─ At-least-once → Default (no idempotency)
      │               ├─ Exactly-once → Enable checkpointing + idempotent writes
      │               └─ At-most-once → No checkpointing (not recommended)
      │
      ├─── Need ML Predictions?
      │    └─ YES → Add PyTorch/TensorFlow ✅
      │       │
      │       ├─── Deployment Strategy?
      │       │    │
      │       │    ├─ In-Database Inference?
      │       │    │  └─ YES → Use UDFs for Inference ✅
      │       │    │     │
      │       │    │     ├─ PySpark → Pandas UDF with model broadcasting
      │       │    │     ├─ PostgreSQL → PL/Python UDF
      │       │    │     ├─ BigQuery → Remote Functions + Cloud Functions
      │       │    │     └─ Snowflake → Python UDF with packages
      │       │    │
      │       │    ├─ Batch Scoring?
      │       │    │  └─ Use `.mapPartitions()` with model loading
      │       │    │
      │       │    └─ Real-time Inference?
      │       │       └─ Use UDF in Streaming query
      │       │
      │       └─── Model Optimization?
      │            ├─ Broadcasting → `sc.broadcast(model)` (critical!)
      │            ├─ Batch processing → Process partitions, not rows
      │            ├─ GPU support → Use `.cuda()` in UDF
      │            └─ Model caching → Load once per executor
      │
      ├─── Data Format?
      │    ├─ CSV → `.read.csv()`
      │    ├─ JSON → `.read.json()`
      │    ├─ Parquet → `.read.parquet()` (recommended for big data)
      │    ├─ Avro → `.read.format("avro")`
      │    ├─ Delta Lake → `.read.format("delta")`
      │    └─ Database → `.read.jdbc()` or UDFs
      │
      ├─── Performance Optimization?
      │    ├─ Partitioning → `.repartition()` or `.coalesce()`
      │    ├─ Caching → `.cache()` or `.persist()`
      │    ├─ Broadcasting → `broadcast()` for small tables in joins
      │    ├─ Columnar storage → Use Parquet
      │    └─ Predicate pushdown → Filter early with `.filter()`
      │
      └─── Fault Tolerance?
           ├─ Checkpointing → `.checkpoint()` for lineage truncation
           ├─ Write-ahead logs → Enable for streaming
           ├─ Retry logic → Configure `spark.task.maxFailures`
           └─ Graceful shutdown → Handle SIGTERM signals

```

### 🎯 Streaming-Specific Decision Path

```
Need PySpark Streaming?
│
├─── Step 1: Choose Trigger Mode
│    ├─ ProcessingTime → `.trigger(processingTime='10 seconds')`
│    ├─ Once → `.trigger(once=True)` (batch-like)
│    ├─ Continuous → `.trigger(continuous='1 second')` (experimental)
│    └─ AvailableNow → `.trigger(availableNow=True)` (Spark 3.3+)
│
├─── Step 2: Configure Checkpointing (Required!)
│    └─ `.option("checkpointLocation", "s3://bucket/checkpoints")`
│       ├─ HDFS → High reliability
│       ├─ S3 → Cloud storage
│       └─ Local → Testing only (not for production)
│
├─── Step 3: Handle State Management
│    ├─ Stateless → Direct transformations (map, filter)
│    ├─ Stateful → Aggregations, joins, windowing
│    │  ├─ State timeout → `.withWatermark("timestamp", "10 minutes")`
│    │  └─ State store → Automatic, but monitor size
│    └─ Custom state → `.mapGroupsWithState()` or `.flatMapGroupsWithState()`
│
├─── Step 4: Implement Watermarking (for Late Data)
│    └─ `.withWatermark("event_time", "10 minutes")`
│       ├─ Defines max lateness allowed
│       ├─ Enables state cleanup
│       └─ Required for outer joins
│
├─── Step 5: Configure Output Mode
│    ├─ Append → Only new rows (default for stateless)
│    ├─ Complete → Entire result table (aggregations only)
│    └─ Update → Only changed rows (stateful operations)
│
└─── Step 6: Monitor & Scale
     ├─ Monitor lag → Check input vs processing rate
     ├─ Scale executors → Increase based on throughput
     ├─ Tune batch size → Balance latency vs throughput
     └─ Track state size → Prevent memory overflow

```

### 🚀 UDF Decision Matrix

```
Need to extend PySpark functionality?
│
├─── Basic Transformations?
│    └─ NO UDF needed → Use built-in functions (faster)
│
├─── Complex Python Logic?
│    └─ YES → Choose UDF Type:
│       │
│       ├─ Row-by-row processing?
│       │  └─ Standard UDF → `@udf(returnType)`
│       │     ⚠️ Slow: Serialization overhead
│       │
│       ├─ Vectorized operations?
│       │  └─ Pandas UDF → `@pandas_udf(returnType)`
│       │     ✅ Fast: Columnar processing with Arrow
│       │     │
│       │     ├─ SCALAR → Transform columns
│       │     ├─ GROUPED_MAP → Custom aggregations per group
│       │     ├─ GROUPED_AGG → Aggregate with Pandas
│       │     └─ SCALAR_ITER → Process batches (ML inference)
│       │
│       └─ ML Model Inference?
│          └─ YES → Use Pandas UDF with Broadcasting ✅
│             │
│             ├─ Step 1: Broadcast model
│             │  └─ `broadcast_model = sc.broadcast(model)`
│             │
│             ├─ Step 2: Create Pandas UDF
│             │  └─ `@pandas_udf(FloatType())`
│             │     def predict(features: pd.Series) -> pd.Series:
│             │         model = broadcast_model.value
│             │         return model.predict(features)
│             │
│             ├─ Step 3: Apply to DataFrame
│             │  └─ `df.withColumn("prediction", predict(col("features")))`
│             │
│             └─ Performance Tips:
│                ├─ Use SCALAR_ITER for large models
│                ├─ Batch predictions for GPU efficiency
│                ├─ Repartition data for balanced load
│                └─ Monitor executor memory usage

```

### 💾 Data Sink Decision Tree

```
Where to write results?
│
├─── Development/Testing?
│    ├─ Console → `.writeStream.format("console").start()`
│    └─ Memory → `.writeStream.format("memory").queryName("table").start()`
│
├─── Production Storage?
│    ├─ File System (HDFS/S3)?
│    │  ├─ Parquet → `.format("parquet")` (recommended)
│    │  ├─ JSON → `.format("json")`
│    │  ├─ CSV → `.format("csv")`
│    │  └─ Delta Lake → `.format("delta")` (ACID transactions)
│    │
│    ├─ Message Queue?
│    │  └─ Kafka → `.format("kafka").option("topic", "output")`
│    │
│    ├─ Database?
│    │  ├─ JDBC → Use `.foreachBatch()` with JDBC write
│    │  ├─ NoSQL → Use `.foreach()` with custom writer
│    │  └─ UDF in DB → Create table, then use SQL UDF for inference
│    │
│    └─ Custom Logic?
│       ├─ `.foreach()` → Row-by-row (slow)
│       └─ `.foreachBatch()` → Batch processing (recommended)
│          └─ def write_batch(df, epoch_id):
│              # Custom write logic (JDBC, API calls, etc.)
│              df.write.jdbc(...)

```

### ⚡ Quick Reference: When to Use What

| Scenario | Tool/Pattern | Why |
|----------|-------------|-----|
| **Data < 10GB** | Pandas | Faster for small data, simpler API |
| **Data 10GB - 1TB** | PySpark | Distributed processing, handles OOM |
| **Data > 1TB** | PySpark + Parquet | Columnar storage, predicate pushdown |
| **ML Inference in PySpark** | Pandas UDF + Broadcasting | Vectorized, efficient model reuse |
| **ML Inference in PostgreSQL** | PL/Python UDF | In-database processing, no data export |
| **ML Inference in BigQuery** | Remote Functions | Serverless, scalable to petabytes |
| **Real-time Streaming** | PySpark Streaming + Kafka | Micro-batch, exactly-once semantics |
| **Sub-second Latency** | Flink or Kafka Streams | True real-time, event-by-event |
| **Complex Aggregations** | Window functions + Watermark | Handle late data, state management |
| **Large Model Inference** | SCALAR_ITER Pandas UDF | Batch processing, GPU support |
| **Join Streams** | Stream-Stream Join + Watermark | Required for bounded state |
| **Deduplication** | `.dropDuplicates()` + Watermark | Stateful, memory-efficient |
| **Checkpointing** | HDFS/S3 location | Fault tolerance, exactly-once |
| **Testing Streaming** | Memory sink + rate source | Fast iteration, no external deps |

---

## 📊 Visual Decision Flow Diagrams

### Main Decision Flow

```mermaid
flowchart TD
    Start([Need to Process Data?]) --> DataSize{Data Size?}
    
    DataSize -->|< 10GB| Pandas[Use Pandas]
    DataSize -->|> 10GB| PySpark[Use PySpark]
    
    Pandas --> PandasML{Need ML?}
    PandasML -->|Yes| PandasTorch[Pandas + PyTorch/sklearn]
    PandasML -->|No| PandasSQL{Need SQL?}
    PandasSQL -->|Yes| SQLite[Pandas + SQLite]
    PandasSQL -->|No| PandasDone[Done]
    
    PySpark --> ProcessMode{Processing Mode?}
    
    ProcessMode -->|Batch| BatchAPI[PySpark DataFrame API]
    ProcessMode -->|Stream| StreamAPI[PySpark Structured Streaming]
    
    BatchAPI --> BatchML{Need ML?}
    BatchML -->|Yes| UDFInference[Use Pandas UDF + Broadcasting]
    BatchML -->|No| BatchDone[Done]
    
    StreamAPI --> StreamSource{Data Source?}
    StreamSource -->|Kafka| KafkaConfig[Configure Kafka Source]
    StreamSource -->|Files| FileConfig[Configure File Source]
    StreamSource -->|Socket| SocketConfig[Configure Socket Source]
    
    KafkaConfig --> StreamML{Need ML?}
    FileConfig --> StreamML
    SocketConfig --> StreamML
    
    StreamML -->|Yes| StreamUDF[Use UDF in Stream Query]
    StreamML -->|No| StreamSink{Output Sink?}
    
    StreamUDF --> StreamSink
    StreamSink -->|Kafka| KafkaSink[Write to Kafka]
    StreamSink -->|Files| FileSink[Write to Files]
    StreamSink -->|Database| DBSink[Write to Database]
    StreamSink -->|Console| ConsoleSink[Console Output]
    
    UDFInference --> Checkpoint1{Need Fault Tolerance?}
    Checkpoint1 -->|Yes| EnableCheckpoint[Enable Checkpointing]
    Checkpoint1 -->|No| BatchDone
    
    KafkaSink --> Checkpoint2{Need Exactly-Once?}
    FileSink --> Checkpoint2
    DBSink --> Checkpoint2
    ConsoleSink --> StreamDone[Done]
    
    Checkpoint2 -->|Yes| ExactlyOnce[Enable Checkpointing + Idempotent Writes]
    Checkpoint2 -->|No| StreamDone
    
    ExactlyOnce --> StreamDone
    EnableCheckpoint --> BatchDone
    
    style Start fill:#2d3748,stroke:#4a5568,stroke-width:2px,color:#fff
    style Pandas fill:#2d3748,stroke:#48bb78,stroke-width:2px,color:#fff
    style PySpark fill:#2d3748,stroke:#4299e1,stroke-width:2px,color:#fff
    style UDFInference fill:#2d3748,stroke:#ed8936,stroke-width:2px,color:#fff
    style StreamUDF fill:#2d3748,stroke:#ed8936,stroke-width:2px,color:#fff
    style BatchAPI fill:#2d3748,stroke:#4299e1,stroke-width:2px,color:#fff
    style StreamAPI fill:#2d3748,stroke:#9f7aea,stroke-width:2px,color:#fff
    style ExactlyOnce fill:#2d3748,stroke:#48bb78,stroke-width:2px,color:#fff
```

### PySpark Streaming Architecture Flow

```mermaid
flowchart LR
    subgraph Sources["📥 Data Sources"]
        Kafka[Kafka Topics]
        Files[File System]
        Socket[Socket Stream]
        Rate[Rate Source]
    end
    
    subgraph Processing["⚙️ Stream Processing"]
        Read[readStream API]
        Transform[Transformations]
        UDF[Pandas UDF<br/>ML Inference]
        Window[Window Functions]
        Watermark[Watermarking]
        State[State Management]
    end
    
    subgraph Sinks["📤 Output Sinks"]
        KafkaOut[Kafka Topics]
        FilesOut[Parquet/JSON/CSV]
        DB[(Database)]
        Console[Console Debug]
        Memory[Memory Table]
    end
    
    subgraph FaultTolerance["🛡️ Fault Tolerance"]
        Checkpoint[Checkpointing]
        WAL[Write-Ahead Log]
        Idempotent[Idempotent Writes]
    end
    
    Kafka --> Read
    Files --> Read
    Socket --> Read
    Rate --> Read
    
    Read --> Transform
    Transform --> UDF
    Transform --> Window
    Window --> Watermark
    Watermark --> State
    UDF --> State
    
    State --> KafkaOut
    State --> FilesOut
    State --> DB
    State --> Console
    State --> Memory
    
    KafkaOut --> Checkpoint
    FilesOut --> Checkpoint
    DB --> Checkpoint
    
    Checkpoint --> WAL
    WAL --> Idempotent
    
    style Kafka fill:#2d3748,stroke:#4299e1,stroke-width:2px,color:#fff
    style Files fill:#2d3748,stroke:#48bb78,stroke-width:2px,color:#fff
    style Socket fill:#2d3748,stroke:#9f7aea,stroke-width:2px,color:#fff
    style UDF fill:#2d3748,stroke:#ed8936,stroke-width:2px,color:#fff
    style Checkpoint fill:#2d3748,stroke:#f56565,stroke-width:2px,color:#fff
    style DB fill:#2d3748,stroke:#4299e1,stroke-width:2px,color:#fff
```

### UDF Inference Decision Flow

```mermaid
flowchart TD
    Start([Need ML Inference?]) --> Deploy{Deployment Strategy?}
    
    Deploy -->|In-Database| DBInfer[UDF for Inference]
    Deploy -->|Batch Scoring| BatchScore[mapPartitions]
    Deploy -->|Real-time Stream| StreamInfer[UDF in Stream Query]
    
    DBInfer --> Platform{Platform?}
    Platform -->|PySpark| PySparkUDF[Pandas UDF + Broadcasting]
    Platform -->|PostgreSQL| PostgresUDF[PL/Python UDF]
    Platform -->|BigQuery| BQRemote[Remote Functions]
    Platform -->|Snowflake| SnowflakeUDF[Python UDF]
    
    PySparkUDF --> Broadcast[1. Broadcast Model]
    Broadcast --> CreateUDF[2. Create Pandas UDF]
    CreateUDF --> Apply[3. Apply to DataFrame]
    Apply --> Optimize{Optimize?}
    
    Optimize -->|Large Model| ScalarIter[Use SCALAR_ITER]
    Optimize -->|GPU| GPUBatch[Batch + .cuda]
    Optimize -->|Load Balance| Repartition[Repartition Data]
    
    PostgresUDF --> PLPython[Define PL/Python Function]
    PLPython --> SQLQuery[Use in SQL Query]
    
    BQRemote --> CloudFunc[Deploy Cloud Function]
    CloudFunc --> BQConnect[Create Remote Function]
    BQConnect --> SQLQuery
    
    SnowflakeUDF --> SnowPython[Define Python UDF]
    SnowPython --> SnowPackages[Specify Packages]
    SnowPackages --> SQLQuery
    
    BatchScore --> LoadModel[Load Model per Partition]
    LoadModel --> ProcessBatch[Process Entire Partition]
    
    StreamInfer --> StreamBroadcast[Broadcast Model]
    StreamBroadcast --> StreamUDF[Apply UDF in Stream]
    StreamUDF --> Monitor[Monitor Lag & Throughput]
    
    SQLQuery --> Done[Done]
    ScalarIter --> Done
    GPUBatch --> Done
    Repartition --> Done
    ProcessBatch --> Done
    Monitor --> Done
    
    style Start fill:#2d3748,stroke:#4a5568,stroke-width:2px,color:#fff
    style DBInfer fill:#2d3748,stroke:#ed8936,stroke-width:2px,color:#fff
    style PySparkUDF fill:#2d3748,stroke:#4299e1,stroke-width:2px,color:#fff
    style Broadcast fill:#2d3748,stroke:#48bb78,stroke-width:2px,color:#fff
    style CloudFunc fill:#2d3748,stroke:#9f7aea,stroke-width:2px,color:#fff
    style Monitor fill:#2d3748,stroke:#f56565,stroke-width:2px,color:#fff
```

### Streaming Trigger & Checkpoint Configuration

```mermaid
flowchart TD
    Start([Configure Streaming Query]) --> Trigger{Choose Trigger Mode}
    
    Trigger -->|ProcessingTime| PT[trigger processingTime='10s']
    Trigger -->|Once| Once[trigger once=True]
    Trigger -->|Continuous| Cont[trigger continuous='1s']
    Trigger -->|AvailableNow| AN[trigger availableNow=True]
    
    PT --> Checkpoint[Configure Checkpointing]
    Once --> Checkpoint
    Cont --> Checkpoint
    AN --> Checkpoint
    
    Checkpoint --> Location{Checkpoint Location?}
    Location -->|HDFS| HDFS[checkpointLocation: hdfs://]
    Location -->|S3| S3[checkpointLocation: s3://]
    Location -->|Local| Local[checkpointLocation: file://]
    
    HDFS --> State{State Management?}
    S3 --> State
    Local --> State
    
    State -->|Stateless| Stateless[Direct Transformations]
    State -->|Stateful| Stateful[Aggregations/Joins]
    
    Stateful --> Watermark[Configure Watermark]
    Watermark --> WMTime[withWatermark event_time 10 minutes]
    
    WMTime --> OutputMode{Output Mode?}
    Stateless --> OutputMode
    
    OutputMode -->|Append| Append[Append: New Rows Only]
    OutputMode -->|Complete| Complete[Complete: Full Table]
    OutputMode -->|Update| Update[Update: Changed Rows]
    
    Append --> Sink[Configure Output Sink]
    Complete --> Sink
    Update --> Sink
    
    Sink --> Monitor[Monitor & Scale]
    Monitor --> Metrics{Check Metrics}
    
    Metrics -->|High Lag| Scale[Scale Executors]
    Metrics -->|Memory Issues| StateSize[Check State Size]
    Metrics -->|Low Throughput| Tune[Tune Batch Size]
    
    Scale --> Done[Start Streaming Query]
    StateSize --> Done
    Tune --> Done
    
    style Start fill:#2d3748,stroke:#4a5568,stroke-width:2px,color:#fff
    style Checkpoint fill:#2d3748,stroke:#ed8936,stroke-width:2px,color:#fff
    style Watermark fill:#2d3748,stroke:#4299e1,stroke-width:2px,color:#fff
    style Monitor fill:#2d3748,stroke:#48bb78,stroke-width:2px,color:#fff
    style Done fill:#2d3748,stroke:#9f7aea,stroke-width:2px,color:#fff
```

---

## 🔧 Configuration Cheat Sheet

### PySpark Streaming Minimal Setup

```python
# 1. Read from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input-topic") \
    .load()

# 2. Transform (with ML inference)
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

# Broadcast model
broadcast_model = spark.sparkContext.broadcast(trained_model)

@pandas_udf(FloatType())
def predict_udf(features: pd.Series) -> pd.Series:
    model = broadcast_model.value
    return pd.Series(model.predict(features.values.reshape(-1, 1)))

# Apply transformations
result_df = stream_df \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("prediction", predict_udf(col("features")))

# 3. Write to Kafka with checkpointing
query = result_df \
    .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-topic") \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .trigger(processingTime='10 seconds') \
    .start()

query.awaitTermination()
```

### Watermarking Example

```python
# Handle late data with watermarking
windowed_df = stream_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("device_id")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        max("temperature").alias("max_temp"),
        count("*").alias("record_count")
    )
```

### Stream-Stream Join with Watermark

```python
# Join two streams with watermarks
stream1 = stream1_df.withWatermark("timestamp", "10 minutes")
stream2 = stream2_df.withWatermark("timestamp", "15 minutes")

joined = stream1.join(
    stream2,
    expr("""
        stream1.id = stream2.id AND
        stream1.timestamp >= stream2.timestamp AND
        stream1.timestamp <= stream2.timestamp + interval 5 minutes
    """)
)
```

---

## 📈 Performance Comparison

| Operation | Standard UDF | Pandas UDF | Pandas UDF + Broadcasting | Built-in Functions |
|-----------|-------------|------------|--------------------------|-------------------|
| **Throughput** | 1x (baseline) | 10-50x | 50-100x | 100-1000x |
| **Serialization** | Row-by-row | Columnar (Arrow) | Columnar (Arrow) | Native |
| **Model Loading** | Per row | Per partition | Once per executor | N/A |
| **Best For** | Simple logic | Vectorized ops | ML inference | SQL operations |
| **GPU Support** | ❌ | ✅ | ✅ | ❌ |

### Streaming Latency Targets

| Trigger Mode | Latency | Use Case |
|--------------|---------|----------|
| **ProcessingTime='1s'** | ~1-2 seconds | Near real-time dashboards |
| **ProcessingTime='10s'** | ~10-15 seconds | Standard streaming ETL |
| **ProcessingTime='1m'** | ~1-2 minutes | Aggregated analytics |
| **Once** | Batch-like | Catch-up processing |
| **Continuous** | < 1 second | Experimental, low latency |

---

## 🎯 Best Practices Summary

### ✅ DO

- **Use Pandas UDF** for ML inference (50-100x faster than standard UDF)
- **Broadcast models** to executors (critical for performance)
- **Enable checkpointing** for streaming (fault tolerance, exactly-once)
- **Use watermarking** for stateful operations (prevents unbounded state)
- **Repartition data** for load balancing
- **Monitor lag** in streaming queries
- **Use Parquet** for storage (columnar, compressed)
- **Filter early** with predicate pushdown
- **Test with memory sink** before production

### ❌ DON'T

- **Don't use standard UDF** for ML inference (too slow)
- **Don't load model per row** (use broadcasting)
- **Don't skip checkpointing** in production streaming
- **Don't ignore late data** (configure watermarks)
- **Don't use CSV** for big data (use Parquet)
- **Don't forget to `.stop()`** streaming queries
- **Don't over-partition** (more partitions = more overhead)
- **Don't cache everything** (monitor memory usage)