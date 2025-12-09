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

---