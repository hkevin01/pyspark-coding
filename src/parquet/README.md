# 📦 Apache Parquet Examples

Comprehensive examples for working with Apache Parquet file format in PySpark.

## 📖 What is Parquet?

Apache Parquet is a **columnar storage file format** designed for efficient data storage and retrieval. It's optimized for use with big data processing frameworks like Apache Spark.

### Key Features

- ✅ **Columnar Storage**: Stores data by column rather than by row
- ✅ **Compression**: Supports multiple codecs (Snappy, Gzip, LZO, Brotli)
- ✅ **Schema Evolution**: Add columns without rewriting existing data
- ✅ **Predicate Pushdown**: Filter data at read time
- ✅ **Column Pruning**: Read only required columns
- ✅ **Nested Data Support**: Efficient storage of complex structures

## 📂 Files in This Directory

### 01_basic_parquet_operations.py
**Beginner-friendly introduction to Parquet**

Topics covered:
- Reading and writing Parquet files
- Column pruning (projection pushdown)
- Predicate pushdown (filter pushdown)
- Compression codec comparison
- Partitioned writes
- Schema evolution

```bash
spark-submit 01_basic_parquet_operations.py
```

### 02_advanced_parquet_techniques.py
**Advanced optimization and performance tuning**

Topics covered:
- Row group size optimization
- Dictionary encoding
- Statistics and metadata
- Coalesce and repartition strategies
- Performance benchmarking
- Nested data structures

```bash
spark-submit 02_advanced_parquet_techniques.py
```

## 🚀 Quick Start

### Basic Write
```python
df.write.mode("overwrite").parquet("/path/to/output")
```

### Basic Read
```python
df = spark.read.parquet("/path/to/input")
```

### With Compression
```python
df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("/path/to/output")
```

### Partitioned Write
```python
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("/path/to/output")
```

## ⚙️ Configuration Options

### Essential Spark Configurations

```python
spark = SparkSession.builder \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.parquet.mergeSchema", "true") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()
```

### Compression Codecs

| Codec | Speed | Compression Ratio | Use Case |
|-------|-------|-------------------|----------|
| uncompressed | Fastest | 1x (no compression) | Very fast I/O required |
| snappy | Fast | 2-4x | **General purpose (recommended)** |
| gzip | Slow | 4-6x | Cold storage, maximize space savings |
| lzo | Fast | 2-3x | Similar to snappy |
| brotli | Slowest | 6-8x | Maximum compression |

**Recommendation**: Use **snappy** for most workloads.

## 📊 Performance Optimization

### 1. Column Pruning
**Read only the columns you need**

```python
# ❌ Bad: Reads all columns
df = spark.read.parquet("/data").select("col1", "col2")

# ✅ Good: Reads only needed columns
df = spark.read.parquet("/data").select("col1", "col2")
# (Parquet reader optimizes this automatically)
```

**Impact**: 70-90% reduction in I/O for selective columns

### 2. Predicate Pushdown
**Filter at read time**

```python
df = spark.read.parquet("/data") \
    .filter(col("date") >= "2024-01-01")
```

**Impact**: Skips entire row groups, can reduce data scanned by 90%+

### 3. File Sizing
**Target 128-512 MB per file**

```python
# Calculate ideal partitions
total_size_mb = 10000  # Your dataset size
target_file_size_mb = 256
ideal_partitions = total_size_mb / target_file_size_mb

df.coalesce(ideal_partitions) \
    .write.parquet("/output")
```

**Impact**: Optimal parallelism and metadata overhead

### 4. Partitioning
**Partition by commonly filtered columns**

```python
df.write \
    .partitionBy("year", "month", "day") \
    .parquet("/data")

# Query benefits from partition pruning
df = spark.read.parquet("/data") \
    .filter(col("year") == 2024)  # Only reads 2024 partition
```

**Impact**: 95%+ reduction in data scanned for time-range queries

## 🎯 When to Use Parquet

### ✅ Ideal Use Cases

1. **Analytical Queries (OLAP)**
   - Aggregations, group by operations
   - Column-heavy operations
   - Read-heavy workloads

2. **Data Lakes**
   - Long-term storage
   - Large datasets (GB to PB scale)
   - Infrequent writes, frequent reads

3. **Machine Learning**
   - Feature stores
   - Training datasets
   - Efficient column access

4. **Data Warehousing**
   - Fact and dimension tables
   - Historical data
   - Batch ETL pipelines

### ❌ Not Ideal For

1. **Row-Level Updates**
   - Parquet is immutable
   - Use Delta Lake, Iceberg, or Hudi instead

2. **Real-Time Streaming**
   - High write throughput needed
   - Consider Avro or ORC for streaming

3. **Small Datasets**
   - < 1 MB files
   - Overhead not worth it

4. **Frequent Schema Changes**
   - While supported, requires care
   - Better to stabilize schema first

## 📈 Performance Comparison

### Storage Efficiency

| Format | Size | Compression | Read Speed | Write Speed |
|--------|------|-------------|------------|-------------|
| CSV | 1000 MB | None | Slow | Fast |
| JSON | 800 MB | None | Slow | Fast |
| Parquet (snappy) | 100 MB | Good | **Very Fast** | Fast |
| Parquet (gzip) | 60 MB | **Best** | Fast | Slow |

*Results vary by dataset characteristics*

### Query Performance

**Scenario**: Select 3 of 20 columns, filter on 1 column, 10 GB dataset

| Format | Time | Data Scanned |
|--------|------|--------------|
| CSV | 120s | 10 GB |
| Parquet (no optimization) | 45s | 10 GB |
| Parquet (column pruning) | 15s | 1.5 GB |
| Parquet (pruning + pushdown) | **3s** | **0.2 GB** |

## 🔧 Troubleshooting

### Issue: Too Many Small Files

**Problem**: Metadata overhead, slow query planning

**Solution**:
```python
# Coalesce before writing
df.coalesce(10).write.parquet("/output")

# Or repartition for better distribution
df.repartition(10).write.parquet("/output")
```

### Issue: Schema Evolution Not Working

**Problem**: Reading fails after schema changes

**Solution**:
```python
# Enable schema merging
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("/data")
```

### Issue: Poor Query Performance

**Problem**: Full table scans

**Solutions**:
1. Enable predicate pushdown: `spark.sql.parquet.filterPushdown=true`
2. Partition data logically
3. Use column pruning (select specific columns)
4. Check file sizes (128-512 MB ideal)

### Issue: Out of Memory

**Problem**: Reading large Parquet files

**Solutions**:
1. Reduce row group size: `parquet.block.size=67108864` (64 MB)
2. Increase executor memory
3. Use column pruning to read fewer columns
4. Repartition data before writing

## 📚 Best Practices Checklist

- [ ] Use snappy compression (general purpose)
- [ ] Enable vectorized reader
- [ ] Enable predicate pushdown
- [ ] Target 128-512 MB file sizes
- [ ] Partition large datasets logically
- [ ] Use column pruning in queries
- [ ] Monitor and tune row group sizes
- [ ] Plan schema evolution carefully
- [ ] Coalesce before writing
- [ ] Use appropriate data types

## 🔗 Related Formats

- **ORC**: Similar columnar format, optimized for Hive
- **Avro**: Row-based, better for streaming
- **Delta Lake**: Parquet + ACID transactions
- **Iceberg**: Parquet + table format for data lakes

## 📖 Additional Resources

- [Apache Parquet Documentation](https://parquet.apache.org/)
- [Parquet File Format Specification](https://github.com/apache/parquet-format)
- [Spark Parquet Guide](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Parquet Benchmarks](https://github.com/apache/parquet-testing)

## 🎓 Learning Path

1. **Start**: `01_basic_parquet_operations.py` - Learn fundamentals
2. **Practice**: Experiment with your own datasets
3. **Advance**: `02_advanced_parquet_techniques.py` - Optimization
4. **Apply**: Use in production pipelines with proper tuning

---

**Next Steps**: Try running the examples and experiment with different configurations!
