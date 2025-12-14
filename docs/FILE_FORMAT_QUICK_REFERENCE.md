# File Format Quick Reference Guide

## TL;DR - Decision Tree

```
Need ACID (UPDATE/DELETE)?
├─ YES → Delta Lake
└─ NO
   ├─ Using Hive? → ORC
   ├─ Kafka streaming? → Avro
   ├─ API integration? → JSON
   ├─ Scientific arrays? → HDF5
   └─ Default analytics → Parquet
```

## Format Comparison Matrix

| Feature | Delta Lake | Parquet | ORC | Avro | JSON | HDF5 |
|---------|-----------|---------|-----|------|------|------|
| **ACID Transactions** | ✅ | ❌ | ⚠️* | ❌ | ❌ | ❌ |
| **UPDATE/DELETE** | ✅ | ❌ | ⚠️* | ❌ | ❌ | ❌ |
| **Time Travel** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Columnar Storage** | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Schema Evolution** | ✅ | ⚠️ | ⚠️ | ✅ | ✅ | ⚠️ |
| **Compression** | Excellent | Excellent | Best | Good | Poor | Good |
| **Human Readable** | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **Spark Performance** | Excellent | Excellent | Excellent | Good | Poor | Poor** |
| **Concurrent Writes** | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| **Array Slicing** | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ |

\* ORC ACID only in Hive context  
\** HDF5 designed for single-machine use

## 1. Delta Lake

### Key Concept
**Parquet + Transaction Log = ACID**

### Operations Parquet Can't Do
```python
# UPDATE (impossible in Parquet)
delta_table.update(
    condition = "salary < 50000",
    set = {"salary": col("salary") * 1.1}
)

# DELETE (impossible in Parquet)
delta_table.delete("status = 'inactive'")

# MERGE/UPSERT (impossible in Parquet)
delta_table.merge(updates, "id = id")
    .whenMatchedUpdate(...)
    .whenNotMatchedInsert(...)
    .execute()

# TIME TRAVEL (impossible in Parquet)
df = spark.read.format("delta").option("versionAsOf", 5).load(path)
```

### Storage Overhead
- Parquet: 600 MB
- Delta Lake: 600 MB + 4 KB transaction log = **0.001% overhead**

### Use When
- ✅ Need UPDATE/DELETE operations
- ✅ Multiple concurrent writers
- ✅ Need audit trail (compliance)
- ✅ CDC (Change Data Capture)
- ✅ Real-time analytics with updates

### Don't Use When
- ❌ Write-once, read-many (use Parquet)
- ❌ Maximum portability needed (use Parquet)
- ❌ Avoid dependencies (use Parquet)

## 2. Parquet

### Key Concept
**Columnar storage optimized for analytics**

### Best For
- ✅ Immutable data (logs, archives)
- ✅ Analytics queries (SELECT, WHERE, GROUP BY)
- ✅ Maximum portability (works everywhere)
- ✅ Single writer scenarios

### Performance
```
SELECT AVG(salary) WHERE department = 'Engineering'

Parquet: Only reads 'salary' and 'department' columns
         10× faster than row-based formats
```

### Limitations
- ❌ Can't UPDATE rows (must rewrite entire file)
- ❌ Can't DELETE rows (must rewrite entire file)
- ❌ No ACID guarantees
- ❌ Race conditions with concurrent writers

## 3. ORC (Hive Ecosystem)

### Key Concept
**Parquet-like format optimized for Hive**

### Hive Architecture
```
┌──────────────────────────────────────┐
│ Hive Metastore (Schema Registry)    │
├──────────────────────────────────────┤
│ HiveQL (SQL-like queries)            │
├──────────────────────────────────────┤
│ Execution Engine (Tez/Spark)         │
├──────────────────────────────────────┤
│ Storage (HDFS + ORC files)           │
└──────────────────────────────────────┘
```

### ORC vs Parquet
- **Compression**: ORC ~10% better (ZLIB vs Snappy)
- **Ecosystem**: ORC = Hive, Parquet = Universal
- **Performance**: Similar for most queries
- **ACID**: ORC supports in Hive only

### Use When
- ✅ Hive data warehouse
- ✅ Hadoop ecosystem (HDFS, YARN)
- ✅ Need slightly better compression

### Don't Use When
- ❌ Cloud-native (use Parquet/Delta)
- ❌ Non-Hive tools (use Parquet)

## 4. Avro (Schema Evolution)

### Key Concept
**Row-based binary format with embedded schema**

### Schema Evolution Example
```
Version 1 (January):
{"id": 1, "name": "Alice", "age": 30}

Version 2 (March):
{"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"}

Read both together:
✅ V1 records: email = null
✅ V2 records: all fields present
✅ No reprocessing needed!
```

### Why Schema Changes Are Frequent
1. **API Evolution**: V1 → V2 adds new fields
2. **Business Requirements**: "Track customer age now"
3. **Agile Development**: Weekly feature releases
4. **Data Integration**: New sources with extra fields
5. **ML Evolution**: New features for model V2

### Kafka Integration
```
Producer → Schema Registry → Kafka → Consumer
    ↓            ↓                       ↓
Register V1  Store V1/V2         Read with either
schema       versions            compatible schema
```

### Use When
- ✅ Kafka message serialization (industry standard)
- ✅ Schema changes frequently
- ✅ RPC/messaging systems

### Don't Use When
- ❌ Analytics (use Parquet - better compression)
- ❌ Rare schema changes (use Parquet)

## 5. JSON (Nested/Hierarchical Data)

### Key Concept
**Human-readable text format for nested structures**

### Nested Data
```json
{
  "user": {
    "name": "Alice",
    "address": {
      "city": "NYC",
      "coordinates": {
        "lat": 40.7128,
        "lon": -74.0060
      }
    }
  }
}
```
Access: `user.address.city = "NYC"`

### Hierarchical Data (Tree Structure)
```
Organization
├── Engineering
│   ├── Backend
│   │   ├── Alice
│   │   └── Bob
│   └── Frontend
│       └── Charlie
└── Sales
    └── Enterprise
        └── Diana
```

### Real-World Examples
- API responses (REST APIs)
- E-commerce orders with items arrays
- Application logs with context
- Configuration files

### Size Comparison
- JSON: 1.2 MB (text, uncompressed)
- Parquet: 150 KB (binary, compressed)
- **Parquet is 8× smaller!**

### Use When
- ✅ API integration
- ✅ Application logs
- ✅ Semi-structured data
- ✅ Human readability important

### Don't Use When
- ❌ Production data lakes (use Parquet)
- ❌ Large datasets (storage expensive)
- ❌ Analytics queries (slow)

## 6. HDF5 (Scientific Arrays)

### Key Concept
**N-dimensional arrays for scientific computing**

### Array Types

**1D Array (Vector):**
```
temperature = [72.5, 73.1, 71.8, 74.2]
temperature[2] = 71.8
```

**2D Array (Matrix):**
```
image[row, col] = pixel_value
image[1, 2] = 160
```

**3D Array (Spatial + Time):**
```
temperature[latitude, longitude, time]
Shape: (1000, 2000, 365)
Access: temp[500, 1000, 180] = temperature at point on day 180
```

**4D Array (Spatial + Time + Variable):**
```
climate[lat, lon, time, var]
var = [temperature, humidity, pressure, wind_speed]
```

### Real-World Examples
- **Medical**: MRI scans (256×256×128)
- **Astronomy**: Sky images (4096×4096×100×1000)
- **Climate**: Ocean temp (100×180×360×3650)
- **Genomics**: Gene expression (20K×10K×50)

### HDF5 vs Parquet
```
Get time series at one location:
HDF5:    temp[500, 1000, :] → Fast (single slice)
Parquet: WHERE lat=500 AND lon=1000 → Slow (full scan)

Get global average:
HDF5:    np.mean(temp) → Slow (not optimized)
Parquet: SELECT AVG(temp) → Fast (columnar)
```

### Use When
- ✅ Scientific computing (NumPy workflows)
- ✅ Multi-dimensional arrays
- ✅ Array slicing operations
- ✅ Single-machine analysis

### Don't Use When
- ❌ Distributed analytics (use Parquet)
- ❌ Spark processing (limited support)
- ❌ Tabular data (use Parquet)

## Quick Decision Flowchart

```
┌─────────────────────────────────────────────┐
│ What's your primary use case?              │
└─────────────────────────────────────────────┘
                  ↓
        ┌─────────┴─────────┐
        ↓                   ↓
   Need ACID?          No ACID needed
   (UPDATE/DELETE)          ↓
        ↓              ┌────┴────┐
   Delta Lake          ↓         ↓
                   Analytics  Streaming
                       ↓         ↓
                   ┌───┴───┐    Avro
                   ↓       ↓    (Kafka)
              Using Hive?  APIs?
                   ↓       ↓
                  ORC    JSON
                   
              Default: Parquet
              (works everywhere)
```

## Storage Cost Comparison

**Same 100 GB dataset:**

| Format | Size | Cost (S3 @ $0.023/GB) | Compression |
|--------|------|----------------------|-------------|
| JSON | 100 GB | $2.30/month | None |
| Avro | 35 GB | $0.81/month | Good |
| Parquet | 12 GB | $0.28/month | Excellent |
| ORC | 11 GB | $0.25/month | Best |
| Delta Lake | 12.001 GB | $0.28/month | Same as Parquet |
| HDF5 | 30 GB | $0.69/month | Good |

**Savings: Parquet vs JSON = $2.02/month = $24/year per 100 GB**

## Performance Comparison

**Query: SELECT AVG(salary) WHERE department = 'Engineering'**

| Format | Time | Explanation |
|--------|------|-------------|
| Parquet | 0.5s | Columnar: Only reads 2 columns |
| ORC | 0.5s | Columnar: Similar to Parquet |
| Delta Lake | 0.5s | Uses Parquet underneath |
| Avro | 2.5s | Row-based: Must scan all data |
| JSON | 5.0s | Text parsing + full scan |
| HDF5 | N/A | Not optimized for SQL queries |

## Common Mistakes

### ❌ Using JSON for production data lakes
**Problem**: 10× larger files, slow queries  
**Solution**: Use Parquet, convert JSON on ingestion

### ❌ Using Parquet when you need UPDATE/DELETE
**Problem**: Must rewrite entire file, not atomic  
**Solution**: Use Delta Lake (same storage, adds ACID)

### ❌ Using HDF5 with Spark
**Problem**: Poor integration, single-machine focused  
**Solution**: Convert to Parquet for distributed processing

### ❌ Using ORC outside Hive ecosystem
**Problem**: Limited tool support  
**Solution**: Use Parquet for universal compatibility

### ❌ Ignoring schema evolution
**Problem**: $50K reprocessing cost when schema changes  
**Solution**: Use Avro (streaming) or Delta Lake (batch)

## Best Practices

### 1. Start with Parquet
- Universal compatibility
- Excellent compression
- Fast analytics
- Upgrade to Delta Lake when you need ACID

### 2. Use Delta Lake for mutable data
- Customer databases
- Inventory systems
- Financial transactions
- Any data that needs UPDATE/DELETE

### 3. Use Avro for streaming
- Kafka message serialization
- Schema registry integration
- Frequent schema changes

### 4. Avoid JSON in production
- OK for APIs and logs
- Convert to Parquet for storage
- 10× storage savings

### 5. Choose ORC only for Hive
- Slight compression advantage
- Hive-specific optimizations
- Use Parquet for cloud-native

## Real-World Example: E-commerce Platform

```
Data Pipeline Design:

1. Raw Events (API) → JSON
   - Real-time ingestion
   - Human-readable logs
   
2. Streaming Pipeline → Avro
   - Kafka messages
   - Schema evolution support
   
3. Data Lake (Immutable) → Parquet
   - Historical orders
   - Analytics queries
   - 10× compression vs JSON
   
4. Data Warehouse (Mutable) → Delta Lake
   - Customer profiles (UPDATE)
   - Inventory levels (DELETE)
   - Product catalog (MERGE)
   
5. Hive Integration → ORC
   - Enterprise reporting
   - Legacy Hive queries
   
6. Scientific Analysis → HDF5
   - Image processing (product photos)
   - Recommendation models (matrices)
```

## Summary: The Complete Picture

**Delta Lake** = Parquet + ACID transactions  
**ORC** = Parquet optimized for Hive  
**Avro** = Row-based with schema evolution  
**JSON** = Human-readable nested data  
**HDF5** = N-dimensional arrays  
**Parquet** = Columnar storage (the safe default)

When in doubt: **Use Parquet**, upgrade to **Delta Lake** when you need ACID.
