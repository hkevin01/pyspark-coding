"""
Broadcast Joins and Parquet File Format
========================================

WHAT THIS MODULE COVERS:
1. Broadcast Joins (Small Table Optimization)
2. Parquet File Format (Columnar Storage)
3. When to Use Parquet vs Alternatives
4. Performance Comparisons

BROADCAST JOIN FUNDAMENTALS:
---------------------------
A broadcast join is an optimization where a small table is replicated (broadcast) 
to all executor nodes, eliminating the need to shuffle the large table.

┌─────────────────────────────────────────────────────────────────┐
│                    REGULAR JOIN (SHUFFLE)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Large Table (1 TB)          Small Table (10 MB)               │
│  ┌──────────┐                ┌──────────┐                      │
│  │ Part 1   │                │ Part 1   │                      │
│  │ Part 2   │   SHUFFLE →    │ Part 2   │   ← SHUFFLE         │
│  │ Part 3   │                │ Part 3   │                      │
│  │ Part ... │                │ Part ... │                      │
│  └──────────┘                └──────────┘                      │
│       ↓                           ↓                            │
│  Data moves across               Data moves across             │
│  network (1 TB!)                 network (10 MB)               │
│                                                                 │
│  Total Network Transfer: 1.01 TB                               │
│  Time: ~30 minutes                                             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                   BROADCAST JOIN (OPTIMIZED)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Large Table (1 TB)          Small Table (10 MB)               │
│  ┌──────────┐                ┌──────────┐                      │
│  │ Part 1   │  ←──────────── │  COPIED  │ (Broadcast)         │
│  │ Part 2   │  ←──────────── │  TO ALL  │                     │
│  │ Part 3   │  ←──────────── │  NODES   │                     │
│  │ Part ... │  ←──────────── │          │                     │
│  └──────────┘                └──────────┘                      │
│       ↓                                                         │
│  Stays in place!             Each executor gets                 │
│  (No shuffle)                copy (10 MB × 10 nodes = 100 MB)  │
│                                                                 │
│  Total Network Transfer: 100 MB (10× fewer!)                   │
│  Time: ~3 minutes (10× faster!)                                │
└─────────────────────────────────────────────────────────────────┘

KEY INSIGHT:
-----------
Broadcasting makes sense when:
• Small table size × number of executors < Large table shuffle cost
• Example: 10 MB × 100 executors = 1 GB << 1 TB shuffle

WHEN TO USE BROADCAST JOINS:
---------------------------
✅ Small table < 10 MB (default broadcast threshold)
✅ One table much smaller than other (1:100+ ratio)
✅ Repeated joins with same small table
✅ Star schema (fact table with dimension tables)
✅ Lookup tables, reference data, configuration tables

❌ Both tables large (> 100 MB each)
❌ Small table changes frequently (broadcast overhead)
❌ Limited driver/executor memory
❌ Network bandwidth constrained

PARQUET FILE FORMAT:
-------------------
Parquet is a columnar storage format optimized for analytics.

ROW-BASED FORMAT (CSV, JSON):
┌─────────────────────────────────────────┐
│ Row 1: id=1, name="Alice", age=25       │
│ Row 2: id=2, name="Bob", age=30         │
│ Row 3: id=3, name="Charlie", age=35     │
└─────────────────────────────────────────┘
• Stores data row-by-row
• Good for: Reading entire rows, transactional systems
• Bad for: Analytical queries on specific columns

COLUMN-BASED FORMAT (Parquet):
┌─────────────────────────────────────────┐
│ Column "id":    [1, 2, 3]               │
│ Column "name":  ["Alice", "Bob", "Charlie"] │
│ Column "age":   [25, 30, 35]            │
└─────────────────────────────────────────┘
• Stores data column-by-column
• Good for: Analytical queries, aggregations, column pruning
• Bad for: Random row access

PARQUET ADVANTAGES:
------------------
1. **Compression**: Similar values compress better (5-10× smaller)
2. **Column Pruning**: Read only needed columns (not entire row)
3. **Predicate Pushdown**: Skip row groups based on metadata
4. **Schema Evolution**: Add/remove columns without rewriting data
5. **Type Safety**: Stores typed data (int, string, timestamp)
6. **Compatibility**: Works across Spark, Hive, Impala, Presto

EXAMPLE COMPRESSION:
-------------------
CSV File (1 GB):
┌──────────────────────────────────────────────────────┐
│ 1,Alice,25,F,2020-01-01,100.50                       │
│ 2,Bob,30,M,2020-01-02,200.75                         │
│ 3,Charlie,35,M,2020-01-03,150.25                     │
│ ... (1 million rows)                                 │
└──────────────────────────────────────────────────────┘
Size: 1 GB

Parquet File (Same Data):
┌──────────────────────────────────────────────────────┐
│ Column "age": [25, 30, 35, 25, 30, ...] → Compressed│
│   (Only ~100 unique values, high compression!)       │
│ Column "gender": [F, M, M, F, M, ...] → Compressed  │
│   (Only 2 unique values, extreme compression!)       │
└──────────────────────────────────────────────────────┘
Size: 100-200 MB (5-10× smaller!)

WHEN TO USE PARQUET:
-------------------
✅ Analytical workloads (aggregations, filtering)
✅ Data warehouse / data lake storage
✅ Read-heavy workloads with column-specific queries
✅ Long-term storage (compressed, efficient)
✅ Large datasets (TB+)
✅ Schema evolution needed

PARQUET ALTERNATIVES:
--------------------
1. CSV
   • Use case: Simple interchange, human-readable
   • Pros: Universal support, text-based
   • Cons: No compression, no types, slow to parse
   • When: Small files (< 100 MB), one-time imports

2. JSON
   • Use case: Nested/hierarchical data, APIs
   • Pros: Flexible schema, nested structures
   • Cons: Large file size, slow parsing
   • When: Semi-structured data, nested objects

3. Avro
   • Use case: Row-based streaming, schema evolution
   • Pros: Compact binary, schema evolution, fast writes
   • Cons: Row-based (slower analytics), less compression
   • When: Kafka streams, frequent writes, schema changes

4. ORC (Optimized Row Columnar)
   • Use case: Hive integration, similar to Parquet
   • Pros: Better compression, ACID support, predicate pushdown
   • Cons: Hive-specific, less universal than Parquet
   • When: Hive ecosystem, need ACID transactions

5. Delta Lake
   • Use case: ACID transactions, time travel, updates/deletes
   • Pros: Built on Parquet + transaction log, ACID, versioning
   • Cons: Requires Delta Lake library, slightly more overhead
   • When: Need updates/deletes, ACID guarantees, audit trail

6. HDF5 (Hierarchical Data Format 5)
   • Use case: Scientific computing, numerical arrays, multi-dimensional data
   • Pros: Fast random access, chunked storage, hierarchical structure
   • Cons: Not distributed-friendly, complex API, poor compression vs Parquet
   • When: Scientific data (numpy arrays), single-machine workflows

COMPARISON TABLE:
----------------
| Format      | Type      | Compression | Speed (Read) | Speed (Write) | Best For              |
|-------------|-----------|-------------|--------------|---------------|-----------------------|
| CSV         | Row       | None        | Slow         | Fast          | Simple interchange    |
| JSON        | Row       | None        | Slow         | Medium        | Nested data           |
| Avro        | Row       | Good        | Medium       | Fast          | Streaming, Kafka      |
| Parquet     | Column    | Excellent   | Fast         | Medium        | Analytics, data lake  |
| ORC         | Column    | Excellent   | Fast         | Medium        | Hive, ACID            |
| Delta Lake  | Column    | Excellent   | Fast         | Medium        | ACID, time travel     |
| HDF5        | Array     | Good        | Very Fast*   | Very Fast*    | Scientific arrays     |

* Fast for random access on single machine, slow for distributed processing

PARQUET vs HDF5 DEEP DIVE:
==========================

Parquet Row Groups vs HDF5 Chunks
----------------------------------

PARQUET ROW GROUPS:
┌─────────────────────────────────────────────────────────────┐
│                    Parquet File Structure                   │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  File Metadata (schema, row group metadata)                │
│  ├─ Total rows: 1,000,000                                  │
│  ├─ Row groups: 10                                         │
│  └─ Columns: id, name, age, salary                         │
│                                                             │
│  Row Group 1 (100,000 rows)                                │
│  ├─ Column Chunk: id [1, 2, ..., 100000]                   │
│  │  └─ Pages: [Page1: 0-10K] [Page2: 10K-20K] ...         │
│  ├─ Column Chunk: name ["Alice", "Bob", ...]               │
│  ├─ Column Chunk: age [25, 30, 35, ...]                    │
│  └─ Column Chunk: salary [50000, 60000, ...]               │
│                                                             │
│  Row Group 2 (100,000 rows)                                │
│  ├─ Column Chunk: id [100001, 100002, ...]                 │
│  └─ ...                                                     │
│                                                             │
│  ... (Row Groups 3-10)                                     │
│                                                             │
│  Footer (metadata, column statistics, offsets)             │
│  ├─ Row Group 1: min/max values per column                 │
│  ├─ Row Group 2: min/max values per column                 │
│  └─ ...                                                     │
└─────────────────────────────────────────────────────────────┘

Row Group Size (default: 128 MB)
• Balances: Memory usage vs parallelism
• Too small: Overhead from metadata, poor compression
• Too large: Memory pressure, coarse parallelism
• Optimal: 128-512 MB per row group

HDF5 CHUNKS:
┌─────────────────────────────────────────────────────────────┐
│                     HDF5 File Structure                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  File Header (format signature, super block)               │
│                                                             │
│  Dataset: /data/temperatures (1000 × 1000 × 365)           │
│  ├─ Datatype: float64                                       │
│  ├─ Dimensions: [latitude, longitude, day_of_year]         │
│  ├─ Chunk size: (100, 100, 1) [10,000 floats = 80 KB]      │
│  └─ Compression: gzip level 4                               │
│                                                             │
│  Physical Layout:                                           │
│  ┌─────────┬─────────┬─────────┬─────────┐                │
│  │ Chunk   │ Chunk   │ Chunk   │ Chunk   │ Day 1          │
│  │ (0,0,0) │ (0,1,0) │ (0,2,0) │ ...     │                │
│  ├─────────┼─────────┼─────────┼─────────┤                │
│  │ Chunk   │ Chunk   │ Chunk   │ Chunk   │                │
│  │ (1,0,0) │ (1,1,0) │ (1,2,0) │ ...     │                │
│  └─────────┴─────────┴─────────┴─────────┘                │
│  ┌─────────┬─────────┬─────────┬─────────┐                │
│  │ Chunk   │ Chunk   │ Chunk   │ Chunk   │ Day 2          │
│  │ (0,0,1) │ (0,1,1) │ (0,2,1) │ ...     │                │
│  └─────────┴─────────┴─────────┴─────────┘                │
│  ... (365 days)                                             │
│                                                             │
│  B-tree Index (chunk locations on disk)                    │
└─────────────────────────────────────────────────────────────┘

Chunk Size (user-defined, critical!)
• Access pattern dependent: Match expected query shape
• Too small: Too many seeks, index overhead
• Too large: Read unnecessary data
• Optimal: Depends on access pattern (1-10 MB typical)

KEY DIFFERENCES:
---------------

1. DATA MODEL:
   Parquet: Tabular data (rows × columns)
   HDF5:    Multi-dimensional arrays (N-dimensional tensors)

2. STORAGE LAYOUT:
   Parquet: Columnar (all values of one column together)
   HDF5:    Chunked arrays (N-dimensional blocks)

3. ROW GROUP SIZE:
   Parquet: Fixed size (128 MB default, automatic)
   HDF5:    User-defined chunks (must match access pattern)

4. COMPRESSION:
   Parquet: Per column chunk (optimal for each type)
   HDF5:    Per chunk (uniform across dataset)

5. METADATA:
   Parquet: Column statistics (min/max per row group) → predicate pushdown
   HDF5:    Dimension info, chunk locations → no query optimization

6. DISTRIBUTED PROCESSING:
   Parquet: Native support (each row group = 1 partition)
   HDF5:    Poor support (designed for single machine, shared filesystem)

7. RANDOM ACCESS:
   Parquet: Column-level (read specific columns from any row group)
   HDF5:    Chunk-level (read specific N-dimensional regions)

8. USE CASE ALIGNMENT:
   Parquet: Business analytics, ETL, data warehousing
   HDF5:    Scientific computing, simulations, image stacks

EXAMPLE COMPARISON:
------------------

Scenario: Store 1 billion temperature readings
• Dimensions: 1000 locations × 1000 sensors × 365 days × 3 years
• Data size: ~3 TB

PARQUET APPROACH:
┌────────────────────────────────────────────────┐
│ Table: temperature_readings                    │
├────────────────────────────────────────────────┤
│ location_id | sensor_id | day | temp | ...    │
│ 1           | 1         | 1   | 25.3 |        │
│ 1           | 1         | 2   | 26.1 |        │
│ ...         | ...       | ... | ...  |        │
└────────────────────────────────────────────────┘
Storage: Columnar by column
• Row groups: ~25,000 (3 TB / 128 MB)
• Parallelism: 25,000 partitions (excellent)
• Query: "Average temp by location" → read only location_id, temp columns
• Compression: Exceptional (temp values similar, dict encoding)

HDF5 APPROACH:
┌────────────────────────────────────────────────┐
│ Dataset: /temperatures [1000, 1000, 365, 3]   │
│ 4D array indexed by [location, sensor, day, year]
│ Chunks: (10, 10, 1, 1) [100 readings = 800 bytes]
└────────────────────────────────────────────────┘
Storage: Chunked multi-dimensional array
• Chunks: 109,500,000 (1000/10 × 1000/10 × 365 × 3)
• Access: temps[500, :, 180, 2] (all sensors at location 500, day 180, year 2)
• Fast random slicing, but poor for distributed processing
• Compression: Good, but not as good as Parquet (less repetition per chunk)

WHEN TO CHOOSE PARQUET:
✅ Tabular data (rows and columns)
✅ Distributed processing (Spark, Dask, etc.)
✅ Analytical queries (aggregations, filtering)
✅ Data warehouse / data lake
✅ Heterogeneous data types per column
✅ Cloud storage (S3, GCS, Azure Blob)
✅ Schema evolution needed

WHEN TO CHOOSE HDF5:
✅ Multi-dimensional numerical arrays
✅ Single-machine workflows
✅ Scientific computing (NumPy-centric)
✅ Random access to array slices critical
✅ Hierarchical data organization needed
✅ Append-heavy workloads
✅ Shared filesystem (not cloud-native)

PARQUET ROW GROUP SIZING:
-------------------------
Row group size affects:
• Memory usage (must fit in executor memory)
• Parallelism (1 row group = 1 task)
• Compression ratio (larger = better compression)
• Predicate pushdown (coarser granularity for large row groups)

Default: 128 MB (good balance)
Configuration: spark.sql.parquet.block.size

Small row groups (32 MB):
+ More parallelism (4× more tasks)
+ Lower memory per task
- More metadata overhead
- Slightly worse compression
When: Large cluster, limited memory per executor

Large row groups (512 MB):
+ Better compression (more context)
+ Less metadata overhead
- Less parallelism (fewer tasks)
- Higher memory per task
When: Smaller cluster, high-memory executors

HDF5 CHUNK SIZING:
-----------------
Chunk size must match access pattern!

Example: Image time series (1000 × 1000 × 10,000 frames)

Access Pattern 1: Process entire frames
Chunks: (1000, 1000, 1) [1 frame = 1 chunk]
✅ Perfect: Read whole frame in 1 I/O
❌ Bad for: Time-series at single pixel

Access Pattern 2: Time-series at pixels
Chunks: (1, 1, 10000) [all time for 1 pixel]
✅ Perfect: Read pixel time-series in 1 I/O
❌ Bad for: Viewing single frames

Access Pattern 3: Mixed workload
Chunks: (100, 100, 100) [balanced]
⚠️  Compromise: Multiple I/O for both patterns

Key insight: HDF5 requires knowing access pattern upfront!
Parquet: Columnar layout works well for most analytical queries.

PARQUET + SPARK INTEGRATION:
----------------------------
• 1 row group = 1 Spark partition (automatic parallelism)
• Column pruning (read only needed columns)
• Predicate pushdown (skip row groups using min/max stats)
• Vectorized reading (columnar → Arrow → fast)
• Cloud-native (works well with S3, GCS, Azure Blob)

HDF5 + SPARK LIMITATIONS:
------------------------
• No native Spark support (must use custom readers)
• Each worker must read entire HDF5 file (no parallel reads)
• No predicate pushdown
• Designed for shared filesystem (NFS), not cloud storage
• Workaround: Convert HDF5 → Parquet for Spark processing

Author: PySpark Training
Date: 2024-12
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, sum as _sum, avg, count, when, 
    current_timestamp, date_format, rand
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import time


def create_spark_session():
    """
    Create Spark session with broadcast join configuration.
    """
    return SparkSession.builder \
        .appName("BroadcastJoins_and_Parquet") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10 MB default \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def example_1_broadcast_join_basics():
    """
    Demonstrate basic broadcast join vs regular join.
    
    SCENARIO:
    ---------
    Large sales table (1M rows) join with small products table (100 rows).
    Without broadcast: Both tables shuffle (expensive).
    With broadcast: Products table copied to all executors (cheap).
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Broadcast Join Basics")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create large sales table (simulating 1M rows)
    print("\n📊 Creating large sales table (1M rows)...")
    sales_data = [
        (i, f"P{i % 100}", i * 10.5, f"2024-{(i % 12) + 1:02d}-01")
        for i in range(1, 100001)  # 100K for demo (imagine 1M)
    ]
    
    df_sales = spark.createDataFrame(
        sales_data,
        ["sale_id", "product_id", "amount", "sale_date"]
    )
    
    print(f"   Sales table: {df_sales.count():,} rows")
    df_sales.show(5, truncate=False)
    
    # Create small products table (100 rows)
    print("\n📦 Creating small products table (100 rows)...")
    products_data = [
        (f"P{i}", f"Product_{i}", f"Category_{i % 10}", 50.0 + i)
        for i in range(100)
    ]
    
    df_products = spark.createDataFrame(
        products_data,
        ["product_id", "product_name", "category", "unit_price"]
    )
    
    print(f"   Products table: {df_products.count()} rows")
    df_products.show(5, truncate=False)
    
    # ❌ REGULAR JOIN (shuffle both sides)
    print("\n❌ REGULAR JOIN (No Broadcast):")
    print("   • Both tables will be shuffled")
    print("   • Network transfer: Large table + small table")
    
    start_time = time.time()
    df_regular_join = df_sales.join(df_products, "product_id")
    result_count = df_regular_join.count()
    regular_time = time.time() - start_time
    
    print(f"   • Result: {result_count:,} rows")
    print(f"   • Time: {regular_time:.2f} seconds")
    df_regular_join.show(5, truncate=False)
    
    # ✅ BROADCAST JOIN (broadcast small table)
    print("\n✅ BROADCAST JOIN (Explicit):")
    print("   • Products table broadcast to all executors")
    print("   • Sales table stays in place (no shuffle)")
    print("   • Network transfer: Only small table × number of executors")
    
    start_time = time.time()
    df_broadcast_join = df_sales.join(broadcast(df_products), "product_id")
    result_count = df_broadcast_join.count()
    broadcast_time = time.time() - start_time
    
    print(f"   • Result: {result_count:,} rows")
    print(f"   • Time: {broadcast_time:.2f} seconds")
    df_broadcast_join.show(5, truncate=False)
    
    # Performance comparison
    print("\n📈 PERFORMANCE COMPARISON:")
    print(f"   Regular Join:   {regular_time:.2f} seconds")
    print(f"   Broadcast Join: {broadcast_time:.2f} seconds")
    if broadcast_time > 0:
        speedup = regular_time / broadcast_time
        print(f"   Speedup:        {speedup:.2f}× faster")
    
    print("\n💡 KEY INSIGHT:")
    print("   Broadcast join eliminates shuffle on large table.")
    print("   Cost: 100 rows × 10 executors = 1,000 rows transferred")
    print("   Savings: 100,000 rows NOT shuffled!")


def example_2_automatic_broadcast():
    """
    Demonstrate automatic broadcast join detection.
    
    Spark automatically broadcasts tables < 10 MB (default threshold).
    """
    print("\n" + "="*70)
    print("EXAMPLE 2: Automatic Broadcast Detection")
    print("="*70)
    
    spark = create_spark_session()
    
    # Small table (will auto-broadcast)
    print("\n📦 Creating dimension tables...")
    df_customers = spark.createDataFrame(
        [(i, f"Customer_{i}", f"Tier_{i % 3}") for i in range(1, 1001)],
        ["customer_id", "customer_name", "tier"]
    )
    
    df_regions = spark.createDataFrame(
        [(i, f"Region_{i}") for i in range(1, 11)],
        ["region_id", "region_name"]
    )
    
    # Large fact table
    print("📊 Creating fact table...")
    df_orders = spark.createDataFrame(
        [
            (i, i % 1000 + 1, i % 10 + 1, i * 25.5)
            for i in range(1, 50001)
        ],
        ["order_id", "customer_id", "region_id", "amount"]
    )
    
    print(f"\n   Orders: {df_orders.count():,} rows")
    print(f"   Customers: {df_customers.count()} rows (< 10 MB → auto-broadcast)")
    print(f"   Regions: {df_regions.count()} rows (< 10 MB → auto-broadcast)")
    
    # Join without explicit broadcast() - Spark auto-detects
    print("\n🔍 Joining without explicit broadcast() call...")
    print("   Spark will automatically broadcast small tables!")
    
    df_enriched = df_orders \
        .join(df_customers, "customer_id") \
        .join(df_regions, "region_id")
    
    print("\n   Result schema:")
    df_enriched.printSchema()
    
    df_enriched.show(5, truncate=False)
    
    print("\n💡 CHECK SPARK UI:")
    print("   • Go to Spark UI → SQL tab")
    print("   • Look for 'BroadcastHashJoin' in query plan")
    print("   • Confirms automatic broadcast optimization")
    
    # Check query plan
    print("\n📋 QUERY PLAN (showing broadcast):")
    df_enriched.explain()
    
    print("\n⚙️  BROADCAST THRESHOLD CONFIGURATION:")
    threshold = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
    print(f"   Current: {int(threshold):,} bytes ({int(threshold) / 1024 / 1024:.1f} MB)")
    print("   Adjust with: spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '20971520')")


def example_3_parquet_vs_csv():
    """
    Demonstrate Parquet file format advantages over CSV.
    
    COMPARISON:
    ----------
    • File size (compression)
    • Read speed (columnar access)
    • Column pruning (selective reads)
    • Predicate pushdown (filter early)
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: Parquet vs CSV Comparison")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create sample dataset
    print("\n📊 Creating sample dataset (10,000 rows)...")
    df_data = spark.range(1, 10001) \
        .withColumn("name", col("id").cast("string")) \
        .withColumn("age", (rand() * 50 + 20).cast("int")) \
        .withColumn("salary", (rand() * 100000 + 30000).cast("double")) \
        .withColumn("department", (rand() * 10).cast("int").cast("string")) \
        .withColumn("hire_date", date_format(current_timestamp(), "yyyy-MM-dd"))
    
    df_data.show(5, truncate=False)
    
    # Write as CSV
    print("\n💾 Writing as CSV...")
    csv_path = "/tmp/pyspark_data.csv"
    start_time = time.time()
    df_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    csv_write_time = time.time() - start_time
    print(f"   Write time: {csv_write_time:.2f} seconds")
    
    # Write as Parquet
    print("\n💾 Writing as Parquet...")
    parquet_path = "/tmp/pyspark_data.parquet"
    start_time = time.time()
    df_data.coalesce(1).write.mode("overwrite").parquet(parquet_path)
    parquet_write_time = time.time() - start_time
    print(f"   Write time: {parquet_write_time:.2f} seconds")
    
    # Compare file sizes
    print("\n📏 FILE SIZE COMPARISON:")
    import subprocess
    csv_size = subprocess.check_output(f"du -sh {csv_path}", shell=True).decode().split()[0]
    parquet_size = subprocess.check_output(f"du -sh {parquet_path}", shell=True).decode().split()[0]
    print(f"   CSV:     {csv_size}")
    print(f"   Parquet: {parquet_size}")
    print("   → Parquet typically 5-10× smaller due to compression")
    
    # Read speed comparison - FULL SCAN
    print("\n⏱️  READ SPEED - Full Scan:")
    
    # CSV read
    start_time = time.time()
    df_csv = spark.read.option("header", "true").csv(csv_path)
    csv_count = df_csv.count()
    csv_read_time = time.time() - start_time
    print(f"   CSV:     {csv_read_time:.2f} seconds ({csv_count:,} rows)")
    
    # Parquet read
    start_time = time.time()
    df_parquet = spark.read.parquet(parquet_path)
    parquet_count = df_parquet.count()
    parquet_read_time = time.time() - start_time
    print(f"   Parquet: {parquet_read_time:.2f} seconds ({parquet_count:,} rows)")
    print(f"   → Parquet {csv_read_time/parquet_read_time:.2f}× faster")
    
    # Column pruning test
    print("\n📊 COLUMN PRUNING TEST (select 2 of 6 columns):")
    print("   CSV must read all columns, then discard unneeded ones.")
    print("   Parquet reads only requested columns from storage.")
    
    # CSV - column select
    start_time = time.time()
    df_csv_select = spark.read.option("header", "true").csv(csv_path).select("id", "salary")
    csv_select_count = df_csv_select.count()
    csv_select_time = time.time() - start_time
    print(f"\n   CSV (select 2 cols):     {csv_select_time:.2f} seconds")
    
    # Parquet - column select
    start_time = time.time()
    df_parquet_select = spark.read.parquet(parquet_path).select("id", "salary")
    parquet_select_count = df_parquet_select.count()
    parquet_select_time = time.time() - start_time
    print(f"   Parquet (select 2 cols): {parquet_select_time:.2f} seconds")
    print(f"   → Parquet {csv_select_time/parquet_select_time:.2f}× faster (column pruning!)")
    
    # Predicate pushdown test
    print("\n🔍 PREDICATE PUSHDOWN TEST (filter on age > 40):")
    print("   Parquet can skip entire row groups based on metadata.")
    print("   CSV must scan all rows.")
    
    # CSV - filter
    start_time = time.time()
    df_csv_filter = spark.read.option("header", "true").csv(csv_path).filter(col("age") > 40)
    csv_filter_count = df_csv_filter.count()
    csv_filter_time = time.time() - start_time
    print(f"\n   CSV (filter):     {csv_filter_time:.2f} seconds ({csv_filter_count:,} rows)")
    
    # Parquet - filter
    start_time = time.time()
    df_parquet_filter = spark.read.parquet(parquet_path).filter(col("age") > 40)
    parquet_filter_count = df_parquet_filter.count()
    parquet_filter_time = time.time() - start_time
    print(f"   Parquet (filter): {parquet_filter_time:.2f} seconds ({parquet_filter_count:,} rows)")
    print(f"   → Parquet {csv_filter_time/parquet_filter_time:.2f}× faster (predicate pushdown!)")
    
    print("\n📊 SUMMARY:")
    print("   ✅ Parquet is smaller (compression)")
    print("   ✅ Parquet reads faster (columnar layout)")
    print("   ✅ Parquet supports column pruning (read only needed columns)")
    print("   ✅ Parquet supports predicate pushdown (skip data early)")


def example_4_parquet_compression_codecs():
    """
    Demonstrate different Parquet compression codecs.
    
    CODECS:
    ------
    • SNAPPY: Fast compression/decompression (default)
    • GZIP: Better compression, slower
    • LZ4: Fastest, less compression
    • ZSTD: Best balance (modern codec)
    • UNCOMPRESSED: No compression
    """
    print("\n" + "="*70)
    print("EXAMPLE 4: Parquet Compression Codecs")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create dataset
    print("\n📊 Creating sample dataset...")
    df = spark.range(1, 100001) \
        .withColumn("text", col("id").cast("string")) \
        .withColumn("value", (rand() * 1000).cast("double"))
    
    codecs = ["none", "snappy", "gzip", "lz4", "zstd"]
    
    print("\n💾 Testing compression codecs...\n")
    
    results = []
    for codec in codecs:
        path = f"/tmp/parquet_{codec}"
        
        # Write
        start_time = time.time()
        df.write.mode("overwrite") \
            .option("compression", codec) \
            .parquet(path)
        write_time = time.time() - start_time
        
        # Get size
        import subprocess
        size_output = subprocess.check_output(f"du -sh {path}", shell=True).decode()
        size = size_output.split()[0]
        
        # Read
        start_time = time.time()
        df_read = spark.read.parquet(path)
        count = df_read.count()
        read_time = time.time() - start_time
        
        results.append({
            "codec": codec.upper(),
            "size": size,
            "write_time": write_time,
            "read_time": read_time
        })
        
        print(f"   {codec.upper():12s} | Size: {size:>8s} | Write: {write_time:>5.2f}s | Read: {read_time:>5.2f}s")
    
    print("\n📊 CODEC COMPARISON:")
    print("   ┌─────────────┬────────────┬──────────────┬─────────────┐")
    print("   │ Codec       │ Size       │ Write Speed  │ Read Speed  │")
    print("   ├─────────────┼────────────┼──────────────┼─────────────┤")
    for r in results:
        print(f"   │ {r['codec']:11s} │ {r['size']:>10s} │ {r['write_time']:>10.2f}s │ {r['read_time']:>9.2f}s │")
    print("   └─────────────┴────────────┴──────────────┴─────────────┘")
    
    print("\n💡 CODEC SELECTION GUIDE:")
    print("   • SNAPPY (default):  Good balance, fast decompression")
    print("   • GZIP:              Best compression, slower (cold storage)")
    print("   • LZ4:               Fastest, use for hot data")
    print("   • ZSTD:              Modern, best compression + speed")
    print("   • UNCOMPRESSED:      Only for benchmarking or pre-compressed data")


def example_5_format_comparison():
    """
    Compare different file formats: CSV, JSON, Avro, Parquet, ORC.
    """
    print("\n" + "="*70)
    print("EXAMPLE 5: File Format Comparison")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create dataset with nested structure
    print("\n📊 Creating sample dataset (nested structure)...")
    df = spark.createDataFrame([
        (1, "Alice", 25, {"street": "123 Main", "city": "NYC"}, ["Python", "Spark"]),
        (2, "Bob", 30, {"street": "456 Oak", "city": "SF"}, ["Java", "Scala"]),
        (3, "Charlie", 35, {"street": "789 Elm", "city": "LA"}, ["Go", "Rust"])
    ] * 1000, ["id", "name", "age", "address", "skills"])
    
    print(f"   Rows: {df.count():,}")
    df.show(3, truncate=False)
    
    formats = {
        "csv": {"path": "/tmp/data.csv", "write_options": {"header": "true"}},
        "json": {"path": "/tmp/data.json", "write_options": {}},
        "parquet": {"path": "/tmp/data.parquet", "write_options": {}},
        # Note: Avro requires spark-avro package
        # "avro": {"path": "/tmp/data.avro", "write_options": {}},
    }
    
    print("\n💾 Writing in different formats...")
    print("   " + "─" * 60)
    
    results = {}
    for fmt, config in formats.items():
        print(f"\n   {fmt.upper()}:")
        
        # Write
        start_time = time.time()
        writer = df.coalesce(1).write.mode("overwrite")
        for key, value in config["write_options"].items():
            writer = writer.option(key, value)
        
        if fmt == "csv":
            writer.csv(config["path"])
        elif fmt == "json":
            writer.json(config["path"])
        elif fmt == "parquet":
            writer.parquet(config["path"])
        
        write_time = time.time() - start_time
        
        # Get size
        import subprocess
        size_output = subprocess.check_output(f"du -sh {config['path']}", shell=True).decode()
        size = size_output.split()[0]
        
        # Read
        start_time = time.time()
        if fmt == "csv":
            df_read = spark.read.option("header", "true").csv(config["path"])
        elif fmt == "json":
            df_read = spark.read.json(config["path"])
        elif fmt == "parquet":
            df_read = spark.read.parquet(config["path"])
        
        count = df_read.count()
        read_time = time.time() - start_time
        
        results[fmt] = {
            "size": size,
            "write_time": write_time,
            "read_time": read_time
        }
        
        print(f"      Size: {size:>8s} | Write: {write_time:.2f}s | Read: {read_time:.2f}s")
    
    print("\n" + "   " + "─" * 60)
    print("\n📊 FORMAT COMPARISON TABLE:")
    print("""
   ┌──────────┬─────────────┬─────────────┬──────────────┬─────────────────────┐
   │ Format   │ Structure   │ Compression │ Best For     │ Use Case            │
   ├──────────┼─────────────┼─────────────┼──────────────┼─────────────────────┤
   │ CSV      │ Row-based   │ Poor        │ Interchange  │ Simple data export  │
   │ JSON     │ Nested      │ Poor        │ APIs         │ Semi-structured     │
   │ Parquet  │ Columnar    │ Excellent   │ Analytics    │ Data lake (best!)   │
   │ Avro     │ Row-based   │ Good        │ Streaming    │ Kafka, schema evo   │
   │ ORC      │ Columnar    │ Excellent   │ Hive         │ Hive warehouse      │
   └──────────┴─────────────┴─────────────┴──────────────┴─────────────────────┘
    """)


def example_6_parquet_row_groups_vs_hdf5_chunks():
    """
    Demonstrate Parquet row group sizing and compare with HDF5 chunking.
    
    KEY CONCEPTS:
    ------------
    Parquet Row Groups:
    • Fixed size (128 MB default)
    • Automatic parallelism (1 row group = 1 Spark partition)
    • Columnar within each row group
    
    HDF5 Chunks:
    • User-defined size (access pattern dependent)
    • Multi-dimensional blocks
    • Must read entire chunk even for partial access
    """
    print("\n" + "="*70)
    print("EXAMPLE 6: Parquet Row Groups vs HDF5 Chunks")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create dataset with different row group sizes
    print("\n📊 Creating sample dataset (100,000 rows)...")
    df = spark.range(1, 100001) \
        .withColumn("value1", (rand() * 1000).cast("double")) \
        .withColumn("value2", (rand() * 1000).cast("double")) \
        .withColumn("value3", (rand() * 1000).cast("double"))
    
    print(f"   Rows: {df.count():,}")
    print(f"   Columns: {len(df.columns)}")
    
    # Test different row group sizes
    row_group_sizes = [
        ("32MB", 32 * 1024 * 1024),
        ("128MB", 128 * 1024 * 1024),  # Default
        ("512MB", 512 * 1024 * 1024)
    ]
    
    print("\n💾 Writing Parquet with different row group sizes...\n")
    
    results = []
    for label, size in row_group_sizes:
        path = f"/tmp/parquet_rowgroup_{label}"
        
        # Write with specific row group size
        start_time = time.time()
        df.write.mode("overwrite") \
            .option("parquet.block.size", str(size)) \
            .parquet(path)
        write_time = time.time() - start_time
        
        # Get file info
        import subprocess
        file_size = subprocess.check_output(f"du -sh {path}", shell=True).decode().split()[0]
        
        # Count actual row groups (read Parquet metadata)
        df_read = spark.read.parquet(path)
        
        # Read with column pruning
        start_time = time.time()
        count = df_read.select("id", "value1").filter(col("value1") > 500).count()
        read_time = time.time() - start_time
        
        results.append({
            "label": label,
            "size": file_size,
            "write_time": write_time,
            "read_time": read_time,
            "filtered_rows": count
        })
        
        print(f"   {label:8s} | File: {file_size:>8s} | Write: {write_time:.2f}s | Read: {read_time:.2f}s")
    
    print("\n📊 ROW GROUP SIZE IMPACT:")
    print("""
   Small Row Groups (32 MB):
   • More parallelism (more partitions)
   • Lower memory per task
   • Slightly more metadata overhead
   • Better for: Large clusters, memory-constrained executors
   
   Default Row Groups (128 MB):
   • Balanced parallelism and compression
   • Standard for most workloads
   • Good predicate pushdown granularity
   • Better for: General purpose analytics
   
   Large Row Groups (512 MB):
   • Best compression (more context)
   • Less parallelism (fewer partitions)
   • Higher memory requirement
   • Better for: Small clusters, high-memory executors
    """)
    
    print("\n💡 HDF5 COMPARISON:")
    print("""
   HDF5 Chunks are fundamentally different:
   
   Parquet Row Groups:
   • Organized by ROWS (horizontal slicing)
   • All columns for rows 1-100K in row group 1
   • Column pruning within row group (read only needed columns)
   • Automatic parallelism in Spark
   
   HDF5 Chunks:
   • Organized by N-DIMENSIONAL BLOCKS
   • For 3D array [1000, 1000, 365], chunk might be [100, 100, 1]
   • Must read entire chunk (all dimensions)
   • No automatic parallelism
   
   Example: Temperature data [lat, lon, day]
   
   Parquet (tabular):
   Row Group 1: rows 1-100K (all columns)
   Row Group 2: rows 100K-200K (all columns)
   Query "avg temp by lat": Read only 'lat' and 'temp' columns
   
   HDF5 (array):
   Chunk (0,0,0): temps[0:100, 0:100, 0]
   Chunk (0,0,1): temps[0:100, 0:100, 1]
   Query temps[:, 500, :]: Must read many chunks to get 1 longitude slice
    """)
    
    print("\n🔍 KEY INSIGHT:")
    print("   Parquet row groups optimize for ANALYTICAL queries (columns, filtering)")
    print("   HDF5 chunks optimize for ARRAY SLICING (spatial/temporal regions)")
    print("   Parquet wins for distributed analytics, HDF5 wins for array computation")


def example_7_broadcast_join_with_parquet():
    """
    Complete example: Broadcast join with Parquet files.
    
    REAL-WORLD SCENARIO:
    -------------------
    • Large sales data stored as Parquet (optimized for analytics)
    • Small dimension tables (products, customers) as Parquet
    • Use broadcast joins for efficient star schema queries
    """
    print("\n" + "="*70)
    print("EXAMPLE 7: Broadcast Join + Parquet (Production Pattern)")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create and save dimension tables as Parquet
    print("\n📦 Creating dimension tables (Parquet)...")
    
    # Products dimension
    df_products = spark.createDataFrame([
        (f"P{i:04d}", f"Product_{i}", f"Category_{i % 5}", 50.0 + i)
        for i in range(1, 101)
    ], ["product_id", "product_name", "category", "unit_price"])
    
    products_path = "/tmp/warehouse/dim_products.parquet"
    df_products.write.mode("overwrite").parquet(products_path)
    print(f"   ✅ Products: {df_products.count()} rows → {products_path}")
    
    # Customers dimension
    df_customers = spark.createDataFrame([
        (f"C{i:05d}", f"Customer_{i}", f"Region_{i % 10}")
        for i in range(1, 1001)
    ], ["customer_id", "customer_name", "region"])
    
    customers_path = "/tmp/warehouse/dim_customers.parquet"
    df_customers.write.mode("overwrite").parquet(customers_path)
    print(f"   ✅ Customers: {df_customers.count()} rows → {customers_path}")
    
    # Create large fact table as Parquet
    print("\n📊 Creating fact table (Parquet)...")
    df_sales = spark.createDataFrame([
        (
            i,
            f"P{(i % 100) + 1:04d}",
            f"C{(i % 1000) + 1:05d}",
            i * 10.5,
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"
        )
        for i in range(1, 100001)
    ], ["sale_id", "product_id", "customer_id", "amount", "sale_date"])
    
    sales_path = "/tmp/warehouse/fact_sales.parquet"
    df_sales.write.mode("overwrite") \
        .partitionBy("sale_date") \
        .option("compression", "snappy") \
        .parquet(sales_path)
    
    print(f"   ✅ Sales: {df_sales.count():,} rows → {sales_path}")
    print("   📁 Partitioned by sale_date for efficient filtering")
    
    # Read back from Parquet
    print("\n📖 Reading from Parquet warehouse...")
    df_products_read = spark.read.parquet(products_path)
    df_customers_read = spark.read.parquet(customers_path)
    df_sales_read = spark.read.parquet(sales_path)
    
    # Analytical query with broadcast joins
    print("\n🔍 ANALYTICAL QUERY: Sales by category and region")
    print("   Using broadcast joins for dimension tables...")
    
    start_time = time.time()
    
    df_analysis = df_sales_read \
        .join(broadcast(df_products_read), "product_id") \
        .join(broadcast(df_customers_read), "customer_id") \
        .groupBy("category", "region") \
        .agg(
            _sum("amount").alias("total_sales"),
            avg("amount").alias("avg_sale"),
            count("*").alias("num_sales")
        ) \
        .orderBy(col("total_sales").desc())
    
    query_time = time.time() - start_time
    
    print(f"\n   Query completed in {query_time:.2f} seconds")
    print("\n   Top 10 Results:")
    df_analysis.show(10, truncate=False)
    
    # Show query plan
    print("\n📋 QUERY PLAN (verify broadcast):")
    df_analysis.explain()
    
    print("\n💡 PRODUCTION BENEFITS:")
    print("   ✅ Parquet: Compressed storage (5-10× smaller)")
    print("   ✅ Parquet: Column pruning (read only needed columns)")
    print("   ✅ Parquet: Partitioned by date (skip irrelevant data)")
    print("   ✅ Broadcast: No shuffle on large fact table")
    print("   ✅ Result: Fast analytical queries on large datasets")


def main():
    """
    Run all broadcast join and Parquet examples.
    """
    print("\n" + "="*70)
    print(" BROADCAST JOINS AND PARQUET FILE FORMAT ")
    print("="*70)
    
    print("""
This module demonstrates:
1. Broadcast join optimization (eliminate shuffles)
2. Parquet columnar storage format
3. File format comparison (CSV, JSON, Parquet, etc.)
4. Production patterns (broadcast + Parquet)

KEY CONCEPTS:
------------
• Broadcast Join: Replicate small table to all executors
• Parquet: Columnar format optimized for analytics
• Column Pruning: Read only needed columns
• Predicate Pushdown: Filter data early using metadata
• Compression: 5-10× smaller files with Parquet

WHEN TO USE:
-----------
Broadcast Join:
  ✅ Small table (< 10 MB)
  ✅ One-to-many relationships
  ✅ Star schema (dimension tables)
  
Parquet:
  ✅ Analytical workloads
  ✅ Data warehouse/lake
  ✅ Large datasets (TB+)
  ✅ Long-term storage
    """)
    
    try:
        example_1_broadcast_join_basics()
        example_2_automatic_broadcast()
        example_3_parquet_vs_csv()
        example_4_parquet_compression_codecs()
        example_5_format_comparison()
        example_6_parquet_row_groups_vs_hdf5_chunks()
        example_7_broadcast_join_with_parquet()
        
        print("\n" + "="*70)
        print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("="*70)
        
        print("\n📚 NEXT STEPS:")
        print("   1. Check Spark UI for BroadcastHashJoin in query plans")
        print("   2. Compare file sizes: ls -lh /tmp/*.{csv,parquet}")
        print("   3. Experiment with broadcast threshold settings")
        print("   4. Try different compression codecs for your data")
        print("   5. Use Parquet for all production data lake storage")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
