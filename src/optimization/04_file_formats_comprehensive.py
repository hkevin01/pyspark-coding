"""
Comprehensive File Format Examples
===================================

WHAT THIS MODULE COVERS:
1. Delta Lake (ACID transactions, time travel)
2. ORC (Optimized Row Columnar for Hive)
3. Avro (Row-based with schema evolution)
4. JSON (Semi-structured data)
5. HDF5 (Scientific multi-dimensional arrays)

QUICK COMPARISON:
----------------
| Format      | Structure  | ACID | Time Travel | Streaming | Best Use Case           |
|-------------|------------|------|-------------|-----------|-------------------------|
| Delta Lake  | Columnar   | ✅   | ✅          | ✅        | Data lake with ACID     |
| ORC         | Columnar   | ✅*  | ❌          | ❌        | Hive warehouse          |
| Avro        | Row-based  | ❌   | ❌          | ✅        | Kafka, schema evolution |
| JSON        | Nested     | ❌   | ❌          | ✅        | APIs, semi-structured   |
| HDF5        | Array      | ❌   | ❌          | ❌        | Scientific computing    |

* ORC ACID only in Hive context

FILE FORMAT SELECTION FLOWCHART:
--------------------------------
┌──────────────────────────────────────────────────────────────────┐
│                    Choose File Format                            │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌─────────────────────────────────────────┐
        │ Need ACID transactions or updates?      │
        └─────────────────────────────────────────┘
                    YES │        │ NO
                        │        │
                        ▼        ▼
                  Delta Lake   ┌─────────────────────────────┐
                               │ Working with Hive ecosystem? │
                               └─────────────────────────────┘
                                     YES │    │ NO
                                         │    │
                                         ▼    ▼
                                       ORC  ┌──────────────────────────┐
                                            │ Schema changes frequently?│
                                            └──────────────────────────┘
                                                  YES │    │ NO
                                                      │    │
                                                      ▼    ▼
                                                   Avro  ┌──────────────────┐
                                                         │ Nested/hierarchical?│
                                                         └──────────────────┘
                                                               YES │  │ NO
                                                                   │  │
                                                                   ▼  ▼
                                                                JSON Parquet
                                                                
                                            ┌────────────────────────────┐
                                            │ Scientific arrays/matrices?│
                                            └────────────────────────────┘
                                                         YES │
                                                             ▼
                                                           HDF5

Author: PySpark Training
Date: 2024-12
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, expr, struct, array,
    from_json, to_json, explode, rand, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, ArrayType
)
import time
import os


def create_spark_session():
    """
    Create Spark session with Delta Lake support.
    """
    return SparkSession.builder \
        .appName("FileFormats_Comprehensive") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()


def example_1_delta_lake_acid_transactions():
    """
    Demonstrate Delta Lake ACID transactions.
    
    WHAT ARE ACID TRANSACTIONS?
    ==========================
    ACID = Atomicity, Consistency, Isolation, Durability
    
    Database guarantees for reliable data operations:
    
    1. ATOMICITY (All or Nothing):
       ┌──────────────────────────────────────────────────┐
       │ Transaction: Transfer $100 from Account A → B   │
       ├──────────────────────────────────────────────────┤
       │ Step 1: Subtract $100 from Account A            │
       │ Step 2: Add $100 to Account B                   │
       │                                                  │
       │ ✅ BOTH steps succeed → Transaction commits     │
       │ ❌ ANY step fails → ENTIRE transaction rolls back│
       │    (No partial updates!)                         │
       └──────────────────────────────────────────────────┘
       
       WITHOUT ACID (Regular Parquet):
       • Write 1000 files
       • Crash after 500 files written
       • Result: Partial data, corrupt state
       
       WITH ACID (Delta Lake):
       • Write 1000 files + transaction log
       • Crash after 500 files written
       • Result: Transaction not committed, old version still valid
    
    2. CONSISTENCY (Rules Always Enforced):
       ┌──────────────────────────────────────────────────┐
       │ Rule: Age must be > 0                            │
       ├──────────────────────────────────────────────────┤
       │ Try to insert: {name: "Alice", age: -5}          │
       │ Result: ❌ REJECTED (violates constraint)        │
       │                                                  │
       │ Try to insert: {name: "Bob", age: 25}            │
       │ Result: ✅ ACCEPTED (satisfies constraint)       │
       └──────────────────────────────────────────────────┘
       
       WITHOUT ACID:
       • Schema enforcement weak or none
       • Data types can be wrong
       • Constraints not enforced
       
       WITH ACID:
       • Schema enforced on every write
       • Type checking automatic
       • Constraints validated
    
    3. ISOLATION (Concurrent Operations Don't Interfere):
       ┌──────────────────────────────────────────────────┐
       │ Timeline:                                        │
       │ T1: User A reads balance = $1000                 │
       │ T2: User B reads balance = $1000                 │
       │ T3: User A withdraws $100 → writes $900          │
       │ T4: User B withdraws $200 → writes $800          │
       │                                                  │
       │ WITHOUT ISOLATION:                               │
       │   Final balance = $800 (lost User A's update!)   │
       │                                                  │
       │ WITH ISOLATION:                                  │
       │   User B's write detects conflict, retries       │
       │   Final balance = $700 (both updates applied)    │
       └──────────────────────────────────────────────────┘
       
       Delta Lake uses Optimistic Concurrency Control:
       • Each transaction reads current version
       • Before commit, checks if base version changed
       • If changed → conflict, transaction retries
       • If same → commit succeeds
    
    4. DURABILITY (Committed Data Never Lost):
       ┌──────────────────────────────────────────────────┐
       │ Transaction commits at 10:00 AM                  │
       │ Server crashes at 10:01 AM                       │
       │ Server restarts at 10:05 AM                      │
       │                                                  │
       │ Result: Transaction data still there! ✅         │
       │ (Written to persistent storage before commit ack)│
       └──────────────────────────────────────────────────┘
       
       Delta Lake guarantees:
       • Transaction log written to storage (S3, HDFS)
       • Data files written before log commit
       • Log commit is atomic (single file rename)
       • Crash recovery: Read log, apply all committed transactions
    
    WHY ACID MATTERS FOR DATA LAKES:
    --------------------------------
    Traditional Data Lake (Parquet files):
    ❌ No atomicity: Partial writes leave corrupt data
    ❌ No consistency: Schema can drift
    ❌ No isolation: Concurrent writes = data loss
    ❌ No durability: No transaction log = no recovery
    
    Delta Lake:
    ✅ Atomicity: All-or-nothing writes
    ✅ Consistency: Schema enforced
    ✅ Isolation: Concurrent reads/writes safe
    ✅ Durability: Transaction log = complete audit trail
    
    DELTA LAKE FUNDAMENTALS:
    -----------------------
    Delta Lake = Parquet files + Transaction Log
    
    STRUCTURE:
    ┌─────────────────────────────────────────────────────┐
    │ Delta Lake Table Directory                          │
    ├─────────────────────────────────────────────────────┤
    │                                                     │
    │ _delta_log/                                         │
    │ ├── 00000000000000000000.json  (Transaction 0)     │
    │ ├── 00000000000000000001.json  (Transaction 1)     │
    │ ├── 00000000000000000002.json  (Transaction 2)     │
    │ └── 00000000000000000010.checkpoint.parquet         │
    │                                                     │
    │ part-00000-*.parquet  (Data file 1)                 │
    │ part-00001-*.parquet  (Data file 2)                 │
    │ part-00002-*.parquet  (Data file 3)                 │
    │ ...                                                 │
    └─────────────────────────────────────────────────────┘
    
    Transaction Log Entry Example:
    {
      "add": {
        "path": "part-00001.parquet",
        "size": 1024,
        "modificationTime": 1234567890,
        "dataChange": true,
        "stats": "{\"numRecords\":100,\"minValues\":{\"id\":1},...}"
      }
    }
    
    How Delta Lake Provides ACID:
    1. Atomicity: Log commit = atomic file operation
    2. Consistency: Schema tracked in log, enforced on write
    3. Isolation: Version numbers + optimistic concurrency
    4. Durability: Log persisted before acknowledging write
    
    File Structure:
    ┌────────────────────────────────────────────────┐
    │ /path/to/delta_table/                          │
    │ ├── _delta_log/                                │
    │ │   ├── 00000000000000000000.json  (V0)       │
    │ │   ├── 00000000000000000001.json  (V1)       │
    │ │   ├── 00000000000000000002.json  (V2)       │
    │ │   └── 00000000000000000010.checkpoint.parquet│
    │ ├── part-00000-xxx.snappy.parquet              │
    │ ├── part-00001-xxx.snappy.parquet              │
    │ └── part-00002-xxx.snappy.parquet              │
    └────────────────────────────────────────────────┘
    
    Transaction Log tracks:
    • Add/remove files
    • Schema changes
    • Metadata updates
    • Commit information
    
    ACID PROPERTIES:
    ---------------
    • Atomicity: All-or-nothing writes
    • Consistency: Schema enforcement
    • Isolation: Serializable isolation
    • Durability: Write-ahead log
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Delta Lake ACID Transactions")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create initial dataset
    print("\n📊 Creating initial Delta table...")
    df_initial = spark.createDataFrame([
        (1, "Alice", 30, 50000.0, "Engineering"),
        (2, "Bob", 35, 60000.0, "Sales"),
        (3, "Charlie", 28, 55000.0, "Engineering"),
        (4, "Diana", 32, 58000.0, "Marketing")
    ], ["id", "name", "age", "salary", "department"])
    
    delta_path = "/tmp/delta_table"
    
    # Write as Delta table (Version 0)
    print(f"\n💾 Writing initial version to {delta_path}")
    df_initial.write.format("delta").mode("overwrite").save(delta_path)
    
    print("\n📖 Reading Delta table:")
    df_read = spark.read.format("delta").load(delta_path)
    df_read.show()
    
    # UPDATE operation (Version 1)
    print("\n🔄 UPDATE: Giving Engineering 10% raise")
    print("   SQL: UPDATE table SET salary = salary * 1.1 WHERE department = 'Engineering'")
    
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, delta_path)
    
    delta_table.update(
        condition=col("department") == "Engineering",
        set={"salary": col("salary") * 1.1}
    )
    
    print("\n   After UPDATE:")
    spark.read.format("delta").load(delta_path).show()
    
    # DELETE operation (Version 2)
    print("\n🗑️  DELETE: Removing employees under 30")
    print("   SQL: DELETE FROM table WHERE age < 30")
    
    delta_table.delete(condition=col("age") < 30)
    
    print("\n   After DELETE:")
    spark.read.format("delta").load(delta_path).show()
    
    # MERGE (UPSERT) operation (Version 3)
    print("\n🔀 MERGE: Upserting new employees")
    df_new = spark.createDataFrame([
        (2, "Bob", 36, 65000.0, "Sales"),  # Update existing
        (5, "Eve", 29, 52000.0, "Engineering")  # Insert new
    ], ["id", "name", "age", "salary", "department"])
    
    print("\n   New data:")
    df_new.show()
    
    delta_table.alias("target").merge(
        df_new.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdate(set={
        "name": col("source.name"),
        "age": col("source.age"),
        "salary": col("source.salary"),
        "department": col("source.department")
    }).whenNotMatchedInsert(values={
        "id": col("source.id"),
        "name": col("source.name"),
        "age": col("source.age"),
        "salary": col("source.salary"),
        "department": col("source.department")
    }).execute()
    
    print("\n   After MERGE:")
    spark.read.format("delta").load(delta_path).show()
    
    # Show version history
    print("\n📜 VERSION HISTORY:")
    history = delta_table.history()
    history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
    
    # Time travel - read old version
    print("\n⏰ TIME TRAVEL: Reading version 0 (original data)")
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
    df_v0.show()
    
    print("\n" + "="*70)
    print("DELTA LAKE vs PARQUET: WHAT'S THE REAL DIFFERENCE?")
    print("="*70)
    
    print("\n🔴 OPERATIONS PARQUET **CANNOT** DO:")
    print("   " + "-"*60)
    print("\n   1. UPDATE (Modify existing rows):")
    print("      ❌ Parquet: Must rewrite ENTIRE file")
    print("      ✅ Delta Lake: Atomic UPDATE operation")
    print("\n      Example: Change 1 row in 1 million rows")
    print("      Parquet:    Rewrite all 1M rows (slow, expensive)")
    print("      Delta Lake: Track change in log (instant)")
    
    print("\n   2. DELETE (Remove rows):")
    print("      ❌ Parquet: Must rewrite file without deleted rows")
    print("      ✅ Delta Lake: Atomic DELETE operation")
    print("\n      Example: Delete inactive users")
    print("      Parquet:    Read all → filter → rewrite (hours)")
    print("      Delta Lake: DELETE WHERE active = false (seconds)")
    
    print("\n   3. MERGE/UPSERT (Insert or Update):")
    print("      ❌ Parquet: Complex manual logic required")
    print("         1. Read existing data")
    print("         2. Join with new data")
    print("         3. Deduplicate")
    print("         4. Overwrite (not atomic!)")
    print("      ✅ Delta Lake: Single MERGE command (atomic)")
    
    print("\n   4. TIME TRAVEL (Query old versions):")
    print("      ❌ Parquet: Keep copies manually (expensive)")
    print("         /data/2024-01-01/")
    print("         /data/2024-01-02/  ← Duplicate storage!")
    print("         /data/2024-01-03/")
    print("      ✅ Delta Lake: Automatic versioning (storage-efficient)")
    print("         Only stores changes, not full copies")
    
    print("\n   5. CONCURRENT WRITES (Multiple writers):")
    print("      ❌ Parquet: Race conditions!")
    print("         Writer 1: Overwrites file at 10:00 AM")
    print("         Writer 2: Overwrites file at 10:01 AM")
    print("         Result: Writer 1's data LOST!")
    print("      ✅ Delta Lake: Optimistic concurrency control")
    print("         Detects conflicts, retries automatically")
    
    print("\n   6. SCHEMA ENFORCEMENT:")
    print("      ❌ Parquet: Write any schema (causes errors later)")
    print("      ✅ Delta Lake: Rejects incompatible writes")
    
    print("\n   7. AUDIT TRAIL:")
    print("      ❌ Parquet: No history (who changed what?)")
    print("      ✅ Delta Lake: Full transaction log")
    
    # Demonstrate the practical difference with code
    print("\n" + "="*70)
    print("CONCRETE CODE COMPARISON")
    print("="*70)
    
    print("\n📝 SCENARIO: Update employee salary")
    print("\n   WITH PARQUET (Manual, Complex, NOT ATOMIC):")
    print("   " + "-"*60)
    print("""
   # Step 1: Read all data
   df = spark.read.parquet("/tmp/parquet_table")
   
   # Step 2: Update in memory
   df_updated = df.withColumn(
       "salary",
       when(col("id") == 2, col("salary") + 5000)
       .otherwise(col("salary"))
   )
   
   # Step 3: Overwrite (NOT ATOMIC!)
   df_updated.write.mode("overwrite").parquet("/tmp/parquet_table")
   
   # Problems:
   # ❌ If crash during write → corrupt data
   # ❌ Concurrent writers → data loss
   # ❌ Must rewrite ALL data (slow for large tables)
   # ❌ No rollback if error
   # ❌ No audit trail
    """)
    
    print("\n   WITH DELTA LAKE (Simple, Fast, ATOMIC):")
    print("   " + "-"*60)
    print("""
   from delta.tables import DeltaTable
   
   # Single atomic operation
   delta_table = DeltaTable.forPath(spark, "/tmp/delta_table")
   delta_table.update(
       condition = "id = 2",
       set = {"salary": col("salary") + 5000}
   )
   
   # Benefits:
   # ✅ Atomic (all-or-nothing)
   # ✅ Concurrent safe
   # ✅ Only updates changed rows (fast)
   # ✅ Auto-rollback on error
   # ✅ Full audit trail
    """)
    
    print("\n" + "="*70)
    print("STORAGE DIFFERENCE")
    print("="*70)
    
    print("\n   PARQUET: Just data files")
    print("   " + "-"*40)
    print("""
   /tmp/parquet_table/
   ├── part-00000.snappy.parquet  (200 MB)
   ├── part-00001.snappy.parquet  (200 MB)
   └── part-00002.snappy.parquet  (200 MB)
   
   Total: 600 MB
    """)
    
    print("\n   DELTA LAKE: Data files + Transaction log")
    print("   " + "-"*40)
    print("""
   /tmp/delta_table/
   ├── _delta_log/
   │   ├── 00000000000000000000.json  (Transaction 0: Initial write)
   │   ├── 00000000000000000001.json  (Transaction 1: UPDATE)
   │   ├── 00000000000000000002.json  (Transaction 2: DELETE)
   │   └── 00000000000000000003.json  (Transaction 3: MERGE)
   ├── part-00000.snappy.parquet  (200 MB)
   ├── part-00001.snappy.parquet  (200 MB)
   └── part-00002.snappy.parquet  (200 MB)
   
   Total: 600 MB (data) + 4 KB (transaction log)
   
   Transaction log overhead: ~0.001% ← Nearly zero!
    """)
    
    print("\n" + "="*70)
    print("WHEN TO USE EACH FORMAT")
    print("="*70)
    
    print("\n   USE PARQUET WHEN:")
    print("   " + "-"*40)
    print("   ✅ Write-once, read-many (immutable data)")
    print("   ✅ Single writer only")
    print("   ✅ No need for updates/deletes")
    print("   ✅ Maximum portability (works everywhere)")
    print("   ✅ Simpler stack (no Delta Lake dependency)")
    print("\n   Example: Historical logs, archives, ML training data")
    
    print("\n   USE DELTA LAKE WHEN:")
    print("   " + "-"*40)
    print("   ✅ Need UPDATE/DELETE operations")
    print("   ✅ Multiple concurrent writers")
    print("   ✅ Need ACID guarantees")
    print("   ✅ Need time travel (query old versions)")
    print("   ✅ Need audit trail (compliance)")
    print("   ✅ CDC (Change Data Capture) pipelines")
    print("   ✅ Real-time analytics with updates")
    print("\n   Example: Customer databases, inventory, financial transactions")
    
    print("\n💡 KEY INSIGHT:")
    print("   " + "="*60)
    print("   Delta Lake IS Parquet + Transaction Log")
    print("   " + "="*60)
    print("   • Same columnar storage (Parquet)")
    print("   • Same compression (Snappy/ZSTD)")
    print("   • Same performance for reads")
    print("   • PLUS: ACID transactions")
    print("   • PLUS: Time travel")
    print("   • PLUS: Schema evolution")
    print("   • Cost: ~4 KB transaction log (negligible!)")


def example_2_orc_hive_ecosystem():
    """
    Demonstrate ORC format optimized for Hive.
    
    WHAT IS THE HIVE ECOSYSTEM?
    ===========================
    Hive is a data warehouse system built on top of Hadoop that provides:
    • SQL interface to query data stored in HDFS
    • Metastore (centralized schema registry)
    • Query execution engine
    • Integration with Hadoop ecosystem (YARN, HDFS, HBase)
    
    HIVE ECOSYSTEM COMPONENTS:
    ┌─────────────────────────────────────────────────────────┐
    │                    Hive Architecture                    │
    ├─────────────────────────────────────────────────────────┤
    │                                                         │
    │  1. Hive Metastore (Schema & Metadata)                  │
    │     ┌────────────────────────────────────────┐         │
    │     │ Table: sales                           │         │
    │     │ Location: hdfs://warehouse/sales/      │         │
    │     │ Format: ORC                            │         │
    │     │ Partitions: year, month                │         │
    │     │ Columns: id, product, amount, date     │         │
    │     │ Statistics: row count, file sizes      │         │
    │     └────────────────────────────────────────┘         │
    │                                                         │
    │  2. Query Interface (HiveQL - SQL-like)                 │
    │     SELECT product, SUM(amount)                         │
    │     FROM sales                                          │
    │     WHERE year = 2024                                   │
    │     GROUP BY product;                                   │
    │                                                         │
    │  3. Execution Engine                                    │
    │     • Translates SQL → MapReduce/Tez/Spark jobs        │
    │     • Optimizes queries                                 │
    │     • Manages resources via YARN                        │
    │                                                         │
    │  4. Storage Layer (HDFS)                                │
    │     /warehouse/sales/year=2024/month=01/file1.orc       │
    │     /warehouse/sales/year=2024/month=02/file2.orc       │
    │                                                         │
    │  5. Integration Points                                  │
    │     • HBase: NoSQL database                             │
    │     • Kafka: Streaming ingestion                        │
    │     • Sqoop: RDBMS import/export                        │
    │     • Spark: Fast query engine                          │
    └─────────────────────────────────────────────────────────┘
    
    WHY ORC FOR HIVE?
    ----------------
    ORC was designed specifically for Hive:
    
    1. Hive Metastore Integration:
       • ORC stores statistics in file footer
       • Hive reads stats without scanning data
       • Faster query planning
    
    2. ACID Support in Hive:
       • ORC files support INSERT, UPDATE, DELETE
       • Row-level modifications tracked
       • Hive manages transaction log
    
    3. Predicate Pushdown:
       • ORC: Min/max in footer + bloom filters
       • Hive optimizer uses stats to skip files
       • Faster than Parquet for some queries
    
    4. Compression:
       • ORC: Slightly better compression than Parquet
       • Uses ZLIB by default (Parquet uses Snappy)
       • 5-10% smaller files
    
    5. Hive-Specific Optimizations:
       • Vectorized query execution
       • Dictionary encoding optimized for Hive
       • Direct integration with Hive operators
    
    HIVE ECOSYSTEM USE CASES:
    ------------------------
    ✅ Enterprise data warehouse on Hadoop
    ✅ Batch processing of large datasets
    ✅ Integration with existing Hadoop infrastructure
    ✅ Need ACID transactions (INSERT/UPDATE/DELETE)
    ✅ Centralized schema management (Metastore)
    ✅ SQL interface for analysts (HiveQL)
    
    ❌ Real-time/streaming (use Kafka + Spark)
    ❌ Cloud-native (use Parquet + Delta Lake)
    ❌ Machine learning (use Parquet for portability)
    
    ORC (Optimized Row Columnar) Format:
    -----------------------------------
    Similar to Parquet but optimized for Hive ecosystem.
    
    Structure:
    
    spark = create_spark_session()
    
    # Create dataset
    print("\n📊 Creating sample dataset...")
    df = spark.range(1, 100001) \
        .withColumn("name", expr("concat('User_', id)")) \
        .withColumn("age", (rand() * 50 + 20).cast("int")) \
        .withColumn("salary", (rand() * 100000 + 30000).cast("double")) \
        .withColumn("active", when(rand() > 0.3, True).otherwise(False))
    
    print(f"   Rows: {df.count():,}")
    df.show(5)
    
    # Write as ORC
    print("\n💾 Writing as ORC...")
    orc_path = "/tmp/data.orc"
    start_time = time.time()
    df.write.mode("overwrite").orc(orc_path)
    orc_write_time = time.time() - start_time
    print(f"   Write time: {orc_write_time:.2f} seconds")
    
    # Compare with Parquet
    print("\n💾 Writing as Parquet (for comparison)...")
    parquet_path = "/tmp/data_compare.parquet"
    start_time = time.time()
    df.write.mode("overwrite").parquet(parquet_path)
    parquet_write_time = time.time() - start_time
    print(f"   Write time: {parquet_write_time:.2f} seconds")
    
    # Compare file sizes
    print("\n📏 FILE SIZE COMPARISON:")
    import subprocess
    orc_size = subprocess.check_output(f"du -sh {orc_path}", shell=True).decode().split()[0]
    parquet_size = subprocess.check_output(f"du -sh {parquet_path}", shell=True).decode().split()[0]
    print(f"   ORC:     {orc_size}")
    print(f"   Parquet: {parquet_size}")
    print("   → ORC typically 10-15% smaller due to better compression")
    
    # Read performance
    print("\n⏱️  READ PERFORMANCE:")
    
    # ORC read
    start_time = time.time()
    df_orc = spark.read.orc(orc_path)
    orc_count = df_orc.filter(col("age") > 40).count()
    orc_read_time = time.time() - start_time
    print(f"   ORC read + filter:     {orc_read_time:.2f} seconds ({orc_count:,} rows)")
    
    # Parquet read
    start_time = time.time()
    df_parquet = spark.read.parquet(parquet_path)
    parquet_count = df_parquet.filter(col("age") > 40).count()
    parquet_read_time = time.time() - start_time
    print(f"   Parquet read + filter: {parquet_read_time:.2f} seconds ({parquet_count:,} rows)")
    
    # ORC compression codecs
    print("\n🗜️  ORC COMPRESSION CODECS:")
    codecs = ["NONE", "ZLIB", "SNAPPY", "LZ4", "ZSTD"]
    
    for codec in codecs:
        path = f"/tmp/orc_{codec.lower()}"
        df.limit(10000).write.mode("overwrite") \
            .option("compression", codec) \
            .orc(path)
        size = subprocess.check_output(f"du -sh {path}", shell=True).decode().split()[0]
        print(f"   {codec:8s}: {size}")
    
    print("\n💡 WHEN TO USE ORC:")
    print("   ✅ Working with Hive ecosystem")
    print("   ✅ Need slightly better compression than Parquet")
    print("   ✅ ACID transactions in Hive")
    print("   ✅ Hive metastore integration")
    print("   ❌ Use Parquet for non-Hive systems (more universal)")


def example_3_avro_schema_evolution():
    """
    Demonstrate Avro format with schema evolution.
    
    WHY ARE SCHEMA CHANGES FREQUENT?
    ================================
    In modern software development, schemas change constantly:
    
    1. API EVOLUTION:
       ┌─────────────────────────────────────────────┐
       │ API Version 1 (January 2024)                │
       │ {                                           │
       │   "user_id": 123,                           │
       │   "name": "Alice",                          │
       │   "email": "alice@example.com"              │
       │ }                                           │
       └─────────────────────────────────────────────┘
                         ↓
       ┌─────────────────────────────────────────────┐
       │ API Version 2 (March 2024)                  │
       │ {                                           │
       │   "user_id": 123,                           │
       │   "name": "Alice",                          │
       │   "email": "alice@example.com",             │
       │   "phone": "+1-555-0123",      ← NEW FIELD  │
       │   "country": "USA"              ← NEW FIELD  │
       │ }                                           │
       └─────────────────────────────────────────────┘
    
    2. BUSINESS REQUIREMENTS:
       • "We need to track customer age for marketing campaigns"
         → Add 'age' column to customer table
       
       • "GDPR compliance: delete user addresses"
         → Remove 'address' fields from schema
       
       • "Rename 'ssn' to 'national_id' for international customers"
         → Field rename with alias
    
    3. AGILE DEVELOPMENT:
       Week 1: Launch MVP with basic fields
       Week 2: Add analytics fields (click_count, session_duration)
       Week 3: Add A/B testing fields (experiment_id, variant)
       Week 4: Add recommendation fields (recommended_items)
       
       Result: Schema changes EVERY WEEK
    
    4. DATA INTEGRATION:
       • Merge data from acquisition: New company has extra fields
       • Add new data source: Different schema structure
       • External API changes: Must adapt to their schema updates
    
    5. MACHINE LEARNING EVOLUTION:
       • Model V1: Uses 10 features
       • Model V2: Adds 5 new features (requires new columns)
       • Model V3: Removes 3 irrelevant features
    
    THE SCHEMA EVOLUTION PROBLEM:
    ============================
    WITHOUT schema evolution support:
    ┌────────────────────────────────────────────────┐
    │ Problem: Schema changed, old data incompatible │
    ├────────────────────────────────────────────────┤
    │ Old data (Jan-Feb):                            │
    │   Parquet files with schema V1                 │
    │   ❌ Can't read with V2 schema                 │
    │                                                │
    │ New data (Mar):                                │
    │   Parquet files with schema V2                 │
    │   ❌ Can't read with V1 schema                 │
    │                                                │
    │ Solution: REPROCESS ALL HISTORICAL DATA        │
    │ Cost: $50,000+ in compute time                 │
    │ Time: 3 days to reprocess 100 TB               │
    └────────────────────────────────────────────────┘
    
    WITH schema evolution (Avro):
    ┌────────────────────────────────────────────────┐
    │ Solution: Read all data together!              │
    ├────────────────────────────────────────────────┤
    │ Old data (Jan-Feb):                            │
    │   Avro files with schema V1                    │
    │   ✅ Read with V2 schema (missing fields = null)│
    │                                                │
    │ New data (Mar):                                │
    │   Avro files with schema V2                    │
    │   ✅ Read with V1 schema (extra fields ignored) │
    │                                                │
    │ Solution: NO REPROCESSING NEEDED               │
    │ Cost: $0                                       │
    │ Time: Instant                                  │
    └────────────────────────────────────────────────┘
    
    AVRO SCHEMA EVOLUTION RULES:
    ---------------------------
    ✅ BACKWARD COMPATIBLE (New code reads old data):
       • Add optional field with default value
       • Delete field
    
    ✅ FORWARD COMPATIBLE (Old code reads new data):
       • Add field (old code ignores it)
       • Delete optional field
    
    ✅ FULL COMPATIBLE (Both directions):
       • Add optional field with default
       • Delete optional field
    
    ❌ BREAKING CHANGES (Not compatible):
       • Change field type (int → string)
       • Rename without alias
       • Remove required field
       • Add required field without default
    
    AVRO FUNDAMENTALS:
    -----------------
    Row-based binary format with embedded schema.
    
    Avro File Structure:
    ┌────────────────────────────────────────┐
    │ Header                                 │
    │ ├─ Magic bytes (Obj\x01)             │
    │ ├─ Schema (JSON)                      │
    │ └─ Codec (compression)                │
    │                                        │
    │ Data Block 1                           │
    │ ├─ Row 1 (binary)                     │
    │ ├─ Row 2 (binary)                     │
    │ └─ Sync marker                        │
    │                                        │
    │ Data Block 2                           │
    │ ...                                    │
    └────────────────────────────────────────┘
    
    Use Cases:
    • Kafka message serialization
    • Streaming data with schema changes
    • RPC systems (Apache protocols)
    • APIs with frequent updates
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: Avro Schema Evolution")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create initial dataset (Version 1 schema)
    print("\n📊 Creating dataset with V1 schema...")
    df_v1 = spark.createDataFrame([
        (1, "Alice", 30),
        (2, "Bob", 35),
        (3, "Charlie", 28)
    ], ["id", "name", "age"])
    
    print("\n   V1 Schema:")
    df_v1.printSchema()
    df_v1.show()
    
    # Write as Avro
    avro_path_v1 = "/tmp/avro_v1"
    print(f"\n💾 Writing V1 as Avro to {avro_path_v1}")
    df_v1.write.format("avro").mode("overwrite").save(avro_path_v1)
    
    # Evolve schema - add new field (Version 2)
    print("\n🔄 Evolving schema - adding 'salary' field")
    df_v2 = spark.createDataFrame([
        (4, "Diana", 32, 58000.0),
        (5, "Eve", 29, 52000.0)
    ], ["id", "name", "age", "salary"])
    
    print("\n   V2 Schema (with salary):")
    df_v2.printSchema()
    df_v2.show()
    
    avro_path_v2 = "/tmp/avro_v2"
    print(f"\n💾 Writing V2 as Avro to {avro_path_v2}")
    df_v2.write.format("avro").mode("overwrite").save(avro_path_v2)
    
    # Read both versions together (schema evolution)
    print("\n📖 Reading both V1 and V2 together (schema evolution)...")
    df_combined = spark.read.format("avro").load(avro_path_v1, avro_path_v2)
    
    print("\n   Combined data (V1 has null salary):")
    df_combined.orderBy("id").show()
    
    # Demonstrate Avro for streaming (Kafka use case simulation)
    print("\n📡 AVRO FOR STREAMING (Kafka simulation):")
    print("""
   Avro is popular for Kafka because:
   
   1. Compact binary format (smaller than JSON)
   2. Schema registry integration (centralized schema management)
   3. Schema evolution (backward/forward compatible)
   4. Fast serialization/deserialization
   
   Workflow:
   ┌──────────┐     ┌────────────────┐     ┌──────────┐
   │ Producer │────▶│ Kafka + Schema │────▶│ Consumer │
   │          │     │    Registry    │     │          │
   └──────────┘     └────────────────┘     └──────────┘
        │                   │                    │
        ▼                   ▼                    ▼
   Register schema    Store schema V1       Read with
   V1 → get ID       Store schema V2       compatible schema
                                           (V1 or V2)
    """)
    
    # Compare Avro vs Parquet file sizes
    print("\n📏 AVRO vs PARQUET SIZE:")
    df_test = spark.range(1, 50001) \
        .withColumn("value", rand() * 1000)
    
    avro_test_path = "/tmp/avro_test"
    parquet_test_path = "/tmp/parquet_test"
    
    df_test.write.format("avro").mode("overwrite").save(avro_test_path)
    df_test.write.mode("overwrite").parquet(parquet_test_path)
    
    import subprocess
    avro_size = subprocess.check_output(f"du -sh {avro_test_path}", shell=True).decode().split()[0]
    parquet_size = subprocess.check_output(f"du -sh {parquet_test_path}", shell=True).decode().split()[0]
    
    print(f"   Avro:    {avro_size} (row-based, good compression)")
    print(f"   Parquet: {parquet_size} (columnar, excellent compression)")
    
    print("\n💡 WHEN TO USE AVRO:")
    print("   ✅ Kafka streaming (industry standard)")
    print("   ✅ Schema evolution frequently")
    print("   ✅ RPC/messaging systems")
    print("   ✅ Row-based access patterns")
    print("   ❌ Use Parquet for analytics (better compression)")


def example_4_json_nested_hierarchical():
    """
    Demonstrate JSON format for nested and hierarchical data structures.
    
    WHAT ARE NESTED AND HIERARCHICAL DATA STRUCTURES?
    ================================================
    
    1. NESTED DATA (Objects within objects):
    ----------------------------------------
    {
      "user": {                    ← Nested object (1 level)
        "id": 123,
        "name": "Alice",
        "address": {               ← Nested object (2 levels deep)
          "street": "123 Main",
          "city": "NYC",
          "coordinates": {         ← Nested object (3 levels deep)
            "lat": 40.7128,
            "lon": -74.0060
          }
        }
      }
    }
    
    Access: user.address.city = "NYC"
            user.address.coordinates.lat = 40.7128
    
    2. HIERARCHICAL DATA (Tree structures):
    ---------------------------------------
    {
      "organization": {
        "name": "TechCorp",
        "departments": [          ← Array of departments
          {
            "name": "Engineering",
            "teams": [            ← Array of teams
              {
                "name": "Backend",
                "employees": [    ← Array of employees
                  {"id": 1, "name": "Alice"},
                  {"id": 2, "name": "Bob"}
                ]
              },
              {
                "name": "Frontend",
                "employees": [
                  {"id": 3, "name": "Charlie"}
                ]
              }
            ]
          },
          {
            "name": "Sales",
            "teams": [
              {
                "name": "Enterprise",
                "employees": [
                  {"id": 4, "name": "Diana"}
                ]
              }
            ]
          }
        ]
      }
    }
    
    Hierarchy (tree):
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
    
    3. REAL-WORLD EXAMPLES:
    ----------------------
    
    API RESPONSE (Nested):
    {
      "status": "success",
      "data": {
        "user": {...},
        "settings": {...},
        "metadata": {...}
      }
    }
    
    E-COMMERCE ORDER (Hierarchical):
    {
      "order_id": "ORD-12345",
      "customer": {
        "id": 789,
        "name": "Alice",
        "shipping_address": {...}
      },
      "items": [                  ← Array of items
        {
          "product_id": "P-001",
          "name": "Laptop",
          "price": 1200,
          "options": [            ← Nested array
            {"name": "RAM", "value": "16GB"},
            {"name": "Storage", "value": "512GB SSD"}
          ]
        },
        {
          "product_id": "P-002",
          "name": "Mouse",
          "price": 25
        }
      ],
      "payment": {
        "method": "credit_card",
        "card": {
          "last4": "1234",
          "brand": "Visa"
        }
      }
    }
    
    APPLICATION LOGS (Semi-structured):
    {
      "timestamp": "2024-01-15T10:30:00Z",
      "level": "ERROR",
      "message": "Database connection failed",
      "context": {
        "service": "api-server",
        "host": "prod-01",
        "error": {
          "type": "ConnectionTimeout",
          "details": {
            "timeout_ms": 5000,
            "retry_count": 3
          }
        }
      }
    }
    
    WHY JSON FOR NESTED/HIERARCHICAL DATA?
    -------------------------------------
    ✅ Natural representation of nested structures
    ✅ Flexible schema (add/remove fields easily)
    ✅ Human-readable (debugging, logging)
    ✅ Universal format (every language supports it)
    ✅ API standard (REST APIs use JSON)
    
    ❌ Large file sizes (text format, no compression)
    ❌ Slow parsing (text → binary conversion)
    ❌ No schema enforcement (errors at read time)
    ❌ Not optimized for analytics
    
    JSON CHARACTERISTICS:
    --------------------
    • Human-readable text format
    • Flexible nested structures
    • Schema-on-read (inferred)
    • Large file sizes (no compression by default)
    
    JSON vs Parquet:
    • JSON: Flexible schema, nested objects, human-readable
    • Parquet: Strict schema, flat/nested, binary, compressed
    
    JSON Use Cases:
    • API responses
    • Log files (application logs)
    • Configuration files
    • Semi-structured data with varying schemas
    """
    print("\n" + "="*70)
    print("EXAMPLE 4: JSON Semi-Structured Data")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create nested JSON structure
    print("\n📊 Creating nested JSON data...")
    df_json = spark.createDataFrame([
        (1, "Alice", {"street": "123 Main St", "city": "NYC", "zip": "10001"},
         [{"skill": "Python", "level": 9}, {"skill": "Spark", "level": 8}]),
        (2, "Bob", {"street": "456 Oak Ave", "city": "SF", "zip": "94102"},
         [{"skill": "Java", "level": 7}, {"skill": "Scala", "level": 8}]),
        (3, "Charlie", {"street": "789 Elm Rd", "city": "LA", "zip": "90001"},
         [{"skill": "SQL", "level": 9}])
    ], ["id", "name", "address", "skills"])
    
    print("\n   Schema:")
    df_json.printSchema()
    df_json.show(truncate=False)
    
    # Write as JSON
    json_path = "/tmp/data.json"
    print(f"\n💾 Writing as JSON to {json_path}")
    df_json.write.mode("overwrite").json(json_path)
    
    # Show raw JSON file
    print("\n📄 Raw JSON content (first 3 records):")
    import subprocess
    result = subprocess.check_output(f"head -n 3 {json_path}/part-*.json", shell=True).decode()
    print(result)
    
    # Read JSON back
    print("\n📖 Reading JSON back:")
    df_read = spark.read.json(json_path)
    df_read.show(truncate=False)
    
    # Query nested fields
    print("\n🔍 Querying nested fields:")
    print("   Query: Select name, city, and Python skill level")
    
    df_query = df_read.select(
        col("name"),
        col("address.city").alias("city"),
        expr("filter(skills, x -> x.skill = 'Python')[0].level").alias("python_level")
    )
    df_query.show()
    
    # Explode array
    print("\n📊 Exploding skills array (flatten):")
    df_exploded = df_read.select(
        col("name"),
        explode(col("skills")).alias("skill_info")
    ).select(
        col("name"),
        col("skill_info.skill").alias("skill"),
        col("skill_info.level").alias("level")
    )
    df_exploded.show()
    
    # Compare JSON vs Parquet
    print("\n📏 JSON vs PARQUET SIZE:")
    parquet_path = "/tmp/data_json_compare.parquet"
    df_json.write.mode("overwrite").parquet(parquet_path)
    
    import subprocess
    json_size = subprocess.check_output(f"du -sh {json_path}", shell=True).decode().split()[0]
    parquet_size = subprocess.check_output(f"du -sh {parquet_path}", shell=True).decode().split()[0]
    
    print(f"   JSON:    {json_size} (text, uncompressed)")
    print(f"   Parquet: {parquet_size} (binary, compressed)")
    print("   → Parquet typically 5-10× smaller")
    
    # JSON Lines format (newline-delimited JSON)
    print("\n📝 JSON LINES FORMAT:")
    print("""
   Standard JSON: Entire file is one JSON object/array
   JSON Lines:    One JSON object per line (better for streaming)
   
   Example:
   {"id": 1, "name": "Alice"}
   {"id": 2, "name": "Bob"}
   {"id": 3, "name": "Charlie"}
   
   Spark uses JSON Lines by default (one record per line).
    """)
    
    print("\n💡 WHEN TO USE JSON:")
    print("   ✅ API integration (REST APIs)")
    print("   ✅ Application logs (structured logging)")
    print("   ✅ Configuration files")
    print("   ✅ Semi-structured data with varying schemas")
    print("   ✅ Human readability important")
    print("   ❌ Use Parquet for production data lakes (much smaller)")


def example_5_hdf5_scientific_arrays_matrices():
    """
    Demonstrate HDF5 for scientific multi-dimensional arrays and matrices.
    
    WHAT ARE SCIENTIFIC ARRAYS AND MATRICES?
    ========================================
    
    1. ARRAYS (1D, 2D, 3D, ..., N-dimensional):
    ------------------------------------------
    
    1D Array (Vector):
    temperature = [72.5, 73.1, 71.8, 74.2, 73.9]
    Length: 5
    Access: temperature[2] = 71.8
    
    2D Array (Matrix):
    image = [
      [255, 128,  64],    ← Row 0
      [192, 224, 160],    ← Row 1
      [ 32,  96, 128]     ← Row 2
    ]
    Shape: (3 rows, 3 columns) = 3×3 matrix
    Access: image[1, 2] = 160 (row 1, column 2)
    
    3D Array (Spatial + Time):
    ┌────────────────────────────────────────┐
    │ temperature[latitude, longitude, time] │
    ├────────────────────────────────────────┤
    │ Dimensions:                            │
    │ • Latitude:  1000 points (90°N to 90°S)│
    │ • Longitude: 2000 points (180°W to 180°E)│
    │ • Time:      365 days                  │
    │                                        │
    │ Shape: (1000, 2000, 365)               │
    │ Total elements: 730,000,000            │
    │                                        │
    │ Access examples:                       │
    │ • temp[500, 1000, 180]                 │
    │   → Temperature at specific point on day 180│
    │                                        │
    │ • temp[500, 1000, :]                   │
    │   → Temperature time series at one location│
    │                                        │
    │ • temp[:, :, 180]                      │
    │   → Entire spatial grid for day 180   │
    └────────────────────────────────────────┘
    
    Visualization:
    
         Time (365 days)
          ↗
         /
        /__________ Longitude (2000 points)
        |
        |
        ↓
     Latitude
    (1000 points)
    
    Each "cell" contains a temperature value.
    
    4D Array (Spatial + Time + Variable):
    climate_data[lat, lon, time, variable]
    • Latitude: 1000
    • Longitude: 2000
    • Time: 365 days
    • Variable: 4 (temperature, humidity, pressure, wind_speed)
    Shape: (1000, 2000, 365, 4)
    
    2. REAL-WORLD SCIENTIFIC DATA EXAMPLES:
    ---------------------------------------
    
    MEDICAL IMAGING (3D MRI scan):
    mri_scan[x, y, z]
    • x: 256 slices (left-right)
    • y: 256 slices (front-back)
    • z: 128 slices (top-bottom)
    Shape: (256, 256, 128)
    Size: 8.4 million voxels (3D pixels)
    
    ASTRONOMY (Telescope images):
    sky_image[x, y, wavelength, time]
    • x, y: 4096×4096 pixels (spatial)
    • wavelength: 100 channels (different light wavelengths)
    • time: 1000 exposures
    Shape: (4096, 4096, 100, 1000)
    Size: 1.67 trillion values!
    
    PARTICLE PHYSICS (Detector readings):
    detector_data[event, particle, measurement]
    • event: 1,000,000 collisions
    • particle: 500 particles per event
    • measurement: 20 properties (position, momentum, energy, etc.)
    Shape: (1000000, 500, 20)
    
    CLIMATE MODELING:
    ocean_temperature[depth, lat, lon, time]
    • depth: 100 levels (surface to 5000m deep)
    • lat: 180 points
    • lon: 360 points
    • time: 365 days × 10 years = 3650
    Shape: (100, 180, 360, 3650)
    
    GENOMICS (Gene expression):
    expression_data[gene, cell, condition]
    • gene: 20,000 genes
    • cell: 10,000 cells
    • condition: 50 experimental conditions
    Shape: (20000, 10000, 50)
    
    3. MATRIX OPERATIONS:
    --------------------
    
    Matrix Multiplication (Linear Algebra):
    A = [[1, 2],      B = [[5, 6],
         [3, 4]]           [7, 8]]
    
    C = A × B = [[19, 22],
                 [43, 50]]
    
    Used in: Machine learning, simulations, physics
    
    Element-wise Operations:
    temperature_celsius = [20, 25, 30]
    temperature_fahrenheit = temperature_celsius * 1.8 + 32
    Result: [68, 77, 86]
    
    Slicing (Access sub-arrays):
    data[0:10, 0:10, :]    ← First 10×10 spatial points, all times
    data[:, :, 0]          ← Entire spatial grid at time 0
    data[50, 100, :]       ← Time series at one location
    
    4. WHY HDF5 FOR SCIENTIFIC DATA?
    --------------------------------
    ✅ Efficient storage of N-dimensional arrays
    ✅ Chunking: Fast access to array slices
    ✅ Compression: GZIP, LZF (smaller files)
    ✅ Partial reads: Read subset without loading all data
    ✅ Metadata: Store units, calibration, etc.
    ✅ Industry standard: NASA, CERN, NIH use HDF5
    
    ❌ Single-machine focused (not distributed)
    ❌ Limited Spark support (need h5spark library)
    ❌ Complex format (steep learning curve)
    
    5. HDF5 vs PARQUET FOR SCIENTIFIC DATA:
    --------------------------------------
    
    HDF5:
    • Optimized for: Array slicing (get slice of 3D array)
    • Storage: N-dimensional arrays natively
    • Access: Fast random access to array slices
    • Use case: Single-machine scientific computing
    
    Parquet:
    • Optimized for: Column scanning (get all values of one column)
    • Storage: Tabular data (rows × columns)
    • Access: Fast column reads, slow row access
    • Use case: Distributed analytics on tabular data
    
    Example:
    Get all temperatures for one location over time:
    • HDF5:    temp[500, 1000, :] → Fast (single slice)
    • Parquet: SELECT temp WHERE lat=500 AND lon=1000 → Slow (scan all)
    
    Get average temperature across all locations:
    • HDF5:    np.mean(temp) → Slow (not optimized for aggregation)
    • Parquet: SELECT AVG(temp) → Fast (columnar aggregation)
    
    HDF5 (Hierarchical Data Format 5):
    ----------------------------------
    Designed for scientific computing and large numerical arrays.
    
    HDF5 Structure:
    ┌────────────────────────────────────────┐
    │ HDF5 File                              │
    ├────────────────────────────────────────┤
    │ /                     (root group)     │
    │ ├── /temperature     (3D dataset)     │
    │ │   ├── Datatype: float64            │
    │ │   ├── Shape: (1000, 1000, 365)     │
    │ │   ├── Chunks: (100, 100, 1)        │
    │ │   └── Compression: gzip            │
    │ ├── /pressure        (3D dataset)     │
    │ └── /metadata        (attributes)     │
    └────────────────────────────────────────┘
    
    HDF5 Features:
    • Hierarchical structure (like filesystem)
    • Multi-dimensional arrays
    • Chunked storage (efficient slicing)
    • Fast random access
    • Metadata attributes
    
    HDF5 vs Parquet:
    • HDF5: Arrays, single-machine, NumPy-centric
    • Parquet: Tables, distributed, Spark-centric
    
    NOTE: HDF5 is NOT natively supported by Spark.
    This example shows conceptual comparison.
    """
    print("\n" + "="*70)
    print("EXAMPLE 5: HDF5 Scientific Arrays (Conceptual)")
    print("="*70)
    
    spark = create_spark_session()
    
    print("\n⚠️  NOTE: HDF5 is NOT natively supported by PySpark.")
    print("   This example demonstrates the concept and shows conversion.")
    
    # Create sample data (simulating sensor readings)
    print("\n📊 Creating sample 3D data (sensors × time × measurements)...")
    print("   Simulating: 100 sensors × 365 days × 24 hours")
    
    # In HDF5, this would be a 3D array
    # In Spark, we represent it as a table
    
    df_sensors = spark.range(0, 100).alias("sensor_id") \
        .crossJoin(spark.range(0, 365).alias("day")) \
        .crossJoin(spark.range(0, 24).alias("hour")) \
        .withColumn("temperature", rand() * 30 + 10) \
        .withColumn("humidity", rand() * 100) \
        .select("sensor_id", "day", "hour", "temperature", "humidity")
    
    print(f"\n   Total records: {df_sensors.count():,}")
    print("   (100 sensors × 365 days × 24 hours = 876,000 records)")
    df_sensors.show(5)
    
    # Conceptual HDF5 structure
    print("\n📦 HDF5 CONCEPTUAL STRUCTURE:")
    print("""
   In HDF5, this would be stored as:
   
   /sensors.h5
   ├── /temperature [100, 365, 24]  (3D array)
   │   └── chunks: (10, 1, 24)  [10 sensors, 1 day, all hours]
   ├── /humidity    [100, 365, 24]  (3D array)
   └── /metadata
       ├── units: "celsius"
       └── source: "weather_stations"
   
   Access patterns:
   • temps[50, :, :]      → All data for sensor 50
   • temps[:, 180, 12]    → All sensors at day 180, hour 12
   • temps[0:10, 0:7, :]  → First 10 sensors, first week
    """)
    
    # PySpark equivalent using Parquet
    print("\n💾 PYSPARK/PARQUET EQUIVALENT:")
    parquet_path = "/tmp/sensors.parquet"
    
    print(f"   Writing as partitioned Parquet to {parquet_path}")
    df_sensors.write.mode("overwrite") \
        .partitionBy("day") \
        .parquet(parquet_path)
    
    print("\n   Querying specific sensor (equivalent to HDF5 slice):")
    df_sensor_50 = spark.read.parquet(parquet_path) \
        .filter(col("sensor_id") == 50)
    print(f"   Sensor 50 data: {df_sensor_50.count()} records")
    df_sensor_50.orderBy("day", "hour").show(5)
    
    print("\n   Querying specific time (day 180, hour 12):")
    df_snapshot = spark.read.parquet(parquet_path) \
        .filter((col("day") == 180) & (col("hour") == 12))
    print(f"   Snapshot: {df_snapshot.count()} sensors")
    df_snapshot.show(5)
    
    # Performance comparison
    print("\n⚡ PERFORMANCE COMPARISON:")
    print("""
   HDF5 (single machine):
   • Random access: Very fast (memory-mapped I/O)
   • Array slicing: Optimized for NumPy operations
   • Parallel: Limited (multi-threading, not distributed)
   • Scale: Limited by single machine memory
   
   Parquet + Spark (distributed):
   • Random access: Slower (file I/O, not memory-mapped)
   • Query: Optimized for SQL-like operations
   • Parallel: Excellent (distributed across cluster)
   • Scale: Unlimited (petabytes)
   
   Rule of thumb:
   • HDF5: Single machine, < 1 TB, array operations, NumPy
   • Parquet + Spark: Distributed, > 1 TB, SQL queries, analytics
    """)
    
    # Conversion workflow
    print("\n🔄 HDF5 ↔ PARQUET CONVERSION WORKFLOW:")
    print("""
   Option 1: Convert HDF5 to Parquet (for Spark processing)
   
   import h5py
   import pandas as pd
   
   # Read HDF5
   with h5py.File('data.h5', 'r') as f:
       temps = f['temperature'][:]  # Load entire array
   
   # Convert to DataFrame
   df_pd = pd.DataFrame({
       'sensor_id': ...,
       'day': ...,
       'temperature': temps.flatten()
   })
   
   # Write to Parquet
   df_spark = spark.createDataFrame(df_pd)
   df_spark.write.parquet('output.parquet')
   
   
   Option 2: Process HDF5 directly (for small data)
   
   from pyspark.sql.types import Row
   
   # Read HDF5 and yield rows
   def read_hdf5(path):
       with h5py.File(path, 'r') as f:
           data = f['dataset'][:]
           for i, row in enumerate(data):
               yield Row(id=i, value=float(row))
   
   # Create Spark DataFrame
   rdd = spark.sparkContext.parallelize(read_hdf5('data.h5'))
   df = spark.createDataFrame(rdd)
    """)
    
    print("\n💡 WHEN TO USE HDF5:")
    print("   ✅ Scientific computing (physics, astronomy, biology)")
    print("   ✅ Multi-dimensional numerical arrays")
    print("   ✅ Single-machine workflows with NumPy")
    print("   ✅ Fast random access to array slices")
    print("   ✅ Hierarchical data organization")
    print("   ❌ Use Parquet for distributed analytics with Spark")


def example_6_format_comparison_benchmark():
    """
    Comprehensive benchmark comparing all formats.
    """
    print("\n" + "="*70)
    print("EXAMPLE 6: Format Comparison Benchmark")
    print("="*70)
    
    spark = create_spark_session()
    
    # Create test dataset
    print("\n📊 Creating benchmark dataset (50,000 rows)...")
    df = spark.range(1, 50001) \
        .withColumn("name", expr("concat('User_', id)")) \
        .withColumn("age", (rand() * 50 + 20).cast("int")) \
        .withColumn("salary", (rand() * 100000 + 30000).cast("double")) \
        .withColumn("department", expr("concat('Dept_', cast(id % 10 as string))"))
    
    print(f"   Rows: {df.count():,}")
    print(f"   Columns: {len(df.columns)}")
    
    # Test formats
    formats = {
        "parquet": {"write": lambda: df.write.mode("overwrite").parquet("/tmp/bench_parquet")},
        "orc": {"write": lambda: df.write.mode("overwrite").orc("/tmp/bench_orc")},
        "avro": {"write": lambda: df.write.format("avro").mode("overwrite").save("/tmp/bench_avro")},
        "json": {"write": lambda: df.write.mode("overwrite").json("/tmp/bench_json")},
    }
    
    print("\n⏱️  BENCHMARK RESULTS:")
    print("   " + "─" * 70)
    
    results = []
    for fmt, ops in formats.items():
        path = f"/tmp/bench_{fmt}"
        
        # Write
        start = time.time()
        ops["write"]()
        write_time = time.time() - start
        
        # Get size
        import subprocess
        size_output = subprocess.check_output(f"du -sh {path}", shell=True).decode()
        size = size_output.split()[0]
        
        # Read
        start = time.time()
        if fmt == "parquet":
            df_read = spark.read.parquet(path)
        elif fmt == "orc":
            df_read = spark.read.orc(path)
        elif fmt == "avro":
            df_read = spark.read.format("avro").load(path)
        elif fmt == "json":
            df_read = spark.read.json(path)
        
        count = df_read.count()
        read_time = time.time() - start
        
        results.append({
            "format": fmt.upper(),
            "size": size,
            "write_time": write_time,
            "read_time": read_time
        })
        
        print(f"\n   {fmt.upper()}:")
        print(f"      Size:  {size:>8s}")
        print(f"      Write: {write_time:>6.2f}s")
        print(f"      Read:  {read_time:>6.2f}s")
    
    print("\n   " + "─" * 70)
    
    print("\n📊 SUMMARY TABLE:")
    print("""
   ┌───────────┬──────────┬─────────────┬────────────┬─────────────────────┐
   │ Format    │ Size     │ Write Speed │ Read Speed │ Best For            │
   ├───────────┼──────────┼─────────────┼────────────┼─────────────────────┤
   │ Parquet   │ Smallest │ Medium      │ Fast       │ Analytics (best!)   │
   │ ORC       │ Smaller  │ Medium      │ Fast       │ Hive ecosystem      │
   │ Avro      │ Medium   │ Fast        │ Medium     │ Streaming, schema   │
   │ JSON      │ Largest  │ Fast        │ Slow       │ APIs, human-read    │
   │ Delta     │ Smallest*│ Medium      │ Fast       │ ACID + time travel  │
   │ HDF5      │ Medium   │ Very Fast** │ Very Fast**│ Scientific arrays   │
   └───────────┴──────────┴─────────────┴────────────┴─────────────────────┘
   
   * Delta = Parquet + transaction log
   ** HDF5 on single machine (not distributed)
    """)
    
    print("\n🎯 FORMAT SELECTION GUIDE:")
    print("""
   Production Data Lake: 
   → Delta Lake (ACID) or Parquet (simple)
   
   Hive Warehouse:
   → ORC
   
   Kafka Streaming:
   → Avro
   
   API Integration:
   → JSON
   
   Scientific Computing:
   → HDF5 (single machine) or Parquet (distributed)
   
   ACID Transactions:
   → Delta Lake
   
   Maximum Compression:
   → ORC or Parquet with ZSTD
    """)


def main():
    """
    Run all file format examples.
    """
    print("\n" + "="*70)
    print(" COMPREHENSIVE FILE FORMAT EXAMPLES ")
    print("="*70)
    
    print("""
This module demonstrates 5 different file formats:

1. Delta Lake  → ACID transactions, time travel, updates/deletes
2. ORC         → Hive integration, better compression
3. Avro        → Schema evolution, Kafka streaming
4. JSON        → Semi-structured, API integration
5. HDF5        → Scientific arrays, NumPy integration

Each format has specific use cases where it excels.
    """)
    
    try:
        example_1_delta_lake_acid_transactions()
        example_2_orc_hive_ecosystem()
        example_3_avro_schema_evolution()
        example_4_json_nested_hierarchical()
        example_5_hdf5_scientific_arrays_matrices()
        example_6_format_comparison_benchmark()
        
        print("\n" + "="*70)
        print("✅ ALL EXAMPLES COMPLETED SUCCESSFULLY")
        print("="*70)
        
        print("\n📚 KEY TAKEAWAYS:")
        print("   1. Delta Lake: Best for data lakes with ACID requirements")
        print("   2. ORC: Best for Hive ecosystem")
        print("   3. Avro: Best for Kafka and schema evolution")
        print("   4. JSON: Best for APIs and semi-structured data")
        print("   5. HDF5: Best for scientific arrays (use Parquet for Spark)")
        print("   6. Parquet: Best default choice for Spark analytics")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
