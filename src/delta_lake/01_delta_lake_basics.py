"""
================================================================================
Delta Lake Integration with PySpark
================================================================================

PURPOSE:
--------
Implement ACID transactions, time travel, and schema evolution on data lakes
using Delta Lake, bringing database reliability to big data processing.

WHAT THIS DOES:
---------------
- ACID transactions on S3/HDFS (atomicity, consistency, isolation, durability)
- Time travel (query historical versions)
- Schema evolution (add/change columns safely)
- UPSERT/MERGE operations (update existing records)
- Data validation and quality checks
- Concurrent read/write without conflicts

WHY DELTA LAKE:
---------------
DATA LAKE PROBLEMS:
- No ACID transactions (partial writes on failure)
- Cannot update/delete records (append-only)
- Schema changes break pipelines
- Concurrent writes cause data corruption
- No audit trail (who changed what when)

DELTA LAKE SOLUTIONS:
- ACID transactions (all-or-nothing writes)
- UPSERT/MERGE/DELETE operations
- Schema evolution (backward/forward compatible)
- Time travel (query old versions)
- Audit history (track all changes)
- 10x faster queries (data skipping, Z-ordering)

REAL-WORLD USE CASES:
- Streaming CDC (Change Data Capture) from databases
- Slowly Changing Dimensions (SCD Type 2)
- Data quality enforcement
- Regulatory compliance (GDPR, audit trails)
- Machine learning feature stores

HOW IT WORKS:
-------------
1. DATA LAYER:
   - Parquet files for data storage
   - JSON transaction log (_delta_log/)
   - Metadata tracks file versions

2. TRANSACTION LOG:
   - Records every operation (add/remove files)
   - Enables ACID guarantees
   - Powers time travel

3. OPTIMIZATIONS:
   - Data skipping (min/max statistics)
   - Z-ordering (co-locate related data)
   - Compaction (merge small files)
   - Vacuum (delete old versions)

KEY FEATURES:
-------------

1. TIME TRAVEL:
   - Query historical data snapshots
   - Rollback to previous versions
   - Audit changes over time
   - Example: df = spark.read.format("delta").option("versionAsOf", 5).load("/data")

2. SCHEMA EVOLUTION:
   - Add columns without rewriting data
   - Rename columns safely
   - Change data types with validation
   - Example: .option("mergeSchema", "true")

3. UPSERT (MERGE):
   - Update existing records
   - Insert new records
   - Delete based on conditions
   - Example: deltaTable.merge(updates, "id = id").whenMatched().update().execute()

4. OPTIMIZE:
   - Compact small files (improve query performance)
   - Z-order clustering (co-locate related data)
   - Example: deltaTable.optimize().executeZOrderBy("date", "customer_id")

5. VACUUM:
   - Delete old file versions
   - Reclaim storage space
   - Configurable retention period
   - Example: deltaTable.vacuum(retentionHours=168)  # 7 days

PERFORMANCE:
------------
Parquet (Without Delta):
- Queries scan all files
- No updates/deletes (rewri

te entire partition)
- Schema changes break readers
- Concurrent writes cause corruption

Delta Lake:
- 10x faster queries (data skipping)
- Updates in seconds (not hours)
- Schema evolution transparent
- Concurrent read/write safe
- Example: 10M row update: 2 min (vs 30 min full rewrite)

ACID TRANSACTIONS:
------------------
WITHOUT DELTA:
1. Write part of data
2. Job fails
3. Corrupted state (partial data visible)
4. Manual cleanup required

WITH DELTA:
1. Write all data to temp location
2. Atomically commit transaction log
3. On failure: nothing changes (automatic rollback)
4. Readers always see consistent state

WHEN TO USE:
------------
✅ Need updates/deletes (not append-only)
✅ Multiple concurrent writers
✅ Streaming + batch on same table
✅ Schema will evolve over time
✅ Regulatory compliance (audit trail)
✅ Machine learning (feature engineering)
❌ Write-once, read-many (plain Parquet is fine)
❌ Extremely high write throughput (>100K rows/sec)

COMPARISON:
-----------
|                  | Parquet | Delta Lake | Apache Hudi | Apache Iceberg |
|------------------|---------|------------|-------------|----------------|
| ACID Transactions| ❌      | ✅         | ✅          | ✅             |
| Time Travel      | ❌      | ✅         | ✅          | ✅             |
| UPSERT/MERGE     | ❌      | ✅         | ✅          | ✅             |
| Schema Evolution | Limited | ✅         | ✅          | ✅             |
| Maturity         | High    | High       | Medium      | Medium         |
| Adoption         | Ubiquitous | Growing | Moderate    | Growing        |

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
import shutil
import os

def create_spark_with_delta():
    """
    Create Spark session with Delta Lake support.
    
    WHAT: Initialize Spark with Delta Lake extensions
    WHY: Enable ACID transactions and time travel
    HOW: Add Delta Lake JARs and configurations
    """
    builder = SparkSession.builder         .appName("DeltaLakeIntegration")         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")         .config("spark.sql.shuffle.partitions", "2")         .master("local[*]")
    
    # Configure Delta Lake
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    print("✅ Spark with Delta Lake initialized")
    print(f"   Delta Lake version: {spark.conf.get('spark.databricks.delta.properties.defaults.dataSkippingNumIndexedCols', 'N/A')}")
    
    return spark


def example_1_basic_delta_operations(spark):
    """
    Example 1: Basic Delta Lake operations (create, read, append).
    
    WHAT THIS DOES:
    ---------------
    Demonstrates creating a Delta table, writing data, and reading with
    automatic schema inference and ACID guarantees.
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: BASIC DELTA LAKE OPERATIONS")
    print("=" * 80)
    
    delta_path = "/tmp/delta_customers"
    
    # Clean up previous runs
    if os.path.exists(delta_path):
        shutil.rmtree(delta_path)
    
    # WHAT: Create initial customer data
    # WHY: Simulate initial data load
    # HOW: Create DataFrame and write in Delta format
    customers = spark.createDataFrame([
        (1, "Alice", "alice@email.com", "2023-01-15"),
        (2, "Bob", "bob@email.com", "2023-02-20"),
        (3, "Charlie", "charlie@email.com", "2023-03-10"),
    ], ["customer_id", "name", "email", "signup_date"])
    
    print("\n📝 Writing initial data to Delta Lake...")
    customers.write.format("delta").mode("overwrite").save(delta_path)
    print(f"   ✅ Wrote {customers.count()} customers to {delta_path}")
    
    # WHAT: Read Delta table
    # WHY: Verify data was written correctly
    # HOW: Use delta format reader
    df = spark.read.format("delta").load(delta_path)
    print("\n📊 Initial Delta table:")
    df.show()
    
    # WHAT: Append new customers
    # WHY: Demonstrate ACID append operation
    # HOW: Use append mode with Delta format
    new_customers = spark.createDataFrame([
        (4, "Diana", "diana@email.com", "2023-04-05"),
        (5, "Eve", "eve@email.com", "2023-05-12"),
    ], ["customer_id", "name", "email", "signup_date"])
    
    print("\n📝 Appending new customers...")
    new_customers.write.format("delta").mode("append").save(delta_path)
    print(f"   ✅ Appended {new_customers.count()} new customers")
    
    # WHAT: Read updated table
    df_updated = spark.read.format("delta").load(delta_path)
    print("\n📊 Updated Delta table:")
    df_updated.show()
    
    print(f"\n📈 Total customers: {df_updated.count()}")
    
    return delta_path


def example_2_time_travel(spark, delta_path):
    """
    Example 2: Time travel - query historical versions.
    
    WHAT THIS DOES:
    ---------------
    Demonstrates Delta Lake's time travel capability to query data as it
    existed at previous points in time or specific versions.
    
    WHY TIME TRAVEL:
    ----------------
    - Audit data changes
    - Rollback incorrect updates
    - A/B testing (compare versions)
    - Reproduce ML training datasets
    - Regulatory compliance
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 2: TIME TRAVEL")
    print("=" * 80)
    
    # WHAT: Load Delta table for operations
    deltaTable = DeltaTable.forPath(spark, delta_path)
    
    # WHAT: View transaction history
    # WHY: See all operations performed on table
    # HOW: Query _delta_log transaction log
    print("\n📜 Transaction History:")
    history = deltaTable.history()
    history.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
    
    # WHAT: Query version 0 (initial data)
    # WHY: See data before append operation
    # HOW: Use versionAsOf option
    print("\n⏪ Query Version 0 (initial state - 3 customers):")
    df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
    df_v0.show()
    print(f"   Records in version 0: {df_v0.count()}")
    
    # WHAT: Query version 1 (after append)
    # WHY: See data after new customers added
    print("\n⏩ Query Version 1 (after append - 5 customers):")
    df_v1 = spark.read.format("delta").option("versionAsOf", 1).load(delta_path)
    df_v1.show()
    print(f"   Records in version 1: {df_v1.count()}")
    
    # WHAT: Query current version (same as version 1)
    print("\n📊 Current version (latest):")
    df_current = spark.read.format("delta").load(delta_path)
    print(f"   Records in current version: {df_current.count()}")
    
    print("\n💡 Time Travel Benefits:")
    print("   • Audit all changes")
    print("   • Rollback to previous state")
    print("   • Compare versions for debugging")
    print("   • Reproduce ML experiments")


def example_3_upsert_merge(spark, delta_path):
    """
    Example 3: UPSERT (UPDATE + INSERT) using MERGE.
    
    WHAT THIS DOES:
    ---------------
    Demonstrates Delta Lake's MERGE operation to update existing records
    and insert new ones in a single atomic transaction.
    
    WHY UPSERT:
    -----------
    - Change Data Capture (CDC) from databases
    - Slowly Changing Dimensions (SCD)
    - Real-time data synchronization
    - Deduplication
    
    MERGE LOGIC:
    ------------
    WHEN MATCHED: Update existing record
    WHEN NOT MATCHED: Insert new record
    WHEN NOT MATCHED BY SOURCE: Optional delete
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 3: UPSERT (MERGE) OPERATIONS")
    print("=" * 80)
    
    deltaTable = DeltaTable.forPath(spark, delta_path)
    
    # WHAT: Create updates and new records
    # WHY: Simulate CDC feed from transactional database
    # HOW: Mix of existing IDs (updates) and new IDs (inserts)
    updates = spark.createDataFrame([
        (2, "Bob Smith", "bob.smith@newemail.com", "2023-02-20"),  # Update Bob's email
        (3, "Chuck", "chuck@email.com", "2023-03-10"),  # Update Charlie's name to Chuck
        (6, "Frank", "frank@email.com", "2023-06-15"),  # New customer
        (7, "Grace", "grace@email.com", "2023-07-20"),  # New customer
    ], ["customer_id", "name", "email", "signup_date"])
    
    print("\n📝 Changes to apply (2 updates + 2 inserts):")
    updates.show()
    
    # WHAT: Execute MERGE operation
    # WHY: Apply updates and inserts atomically
    # HOW: WHEN MATCHED update, WHEN NOT MATCHED insert
    print("\n🔄 Executing MERGE operation...")
    
    deltaTable.alias("target").merge(
        updates.alias("source"),
        "target.customer_id = source.customer_id"  # Join condition
    ).whenMatchedUpdate(set={
        "name": "source.name",
        "email": "source.email"
    }).whenNotMatchedInsert(values={
        "customer_id": "source.customer_id",
        "name": "source.name",
        "email": "source.email",
        "signup_date": "source.signup_date"
    }).execute()
    
    print("   ✅ MERGE complete!")
    
    # WHAT: Verify results
    print("\n📊 Table after MERGE:")
    result = spark.read.format("delta").load(delta_path)
    result.orderBy("customer_id").show()
    
    print(f"\n📈 Total customers: {result.count()} (was 5, now 7)")
    print("   • Bob's email updated")
    print("   • Charlie renamed to Chuck")
    print("   • Frank and Grace inserted")
    
    # WHAT: Check transaction history
    print("\n📜 Latest operations:")
    deltaTable.history().select("version", "operation", "operationMetrics").show(3, truncate=False)


def example_4_schema_evolution(spark, delta_path):
    """
    Example 4: Schema evolution - add columns without breaking existing data.
    
    WHAT THIS DOES:
    ---------------
    Demonstrates Delta Lake's schema evolution capability to add new columns
    to existing tables without rewriting all data.
    
    WHY SCHEMA EVOLUTION:
    ---------------------
    - Business requirements change
    - Add features without downtime
    - Backward compatibility
    - No need to reprocess historical data
    
    MERGE SCHEMA:
    -------------
    - Automatically adds new columns
    - Existing data gets NULL for new columns
    - Forward compatible (old readers still work)
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 4: SCHEMA EVOLUTION")
    print("=" * 80)
    
    # WHAT: Show current schema
    print("\n📋 Current schema:")
    current_df = spark.read.format("delta").load(delta_path)
    current_df.printSchema()
    
    # WHAT: Add new column (loyalty_points) to new data
    # WHY: Business wants to track customer loyalty
    # HOW: Include new column in DataFrame, enable mergeSchema
    new_data_with_column = spark.createDataFrame([
        (8, "Henry", "henry@email.com", "2023-08-01", 100),
        (9, "Iris", "iris@email.com", "2023-09-05", 50),
    ], ["customer_id", "name", "email", "signup_date", "loyalty_points"])
    
    print("\n📝 New data with additional column (loyalty_points):")
    new_data_with_column.show()
    
    print("\n🔄 Writing with schema merge enabled...")
    new_data_with_column.write.format("delta")         .mode("append")         .option("mergeSchema", "true")         .save(delta_path)
    
    print("   ✅ Schema evolved! New column added.")
    
    # WHAT: Read table with new schema
    # WHY: Verify old records have NULL, new records have values
    print("\n📊 Table with evolved schema:")
    evolved_df = spark.read.format("delta").load(delta_path)
    evolved_df.printSchema()
    evolved_df.orderBy("customer_id").show()
    
    print("\n💡 Schema Evolution Benefits:")
    print("   • No rewrite of historical data")
    print("   • Backward compatible")
    print("   • Business agility")
    print("   • Existing queries still work")


def example_5_optimize_and_vacuum(spark, delta_path):
    """
    Example 5: OPTIMIZE and VACUUM for performance and storage.
    
    WHAT THIS DOES:
    ---------------
    - OPTIMIZE: Compacts small files into larger ones (faster queries)
    - Z-ORDER: Co-locates related data (data skipping optimization)
    - VACUUM: Deletes old file versions (reclaims storage)
    
    WHY MAINTENANCE:
    ----------------
    - Many small files slow down queries
    - Old versions consume storage
    - Z-ordering improves filter performance 10x
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 5: OPTIMIZE & VACUUM")
    print("=" * 80)
    
    deltaTable = DeltaTable.forPath(spark, delta_path)
    
    # WHAT: Show file statistics before optimization
    print("\n📊 Before OPTIMIZE:")
    print("   (Multiple small files from incremental writes)")
    
    # WHAT: Compact small files
    # WHY: Improve query performance
    # HOW: Rewrite small files into larger optimized files
    print("\n🔄 Running OPTIMIZE...")
    deltaTable.optimize().executeCompaction()
    print("   ✅ Table optimized! Files compacted.")
    
    # WHAT: Z-Order by frequently filtered columns
    # WHY: Co-locate related data for faster filtering
    # HOW: Rearrange data based on column values
    print("\n🔄 Running Z-ORDER by customer_id...")
    deltaTable.optimize().executeZOrderBy("customer_id")
    print("   ✅ Z-ordering complete!")
    
    print("\n💡 OPTIMIZE Benefits:")
    print("   • Fewer files = faster queries")
    print("   • Z-ordering = 10x faster filters")
    print("   • Automatic in Databricks (can disable)")
    
    # WHAT: Vacuum old file versions
    # WHY: Reclaim storage space
    # HOW: Delete files older than retention period
    print("\n🔄 Running VACUUM (retention = 0 hours for demo)...")
    print("   ⚠️  Production: Use 168 hours (7 days) retention!")
    
    # Set retention to 0 for demo (normally 7+ days)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    deltaTable.vacuum(0)
    print("   ✅ Old versions deleted!")
    
    print("\n⚠️  VACUUM Caution:")
    print("   • Can't time travel to vacuumed versions")
    print("   • Use 7+ days retention in production")
    print("   • Enables long-running queries to complete")


def main():
    """Main execution: Delta Lake integration examples."""
    print("\n" + "🔺" * 40)
    print("DELTA LAKE INTEGRATION WITH PYSPARK")
    print("🔺" * 40)
    
    # Create Spark with Delta Lake
    spark = create_spark_with_delta()
    
    try:
        # Example 1: Basic operations
        delta_path = example_1_basic_delta_operations(spark)
        
        # Example 2: Time travel
        example_2_time_travel(spark, delta_path)
        
        # Example 3: UPSERT/MERGE
        example_3_upsert_merge(spark, delta_path)
        
        # Example 4: Schema evolution
        example_4_schema_evolution(spark, delta_path)
        
        # Example 5: Optimize and vacuum
        example_5_optimize_and_vacuum(spark, delta_path)
        
        print("\n" + "=" * 80)
        print("✅ DELTA LAKE EXAMPLES COMPLETE")
        print("=" * 80)
        
        print("\n🎯 Key Takeaways:")
        print("   1. ACID transactions prevent data corruption")
        print("   2. Time travel enables auditing and rollbacks")
        print("   3. MERGE simplifies CDC and SCD patterns")
        print("   4. Schema evolution supports business agility")
        print("   5. OPTIMIZE + VACUUM maintain performance")
        
        print("\n📚 Next Steps:")
        print("   • Production: Use S3/HDFS instead of /tmp")
        print("   • Enable auto-optimize in Databricks")
        print("   • Set up retention policies (7+ days)")
        print("   • Monitor table statistics")
        print("   • Integrate with data quality checks")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
