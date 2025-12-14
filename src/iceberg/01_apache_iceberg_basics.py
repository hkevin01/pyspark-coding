"""
================================================================================
Apache Iceberg Integration with PySpark
================================================================================

PURPOSE:
--------
Implement table format abstraction with Apache Iceberg for efficient data lake
operations, hidden partitioning, and cross-engine compatibility.

WHAT ICEBERG PROVIDES:
----------------------
- Hidden partitioning (no partition columns in queries)
- Partition evolution (change partitioning without rewriting data)
- Time travel and snapshots
- Schema evolution
- ACID transactions
- Cross-engine support (Spark, Flink, Presto, Trino)

WHY APACHE ICEBERG:
-------------------
ADVANTAGES OVER DELTA:
- Vendor-neutral (Apache Foundation vs Databricks)
- Better partition evolution
- Hidden partitioning (users don't need to know partitions)
- Multi-catalog support (Hive, AWS Glue, Nessie)
- Better for query engines (Presto, Trino, Athena)

REAL-WORLD USE CASES:
- Netflix: 100 PB+ data lake on Iceberg
- Apple: Petabyte-scale analytics
- Adobe: Real-time event streaming
- Airbnb: Data platform modernization

KEY FEATURES:
-------------

1. HIDDEN PARTITIONING:
   Users query: SELECT * FROM events WHERE event_date = '2023-12-01'
   Iceberg automatically: Prunes partitions without users knowing scheme

2. PARTITION EVOLUTION:
   - Change from daily to hourly partitions
   - No data rewrite required
   - Transparent to users

3. TIME TRAVEL:
   - Query snapshots by ID or timestamp
   - Rollback to previous state
   - Audit trail

COMPARISON:
-----------
|                    | Delta Lake | Iceberg | Hudi |
|--------------------|------------|---------|------|
| Vendor             | Databricks | Apache  | Apache |
| Hidden Partitioning| ❌         | ✅      | ❌    |
| Partition Evolution| Limited    | ✅      | ✅    |
| Engine Support     | Spark      | All     | All   |
| Maturity           | High       | Medium  | Medium|

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def create_spark_with_iceberg():
    """Create Spark session with Iceberg support."""
    return SparkSession.builder \
        .appName("Iceberg_Integration") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
        .config("spark.sql.catalog.spark_catalog.type", "hive") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-warehouse") \
        .getOrCreate()

def example_1_create_iceberg_table(spark):
    """
    Example 1: Create Iceberg table with hidden partitioning.
    
    WHAT: Create partitioned table without exposing partitions to users
    WHY: Simplify queries (no partition predicates needed)
    HOW: Use PARTITIONED BY in CREATE TABLE
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: CREATE ICEBERG TABLE")
    print("=" * 80)
    
    # Create sample event data
    events = spark.createDataFrame([
        (1, "page_view", "2023-12-01", "user1"),
        (2, "click", "2023-12-01", "user2"),
        (3, "purchase", "2023-12-02", "user1"),
    ], ["event_id", "event_type", "event_date", "user_id"])
    
    # Write as Iceberg table with hidden partitioning
    events.writeTo("local.db.events") \
        .using("iceberg") \
        .partitionedBy("event_date") \
        .createOrReplace()
    
    print("✅ Iceberg table created with hidden partitioning")
    
    # Query without specifying partitions
    result = spark.table("local.db.events")
    result.show()
    
    return result

def main():
    """Main execution."""
    print("\n" + "🧊" * 40)
    print("APACHE ICEBERG INTEGRATION")
    print("🧊" * 40)
    
    spark = create_spark_with_iceberg()
    example_1_create_iceberg_table(spark)
    
    print("\n✅ Iceberg examples complete!")
    spark.stop()

if __name__ == "__main__":
    main()
