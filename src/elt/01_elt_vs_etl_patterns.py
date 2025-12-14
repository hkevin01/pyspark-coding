"""
ELT vs ETL Patterns in PySpark

This module demonstrates the difference between ETL and ELT patterns,
when to use each approach, and practical examples.

WHAT IS ELT?
============
ELT = Extract → Load → Transform

You use ELT when:
• You have a powerful data warehouse (BigQuery, Snowflake, Redshift, Databricks)
• It's faster to LOAD raw data first and then do transformations INSIDE the warehouse
• You want to keep raw data for future re-processing
• Your data warehouse can handle complex transformations efficiently

WHY ELT?
========
1. Speed: Modern data warehouses are optimized for large-scale transformations
2. Flexibility: Raw data is available for different transformation logic later
3. Simplicity: Less moving parts, transformations happen in one place
4. Cost: Separation of compute and storage (cloud warehouses)

WHEN TO USE ELT:
===============
✅ Cloud data warehouses (BigQuery, Snowflake, Redshift)
✅ Large datasets (TB to PB scale)
✅ Need to preserve raw data
✅ Multiple teams need different views of same data
✅ Schema changes frequently
✅ Powerful query engine available

WHEN TO USE ETL (Transform BEFORE Load):
=======================================
✅ Limited warehouse capacity
✅ Need to cleanse/filter before storage (reduce size)
✅ Legacy systems
✅ Data quality issues must be fixed before storage
✅ Compliance requirements (PII removal)

Author: PySpark Learning Series
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time


def create_spark_session():
    """Create Spark session for ELT examples."""
    print("\n" + "=" * 70)
    print("CREATING SPARK SESSION FOR ELT PATTERNS")
    print("=" * 70)
    
    spark = SparkSession.builder \
        .appName("ELT vs ETL Patterns") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("✅ Spark session created")
    return spark


def example_1_etl_pattern_traditional(spark):
    """
    ETL Pattern: Extract → Transform → Load
    
    Transform data BEFORE loading into warehouse.
    Use when: Storage is expensive or limited.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: ETL PATTERN (Traditional)")
    print("=" * 70)
    
    print("""
    ETL FLOW:
    ========
    1. EXTRACT: Read raw data from source
    2. TRANSFORM: Clean, filter, aggregate (BEFORE loading)
    3. LOAD: Write transformed data to warehouse
    
    ┌─────────────┐
    │   SOURCE    │
    │  (Raw Data) │
    └──────┬──────┘
           │ EXTRACT
           ▼
    ┌─────────────┐
    │  TRANSFORM  │ ← All transformations happen HERE
    │  (Clean,    │   (filter, aggregate, join)
    │   Filter)   │
    └──────┬──────┘
           │ LOAD
           ▼
    ┌─────────────┐
    │  WAREHOUSE  │ ← Only clean data stored
    │ (Clean Data)│
    └─────────────┘
    
    Advantages:
    • Less storage (only clean data)
    • Faster queries (pre-aggregated)
    
    Disadvantages:
    • Raw data lost
    • Can't re-process with different logic
    """)
    
    # Sample raw sales data
    raw_data = [
        (1, "Alice", "Electronics", 1200, "2024-01-15", "NY"),
        (2, "Bob", "Electronics", -50, "2024-01-16", "NY"),  # Invalid (negative)
        (3, None, "Clothing", 300, "2024-01-17", "CA"),      # Invalid (no name)
        (4, "Charlie", "Electronics", 800, "2024-01-18", "TX"),
        (5, "Diana", "Clothing", 450, "2024-01-19", "CA"),
        (6, "Eve", "Electronics", 1500, "2024-01-20", "NY"),
        (7, "Frank", "Furniture", 0, "2024-01-21", "TX"),    # Invalid (zero)
    ]
    
    raw_df = spark.createDataFrame(raw_data, 
        ["id", "customer", "category", "amount", "date", "state"])
    
    print("\n📊 RAW DATA (Before ETL):")
    raw_df.show()
    print(f"Total records: {raw_df.count()}")
    
    # TRANSFORM: Clean and filter BEFORE loading
    print("\n🔧 TRANSFORMING DATA (Before Load)...")
    
    cleaned_df = raw_df \
        .filter(col("customer").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .select("id", "customer", "category", "amount", "state", "year", "month")
    
    print("\n✅ TRANSFORMED DATA (After Cleaning):")
    cleaned_df.show()
    print(f"Total records after cleaning: {cleaned_df.count()}")
    print(f"Records removed: {raw_df.count() - cleaned_df.count()}")
    
    # LOAD: Save only cleaned data
    print("\n💾 LOADING to warehouse (only clean data)...")
    
    # In real scenario: cleaned_df.write.mode("overwrite").parquet("/warehouse/sales")
    print("✅ Loaded to warehouse: /warehouse/sales")
    print("❌ Raw data NOT saved (lost forever)")
    
    print("\n" + "=" * 70)
    print("ETL PATTERN COMPLETE")
    print("Result: Only clean data in warehouse, raw data discarded")
    print("=" * 70)


def example_2_elt_pattern_modern(spark):
    """
    ELT Pattern: Extract → Load → Transform
    
    Load raw data first, transform INSIDE warehouse.
    Use when: Have powerful data warehouse (BigQuery, Snowflake, etc.)
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: ELT PATTERN (Modern)")
    print("=" * 70)
    
    print("""
    ELT FLOW:
    ========
    1. EXTRACT: Read raw data from source
    2. LOAD: Load raw data to warehouse (NO transformation)
    3. TRANSFORM: Transform INSIDE warehouse (using SQL/Spark)
    
    ┌─────────────┐
    │   SOURCE    │
    │  (Raw Data) │
    └──────┬──────┘
           │ EXTRACT
           ▼
    ┌─────────────┐
    │  WAREHOUSE  │
    │  RAW LAYER  │ ← Load raw data FIRST
    │  (No Clean) │
    └──────┬──────┘
           │ TRANSFORM (inside warehouse)
           ▼
    ┌─────────────┐
    │  WAREHOUSE  │
    │ CLEAN LAYER │ ← Transform AFTER loading
    │ (Cleaned)   │
    └─────────────┘
    
    Advantages:
    • Raw data preserved for re-processing
    • Flexibility to apply different transformations
    • Leverage warehouse compute power
    
    Disadvantages:
    • More storage (raw + clean)
    • Initial load faster but query might be slower
    """)
    
    # Same raw data
    raw_data = [
        (1, "Alice", "Electronics", 1200, "2024-01-15", "NY"),
        (2, "Bob", "Electronics", -50, "2024-01-16", "NY"),
        (3, None, "Clothing", 300, "2024-01-17", "CA"),
        (4, "Charlie", "Electronics", 800, "2024-01-18", "TX"),
        (5, "Diana", "Clothing", 450, "2024-01-19", "CA"),
        (6, "Eve", "Electronics", 1500, "2024-01-20", "NY"),
        (7, "Frank", "Furniture", 0, "2024-01-21", "TX"),
    ]
    
    raw_df = spark.createDataFrame(raw_data, 
        ["id", "customer", "category", "amount", "date", "state"])
    
    # LOAD: Save raw data FIRST (no transformation)
    print("\n💾 LOADING raw data to warehouse (NO cleaning)...")
    print("✅ Loaded to warehouse: /warehouse/sales_raw")
    
    # Create temporary view for SQL transformation
    raw_df.createOrReplaceTempView("sales_raw")
    
    print("\n📊 RAW DATA LOADED:")
    raw_df.show()
    print(f"Total records in raw layer: {raw_df.count()}")
    
    # TRANSFORM: Now transform INSIDE warehouse using SQL
    print("\n🔧 TRANSFORMING DATA (Inside Warehouse using SQL)...")
    
    cleaned_df = spark.sql("""
        SELECT 
            id,
            customer,
            category,
            amount,
            state,
            YEAR(date) as year,
            MONTH(date) as month
        FROM sales_raw
        WHERE customer IS NOT NULL
          AND amount > 0
    """)
    
    print("\n✅ TRANSFORMED DATA (Created from raw):")
    cleaned_df.show()
    print(f"Total records in clean layer: {cleaned_df.count()}")
    
    # Save cleaned view
    print("\n💾 SAVING cleaned view...")
    print("✅ Created view: /warehouse/sales_clean")
    
    print("\n🔄 KEY DIFFERENCE:")
    print("• RAW data still available: /warehouse/sales_raw")
    print("• CLEAN data also available: /warehouse/sales_clean")
    print("• Can re-transform raw data anytime with different logic!")
    
    # Example: Different transformation on same raw data
    print("\n🔄 CREATING DIFFERENT VIEW (Same raw data, different logic):")
    
    summary_df = spark.sql("""
        SELECT 
            category,
            state,
            COUNT(*) as total_orders,
            SUM(amount) as total_sales,
            AVG(amount) as avg_sale
        FROM sales_raw
        WHERE amount > 0
        GROUP BY category, state
        ORDER BY total_sales DESC
    """)
    
    print("\n📊 SUMMARY VIEW (Different transformation):")
    summary_df.show()
    
    print("\n" + "=" * 70)
    print("ELT PATTERN COMPLETE")
    print("Result: Raw data preserved, multiple views created")
    print("=" * 70)


def example_3_elt_use_case_bigquery_style(spark):
    """
    Real-world ELT pattern similar to BigQuery/Snowflake.
    
    Multi-layer architecture:
    • Bronze: Raw data (as-is)
    • Silver: Cleaned data
    • Gold: Aggregated/business metrics
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: REAL-WORLD ELT (Multi-Layer Architecture)")
    print("=" * 70)
    
    print("""
    MULTI-LAYER ELT ARCHITECTURE:
    ============================
    
    ┌───────────────────────────────────────────┐
    │         BRONZE LAYER (Raw)                │
    │  • Load data as-is                        │
    │  • No transformations                     │
    │  • Preserve original format               │
    │  • Source of truth                        │
    └────────────────┬──────────────────────────┘
                     │
                     ▼
    ┌───────────────────────────────────────────┐
    │         SILVER LAYER (Cleaned)            │
    │  • Data quality checks                    │
    │  • Standardization                        │
    │  • Type conversions                       │
    │  • Deduplication                          │
    └────────────────┬──────────────────────────┘
                     │
                     ▼
    ┌───────────────────────────────────────────┐
    │         GOLD LAYER (Business Metrics)     │
    │  • Aggregations                           │
    │  • KPIs and metrics                       │
    │  • Ready for BI tools                     │
    │  • Optimized for queries                  │
    └───────────────────────────────────────────┘
    
    Used by: BigQuery, Snowflake, Databricks, Redshift
    """)
    
    # Simulate raw data with quality issues
    raw_data = [
        (1, "alice@email.com", "Electronics", "1200", "2024-01-15", "new york"),
        (2, "bob@EMAIL.COM", "ELECTRONICS", "850", "2024-01-16", "NEW YORK"),
        (1, "alice@email.com", "Electronics", "1200", "2024-01-15", "new york"),  # Duplicate
        (3, "charlie@email", "Clothing", "abc", "2024-01-17", "Los Angeles"),    # Bad amount
        (4, "DIANA@email.com", "clothing", "450", "2024-01-18", "los angeles"),
        (5, "", "Furniture", "1500", "2024-01-19", "Chicago"),                   # Missing email
    ]
    
    # BRONZE LAYER: Load as-is
    print("\n🥉 BRONZE LAYER: Loading raw data...")
    bronze_df = spark.createDataFrame(raw_data, 
        ["id", "email", "category", "amount_str", "date", "city"])
    
    bronze_df.createOrReplaceTempView("bronze_sales")
    print("\n📊 BRONZE DATA (Raw, as-is):")
    bronze_df.show(truncate=False)
    print(f"Total records: {bronze_df.count()}")
    
    # SILVER LAYER: Clean and standardize
    print("\n🥈 SILVER LAYER: Cleaning and standardizing...")
    
    silver_df = spark.sql("""
        SELECT 
            id,
            LOWER(TRIM(email)) as email,
            INITCAP(category) as category,
            CAST(amount_str AS DOUBLE) as amount,
            TO_DATE(date) as date,
            INITCAP(TRIM(city)) as city
        FROM bronze_sales
        WHERE email != ''
          AND email LIKE '%@%'
          AND amount_str RLIKE '^[0-9]+$'
    """).dropDuplicates(["id", "email", "date"])
    
    silver_df.createOrReplaceTempView("silver_sales")
    print("\n📊 SILVER DATA (Cleaned):")
    silver_df.show(truncate=False)
    print(f"Total records: {silver_df.count()}")
    print(f"Records cleaned: {bronze_df.count() - silver_df.count()}")
    
    # GOLD LAYER: Business metrics
    print("\n🥇 GOLD LAYER: Creating business metrics...")
    
    gold_df = spark.sql("""
        SELECT 
            category,
            city,
            COUNT(DISTINCT id) as unique_customers,
            COUNT(*) as total_orders,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value,
            MIN(date) as first_order,
            MAX(date) as last_order
        FROM silver_sales
        GROUP BY category, city
        ORDER BY total_revenue DESC
    """)
    
    print("\n📊 GOLD DATA (Business Metrics):")
    gold_df.show(truncate=False)
    
    print("\n" + "=" * 70)
    print("MULTI-LAYER ELT SUMMARY:")
    print("=" * 70)
    print("🥉 Bronze: 6 raw records (with duplicates and errors)")
    print("🥈 Silver: 3 clean records (deduplicated, validated)")
    print("🥇 Gold: 2 metric rows (aggregated for business)")
    print("\n✅ All layers preserved - can re-process anytime!")
    print("=" * 70)


def example_4_when_to_use_elt(spark):
    """
    Decision guide: When to use ELT vs ETL.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: WHEN TO USE ELT vs ETL")
    print("=" * 70)
    
    print("""
    ┌────────────────────────────────────────────────────────────┐
    │                    USE ELT WHEN:                           │
    ├────────────────────────────────────────────────────────────┤
    │ ✅ Cloud data warehouse (BigQuery, Snowflake, Redshift)   │
    │ ✅ Storage is cheap (cloud storage)                        │
    │ ✅ Compute is powerful (MPP database)                      │
    │ ✅ Need to preserve raw data                               │
    │ ✅ Schema evolves frequently                               │
    │ ✅ Multiple teams need different views                     │
    │ ✅ Large datasets (TB to PB scale)                         │
    │ ✅ Want to re-process with different logic                 │
    │ ✅ Data lake architecture                                  │
    │ ✅ Modern cloud-native stack                               │
    └────────────────────────────────────────────────────────────┘
    
    ┌────────────────────────────────────────────────────────────┐
    │                    USE ETL WHEN:                           │
    ├────────────────────────────────────────────────────────────┤
    │ ✅ Storage is expensive/limited                            │
    │ ✅ Must filter out sensitive data (PII, compliance)        │
    │ ✅ Data quality issues must be fixed BEFORE storage        │
    │ ✅ Target system is not powerful (legacy)                  │
    │ ✅ Small datasets (GB scale)                               │
    │ ✅ Transformation logic is stable (won't change)           │
    │ ✅ Network bandwidth is limited                            │
    │ ✅ On-premise data centers                                 │
    │ ✅ Traditional data warehouses (Oracle, SQL Server)        │
    └────────────────────────────────────────────────────────────┘
    
    REAL-WORLD EXAMPLES:
    ===================
    
    ELT Use Cases:
    • Google BigQuery: Load CSV/JSON → Transform with SQL
    • Snowflake: Load data into stages → Transform in warehouse
    • Databricks: Bronze → Silver → Gold (Medallion Architecture)
    • AWS Redshift: S3 → Redshift → Transform with Spectrum
    
    ETL Use Cases:
    • Apache Spark: Read → Transform → Write to Hive
    • Legacy systems: SSIS, Informatica, Talend
    • On-premise: Transform before loading to limited storage
    • Compliance: Remove PII before storing
    """)
    
    # Practical example: Cost comparison
    print("\n💰 COST COMPARISON:")
    print("=" * 70)
    
    cost_data = [
        ("ETL (Traditional)", "Small", "Low storage cost", "High compute cost", "Data lost"),
        ("ELT (Modern)", "Large", "Medium storage cost", "Low compute cost", "Data preserved"),
    ]
    
    cost_df = spark.createDataFrame(cost_data, 
        ["Pattern", "Data Volume", "Storage", "Compute", "Raw Data"])
    
    cost_df.show(truncate=False)
    
    print("\n📊 TREND: Industry moving towards ELT")
    print("Reason: Cloud storage is cheap, compute is scalable")
    print("=" * 70)


def main():
    """Run all ELT vs ETL examples."""
    spark = create_spark_session()
    
    try:
        # Run all examples
        example_1_etl_pattern_traditional(spark)
        example_2_elt_pattern_modern(spark)
        example_3_elt_use_case_bigquery_style(spark)
        example_4_when_to_use_elt(spark)
        
        print("\n" + "=" * 70)
        print("KEY TAKEAWAYS:")
        print("=" * 70)
        print("""
        1. ELT = Extract → Load → Transform (inside warehouse)
        2. ETL = Extract → Transform → Load (before warehouse)
        
        3. Use ELT when:
           • Powerful data warehouse (BigQuery, Snowflake)
           • Need to preserve raw data
           • Storage is cheap
        
        4. Use ETL when:
           • Limited storage
           • Must cleanse before storing
           • Legacy systems
        
        5. Modern trend: ELT (cloud data warehouses)
        
        6. Multi-layer architecture:
           Bronze (raw) → Silver (clean) → Gold (metrics)
        """)
        
    finally:
        spark.stop()
        print("\n✅ Spark session stopped")


if __name__ == "__main__":
    main()
