"""
================================================================================
BEGINNER 05: Simple ETL Pipeline
================================================================================

PURPOSE: Put it all together! Build a complete Extract-Transform-Load (ETL)
pipeline combining everything you've learned.

WHAT YOU'LL BUILD:
- Extract: Read raw data from CSV
- Transform: Clean, join, aggregate
- Load: Write results to Parquet

WHY: This is the foundation of data engineering - moving and transforming data.

REAL-WORLD: Daily sales ETL pipeline for e-commerce analytics dashboard.

TIME: 30 minutes | DIFFICULTY: ⭐⭐⭐☆☆ (3/5)
================================================================================
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, current_timestamp, lit


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Beginner05_ETL")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def setup_raw_data(spark):
    """Simulate raw data from source systems."""
    print("\n" + "=" * 80)
    print("SETUP: Creating Raw Data Files")
    print("=" * 80)
    
    data_dir = "/tmp/pyspark_etl"
    os.makedirs(data_dir, exist_ok=True)
    
    # Raw sales data (messy!)
    sales = [
        (1, 101, 1, 2, 150.00, "2024-01-15"),
        (2, 102, 2, 1, 299.99, "2024-01-15"),
        (3, 103, 1, 3, 75.00, "2024-01-16"),
        (3, 103, 1, 3, 75.00, "2024-01-16"),  # Duplicate!
        (4, None, 3, 1, 199.99, "2024-01-16"),  # Missing customer_id
        (5, 104, 2, 5, 49.99, "2024-01-17"),
    ]
    df_sales = spark.createDataFrame(
        sales,
        ["sale_id", "customer_id", "product_id", "quantity", "amount", "sale_date"]
    )
    
    # Customer data
    customers = [
        (101, "Alice", "alice@email.com", "Premium"),
        (102, "Bob", "bob@email.com", "Standard"),
        (103, "Charlie", "charlie@email.com", "Premium"),
        (104, "Diana", "diana@email.com", "Standard"),
    ]
    df_customers = spark.createDataFrame(
        customers,
        ["customer_id", "name", "email", "tier"]
    )
    
    # Product data
    products = [
        (1, "Laptop", "Electronics", 999.99),
        (2, "Mouse", "Electronics", 29.99),
        (3, "Desk", "Furniture", 299.99),
    ]
    df_products = spark.createDataFrame(
        products,
        ["product_id", "product_name", "category", "unit_price"]
    )
    
    # Write raw data
    df_sales.write.mode("overwrite").csv(f"{data_dir}/raw/sales", header=True)
    df_customers.write.mode("overwrite").csv(f"{data_dir}/raw/customers", header=True)
    df_products.write.mode("overwrite").csv(f"{data_dir}/raw/products", header=True)
    
    print(f"\n✅ Raw data created in {data_dir}/raw/")
    return data_dir


def step_1_extract(spark, data_dir):
    """EXTRACT: Load raw data from source."""
    print("\n" + "=" * 80)
    print("STEP 1: EXTRACT (Load Raw Data)")
    print("=" * 80)
    
    # Read raw data
    df_sales = spark.read.csv(f"{data_dir}/raw/sales", header=True, inferSchema=True)
    df_customers = spark.read.csv(f"{data_dir}/raw/customers", header=True, inferSchema=True)
    df_products = spark.read.csv(f"{data_dir}/raw/products", header=True, inferSchema=True)
    
    print("\n📊 Raw Sales Data:")
    df_sales.show()
    
    print(f"✅ Extracted {df_sales.count()} sales records")
    print(f"✅ Extracted {df_customers.count()} customers")
    print(f"✅ Extracted {df_products.count()} products")
    
    return df_sales, df_customers, df_products


def step_2_transform(df_sales, df_customers, df_products):
    """TRANSFORM: Clean, enrich, and aggregate data."""
    print("\n" + "=" * 80)
    print("STEP 2: TRANSFORM (Clean & Enrich)")
    print("=" * 80)
    
    # 2.1: Data Quality - Remove duplicates and nulls
    print("\n🧹 Cleaning data...")
    df_sales_clean = df_sales.dropDuplicates().dropna(subset=["customer_id"])
    print(f"   Removed {df_sales.count() - df_sales_clean.count()} bad records")
    
    # 2.2: Enrich - Join with customers and products
    print("\n🔗 Joining tables...")
    df_enriched = df_sales_clean \
        .join(df_customers, "customer_id", "left") \
        .join(df_products, "product_id", "left")
    
    # 2.3: Add calculated fields
    print("\n➕ Adding calculated fields...")
    df_enriched = df_enriched.withColumn(
        "revenue", col("amount")
    ).withColumn(
        "processed_at", current_timestamp()
    )
    
    print("\n✅ Enriched Data:")
    df_enriched.select(
        "sale_id", "name", "product_name", "quantity", "revenue", "tier"
    ).show()
    
    # 2.4: Create summary table
    print("\n📊 Creating daily summary...")
    df_summary = df_enriched.groupBy("sale_date", "category").agg(
        count("sale_id").alias("num_sales"),
        spark_sum("revenue").alias("total_revenue"),
        avg("revenue").alias("avg_sale_value")
    ).orderBy("sale_date", "category")
    
    print("\n✅ Daily Summary:")
    df_summary.show()
    
    return df_enriched, df_summary


def step_3_load(df_enriched, df_summary, data_dir):
    """LOAD: Write processed data to data warehouse."""
    print("\n" + "=" * 80)
    print("STEP 3: LOAD (Write to Data Warehouse)")
    print("=" * 80)
    
    output_dir = f"{data_dir}/warehouse"
    
    # Write fact table (detailed sales)
    fact_path = f"{output_dir}/fact_sales"
    df_enriched.write.mode("overwrite").partitionBy("sale_date").parquet(fact_path)
    print(f"\n✅ Fact table saved: {fact_path}")
    
    # Write summary table (aggregated)
    summary_path = f"{output_dir}/summary_daily_sales"
    df_summary.write.mode("overwrite").parquet(summary_path)
    print(f"✅ Summary table saved: {summary_path}")
    
    print("\n💡 Data warehouse structure:")
    print(f"   {output_dir}/")
    print(f"   ├── fact_sales/")
    print(f"   │   ├── sale_date=2024-01-15/")
    print(f"   │   ├── sale_date=2024-01-16/")
    print(f"   │   └── sale_date=2024-01-17/")
    print(f"   └── summary_daily_sales/")
    
    return fact_path, summary_path


def step_4_verify(spark, fact_path, summary_path):
    """Verify the ETL pipeline results."""
    print("\n" + "=" * 80)
    print("STEP 4: VERIFY (Quality Check)")
    print("=" * 80)
    
    # Read back from warehouse
    df_fact = spark.read.parquet(fact_path)
    df_summary = spark.read.parquet(summary_path)
    
    print("\n✅ Verification:")
    print(f"   Fact table rows: {df_fact.count()}")
    print(f"   Summary rows: {df_summary.count()}")
    
    print("\n📊 Sample from warehouse:")
    df_fact.select("sale_id", "name", "product_name", "revenue", "sale_date").show(5)
    
    print("\n📈 Daily totals:")
    df_summary.show()
    
    print("\n🎯 ETL Pipeline Complete!")


def main():
    """Run complete ETL pipeline."""
    print("\n" + "🏭" * 40)
    print("BEGINNER LESSON 5: SIMPLE ETL PIPELINE")
    print("🏭" * 40)
    
    spark = create_spark_session()
    
    try:
        # Setup
        data_dir = setup_raw_data(spark)
        
        # ETL Steps
        df_sales, df_customers, df_products = step_1_extract(spark, data_dir)
        df_enriched, df_summary = step_2_transform(df_sales, df_customers, df_products)
        fact_path, summary_path = step_3_load(df_enriched, df_summary, data_dir)
        step_4_verify(spark, fact_path, summary_path)
        
        print("\n" + "=" * 80)
        print("🎉 ETL PIPELINE COMPLETE!")
        print("=" * 80)
        print("\n✅ You built a complete data pipeline:")
        print("   1. EXTRACT: Read raw CSV data")
        print("   2. TRANSFORM: Clean, join, aggregate")
        print("   3. LOAD: Write to Parquet data warehouse")
        print("   4. VERIFY: Quality check results")
        
        print("\n🏆 CONGRATULATIONS - BEGINNER TRACK COMPLETE!")
        print("\n✨ You now know:")
        print("   ✅ DataFrame basics")
        print("   ✅ Reading/writing files")
        print("   ✅ Data cleaning")
        print("   ✅ Joins")
        print("   ✅ Building ETL pipelines")
        
        print("\n➡️  NEXT LEVEL: Try intermediate/01_advanced_transformations.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
