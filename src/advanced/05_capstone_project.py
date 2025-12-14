"""
ADVANCED 05: Capstone Project
Build a complete production-grade ETL pipeline combining all techniques.
Comprehensive project: ingestion → transformation → quality → optimization → output.
TIME: 60 minutes | DIFFICULTY: ⭐⭐⭐⭐⭐ (5/5)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, current_timestamp, lit
import os

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Advanced05_Capstone")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
        .getOrCreate()
    )

def stage_1_ingestion(spark):
    """Stage 1: Ingest raw data."""
    print("\n" + "=" * 80)
    print("STAGE 1: DATA INGESTION")
    print("=" * 80)
    
    # Create sample data
    sales_data = [
        (1, 101, 1, 500.00, "2024-01-15"),
        (2, 102, 2, 300.00, "2024-01-16"),
        (3, 101, 3, 750.00, "2024-01-17"),
    ]
    
    df_sales = spark.createDataFrame(
        sales_data,
        ["sale_id", "customer_id", "product_id", "amount", "sale_date"]
    )
    
    print("\n✅ Ingested sales data:")
    df_sales.show()
    return df_sales

def stage_2_transformation(df):
    """Stage 2: Transform data."""
    print("\n" + "=" * 80)
    print("STAGE 2: TRANSFORMATION")
    print("=" * 80)
    
    # Add audit columns
    df_transformed = df.withColumn("processed_at", current_timestamp()) \
                       .withColumn("pipeline_version", lit("v1.0"))
    
    print("\n✅ Transformed data:")
    df_transformed.show()
    return df_transformed

def stage_3_quality_check(df):
    """Stage 3: Data quality validation."""
    print("\n" + "=" * 80)
    print("STAGE 3: QUALITY CHECK")
    print("=" * 80)
    
    # Check for nulls
    null_count = df.select([count(col(c).isNull()).alias(c) for c in df.columns])
    print("\n📊 Null check:")
    null_count.show()
    
    # Validate business rules
    invalid_sales = df.filter(col("amount") <= 0).count()
    print(f"\n✅ Quality check: {invalid_sales} invalid records")
    
    return df

def stage_4_aggregation(df):
    """Stage 4: Business metrics."""
    print("\n" + "=" * 80)
    print("STAGE 4: AGGREGATION")
    print("=" * 80)
    
    # Daily summary
    df_summary = df.groupBy("sale_date").agg(
        count("*").alias("num_sales"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_sale")
    ).orderBy("sale_date")
    
    print("\n✅ Daily summary:")
    df_summary.show()
    return df_summary

def stage_5_output(df):
    """Stage 5: Write results."""
    print("\n" + "=" * 80)
    print("STAGE 5: OUTPUT")
    print("=" * 80)
    
    output_path = "/tmp/pyspark_capstone_output"
    os.makedirs(output_path, exist_ok=True)
    
    df.write.mode("overwrite").parquet(f"{output_path}/sales_summary")
    
    print(f"\n✅ Data written to: {output_path}")
    print(f"   Total records: {df.count()}")

def main():
    """Run complete capstone project."""
    print("\n" + "🏆" * 40)
    print("ADVANCED LESSON 5: CAPSTONE PROJECT")
    print("🏆" * 40)
    print("\nBuilding production ETL pipeline with:")
    print("   1. Data Ingestion")
    print("   2. Transformation")
    print("   3. Quality Validation")
    print("   4. Aggregation")
    print("   5. Output")
    
    spark = create_spark_session()
    
    try:
        df = stage_1_ingestion(spark)
        df = stage_2_transformation(df)
        df = stage_3_quality_check(df)
        df_summary = stage_4_aggregation(df)
        stage_5_output(df_summary)
        
        print("\n" + "=" * 80)
        print("🎉 CAPSTONE PROJECT COMPLETE!")
        print("=" * 80)
        print("\n🏆 CONGRATULATIONS - ADVANCED TRACK COMPLETE!")
        print("\n✨ You now have COMPLETE PySpark mastery:")
        print("\n   BEGINNER:")
        print("   ✅ DataFrames, file I/O, cleaning, joins, ETL pipelines")
        print("\n   INTERMEDIATE:")
        print("   ✅ Advanced transforms, windows, UDFs, aggregations, quality")
        print("\n   ADVANCED:")
        print("   ✅ Performance tuning, streaming, ML, production patterns")
        
        print("\n🚀 YOU ARE NOW A PYSPARK EXPERT!")
        print("   - Ready for production data engineering")
        print("   - Equipped for big data interviews")
        print("   - Prepared for Spark certification")
        
        print("\n📚 Keep learning:")
        print("   - Explore Delta Lake (data/delta_lake/)")
        print("   - Try Airflow orchestration (src/airflow_dags/)")
        print("   - Build real projects!")
        
        print("\n" + "=" * 80 + "\n")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
