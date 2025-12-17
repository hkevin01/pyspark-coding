#!/usr/bin/env python3
"""
Long-Running Spark Job for UI Demonstration
============================================
This job processes more data with multiple stages to keep the UI active longer.
Perfect for exploring the Spark Application UI at http://localhost:4040
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

def main():
    print("=" * 70)
    print("🚀 LONG-RUNNING SPARK JOB - UI DEMO")
    print("=" * 70)
    print("\n📊 Open Spark UI now: http://localhost:4040")
    print("⏰ This job will run for ~60 seconds\n")
    
    spark = SparkSession.builder \
        .appName("UI Demo - Long Running Job") \
        .getOrCreate()
    
    sc = spark.sparkContext
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 1: Create large dataset (10 million records)
    # ═══════════════════════════════════════════════════════════════
    print("📈 STAGE 1: Creating 10 million records...")
    time.sleep(2)  # Give time to open UI
    
    df = spark.range(0, 10_000_000, 1, numPartitions=24)
    df = df.withColumn("value", F.col("id") * 2)
    df = df.withColumn("category", (F.col("id") % 100).cast("string"))
    df = df.withColumn("squared", F.col("id") * F.col("id"))
    
    df.cache()  # Cache for reuse - visible in Storage tab
    print(f"   ✓ Created {df.count():,} records")
    print("   💡 Check Storage tab in UI to see cached data\n")
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 2: Complex aggregations
    # ═══════════════════════════════════════════════════════════════
    print("📊 STAGE 2: Running aggregations...")
    
    result1 = df.groupBy("category") \
        .agg(
            F.count("*").alias("count"),
            F.sum("squared").alias("sum_squared"),
            F.avg("value").alias("avg_value"),
            F.max("id").alias("max_id"),
            F.min("id").alias("min_id")
        )
    
    print(f"   ✓ Aggregated into {result1.count()} groups")
    print("   💡 Check Stages tab to see task distribution\n")
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 3: Window functions
    # ═══════════════════════════════════════════════════════════════
    print("🪟 STAGE 3: Applying window functions...")
    
    from pyspark.sql.window import Window
    
    windowSpec = Window.partitionBy("category").orderBy("id")
    
    df_windowed = df.withColumn("row_num", F.row_number().over(windowSpec)) \
                    .withColumn("running_sum", F.sum("value").over(windowSpec))
    
    sample = df_windowed.filter(F.col("row_num") <= 10).count()
    print(f"   ✓ Applied window functions, sampled {sample:,} records")
    print("   💡 Check SQL tab to see execution plan\n")
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 4: Joins
    # ═══════════════════════════════════════════════════════════════
    print("🔗 STAGE 4: Performing joins...")
    
    # Create a smaller lookup table
    lookup = spark.range(0, 100).toDF("category_id")
    lookup = lookup.withColumn("category", F.col("category_id").cast("string"))
    lookup = lookup.withColumn("category_name", F.concat(F.lit("Category_"), F.col("category")))
    
    # Join with main dataset
    joined = result1.join(lookup, "category", "left")
    
    print(f"   ✓ Joined {joined.count()} records")
    print("   💡 Check Executors tab to see shuffle metrics\n")
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 5: Multiple filters and transformations
    # ═══════════════════════════════════════════════════════════════
    print("🔍 STAGE 5: Filtering and transformations...")
    
    filtered = df.filter(F.col("id") % 10 == 0) \
                 .filter(F.col("value") > 1000) \
                 .select("id", "value", "category", "squared")
    
    print(f"   ✓ Filtered to {filtered.count():,} records")
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 6: Sort and collect top records
    # ═══════════════════════════════════════════════════════════════
    print("📋 STAGE 6: Sorting and collecting results...")
    
    top_records = df.orderBy(F.col("squared").desc()).limit(20)
    results = top_records.collect()
    
    print("\n   Top 10 records by squared value:")
    print("   " + "-" * 50)
    for i, row in enumerate(results[:10], 1):
        print(f"   {i:2d}. ID: {row['id']:8,} | Squared: {row['squared']:15,}")
    
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 7: Statistical operations
    # ═══════════════════════════════════════════════════════════════
    print("\n📐 STAGE 7: Computing statistics...")
    
    stats = df.select(
        F.count("*").alias("total_count"),
        F.sum("value").alias("total_sum"),
        F.avg("value").alias("mean"),
        F.stddev("value").alias("stddev"),
        F.min("value").alias("min_value"),
        F.max("value").alias("max_value")
    ).collect()[0]
    
    print(f"\n   Statistics:")
    print(f"   • Total records: {stats['total_count']:,}")
    print(f"   • Sum: {stats['total_sum']:,}")
    print(f"   • Mean: {stats['mean']:,.2f}")
    print(f"   • Std Dev: {stats['stddev']:,.2f}")
    print(f"   • Range: [{stats['min_value']:,} - {stats['max_value']:,}]")
    
    time.sleep(3)
    
    # ═══════════════════════════════════════════════════════════════
    # STAGE 8: Final aggregation with multiple operations
    # ═══════════════════════════════════════════════════════════════
    print("\n🎯 STAGE 8: Final complex aggregation...")
    
    final_result = joined.groupBy("category_name") \
        .agg(
            F.sum("count").alias("total_records"),
            F.avg("avg_value").alias("overall_avg"),
            F.max("max_id").alias("highest_id")
        ) \
        .orderBy(F.col("total_records").desc()) \
        .limit(10)
    
    print("\n   Top 10 categories by record count:")
    print("   " + "-" * 66)
    final_result.show(10, truncate=False)
    
    time.sleep(5)
    
    # ═══════════════════════════════════════════════════════════════
    # Summary
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("✅ JOB COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print("\n📊 Summary:")
    print(f"   • Processed: 10,000,000 records")
    print(f"   • Stages: 8 major stages")
    print(f"   • Partitions: 24")
    print(f"   • Operations: Aggregations, Joins, Windows, Filters")
    print("\n💡 UI Insights:")
    print("   • Jobs Tab: See all 8+ jobs executed")
    print("   • Stages Tab: View task timeline and distribution")
    print("   • Storage Tab: Check cached DataFrame")
    print("   • Executors Tab: Monitor worker performance")
    print("   • SQL Tab: Explore query plans")
    print("\n🌐 Master UI: http://localhost:9080")
    print("   View completed application details\n")
    
    # Keep UI alive for a few more seconds
    print("⏰ Keeping UI alive for 10 more seconds...")
    print("   Explore the UI now!")
    time.sleep(10)
    
    df.unpersist()
    spark.stop()
    print("\n🛑 Spark session stopped. UI will close now.\n")

if __name__ == "__main__":
    main()
