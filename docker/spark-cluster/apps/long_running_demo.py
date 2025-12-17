#!/usr/bin/env python3
"""
Long-Running Spark Job for UI Demonstration
============================================
This job processes data with multiple stages to keep the UI active longer.
Perfect for exploring the Spark Dashboard execution flow visualization.

🕐 RUNTIME: ~2-3 minutes (optimized for desktop systems)
⚙️  RESOURCE-FRIENDLY: Uses limited memory and CPU
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION - Adjust these for your system
# ═══════════════════════════════════════════════════════════════════════════
RECORDS_SMALL = 2_000_000  # 2M records (was 50M)
RECORDS_MEDIUM = 500_000  # 500K records (was 20M)
PARTITIONS = 12  # Fewer partitions = less overhead
PAUSE_SECONDS = 6  # Pause between stages for observation


def stage_pause(seconds=PAUSE_SECONDS):
    """Pause between stages so you can observe the dashboard"""
    print(f"   ⏳ Pausing {seconds}s for dashboard observation...")
    time.sleep(seconds)


def main():
    print("=" * 70)
    print("🚀 LONG-RUNNING SPARK JOB - DASHBOARD DEMO (Resource-Friendly)")
    print("=" * 70)
    print("\n📊 Open Dashboard: http://localhost:3000")
    print("🔥 This job will run for ~2-3 MINUTES")
    print("⚙️  Optimized for desktop systems (low memory footprint)")
    print("👀 Watch the Execution Flow update in real-time!\n")

    spark = (
        SparkSession.builder.appName("Extended Demo - Watch Execution Flow")
        .config("spark.sql.shuffle.partitions", str(PARTITIONS))
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.memory.fraction", "0.4")
        .getOrCreate()
    )

    sc = spark.sparkContext

    print("⏰ Starting in 3 seconds - get the dashboard ready!")
    time.sleep(3)

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 1: Create first dataset (2 million records - manageable size)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print(f"📈 STAGE 1/10: Creating {RECORDS_SMALL:,} records...")
    print("   This generates the base RDD partitioned across workers")
    print("─" * 70)

    df1 = spark.range(0, RECORDS_SMALL, 1, numPartitions=PARTITIONS)
    df1 = df1.withColumn("value", F.col("id") * 2)
    df1 = df1.withColumn("category", (F.col("id") % 100).cast("string"))
    df1 = df1.withColumn("squared", F.col("id") * F.col("id"))
    df1 = df1.withColumn("random_val", F.rand() * 1000)

    # Persist with MEMORY_AND_DISK to avoid OOM
    df1.persist()
    count1 = df1.count()
    print(f"   ✅ Created {count1:,} records across {PARTITIONS} partitions")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 2: Create second dataset for joins
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print(f"📈 STAGE 2/10: Creating second dataset ({RECORDS_MEDIUM:,} records)...")
    print("   This will be used for join operations later")
    print("─" * 70)

    df2 = spark.range(0, RECORDS_MEDIUM, 1, numPartitions=PARTITIONS // 2)
    df2 = df2.withColumn("key", (F.col("id") % 100).cast("string"))
    df2 = df2.withColumn("extra_value", F.col("id") * 3)
    df2 = df2.withColumn("flag", F.when(F.col("id") % 2 == 0, "EVEN").otherwise("ODD"))

    count2 = df2.count()
    print(f"   ✅ Created {count2:,} records for joining")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 3: First aggregation (groupBy causes SHUFFLE!)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("🔀 STAGE 3/10: First aggregation - GROUP BY (triggers SHUFFLE!)")
    print("   Watch the Shuffle section - data moves between nodes here")
    print("─" * 70)

    agg1 = df1.groupBy("category").agg(
        F.count("*").alias("count"),
        F.sum("squared").alias("sum_squared"),
        F.avg("value").alias("avg_value"),
        F.max("id").alias("max_id"),
    )

    agg_count = agg1.count()
    print(f"   ✅ Aggregated into {agg_count} category groups")
    print("   🔀 Shuffle complete - check Shuffle Read/Write metrics")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 4: Aggregation with value buckets
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("📊 STAGE 4/10: Aggregation with value ranges...")
    print("   Creating buckets and aggregating by range")
    print("─" * 70)

    df_bucketed = df1.withColumn(
        "value_bucket",
        F.when(F.col("value") < 1000, "small")
        .when(F.col("value") < 10000, "medium")
        .otherwise("large"),
    )

    bucket_agg = df_bucketed.groupBy("value_bucket").agg(
        F.count("*").alias("bucket_count"), F.sum("value").alias("bucket_sum")
    )

    bucket_count = bucket_agg.count()
    print(f"   ✅ Created {bucket_count} value buckets")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 5: Window function (lighter version)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("🪟 STAGE 5/10: Applying WINDOW FUNCTION...")
    print("   Computing row numbers per category (sampled)")
    print("─" * 70)

    # Use smaller sample for window function
    df_sample = df1.sample(0.1)  # 10% sample
    windowSpec = Window.partitionBy("category").orderBy("id")

    df_windowed = df_sample.withColumn("row_num", F.row_number().over(windowSpec))
    window_count = df_windowed.filter(F.col("row_num") <= 10).count()
    print(f"   ✅ Window function applied, sampled {window_count:,} records")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 6: JOIN operation (controlled size)
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("🔗 STAGE 6/10: JOIN OPERATION (shuffle)")
    print(f"   Joining {RECORDS_SMALL:,} with {RECORDS_MEDIUM:,} records")
    print("   Watch Shuffle metrics increase!")
    print("─" * 70)

    df2_renamed = df2.withColumnRenamed("key", "category")

    # Limit join size for safety
    joined = df1.join(df2_renamed, "category", "inner")
    join_count = joined.limit(100000).count()
    print(f"   ✅ Join complete - sampled {join_count:,} matched records")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 7: Filter chain
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("🔍 STAGE 7/10: Applying FILTER chain (narrow transformation)...")
    print("   Multiple filter conditions - no shuffle needed")
    print("─" * 70)

    filtered = (
        df1.filter(F.col("id") % 7 == 0)
        .filter(F.col("value") > 1000)
        .filter(F.col("random_val") > 500)
    )

    filter_count = filtered.count()
    print(f"   ✅ Filtered to {filter_count:,} records")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 8: Statistical aggregations
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("📐 STAGE 8/10: Statistical aggregations...")
    print("   Computing mean, stddev, min, max")
    print("─" * 70)

    stats = df1.select(
        F.count("*").alias("total_count"),
        F.avg("value").alias("mean"),
        F.stddev("value").alias("stddev"),
        F.min("value").alias("min_value"),
        F.max("value").alias("max_value"),
    ).collect()[0]

    print(f"   📊 Statistics:")
    print(f"      • Total: {stats['total_count']:,} records")
    print(f"      • Mean: {stats['mean']:,.2f}")
    print(f"      • Std Dev: {stats['stddev']:,.2f}")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 9: Sort operation
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("📋 STAGE 9/10: SORT operation (triggers shuffle)")
    print("   Sorting by value descending")
    print("─" * 70)

    sorted_df = df1.orderBy(F.col("value").desc())
    top_records = sorted_df.limit(10).collect()

    print("   📊 Top 5 records by value:")
    for i, row in enumerate(top_records[:5], 1):
        print(f"      {i}. ID: {row['id']:,} → Value: {row['value']:,}")
    stage_pause()

    # ═══════════════════════════════════════════════════════════════════════════
    # STAGE 10: Final summary
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "─" * 70)
    print("🎯 STAGE 10/10: Final aggregation...")
    print("─" * 70)

    final_result = agg1.orderBy(F.col("count").desc()).limit(5)
    print("\n   🏆 Top 5 Categories:")
    final_result.show(5, truncate=False)

    # ═══════════════════════════════════════════════════════════════════════════
    # CLEANUP
    # ═══════════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("✅ JOB COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\n📊 Execution Summary:")
    print(f"   • Records Processed: {RECORDS_SMALL + RECORDS_MEDIUM:,}")
    print(f"   • Stages Executed: 10")
    print(f"   • Partitions: {PARTITIONS}")
    print("\n👀 What You Should Have Seen:")
    print("   ✓ Driver started and built DAG")
    print("   ✓ Stages created at shuffle boundaries")
    print("   ✓ Tasks running on worker nodes")
    print("   ✓ Shuffle data moving between executors")
    print("\n⏳ Keeping session alive for 10 more seconds...")
    time.sleep(10)

    # Unpersist
    df1.unpersist()

    spark.stop()
    print("\n🛑 Spark session stopped.\n")


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
