#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════════
DISTRIBUTED SPARK JOB - Runs Across Multiple Worker Nodes
═══════════════════════════════════════════════════════════════════════════════

This script demonstrates distributed computing across a Spark cluster.
Each partition is processed on a different worker node.

Run with:
    docker exec spark-master spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/example_job.py
"""

import os
import socket

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    print("=" * 70)
    print("🚀 STARTING DISTRIBUTED SPARK JOB")
    print("=" * 70)

    # Create Spark session connected to the cluster
    spark = SparkSession.builder.appName("Distributed Cluster Demo").getOrCreate()

    sc = spark.sparkContext

    print(f"\n📍 Driver running on: {socket.gethostname()}")
    print(f"📍 Spark Master: {sc.master}")
    print(f"📍 App ID: {sc.applicationId}")

    # ─────────────────────────────────────────────────────────────────────────
    # Create sample data distributed across workers
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("📊 CREATING DISTRIBUTED DATA")
    print("─" * 70)

    # Create RDD with 12 partitions (will be distributed across 3 workers)
    data = range(1, 1000001)  # 1 million numbers
    rdd = sc.parallelize(data, numSlices=12)

    print(f"✓ Created RDD with {rdd.getNumPartitions()} partitions")
    print(f"✓ Data will be distributed across worker nodes")

    # ─────────────────────────────────────────────────────────────────────────
    # Show which worker processes each partition
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("🖥️  WORKER NODE EXECUTION")
    print("─" * 70)

    def process_partition(iterator):
        """This function runs on WORKER nodes, not the driver!"""
        hostname = socket.gethostname()
        pid = os.getpid()
        # Use len() with list() instead of sum() to avoid conflict with PySpark's sum
        data_list = list(iterator)
        count = len(data_list)
        return [(hostname, pid, count)]

    # See which workers processed which partitions
    worker_stats = rdd.mapPartitions(process_partition).collect()

    print("\nPartition Distribution Across Workers:")
    print("-" * 50)

    worker_counts = {}
    for hostname, pid, count in worker_stats:
        if hostname not in worker_counts:
            worker_counts[hostname] = {"partitions": 0, "records": 0}
        worker_counts[hostname]["partitions"] += 1
        worker_counts[hostname]["records"] += count

    for worker, stats in sorted(worker_counts.items()):
        print(
            f"  {worker}: {stats['partitions']} partitions, {stats['records']:,} records"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Run distributed computation
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("⚡ DISTRIBUTED COMPUTATION")
    print("─" * 70)

    # This computation runs in parallel across all workers
    total_sum = rdd.reduce(lambda a, b: a + b)
    count = rdd.count()
    avg = total_sum / count

    print(f"  Total records: {count:,}")
    print(f"  Sum: {total_sum:,}")
    print(f"  Average: {avg:,.2f}")

    # ─────────────────────────────────────────────────────────────────────────
    # DataFrame example with distributed processing
    # ─────────────────────────────────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("📋 DATAFRAME DISTRIBUTED OPERATIONS")
    print("─" * 70)

    # Create DataFrame
    df = spark.range(0, 1000000, 1, numPartitions=12)
    df = df.withColumn("squared", F.col("id") * F.col("id"))
    df = df.withColumn("category", (F.col("id") % 10).cast("string"))

    # Aggregation runs distributed across workers
    result = (
        df.groupBy("category")
        .agg(
            F.count("*").alias("count"),
            F.sum("squared").alias("sum_squared"),
            F.avg("id").alias("avg_id"),
        )
        .orderBy("category")
    )

    print("\nAggregation Results (computed across all workers):")
    result.show()

    print("\n" + "=" * 70)
    print("✅ JOB COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print("\n📊 Check the Spark UI at http://localhost:8080 to see:")
    print("   • Worker nodes registered")
    print("   • Job stages and tasks")
    print("   • Task distribution across executors")
    print()

    spark.stop()


if __name__ == "__main__":
    main()
