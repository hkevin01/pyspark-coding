#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PYSPARK OPTIMIZATION BEST PRACTICES - COMPREHENSIVE GUIDE
================================================================================

MODULE OVERVIEW:
----------------
This module demonstrates the CORRECT approaches to common PySpark optimization
pitfalls, explaining:
• WHAT the correct approach is
• WHY it's better than the anti-pattern
• WHEN to apply it (timing matters!)
• HOW to implement it properly

ANTI-PATTERNS vs BEST PRACTICES:
---------------------------------

┌────────────────────────────────────────────────────────────────────────────┐
│ ❌ ANTI-PATTERN                    ✅ BEST PRACTICE                         │
├────────────────────────────────────────────────────────────────────────────┤
│ collect() on large data            show(n) or write to storage             │
│ → Driver OOM crash                 → Controlled memory usage               │
│                                                                             │
│ Loading model per row              Broadcast model once                    │
│ → 10M rows = 10M model loads       → Load once, share across executors    │
│                                                                             │
│ Ignoring data skew                 Salt keys + repartition                 │
│ → 1 partition takes 99% time       → Balanced work distribution           │
│                                                                             │
│ Schema inference                   Explicit schema definition              │
│ → Slow, type errors                → Fast, type-safe                       │
│                                                                             │
│ No caching                         Cache frequently-used DFs               │
│ → Recompute from source            → Compute once, reuse many times       │
│                                                                             │
│ UDFs for everything                Spark SQL functions first               │
│ → Python serialization overhead    → Native Spark execution (10-100× faster)│
└────────────────────────────────────────────────────────────────────────────┘

TIMING IS CRITICAL:
-------------------

The ORDER and TIMING of optimizations matter! Apply them at the right stage:

1. DEFINE SCHEMA          → At data load (before any processing)
2. BROADCAST MODELS       → Before map/filter operations that use them
3. CACHE STRATEGICALLY    → After expensive transformations, before reuse
4. USE SPARK SQL FIRST    → During transformation logic
5. HANDLE SKEW            → When joins/aggregations are slow
6. AVOID COLLECT()        → At output stage (use show/write instead)

Think of it like cooking:
• Add salt at the right time (not all at the end!)
• Prep ingredients before cooking (schema definition)
• Use proper tools (Spark SQL not UDFs)
• Don't bring entire ocean to your kitchen (no collect())

PERFORMANCE IMPACT:
-------------------

Real-world speedups from these practices:

┌──────────────────────────────┬────────────────┬────────────────┐
│ Optimization                 │ Typical Speedup│ Memory Savings │
├──────────────────────────────┼────────────────┼────────────────┤
│ Explicit schema vs inference │ 2-5×           │ 20-30%         │
│ Broadcast vs repeated load   │ 10-100×        │ 90%+           │
│ Caching frequently-used DFs  │ 3-10×          │ Varies         │
│ Spark SQL vs UDFs            │ 10-100×        │ 50%+           │
│ Salting skewed keys          │ 5-50×          │ Better balance │
│ show() vs collect()          │ Infinite       │ Prevents OOM   │
└──────────────────────────────┴────────────────┴────────────────┘

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - Optimization Best Practices
UPDATED: December 2024
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, rand, concat, expr, broadcast,
    count, sum as spark_sum, avg, max as spark_max,
    explode, array, struct, to_json, from_json,
    udf, pandas_udf, PandasUDFType
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, ArrayType, MapType, LongType
)
from pyspark.broadcast import Broadcast
import time
import numpy as np
import pandas as pd
from typing import Iterator


def create_spark_session():
    """
    Create SparkSession with optimized configuration.
    
    WHY these configurations:
    • adaptive.enabled: Let Spark optimize at runtime
    • shuffle.partitions: Default 200 too high for small data, too low for large
    • memory settings: Prevent OOM, enable caching
    """
    return SparkSession.builder \
        .appName("OptimizationBestPractices") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "auto") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .getOrCreate()


def best_practice_1_explicit_schema(spark: SparkSession):
    """
    ✅ BEST PRACTICE #1: Define Schema Explicitly
    
    WHEN: At data load time, BEFORE any processing
    WHY: Fast loading, type safety, memory efficiency
    HOW: Define StructType with exact column types
    
    ❌ Anti-Pattern:
    ───────────────
    df = spark.read.csv("data.csv", header=True, inferSchema=True)
    
    Problems:
    • Reads data TWICE (once for inference, once for loading)
    • May infer wrong types (e.g., "123" could be String or Int)
    • Slower startup (scans all data to infer)
    • Memory waste (wrong types use more memory)
    
    ✅ Best Practice:
    ─────────────────
    schema = StructType([...])
    df = spark.read.csv("data.csv", header=True, schema=schema)
    
    Benefits:
    • Reads data ONCE (50% faster)
    • Guaranteed type safety (no surprises)
    • Better memory usage (correct types)
    • Catches errors early (invalid data fails fast)
    """
    print("=" * 80)
    print("BEST PRACTICE #1: EXPLICIT SCHEMA DEFINITION")
    print("=" * 80)
    
    # ❌ WRONG: Schema inference (slow, unreliable)
    print("\n❌ Anti-Pattern: Schema Inference")
    print("   df = spark.read.csv(..., inferSchema=True)")
    print("   • Reads data twice")
    print("   • May infer wrong types")
    print("   • Slower startup\n")
    
    # ✅ CORRECT: Explicit schema
    print("✅ Best Practice: Explicit Schema Definition")
    print("   WHEN: At data load, BEFORE processing")
    print("   WHY: Fast, type-safe, memory-efficient\n")
    
    # Define schema explicitly
    sales_schema = StructType([
        StructField("transaction_id", StringType(), False),      # NOT NULL
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), True),              # NULLABLE
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("region", StringType(), True),
        StructField("payment_method", StringType(), True)
    ])
    
    print("Schema defined:")
    print(sales_schema.simpleString())
    
    # Create sample data
    sample_data = [
        ("TXN001", "C001", "P001", "Electronics", 1200.50, 1, "2024-01-15 10:30:00", "US-WEST", "Credit"),
        ("TXN002", "C002", "P002", "Electronics", 899.99, 2, "2024-01-15 11:45:00", "US-EAST", "Debit"),
        ("TXN003", "C001", "P003", "Clothing", 49.99, 3, "2024-01-15 12:00:00", "US-WEST", "Credit"),
        ("TXN004", "C003", "P001", "Electronics", 1200.50, 1, "2024-01-15 13:15:00", "EU", "PayPal"),
        ("TXN005", "C002", "P004", "Books", 29.99, 5, "2024-01-15 14:30:00", "US-EAST", "Credit")
    ] * 200  # Simulate 1000 rows
    
    # Apply schema at load time (WHEN: Now, at the start!)
    df_sales = spark.createDataFrame(sample_data, schema=sales_schema)
    
    print(f"\n✅ DataFrame created with explicit schema")
    print(f"   Rows: {df_sales.count():,}")
    print(f"   Columns: {len(df_sales.columns)}")
    print(f"\n   Benefits:")
    print(f"   • Type safety guaranteed")
    print(f"   • Fast load (single pass)")
    print(f"   • Memory optimized")
    
    df_sales.printSchema()
    df_sales.show(3, truncate=False)
    
    return df_sales


def best_practice_2_broadcast_models(spark: SparkSession, df: "DataFrame"):
    """
    ✅ BEST PRACTICE #2: Broadcast Models and Lookup Tables
    
    WHEN: BEFORE map/filter/UDF operations that use the model/lookup
    WHY: Load once, share across all executors (not once per row!)
    HOW: Use spark.sparkContext.broadcast() or broadcast() hint
    
    ❌ Anti-Pattern:
    ───────────────
    def predict_udf(features):
        model = load_model()  # ❌ Loads model FOR EACH ROW!
        return model.predict(features)
    
    If you have 10M rows → loads model 10M times!
    • Extremely slow (I/O bottleneck)
    • Wastes memory (multiple copies)
    • Network overhead (repeated loads)
    
    ✅ Best Practice:
    ─────────────────
    model = load_model()
    broadcast_model = spark.sparkContext.broadcast(model)
    
    def predict_udf(features):
        return broadcast_model.value.predict(features)  # ✅ Reuses model!
    
    Loads model ONCE per executor (not per row):
    • 10M rows → loads model ~10 times (one per executor)
    • 1,000,000× fewer loads!
    • Dramatic speedup (10-100×)
    
    TIMING: Broadcast BEFORE the operation that uses it!
    """
    print("\n" + "=" * 80)
    print("BEST PRACTICE #2: BROADCAST MODELS AND LOOKUP TABLES")
    print("=" * 80)
    
    # ❌ WRONG: Load model/lookup in UDF (per row)
    print("\n❌ Anti-Pattern: Load Model Per Row")
    print("   def predict_udf(x):")
    print("       model = load_model()  # ❌ Loads for EACH row!")
    print("       return model.predict(x)")
    print("\n   Impact: 10M rows = 10M model loads = DISASTER\n")
    
    # ✅ CORRECT: Broadcast model once
    print("✅ Best Practice: Broadcast Model Once")
    print("   WHEN: BEFORE map/filter/UDF operations")
    print("   WHY: Load once per executor, not per row\n")
    
    # Simulate a "model" (pricing rules lookup table)
    # In production, this could be a ML model, config dict, etc.
    pricing_rules = {
        "Electronics": {"discount": 0.10, "tax": 0.08, "shipping": 15.99},
        "Clothing": {"discount": 0.15, "tax": 0.06, "shipping": 5.99},
        "Books": {"discount": 0.05, "tax": 0.00, "shipping": 3.99},
        "default": {"discount": 0.00, "tax": 0.07, "shipping": 9.99}
    }
    
    print("Pricing rules (simulated model):")
    for category, rules in list(pricing_rules.items())[:3]:
        print(f"  {category}: {rules}")
    
    # TIMING: Broadcast NOW, before we use it!
    # This is THE RIGHT TIME - after defining, before using
    broadcast_rules = spark.sparkContext.broadcast(pricing_rules)
    
    print(f"\n✅ Model broadcast to all executors")
    print(f"   Size: {len(pricing_rules)} categories")
    print(f"   Cost: Load once per executor (not per row!)")
    
    # Define UDF that uses broadcast variable
    # Note: It accesses broadcast_rules.value (not loading fresh each time)
    def calculate_final_price_udf(category: str, amount: float) -> float:
        """
        Calculate final price with discount, tax, and shipping.
        
        WHY this works well:
        • broadcast_rules.value gives us the model (already loaded!)
        • No I/O per row
        • No repeated deserialization
        • Shared across all tasks on this executor
        """
        rules = broadcast_rules.value.get(category, broadcast_rules.value["default"])
        discounted = amount * (1 - rules["discount"])
        with_tax = discounted * (1 + rules["tax"])
        final = with_tax + rules["shipping"]
        return round(final, 2)
    
    # Register UDF
    calculate_price_spark_udf = udf(calculate_final_price_udf, DoubleType())
    
    # Apply UDF (using broadcast model)
    df_with_prices = df.withColumn(
        "final_price",
        calculate_price_spark_udf(col("category"), col("amount"))
    )
    
    print("\n✅ Applied UDF using broadcast model")
    print("   Each executor loads model ONCE")
    print("   All rows on that executor reuse the same model")
    
    df_with_prices.select(
        "transaction_id", "category", "amount", "final_price"
    ).show(5, truncate=False)
    
    print("\n💡 Performance Impact:")
    print("   ❌ Without broadcast: 1000 rows = 1000 model loads")
    print("   ✅ With broadcast: 1000 rows = ~10 model loads (one per executor)")
    print("   🚀 Speedup: ~100× faster!")
    
    return df_with_prices, broadcast_rules


def best_practice_3_avoid_collect(spark: SparkSession, df: "DataFrame"):
    """
    ✅ BEST PRACTICE #3: Never collect() Large Data to Driver
    
    WHEN: At output/inspection stage
    WHY: Driver has limited memory (will crash with OOM)
    HOW: Use show(n), write(), or aggregations instead
    
    ❌ Anti-Pattern:
    ───────────────
    results = df.collect()  # ❌ Brings ALL data to driver!
    for row in results:
        print(row)
    
    Problems:
    • Driver OOM crash (driver has ~4-16GB, data has 500GB)
    • Defeats purpose of distributed computing
    • Single point of failure
    • Network bottleneck (all executors → one driver)
    
    ✅ Best Practice:
    ─────────────────
    # For inspection:
    df.show(20)              # Show sample (doesn't load all data)
    
    # For analysis:
    df.groupBy("category").count().show()  # Aggregate first!
    
    # For output:
    df.write.parquet("output/")  # Distributed write (each executor writes)
    
    # For small results:
    summary = df.groupBy("region").count().collect()  # OK (few rows)
    
    TIMING: At the END, when outputting results
    """
    print("\n" + "=" * 80)
    print("BEST PRACTICE #3: AVOID collect() ON LARGE DATA")
    print("=" * 80)
    
    # ❌ WRONG: collect() all data to driver
    print("\n❌ Anti-Pattern: collect() Large Data")
    print("   results = df.collect()  # ❌ Brings ALL data to driver!")
    print("   for row in results:")
    print("       process(row)")
    print("\n   Impact:")
    print("   • Driver OOM crash (500GB data → 4GB driver)")
    print("   • Defeats distributed computing")
    print("   • Single point of failure\n")
    
    # ✅ CORRECT: Use appropriate alternatives
    print("✅ Best Practice: Use Alternatives to collect()")
    print("   WHEN: At output stage, for inspection or writing")
    print("   WHY: Driver has limited memory\n")
    
    print("📊 OPTION 1: show() for inspection")
    print("   df.show(20)  # Only brings 20 rows to driver")
    df.show(5, truncate=False)
    
    print("\n📊 OPTION 2: Aggregate first, then collect()")
    print("   summary = df.groupBy('category').count().collect()")
    print("   ✅ OK because result is small (few categories)\n")
    
    summary = df.groupBy("category").agg(
        count("*").alias("count"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_amount")
    )
    summary.show(truncate=False)
    
    # This collect() is OK because summary is small (few rows)
    summary_rows = summary.collect()
    print(f"   ✅ Collected {len(summary_rows)} rows (SMALL result set)")
    
    print("\n📊 OPTION 3: Write to storage (distributed)")
    print("   df.write.parquet('output/')  # Each executor writes its partitions")
    print("   ✅ No driver bottleneck, truly distributed")
    
    # Simulate writing (would write to disk in production)
    print("\n   Partitions before write:")
    print(f"   • Current partitions: {df.rdd.getNumPartitions()}")
    print(f"   • Each executor writes its own partitions")
    print(f"   • Driver only coordinates, doesn't handle data")
    
    print("\n💡 Memory Comparison:")
    print("   Scenario: 100GB data, 4GB driver memory")
    print("   ❌ collect(): Tries to load 100GB → OOM crash")
    print("   ✅ show(20): Loads ~1KB → works perfectly")
    print("   ✅ write(): Each executor writes its chunk → works perfectly")
    
    print("\n🎯 RULE OF THUMB:")
    print("   • Inspecting: show(n) or take(n)")
    print("   • Analyzing: Aggregate first, then collect()")
    print("   • Outputting: write() to storage")
    print("   • Never: collect() entire large DataFrame")
    
    return df


def best_practice_4_handle_data_skew(spark: SparkSession, df: "DataFrame"):
    """
    ✅ BEST PRACTICE #4: Handle Data Skew
    
    WHEN: When joins/aggregations are slow with uneven partition sizes
    WHY: One giant partition blocks entire job (Spark is only as fast as slowest task)
    HOW: Salt keys, repartition, increase parallelism
    
    ❌ Anti-Pattern:
    ───────────────
    df.groupBy("customer_id").count()  # ❌ If one customer has 90% of data!
    
    Problems:
    • One partition has 90% of data (whale customer)
    • Other partitions finish in 1 second
    • Giant partition takes 100 seconds
    • Total time: 100 seconds (limited by slowest!)
    • 99% of cluster sits idle waiting
    
    ✅ Best Practice: SALTING
    ─────────────────────────
    # Add random salt to split hot key
    df_salted = df.withColumn("salt", (rand() * 10).cast("int"))
    df_salted = df_salted.withColumn(
        "customer_id_salted",
        concat(col("customer_id"), lit("_"), col("salt"))
    )
    
    # Group by salted key (splits whale customer into 10 pieces)
    df_partial = df_salted.groupBy("customer_id_salted").count()
    
    # Remove salt and combine
    df_final = df_partial.withColumn(
        "customer_id",
        expr("substring(customer_id_salted, 1, length(customer_id_salted) - 2)")
    ).groupBy("customer_id").sum("count")
    
    Result:
    • Whale customer split across 10 partitions
    • Each partition: ~10 seconds (instead of 1 waiting for 100)
    • Total time: ~10 seconds
    • 10× speedup!
    
    TIMING: Apply WHEN you detect skew (slow stages in Spark UI)
    """
    print("\n" + "=" * 80)
    print("BEST PRACTICE #4: HANDLE DATA SKEW")
    print("=" * 80)
    
    # ❌ WRONG: Ignore skew
    print("\n❌ Anti-Pattern: Ignore Data Skew")
    print("   df.groupBy('customer_id').count()")
    print("   Problem: If C001 has 90% of data:")
    print("   • 1 partition: 90% of work (takes 100 seconds)")
    print("   • 9 partitions: 10% of work (takes 1 second each)")
    print("   • Total time: 100 seconds (limited by slowest!)")
    print("   • 99% of cluster idle waiting for that 1 partition\n")
    
    # ✅ CORRECT: Salt skewed keys
    print("✅ Best Practice: Salt Skewed Keys")
    print("   WHEN: When joins/aggregations show uneven partition sizes")
    print("   WHY: Split hot key across multiple partitions")
    print("   HOW: Add random salt, group twice\n")
    
    # Simulate skewed data (C001 has 70% of transactions)
    skewed_data = [
        ("TXN_C001_" + str(i), "C001", "P001", "Electronics", 100.0, 1)
        for i in range(700)  # Customer C001: 700 transactions
    ] + [
        ("TXN_C002_" + str(i), "C002", "P002", "Books", 50.0, 1)
        for i in range(100)  # Customer C002: 100 transactions
    ] + [
        ("TXN_C003_" + str(i), "C003", "P003", "Clothing", 75.0, 1)
        for i in range(200)  # Customer C003: 200 transactions
    ]
    
    skewed_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False)
    ])
    
    df_skewed = spark.createDataFrame(skewed_data, schema=skewed_schema)
    
    print("📊 Skewed Data Distribution:")
    df_skewed.groupBy("customer_id").count().orderBy(col("count").desc()).show()
    print("   ⚠️  C001 has 70% of data (700/1000 transactions)")
    print("   This will cause skew in groupBy/join operations!\n")
    
    # STEP 1: Add salt (TIMING: Now, before expensive operations)
    print("STEP 1: Add Salt to Split Hot Keys")
    print("   WHY: rand() * 10 creates uniform distribution 0-9")
    print("   EFFECT: C001 becomes C001_0, C001_1, ..., C001_9\n")
    
    df_salted = df_skewed.withColumn(
        "salt",
        (rand() * 10).cast("int")  # Random 0-9
    ).withColumn(
        "customer_id_salted",
        concat(col("customer_id"), lit("_"), col("salt"))  # C001 → C001_0, C001_1, etc.
    )
    
    print("✅ Salt added:")
    df_salted.select("customer_id", "salt", "customer_id_salted").show(10)
    
    # STEP 2: Partial aggregation (with salt)
    print("\nSTEP 2: Partial Aggregation (with salt)")
    print("   WHY: Distribute C001's work across 10 partitions")
    print("   EFFECT: Each partition handles ~70 rows instead of 700\n")
    
    df_partial = df_salted.groupBy("customer_id", "customer_id_salted").agg(
        count("*").alias("partial_count"),
        spark_sum("amount").alias("partial_sum")
    )
    
    print("✅ Partial aggregation (C001 split into 10 pieces):")
    df_partial.filter(col("customer_id") == "C001").show(10)
    
    # STEP 3: Final aggregation (remove salt)
    print("\nSTEP 3: Final Aggregation (remove salt)")
    print("   WHY: Combine the 10 partial results for C001")
    print("   EFFECT: C001_0 + C001_1 + ... + C001_9 = C001 total\n")
    
    df_final = df_partial.groupBy("customer_id").agg(
        spark_sum("partial_count").alias("total_transactions"),
        spark_sum("partial_sum").alias("total_revenue")
    )
    
    print("✅ Final aggregation (salt removed):")
    df_final.orderBy(col("total_transactions").desc()).show()
    
    print("\n💡 Performance Impact:")
    print("   ❌ Without salting:")
    print("      • C001 partition: 700 rows → takes 100 seconds")
    print("      • Other partitions: done in 1 second")
    print("      • Total time: 100 seconds")
    print("      • Resource utilization: 10% (one partition working)")
    print("\n   ✅ With salting:")
    print("      • C001 split across 10 partitions: 70 rows each")
    print("      • Each partition: ~10 seconds")
    print("      • Total time: ~10 seconds")
    print("      • Resource utilization: 100% (all partitions working)")
    print("      • 🚀 Speedup: 10× faster!")
    
    print("\n🎯 When to Apply Salting:")
    print("   • Spark UI shows uneven partition sizes")
    print("   • One task takes much longer than others")
    print("   • Join with hot keys (few keys have lots of data)")
    print("   • GroupBy with skewed distribution")
    
    return df_final


def best_practice_5_cache_strategically(spark: SparkSession, df: "DataFrame"):
    """
    ✅ BEST PRACTICE #5: Cache Frequently-Used DataFrames
    
    WHEN: After expensive transformations, BEFORE multiple reuses
    WHY: Avoid recomputing from source multiple times
    HOW: Use .cache() or .persist() strategically
    
    ❌ Anti-Pattern:
    ───────────────
    df_expensive = df.join(...).filter(...).groupBy(...)  # Expensive operations
    
    result1 = df_expensive.filter(condition1).count()     # Computes from source
    result2 = df_expensive.filter(condition2).count()     # Computes from source AGAIN
    result3 = df_expensive.filter(condition3).count()     # Computes from source AGAIN
    
    3 actions = 3 full recomputations of join + filter + groupBy!
    
    ✅ Best Practice:
    ─────────────────
    df_expensive = df.join(...).filter(...).groupBy(...)
    df_expensive.cache()  # TIMING: After expensive ops, BEFORE reuse!
    
    result1 = df_expensive.filter(condition1).count()     # Computes & caches
    result2 = df_expensive.filter(condition2).count()     # Reads from cache!
    result3 = df_expensive.filter(condition3).count()     # Reads from cache!
    
    1 computation + 2 cache reads = 3× faster!
    
    TIMING:
    • TOO EARLY: Caching raw data (not expensive yet)
    • JUST RIGHT: After expensive transformations, before reuse
    • TOO LATE: After all uses (no benefit)
    
    Think: "I will use this DF multiple times, cache it NOW!"
    """
    print("\n" + "=" * 80)
    print("BEST PRACTICE #5: CACHE STRATEGICALLY")
    print("=" * 80)
    
    # ❌ WRONG: No caching (recomputes every time)
    print("\n❌ Anti-Pattern: No Caching")
    print("   df_exp = df.join(...).filter(...).groupBy(...)")
    print("   result1 = df_exp.filter(cond1).count()  # Full computation")
    print("   result2 = df_exp.filter(cond2).count()  # Full computation AGAIN")
    print("   result3 = df_exp.filter(cond3).count()  # Full computation AGAIN")
    print("\n   Impact: 3 actions = 3 full recomputations = 3× slower\n")
    
    # ✅ CORRECT: Cache after expensive ops, before reuse
    print("✅ Best Practice: Cache After Expensive Transformations")
    print("   WHEN: After expensive ops, BEFORE multiple reuses")
    print("   WHY: Compute once, reuse many times\n")
    
    # Simulate expensive transformation
    print("STEP 1: Expensive Transformations (join + aggregation)")
    
    # Create a customers DataFrame for join
    customers_data = [
        ("C001", "Alice", "Gold", "US-WEST"),
        ("C002", "Bob", "Silver", "US-EAST"),
        ("C003", "Charlie", "Bronze", "EU")
    ]
    customers_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("tier", StringType(), False),
        StructField("region", StringType(), False)
    ])
    df_customers = spark.createDataFrame(customers_data, schema=customers_schema)
    
    # Expensive transformation (join + aggregation)
    df_expensive = df.join(
        df_customers,
        on="customer_id",
        how="inner"
    ).groupBy("customer_id", "name", "tier", "region").agg(
        count("*").alias("transaction_count"),
        spark_sum("amount").alias("total_spent"),
        avg("amount").alias("avg_transaction"),
        spark_max("amount").alias("max_transaction")
    )
    
    print("   ✅ Expensive DataFrame created (join + aggregation)")
    print("   This is expensive to compute!\n")
    
    # TIMING: Cache NOW (after expensive ops, before reuse)!
    print("STEP 2: Cache Now (Before Multiple Uses)")
    print("   ⏰ TIMING: Right after expensive transformation")
    print("   ❓ WHY NOW: About to use this DF 3 times\n")
    
    df_expensive.cache()  # Cache in memory
    
    print("   ✅ DataFrame cached")
    print("   First action will compute & cache")
    print("   Subsequent actions will read from cache\n")
    
    # Trigger caching with first action
    print("STEP 3: First Use (Computes and Caches)")
    print("   Query 1: Count Gold tier customers")
    start_time = time.time()
    count_gold = df_expensive.filter(col("tier") == "Gold").count()
    time_1 = time.time() - start_time
    print(f"   ✅ Gold customers: {count_gold} (computed in {time_1:.3f}s)")
    print("   • Ran full computation (join + aggregation)")
    print("   • Stored result in cache\n")
    
    # Second use (reads from cache)
    print("STEP 4: Second Use (Reads from Cache)")
    print("   Query 2: Count Silver tier customers")
    start_time = time.time()
    count_silver = df_expensive.filter(col("tier") == "Silver").count()
    time_2 = time.time() - start_time
    print(f"   ✅ Silver customers: {count_silver} (computed in {time_2:.3f}s)")
    print("   • Read from cache (no recomputation!)")
    print(f"   • {time_1/max(time_2, 0.001):.1f}× faster than first query\n")
    
    # Third use (reads from cache)
    print("STEP 5: Third Use (Reads from Cache)")
    print("   Query 3: High spenders (total_spent > 1000)")
    start_time = time.time()
    high_spenders = df_expensive.filter(col("total_spent") > 1000)
    count_high = high_spenders.count()
    time_3 = time.time() - start_time
    print(f"   ✅ High spenders: {count_high} (computed in {time_3:.3f}s)")
    print("   • Read from cache (no recomputation!)")
    print(f"   • {time_1/max(time_3, 0.001):.1f}× faster than first query\n")
    
    # Show results
    print("📊 Customer Analysis Results:")
    df_expensive.show(truncate=False)
    
    print("\n💡 Performance Impact:")
    print(f"   ❌ Without caching:")
    print(f"      • Query 1: {time_1:.3f}s (full computation)")
    print(f"      • Query 2: {time_1:.3f}s (full computation again)")
    print(f"      • Query 3: {time_1:.3f}s (full computation again)")
    print(f"      • Total: {time_1 * 3:.3f}s")
    print(f"\n   ✅ With caching:")
    print(f"      • Query 1: {time_1:.3f}s (compute & cache)")
    print(f"      • Query 2: {time_2:.3f}s (read from cache)")
    print(f"      • Query 3: {time_3:.3f}s (read from cache)")
    print(f"      • Total: {time_1 + time_2 + time_3:.3f}s")
    print(f"      • 🚀 Speedup: ~{(time_1 * 3) / max(time_1 + time_2 + time_3, 0.001):.1f}× faster!")
    
    print("\n🎯 When to Cache:")
    print("   ✅ DO cache when:")
    print("      • DataFrame will be reused multiple times")
    print("      • After expensive transformations (joins, aggregations)")
    print("      • Iterative algorithms (ML training)")
    print("\n   ❌ DON'T cache when:")
    print("      • DataFrame used only once")
    print("      • Data is too large (exceeds memory)")
    print("      • Source read is fast (e.g., already in Parquet)")
    
    print("\n🧹 Cleanup: Unpersist when done")
    df_expensive.unpersist()
    print("   ✅ Cache released (frees memory for other operations)")
    
    return df_expensive


def best_practice_6_use_spark_sql_not_udfs(spark: SparkSession, df: "DataFrame"):
    """
    ✅ BEST PRACTICE #6: Use Spark SQL Functions, Not UDFs
    
    WHEN: During transformation logic (any data manipulation)
    WHY: Native Spark functions are 10-100× faster than UDFs
    HOW: Use functions from pyspark.sql.functions instead of UDFs
    
    ❌ Anti-Pattern:
    ───────────────
    def calculate_discount_udf(amount):
        if amount > 1000:
            return amount * 0.9
        elif amount > 500:
            return amount * 0.95
        else:
            return amount
    
    df.withColumn("discounted", udf_function(col("amount")))
    
    Problems:
    • Python UDF = serialize data → Python process → deserialize result
    • Row-by-row processing (can't vectorize)
    • No Catalyst optimization
    • GIL (Global Interpreter Lock) bottleneck
    • 10-100× slower than native Spark
    
    ✅ Best Practice:
    ─────────────────
    df.withColumn(
        "discounted",
        when(col("amount") > 1000, col("amount") * 0.9)
        .when(col("amount") > 500, col("amount") * 0.95)
        .otherwise(col("amount"))
    )
    
    Benefits:
    • Native Spark execution (Tungsten engine)
    • Vectorized operations (SIMD)
    • Catalyst query optimization
    • No serialization overhead
    • 10-100× faster!
    
    TIMING: Use Spark SQL functions FIRST, UDFs only when absolutely necessary
    """
    print("\n" + "=" * 80)
    print("BEST PRACTICE #6: USE SPARK SQL FUNCTIONS, NOT UDFs")
    print("=" * 80)
    
    # ❌ WRONG: Use UDF for simple logic
    print("\n❌ Anti-Pattern: UDF for Simple Logic")
    print("   def calculate_discount_udf(amount):")
    print("       if amount > 1000: return amount * 0.9")
    print("       elif amount > 500: return amount * 0.95")
    print("       else: return amount")
    print("\n   df.withColumn('discounted', udf_function(col('amount')))")
    print("\n   Impact:")
    print("   • Serialization: Spark → Python → Spark")
    print("   • Row-by-row processing (no vectorization)")
    print("   • No Catalyst optimization")
    print("   • 10-100× slower than native Spark\n")
    
    # ✅ CORRECT: Use Spark SQL functions
    print("✅ Best Practice: Use Native Spark SQL Functions")
    print("   WHEN: For any data transformation logic")
    print("   WHY: Native execution is 10-100× faster\n")
    
    # Example 1: Conditional logic (when/otherwise)
    print("EXAMPLE 1: Conditional Logic")
    print("   Goal: Apply tiered discounts based on amount\n")
    
    df_with_discount = df.withColumn(
        "discount_rate",
        when(col("amount") > 1000, lit(0.10))
        .when(col("amount") > 500, lit(0.05))
        .otherwise(lit(0.0))
    ).withColumn(
        "discounted_amount",
        col("amount") * (1 - col("discount_rate"))
    )
    
    print("   ✅ Using when/otherwise (native Spark):")
    df_with_discount.select(
        "transaction_id", "amount", "discount_rate", "discounted_amount"
    ).show(5, truncate=False)
    
    # Example 2: String operations
    print("\nEXAMPLE 2: String Operations")
    print("   Goal: Create full product code from parts\n")
    
    from pyspark.sql.functions import concat_ws, upper, substring
    
    df_with_codes = df.withColumn(
        "product_code",
        concat_ws("-", 
                  upper(col("category")),
                  col("product_id"),
                  substring(col("region"), 1, 2))
    )
    
    print("   ✅ Using concat_ws/upper/substring (native Spark):")
    df_with_codes.select(
        "product_id", "category", "region", "product_code"
    ).show(5, truncate=False)
    
    # Example 3: Numerical operations
    print("\nEXAMPLE 3: Numerical Operations")
    print("   Goal: Calculate derived metrics\n")
    
    from pyspark.sql.functions import round as spark_round, sqrt, pow
    
    df_with_metrics = df.withColumn(
        "amount_squared", pow(col("amount"), 2)
    ).withColumn(
        "amount_sqrt", sqrt(col("amount"))
    ).withColumn(
        "amount_rounded", spark_round(col("amount"), 0)
    )
    
    print("   ✅ Using pow/sqrt/round (native Spark):")
    df_with_metrics.select(
        "amount", "amount_squared", "amount_sqrt", "amount_rounded"
    ).show(5, truncate=False)
    
    # Example 4: Date/time operations
    print("\nEXAMPLE 4: Date/Time Operations")
    print("   Goal: Extract date parts and calculate age\n")
    
    from pyspark.sql.functions import year, month, dayofweek, date_format, current_date, datediff
    
    df_with_dates = df.withColumn(
        "year", year(col("timestamp"))
    ).withColumn(
        "month", month(col("timestamp"))
    ).withColumn(
        "day_of_week", dayofweek(col("timestamp"))
    ).withColumn(
        "formatted_date", date_format(col("timestamp"), "yyyy-MM-dd")
    ).withColumn(
        "days_ago", datediff(current_date(), col("timestamp"))
    )
    
    print("   ✅ Using year/month/dayofweek/datediff (native Spark):")
    df_with_dates.select(
        "timestamp", "year", "month", "day_of_week", "days_ago"
    ).show(5, truncate=False)
    
    print("\n💡 Performance Comparison:")
    print("   ❌ UDF approach:")
    print("      • Serialization: Java → Python → Java")
    print("      • Row-by-row processing")
    print("      • Python interpreter overhead")
    print("      • No Catalyst optimization")
    print("      • Time: 10-100 seconds")
    print("\n   ✅ Native Spark SQL:")
    print("      • No serialization (stays in JVM)")
    print("      • Vectorized operations (SIMD)")
    print("      • Catalyst query optimization")
    print("      • Tungsten execution engine")
    print("      • Time: 1 second")
    print("      • 🚀 Speedup: 10-100× faster!")
    
    print("\n🎯 When to Use UDFs:")
    print("   ✅ ONLY when:")
    print("      • Complex Python libraries needed (scikit-learn, nltk)")
    print("      • Business logic too complex for SQL")
    print("      • No equivalent Spark SQL function exists")
    print("\n   ❌ NEVER when:")
    print("      • Simple conditionals (use when/otherwise)")
    print("      • String operations (use string functions)")
    print("      • Math operations (use math functions)")
    print("      • Date operations (use date functions)")
    
    print("\n📚 Common Spark SQL Functions:")
    print("   • Conditional: when, otherwise, coalesce, nvl")
    print("   • String: concat, substring, upper, lower, trim, regexp_replace")
    print("   • Math: round, sqrt, pow, abs, ceil, floor")
    print("   • Date: year, month, day, date_add, datediff, to_date")
    print("   • Aggregate: sum, avg, count, min, max, stddev")
    print("   • Window: row_number, rank, lag, lead, first, last")
    print("   • Array: explode, array_contains, size, sort_array")
    print("   • JSON: from_json, to_json, get_json_object")
    
    return df_with_metrics


def demonstrate_all_best_practices_together():
    """
    Demonstrate all 6 best practices in a realistic workflow.
    
    This shows the TIMING and ORDER of applying optimizations:
    1. Define schema (at load)
    2. Load data with schema
    3. Broadcast models (before operations)
    4. Use Spark SQL functions (during transformations)
    5. Handle skew (when detected)
    6. Cache strategically (before reuse)
    7. Avoid collect() (at output)
    """
    print("\n" + "🔷 " * 40)
    print("COMPLETE WORKFLOW: ALL BEST PRACTICES TOGETHER")
    print("🔷 " * 40)
    
    print("""
This section demonstrates THE CORRECT ORDER and TIMING:

┌────────────────────────────────────────────────────────────┐
│ OPTIMIZATION WORKFLOW (Proper Order)                       │
├────────────────────────────────────────────────────────────┤
│ 1. ✅ Define Schema          → At data load               │
│ 2. ✅ Load with Schema        → Read data once            │
│ 3. ✅ Broadcast Models        → Before map/filter ops     │
│ 4. ✅ Use Spark SQL           → During transformations    │
│ 5. ✅ Handle Skew             → When joins/aggs are slow  │
│ 6. ✅ Cache Strategically     → Before reuse              │
│ 7. ✅ Avoid collect()         → At output stage           │
└────────────────────────────────────────────────────────────┘

Let's see this in action!
    """)
    
    spark = create_spark_session()
    
    print("\n" + "=" * 80)
    print("STEP-BY-STEP OPTIMIZED WORKFLOW")
    print("=" * 80)
    
    # Step 1: Define schema (TIMING: Before load)
    print("\n📋 STEP 1: Define Schema (BEFORE loading)")
    df_sales = best_practice_1_explicit_schema(spark)
    
    # Step 2: Broadcast model (TIMING: Before operations that use it)
    print("\n🔊 STEP 2: Broadcast Model (BEFORE map/filter operations)")
    df_with_prices, broadcast_model = best_practice_2_broadcast_models(spark, df_sales)
    
    # Step 3: Use Spark SQL (TIMING: During transformations)
    print("\n⚡ STEP 3: Use Spark SQL Functions (DURING transformations)")
    df_enriched = best_practice_6_use_spark_sql_not_udfs(spark, df_with_prices)
    
    # Step 4: Handle skew (TIMING: When detected in slow operations)
    print("\n⚖️  STEP 4: Handle Data Skew (WHEN joins/aggs are slow)")
    df_aggregated = best_practice_4_handle_data_skew(spark, df_sales)
    
    # Step 5: Cache (TIMING: Before reuse)
    print("\n💾 STEP 5: Cache Strategically (BEFORE reuse)")
    df_cached = best_practice_5_cache_strategically(spark, df_enriched)
    
    # Step 6: Avoid collect() (TIMING: At output)
    print("\n📤 STEP 6: Output Without collect() (AT output stage)")
    best_practice_3_avoid_collect(spark, df_cached)
    
    print("\n" + "=" * 80)
    print("✅ COMPLETE WORKFLOW FINISHED")
    print("=" * 80)
    
    print("""
🎓 KEY LESSONS - TIMING MATTERS!

1. Schema Definition: FIRST (at load, not later)
   ⏰ When: Before reading data
   ❓ Why: Read once, not twice
   
2. Broadcasting: BEFORE operations that need it
   ⏰ When: After model creation, before map/filter
   ❓ Why: Load once per executor, not per row
   
3. Spark SQL: DURING all transformations
   ⏰ When: Any data manipulation
   ❓ Why: 10-100× faster than UDFs
   
4. Skew Handling: WHEN detected (slow stages)
   ⏰ When: After seeing uneven partitions
   ❓ Why: Balance work across cluster
   
5. Caching: AFTER expensive ops, BEFORE reuse
   ⏰ When: Right before multiple uses
   ❓ Why: Compute once, reuse many times
   
6. Avoid collect(): AT output stage
   ⏰ When: Final results
   ❓ Why: Driver has limited memory

🚀 PERFORMANCE SUMMARY (Typical Improvements):
───────────────────────────────────────────────
• Explicit schema: 2-5× faster load
• Broadcast: 10-100× fewer model loads
• Spark SQL: 10-100× faster than UDFs
• Skew handling: 5-50× speedup on slow stages
• Caching: 3-10× faster for reused DFs
• No collect(): Prevents OOM crashes

Combined: 100-1000× overall speedup! 🎉
    """)
    
    spark.stop()


def main():
    """
    Main execution function.
    """
    print("\n" + "🔷 " * 40)
    print("PYSPARK OPTIMIZATION BEST PRACTICES - COMPREHENSIVE GUIDE")
    print("🔷 " * 40)
    
    # Run complete demonstration
    demonstrate_all_best_practices_together()
    
    print("\n" + "=" * 80)
    print("✅ OPTIMIZATION GUIDE COMPLETE")
    print("=" * 80)
    
    print("""
📚 Summary of All 6 Best Practices:

1. ✅ Explicit Schema
   • WHEN: At data load
   • WHY: Fast, type-safe, memory-efficient
   • HOW: Define StructType before reading

2. ✅ Broadcast Models
   • WHEN: Before map/filter operations
   • WHY: Load once per executor, not per row
   • HOW: spark.sparkContext.broadcast()

3. ✅ Avoid collect()
   • WHEN: At output stage
   • WHY: Driver has limited memory
   • HOW: Use show(), write(), or aggregate first

4. ✅ Handle Skew
   • WHEN: When joins/aggs are slow
   • WHY: Balance work across cluster
   • HOW: Salt keys, repartition, increase parallelism

5. ✅ Cache Strategically
   • WHEN: After expensive ops, before reuse
   • WHY: Compute once, reuse many times
   • HOW: .cache() or .persist()

6. ✅ Use Spark SQL
   • WHEN: For all transformations
   • WHY: 10-100× faster than UDFs
   • HOW: Use pyspark.sql.functions

🎯 Remember: TIMING is everything!
   Apply optimizations at the RIGHT stage in your workflow.

🔗 Related Files:
   • src/optimization/01_join_strategies.py
   • src/cluster_computing/02_data_partitioning.py
   • src/cluster_computing/04_aggregations_at_scale.py
    """)


if __name__ == "__main__":
    main()
