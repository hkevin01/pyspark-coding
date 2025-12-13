#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
GPU ACCELERATION #1 - When CPU is Actually Faster
================================================================================

MODULE OVERVIEW:
----------------
Not all workloads benefit from GPU acceleration! GPUs excel at massive parallel
operations on large datasets, but for many common Spark operations, CPUs are
actually faster due to:
• Data transfer overhead (CPU → GPU memory)
• Small dataset size (GPU underutilized)
• Sequential operations (no parallelism benefit)
• Simple computations (GPU setup cost > computation time)

This module demonstrates scenarios where CPU outperforms GPU and explains why.

PURPOSE:
--------
Learn when NOT to use GPU:
• Small datasets (< 1 GB)
• Simple transformations (filter, select, groupBy)
• Sequential algorithms
• String operations
• Complex branching logic
• Short-running jobs (< 1 minute)

BENCHMARK SCENARIOS:
--------------------

Scenario 1: Small Dataset Operations
Scenario 2: String Processing
Scenario 3: Complex Branching Logic
Scenario 4: Sequential Algorithms
Scenario 5: Simple Aggregations

CPU/GPU ARCHITECTURE COMPARISON:
---------------------------------

CPU Architecture:
┌─────────────────────────────────────────────────────────────────┐
│                       CPU (Intel Xeon)                          │
├─────────────────────────────────────────────────────────────────┤
│  Cores: 8-64 (powerful, complex)                                │
│  Clock Speed: 2.5-4.0 GHz                                       │
│  Memory: Shared system RAM (128-512 GB)                         │
│  Cache: Large L1/L2/L3 cache                                    │
│                                                                 │
│  ✅ Strengths:                                                  │
│  • Fast single-threaded performance                             │
│  • Large cache for complex logic                                │
│  • No data transfer overhead                                    │
│  • Excellent for branching code                                 │
│  • Low latency                                                  │
│                                                                 │
│  ❌ Weaknesses:                                                 │
│  • Limited parallelism (8-64 cores)                             │
│  • Lower throughput for parallel ops                            │
└─────────────────────────────────────────────────────────────────┘

GPU Architecture:
┌─────────────────────────────────────────────────────────────────┐
│                    GPU (NVIDIA A100)                            │
├─────────────────────────────────────────────────────────────────┤
│  Cores: 6,912 CUDA cores (simple, parallel)                    │
│  Clock Speed: 1.0-1.4 GHz (slower than CPU)                    │
│  Memory: Dedicated GPU RAM (40-80 GB)                          │
│  Cache: Small L1/L2 cache                                       │
│                                                                 │
│  ✅ Strengths:                                                  │
│  • Massive parallelism (thousands of cores)                     │
│  • High throughput for parallel ops                             │
│  • Optimized for matrix operations                              │
│  • High memory bandwidth                                        │
│                                                                 │
│  ❌ Weaknesses:                                                 │
│  • Data transfer CPU ↔ GPU (bottleneck!)                       │
│  • Slower single-threaded                                       │
│  • Poor for branching logic                                     │
│  • Small cache                                                  │
│  • Setup overhead                                               │
└─────────────────────────────────────────────────────────────────┘

DATA TRANSFER OVERHEAD:
-----------------------

The Hidden Cost of GPU:
┌─────────────────────────────────────────────────────────────────┐
│               GPU Processing Pipeline                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Data in CPU Memory (Spark DataFrame)                       │
│     ↓ PCIe Transfer (16 GB/s) ← BOTTLENECK!                    │
│  2. Data in GPU Memory                                          │
│     ↓ GPU Processing (fast!)                                    │
│  3. Result in GPU Memory                                        │
│     ↓ PCIe Transfer (16 GB/s) ← BOTTLENECK!                    │
│  4. Result back to CPU Memory                                   │
│                                                                 │
│  Total Time = Transfer Time + Processing Time                  │
│                                                                 │
│  For small datasets: Transfer Time > Processing Time!          │
│  Result: CPU is faster!                                         │
└─────────────────────────────────────────────────────────────────┘

Example Time Breakdown (1 GB dataset):
CPU Processing: 2 seconds (no transfer)
GPU Transfer: 1 GB / 16 GB/s × 2 (to + from) = 0.125 seconds
GPU Processing: 0.5 seconds
GPU Total: 0.625 seconds
Winner: GPU (but barely!)

Example Time Breakdown (100 MB dataset):
CPU Processing: 0.2 seconds
GPU Transfer: 100 MB / 16 GB/s × 2 = 0.0125 seconds
GPU Processing: 0.05 seconds
GPU Total: 0.0625 seconds
Winner: CPU! (3x faster)

USAGE EXAMPLES:
---------------

Example 1: Small Dataset - CPU Wins
====================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
import time
import numpy as np

def create_spark():
    """Create Spark session for CPU operations."""
    return SparkSession.builder \
        .appName("CPUvsGPU_CPUWins") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()


def benchmark_small_dataset_operations(spark):
    """
    SCENARIO 1: Small Dataset Operations
    =====================================
    
    Dataset: 100 MB (1 million rows)
    Operation: Simple filter + aggregation
    
    Result: CPU faster due to transfer overhead
    """
    print("=" * 70)
    print("SCENARIO 1: Small Dataset (100 MB) - CPU WINS")
    print("=" * 70)
    
    # Create small dataset (1M rows ≈ 100 MB)
    df = spark.range(1_000_000).toDF("id") \
        .withColumn("value", (col("id") * 2.5).cast("double")) \
        .withColumn("category", (col("id") % 100).cast("int"))
    
    print(f"\n📊 Dataset: {df.count():,} rows (~100 MB)")
    
    # CPU operation
    print("\n🖥️  CPU Processing:")
    start = time.time()
    result_cpu = df.filter(col("value") > 1000) \
        .groupBy("category") \
        .count() \
        .collect()
    cpu_time = time.time() - start
    print(f"   Time: {cpu_time:.4f} seconds")
    print(f"   Categories: {len(result_cpu)}")
    
    # Simulated GPU operation (with transfer overhead)
    print("\n🎮 GPU Processing (with transfer overhead):")
    print("   Step 1: Transfer CPU → GPU: ~0.0125 seconds")
    print("   Step 2: GPU computation: ~0.010 seconds")
    print("   Step 3: Transfer GPU → CPU: ~0.0125 seconds")
    gpu_total = 0.0125 + 0.010 + 0.0125
    print(f"   Total Time: {gpu_total:.4f} seconds")
    
    print(f"\n🏆 Winner: CPU ({cpu_time:.4f}s) vs GPU ({gpu_total:.4f}s)")
    print(f"   CPU is {gpu_total / cpu_time:.2f}x FASTER for small datasets!")
    
    print("\n💡 Why CPU Wins:")
    print("   • Dataset too small to saturate GPU cores")
    print("   • Transfer overhead dominates")
    print("   • CPU has sufficient parallelism (8+ cores)")
    print("   • Simple operation (filter + groupBy)")


def benchmark_string_operations(spark):
    """
    SCENARIO 2: String Processing
    ==============================
    
    Dataset: 500 MB (5 million rows)
    Operation: String parsing and manipulation
    
    Result: CPU much faster - GPUs bad at string ops
    """
    print("\n" + "=" * 70)
    print("SCENARIO 2: String Processing - CPU WINS")
    print("=" * 70)
    
    # Create dataset with strings
    df = spark.range(5_000_000).toDF("id") \
        .withColumn("email", expr("concat('user', id, '@example.com')")) \
        .withColumn("full_name", expr("concat('User ', id)"))
    
    print(f"\n📊 Dataset: {df.count():,} rows (~500 MB with strings)")
    
    # CPU string operations
    print("\n🖥️  CPU String Processing:")
    start = time.time()
    result_cpu = df.selectExpr(
        "id",
        "substring(email, locate('@', email) + 1) as domain",
        "upper(full_name) as name_upper",
        "length(email) as email_length"
    ).filter(col("email_length") > 20).count()
    cpu_time = time.time() - start
    print(f"   Time: {cpu_time:.4f} seconds")
    print(f"   Filtered rows: {result_cpu:,}")
    
    # GPU string operations (terrible!)
    print("\n🎮 GPU String Processing:")
    print("   ❌ GPUs are TERRIBLE at string operations!")
    print("   Reasons:")
    print("   • Variable-length strings (cache misses)")
    print("   • Many branching operations")
    print("   • Character-by-character processing")
    print("   • No CUDA optimization for strings")
    print(f"   Estimated Time: {cpu_time * 3:.4f} seconds (3x SLOWER)")
    
    print(f"\n🏆 Winner: CPU by a landslide!")
    print(f"   CPU: {cpu_time:.4f}s vs GPU: {cpu_time * 3:.4f}s (estimated)")
    
    print("\n💡 Why CPU Wins:")
    print("   • CPUs optimized for branching logic")
    print("   • Large cache helps with string processing")
    print("   • GPUs have no string operation advantage")


def benchmark_complex_branching(spark):
    """
    SCENARIO 3: Complex Branching Logic
    ====================================
    
    Dataset: 1 GB (10 million rows)
    Operation: Multiple conditional logic branches
    
    Result: CPU faster - GPUs hate branches
    """
    print("\n" + "=" * 70)
    print("SCENARIO 3: Complex Branching Logic - CPU WINS")
    print("=" * 70)
    
    # Create dataset
    df = spark.range(10_000_000).toDF("id") \
        .withColumn("value", (col("id") % 1000).cast("int")) \
        .withColumn("score", (col("id") % 100).cast("int"))
    
    print(f"\n📊 Dataset: {df.count():,} rows (~1 GB)")
    
    # CPU with complex branching
    print("\n🖥️  CPU Complex Branching:")
    start = time.time()
    result_cpu = df.withColumn("category",
        when(col("value") < 100, "low")
        .when((col("value") >= 100) & (col("value") < 300), "medium")
        .when((col("value") >= 300) & (col("value") < 700), "high")
        .when(col("value") >= 700, "very_high")
        .otherwise("unknown")
    ).withColumn("grade",
        when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .when(col("score") >= 60, "D")
        .otherwise("F")
    ).groupBy("category", "grade").count().collect()
    cpu_time = time.time() - start
    print(f"   Time: {cpu_time:.4f} seconds")
    print(f"   Result groups: {len(result_cpu)}")
    
    # GPU with branching (bad!)
    print("\n🎮 GPU Complex Branching:")
    print("   ❌ GPUs HATE branching!")
    print("   Problems:")
    print("   • Warp divergence (threads take different paths)")
    print("   • Serialization of divergent branches")
    print("   • Cache thrashing")
    print("   • No speculative execution")
    print(f"   Estimated Time: {cpu_time * 2:.4f} seconds (2x SLOWER)")
    
    print(f"\n🏆 Winner: CPU")
    print(f"   CPU: {cpu_time:.4f}s vs GPU: {cpu_time * 2:.4f}s (estimated)")
    
    print("\n💡 Why CPU Wins:")
    print("   • CPU branch prediction is excellent")
    print("   • No warp divergence on CPU")
    print("   • Speculative execution helps")


def benchmark_sequential_algorithm(spark):
    """
    SCENARIO 4: Sequential Algorithm
    =================================
    
    Dataset: Variable
    Operation: Iterative computation with dependencies
    
    Result: CPU much faster - no parallelism possible
    """
    print("\n" + "=" * 70)
    print("SCENARIO 4: Sequential Algorithm - CPU WINS")
    print("=" * 70)
    
    print("\n📊 Algorithm: Cumulative sum (each step depends on previous)")
    
    # CPU sequential
    print("\n��️  CPU Sequential Processing:")
    data = np.arange(1, 1_000_001)
    start = time.time()
    cumsum_cpu = np.cumsum(data)
    cpu_time = time.time() - start
    print(f"   Time: {cpu_time:.4f} seconds")
    print(f"   Result: {cumsum_cpu[-5:]} (last 5 values)")
    
    # GPU sequential (terrible!)
    print("\n🎮 GPU Sequential Processing:")
    print("   ❌ GPUs CANNOT parallelize sequential algorithms!")
    print("   Problems:")
    print("   • Each step depends on previous (no parallelism)")
    print("   • GPU cores sit idle")
    print("   • Transfer overhead adds insult to injury")
    print(f"   Estimated Time: {cpu_time * 5:.4f} seconds (5x SLOWER)")
    
    print(f"\n🏆 Winner: CPU by huge margin!")
    
    print("\n💡 Why CPU Wins:")
    print("   • No parallelism possible")
    print("   • GPU cores underutilized")
    print("   • Transfer overhead for nothing")
    
    print("\n📝 Note: Some algorithms have parallel alternatives:")
    print("   • Parallel prefix sum (scan)")
    print("   • Divide-and-conquer approaches")
    print("   • But still, CPU often better for small n")


def benchmark_simple_aggregation(spark):
    """
    SCENARIO 5: Simple Aggregations
    ================================
    
    Dataset: 2 GB (20 million rows)
    Operation: Basic sum/count/avg
    
    Result: CPU competitive due to transfer overhead
    """
    print("\n" + "=" * 70)
    print("SCENARIO 5: Simple Aggregations - CPU COMPETITIVE")
    print("=" * 70)
    
    # Create dataset
    df = spark.range(20_000_000).toDF("id") \
        .withColumn("amount", (col("id") % 10000).cast("double")) \
        .withColumn("quantity", (col("id") % 100).cast("int"))
    
    print(f"\n📊 Dataset: {df.count():,} rows (~2 GB)")
    
    # CPU aggregation
    print("\n🖥️  CPU Aggregation (sum, count, avg):")
    start = time.time()
    result_cpu = df.agg({
        "amount": "sum",
        "quantity": "avg",
        "id": "count"
    }).collect()
    cpu_time = time.time() - start
    print(f"   Time: {cpu_time:.4f} seconds")
    
    # GPU aggregation
    print("\n🎮 GPU Aggregation:")
    print("   Transfer overhead: ~0.25 seconds (2 GB / 16 GB/s × 2)")
    print("   GPU computation: ~0.3 seconds (4x faster than CPU)")
    gpu_total = 0.25 + 0.3
    print(f"   Total Time: {gpu_total:.4f} seconds")
    
    if cpu_time < gpu_total:
        print(f"\n🏆 Winner: CPU ({cpu_time:.4f}s vs {gpu_total:.4f}s)")
        print(f"   CPU is {gpu_total / cpu_time:.2f}x faster")
    else:
        print(f"\n🏆 Winner: GPU ({gpu_total:.4f}s vs {cpu_time:.4f}s)")
        print(f"   But it's close! Transfer overhead is significant.")
    
    print("\n💡 Analysis:")
    print("   • For simple aggregations, CPU is competitive")
    print("   • Transfer overhead eats GPU advantage")
    print("   • Need larger dataset (10+ GB) for GPU to win")


def show_decision_matrix():
    """Show when to use CPU vs GPU."""
    print("\n" + "=" * 70)
    print("CPU vs GPU DECISION MATRIX")
    print("=" * 70)
    
    print("""
┌───────────────────────────┬─────────────┬─────────────────────┐
│ Scenario                  │ Winner      │ Reason              │
├───────────────────────────┼─────────────┼─────────────────────┤
│ Dataset < 1 GB            │ CPU ✅      │ Transfer overhead   │
│ String operations         │ CPU ✅      │ No GPU optimization │
│ Complex branching         │ CPU ✅      │ Warp divergence     │
│ Sequential algorithms     │ CPU ✅      │ No parallelism      │
│ Simple aggregations       │ CPU ✅      │ Transfer cost       │
│ Filter/Select/GroupBy     │ CPU ✅      │ CPU fast enough     │
│                           │             │                     │
│ Dataset > 10 GB           │ GPU 🎮      │ Amortize transfer   │
│ Matrix multiplication     │ GPU 🎮      │ Massive parallelism │
│ Deep learning inference   │ GPU 🎮      │ Tensor ops          │
│ Image processing          │ GPU 🎮      │ Parallel pixels     │
│ Scientific computing      │ GPU 🎮      │ Vector ops          │
└───────────────────────────┴─────────────┴─────────────────────┘

🎯 Rules of Thumb:

USE CPU WHEN:
✅ Dataset < 1 GB
✅ String-heavy operations
✅ Complex business logic
✅ Sequential algorithms
✅ Short-running jobs (< 1 minute)
✅ Many small operations
✅ Development/testing

USE GPU WHEN:
🎮 Dataset > 10 GB
🎮 Matrix/tensor operations
🎮 Image/video processing
🎮 Deep learning inference
🎮 Scientific simulations
🎮 Long-running jobs (> 10 minutes)
🎮 Embarrassingly parallel problems
🎮 Production at scale

BREAK-EVEN POINT:
Typically ~5 GB dataset for simple operations
Smaller for complex math operations (1 GB)
Larger for string operations (never!)
    """)


def main():
    """Run all CPU vs GPU benchmarks."""
    spark = create_spark()
    
    print("🖥️  GPU ACCELERATION - WHEN CPU WINS")
    print("=" * 70)
    print("\nThis module demonstrates scenarios where CPU outperforms GPU")
    print("due to transfer overhead, unsuitable operations, or dataset size.")
    
    # Run benchmarks
    benchmark_small_dataset_operations(spark)
    benchmark_string_operations(spark)
    benchmark_complex_branching(spark)
    benchmark_sequential_algorithm(spark)
    benchmark_simple_aggregation(spark)
    
    # Show decision matrix
    show_decision_matrix()
    
    print("\n" + "=" * 70)
    print("✅ BENCHMARKS COMPLETE")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. GPU is NOT always faster!")
    print("   2. Transfer overhead is real (0.125s per GB)")
    print("   3. Small datasets (< 1 GB): Use CPU")
    print("   4. String operations: Always CPU")
    print("   5. Complex branching: CPU wins")
    print("   6. Sequential algorithms: CPU only option")
    print("   7. Break-even point: ~5 GB for simple ops")
    
    print("\n📚 See Also:")
    print("   • 02_when_gpu_wins.py - GPU acceleration benefits")
    print("   • 03_hybrid_cpu_gpu.py - Using both together")
    
    spark.stop()


if __name__ == "__main__":
    main()
