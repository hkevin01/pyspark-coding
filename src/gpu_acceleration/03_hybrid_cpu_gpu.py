#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
GPU ACCELERATION #3 - Hybrid CPU + GPU Architecture
================================================================================

MODULE OVERVIEW:
----------------
The best performance often comes from intelligently combining CPU and GPU:
• CPU handles preprocessing (strings, logic, IO)
• GPU handles heavy computation (matrix ops, ML)
• CPU handles postprocessing (aggregation, formatting)

This "best of both worlds" approach maximizes throughput while minimizing
transfer overhead.

PURPOSE:
--------
Learn hybrid CPU/GPU patterns:
• Pipeline: CPU → GPU → CPU
• Load balancing between processors
• Minimizing data transfer
• Cost optimization strategies
• When to use each processor

ARCHITECTURE PATTERNS:
----------------------
Pattern 1: CPU Preprocessing → GPU Compute → CPU Postprocessing
Pattern 2: Parallel CPU + GPU (different operations)
Pattern 3: Dynamic load balancing
Pattern 4: Batch processing optimization
Pattern 5: Cost-aware routing

HYBRID PIPELINE ARCHITECTURE:
------------------------------

┌─────────────────────────────────────────────────────────────────┐
│                    HYBRID CPU/GPU PIPELINE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Stage 1: CPU PREPROCESSING (Fast on CPU)                      │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • Load data from storage (IO-bound)                │        │
│  │ • Parse strings, JSON, XML                         │        │
│  │ • Data validation and filtering                    │        │
│  │ • Feature extraction (text → numbers)              │        │
│  │ • Outlier removal                                  │        │
│  └────────────────────────────────────────────────────┘        │
│                         ↓                                       │
│                 Batch for GPU Transfer                          │
│                 (Minimize transfers!)                           │
│                         ↓                                       │
│  Stage 2: GPU COMPUTATION (Fast on GPU)                        │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • Matrix multiplication                            │        │
│  │ • Deep learning inference                          │        │
│  │ • Complex math transformations                     │        │
│  │ • Image/signal processing                          │        │
│  │ • Scientific simulations                           │        │
│  └────────────────────────────────────────────────────┘        │
│                         ↓                                       │
│                 Results back to CPU                             │
│                         ↓                                       │
│  Stage 3: CPU POSTPROCESSING (Fast on CPU)                     │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • Aggregations (sum, count, avg)                   │        │
│  │ • Format results (numbers → strings)               │        │
│  │ • Write to storage (IO-bound)                      │        │
│  │ • Generate reports                                 │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

KEY PRINCIPLE: Minimize CPU ↔ GPU transfers!
Each transfer costs ~0.125s per GB (PCIe bottleneck)
Solution: Batch operations, do ALL GPU work in one transfer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, udf
from pyspark.sql.types import StringType, DoubleType, ArrayType
import pandas as pd
import numpy as np
import time


def create_spark():
    """Create Spark session optimized for hybrid CPU/GPU."""
    return SparkSession.builder \
        .appName("Hybrid_CPU_GPU") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "100000") \
        .getOrCreate()


def pattern1_preprocessing_pipeline(spark):
    """
    PATTERN 1: CPU Preprocessing → GPU Compute → CPU Postprocessing
    ==============================================================
    
    Use Case: ML inference on text data
    Pipeline: Parse text (CPU) → Model inference (GPU) → Format results (CPU)
    """
    print("=" * 70)
    print("PATTERN 1: Full Pipeline (CPU → GPU → CPU)")
    print("=" * 70)
    
    # Create sample dataset
    df = spark.range(1_000_000).toDF("id") \
        .withColumn("text", expr("concat('Sample text ', id, ' for processing')"))
    
    print(f"\n📊 Dataset: {df.count():,} rows of text data")
    
    # Stage 1: CPU Preprocessing (string operations)
    print("\n🖥️  STAGE 1: CPU Preprocessing")
    print("   Task: Parse text, extract features, validate")
    
    @pandas_udf("array<double>")
    def cpu_preprocess_udf(texts: pd.Series) -> pd.Series:
        """CPU-optimized text preprocessing."""
        start = time.time()
        
        results = []
        for text in texts:
            # String operations (BAD on GPU, GOOD on CPU)
            words = str(text).split()
            features = [
                len(words),                    # word count
                len(str(text)),                # char count
                sum(len(w) for w in words),    # total word length
                max((len(w) for w in words), default=0),  # max word len
            ]
            results.append(features)
        
        elapsed = time.time() - start
        print(f"   CPU preprocessing: {elapsed:.4f}s for {len(texts)} rows")
        return pd.Series(results)
    
    df_preprocessed = df.withColumn("features", cpu_preprocess_udf(col("text")))
    
    # Stage 2: GPU Computation (heavy math)
    print("\n🎮 STAGE 2: GPU Computation")
    print("   Task: Complex transformations on features")
    
    @pandas_udf("array<double>")
    def gpu_compute_udf(features: pd.Series) -> pd.Series:
        """GPU-optimized mathematical transformations."""
        start = time.time()
        
        # Simulate GPU operations (in real code, use cupy)
        # import cupy as cp
        # gpu_features = cp.asarray(features.tolist())
        # result = cp.exp(gpu_features) * cp.sin(gpu_features)
        # return pd.Series(cp.asnumpy(result).tolist())
        
        # CPU simulation
        np_features = np.array(features.tolist())
        result = np.exp(np_features) * np.sin(np_features)
        
        elapsed = time.time() - start
        print(f"   GPU computation: {elapsed:.4f}s for {len(features)} rows")
        return pd.Series(result.tolist())
    
    df_computed = df_preprocessed.withColumn(
        "transformed", gpu_compute_udf(col("features"))
    )
    
    # Stage 3: CPU Postprocessing (aggregation)
    print("\n🖥️  STAGE 3: CPU Postprocessing")
    print("   Task: Aggregate results, format output")
    
    result = df_computed.selectExpr(
        "AVG(transformed[0]) as avg_val",
        "MAX(transformed[1]) as max_val",
        "COUNT(*) as total_count"
    ).collect()[0]
    
    print(f"   Results: avg={result['avg_val']:.4f}, "
          f"max={result['max_val']:.4f}, count={result['total_count']:,}")
    
    print("\n💡 Hybrid Benefits:")
    print("   ✅ CPU handles strings efficiently")
    print("   ✅ GPU handles heavy math efficiently")
    print("   ✅ Minimize transfers (batch processing)")
    print("   ✅ Each processor does what it's best at")


def pattern2_parallel_processing(spark):
    """
    PATTERN 2: Parallel CPU + GPU (Different Operations)
    ====================================================
    
    Use Case: Process different data types simultaneously
    Strategy: CPU processes strings while GPU processes numbers
    """
    print("\n" + "=" * 70)
    print("PATTERN 2: Parallel CPU + GPU Processing")
    print("=" * 70)
    
    df = spark.range(5_000_000).toDF("id") \
        .withColumn("text_data", expr("concat('Text ', id)")) \
        .withColumn("numeric_data", (col("id") * 2.5).cast("double"))
    
    print(f"\n📊 Dataset: {df.count():,} rows (text + numeric)")
    
    print("\n🔀 PARALLEL PROCESSING:")
    print("   CPU Task: String operations on text_data")
    print("   GPU Task: Math operations on numeric_data")
    print("   ⚡ Both run simultaneously!")
    
    # CPU path: String operations
    @pandas_udf(StringType())
    def cpu_string_ops(texts: pd.Series) -> pd.Series:
        return texts.str.upper() + "_PROCESSED"
    
    # GPU path: Numeric operations
    @pandas_udf(DoubleType())
    def gpu_numeric_ops(values: pd.Series) -> pd.Series:
        # Simulate GPU math (use cupy in real code)
        return values.apply(lambda x: np.sin(x) * np.exp(x))
    
    # Apply both transformations
    df_result = df.withColumn("text_result", cpu_string_ops(col("text_data"))) \
                  .withColumn("numeric_result", gpu_numeric_ops(col("numeric_data")))
    
    sample = df_result.limit(3).collect()
    print("\n📝 Sample Results:")
    for row in sample:
        print(f"   ID {row['id']}: text='{row['text_result']}', "
              f"numeric={row['numeric_result']:.4f}")
    
    print("\n💡 Parallel Benefits:")
    print("   ✅ CPU and GPU work simultaneously")
    print("   ✅ No idle processors")
    print("   ✅ Maximum resource utilization")
    print("   ✅ Overall throughput increased")


def pattern3_dynamic_routing(spark):
    """
    PATTERN 3: Dynamic Load Balancing (Route by Data Size)
    ======================================================
    
    Use Case: Route small batches to CPU, large batches to GPU
    Strategy: Decision based on data size and operation complexity
    """
    print("\n" + "=" * 70)
    print("PATTERN 3: Dynamic CPU/GPU Routing")
    print("=" * 70)
    
    print("\n🔀 ROUTING LOGIC:")
    print("""
    ┌──────────────────────┬─────────────┬──────────────────┐
    │ Data Size            │ Processor   │ Reason           │
    ├──────────────────────┼─────────────┼──────────────────┤
    │ < 1 GB               │ CPU         │ Transfer overhead│
    │ 1-10 GB              │ Dynamic     │ Check complexity │
    │ > 10 GB              │ GPU         │ Amortize transfer│
    │ String operations    │ CPU always  │ GPU bad at text  │
    │ Matrix ops           │ GPU always  │ GPU optimized    │
    └──────────────────────┴─────────────┴──────────────────┘
    """)
    
    def route_computation(data_size_gb, operation_type):
        """Decide CPU or GPU based on data and operation."""
        
        # Rule 1: Strings always go to CPU
        if operation_type == "string":
            return "CPU", "Strings bad on GPU"
        
        # Rule 2: Small data goes to CPU
        if data_size_gb < 1:
            return "CPU", "Transfer overhead dominates"
        
        # Rule 3: Large data goes to GPU
        if data_size_gb > 10:
            return "GPU", "Amortize transfer cost"
        
        # Rule 4: Medium data - check operation
        if operation_type in ["matrix", "ml_inference", "image"]:
            return "GPU", "GPU-optimized operation"
        else:
            return "CPU", "Simple operation"
    
    # Test scenarios
    scenarios = [
        (0.5, "aggregation", "Small aggregation"),
        (2.0, "string", "Medium string processing"),
        (5.0, "matrix", "Medium matrix multiplication"),
        (15.0, "ml_inference", "Large ML inference"),
        (0.8, "filter", "Small filter operation"),
        (50.0, "image", "Large image processing"),
    ]
    
    print("\n📊 Routing Decisions:")
    for size, op_type, description in scenarios:
        processor, reason = route_computation(size, op_type)
        emoji = "🖥️ " if processor == "CPU" else "🎮"
        print(f"   {emoji} {description} ({size} GB, {op_type})")
        print(f"      → Route to {processor}: {reason}")
    
    print("\n💡 Dynamic Routing Benefits:")
    print("   ✅ Optimal processor for each workload")
    print("   ✅ Minimize costs (GPU more expensive)")
    print("   ✅ Maximize throughput")
    print("   ✅ Adapt to workload characteristics")


def pattern4_batch_optimization(spark):
    """
    PATTERN 4: Batch Processing to Minimize Transfers
    =================================================
    
    Use Case: Multiple operations on same data
    Strategy: Batch ALL GPU operations together
    """
    print("\n" + "=" * 70)
    print("PATTERN 4: Batch GPU Operations")
    print("=" * 70)
    
    print("\n❌ BAD: Multiple separate GPU calls")
    print("""
    CPU → GPU (transfer 10 GB)
    GPU: Operation 1
    GPU → CPU (transfer 10 GB)
    CPU → GPU (transfer 10 GB)  ← WASTEFUL!
    GPU: Operation 2
    GPU → CPU (transfer 10 GB)  ← WASTEFUL!
    
    Total transfers: 40 GB
    Transfer time: 40 GB / 16 GB/s = 2.5 seconds WASTED
    """)
    
    print("\n✅ GOOD: Batch all GPU operations")
    print("""
    CPU → GPU (transfer 10 GB)
    GPU: Operation 1
    GPU: Operation 2  ← No transfer!
    GPU: Operation 3  ← No transfer!
    GPU → CPU (transfer 10 GB)
    
    Total transfers: 20 GB
    Transfer time: 20 GB / 16 GB/s = 1.25 seconds
    Savings: 1.25 seconds (50% faster!)
    """)
    
    @pandas_udf("array<double>")
    def batched_gpu_operations(features: pd.Series) -> pd.Series:
        """Batch multiple GPU operations together."""
        # import cupy as cp
        
        # Transfer to GPU ONCE
        # gpu_data = cp.asarray(features.tolist())
        
        # Do ALL operations on GPU (no transfers!)
        # result1 = cp.exp(gpu_data)
        # result2 = cp.sin(result1)
        # result3 = result2 * cp.log(gpu_data + 1)
        # final = cp.sqrt(result3)
        
        # Transfer back ONCE
        # return pd.Series(cp.asnumpy(final).tolist())
        
        # CPU simulation
        np_data = np.array(features.tolist())
        result1 = np.exp(np_data)
        result2 = np.sin(result1)
        result3 = result2 * np.log(np_data + 1)
        final = np.sqrt(np.abs(result3))
        return pd.Series(final.tolist())
    
    print("\n💡 Batching Benefits:")
    print("   ✅ Minimize transfers (biggest bottleneck)")
    print("   ✅ 50-200% speedup possible")
    print("   ✅ GPU stays busy (no idle time)")
    print("   ✅ Plan your pipeline carefully!")


def pattern5_cost_optimization(spark):
    """
    PATTERN 5: Cost-Aware Routing
    ==============================
    
    Use Case: Balance performance vs cost
    Strategy: Use GPU only when ROI justifies cost
    """
    print("\n" + "=" * 70)
    print("PATTERN 5: Cost-Aware CPU/GPU Routing")
    print("=" * 70)
    
    print("\n💰 COST ANALYSIS:")
    print("""
    AWS Pricing (per hour):
    • CPU (r5.8xlarge):  $2.02  (32 vCPUs, 256 GB)
    • GPU (p3.2xlarge):  $3.06  (1× V100, 8 vCPUs, 61 GB)
    • GPU (p3.8xlarge):  $12.24 (4× V100, 32 vCPUs, 244 GB)
    
    Cost Ratio: GPU is 1.5-6x more expensive
    
    Decision Rule:
    Use GPU only if: Speedup > (GPU_cost / CPU_cost)
    
    Examples:
    • p3.2xlarge: Need > 1.5x speedup to break even
    • p3.8xlarge: Need > 6x speedup to break even
    """)
    
    def cost_aware_routing(data_size_gb, estimated_speedup, gpu_type="p3.2xlarge"):
        """Route based on cost-benefit analysis."""
        
        costs = {
            "p3.2xlarge": 3.06,
            "p3.8xlarge": 12.24,
            "g5.xlarge": 1.01,
        }
        cpu_cost = 2.02
        
        gpu_cost = costs[gpu_type]
        cost_ratio = gpu_cost / cpu_cost
        
        if estimated_speedup > cost_ratio:
            roi = ((estimated_speedup - cost_ratio) / cost_ratio) * 100
            return "GPU", f"{roi:.1f}% ROI"
        else:
            return "CPU", "Not cost-effective"
    
    # Test scenarios
    scenarios = [
        (5.0, 2.0, "p3.2xlarge", "Small ML inference"),
        (50.0, 30.0, "p3.2xlarge", "Large ML inference"),
        (100.0, 10.0, "p3.8xlarge", "Huge matrix ops"),
        (10.0, 3.0, "g5.xlarge", "Image processing"),
    ]
    
    print("\n📊 Cost-Aware Decisions:")
    for size, speedup, gpu_type, description in scenarios:
        processor, reason = cost_aware_routing(size, speedup, gpu_type)
        emoji = "��️ " if processor == "CPU" else "🎮"
        print(f"\n   {description}")
        print(f"   • Data: {size} GB")
        print(f"   • Speedup: {speedup}x")
        print(f"   • GPU Type: {gpu_type}")
        print(f"   {emoji} Decision: {processor} ({reason})")
    
    print("\n💡 Cost Optimization Benefits:")
    print("   ✅ Balance performance vs cost")
    print("   ✅ Avoid overspending on GPU")
    print("   ✅ Use spot instances for 70% savings")
    print("   ✅ Consider g5.xlarge for better ROI")


def show_hybrid_best_practices():
    """Show best practices for hybrid CPU/GPU."""
    print("\n" + "=" * 70)
    print("HYBRID CPU/GPU BEST PRACTICES")
    print("=" * 70)
    
    print("""
🎯 GOLDEN RULES:

1. Minimize Transfers
   • Transfer cost: ~0.125s per GB
   • Batch operations: Do ALL GPU work in one transfer
   • Pipeline: CPU → GPU (many ops) → CPU

2. Right Tool for the Job
   • CPU: Strings, branching, IO, small data
   • GPU: Matrix ops, ML, images, large data

3. Cost-Aware Decisions
   • GPU 1.5-6x more expensive
   • Need speedup > cost ratio
   • Use spot instances (70% cheaper)

4. Dynamic Routing
   • < 1 GB: CPU
   • 1-10 GB: Check operation type
   • > 10 GB: GPU (if parallelizable)

5. Monitor and Profile
   • GPU utilization > 80% (target)
   • If < 50%: Transfer overhead dominates
   • Use nvidia-smi to monitor

📋 IMPLEMENTATION CHECKLIST:

✅ Profile your workload
   • Measure CPU time
   • Estimate GPU speedup
   • Calculate transfer overhead

✅ Design pipeline
   • CPU preprocessing (strings, validation)
   • GPU computation (math, ML)
   • CPU postprocessing (aggregation, formatting)

✅ Batch GPU operations
   • Minimize CPU ↔ GPU transfers
   • Do ALL GPU work in one batch

✅ Cost analysis
   • Calculate break-even speedup
   • Consider spot instances
   • Monitor actual costs

✅ Monitor performance
   • GPU utilization (nvidia-smi)
   • Transfer time vs compute time
   • Overall throughput

🔧 CODE TEMPLATE:

from pyspark.sql.functions import pandas_udf
import cupy as cp

@pandas_udf("array<double>")
def hybrid_pipeline_udf(batch: pd.Series) -> pd.Series:
    # Stage 1: CPU preprocessing
    cpu_data = batch.apply(lambda x: preprocess(x))
    
    # Stage 2: GPU computation (batch ALL operations!)
    gpu_array = cp.asarray(cpu_data.tolist())
    
    # Do ALL GPU operations here (minimize transfers!)
    result1 = cp.exp(gpu_array)
    result2 = cp.sin(result1)
    result3 = result2 * cp.log(gpu_array + 1)
    final_gpu = cp.sqrt(result3)
    
    # Stage 3: Transfer back and CPU postprocess
    final_cpu = cp.asnumpy(final_gpu)
    return pd.Series(postprocess(final_cpu).tolist())

# Apply to DataFrame
df_result = df.withColumn("result", hybrid_pipeline_udf(col("features")))
    """)


def main():
    """Run all hybrid CPU/GPU patterns."""
    spark = create_spark()
    
    print("🔀 HYBRID CPU + GPU ARCHITECTURE")
    print("=" * 70)
    print("\nBest of both worlds: CPU + GPU working together!")
    
    # Run patterns
    pattern1_preprocessing_pipeline(spark)
    pattern2_parallel_processing(spark)
    pattern3_dynamic_routing(spark)
    pattern4_batch_optimization(spark)
    pattern5_cost_optimization(spark)
    
    # Show best practices
    show_hybrid_best_practices()
    
    print("\n" + "=" * 70)
    print("✅ HYBRID PATTERNS COMPLETE")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Pipeline: CPU → GPU → CPU")
    print("   2. Minimize transfers (batch operations)")
    print("   3. Right tool for job (strings=CPU, math=GPU)")
    print("   4. Dynamic routing by size and operation")
    print("   5. Cost-aware decisions (ROI analysis)")
    print("   6. Monitor GPU utilization (target > 80%)")
    
    print("\n�� See Also:")
    print("   • 01_when_cpu_wins.py - CPU advantages")
    print("   • 02_when_gpu_wins.py - GPU advantages")
    print("   • 04_rapids_cudf_example.py - GPU DataFrames")
    
    spark.stop()


if __name__ == "__main__":
    # Add missing import
    from pyspark.sql.functions import expr
    main()
