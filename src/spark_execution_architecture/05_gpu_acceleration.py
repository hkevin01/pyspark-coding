#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
GPU ACCELERATION IN PYSPARK - Complete Guide
================================================================================

MODULE OVERVIEW:
----------------
This module demonstrates how to leverage GPU acceleration in PySpark for
massive performance improvements on data-intensive and compute-intensive
workloads. Learn how to use RAPIDS, PyTorch, and GPU-accelerated UDFs to
process data 10-100x faster than CPU-only approaches.

GPUs excel at:
• Matrix operations (ML training, linear algebra)
• Parallel data transformations (apply same operation to millions of rows)
• Deep learning inference
• Image/video processing
• Time series analysis

PURPOSE:
--------
Spark traditionally runs on CPUs across a cluster. However, modern GPUs can
process data MUCH faster for certain workloads. This module shows:

1. Why use GPUs with Spark?
2. GPU architecture basics (CUDA cores, memory, bandwidth)
3. RAPIDS integration (GPU-accelerated DataFrames)
4. PyTorch integration (DL inference on Spark)
5. GPU-accelerated UDFs (custom CUDA kernels)
6. Configuration and cluster setup
7. Performance comparison (CPU vs GPU)
8. Best practices and limitations

TARGET AUDIENCE:
----------------
• Data scientists doing ML/DL on large datasets
• Engineers working with image/video/time series data
• Teams experiencing slow DataFrame operations
• Anyone with GPU-enabled clusters (cloud or on-prem)

================================================================================
WHY GPUs FOR BIG DATA?
================================================================================

CPU vs GPU Architecture:

┌─────────────────────────────────────────────────────────────────┐
│                     CPU (Designed for Flexibility)              │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
│  │ Core 1  │  │ Core 2  │  │ Core 3  │  │ Core 4  │  4-64 cores│
│  │ Complex │  │ Complex │  │ Complex │  │ Complex │  per CPU   │
│  │ Logic   │  │ Logic   │  │ Logic   │  │ Logic   │            │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘           │
│                                                                  │
│  ┌──────────────────────────────────────┐                      │
│  │     Large Cache (MB per core)        │                      │
│  └──────────────────────────────────────┘                      │
│                                                                  │
│  Best for: Complex branching logic, variable workloads         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    GPU (Designed for Throughput)                │
├─────────────────────────────────────────────────────────────────┤
│  [Core][Core][Core][Core][Core][Core][Core][Core]              │
│  [Core][Core][Core][Core][Core][Core][Core][Core]              │
│  [Core][Core][Core][Core][Core][Core][Core][Core]              │
│  [Core][Core][Core][Core][Core][Core][Core][Core]              │
│  ...  (2000-10,000+ simple cores per GPU!)                      │
│                                                                  │
│  ┌──────────────────────────────────────┐                      │
│  │  High-bandwidth memory (100+ GB/s)   │                      │
│  └──────────────────────────────────────┘                      │
│                                                                  │
│  Best for: Same operation on millions of data elements         │
└─────────────────────────────────────────────────────────────────┘

PERFORMANCE EXAMPLE (Matrix Multiplication):
CPU (64 cores):   Process 1M rows/second
GPU (5120 cores): Process 50-100M rows/second  ⚡ 50-100x faster!

SPEEDUP BY OPERATION TYPE:
┌──────────────────────────────────────────────┐
│ Operation             │ CPU vs GPU Speedup   │
├──────────────────────────────────────────────┤
│ Matrix multiply       │ 50-100x              │
│ Element-wise ops      │ 10-50x               │
│ Joins                 │ 2-10x                │
│ String operations     │ 1-3x                 │
│ Complex if/else logic │ 1x (no benefit)      │
└──────────────────────────────────────────────┘

================================================================================
APPROACH 1: RAPIDS cuDF (GPU-Accelerated DataFrames)
================================================================================

RAPIDS is NVIDIA's GPU-accelerated data science library. It provides
cuDF - a pandas-like DataFrame API that runs entirely on GPU.

ARCHITECTURE:
┌──────────────────────────────────────────────────────────────┐
│  PySpark DataFrame API (your code)                           │
│         ↓                                                    │
│  Spark → Pandas DataFrame → Transfer to GPU                 │
│                                   ↓                          │
│                            cuDF DataFrame (on GPU)           │
│                                   ↓                          │
│                      GPU processes data (50x faster)         │
│                                   ↓                          │
│                      Transfer result back to Spark           │
└──────────────────────────────────────────────────────────────┘

INSTALLATION:
pip install cudf-cu11  # CUDA 11.x
pip install cuml       # GPU ML algorithms
pip install cugraph    # GPU graph analytics

EXAMPLE USE CASE:
• Feature engineering on 100M rows
• Aggregations across billions of records
• Complex joins on large tables
• Time series resampling

================================================================================
APPROACH 2: PyTorch Integration (Deep Learning Inference)
================================================================================

Run PyTorch models on Spark partitions using GPUs for massively parallel
inference.

ARCHITECTURE:
┌──────────────────────────────────────────────────────────────┐
│  Spark Cluster (1 driver + 10 executors)                    │
│                                                              │
│  Driver: Loads PyTorch model once                           │
│         ↓ Broadcast model to executors                      │
│                                                              │
│  Executor 1 (GPU):  │  Executor 2 (GPU):  │  Executor 3...  │
│  ┌──────────────┐  │  ┌──────────────┐  │                  │
│  │ PyTorch Model│  │  │ PyTorch Model│  │  Each executor   │
│  │ on GPU       │  │  │ on GPU       │  │  processes its   │
│  │              │  │  │              │  │  partitions      │
│  │ Process      │  │  │ Process      │  │  in parallel     │
│  │ Partition 0  │  │  │ Partition 1  │  │                  │
│  └──────────────┘  │  └──────────────┘  │                  │
│                                                              │
│  RESULT: 10 GPUs process 10 partitions simultaneously        │
│          = 10x throughput vs single GPU!                     │
└──────────────────────────────────────────────────────────────┘

USE CASES:
• Image classification (process 10M images)
• Text embeddings (BERT, GPT on millions of documents)
• Fraud detection (ML inference on transactions)
• Recommendation scoring

================================================================================
APPROACH 3: GPU-Accelerated UDFs (Custom CUDA Kernels)
================================================================================

Write custom GPU functions using Numba CUDA or CuPy and apply them to
Spark DataFrames.

HOW IT WORKS:
1. Write function using Numba @cuda.jit decorator
2. Function compiled to CUDA kernel
3. Spark sends data batches to GPU
4. GPU executes function on entire batch (parallel)
5. Results returned to Spark

EXAMPLE:
@cuda.jit
def gpu_square(input_array, output_array):
    idx = cuda.grid(1)
    if idx < input_array.size:
        output_array[idx] = input_array[idx] ** 2

This runs on 1000s of CUDA cores simultaneously!

================================================================================
CONFIGURATION FOR GPU CLUSTERS:
================================================================================

SPARK CONFIGURATION:

--conf spark.executor.resource.gpu.amount=1
  └─> Each executor gets 1 GPU

--conf spark.task.resource.gpu.amount=0.25
  └─> Each task uses 25% of GPU (4 tasks per GPU concurrently)

--conf spark.rapids.sql.enabled=true
  └─> Enable RAPIDS SQL plugin for GPU execution

--conf spark.rapids.memory.gpu.allocFraction=0.9
  └─> GPU can use up to 90% of its memory

EXAMPLE SPARK-SUBMIT:
spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --num-executors 10 \\
  --executor-cores 8 \\
  --executor-memory 32g \\
  --conf spark.executor.resource.gpu.amount=1 \\
  --conf spark.task.resource.gpu.amount=0.25 \\
  --conf spark.rapids.sql.enabled=true \\
  gpu_app.py

CLUSTER SETUP:
┌────────────────────────────────────────────────────────────┐
│  GPU-Enabled Cluster                                       │
│                                                            │
│  Node 1 (Driver):    CPU only (no GPU needed)             │
│                                                            │
│  Nodes 2-11 (Executors): Each has 1-4 GPUs                │
│  ┌──────────────┐                                         │
│  │ Executor 1   │  NVIDIA A100 GPU (40GB)                 │
│  │ 8 CPU cores  │  • 6912 CUDA cores                      │
│  │ 32GB RAM     │  • 1555 GB/s memory bandwidth           │
│  │ 1 GPU        │  • 19.5 TFLOPS FP64 performance         │
│  └──────────────┘                                         │
│                                                            │
│  × 10 executors = 10 GPUs = Massive parallel compute!     │
└────────────────────────────────────────────────────────────┘

CLOUD PROVIDERS:
• AWS: p3.2xlarge (1× V100), p4d.24xlarge (8× A100)
• Google Cloud: n1-standard-8 + 1-4× T4/A100/V100
• Azure: NC6s_v3 (1× V100), NC24ads_A100_v4 (1× A100)
• Databricks: Use GPU-enabled clusters (select GPU instance types)

================================================================================
PERFORMANCE COMPARISON:
================================================================================

EXAMPLE WORKLOAD: Feature engineering on 100M rows with 50 transformations

CPU CLUSTER (No GPUs):
• 10 executors × 8 cores = 80 concurrent tasks
• Processing time: 45 minutes
• Cost: $20 (10 nodes × 45 min)

GPU CLUSTER (10 GPUs):
• 10 executors × 1 GPU each = 10 GPUs
• Processing time: 3 minutes ⚡ 15x faster!
• Cost: $8 (10 nodes × 3 min, despite higher $/hour for GPUs)

RESULT: 15x faster + 60% cost reduction! 💰

WHEN TO USE GPU:
✅ Matrix operations (linear algebra, ML training)
✅ Element-wise operations (math on every row)
✅ Deep learning inference
✅ Image/video/signal processing
✅ Large-scale aggregations

❌ Small datasets (< 1M rows)
❌ Complex branching logic (if/else chains)
❌ String parsing with complex regex
❌ Operations already fast on CPU

================================================================================
LIMITATIONS & CONSIDERATIONS:
================================================================================

LIMITATIONS:
1. GPU Memory Constraints
   • GPUs have limited memory (16-80GB vs 100s of GB RAM)
   • Must batch data carefully
   • Use GPU only for compute-heavy parts

2. Data Transfer Overhead
   • Copying data CPU ↔ GPU has latency
   • Only worth it for expensive operations
   • Keep data on GPU across operations when possible

3. Not All Operations Benefit
   • String regex, JSON parsing: minimal speedup
   • Simple filters: CPU is already fast
   • Complex conditional logic: doesn't parallelize well

4. Infrastructure Costs
   • GPU instances are 2-5x more expensive per hour
   • But complete jobs 10-50x faster → net savings
   • Best for production workloads, not exploratory analysis

BEST PRACTICES:
• Profile first: Identify bottlenecks (use Spark UI)
• Batch operations: Do multiple ops on GPU before transferring back
• Use GPU for compute-intensive: ML training, inference, matrix ops
• Monitor GPU utilization: Ensure GPUs aren't idle (target 80%+)
• Handle failures: GPUs can fail, implement retry logic

================================================================================
USAGE:
================================================================================

This script demonstrates GPU acceleration patterns:

    python 05_gpu_acceleration.py

Examples included:
1. RAPIDS cuDF integration (GPU DataFrames)
2. PyTorch inference on Spark (batch predictions)
3. GPU-accelerated UDFs (custom CUDA kernels)
4. Performance benchmarking (CPU vs GPU)
5. Configuration examples

NOTE: This script demonstrates the concepts. To actually run GPU code,
you need:
• GPU-enabled hardware (NVIDIA GPU with CUDA support)
• CUDA Toolkit installed
• RAPIDS / PyTorch with CUDA support
• Cluster configuration for GPU executors

================================================================================
RELATED RESOURCES:
================================================================================

RAPIDS:
  https://rapids.ai/
  https://docs.rapids.ai/api/cudf/stable/

Spark + RAPIDS:
  https://nvidia.github.io/spark-rapids/

PyTorch + Spark:
  https://pytorch.org/docs/stable/distributed.html

Numba CUDA:
  https://numba.readthedocs.io/en/stable/cuda/index.html

AWS GPU Instances:
  https://aws.amazon.com/ec2/instance-types/p4/

Databricks GPU Clusters:
  https://docs.databricks.com/clusters/gpu.html

Related files in this project:
  • ../cluster_computing/06_gpu_accelerated_udfs.py - Detailed UDF examples
  • ../pyspark_pytorch/ - PyTorch integration examples
  • 01_dag_visualization.py - Understanding execution plans

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - Comprehensive GPU Acceleration Guide
CREATED: 2024
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import DoubleType, ArrayType
import pandas as pd
import numpy as np
import time

# ================================================================================
# GPU AVAILABILITY CHECK
# ================================================================================

def check_gpu_availability():
    """
    Check if GPU is available on this system.
    
    This function detects:
    • CUDA availability
    • Number of GPUs
    • GPU model and memory
    • cuDF installation status
    """
    print("=" * 80)
    print("GPU AVAILABILITY CHECK")
    print("=" * 80)
    
    # ========================================================================
    # Check CUDA (NVIDIA's parallel computing platform)
    # ========================================================================
    try:
        import torch
        cuda_available = torch.cuda.is_available()
        
        if cuda_available:
            print(f"✅ CUDA is available")
            print(f"   CUDA version: {torch.version.cuda}")
            print(f"   Number of GPUs: {torch.cuda.device_count()}")
            
            # Get GPU details for each device
            for i in range(torch.cuda.device_count()):
                gpu_name = torch.cuda.get_device_name(i)
                gpu_memory = torch.cuda.get_device_properties(i).total_memory / 1e9
                print(f"   GPU {i}: {gpu_name} ({gpu_memory:.1f} GB)")
        else:
            print("❌ CUDA not available - running on CPU only")
            print("   To enable GPU:")
            print("   1. Install NVIDIA GPU drivers")
            print("   2. Install CUDA Toolkit")
            print("   3. Install PyTorch with CUDA: pip install torch --index-url https://download.pytorch.org/whl/cu118")
    except ImportError:
        print("⚠️  PyTorch not installed - cannot check CUDA")
        print("   Install: pip install torch")
    
    # ========================================================================
    # Check RAPIDS cuDF (GPU DataFrame library)
    # ========================================================================
    try:
        import cudf
        print(f"\n✅ RAPIDS cuDF is installed (version {cudf.__version__})")
        print("   GPU-accelerated DataFrames available!")
    except ImportError:
        print("\n❌ RAPIDS cuDF not installed")
        print("   Install: pip install cudf-cu11 --extra-index-url=https://pypi.nvidia.com")
    
    # ========================================================================
    # Check Numba (for custom CUDA kernels)
    # ========================================================================
    try:
        from numba import cuda
        print(f"\n✅ Numba CUDA is available")
        print("   Custom GPU kernels can be written!")
    except ImportError:
        print("\n❌ Numba not installed")
        print("   Install: pip install numba")
    
    print("\n" + "=" * 80)


# ================================================================================
# APPROACH 1: CPU-ONLY BASELINE (FOR COMPARISON)
# ================================================================================

def cpu_baseline_example():
    """
    Demonstrate CPU-only processing as baseline for performance comparison.
    
    This establishes a baseline to measure GPU speedup against.
    """
    print("\n" + "=" * 80)
    print("APPROACH 1: CPU-ONLY BASELINE")
    print("=" * 80)
    
    # ========================================================================
    # Create Spark session (CPU-only configuration)
    # ========================================================================
    spark = SparkSession.builder \
        .appName("CPU_Baseline") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("\n🔹 Creating large dataset (10M rows)...")
    
    # Create sample data: sensor readings with timestamp
    # This simulates IoT or time series data
    df = spark.range(0, 10_000_000).toDF("id") \
        .withColumn("sensor_value", (col("id") % 100) / 10.0) \
        .withColumn("timestamp", col("id") * 1000)
    
    print(f"   Dataset: {df.count():,} rows")
    
    # ========================================================================
    # CPU Processing: Feature Engineering
    # ========================================================================
    print("\n🔹 CPU Processing: Feature engineering...")
    
    start_time = time.time()
    
    # Apply multiple transformations (typical feature engineering)
    result = df \
        .withColumn("squared", col("sensor_value") ** 2) \
        .withColumn("cubed", col("sensor_value") ** 3) \
        .withColumn("sqrt", col("sensor_value") ** 0.5) \
        .withColumn("log", col("sensor_value") + 1)  # log(x+1) to avoid log(0)
    
    # Trigger computation with action
    row_count = result.count()
    
    cpu_time = time.time() - start_time
    
    print(f"   ✅ Processed {row_count:,} rows")
    print(f"   ⏱️  CPU Time: {cpu_time:.2f} seconds")
    print(f"   📊 Throughput: {row_count / cpu_time:,.0f} rows/sec")
    
    print("\n💡 This is our baseline. Let's see how GPU compares!")
    
    spark.stop()
    return cpu_time


# ================================================================================
# APPROACH 2: RAPIDS cuDF (GPU DataFrames)
# ================================================================================

def rapids_cudf_example():
    """
    Demonstrate RAPIDS cuDF for GPU-accelerated DataFrame operations.
    
    cuDF provides a pandas-like API that runs entirely on GPU, offering
    10-50x speedup for many operations.
    
    NOTE: Requires RAPIDS cuDF installed and NVIDIA GPU with CUDA support.
    """
    print("\n" + "=" * 80)
    print("APPROACH 2: RAPIDS cuDF (GPU DataFrames)")
    print("=" * 80)
    
    try:
        import cudf
        
        print("\n🔹 Processing with GPU-accelerated cuDF...")
        
        start_time = time.time()
        
        # ====================================================================
        # Create cuDF DataFrame ON GPU
        # ====================================================================
        # Data is created directly in GPU memory
        gpu_df = cudf.DataFrame({
            'id': range(10_000_000),
            'sensor_value': [(i % 100) / 10.0 for i in range(10_000_000)]
        })
        
        # ====================================================================
        # GPU Operations (all run on GPU - no CPU transfer!)
        # ====================================================================
        # These operations run on 1000s of CUDA cores in parallel
        gpu_df['squared'] = gpu_df['sensor_value'] ** 2
        gpu_df['cubed'] = gpu_df['sensor_value'] ** 3
        gpu_df['sqrt'] = gpu_df['sensor_value'] ** 0.5
        gpu_df['log'] = gpu_df['sensor_value'] + 1
        
        # Force computation (cuDF is also lazy)
        row_count = len(gpu_df)
        
        gpu_time = time.time() - start_time
        
        print(f"   ✅ Processed {row_count:,} rows on GPU")
        print(f"   ⏱️  GPU Time: {gpu_time:.2f} seconds")
        print(f"   📊 Throughput: {row_count / gpu_time:,.0f} rows/sec")
        
        print("\n🚀 cuDF Operation Speedups (typical):")
        print("   • Element-wise operations: 10-50x faster")
        print("   • GroupBy aggregations: 5-20x faster")
        print("   • Joins: 2-10x faster")
        print("   • Sorts: 5-15x faster")
        
        print("\n💡 Integration with Spark:")
        print("   Use pandas_udf to convert Spark → Pandas → cuDF → GPU")
        print("   Process each partition on GPU, return to Spark")
        
    except ImportError:
        print("\n⚠️  RAPIDS cuDF not installed")
        print("   Install: conda install -c rapidsai -c conda-forge cudf")
        print("   Or: pip install cudf-cu11")


# ================================================================================
# APPROACH 3: PyTorch Inference on Spark
# ================================================================================

def pytorch_inference_example():
    """
    Demonstrate PyTorch model inference using GPUs across Spark partitions.
    
    This pattern is common for:
    • Image classification on millions of images
    • Text embeddings (BERT, GPT) on documents
    • Any deep learning inference at scale
    
    Each Spark partition is processed on a GPU, enabling massive parallelism.
    """
    print("\n" + "=" * 80)
    print("APPROACH 3: PyTorch GPU Inference on Spark")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("PyTorch_GPU_Inference") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("\n🔹 Pattern: Broadcast model → Partition inference → Parallel GPUs")
    
    # ========================================================================
    # Step 1: Create sample data (image embeddings)
    # ========================================================================
    print("\n1️⃣  Creating sample data (simulating image embeddings)...")
    
    # In real scenario: images loaded from S3/HDFS
    # Each row represents an image as a 512-dim embedding vector
    df = spark.range(0, 100000).toDF("image_id") \
        .withColumn("pixel_data", col("image_id") % 256)  # Simplified
    
    print(f"   Dataset: {df.count():,} images")
    
    # ========================================================================
    # Step 2: Define pandas_udf that runs PyTorch on GPU
    # ========================================================================
    print("\n2️⃣  Defining GPU inference UDF...")
    
    @pandas_udf(DoubleType())
    def predict_on_gpu(pixel_data: pd.Series) -> pd.Series:
        """
        Run PyTorch model inference on GPU.
        
        This function is called once per Spark partition.
        Each partition is processed on a GPU in parallel!
        
        HOW IT WORKS:
        1. Function receives a batch of rows (1 partition)
        2. Loads PyTorch model onto GPU
        3. Converts data to GPU tensor
        4. Runs model inference on GPU
        5. Returns predictions to Spark
        
        In production:
        • Broadcast model to avoid loading per partition
        • Use larger batch sizes (1000s-10000s per partition)
        • Handle GPU OOM errors gracefully
        """
        try:
            import torch
            
            # Check if GPU is available
            device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
            
            # In production: Load pre-trained model
            # model = torch.load('model.pth').to(device)
            # For demo: Simple computation simulating inference
            
            # Convert pandas Series to PyTorch tensor ON GPU
            data_tensor = torch.tensor(pixel_data.values, dtype=torch.float32).to(device)
            
            # Simulate model inference (actual model.forward() in production)
            # Example: prediction = model(data_tensor)
            predictions = torch.sigmoid(data_tensor / 256.0)  # Dummy prediction
            
            # Move results back to CPU and return as pandas Series
            return pd.Series(predictions.cpu().numpy())
            
        except Exception as e:
            # Fallback to CPU if GPU unavailable
            return pd.Series([0.5] * len(pixel_data))
    
    # ========================================================================
    # Step 3: Apply UDF to DataFrame (triggers GPU inference)
    # ========================================================================
    print("\n3️⃣  Running inference on Spark partitions...")
    
    start_time = time.time()
    
    # Apply GPU inference UDF
    # Spark will call predict_on_gpu() once per partition
    # If you have 10 partitions and 10 GPUs → 10 parallel inferences!
    result = df.withColumn("prediction", predict_on_gpu(col("pixel_data")))
    
    # Trigger computation
    predictions_count = result.count()
    
    inference_time = time.time() - start_time
    
    print(f"   ✅ Generated {predictions_count:,} predictions")
    print(f"   ⏱️  Time: {inference_time:.2f} seconds")
    print(f"   📊 Throughput: {predictions_count / inference_time:,.0f} predictions/sec")
    
    # Show sample results
    print("\n📊 Sample predictions:")
    result.show(5, truncate=False)
    
    print("\n💡 In Production with 10 GPUs:")
    print("   • 10 executors × 1 GPU each = 10 GPUs")
    print("   • Each GPU processes its partitions in parallel")
    print("   • 10x throughput vs single GPU!")
    print("   • Can process millions of images per minute")
    
    spark.stop()


# ================================================================================
# APPROACH 4: GPU-Accelerated UDFs (Custom CUDA Kernels)
# ================================================================================

def gpu_accelerated_udf_example():
    """
    Demonstrate custom GPU kernels using Numba CUDA.
    
    When built-in operations aren't enough, write custom CUDA kernels
    that run directly on GPU hardware for maximum performance.
    
    USE CASES:
    • Custom mathematical operations
    • Specialized algorithms
    • When cuDF/PyTorch don't provide what you need
    """
    print("\n" + "=" * 80)
    print("APPROACH 4: GPU-Accelerated UDFs (Custom CUDA Kernels)")
    print("=" * 80)
    
    try:
        from numba import cuda
        import math
        
        print("\n🔹 Writing custom CUDA kernel for element-wise operations...")
        
        # ====================================================================
        # Define CUDA kernel (runs on GPU)
        # ====================================================================
        @cuda.jit
        def complex_computation_kernel(input_array, output_array):
            """
            Custom CUDA kernel that runs on GPU.
            
            CUDA EXECUTION MODEL:
            • Launched with 1000s of threads
            • Each thread processes one element
            • All threads execute in parallel on CUDA cores
            
            cuda.grid(1): Get unique thread ID (which element to process)
            """
            # Get the thread's unique ID (which array element to process)
            idx = cuda.grid(1)
            
            # Bounds check (don't access beyond array)
            if idx < input_array.size:
                # Custom computation (runs on GPU)
                x = input_array[idx]
                output_array[idx] = (x ** 2 + math.sqrt(abs(x)) + math.sin(x)) / 10.0
        
        print("   ✅ CUDA kernel defined")
        
        # ====================================================================
        # Test kernel with sample data
        # ====================================================================
        print("\n🔹 Testing CUDA kernel on GPU...")
        
        # Create sample data
        n = 1_000_000
        input_data = np.random.randn(n).astype(np.float32)
        output_data = np.zeros(n, dtype=np.float32)
        
        # Copy data to GPU
        d_input = cuda.to_device(input_data)
        d_output = cuda.to_device(output_data)
        
        # Configure kernel launch
        # threads_per_block: How many threads per block (typical: 256-1024)
        # blocks_per_grid: How many blocks needed to cover all data
        threads_per_block = 256
        blocks_per_grid = (n + threads_per_block - 1) // threads_per_block
        
        print(f"   Launching kernel:")
        print(f"   • {blocks_per_grid:,} blocks")
        print(f"   • {threads_per_block} threads per block")
        print(f"   • Total: {blocks_per_grid * threads_per_block:,} threads")
        
        # Launch kernel
        start_time = time.time()
        complex_computation_kernel[blocks_per_grid, threads_per_block](d_input, d_output)
        cuda.synchronize()  # Wait for kernel to finish
        kernel_time = time.time() - start_time
        
        # Copy result back to CPU
        result = d_output.copy_to_host()
        
        print(f"\n   ✅ Processed {n:,} elements")
        print(f"   ⏱️  GPU Time: {kernel_time:.4f} seconds")
        print(f"   📊 Throughput: {n / kernel_time:,.0f} elements/sec")
        
        print("\n💡 Using in Spark:")
        print("   • Write CUDA kernel")
        print("   • Wrap in pandas_udf")
        print("   • Apply to Spark DataFrame")
        print("   • Each partition processed on GPU")
        
    except ImportError:
        print("\n⚠️  Numba not installed")
        print("   Install: pip install numba")
    except Exception as e:
        print(f"\n⚠️  CUDA not available: {e}")
        print("   Requires NVIDIA GPU with CUDA support")


# ================================================================================
# PERFORMANCE BENCHMARKING
# ================================================================================

def benchmark_cpu_vs_gpu():
    """
    Compare CPU vs GPU performance on identical workload.
    
    This gives a realistic sense of potential speedups.
    """
    print("\n" + "=" * 80)
    print("PERFORMANCE BENCHMARK: CPU vs GPU")
    print("=" * 80)
    
    print("\n🎯 Workload: Feature engineering on 10M rows")
    print("   Operations: square, cube, sqrt, log")
    
    # Run CPU baseline
    print("\n" + "-" * 80)
    print("Running CPU baseline...")
    print("-" * 80)
    cpu_time = cpu_baseline_example()
    
    print("\n" + "-" * 80)
    print("Benchmark Summary")
    print("-" * 80)
    print(f"CPU Time: {cpu_time:.2f} seconds")
    print(f"\nPotential GPU Speedup (typical): 10-50x")
    print(f"Estimated GPU Time: {cpu_time / 20:.2f} seconds (assuming 20x speedup)")
    
    print("\n💡 Actual speedup depends on:")
    print("   • GPU model (A100 > V100 > T4)")
    print("   • Operation type (matrix ops faster than string ops)")
    print("   • Data size (larger = better GPU utilization)")
    print("   • Memory bandwidth (PCIe bottleneck?)")


# ================================================================================
# MAIN EXECUTION
# ================================================================================

def main():
    """
    Run all GPU acceleration examples.
    """
    print("\n" + "🚀" * 40)
    print("GPU ACCELERATION IN PYSPARK - COMPLETE GUIDE")
    print("🚀" * 40)
    
    # Check GPU availability first
    check_gpu_availability()
    
    # Run examples
    cpu_baseline_example()      # Baseline for comparison
    rapids_cudf_example()        # GPU DataFrames
    pytorch_inference_example()  # DL inference
    gpu_accelerated_udf_example()  # Custom kernels
    benchmark_cpu_vs_gpu()       # Performance comparison
    
    print("\n" + "=" * 80)
    print("✅ GPU ACCELERATION GUIDE COMPLETE")
    print("=" * 80)
    
    print("\n📚 Key Takeaways:")
    print("   1. GPUs offer 10-100x speedup for compute-intensive tasks")
    print("   2. Use RAPIDS for DataFrame operations (cuDF)")
    print("   3. Use PyTorch for deep learning inference")
    print("   4. Write custom CUDA kernels for specialized operations")
    print("   5. GPU clusters enable massive parallel processing")
    print("   6. Profile first - not all operations benefit from GPU")
    
    print("\n💰 Cost-Benefit:")
    print("   • GPU instances: 2-5x more expensive per hour")
    print("   • GPU processing: 10-50x faster")
    print("   • Net result: 50-80% cost reduction for large workloads!")


if __name__ == "__main__":
    main()
