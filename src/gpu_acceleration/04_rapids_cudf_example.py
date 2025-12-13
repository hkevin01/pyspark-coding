#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
GPU ACCELERATION #4 - RAPIDS cuDF for GPU DataFrames
================================================================================

MODULE OVERVIEW:
----------------
RAPIDS cuDF is a GPU DataFrame library that mimics pandas API but runs on GPU.
It provides 10-50x speedup over pandas for large datasets by leveraging CUDA.

Key Features:
• Drop-in replacement for pandas (similar API)
• GPU-accelerated operations
• Integration with PySpark via Arrow
• Support for most pandas operations
• Native CUDA kernels for performance

PURPOSE:
--------
Learn GPU DataFrames with RAPIDS:
• cuDF basics (GPU pandas)
• PySpark + cuDF integration
• Performance comparisons
• When to use cuDF vs pandas
• Memory management on GPU

INSTALLATION:
-------------
# For CUDA 11.x
pip install cudf-cu11 cuml-cu11

# Check GPU
nvidia-smi

ARCHITECTURE:
-------------
┌─────────────────────────────────────────────────────────────────┐
│                    RAPIDS ECOSYSTEM                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  cuDF: GPU DataFrames (pandas-like API)                        │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • read_csv(), read_parquet()                       │        │
│  │ • filter(), groupby(), merge()                     │        │
│  │ • All operations run on GPU                        │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                 │
│  cuML: GPU Machine Learning                                    │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • Linear models, trees, clustering                 │        │
│  │ • 10-50x faster than scikit-learn                  │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                 │
│  cuGraph: GPU Graph Analytics                                  │
│  ┌────────────────────────────────────────────────────┐        │
│  │ • PageRank, community detection                    │        │
│  └────────────────────────────────────────────────────┘        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np
import time

# Try to import cuDF (optional - gracefully degrade to pandas)
try:
    import cudf
    CUDF_AVAILABLE = True
except ImportError:
    CUDF_AVAILABLE = False
    print("⚠️  cuDF not installed. Install with: pip install cudf-cu11")
    print("   Falling back to pandas for demonstration.")


def create_spark():
    """Create Spark session with Arrow enabled."""
    return SparkSession.builder \
        .appName("RAPIDS_cuDF") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "100000") \
        .getOrCreate()


def example1_cudf_basics():
    """
    EXAMPLE 1: cuDF Basics (GPU pandas)
    ===================================
    
    Demonstrates basic cuDF operations that run on GPU.
    """
    print("=" * 70)
    print("EXAMPLE 1: cuDF Basics")
    print("=" * 70)
    
    if not CUDF_AVAILABLE:
        print("\n⚠️  cuDF not available. Using pandas for demo.")
        # Pandas equivalent
        df = pd.DataFrame({
            'a': np.arange(10_000_000),
            'b': np.random.randn(10_000_000),
            'c': np.random.randn(10_000_000)
        })
        
        start = time.time()
        result = df[df['b'] > 0].groupby('a')['c'].mean()
        pandas_time = time.time() - start
        
        print(f"\n🐼 Pandas time: {pandas_time:.4f}s")
        print(f"   Result: {len(result):,} groups")
        return
    
    print("\n📊 Creating GPU DataFrame (10M rows)")
    
    # Create cuDF DataFrame (on GPU!)
    gdf = cudf.DataFrame({
        'a': cp.arange(10_000_000),
        'b': cp.random.randn(10_000_000),
        'c': cp.random.randn(10_000_000)
    })
    
    print(f"   Memory: {gdf.memory_usage(deep=True).sum() / 1e9:.2f} GB (on GPU)")
    
    # Pandas baseline
    print("\n🐼 Pandas Baseline (CPU):")
    pdf = gdf.to_pandas()  # Transfer to CPU
    start = time.time()
    pandas_result = pdf[pdf['b'] > 0].groupby('a')['c'].mean()
    pandas_time = time.time() - start
    print(f"   Time: {pandas_time:.4f}s")
    
    # cuDF (GPU)
    print("\n🎮 cuDF (GPU):")
    start = time.time()
    cudf_result = gdf[gdf['b'] > 0].groupby('a')['c'].mean()
    cudf_time = time.time() - start
    print(f"   Time: {cudf_time:.4f}s")
    
    print(f"\n🏆 Speedup: {pandas_time / cudf_time:.1f}x faster on GPU!")
    print(f"   Result: {len(cudf_result):,} groups")


def example2_pyspark_cudf_integration(spark):
    """
    EXAMPLE 2: PySpark + cuDF Integration
    ======================================
    
    Use cuDF inside Pandas UDFs for GPU acceleration.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: PySpark + cuDF Integration")
    print("=" * 70)
    
    # Create Spark DataFrame
    df = spark.range(5_000_000).toDF("id") \
        .withColumn("value1", (col("id") * 2.5).cast("double")) \
        .withColumn("value2", (col("id") % 1000).cast("double"))
    
    print(f"\n📊 Dataset: {df.count():,} rows")
    
    @pandas_udf("double")
    def gpu_transform_udf(batch: pd.Series) -> pd.Series:
        """Transform using cuDF (GPU) if available."""
        
        if not CUDF_AVAILABLE:
            # Fallback to pandas
            return batch.apply(lambda x: np.sin(x) * np.exp(x))
        
        # Transfer to GPU
        gdf = cudf.Series(batch)
        
        # GPU operations
        result = gdf.sin() * gdf.exp()
        
        # Transfer back to CPU
        return result.to_pandas()
    
    print("\n🎮 Applying GPU transformation...")
    start = time.time()
    result_df = df.withColumn("transformed", gpu_transform_udf(col("value1")))
    result_count = result_df.count()
    elapsed = time.time() - start
    
    print(f"   Time: {elapsed:.4f}s")
    print(f"   Processed: {result_count:,} rows")
    
    # Show sample
    print("\n📝 Sample results:")
    result_df.show(5, truncate=False)


def example3_performance_comparison(spark):
    """
    EXAMPLE 3: cuDF vs Pandas Performance
    ======================================
    
    Benchmark various operations on CPU vs GPU.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Performance Comparison (cuDF vs Pandas)")
    print("=" * 70)
    
    sizes = [1_000_000, 5_000_000, 10_000_000]
    operations = ["filter", "groupby", "merge", "sort"]
    
    print("\n📊 Benchmarking operations at different scales:")
    print("   (Estimated speedups - actual requires GPU)")
    
    results = []
    for size in sizes:
        print(f"\n   Dataset: {size:,} rows")
        for op in operations:
            # Estimate speedups based on typical cuDF performance
            speedups = {
                "filter": 5,
                "groupby": 15,
                "merge": 20,
                "sort": 10
            }
            speedup = speedups[op]
            
            print(f"      {op.ljust(10)}: {speedup}x faster on GPU")
    
    print("\n💡 Key Insights:")
    print("   • groupby: 10-20x speedup (GPU excels)")
    print("   • merge/join: 15-25x speedup")
    print("   • sort: 8-12x speedup")
    print("   • filter: 5-8x speedup")
    print("   • Larger datasets = bigger speedups")


def example4_memory_management():
    """
    EXAMPLE 4: GPU Memory Management
    ================================
    
    Managing GPU memory is crucial for cuDF.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: GPU Memory Management")
    print("=" * 70)
    
    print("""
    🎯 GPU MEMORY BEST PRACTICES:
    
    1. Monitor GPU Memory:
       import cudf
       print(cudf.get_memory_info())
       
       # Returns: (used_memory, total_memory)
    
    2. Free Memory When Done:
       del gdf  # Delete DataFrame
       import gc
       gc.collect()  # Garbage collection
    
    3. Chunked Processing for Large Data:
       chunk_size = 10_000_000
       for chunk in pd.read_csv('large.csv', chunksize=chunk_size):
           gdf = cudf.from_pandas(chunk)
           # Process gdf
           del gdf  # Free GPU memory
    
    4. Spill to CPU if GPU Full:
       try:
           gdf = cudf.read_csv('file.csv')
       except MemoryError:
           # Fallback to pandas
           pdf = pd.read_csv('file.csv')
    
    5. Monitor with nvidia-smi:
       watch -n 1 nvidia-smi
       
       # Check GPU utilization and memory
    
    ⚠️  COMMON PITFALLS:
    
    ❌ Forgetting to delete DataFrames
    ❌ Not calling gc.collect()
    ❌ Loading full dataset that exceeds GPU RAM
    ❌ Not monitoring GPU memory usage
    
    ✅ SOLUTIONS:
    
    ✅ Process in chunks
    ✅ Delete DataFrames immediately after use
    ✅ Use context managers for automatic cleanup
    ✅ Monitor with nvidia-smi
    """)


def example5_cudf_code_patterns():
    """
    EXAMPLE 5: Common cuDF Patterns
    ================================
    
    Practical code patterns for GPU DataFrames.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Common cuDF Code Patterns")
    print("=" * 70)
    
    print("""
    🔧 PATTERN 1: CSV Processing
    =============================
    
    import cudf
    
    # Read CSV to GPU
    gdf = cudf.read_csv('large_file.csv')
    
    # Operations on GPU
    gdf = gdf[gdf['amount'] > 1000]
    gdf['total'] = gdf['price'] * gdf['quantity']
    result = gdf.groupby('category')['total'].sum()
    
    # Write back
    result.to_pandas().to_csv('output.csv')
    
    
    🔧 PATTERN 2: PySpark Integration
    ==================================
    
    from pyspark.sql.functions import pandas_udf
    import cudf
    
    @pandas_udf("double")
    def gpu_udf(batch: pd.Series) -> pd.Series:
        # Convert pandas to cuDF (transfer to GPU)
        gdf = cudf.Series(batch)
        
        # GPU operations
        result = gdf.sin() * gdf.cos() + gdf.exp()
        
        # Convert back to pandas (transfer to CPU)
        return result.to_pandas()
    
    # Apply to Spark DataFrame
    df_result = df.withColumn("result", gpu_udf(col("features")))
    
    
    🔧 PATTERN 3: Chunked Processing
    =================================
    
    import cudf
    import pandas as pd
    
    chunk_size = 10_000_000
    results = []
    
    for chunk in pd.read_csv('huge.csv', chunksize=chunk_size):
        # Transfer chunk to GPU
        gdf = cudf.from_pandas(chunk)
        
        # Process on GPU
        processed = gdf[gdf['value'] > 0].groupby('key')['value'].mean()
        
        # Transfer result back
        results.append(processed.to_pandas())
        
        # Free GPU memory
        del gdf
        gc.collect()
    
    # Combine results
    final = pd.concat(results)
    
    
    🔧 PATTERN 4: Hybrid CPU/GPU
    =============================
    
    import cudf
    import pandas as pd
    
    # Stage 1: CPU preprocessing (strings)
    pdf = pd.read_csv('data.csv')
    pdf['processed_text'] = pdf['text'].str.upper()
    
    # Stage 2: GPU computation (numbers)
    gdf = cudf.from_pandas(pdf[['value1', 'value2']])
    gdf['result'] = gdf['value1'].sin() * gdf['value2'].exp()
    
    # Stage 3: CPU postprocessing
    pdf['result'] = gdf['result'].to_pandas()
    pdf.to_csv('output.csv')
    
    
    🔧 PATTERN 5: Error Handling
    =============================
    
    import cudf
    import pandas as pd
    
    def process_with_gpu_fallback(file_path):
        try:
            # Try GPU
            gdf = cudf.read_csv(file_path)
            result = gdf.groupby('key')['value'].mean()
            return result.to_pandas()
        except (MemoryError, RuntimeError):
            # Fallback to CPU
            print("⚠️  GPU out of memory, using CPU")
            pdf = pd.read_csv(file_path)
            return pdf.groupby('key')['value'].mean()
    """)


def show_cudf_decision_matrix():
    """When to use cuDF vs pandas."""
    print("\n" + "=" * 70)
    print("cuDF vs Pandas DECISION MATRIX")
    print("=" * 70)
    
    print("""
    ┌───────────────────────────┬─────────────┬─────────────────────┐
    │ Scenario                  │ Choice      │ Reason              │
    ├───────────────────────────┼─────────────┼─────────────────────┤
    │ Dataset < 1 GB            │ Pandas      │ Transfer overhead   │
    │ Dataset 1-10 GB           │ cuDF ✅     │ Good GPU speedup    │
    │ Dataset > 10 GB           │ cuDF ✅     │ Great GPU speedup   │
    │ String operations         │ Pandas      │ cuDF limited support│
    │ Numeric operations        │ cuDF ✅     │ GPU optimized       │
    │ GroupBy aggregations      │ cuDF ✅     │ 15-20x speedup      │
    │ Merge/Join                │ cuDF ✅     │ 20-30x speedup      │
    │ Complex pandas features   │ Pandas      │ cuDF missing some   │
    │ Production pipeline       │ cuDF ✅     │ If GPU available    │
    │ Development/prototyping   │ Pandas      │ Easier debugging    │
    └───────────────────────────┴─────────────┴─────────────────────┘
    
    🎯 USE cuDF WHEN:
    ✅ Dataset > 1 GB
    ✅ Numeric-heavy operations
    ✅ GroupBy, merge, join operations
    ✅ GPU available
    ✅ Production workload at scale
    
    🎯 USE Pandas WHEN:
    🐼 Dataset < 1 GB
    🐼 String-heavy operations
    🐼 Need specific pandas features
    🐼 Development/debugging
    🐼 No GPU available
    
    💰 COST CONSIDERATION:
    GPU instances cost 1.5-6x more than CPU
    Break-even: Need 10-20x speedup for ROI
    cuDF typically provides 10-50x speedup
    → Usually cost-effective for large datasets!
    """)


def main():
    """Run all cuDF examples."""
    spark = create_spark()
    
    print("🎮 RAPIDS cuDF - GPU DataFrames")
    print("=" * 70)
    print("\nGPU-accelerated pandas-like DataFrames with 10-50x speedup!")
    
    # Check cuDF availability
    if CUDF_AVAILABLE:
        print("\n✅ cuDF available! Running GPU examples.")
    else:
        print("\n⚠️  cuDF not available. Showing examples with pandas fallback.")
        print("   Install: pip install cudf-cu11 cuml-cu11")
    
    # Run examples
    example1_cudf_basics()
    example2_pyspark_cudf_integration(spark)
    example3_performance_comparison(spark)
    example4_memory_management()
    example5_cudf_code_patterns()
    
    # Show decision matrix
    show_cudf_decision_matrix()
    
    print("\n" + "=" * 70)
    print("✅ cuDF EXAMPLES COMPLETE")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. cuDF = GPU pandas (similar API)")
    print("   2. 10-50x speedup for large datasets (> 1 GB)")
    print("   3. Best for: groupby, merge, numeric ops")
    print("   4. Limited string support (use pandas)")
    print("   5. GPU memory management crucial")
    print("   6. Easy integration with PySpark")
    
    print("\n📚 See Also:")
    print("   • 02_when_gpu_wins.py - GPU benefits")
    print("   • 03_hybrid_cpu_gpu.py - CPU+GPU patterns")
    print("   • 05_ml_inference_gpu.py - Deep learning on GPU")
    
    print("\n🔗 Resources:")
    print("   • RAPIDS docs: https://docs.rapids.ai")
    print("   • cuDF API: https://docs.rapids.ai/api/cudf/stable/")
    print("   • Examples: https://github.com/rapidsai/cudf")
    
    spark.stop()


if __name__ == "__main__":
    # Add missing imports
    from pyspark.sql.functions import col
    try:
        import cupy as cp
    except ImportError:
        cp = np  # Fallback
    
    main()
