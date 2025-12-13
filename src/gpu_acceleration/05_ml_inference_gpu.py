#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
GPU ACCELERATION #5 - Deep Learning Inference on GPU
================================================================================

MODULE OVERVIEW:
----------------
Deep learning model inference is one of the BEST use cases for GPU acceleration.
GPUs provide 30-100x speedup over CPU for neural network inference due to:
• Matrix multiplication (core of neural networks)
• Batch processing (process hundreds of samples in parallel)
• Optimized libraries (cuDNN, TensorRT)
• High memory bandwidth

This module demonstrates GPU-accelerated ML inference in PySpark.

PURPOSE:
--------
Learn GPU ML inference patterns:
• PyTorch/TensorFlow models on GPU
• Batch inference at scale
• PySpark + GPU integration
• Model serving patterns
• Distributed GPU inference

KEY CONCEPTS:
-------------
• Model Loading: Load once, reuse across batches
• Batch Processing: Process multiple inputs together
• GPU Memory Management: Monitor VRAM usage
• Transfer Overhead: Minimize CPU ↔ GPU transfers

ARCHITECTURE:
-------------
┌─────────────────────────────────────────────────────────────────┐
│              GPU ML INFERENCE PIPELINE                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Load Model to GPU (once per executor)                      │
│     ┌────────────────────────────────────┐                     │
│     │ model = torch.load('model.pt')     │                     │
│     │ model.to('cuda')  # Move to GPU    │                     │
│     │ model.eval()      # Inference mode │                     │
│     └────────────────────────────────────┘                     │
│                        ↓                                        │
│  2. Batch Input Data (minimize transfers)                      │
│     ┌────────────────────────────────────┐                     │
│     │ batch = torch.tensor(data)         │                     │
│     │ batch = batch.to('cuda')           │                     │
│     └────────────────────────────────────┘                     │
│                        ↓                                        │
│  3. GPU Inference (fast!)                                      │
│     ┌────────────────────────────────────┐                     │
│     │ with torch.no_grad():              │                     │
│     │     output = model(batch)          │                     │
│     └────────────────────────────────────┘                     │
│                        ↓                                        │
│  4. Transfer Results Back                                      │
│     ┌────────────────────────────────────┐                     │
│     │ results = output.cpu().numpy()     │                     │
│     └────────────────────────────────────┘                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

PERFORMANCE COMPARISON:
-----------------------
Image Classification (ResNet-50):
• CPU: ~200ms per image
• GPU: ~5ms per image (batch size 256)
• Speedup: 40x faster!

Text Classification (BERT):
• CPU: ~100ms per text
• GPU: ~2ms per text (batch size 128)
• Speedup: 50x faster!
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import FloatType, ArrayType
import pandas as pd
import numpy as np
import time

# Try to import PyTorch (optional)
try:
    import torch
    import torch.nn as nn
    TORCH_AVAILABLE = True
    GPU_AVAILABLE = torch.cuda.is_available()
except ImportError:
    TORCH_AVAILABLE = False
    GPU_AVAILABLE = False


def create_spark():
    """Create Spark session for GPU ML inference."""
    return SparkSession.builder \
        .appName("GPU_ML_Inference") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
        .getOrCreate()


def example1_simple_pytorch_inference(spark):
    """
    EXAMPLE 1: Simple PyTorch Model Inference
    ==========================================
    
    Basic example of using PyTorch model on GPU.
    """
    print("=" * 70)
    print("EXAMPLE 1: Simple PyTorch GPU Inference")
    print("=" * 70)
    
    if not TORCH_AVAILABLE:
        print("\n⚠️  PyTorch not available. Install with: pip install torch")
        return
    
    print(f"\n🎮 GPU Available: {GPU_AVAILABLE}")
    if GPU_AVAILABLE:
        print(f"   GPU Device: {torch.cuda.get_device_name(0)}")
        print(f"   GPU Memory: {torch.cuda.get_device_properties(0).total_memory / 1e9:.2f} GB")
    
    # Define a simple model
    class SimpleModel(nn.Module):
        def __init__(self):
            super().__init__()
            self.fc1 = nn.Linear(100, 50)
            self.fc2 = nn.Linear(50, 10)
            self.relu = nn.ReLU()
        
        def forward(self, x):
            x = self.relu(self.fc1(x))
            return self.fc2(x)
    
    # Create and move model to GPU
    model = SimpleModel()
    device = 'cuda' if GPU_AVAILABLE else 'cpu'
    model = model.to(device)
    model.eval()
    
    print(f"\n📊 Model loaded on: {device}")
    
    # Create test data
    batch_size = 1000
    test_data = torch.randn(batch_size, 100)
    
    # CPU inference
    print("\n🖥️  CPU Inference:")
    model_cpu = model.cpu()
    start = time.time()
    with torch.no_grad():
        output_cpu = model_cpu(test_data)
    cpu_time = time.time() - start
    print(f"   Time: {cpu_time:.4f}s for {batch_size} samples")
    print(f"   Throughput: {batch_size / cpu_time:.0f} samples/sec")
    
    # GPU inference
    if GPU_AVAILABLE:
        print("\n🎮 GPU Inference:")
        model_gpu = model.cuda()
        test_data_gpu = test_data.cuda()
        
        # Warmup
        with torch.no_grad():
            _ = model_gpu(test_data_gpu)
        
        start = time.time()
        with torch.no_grad():
            output_gpu = model_gpu(test_data_gpu)
        gpu_time = time.time() - start
        print(f"   Time: {gpu_time:.4f}s for {batch_size} samples")
        print(f"   Throughput: {batch_size / gpu_time:.0f} samples/sec")
        print(f"\n🏆 Speedup: {cpu_time / gpu_time:.1f}x faster!")
    else:
        print("\n⚠️  No GPU available for comparison")


def example2_batch_inference_pattern(spark):
    """
    EXAMPLE 2: Batch Inference Pattern (Production)
    ===============================================
    
    Best practice: Load model once, process batches.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Batch Inference Pattern")
    print("=" * 70)
    
    # Create sample dataset
    df = spark.range(100_000).toDF("id") \
        .selectExpr(
            "id",
            "array(cast(rand() as float), cast(rand() as float), " +
            "cast(rand() as float), cast(rand() as float)) as features"
        )
    
    print(f"\n📊 Dataset: {df.count():,} rows")
    
    @pandas_udf(ArrayType(FloatType()))
    def gpu_inference_udf(features_batch: pd.Series) -> pd.Series:
        """
        GPU inference UDF with model caching.
        
        Key Pattern: Load model once per executor (not per batch!)
        """
        # This would be in real code (model loaded once)
        # global _model
        # if '_model' not in globals():
        #     _model = load_model()
        #     _model = _model.to('cuda')
        #     _model.eval()
        
        if not TORCH_AVAILABLE:
            # Fallback simulation
            return features_batch.apply(
                lambda x: [float(sum(x)) * 2.0]
            )
        
        device = 'cuda' if GPU_AVAILABLE else 'cpu'
        
        # Convert batch to tensor
        input_data = np.array(features_batch.tolist())
        input_tensor = torch.tensor(input_data, dtype=torch.float32).to(device)
        
        # Inference (simulate with simple operation)
        with torch.no_grad():
            # In real code: output = _model(input_tensor)
            output = torch.sum(input_tensor, dim=1, keepdim=True) * 2.0
        
        # Convert back to list
        results = output.cpu().numpy().tolist()
        return pd.Series(results)
    
    print("\n🎮 Running batch inference...")
    start = time.time()
    result_df = df.withColumn("predictions", gpu_inference_udf(col("features")))
    count = result_df.count()
    elapsed = time.time() - start
    
    print(f"   Time: {elapsed:.4f}s")
    print(f"   Throughput: {count / elapsed:.0f} samples/sec")
    
    print("\n📝 Sample predictions:")
    result_df.show(5, truncate=False)
    
    print("\n💡 Key Pattern:")
    print("   ✅ Load model once per executor (global variable)")
    print("   ✅ Process batches efficiently")
    print("   ✅ Minimize model loading overhead")


def example3_image_classification(spark):
    """
    EXAMPLE 3: Image Classification (ResNet Example)
    ================================================
    
    Realistic example: Image classification at scale.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Image Classification on GPU")
    print("=" * 70)
    
    print("""
    �� SCENARIO: Classify 1 Million Images
    
    Model: ResNet-50 (pretrained ImageNet)
    Input: 224×224×3 images
    Output: 1000 class probabilities
    
    🖥️  CPU Performance:
    • Inference time: ~200ms per image
    • Single core: 5 images/sec
    • 16 cores parallel: 80 images/sec
    • Total time: 1M / 80 = 12,500 seconds (~3.5 hours)
    
    🎮 GPU Performance (V100):
    • Batch size: 256
    • Inference time: ~5ms per image
    • Throughput: 200 images/sec per GPU
    • 4 GPUs: 800 images/sec
    • Total time: 1M / 800 = 1,250 seconds (~21 minutes)
    
    🏆 Result: 10x faster with GPU!
    
    💰 COST ANALYSIS:
    
    CPU (r5.4xlarge): $0.672/hour
    • Time: 3.5 hours
    • Cost: $2.35
    
    GPU (p3.2xlarge): $3.06/hour
    • Time: 0.35 hours (21 min)
    • Cost: $1.07
    
    🎯 GPU is FASTER and CHEAPER!
    """)
    
    print("\n🔧 Code Pattern:")
    print("""
    from torchvision import models, transforms
    import torch
    
    # Load model once
    model = models.resnet50(pretrained=True)
    model = model.cuda()
    model.eval()
    
    # Preprocessing
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406],
                           std=[0.229, 0.224, 0.225]),
    ])
    
    @pandas_udf("array<float>")
    def classify_images_udf(image_paths: pd.Series) -> pd.Series:
        results = []
        
        for image_path in image_paths:
            # Load and preprocess image
            image = Image.open(image_path)
            input_tensor = preprocess(image).unsqueeze(0).cuda()
            
            # Inference on GPU
            with torch.no_grad():
                output = model(input_tensor)
                probabilities = torch.nn.functional.softmax(output[0], dim=0)
            
            results.append(probabilities.cpu().numpy().tolist())
        
        return pd.Series(results)
    
    # Apply to Spark DataFrame
    df_classified = df.withColumn("predictions", 
        classify_images_udf(col("image_path")))
    """)


def example4_text_classification(spark):
    """
    EXAMPLE 4: Text Classification (BERT Example)
    =============================================
    
    NLP example: BERT model for text classification.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Text Classification (BERT on GPU)")
    print("=" * 70)
    
    print("""
    📝 SCENARIO: Sentiment Analysis on 10M Tweets
    
    Model: BERT-base-uncased
    Task: Binary sentiment (positive/negative)
    
    🖥️  CPU Performance:
    • Inference time: ~100ms per text
    • Single core: 10 texts/sec
    • 32 cores: 320 texts/sec
    • Total time: 10M / 320 = 31,250 seconds (~8.7 hours)
    
    🎮 GPU Performance (A100):
    • Batch size: 128
    • Inference time: ~2ms per text
    • Throughput: 500 texts/sec per GPU
    • 8 GPUs: 4,000 texts/sec
    • Total time: 10M / 4000 = 2,500 seconds (~42 minutes)
    
    🏆 Result: 12.5x faster with GPU!
    
    🔧 Code Pattern:
    
    from transformers import BertTokenizer, BertForSequenceClassification
    import torch
    
    # Load model and tokenizer
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    model = BertForSequenceClassification.from_pretrained('bert-base-uncased')
    model = model.cuda()
    model.eval()
    
    @pandas_udf("float")
    def classify_text_udf(texts: pd.Series) -> pd.Series:
        # Tokenize batch
        inputs = tokenizer(
            texts.tolist(),
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="pt"
        ).to('cuda')
        
        # Inference on GPU
        with torch.no_grad():
            outputs = model(**inputs)
            logits = outputs.logits
            predictions = torch.nn.functional.softmax(logits, dim=1)
            sentiment_scores = predictions[:, 1].cpu().numpy()
        
        return pd.Series(sentiment_scores)
    
    # Apply to DataFrame
    df_sentiment = df.withColumn("sentiment", 
        classify_text_udf(col("text")))
    """)


def example5_distributed_gpu_inference(spark):
    """
    EXAMPLE 5: Distributed GPU Inference
    ====================================
    
    Scale across multiple GPUs and nodes.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Distributed GPU Inference")
    print("=" * 70)
    
    print("""
    🌐 DISTRIBUTED GPU ARCHITECTURE:
    
    ┌──────────────────────────────────────────────────────────┐
    │               SPARK CLUSTER (4 Nodes)                    │
    ├──────────────────────────────────────────────────────────┤
    │                                                          │
    │  Node 1 (p3.8xlarge - 4× V100 GPUs)                    │
    │  ┌──────────────────────────────────────────────┐       │
    │  │ Executor 1: GPU 0 (processes partition 1)    │       │
    │  │ Executor 2: GPU 1 (processes partition 2)    │       │
    │  │ Executor 3: GPU 2 (processes partition 3)    │       │
    │  │ Executor 4: GPU 3 (processes partition 4)    │       │
    │  └──────────────────────────────────────────────┘       │
    │                                                          │
    │  Node 2-4: Same configuration                           │
    │                                                          │
    │  Total: 16 GPUs processing in parallel!                 │
    │                                                          │
    └──────────────────────────────────────────────────────────┘
    
    📊 CONFIGURATION:
    
    spark = SparkSession.builder \\
        .appName("Distributed_GPU_Inference") \\
        .config("spark.executor.instances", "16") \\
        .config("spark.executor.cores", "1") \\
        .config("spark.executor.memory", "16g") \\
        .config("spark.executor.resource.gpu.amount", "1") \\
        .config("spark.task.resource.gpu.amount", "1") \\
        .getOrCreate()
    
    💡 KEY PRINCIPLES:
    
    1. One GPU per Executor
       • Avoid contention
       • Maximize utilization
    
    2. Partition Data by GPU Count
       • df.repartition(16)  # Match GPU count
       • Even distribution
    
    3. Load Model Once per Executor
       • Use global variable
       • Avoid reloading overhead
    
    4. Batch Processing
       • Process multiple samples per task
       • Amortize transfer overhead
    
    5. Monitor GPU Usage
       • nvidia-smi on each node
       • Target > 80% utilization
    
    📈 PERFORMANCE SCALING:
    
    ┌────────┬──────────┬────────────┬─────────────┐
    │ GPUs   │ Time     │ Throughput │ Cost/hour   │
    ├────────┼──────────┼────────────┼─────────────┤
    │ 1      │ 100 min  │ 100K/sec   │ $3.06       │
    │ 4      │ 25 min   │ 400K/sec   │ $12.24      │
    │ 8      │ 12.5 min │ 800K/sec   │ $24.48      │
    │ 16     │ 6.25 min │ 1.6M/sec   │ $48.96      │
    └────────┴──────────┴────────────┴─────────────┘
    
    🎯 LINEAR SCALING!
    Doubling GPUs = Half the time
    """)


def show_ml_gpu_best_practices():
    """Best practices for GPU ML inference."""
    print("\n" + "=" * 70)
    print("GPU ML INFERENCE BEST PRACTICES")
    print("=" * 70)
    
    print("""
    🎯 BEST PRACTICES:
    
    1. MODEL LOADING
       ✅ Load once per executor (global variable)
       ✅ Use .eval() mode for inference
       ✅ Disable gradient computation (torch.no_grad())
       ❌ Don't reload model for each batch
    
    2. BATCH PROCESSING
       ✅ Larger batches = better GPU utilization
       ✅ Typical batch sizes: 64-256 (images), 16-64 (text)
       ✅ Adjust based on GPU memory
       ❌ Don't process one sample at a time
    
    3. MEMORY MANAGEMENT
       ✅ Monitor GPU memory (nvidia-smi)
       ✅ Clear cache: torch.cuda.empty_cache()
       ✅ Use mixed precision (float16) for 2x speedup
       ❌ Don't exceed GPU RAM (causes OOM)
    
    4. DATA TRANSFER
       ✅ Minimize CPU ↔ GPU transfers
       ✅ Keep data on GPU between operations
       ✅ Use pinned memory for faster transfers
       ❌ Don't transfer back and forth unnecessarily
    
    5. DISTRIBUTED INFERENCE
       ✅ One GPU per executor
       ✅ Partition data by GPU count
       ✅ Monitor all GPUs with nvidia-smi
       ❌ Don't oversubscribe GPUs
    
    6. OPTIMIZATION
       ✅ Use TensorRT for 2-5x speedup
       ✅ Quantization (int8) for 4x speedup
       ✅ Mixed precision (float16) for 2x speedup
       ✅ Batch normalization folding
    
    📊 PERFORMANCE CHECKLIST:
    
    ✅ GPU utilization > 80% (check nvidia-smi)
    ✅ Batch size maximized (but fits in memory)
    ✅ Model in .eval() mode
    ✅ Using torch.no_grad()
    ✅ Data parallelism across GPUs
    ✅ Monitoring memory usage
    ✅ Profiling bottlenecks
    
    🚀 OPTIMIZATION TECHNIQUES:
    
    1. TensorRT Conversion:
       import torch_tensorrt
       trt_model = torch_tensorrt.compile(model, ...)
       # 2-5x faster!
    
    2. Mixed Precision:
       with torch.cuda.amp.autocast():
           output = model(input)
       # 2x faster!
    
    3. Quantization:
       quantized_model = torch.quantization.quantize_dynamic(
           model, {nn.Linear}, dtype=torch.qint8
       )
       # 4x faster!
    
    4. Batch Size Tuning:
       # Find optimal batch size
       for batch_size in [16, 32, 64, 128, 256]:
           measure_throughput(batch_size)
       # Pick the best!
    
    💰 COST OPTIMIZATION:
    
    • Use spot instances (70% cheaper)
    • Right-size GPU (don't over-provision)
    • Batch jobs to maximize utilization
    • Consider g5.xlarge ($1.01/hour vs $3.06)
    • Auto-scale based on workload
    """)


def main():
    """Run all GPU ML inference examples."""
    spark = create_spark()
    
    print("🎮 GPU DEEP LEARNING INFERENCE")
    print("=" * 70)
    print("\nGPU-accelerated neural network inference at scale!")
    
    # Check PyTorch and GPU availability
    if TORCH_AVAILABLE:
        print(f"\n✅ PyTorch available: {torch.__version__}")
        print(f"✅ CUDA available: {GPU_AVAILABLE}")
        if GPU_AVAILABLE:
            print(f"   GPU: {torch.cuda.get_device_name(0)}")
    else:
        print("\n⚠️  PyTorch not available")
        print("   Install: pip install torch")
    
    # Run examples
    example1_simple_pytorch_inference(spark)
    example2_batch_inference_pattern(spark)
    example3_image_classification(spark)
    example4_text_classification(spark)
    example5_distributed_gpu_inference(spark)
    
    # Show best practices
    show_ml_gpu_best_practices()
    
    print("\n" + "=" * 70)
    print("✅ GPU ML INFERENCE EXAMPLES COMPLETE")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. 30-100x speedup for neural networks")
    print("   2. Load model once per executor")
    print("   3. Batch processing crucial")
    print("   4. Monitor GPU utilization (> 80%)")
    print("   5. Distributed inference scales linearly")
    print("   6. TensorRT/quantization for 2-5x extra speedup")
    
    print("\n📚 See Also:")
    print("   • 02_when_gpu_wins.py - GPU performance benefits")
    print("   • 03_hybrid_cpu_gpu.py - Hybrid CPU+GPU patterns")
    print("   • 04_rapids_cudf_example.py - GPU DataFrames")
    
    print("\n🔗 Resources:")
    print("   • PyTorch: https://pytorch.org")
    print("   • TensorFlow: https://www.tensorflow.org")
    print("   • TensorRT: https://developer.nvidia.com/tensorrt")
    
    spark.stop()


if __name__ == "__main__":
    main()
