# GPU vs CPU Decision Matrix for PySpark

## Executive Summary

This guide helps you decide when to use GPU vs CPU for PySpark workloads, understand PySpark defaults, and configure GPU acceleration.

---

## 🎯 Quick Decision Matrix

| Workload Type | Recommended | Speedup | Cost-Effective |
|---------------|-------------|---------|----------------|
| **Deep Learning Inference** | GPU | 10-100x | ✅ Yes |
| **Image/Video Processing** | GPU | 20-50x | ✅ Yes |
| **Large Matrix Operations** | GPU | 50-100x | ✅ Yes |
| **Batch ML Inference (>32 items)** | GPU | 10-30x | ✅ Yes |
| **Text Embeddings (BERT, etc.)** | GPU | 15-25x | ✅ Yes |
| **Simple Aggregations** | CPU | 1x | ✅ Yes |
| **String Operations** | CPU | 1x | ✅ Yes |
| **ETL Transformations** | CPU | 1x | ✅ Yes |
| **Small Datasets (<1K rows)** | CPU | 1x | ✅ Yes |
| **Single-Item Processing** | CPU | 1x | ✅ Yes |

---

## 🔥 When to Use GPU

### ✅ Ideal GPU Workloads

#### 1. **Deep Learning Inference**
- **Models**: ResNet, BERT, YOLO, GPT, Vision Transformers
- **Speedup**: 20-100x
- **Why**: Parallel matrix operations, optimized for neural networks
- **Example**:
  ```python
  # Image classification with ResNet50
  # CPU: 100 images/sec
  # GPU: 2000 images/sec (20x speedup)
  ```

#### 2. **Image & Video Processing**
- **Tasks**: Object detection, face recognition, segmentation, frame analysis
- **Speedup**: 20-50x
- **Why**: Parallel pixel processing, convolution operations
- **Example**:
  ```python
  # Video frame analysis with YOLOv8
  # CPU: 20 frames/sec per node
  # GPU: 500 frames/sec per node (25x speedup)
  ```

#### 3. **Large Matrix Operations**
- **Tasks**: Matrix multiplication, SVD, eigenvalue decomposition
- **Speedup**: 50-100x
- **Why**: GPUs designed for parallel matrix math (thousands of cores)
- **Example**:
  ```python
  # 1000x1000 matrix multiplication
  # CPU (NumPy): 500ms
  # GPU (CuPy): 5ms (100x speedup)
  ```

#### 4. **Batch Processing (>32 items)**
- **Tasks**: Batch inference, feature extraction, embeddings
- **Speedup**: 10-30x
- **Why**: GPUs most efficient with batched operations
- **Batch Size Impact**:
  ```
  Batch Size    GPU Utilization    Throughput
  ----------    ---------------    ----------
  1             50%                100/sec
  8             70%                600/sec
  32            90%                2000/sec
  128           95%                5000/sec  ← Optimal
  ```

#### 5. **Text Embeddings & NLP**
- **Models**: BERT, Sentence Transformers, GPT embeddings
- **Speedup**: 15-25x
- **Why**: Transformer attention mechanisms highly parallelizable
- **Example**:
  ```python
  # Generate embeddings with BERT
  # CPU: 50 texts/sec
  # GPU: 1000 texts/sec (20x speedup)
  ```

### 📊 GPU Performance Characteristics

| Characteristic | GPU Advantage |
|----------------|---------------|
| **Throughput** | 10-100x higher for parallel tasks |
| **Latency** | Slightly higher per item (batching overhead) |
| **Memory** | 16-80 GB per GPU (limited vs system RAM) |
| **Parallelism** | Thousands of cores (vs 8-64 CPU cores) |
| **Cost** | Higher per hour, but much higher throughput/$ |

---

## 💻 When to Use CPU

### ✅ Ideal CPU Workloads

#### 1. **Simple Aggregations**
- **Operations**: `count()`, `sum()`, `avg()`, `min()`, `max()`
- **Why**: Already optimized, minimal computation per row
- **Example**:
  ```python
  df.groupBy("category").agg(count("*"), avg("price"))
  # CPU is sufficient - already very fast
  ```

#### 2. **String Operations**
- **Operations**: Substring, regex, concat, split, case conversion
- **Why**: Sequential operations, not parallelizable
- **Example**:
  ```python
  df.withColumn("upper_name", upper(col("name")))
  # CPU handles string ops efficiently
  ```

#### 3. **ETL Transformations**
- **Operations**: Filter, select, join, union, drop duplicates
- **Why**: I/O bound, not compute bound
- **Example**:
  ```python
  df.filter(col("age") > 18).select("name", "email")
  # GPU overhead not worth it for simple filters
  ```

#### 4. **Small Datasets (<1K rows)**
- **Why**: GPU initialization overhead > computation time
- **Threshold**: Use GPU when dataset > 1000 rows and compute-heavy
- **Example**:
  ```python
  # 100 images: Use CPU (GPU overhead too high)
  # 100,000 images: Use GPU (computation >> overhead)
  ```

#### 5. **Single-Item Processing**
- **Why**: GPUs need batches to be efficient
- **Example**:
  ```python
  # Real-time API: 1 request at a time → Use CPU
  # Batch job: 1000 requests at once → Use GPU
  ```

### 📊 CPU Performance Characteristics

| Characteristic | CPU Advantage |
|----------------|---------------|
| **Low Latency** | Better for single-item processing |
| **Large Memory** | 128-512 GB system RAM |
| **No Overhead** | No GPU initialization, data transfer |
| **Sequential Tasks** | Better for non-parallel work |
| **Cost** | Lower per hour for simple workloads |

---

## ⚙️ PySpark Default Behavior

### 🔵 CPU is Default

**PySpark defaults to CPU for all operations** unless explicitly configured to use GPU.

#### Why CPU Default?
1. **Universal availability**: All clusters have CPUs
2. **Simplicity**: No special hardware or drivers required
3. **Cost**: CPUs are cheaper for most workloads
4. **Compatibility**: Works everywhere (local, YARN, Kubernetes, cloud)

### 🎮 GPU Must Be Explicitly Enabled

GPUs require:
1. ✅ GPU hardware (NVIDIA, AMD, etc.)
2. ✅ GPU drivers installed
3. ✅ CUDA/ROCm libraries
4. ✅ Spark configuration for GPU resources
5. ✅ GPU-enabled Python libraries (PyTorch, TensorFlow, CuPy)

---

## 🔧 How to Configure PySpark for GPU

### Option 1: YARN Cluster

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 10 \
    --executor-cores 4 \
    --executor-memory 16g \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=1 \
    --conf spark.executor.resource.gpu.discoveryScript=/path/to/getGpusResources.sh \
    --files /path/to/getGpusResources.sh \
    your_script.py
```

**Key Configs**:
- `spark.executor.resource.gpu.amount=1`: Each executor gets 1 GPU
- `spark.task.resource.gpu.amount=1`: Each task uses 1 GPU
- `discoveryScript`: Script to detect GPUs on nodes

### Option 2: Kubernetes Cluster

```bash
spark-submit \
    --master k8s://https://kubernetes-api:443 \
    --deploy-mode cluster \
    --name gpu-inference \
    --conf spark.kubernetes.container.image=nvidia/cuda:11.8.0-runtime-ubuntu22.04 \
    --conf spark.kubernetes.executor.request.cores=4 \
    --conf spark.executor.memory=16g \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.executor.resource.gpu.vendor=nvidia.com/gpu \
    --conf spark.task.resource.gpu.amount=1 \
    your_script.py
```

**Key Configs**:
- `spark.kubernetes.container.image`: CUDA-enabled Docker image
- `spark.executor.resource.gpu.vendor`: Kubernetes GPU resource name
- `spark.executor.resource.gpu.amount=1`: Request 1 GPU per executor

### Option 3: Standalone Cluster

```bash
spark-submit \
    --master spark://master:7077 \
    --deploy-mode cluster \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=1 \
    --conf spark.worker.resource.gpu.amount=1 \
    --conf spark.worker.resource.gpu.discoveryScript=/path/to/getGpusResources.sh \
    your_script.py
```

### Option 4: SparkSession Configuration

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("GPUAcceleratedApp") \
    .config("spark.executor.resource.gpu.amount", "1") \
    .config("spark.task.resource.gpu.amount", "1") \
    .getOrCreate()
```

---

## 📋 GPU Configuration Reference

### Essential Spark Configs

| Config | Purpose | Default | Recommended |
|--------|---------|---------|-------------|
| `spark.executor.resource.gpu.amount` | GPUs per executor | 0 | 1 |
| `spark.task.resource.gpu.amount` | GPUs per task | 0 | 1 |
| `spark.executor.resource.gpu.vendor` | GPU resource name (K8s) | - | `nvidia.com/gpu` |
| `spark.executor.resource.gpu.discoveryScript` | GPU detection script | - | `/path/to/script.sh` |
| `spark.rapids.sql.enabled` | RAPIDS accelerator | false | true (optional) |

### GPU Discovery Script (getGpusResources.sh)

```bash
#!/bin/bash
# getGpusResources.sh - Detect GPUs for Spark

nvidia-smi --query-gpu=index --format=csv,noheader | \
awk '{print "{\"name\": \"gpu\", \"addresses\": [\""$1"\"]}"}'
```

Make executable:
```bash
chmod +x getGpusResources.sh
```

---

## 🎓 Implementation Guide

### Step 1: Write GPU UDF

```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
import torch

@pandas_udf(ArrayType(FloatType()))
def gpu_inference_udf(features: pd.Series) -> pd.Series:
    """Run inference on GPU."""
    
    # Initialize model on GPU (once per executor)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = load_model().to(device)
    model.eval()
    
    results = []
    batch_size = 128  # Optimal for most GPUs
    
    # Process in batches
    features_list = features.tolist()
    for i in range(0, len(features_list), batch_size):
        batch = features_list[i:i + batch_size]
        batch_tensor = torch.tensor(batch).to(device)
        
        with torch.no_grad():
            outputs = model(batch_tensor)
        
        results.extend(outputs.cpu().numpy().tolist())
    
    return pd.Series(results)
```

### Step 2: Apply UDF to DataFrame

```python
# Apply GPU UDF
result_df = df.withColumn(
    "predictions",
    gpu_inference_udf(col("features"))
)

result_df.write.parquet("s3://output/predictions/")
```

### Step 3: Submit Job

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.task.resource.gpu.amount=1 \
    gpu_inference_job.py
```

---

## 💰 Cost Analysis

### Example: Process 1M Images

#### CPU Cluster
- **Setup**: 10 nodes × 16 cores = 160 cores
- **Throughput**: 100 images/sec/node = 1,000 images/sec
- **Time**: 1,000,000 / 1,000 = 1,000 seconds (16.7 minutes)
- **Cost**: $5/hour × 10 nodes × 0.28 hours = **$14.00**

#### GPU Cluster
- **Setup**: 10 nodes × 1 GPU (A100) = 10 GPUs
- **Throughput**: 2,000 images/sec/node = 20,000 images/sec
- **Time**: 1,000,000 / 20,000 = 50 seconds (0.8 minutes)
- **Cost**: $12/hour × 10 nodes × 0.014 hours = **$1.68**

**Result**: GPU cluster is **20x faster** and **8.3x cheaper**! 🚀

### When GPU Wins on Cost

GPU is more cost-effective when:
- ✅ Compute-bound workloads (not I/O bound)
- ✅ Large batches (>1000 items)
- ✅ Complex models (neural networks, not linear regression)
- ✅ Frequent jobs (amortize setup cost)

---

## 🔍 Monitoring GPU Usage

### Check GPU Utilization

```bash
# Watch GPU usage in real-time
watch -n 1 nvidia-smi

# Monitor GPU utilization percentage
nvidia-smi dmon -s u

# Monitor GPU memory
nvidia-smi dmon -s m
```

### Expected Utilization

| Scenario | GPU Utilization | Issue |
|----------|----------------|-------|
| Optimal batch inference | 90-100% | None |
| Small batches | 50-70% | Increase batch size |
| Single-item processing | 10-30% | Use batching |
| Idle | 0% | GPU not being used |

---

## 🚨 Common Pitfalls

### ❌ Mistake 1: Loading Model Per Row

**Wrong**:
```python
def slow_udf(feature):
    model = load_model().cuda()  # Loads EVERY row!
    return model(feature)
```

**Right**:
```python
_model = None
def get_model():
    global _model
    if _model is None:
        _model = load_model().cuda()  # Loads ONCE
    return _model

@pandas_udf(...)
def fast_udf(features: pd.Series) -> pd.Series:
    model = get_model()
    # Process batch
```

### ❌ Mistake 2: Not Using Batching

**Wrong**:
```python
for row in df.collect():
    result = model(row)  # One at a time
```

**Right**:
```python
@pandas_udf(...)  # Automatically batches!
def batch_inference(features: pd.Series) -> pd.Series:
    # Process entire batch at once
```

### ❌ Mistake 3: Batch Size Too Large

**Wrong**:
```python
batch_size = 1024  # May cause GPU OOM
```

**Right**:
```python
# Start with 128 and tune based on GPU memory
batch_size = 128  # Works for most GPUs
```

### ❌ Mistake 4: Forgetting GPU Memory Cleanup

**Wrong**:
```python
# Old tensors accumulate in GPU memory
for batch in batches:
    output = model(batch)
```

**Right**:
```python
for batch in batches:
    output = model(batch)
    torch.cuda.empty_cache()  # Clean up
```

---

## 📚 Required Dependencies

### PyTorch (NVIDIA GPU)
```bash
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118
```

### TensorFlow (NVIDIA GPU)
```bash
pip install tensorflow[and-cuda]
```

### CuPy (CUDA operations)
```bash
pip install cupy-cuda11x  # Match your CUDA version
```

### RAPIDS (GPU-accelerated DataFrame operations)
```bash
conda install -c rapidsai -c conda-forge -c nvidia \
    rapids=24.02 python=3.10 cudatoolkit=11.8
```

---

## 🎯 Decision Flowchart

```
Start
  │
  ├─ Is it deep learning inference?
  │    Yes → GPU (10-100x speedup)
  │    No → Continue
  │
  ├─ Is it image/video processing?
  │    Yes → GPU (20-50x speedup)
  │    No → Continue
  │
  ├─ Is it large matrix operations?
  │    Yes → GPU (50-100x speedup)
  │    No → Continue
  │
  ├─ Is batch size > 32?
  │    No → CPU (GPU overhead too high)
  │    Yes → Continue
  │
  ├─ Is dataset > 1000 rows?
  │    No → CPU (too small for GPU)
  │    Yes → Continue
  │
  ├─ Is it compute-bound (not I/O)?
  │    No → CPU (I/O is bottleneck)
  │    Yes → GPU (worth the speedup)
  │
  └─ Simple aggregation/strings?
       Yes → CPU (already fast)
       No → GPU (complex computation)
```

---

## 📖 Additional Resources

- **Official Spark GPU Docs**: https://spark.apache.org/docs/latest/configuration.html#custom-resource-scheduling-and-configuration-overview
- **RAPIDS AI**: https://rapids.ai/
- **PyTorch GPU Guide**: https://pytorch.org/docs/stable/notes/cuda.html
- **TensorFlow GPU Guide**: https://www.tensorflow.org/guide/gpu

---

## ✅ Summary

### Use GPU When:
1. ✅ Deep learning inference (ResNet, BERT, YOLO)
2. ✅ Image/video processing
3. ✅ Large matrix operations
4. ✅ Batch size > 32
5. ✅ Dataset > 1K rows
6. ✅ Compute-bound workload

### Use CPU When:
1. ✅ Simple aggregations (count, sum, avg)
2. ✅ String operations
3. ✅ ETL transformations
4. ✅ Small datasets (<1K rows)
5. ✅ Single-item processing
6. ✅ I/O-bound workloads

### Key Takeaway:
**PySpark defaults to CPU. Enable GPU for compute-heavy, parallel workloads with batches >32 items. GPU provides 10-100x speedup for deep learning but CPU is fine for simple operations.**
