# GPU-Accelerated PySpark UDFs - Quick Start 🎮

This guide shows how to use GPUs on each cluster node for distributed deep learning with PySpark.

## 🎯 What is GPU Acceleration?

Instead of using CPUs on each worker node, you can use GPUs (Graphics Processing Units) which are 10-100x faster for:
- Deep learning inference
- Image/video processing
- Large matrix operations
- Neural network computations

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────┐
│              DRIVER NODE (CPU)                  │
│         Coordinates work distribution           │
└─────────────────────────────────────────────────┘
                     |
     ┌───────────────┼───────────────┐
     ▼               ▼               ▼
┌──────────┐  ┌──────────┐  ┌──────────┐
│ WORKER 1 │  │ WORKER 2 │  │ WORKER 3 │
│ ┌──────┐ │  │ ┌──────┐ │  │ ┌──────┐ │
│ │ GPU  │ │  │ │ GPU  │ │  │ │ GPU  │ │
│ │(CUDA)│ │  │ │(CUDA)│ │  │ │(CUDA)│ │
│ └──────┘ │  │ └──────┘ │  │ └──────┘ │
│ Process  │  │ Process  │  │ Process  │
│ 1000 imgs│  │ 1000 imgs│  │ 1000 imgs│
└──────────┘  └──────────┘  └──────────┘

Each GPU processes its partition independently!
```

## 🚀 Quick Example: Image Classification

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import ArrayType, FloatType
import pandas as pd
import torch
import torchvision.models as models

# 1. Create Spark session with GPU config
spark = SparkSession.builder \
    .appName("GPUInference") \
    .config("spark.task.resource.gpu.amount", "1") \
    .config("spark.executor.resource.gpu.amount", "1") \
    .getOrCreate()

# 2. Define GPU UDF (runs on each executor's GPU)
@pandas_udf(ArrayType(FloatType()))
def classify_images_gpu(image_paths: pd.Series) -> pd.Series:
    """Classify images using ResNet50 on GPU."""
    
    # Load model on GPU (once per executor)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = models.resnet50(pretrained=True).to(device)
    model.eval()
    
    results = []
    with torch.no_grad():
        for img_path in image_paths:
            # Load image and run inference on GPU
            img_tensor = load_and_preprocess(img_path).to(device)
            output = model(img_tensor)
            results.append(output.cpu().numpy().tolist())
    
    return pd.Series(results)

# 3. Apply to dataset (distributed across GPUs)
images_df = spark.read.parquet("s3://bucket/images/")
classified = images_df.withColumn(
    "predictions",
    classify_images_gpu(col("image_path"))
)

classified.write.parquet("s3://bucket/results/")
```

## ⚡ Performance Comparison

| Task | CPU (16 cores) | GPU (1x A100) | Speedup |
|------|----------------|---------------|---------|
| ResNet50 inference | 100 img/sec | 2000 img/sec | 20x |
| BERT embeddings | 50 text/sec | 1000 text/sec | 20x |
| YOLOv8 detection | 20 frames/sec | 500 frames/sec | 25x |
| Matrix operations | 500ms | 5ms | 100x |

**10 GPUs in cluster = 20,000 images/sec!** 🚀

## 🔧 Configuration

### For YARN Cluster

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
    gpu_inference.py
```

### For Kubernetes Cluster

```bash
spark-submit \
    --master k8s://https://kubernetes-api:443 \
    --deploy-mode cluster \
    --name gpu-inference \
    --conf spark.kubernetes.container.image=nvidia/cuda:11.8.0-runtime-ubuntu22.04 \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.executor.resource.gpu.vendor=nvidia.com/gpu \
    --conf spark.task.resource.gpu.amount=1 \
    gpu_inference.py
```

## 📋 Prerequisites

### 1. Install GPU Libraries

```bash
# PyTorch with CUDA
pip install torch torchvision --index-url https://download.pytorch.org/whl/cu118

# TensorFlow with GPU
pip install tensorflow-gpu

# CuPy for custom CUDA
pip install cupy-cuda11x

# Deep learning models
pip install ultralytics transformers
```

### 2. Verify GPU Availability

```python
import torch
print(f"CUDA available: {torch.cuda.is_available()}")
print(f"GPU count: {torch.cuda.device_count()}")
print(f"GPU name: {torch.cuda.get_device_name(0)}")
```

### 3. Enable Arrow for Fast Data Transfer

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "128")
```

## 💡 Best Practices

### ✅ DO

1. **Use Pandas UDF** (automatic batching)
   ```python
   @pandas_udf(return_type)
   def gpu_udf(batch: pd.Series) -> pd.Series:
       # Process batch on GPU
       pass
   ```

2. **Load model once per executor**
   ```python
   _model = None
   def get_model():
       global _model
       if _model is None:
           _model = load_model().cuda()
       return _model
   ```

3. **Set optimal batch size** (32-128)
   ```python
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "128")
   ```

4. **Clean GPU memory between batches**
   ```python
   torch.cuda.empty_cache()
   ```

### ❌ DON'T

1. **Don't load model for every row** (extremely slow!)
2. **Don't use too large batch size** (GPU OOM)
3. **Don't forget to move tensors to GPU**
   ```python
   tensor = tensor.to(device)  # Essential!
   ```

## 🎓 Use Cases

| Use Case | Model | Speedup | Example |
|----------|-------|---------|---------|
| Image classification | ResNet50 | 20x | Product categorization |
| Object detection | YOLOv8 | 25x | Video surveillance |
| Text embeddings | BERT | 20x | Semantic search |
| Face recognition | FaceNet | 30x | Security systems |
| Image segmentation | U-Net | 15x | Medical imaging |

## 📊 Real-World Example

**Task**: Classify 10 million product images

**CPU Cluster** (10 nodes, 16 cores each):
- Speed: 1,000 images/sec
- Time: 2.8 hours
- Cost: $5/hour × 2.8 = $14

**GPU Cluster** (10 nodes, 1 A100 each):
- Speed: 20,000 images/sec
- Time: 8.3 minutes
- Cost: $12/hour × 0.14 = $1.68

**Result**: 20x faster, 88% cheaper! 🎉

## 🔍 Monitoring

```bash
# Check GPU utilization on workers
nvidia-smi dmon -s u

# Monitor GPU memory
nvidia-smi dmon -s m

# Full GPU dashboard
watch -n 1 nvidia-smi
```

## 📚 Framework Comparison

| Framework | Best For | Setup Complexity |
|-----------|----------|------------------|
| **PyTorch** | Computer vision, custom models | Easy |
| **TensorFlow** | Production, TensorFlow Hub | Medium |
| **CuPy** | Custom CUDA, matrix ops | Easy |
| **Hugging Face** | NLP, transformers | Easy |
| **RAPIDS** | DataFrame operations on GPU | Hard |

## 🚀 Getting Started

1. **Run the example**:
   ```bash
   python src/cluster_computing/06_gpu_accelerated_udfs.py
   ```

2. **Adapt for your use case**:
   - Replace model (ResNet50 → your model)
   - Adjust batch size for your GPU memory
   - Configure cluster with GPU resources

3. **Test locally first** (with `master("local[*]")`)

4. **Deploy to cluster** (YARN/Kubernetes)

## 💰 Cost Optimization

**When to use GPU**:
- ✅ Deep learning (ResNet, BERT, YOLO)
- ✅ Processing > 10,000 items
- ✅ Complex neural networks
- ✅ Time-sensitive workloads

**When to use CPU**:
- ✅ Simple ETL
- ✅ Small datasets (< 1,000 items)
- ✅ String operations
- ✅ Cost-sensitive batch jobs

## 🎯 Next Steps

1. Read full example: `06_gpu_accelerated_udfs.py`
2. Check cluster setup: `01_cluster_setup.py`
3. Understand partitioning: `02_data_partitioning.py`
4. Learn fault tolerance: `05_fault_tolerance.py`

---

**Need help?** Check the main README.md or open an issue!

🎮 **Happy GPU computing!** 🚀
