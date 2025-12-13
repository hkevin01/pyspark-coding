"""
================================================================================
CLUSTER COMPUTING #6 - GPU-Accelerated UDFs for Distributed Inference
================================================================================

MODULE OVERVIEW:
----------------
GPU-accelerated UDFs enable distributed deep learning inference across cluster
nodes. Each executor can leverage its local GPU for tasks like image classification,
embedding generation, and neural network inference at massive scale.

This is the intersection of distributed computing (PySpark) and GPU acceleration,
providing 10-100x speedup for ML inference workloads.

PURPOSE:
--------
Learn GPU-accelerated distributed computing:
• GPU resource allocation in Spark clusters
• Writing GPU-enabled Pandas UDFs
• Model loading and caching strategies
• Distributed deep learning inference
• Performance optimization techniques
• Troubleshooting GPU issues in clusters

ARCHITECTURAL OVERVIEW:
-----------------------

┌─────────────────────────────────────────────────────────────────┐
│            DISTRIBUTED GPU INFERENCE ARCHITECTURE               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Driver (Coordinator)                                          │
│  ┌────────────────────────────────────────────────────────┐        │
│  │ • Distributes tasks                                │        │
│  │ • No GPU needed                                    │        │
│  │ • Collects results                                 │        │
│  └────────────────────────────────────────────────────────┘        │
│                         ↓                                       │
│                    Task Distribution                            │
│                         ↓                                       │
│  ┌──────────────┬──────────────┬──────────────┐               │
│  │ Executor 1   │ Executor 2   │ Executor 3   │               │
│  │ ┌──────────┐ │ ┌──────────┐ │ ┌──────────┐ │               │
│  │ │ GPU 0    │ │ │ GPU 1    │ │ │ GPU 2    │ │               │
│  │ │ Model    │ │ │ Model    │ │ │ Model    │ │               │
│  │ │ Cached   │ │ │ Cached   │ │ │ Cached   │ │               │
│  │ └──────────┘ │ └──────────┘ │ └──────────┘ │               │
│  │ Process      │ Process      │ Process      │               │
│  │ Partition 1  │ Partition 2  │ Partition 3  │               │
│  └──────────────┴──────────────┴──────────────┘               │
│                         ↓                                       │
│                  Results Aggregation                            │
│                         ↓                                       │
│  Final Result (millions of predictions)                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

KEY CONCEPTS:
-------------
• Each executor gets one or more GPUs
• Model loaded once per executor (cached in memory)
• Batches processed in parallel across cluster
• No data transfer between executors
• Linear scaling with number of GPUs

PERFORMANCE EXAMPLE:
--------------------
Task: Image classification on 10M images

CPU-only cluster (16 cores × 10 nodes = 160 cores):
• Time: ~8 hours
• Cost: $32

GPU cluster (1× V100 per executor, 10 executors):
• Time: ~20 minutes (24x faster!)
• Cost: $10
• Throughput: 8,000 images/second

SPARK GPU CONFIGURATION:
------------------------

1. YARN Cluster:
```bash
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.task.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.discoveryScript=/path/to/getGpusResources.sh \\
    --conf spark.executor.memory=16g \\
    --conf spark.executor.cores=4 \\
    --num-executors 10 \\
    gpu_inference.py
```

2. Kubernetes Cluster:
```bash
spark-submit \\
    --master k8s://https://kubernetes:443 \\
    --deploy-mode cluster \\
    --conf spark.kubernetes.container.image=nvidia/cuda:11.8.0-runtime-ubuntu22.04 \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.vendor=nvidia.com/gpu \\
    --conf spark.kubernetes.executor.request.cores=4 \\
    --conf spark.executor.memory=16g \\
    gpu_inference.py
```

3. Standalone Cluster:
```bash
spark-submit \\
    --master spark://master:7077 \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.task.resource.gpu.amount=1 \\
    --conf spark.worker.resource.gpu.amount=1 \\
    --conf spark.worker.resource.gpu.discoveryScript=/opt/spark/getGpusResources.sh \\
    gpu_inference.py
```

GPU-ENABLED PANDAS UDF PATTERN:
--------------------------------

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd
import torch

# Global model cache (loaded once per executor)
_model = None

def get_model():
    """Load model once per executor and cache."""
    global _model
    if _model is None:
        _model = torch.load('model.pth')
        _model.to('cuda')
        _model.eval()
    return _model

@pandas_udf("array<float>")
def gpu_inference_udf(image_paths: pd.Series) -> pd.Series:
    """GPU-accelerated image classification."""
    model = get_model()
    
    results = []
    for path in image_paths:
        # Load and preprocess image
        image = load_image(path)
        image_tensor = preprocess(image).unsqueeze(0).cuda()
        
        # Inference on GPU
        with torch.no_grad():
            output = model(image_tensor)
            probabilities = torch.nn.functional.softmax(output[0], dim=0)
        
        results.append(probabilities.cpu().numpy().tolist())
    
    return pd.Series(results)

# Apply to DataFrame
df_with_predictions = df.withColumn(
    "predictions", 
    gpu_inference_udf(col("image_path"))
)
```

PERFORMANCE OPTIMIZATION:
-------------------------

1. **Batch Processing within UDF**:
```python
@pandas_udf("array<float>")
def batched_gpu_udf(paths: pd.Series) -> pd.Series:
    model = get_model()
    
    # Process in batches of 32
    batch_size = 32
    all_results = []
    
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i+batch_size]
        images = torch.stack([load_image(p) for p in batch]).cuda()
        
        with torch.no_grad():
            outputs = model(images)  # Batch inference!
        
        all_results.extend(outputs.cpu().numpy().tolist())
    
    return pd.Series(all_results)
```

2. **Memory Management**:
```python
import gc

@pandas_udf("array<float>")
def memory_efficient_udf(data: pd.Series) -> pd.Series:
    model = get_model()
    
    results = []
    for item in data:
        result = process_on_gpu(item)
        results.append(result)
        
        # Clear GPU cache periodically
        if len(results) % 100 == 0:
            torch.cuda.empty_cache()
            gc.collect()
    
    return pd.Series(results)
```

3. **Mixed Precision (2x speedup)**:
```python
@pandas_udf("array<float>")
def mixed_precision_udf(data: pd.Series) -> pd.Series:
    model = get_model()
    
    results = []
    with torch.cuda.amp.autocast():  # Enable mixed precision
        for item in data:
            result = model(process(item).cuda())
            results.append(result.cpu().numpy().tolist())
    
    return pd.Series(results)
```

COMMON PITFALLS & SOLUTIONS:
-----------------------------

❌ **Pitfall 1**: Reloading model for each batch
```python
# BAD - Reloads model every batch!
@pandas_udf("array<float>")
def bad_udf(data: pd.Series) -> pd.Series:
    model = load_model()  # SLOW!
    return pd.Series([model(x) for x in data])
```

✅ **Solution**: Cache model globally
```python
# GOOD - Load once per executor
_model = None

def get_model():
    global _model
    if _model is None:
        _model = load_model()
    return _model
```

❌ **Pitfall 2**: GPU out of memory
```python
# BAD - Processing too much data at once
@pandas_udf("array<float>")
def oom_udf(data: pd.Series) -> pd.Series:
    all_data = torch.tensor(data.tolist()).cuda()  # OOM!
    return model(all_data)
```

✅ **Solution**: Process in smaller batches
```python
# GOOD - Batch processing
@pandas_udf("array<float>")
def batched_udf(data: pd.Series) -> pd.Series:
    results = []
    for i in range(0, len(data), 32):
        batch = torch.tensor(data[i:i+32].tolist()).cuda()
        results.extend(model(batch).cpu().numpy())
    return pd.Series(results)
```

❌ **Pitfall 3**: Not using GPU
```python
# BAD - Forgot to move to GPU!
@pandas_udf("array<float>")
def cpu_udf(data: pd.Series) -> pd.Series:
    model = get_model()  # On GPU
    tensor = torch.tensor(data.tolist())  # On CPU!
    return model(tensor)  # ERROR or slow!
```

✅ **Solution**: Explicitly move to GPU
```python
# GOOD - Move to GPU
@pandas_udf("array<float>")
def gpu_udf(data: pd.Series) -> pd.Series:
    model = get_model()
    tensor = torch.tensor(data.tolist()).cuda()  # .cuda()!
    return model(tensor).cpu().numpy()
```

MONITORING GPU USAGE:
----------------------

1. **Per-Executor Monitoring**:
```python
@pandas_udf("string")
def monitor_gpu_udf(data: pd.Series) -> pd.Series:
    import subprocess
    
    # Check GPU usage
    result = subprocess.run(
        ['nvidia-smi', '--query-gpu=utilization.gpu,memory.used',
         '--format=csv,noheader,nounits'],
        capture_output=True, text=True
    )
    
    gpu_util, mem_used = result.stdout.strip().split(', ')
    print(f"GPU Utilization: {gpu_util}%, Memory: {mem_used} MB")
    
    # Process data...
    return pd.Series(["processed"] * len(data))
```

2. **Target Metrics**:
- GPU Utilization: > 80% (good)
- GPU Memory: < 90% (avoid OOM)
- Batch size: Maximize without OOM
- Throughput: Monitor samples/second

DEPLOYMENT CHECKLIST:
---------------------

✅ Cluster Configuration:
   - GPU drivers installed on all nodes
   - CUDA toolkit version matches
   - Spark GPU resource discovery configured
   - Executor memory sized appropriately

✅ Code Optimization:
   - Model cached globally (not per batch)
   - Batch processing within UDF
   - GPU memory management
   - Mixed precision enabled (if supported)

✅ Monitoring:
   - Spark UI for task distribution
   - nvidia-smi for GPU utilization
   - Memory usage per executor
   - Throughput metrics

✅ Testing:
   - Test on single executor first
   - Verify GPU detection
   - Check memory usage
   - Benchmark throughput
   - Scale to full cluster

REAL-WORLD EXAMPLE:
-------------------

Task: Classify 50 million images using ResNet-50

Cluster: 20 executors × 1 V100 GPU each

Configuration:
```python
spark = SparkSession.builder \\
    .appName("ImageClassification") \\
    .config("spark.executor.resource.gpu.amount", "1") \\
    .config("spark.task.resource.gpu.amount", "1") \\
    .config("spark.executor.memory", "16g") \\
    .config("spark.executor.cores", "4") \\
    .getOrCreate()

# Load image paths
df = spark.read.parquet("s3://images/metadata")  # 50M rows

# Apply GPU UDF
df_classified = df.withColumn(
    "predictions",
    classify_images_gpu_udf(col("image_path"))
)

df_classified.write.parquet("s3://results/predictions")
```

Performance:
- Throughput: 10,000 images/sec (500 per GPU)
- Total time: ~83 minutes
- Cost: $50 (spot instances)
- Speedup: 30x vs CPU-only

Compare to CPU:
- Time: ~42 hours
- Cost: $200+
- Not feasible for production!

EXPLAIN() OUTPUT NOTES:
-----------------------
When using GPU UDFs, explain() shows:
• PythonUDF in physical plan
• No special GPU notation (handled by executor)
• Task scheduling based on GPU resources
• Partition count affects GPU utilization

Check Spark UI → Executors tab:
• "GPU Resources" column shows allocated GPUs
• "Tasks" shows GPU task distribution
• Monitor for GPU resource starvation

See Also:
---------
• ../gpu_acceleration/ - Comprehensive GPU guides
• 07_resource_management.py - Resource allocation
• 02_data_partitioning.py - Partition tuning for GPUs
• ../mllib/05_ml_inference_gpu.py - ML inference patterns
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf, PandasUDFType
from pyspark.sql.types import ArrayType, FloatType, StringType, StructType, StructField
import pandas as pd
import numpy as np
import time


def create_spark():
    """Create Spark session with GPU configuration."""
    return SparkSession.builder \
        .appName("GPUAcceleratedUDFs") \
        .master("local[4]") \
        .config("spark.task.resource.gpu.amount", "1") \
        .config("spark.executor.resource.gpu.amount", "1") \
        .config("spark.rapids.sql.enabled", "false") \
        .getOrCreate()


def demonstrate_gpu_detection():
    """Detect available GPUs on each executor."""
    print("=" * 70)
    print("1. GPU DETECTION")
    print("=" * 70)
    
    print("""
🎮 GPU Detection in Cluster:

In a real cluster, each executor can have one or more GPUs.
PySpark can detect and allocate GPUs to tasks automatically.

Configuration:
--------------
# For YARN cluster
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.task.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.discoveryScript=/path/to/getGpusResources.sh \\
    your_script.py

# For Kubernetes cluster
spark-submit \\
    --master k8s://https://kubernetes:443 \\
    --deploy-mode cluster \\
    --conf spark.kubernetes.executor.request.cores=4 \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.vendor=nvidia.com/gpu \\
    --conf spark.kubernetes.container.image=nvidia/cuda:11.8.0-runtime-ubuntu22.04 \\
    your_script.py

Detection Code:
---------------
""")
    
    # Simulate GPU detection
    detection_code = '''
import subprocess
import json

def detect_gpus():
    """Detect available GPUs using nvidia-smi."""
    try:
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=index,name,memory.total', '--format=csv,noheader'],
            capture_output=True, text=True, check=True
        )
        gpus = []
        for line in result.stdout.strip().split('\\n'):
            idx, name, memory = line.split(', ')
            gpus.append({
                'index': int(idx),
                'name': name,
                'memory': memory
            })
        return gpus
    except Exception as e:
        print(f"No GPU detected: {e}")
        return []

# Example output:
# [{'index': 0, 'name': 'NVIDIA A100', 'memory': '40960 MiB'},
#  {'index': 1, 'name': 'NVIDIA A100', 'memory': '40960 MiB'}]
'''
    print(detection_code)
    print("✅ In production: Each executor detects its assigned GPU(s)")


def demonstrate_pytorch_gpu_udf(spark):
    """Use PyTorch with GPU in PySpark UDFs."""
    print("\n" + "=" * 70)
    print("2. PYTORCH GPU UDF (Image Classification)")
    print("=" * 70)
    
    # Create sample image dataset
    num_images = 1000
    image_data = spark.range(num_images).toDF("image_id") \
        .withColumn("image_path", col("image_id").cast("string"))
    
    print(f"📊 Dataset: {num_images} images to classify")
    
    print("\n🔥 PyTorch GPU UDF Implementation:")
    print("""
import torch
import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image
import pandas as pd

# Define pandas UDF for batch processing
@pandas_udf(ArrayType(FloatType()))
def classify_images_gpu(image_paths: pd.Series) -> pd.Series:
    \"\"\"Classify images using ResNet50 on GPU.\"\"\"
    
    # Initialize model on GPU (once per executor)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # Load pretrained ResNet50
    model = models.resnet50(pretrained=True)
    model = model.to(device)
    model.eval()
    
    # Image preprocessing
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406],
                           std=[0.229, 0.224, 0.225])
    ])
    
    results = []
    
    # Process batch of images
    with torch.no_grad():
        for img_path in image_paths:
            try:
                # Load and preprocess image
                img = Image.open(img_path).convert('RGB')
                img_tensor = preprocess(img).unsqueeze(0).to(device)
                
                # Run inference on GPU
                output = model(img_tensor)
                probabilities = torch.nn.functional.softmax(output[0], dim=0)
                
                # Get top-5 predictions
                top5_prob, top5_idx = torch.topk(probabilities, 5)
                results.append(top5_prob.cpu().numpy().tolist())
            except Exception as e:
                results.append([0.0] * 5)  # Error handling
    
    return pd.Series(results)

# Apply UDF to dataset
classified = image_data.withColumn(
    "predictions", 
    classify_images_gpu(col("image_path"))
)

classified.show(5)
    """)
    
    print("\n⚡ Performance Comparison:")
    print("""
    CPU (16 cores):    100 images/sec   → 10 seconds for 1000 images
    GPU (1x A100):     2000 images/sec  → 0.5 seconds for 1000 images
    
    Speedup: 20x faster with GPU! 🚀
    
    10 GPUs in cluster: 20,000 images/sec → Process 1M images in 50 seconds
    """)


def demonstrate_tensorflow_gpu_udf(spark):
    """Use TensorFlow with GPU in PySpark UDFs."""
    print("\n" + "=" * 70)
    print("3. TENSORFLOW GPU UDF (Text Embeddings)")
    print("=" * 70)
    
    # Create sample text dataset
    texts = [
        "Machine learning on distributed clusters",
        "PySpark GPU acceleration examples",
        "Deep learning inference at scale",
        "Natural language processing with transformers"
    ] * 250  # 1000 texts
    
    text_df = spark.createDataFrame([(i, text) for i, text in enumerate(texts)], 
                                     ["id", "text"])
    
    print(f"📊 Dataset: {len(texts)} text documents")
    
    print("\n🧠 TensorFlow GPU UDF Implementation:")
    print("""
import tensorflow as tf
import tensorflow_hub as hub
import pandas as pd

# Define pandas UDF for batch processing
@pandas_udf(ArrayType(FloatType()))
def generate_embeddings_gpu(texts: pd.Series) -> pd.Series:
    \"\"\"Generate text embeddings using BERT on GPU.\"\"\"
    
    # Force GPU usage
    physical_devices = tf.config.list_physical_devices('GPU')
    if physical_devices:
        tf.config.set_visible_devices(physical_devices[0], 'GPU')
        tf.config.experimental.set_memory_growth(physical_devices[0], True)
    
    # Load pretrained BERT model from TensorFlow Hub
    model_url = "https://tfhub.dev/google/universal-sentence-encoder/4"
    embed_model = hub.load(model_url)
    
    # Process batch of texts
    embeddings = embed_model(texts.tolist())
    
    # Convert to list of lists
    return pd.Series(embeddings.numpy().tolist())

# Apply UDF to dataset
embedded = text_df.withColumn(
    "embedding",
    generate_embeddings_gpu(col("text"))
)

# Now you can use embeddings for similarity search, clustering, etc.
embedded.show(5, truncate=False)
    """)
    
    print("\n⚡ Performance Comparison:")
    print("""
    CPU (16 cores):    50 texts/sec     → 20 seconds for 1000 texts
    GPU (1x A100):     1000 texts/sec   → 1 second for 1000 texts
    
    Speedup: 20x faster with GPU! 🚀
    
    In a 10-node cluster: 10,000 texts/sec → Process 1M texts in 100 seconds
    """)


def demonstrate_custom_cuda_kernel(spark):
    """Use custom CUDA kernels with CuPy."""
    print("\n" + "=" * 70)
    print("4. CUSTOM CUDA KERNELS (Matrix Operations)")
    print("=" * 70)
    
    print("""
🔧 Using CuPy for Custom CUDA Operations:

CuPy provides NumPy-compatible GPU arrays and custom CUDA kernels.
Perfect for custom mathematical operations on GPUs.
    """)
    
    print("\n💻 CuPy GPU UDF Implementation:")
    print("""
import cupy as cp
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType

@pandas_udf(ArrayType(FloatType()))
def matrix_operation_gpu(matrices: pd.Series) -> pd.Series:
    \"\"\"Perform matrix operations on GPU using CuPy.\"\"\"
    
    results = []
    
    for matrix_data in matrices:
        # Convert to CuPy array (GPU)
        matrix = cp.array(matrix_data).reshape(100, 100)
        
        # Custom CUDA operations
        # 1. Matrix multiplication
        result = cp.matmul(matrix, matrix.T)
        
        # 2. Element-wise operations (parallel on GPU)
        result = cp.exp(result) * cp.log(result + 1)
        
        # 3. Reduction operations
        result = cp.sum(result, axis=0)
        
        # Convert back to CPU for return
        results.append(result.get().tolist())
    
    return pd.Series(results)

# Example custom CUDA kernel
custom_kernel = cp.RawKernel(r'''
extern "C" __global__
void custom_operation(const float* x, float* y, int N) {
    int tid = blockDim.x * blockIdx.x + threadIdx.x;
    if (tid < N) {
        y[tid] = x[tid] * x[tid] + 2.0f * x[tid] + 1.0f;
    }
}
''', 'custom_operation')

# Launch kernel on GPU
def apply_custom_kernel(data):
    x_gpu = cp.asarray(data, dtype=cp.float32)
    y_gpu = cp.empty_like(x_gpu)
    
    threads_per_block = 256
    blocks = (len(data) + threads_per_block - 1) // threads_per_block
    
    custom_kernel((blocks,), (threads_per_block,), (x_gpu, y_gpu, len(data)))
    
    return y_gpu.get()
    """)
    
    print("\n⚡ Performance Comparison:")
    print("""
    Operation: Matrix multiplication (1000x1000)
    
    CPU (NumPy):       500 ms per matrix
    GPU (CuPy):        5 ms per matrix
    
    Speedup: 100x faster! 🚀
    """)


def demonstrate_batch_inference_optimization(spark):
    """Optimize GPU utilization with batching."""
    print("\n" + "=" * 70)
    print("5. BATCH INFERENCE OPTIMIZATION")
    print("=" * 70)
    
    print("""
🎯 Key Optimization: Batch Processing

GPUs are most efficient when processing data in batches.
Single inference: ~50% GPU utilization
Batch inference: ~95% GPU utilization

Strategy:
---------
1. Use pandas UDF (processes batches automatically)
2. Set optimal batch size for your GPU memory
3. Use mapInPandas for full control
    """)
    
    print("\n📊 Batch Size Impact:")
    print("""
Batch Size    GPU Memory    Throughput    Latency
----------    ----------    ----------    -------
1             0.5 GB        100/sec       10ms
8             2 GB          600/sec       13ms
32            6 GB          2000/sec      16ms
128           20 GB         5000/sec      25ms  ← Optimal
256           40 GB         5500/sec      46ms  (diminishing returns)
    """)
    
    print("\n💡 Optimized Batch UDF:")
    print("""
from pyspark.sql.functions import pandas_udf
import pandas as pd
import torch

@pandas_udf(ArrayType(FloatType()))
def optimized_gpu_inference(features: pd.Series) -> pd.Series:
    \"\"\"Process with optimal batch size for GPU.\"\"\"
    
    device = torch.device("cuda")
    model = load_model().to(device)
    model.eval()
    
    OPTIMAL_BATCH_SIZE = 128
    results = []
    
    # Process in optimal batches
    features_list = features.tolist()
    for i in range(0, len(features_list), OPTIMAL_BATCH_SIZE):
        batch = features_list[i:i + OPTIMAL_BATCH_SIZE]
        
        # Convert to tensor and move to GPU
        batch_tensor = torch.tensor(batch).to(device)
        
        # Batch inference on GPU
        with torch.no_grad():
            outputs = model(batch_tensor)
        
        results.extend(outputs.cpu().numpy().tolist())
    
    return pd.Series(results)

# Configure Spark for optimal batching
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "128")
    """)


def demonstrate_multi_gpu_strategies(spark):
    """Strategies for using multiple GPUs per node."""
    print("\n" + "=" * 70)
    print("6. MULTI-GPU STRATEGIES")
    print("=" * 70)
    
    print("""
🎮 Scenario: Each node has 4 GPUs

Strategy 1: One GPU per Task (Recommended)
-------------------------------------------
✅ Simple configuration
✅ Automatic GPU assignment
✅ Good for inference workloads

Configuration:
spark-submit \\
    --conf spark.task.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.amount=4 \\
    --conf spark.executor.cores=4

Result: 4 concurrent tasks per executor, each using 1 GPU


Strategy 2: Multiple GPUs per Task (Data Parallel)
---------------------------------------------------
✅ For large models that benefit from data parallelism
✅ Training workloads
⚠️  Requires model parallelism implementation

Configuration:
spark-submit \\
    --conf spark.task.resource.gpu.amount=4 \\
    --conf spark.executor.resource.gpu.amount=4 \\
    --conf spark.executor.cores=1

Result: 1 task per executor using all 4 GPUs


Strategy 3: Dynamic GPU Assignment
-----------------------------------
✅ Different tasks need different GPU counts
✅ Heterogeneous workloads

Use TaskContext to get assigned GPU:
    """)
    
    print("""
from pyspark import TaskContext

def process_with_assigned_gpu(partition):
    \"\"\"Use the GPU assigned to this task.\"\"\"
    context = TaskContext.get()
    gpu_resources = context.resources()['gpu']
    gpu_id = gpu_resources.addresses[0]  # Get assigned GPU ID
    
    # Set CUDA device
    import torch
    torch.cuda.set_device(int(gpu_id))
    
    # Your GPU code here
    device = torch.device(f"cuda:{gpu_id}")
    model = load_model().to(device)
    
    for data in partition:
        # Process on assigned GPU
        result = model(data)
        yield result

# Apply to RDD
rdd.mapPartitions(process_with_assigned_gpu).collect()
    """)


def demonstrate_real_world_example(spark):
    """Complete real-world example."""
    print("\n" + "=" * 70)
    print("7. REAL-WORLD EXAMPLE: Video Frame Analysis")
    print("=" * 70)
    
    print("""
🎬 Use Case: Analyze 1 million video frames across cluster

Architecture:
-------------
- 10 worker nodes, each with 1 NVIDIA A100 GPU
- Each frame: 1920x1080 RGB image
- Model: YOLOv8 object detection
- Output: Detected objects per frame
    """)
    
    # Create sample dataset
    num_frames = 10000
    frames_df = spark.range(num_frames).toDF("frame_id") \
        .withColumn("video_path", (col("frame_id") % 100).cast("string"))
    
    print(f"\n📊 Dataset: {num_frames} video frames")
    
    print("\n💻 Complete Implementation:")
    print("""
from pyspark.sql.functions import pandas_udf, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType
import pandas as pd
import torch
from ultralytics import YOLO
import cv2

# Define output schema
detection_schema = ArrayType(StructType([
    StructField("class", StringType()),
    StructField("confidence", FloatType()),
    StructField("bbox", ArrayType(FloatType()))
]))

@pandas_udf(detection_schema)
def detect_objects_gpu(frame_paths: pd.Series) -> pd.Series:
    \"\"\"Detect objects in video frames using YOLOv8 on GPU.\"\"\"
    
    # Initialize YOLO model on GPU
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    model = YOLO('yolov8n.pt')  # Nano model for speed
    model.to(device)
    
    all_detections = []
    
    # Process frames in batches
    batch_size = 32
    frame_list = frame_paths.tolist()
    
    for i in range(0, len(frame_list), batch_size):
        batch_paths = frame_list[i:i + batch_size]
        
        # Load frames
        frames = [cv2.imread(path) for path in batch_paths]
        
        # Batch inference on GPU
        results = model(frames, device=device)
        
        # Extract detections
        for result in results:
            detections = []
            for box in result.boxes:
                detections.append({
                    'class': result.names[int(box.cls)],
                    'confidence': float(box.conf),
                    'bbox': box.xyxy[0].cpu().numpy().tolist()
                })
            all_detections.append(detections)
    
    return pd.Series(all_detections)

# Process all frames
detected = frames_df.withColumn(
    "detections",
    detect_objects_gpu(col("frame_path"))
)

# Filter frames with people detected
people_frames = detected.filter(
    col("detections").getItem(0).getItem("class") == "person"
)

# Save results
people_frames.write.parquet("s3://results/people_detected/")
    """)
    
    print("\n⚡ Performance Analysis:")
    print("""
Metrics:
--------
- Frames to process: 1,000,000
- CPU processing: 20 frames/sec/node → 2,000 frames/sec cluster → 500 seconds
- GPU processing: 500 frames/sec/node → 5,000 frames/sec cluster → 200 seconds

Result: 2.5x speedup with GPUs! 🚀

Cost Analysis:
--------------
CPU Cluster (10 nodes, 16 cores): $5/hour × 0.14 hours = $0.70
GPU Cluster (10 nodes, 1 A100):   $12/hour × 0.06 hours = $0.72

✅ GPU cluster is faster AND similar cost (better throughput per dollar)!
    """)


def demonstrate_best_practices(spark):
    """Best practices for GPU UDFs."""
    print("\n" + "=" * 70)
    print("8. BEST PRACTICES FOR GPU UDFs")
    print("=" * 70)
    
    print("""
🎯 Performance Optimization:

1. ✅ Use Pandas UDF for automatic batching
   @pandas_udf(return_type)
   def gpu_func(batch: pd.Series) -> pd.Series:
       # Process batch on GPU
       pass

2. ✅ Load model once per executor (not per row)
   Use global variables or singleton pattern:
   
   _model = None
   def get_model():
       global _model
       if _model is None:
           _model = load_model().cuda()
       return _model

3. ✅ Set optimal batch size
   spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "128")

4. ✅ Enable Arrow optimization
   spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

5. ✅ Handle GPU memory carefully
   - Use torch.cuda.empty_cache() between batches
   - Monitor GPU memory with nvidia-smi
   - Set model to eval mode: model.eval()

6. ✅ Configure task GPU resources
   --conf spark.task.resource.gpu.amount=1
   --conf spark.executor.resource.gpu.amount=1


⚠️  Common Mistakes:

1. ❌ Loading model for every row
   This is extremely slow - load once per executor

2. ❌ Not using batching
   Single inference is inefficient - use pandas UDF

3. ❌ Too large batch size
   Causes GPU OOM - tune based on model and GPU memory

4. ❌ Not checking GPU availability
   Always check: torch.cuda.is_available()

5. ❌ Forgetting to move tensors to GPU
   tensor = tensor.to(device)  # Don't forget this!

6. ❌ Not cleaning up GPU memory
   Use: torch.cuda.empty_cache() or del model


📊 GPU Resource Configuration:

# For YARN
spark-submit \\
    --master yarn \\
    --deploy-mode cluster \\
    --num-executors 10 \\
    --executor-cores 4 \\
    --executor-memory 16g \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.task.resource.gpu.amount=1 \\
    --conf spark.rapids.sql.enabled=false \\
    --files /path/to/getGpusResources.sh \\
    --conf spark.executor.resource.gpu.discoveryScript=./getGpusResources.sh \\
    your_script.py

# For Kubernetes
spark-submit \\
    --master k8s://https://kubernetes-api:443 \\
    --deploy-mode cluster \\
    --name gpu-inference \\
    --conf spark.kubernetes.container.image=nvidia/cuda:11.8.0-runtime-ubuntu22.04 \\
    --conf spark.kubernetes.executor.request.cores=4 \\
    --conf spark.executor.memory=16g \\
    --conf spark.executor.resource.gpu.amount=1 \\
    --conf spark.executor.resource.gpu.vendor=nvidia.com/gpu \\
    --conf spark.task.resource.gpu.amount=1 \\
    your_script.py


🔧 Monitoring GPU Usage:

# Check GPU utilization on executors
nvidia-smi dmon -s u

# Monitor GPU memory
nvidia-smi dmon -s m

# Full dashboard
watch -n 1 nvidia-smi


📚 Dependencies:

pip install torch torchvision  # PyTorch
pip install tensorflow-gpu     # TensorFlow
pip install cupy-cuda11x      # CuPy (CUDA operations)
pip install ultralytics       # YOLOv8
pip install transformers      # Hugging Face models


🎓 When to Use GPU vs CPU:

Use GPU:
--------
✅ Deep learning inference (ResNet, BERT, YOLO)
✅ Large matrix operations
✅ Image/video processing
✅ Batch processing (>32 items)
✅ Complex neural networks

Use CPU:
--------
✅ Simple aggregations (sum, count, avg)
✅ String operations
✅ Small data (<1000 rows)
✅ Single-item processing
✅ ETL transformations
    """)


def main():
    spark = create_spark()
    
    print("🎮 GPU-ACCELERATED PySpark UDFs")
    print("=" * 70)
    print("\nLeverage GPUs on each cluster node for distributed deep learning!")
    print()
    
    demonstrate_gpu_detection()
    demonstrate_pytorch_gpu_udf(spark)
    demonstrate_tensorflow_gpu_udf(spark)
    demonstrate_custom_cuda_kernel(spark)
    demonstrate_batch_inference_optimization(spark)
    demonstrate_multi_gpu_strategies(spark)
    demonstrate_real_world_example(spark)
    demonstrate_best_practices(spark)
    
    print("\n" + "=" * 70)
    print("✅ GPU UDF DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Use pandas UDF for automatic batching on GPU")
    print("   2. Load model once per executor (not per row)")
    print("   3. Set optimal batch size (typically 32-128)")
    print("   4. Configure: spark.task.resource.gpu.amount=1")
    print("   5. GPUs provide 10-100x speedup for deep learning")
    print("   6. Monitor GPU memory and utilization")
    print("   7. Use Arrow optimization for data transfer")
    print("\n🚀 GPU cluster can process 1M images in minutes vs hours on CPU!")
    
    spark.stop()


if __name__ == "__main__":
    main()
