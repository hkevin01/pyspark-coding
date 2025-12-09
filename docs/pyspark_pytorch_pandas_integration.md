# PySpark + PyTorch + Pandas Integration Guide
## Complete Guide for Data Engineers and ML Practitioners

---

## Table of Contents
1. [Overview](#overview)
2. [The Three Pillars](#the-three-pillars)
3. [Data Flow Patterns](#data-flow-patterns)
4. [Conversion Strategies](#conversion-strategies)
5. [Performance Optimization](#performance-optimization)
6. [Production Patterns](#production-patterns)
7. [Common Pitfalls](#common-pitfalls)
8. [Real-World Examples](#real-world-examples)

---

## Overview

### Why Use All Three Together?

Each framework excels at different tasks:

- **PySpark:** Distributed data processing at scale (TBs)
- **Pandas:** In-memory data manipulation and analysis (GBs)
- **PyTorch:** Deep learning and tensor operations (neural networks)

**The Sweet Spot:** Use PySpark for data preparation → Pandas as the bridge → PyTorch for ML

---

## The Three Pillars

### 1. PySpark (Distributed Computing)

**Best For:**
- Loading massive datasets (> 100GB)
- Distributed transformations (joins, aggregations)
- ETL pipelines
- Feature engineering at scale
- Batch inference on billions of rows

**Data Structure:** DataFrame (distributed, lazy evaluation)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session
spark = SparkSession.builder \
    .appName("DataProcessing") \
    .getOrCreate()

# Read large dataset
df = spark.read.parquet("s3://data/large_dataset.parquet")

# Distributed operations
result = df.filter(F.col("age") > 25) \
           .groupBy("city") \
           .agg(F.mean("salary").alias("avg_salary"))
```

**Key Characteristics:**
- ✅ Handles data larger than memory
- ✅ Fault-tolerant and scalable
- ✅ SQL-like API
- ❌ Not ideal for iterative algorithms
- ❌ Higher overhead for small data

---

### 2. Pandas (In-Memory Analysis)

**Best For:**
- Exploratory data analysis
- Quick data manipulation
- Statistical analysis
- Bridge between PySpark and PyTorch
- Datasets that fit in memory (< 10GB)

**Data Structure:** DataFrame (in-memory, eager execution)

```python
import pandas as pd
import numpy as np

# Read data
df = pd.read_csv("data.csv")

# In-memory operations
result = df[df['age'] > 25] \
    .groupby('city') \
    .agg({'salary': 'mean'})

# Convert to NumPy for ML
X = df[['feature1', 'feature2']].values  # NumPy array
```

**Key Characteristics:**
- ✅ Fast for small to medium data
- ✅ Rich API for data manipulation
- ✅ Easy bridge to NumPy/PyTorch
- ❌ Limited by single machine memory
- ❌ No built-in parallelization

---

### 3. PyTorch (Deep Learning)

**Best For:**
- Neural network training
- Tensor operations
- GPU acceleration
- Complex ML models
- Transfer learning

**Data Structure:** Tensor (multi-dimensional arrays)

```python
import torch
import torch.nn as nn

# Create tensor from NumPy
X = torch.from_numpy(data_array).float()

# Define model
model = nn.Sequential(
    nn.Linear(10, 64),
    nn.ReLU(),
    nn.Linear(64, 1)
)

# Training
optimizer = torch.optim.Adam(model.parameters())
loss_fn = nn.MSELoss()

for epoch in range(100):
    predictions = model(X)
    loss = loss_fn(predictions, y)
    loss.backward()
    optimizer.step()
    optimizer.zero_grad()
```

**Key Characteristics:**
- ✅ GPU acceleration
- ✅ Automatic differentiation
- ✅ Flexible neural network architecture
- ❌ Requires data in memory
- ❌ Steeper learning curve

---

## Data Flow Patterns

### Pattern 1: PySpark → Pandas → PyTorch (Training)

**Use Case:** Train ML model on large dataset

```python
# Step 1: PySpark - Load and preprocess large data
df_spark = spark.read.parquet("s3://data/large_dataset.parquet")

# Feature engineering at scale
df_features = df_spark \
    .withColumn("age_squared", F.col("age") ** 2) \
    .withColumn("income_log", F.log(F.col("income"))) \
    .filter(F.col("age").isNotNull())

# Sample for training (if data too large)
df_sample = df_features.sample(fraction=0.1, seed=42)

# Step 2: Pandas - Convert to Pandas (bridge)
df_pandas = df_sample.toPandas()

# Data preparation
X = df_pandas[['age', 'age_squared', 'income_log']].values
y = df_pandas['target'].values

# Step 3: PyTorch - Convert to tensors and train
X_tensor = torch.from_numpy(X).float()
y_tensor = torch.from_numpy(y).float()

# Create dataset and loader
dataset = torch.utils.data.TensorDataset(X_tensor, y_tensor)
loader = torch.utils.data.DataLoader(dataset, batch_size=32)

# Train model
model = create_model()
train(model, loader)
```

**Why This Works:**
- PySpark handles billions of rows efficiently
- Pandas provides familiar API for conversion
- PyTorch gets data in tensor format for training

---

### Pattern 2: PyTorch → PySpark (Inference)

**Use Case:** Apply trained model to large dataset

```python
# Step 1: Train model (PyTorch)
model = train_pytorch_model(X_train, y_train)
model.eval()

# Step 2: Broadcast model to Spark workers
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# Step 3: Create Pandas UDF UDF (User‑Defined Function) for inference
# When people talk about UDFs for inference, they mean embedding machine learning 
# model inference inside a database or data 
# processing system by wrapping the model call in a UDF.
# UDFs let you run inference on streaming data in real time.

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType

@pandas_udf(FloatType())
def predict_udf(features: pd.Series) -> pd.Series:
    """Apply PyTorch model to Pandas Series"""
    # Load model from broadcast
    model = create_model()
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    # Convert Pandas Series → NumPy → Tensor
    X = np.array(features.tolist())
    X_tensor = torch.from_numpy(X).float()
    
    # Inference
    with torch.no_grad():
        predictions = model(X_tensor).numpy()
    
    # Return as Pandas Series
    return pd.Series(predictions.flatten())

# Step 4: Apply at scale with PySpark
df_predictions = df_spark.withColumn(
    "prediction", 
    predict_udf(F.array("feature1", "feature2", "feature3"))
)

# Write results
df_predictions.write.parquet("s3://output/predictions.parquet")
```

**Why This Works:**
- Model trained once on representative sample
- Broadcast avoids loading model per partition
- Pandas UDF processes batches efficiently
- PySpark handles distribution automatically

---

### Pattern 3: Full ML Pipeline

**Use Case:** Complete end-to-end ML workflow

```python
def ml_pipeline(spark, data_path, model_path, output_path):
    """Complete ML pipeline using all three frameworks"""
    
    # ============================================================
    # PHASE 1: DATA PREPARATION (PySpark)
    # ============================================================
    print("Phase 1: Data Preparation with PySpark")
    
    # Load raw data
    df_raw = spark.read.parquet(data_path)
    
    # Feature engineering
    df_features = df_raw \
        .withColumn("age_group", 
            F.when(F.col("age") < 25, "young")
             .when(F.col("age") < 50, "middle")
             .otherwise("senior")) \
        .withColumn("log_income", F.log1p(F.col("income"))) \
        .withColumn("is_weekend", 
            F.dayofweek(F.col("date")).isin([1, 7]))
    
    # Split data
    train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
    
    # ============================================================
    # PHASE 2: MODEL TRAINING (Pandas + PyTorch)
    # ============================================================
    print("Phase 2: Model Training with PyTorch")
    
    # Sample if needed (PyTorch works best with in-memory data)
    train_sample = train_df.sample(fraction=0.1).toPandas()
    
    # Prepare features and labels
    feature_cols = ['age', 'log_income', 'is_weekend']
    X_train = train_sample[feature_cols].values
    y_train = train_sample['target'].values
    
    # Convert to PyTorch tensors
    X_tensor = torch.from_numpy(X_train).float()
    y_tensor = torch.from_numpy(y_train).float().reshape(-1, 1)
    
    # Define model
    model = nn.Sequential(
        nn.Linear(len(feature_cols), 64),
        nn.ReLU(),
        nn.Dropout(0.3),
        nn.Linear(64, 32),
        nn.ReLU(),
        nn.Linear(32, 1),
        nn.Sigmoid()
    )
    
    # Train
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    loss_fn = nn.BCELoss()
    
    for epoch in range(100):
        predictions = model(X_tensor)
        loss = loss_fn(predictions, y_tensor)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()
        
        if epoch % 10 == 0:
            print(f"Epoch {epoch}, Loss: {loss.item():.4f}")
    
    # Save model
    torch.save(model.state_dict(), model_path)
    
    # ============================================================
    # PHASE 3: DISTRIBUTED INFERENCE (PySpark + PyTorch)
    # ============================================================
    print("Phase 3: Distributed Inference with PySpark")
    
    # Broadcast model
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    # Pandas UDF for inference
    @pandas_udf(FloatType())
    def predict(age: pd.Series, log_income: pd.Series, 
                is_weekend: pd.Series) -> pd.Series:
        # Reconstruct model
        model = nn.Sequential(
            nn.Linear(3, 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
        model.load_state_dict(broadcast_model.value)
        model.eval()
        
        # Prepare batch
        X = np.column_stack([age, log_income, is_weekend])
        X_tensor = torch.from_numpy(X).float()
        
        # Predict
        with torch.no_grad():
            predictions = model(X_tensor).numpy().flatten()
        
        return pd.Series(predictions)
    
    # Apply to test set
    df_predictions = test_df.withColumn(
        "prediction",
        predict(F.col("age"), F.col("log_income"), F.col("is_weekend"))
    )
    
    # ============================================================
    # PHASE 4: EVALUATION (PySpark)
    # ============================================================
    print("Phase 4: Evaluation with PySpark")
    
    # Calculate metrics
    df_eval = df_predictions.withColumn(
        "predicted_class",
        F.when(F.col("prediction") > 0.5, 1).otherwise(0)
    )
    
    accuracy = df_eval.filter(
        F.col("target") == F.col("predicted_class")
    ).count() / df_eval.count()
    
    print(f"Accuracy: {accuracy:.4f}")
    
    # Save results
    df_predictions.write.mode("overwrite").parquet(output_path)
    
    return model, accuracy

# Run pipeline
model, accuracy = ml_pipeline(
    spark=spark,
    data_path="s3://data/input.parquet",
    model_path="models/model.pth",
    output_path="s3://data/predictions.parquet"
)
```

---

## Conversion Strategies

### Converting Between Frameworks

#### PySpark → Pandas

```python
# Method 1: Direct conversion (small data)
df_pandas = df_spark.toPandas()

# Method 2: Sample first (large data)
df_pandas = df_spark.sample(0.01).toPandas()

# Method 3: Select specific columns
df_pandas = df_spark.select("col1", "col2").toPandas()

# Method 4: Aggregate first
df_pandas = df_spark.groupBy("category") \
    .agg(F.mean("value")) \
    .toPandas()

# ⚠️ Warning: toPandas() loads entire result into memory!
```

#### Pandas → PySpark

```python
# Method 1: From Pandas DataFrame
df_spark = spark.createDataFrame(df_pandas)

# Method 2: With explicit schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True)
])
df_spark = spark.createDataFrame(df_pandas, schema=schema)

# Method 3: From list of Row objects
from pyspark.sql import Row
rows = [Row(id=r.id, name=r.name) for _, r in df_pandas.iterrows()]
df_spark = spark.createDataFrame(rows)
```

#### Pandas → PyTorch

```python
# Method 1: Via NumPy (most common)
X = df_pandas[['feature1', 'feature2']].values  # NumPy array
X_tensor = torch.from_numpy(X).float()

# Method 2: From list
X_tensor = torch.tensor(df_pandas['feature'].tolist()).float()

# Method 3: Direct conversion (less common)
X_tensor = torch.from_numpy(df_pandas.to_numpy()).float()

# For labels
y_tensor = torch.from_numpy(df_pandas['label'].values).long()  # For classification
y_tensor = torch.from_numpy(df_pandas['label'].values).float()  # For regression
```

#### PyTorch → Pandas

```python
# Method 1: Via NumPy
df_pandas = pd.DataFrame(tensor.numpy(), columns=['col1', 'col2'])

# Method 2: For predictions
predictions = model(X_tensor).detach().numpy()
df_pandas['prediction'] = predictions

# Method 3: From detached tensor
df_result = pd.DataFrame({
    'id': ids,
    'prediction': tensor.detach().cpu().numpy()
})
```

#### PySpark → PyTorch (Direct)

```python
# NOT RECOMMENDED: Don't do this on large data!
# df_spark.toPandas() can cause OOM

# RECOMMENDED: Use sampling
df_sample = df_spark.sample(fraction=0.1)
df_pandas = df_sample.toPandas()
X_tensor = torch.from_numpy(df_pandas.values).float()

# OR: Process in partitions (see distributed inference pattern)
```

---

## Performance Optimization

### 1. Memory Management

```python
# ❌ BAD: Convert entire large dataset
df_pandas = df_spark.toPandas()  # OOM on 100GB data!

# ✅ GOOD: Sample first
df_sample = df_spark.sample(0.01)  # 1% sample
df_pandas = df_sample.toPandas()

# ✅ GOOD: Select only needed columns
df_pandas = df_spark.select("id", "features", "label").toPandas()

# ✅ GOOD: Filter first
df_pandas = df_spark.filter(F.col("date") >= "2024-01-01").toPandas()
```

### 2. Efficient Pandas UDF

```python
# ❌ BAD: Load model in every call
@pandas_udf(FloatType())
def predict_bad(features: pd.Series) -> pd.Series:
    model = load_model_from_disk()  # Slow! Called many times
    return model.predict(features)

# ✅ GOOD: Broadcast model
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

@pandas_udf(FloatType())
def predict_good(features: pd.Series) -> pd.Series:
    model = load_from_broadcast(broadcast_model.value)  # Fast!
    return model.predict(features)

# ✅ BETTER: Use iterator pattern for large batches
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf(FloatType(), PandasUDFType.SCALAR_ITER)
def predict_best(iterator):
    model = load_model()  # Load once per partition
    for batch in iterator:
        yield pd.Series(model.predict(batch))
```

### 3. Batch Processing

```python
# ✅ Process data in batches
batch_size = 10000

for i in range(0, len(df_spark), batch_size):
    batch = df_spark.limit(batch_size).offset(i).toPandas()
    X_batch = torch.from_numpy(batch.values).float()
    
    # Process batch
    predictions = model(X_batch)
    
    # Save or accumulate results
```

### 4. Partitioning Strategy

```python
# ✅ Repartition before Pandas UDF
df_spark = df_spark.repartition(200)  # Increase parallelism

# ✅ Partition by key for better distribution
df_spark = df_spark.repartition("user_id")

# ✅ Coalesce before collecting
df_result = df_result.coalesce(1).toPandas()  # Single output file
```

### 5. Caching Strategy

```python
# ✅ Cache frequently accessed DataFrames
df_features = df_raw.select("features", "label").cache()

# Use cached DataFrame multiple times
X_train = df_features.filter(F.col("split") == "train").toPandas()
X_test = df_features.filter(F.col("split") == "test").toPandas()

# ✅ Unpersist when done
df_features.unpersist()
```

---

## Production Patterns

### Pattern A: Batch Training Pipeline

```python
class BatchMLPipeline:
    """Production ML pipeline using PySpark + PyTorch"""
    
    def __init__(self, spark):
        self.spark = spark
        self.model = None
        
    def load_data(self, path):
        """Load data with PySpark"""
        return self.spark.read.parquet(path)
    
    def feature_engineering(self, df):
        """Feature engineering at scale"""
        return df \
            .withColumn("feature1_log", F.log1p("feature1")) \
            .withColumn("feature2_squared", F.col("feature2") ** 2) \
            .withColumn("interaction", F.col("feature1") * F.col("feature2"))
    
    def prepare_training_data(self, df, sample_fraction=0.1):
        """Sample and convert to PyTorch format"""
        df_sample = df.sample(sample_fraction).toPandas()
        
        X = df_sample.drop("label", axis=1).values
        y = df_sample["label"].values
        
        return (
            torch.from_numpy(X).float(),
            torch.from_numpy(y).float()
        )
    
    def train_model(self, X, y, epochs=100):
        """Train PyTorch model"""
        self.model = nn.Sequential(
            nn.Linear(X.shape[1], 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )
        
        optimizer = torch.optim.Adam(self.model.parameters())
        loss_fn = nn.MSELoss()
        
        for epoch in range(epochs):
            predictions = self.model(X)
            loss = loss_fn(predictions, y.reshape(-1, 1))
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
            
            if epoch % 10 == 0:
                print(f"Epoch {epoch}, Loss: {loss.item():.4f}")
    
    def distributed_inference(self, df):
        """Apply model at scale"""
        broadcast_model = self.spark.sparkContext.broadcast(
            self.model.state_dict()
        )
        
        @pandas_udf(FloatType())
        def predict_udf(features: pd.Series) -> pd.Series:
            model = self._load_model(broadcast_model.value)
            X = np.array(features.tolist())
            X_tensor = torch.from_numpy(X).float()
            
            with torch.no_grad():
                preds = model(X_tensor).numpy().flatten()
            
            return pd.Series(preds)
        
        return df.withColumn("prediction", predict_udf(F.col("features")))
    
    def run(self, input_path, output_path):
        """Run complete pipeline"""
        # Load
        df = self.load_data(input_path)
        
        # Feature engineering
        df = self.feature_engineering(df)
        
        # Train
        X, y = self.prepare_training_data(df)
        self.train_model(X, y)
        
        # Inference
        df_pred = self.distributed_inference(df)
        
        # Save
        df_pred.write.mode("overwrite").parquet(output_path)

# Usage
pipeline = BatchMLPipeline(spark)
pipeline.run("s3://input/data.parquet", "s3://output/predictions.parquet")
```

### Pattern B: Real-Time Inference

```python
class RealtimeInferenceService:
    """Real-time inference with pre-trained PyTorch model"""
    
    def __init__(self, spark, model_path):
        self.spark = spark
        self.model = torch.load(model_path)
        self.model.eval()
        
        # Broadcast model once
        self.broadcast_model = spark.sparkContext.broadcast(
            self.model.state_dict()
        )
    
    def predict_batch(self, df_batch):
        """Predict on batch using Pandas UDF"""
        
        @pandas_udf(FloatType())
        def predict_udf(features: pd.Series) -> pd.Series:
            # Load model from broadcast
            model = self._reconstruct_model(self.broadcast_model.value)
            
            # Convert to tensor
            X = np.array(features.tolist())
            X_tensor = torch.from_numpy(X).float()
            
            # Inference
            with torch.no_grad():
                predictions = model(X_tensor).numpy().flatten()
            
            return pd.Series(predictions)
        
        return df_batch.withColumn("prediction", predict_udf(F.col("features")))
    
    def stream_inference(self, stream_df):
        """Apply to streaming DataFrame"""
        return stream_df \
            .writeStream \
            .foreachBatch(lambda batch, _: self.predict_batch(batch)) \
            .start()

# Usage
service = RealtimeInferenceService(spark, "models/model.pth")
stream = spark.readStream.format("kafka").load()
query = service.stream_inference(stream)
query.awaitTermination()
```

---

## Common Pitfalls

### Pitfall 1: Out of Memory

```python
# ❌ WRONG: Converting large DataFrame to Pandas
df_pandas = large_spark_df.toPandas()  # OOM!

# ✅ RIGHT: Sample or filter first
df_pandas = large_spark_df \
    .sample(0.01) \
    .filter(F.col("date") >= "2024-01-01") \
    .select("id", "features") \
    .toPandas()
```

### Pitfall 2: Inefficient UDF

```python
# ❌ WRONG: Loading model in UDF
@pandas_udf(FloatType())
def predict(x):
    model = torch.load("model.pth")  # Loaded many times!
    return model.predict(x)

# ✅ RIGHT: Broadcast model
bc_model = spark.sparkContext.broadcast(model)

@pandas_udf(FloatType())
def predict(x):
    model = use_broadcast(bc_model)  # Loaded once per partition
    return model.predict(x)
```

### Pitfall 3: Data Type Mismatch

```python
# ❌ WRONG: Incorrect tensor types
X = torch.from_numpy(data)  # Defaults to float64
y = torch.from_numpy(labels)  # May be float64

loss = loss_fn(model(X), y)  # Type mismatch!

# ✅ RIGHT: Explicit type conversion
X = torch.from_numpy(data).float()  # float32
y = torch.from_numpy(labels).long()  # int64 for classification
# or
y = torch.from_numpy(labels).float()  # float32 for regression
```

### Pitfall 4: Not Using GPU

```python
# ❌ WRONG: PyTorch on CPU when GPU available
model = create_model()
X_tensor = torch.from_numpy(X)
predictions = model(X_tensor)

# ✅ RIGHT: Use GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = create_model().to(device)
X_tensor = torch.from_numpy(X).to(device)
predictions = model(X_tensor).cpu().numpy()  # Back to CPU for PySpark
```

### Pitfall 5: Lazy Evaluation Confusion

```python
# ❌ WRONG: Assuming immediate execution
df_filtered = df_spark.filter(F.col("age") > 25)  # Not executed yet!
print(df_filtered.count())  # Executed here

# ✅ RIGHT: Understand when execution happens
df_filtered = df_spark.filter(F.col("age") > 25)  # Lazy
df_filtered.cache()  # Still lazy
df_filtered.count()  # Triggers execution, data cached
df_filtered.show()  # Uses cached data
```

---

## Real-World Examples

### Example 1: Image Classification at Scale

```python
"""
Use Case: Classify millions of images using ResNet
- PySpark: Load image paths, distribute work
- PyTorch: ResNet model for classification
- Pandas: Bridge for batch processing
"""

import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image

# Load pre-trained ResNet
resnet = models.resnet50(pretrained=True)
resnet.eval()

# Broadcast model
broadcast_model = spark.sparkContext.broadcast(resnet.state_dict())

# Define image preprocessing
preprocess = transforms.Compose([
    transforms.Resize(256),
    transforms.CenterCrop(224),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406],
                       std=[0.229, 0.224, 0.225])
])

def classify_images_batch(image_paths):
    """Classify batch of images"""
    # Load model from broadcast
    model = models.resnet50()
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    results = []
    for path in image_paths:
        # Load and preprocess image
        img = Image.open(path)
        img_tensor = preprocess(img).unsqueeze(0)
        
        # Classify
        with torch.no_grad():
            output = model(img_tensor)
            _, predicted = torch.max(output, 1)
        
        results.append(predicted.item())
    
    return results

# Create Pandas UDF
@pandas_udf("array<int>")
def classify_udf(paths: pd.Series) -> pd.Series:
    return pd.Series([classify_images_batch(p) for p in paths])

# Apply at scale
df_images = spark.read.parquet("s3://images/metadata.parquet")
df_classified = df_images.withColumn(
    "class",
    classify_udf(F.col("image_path"))
)

df_classified.write.parquet("s3://images/classified.parquet")
```

### Example 2: Time Series Forecasting

```python
"""
Use Case: Forecast sales for 1M products
- PySpark: Aggregate sales data, create features
- PyTorch: LSTM for forecasting
- Pandas: Sequence generation
"""

# Feature engineering with PySpark
df_sales = spark.read.parquet("s3://sales/transactions.parquet")

df_features = df_sales \
    .groupBy("product_id", "date") \
    .agg(F.sum("sales").alias("daily_sales")) \
    .withColumn("day_of_week", F.dayofweek("date")) \
    .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]))

# Window function for rolling features
from pyspark.sql.window import Window

window_spec = Window.partitionBy("product_id") \
    .orderBy("date") \
    .rowsBetween(-7, -1)

df_features = df_features.withColumn(
    "sales_7day_avg",
    F.avg("daily_sales").over(window_spec)
)

# Sample for training
df_train = df_features.sample(0.01).toPandas()

# Create sequences for LSTM
def create_sequences(data, seq_length=30):
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i+seq_length])
        y.append(data[i+seq_length])
    return np.array(X), np.array(y)

X, y = create_sequences(df_train['daily_sales'].values)
X_tensor = torch.from_numpy(X).float().unsqueeze(-1)
y_tensor = torch.from_numpy(y).float()

# Define LSTM model
class LSTMForecaster(nn.Module):
    def __init__(self, input_size=1, hidden_size=64, num_layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.linear = nn.Linear(hidden_size, 1)
    
    def forward(self, x):
        lstm_out, _ = self.lstm(x)
        predictions = self.linear(lstm_out[:, -1, :])
        return predictions

# Train model
model = LSTMForecaster()
optimizer = torch.optim.Adam(model.parameters())
loss_fn = nn.MSELoss()

for epoch in range(100):
    predictions = model(X_tensor)
    loss = loss_fn(predictions.squeeze(), y_tensor)
    loss.backward()
    optimizer.step()
    optimizer.zero_grad()

# Save and broadcast
torch.save(model.state_dict(), "models/lstm_forecaster.pth")
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# Distributed forecasting with Pandas UDF
@pandas_udf(FloatType())
def forecast_udf(sales_history: pd.Series) -> pd.Series:
    model = LSTMForecaster()
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    forecasts = []
    for history in sales_history:
        X = torch.tensor(history[-30:]).float().unsqueeze(0).unsqueeze(-1)
        with torch.no_grad():
            pred = model(X).item()
        forecasts.append(pred)
    
    return pd.Series(forecasts)

# Apply forecasting
df_forecast = df_features \
    .groupBy("product_id") \
    .agg(F.collect_list("daily_sales").alias("sales_history")) \
    .withColumn("forecast", forecast_udf(F.col("sales_history")))

df_forecast.write.parquet("s3://sales/forecasts.parquet")
```

### Example 3: Natural Language Processing

```python
"""
Use Case: Sentiment analysis on millions of reviews
- PySpark: Load and preprocess text
- PyTorch: BERT for sentiment analysis
- Pandas: Text batching
"""

from transformers import BertTokenizer, BertForSequenceClassification

# Load BERT model
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertForSequenceClassification.from_pretrained('bert-base-uncased')
model.eval()

# Broadcast model
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# Pandas UDF for sentiment analysis
@pandas_udf("float")
def sentiment_udf(texts: pd.Series) -> pd.Series:
    # Load model from broadcast
    model = BertForSequenceClassification.from_pretrained('bert-base-uncased')
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    results = []
    for text in texts:
        # Tokenize
        inputs = tokenizer(text, return_tensors="pt", 
                          truncation=True, padding=True, max_length=512)
        
        # Inference
        with torch.no_grad():
            outputs = model(**inputs)
            sentiment = torch.softmax(outputs.logits, dim=1)[0][1].item()
        
        results.append(sentiment)
    
    return pd.Series(results)

# Load reviews
df_reviews = spark.read.parquet("s3://reviews/customer_reviews.parquet")

# Text preprocessing with PySpark
df_clean = df_reviews \
    .withColumn("text_clean", F.lower(F.col("review_text"))) \
    .withColumn("text_clean", F.regexp_replace("text_clean", "[^a-z\\s]", ""))

# Apply sentiment analysis
df_sentiment = df_clean.withColumn(
    "sentiment_score",
    sentiment_udf(F.col("text_clean"))
)

# Aggregate results
df_summary = df_sentiment \
    .groupBy("product_id") \
    .agg(
        F.mean("sentiment_score").alias("avg_sentiment"),
        F.count("*").alias("num_reviews")
    )

df_summary.write.parquet("s3://reviews/sentiment_summary.parquet")
```

---

## Performance Comparison

### When to Use What

| Task | PySpark | Pandas | PyTorch | Reasoning |
|------|---------|--------|---------|-----------|
| **Load 1TB CSV** | ✅ | ❌ | ❌ | Only PySpark handles distributed I/O |
| **Filter 100M rows** | ✅ | ❌ | ❌ | PySpark optimized for large filtering |
| **Group by aggregation (1B rows)** | ✅ | ❌ | ❌ | Distributed aggregation |
| **Join two 100GB tables** | ✅ | ❌ | ❌ | Distributed join |
| **Train neural network** | ❌ | ❌ | ✅ | PyTorch designed for deep learning |
| **Feature engineering (1M rows)** | ✅ | ✅ | ❌ | Both work, PySpark better for parallel |
| **Quick EDA (10K rows)** | ❌ | ✅ | ❌ | Pandas faster for small data |
| **Matrix operations** | ❌ | ❌ | ✅ | PyTorch optimized, GPU support |
| **Inference on 1B records** | ✅+✅ | ❌ | ❌ | PySpark + PyTorch (Pandas UDF) |
| **Real-time prediction** | ❌ | ❌ | ✅ | PyTorch for low latency |

---

## Best Practices Summary

### ✅ DO

1. **Use PySpark for data preprocessing** - It's designed for big data
2. **Sample before training** - PyTorch works best with in-memory data
3. **Broadcast models** - Avoid loading model per partition
4. **Cache intelligently** - Cache DataFrames used multiple times
5. **Use Pandas as bridge** - It's the natural interface between PySpark and PyTorch
6. **Monitor memory usage** - Watch for OOM errors with large conversions
7. **Partition wisely** - More partitions = better parallelism
8. **Use GPU when available** - Significant speedup for PyTorch
9. **Test on small data first** - Validate pipeline before scaling
10. **Profile your code** - Use Spark UI to identify bottlenecks

### ❌ DON'T

1. **Don't convert entire large DataFrame to Pandas** - Use sampling
2. **Don't load model in each UDF call** - Use broadcasting
3. **Don't ignore data types** - Match tensor types to operations
4. **Don't skip caching** - If DataFrame used multiple times
5. **Don't use PyTorch for data preprocessing** - Use PySpark/Pandas
6. **Don't collect() on large DataFrames** - Use write() instead
7. **Don't use too many/few partitions** - Balance parallelism and overhead
8. **Don't ignore lazy evaluation** - Understand when Spark executes
9. **Don't train on full dataset** - Sample or use distributed training
10. **Don't forget to unpersist** - Free memory when done

---

## Troubleshooting Guide

### Issue: OutOfMemoryError

**Symptoms:** Driver or executor crashes with OOM

**Solutions:**
```python
# 1. Sample data before conversion
df_sample = df_spark.sample(0.01).toPandas()

# 2. Select only needed columns
df_pandas = df_spark.select("col1", "col2").toPandas()

# 3. Process in batches
for batch in df_spark.randomSplit([0.1] * 10):
    process_batch(batch.toPandas())

# 4. Increase executor memory
spark.conf.set("spark.executor.memory", "8g")
```

### Issue: Slow UDF Execution

**Symptoms:** Pandas UDF takes hours

**Solutions:**
```python
# 1. Broadcast model (don't load per call)
bc_model = spark.sparkContext.broadcast(model.state_dict())

# 2. Use iterator UDF for large batches
@pandas_udf(FloatType(), PandasUDFType.SCALAR_ITER)
def predict(iterator):
    model = load_once()
    for batch in iterator:
        yield predict_batch(batch)

# 3. Increase parallelism
df = df.repartition(200)
```

### Issue: GPU Not Used

**Symptoms:** PyTorch runs on CPU despite GPU available

**Solutions:**
```python
# 1. Check availability
print(torch.cuda.is_available())  # Should be True

# 2. Move model to GPU
device = torch.device("cuda")
model = model.to(device)
X = X.to(device)

# 3. For Pandas UDF, keep on CPU
# (UDFs run on executors without GPU access)
predictions = model(X).cpu().numpy()
```

---

## Conclusion

The combination of **PySpark + PyTorch + Pandas** creates a powerful ecosystem for large-scale machine learning:

- **PySpark**: Handles data at scale (preprocessing, ETL)
- **Pandas**: Provides the bridge (conversions, small data ops)
- **PyTorch**: Powers deep learning (training, inference)

**Key Takeaway:** Use each tool for what it does best, and leverage Pandas as the glue between PySpark and PyTorch.

---

**Ready to build production ML pipelines? 🚀**

*Check out the practical examples in `src/pyspark_pytorch/` for hands-on code.*
