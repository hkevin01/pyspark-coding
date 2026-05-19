# PySpark + PyTorch Integration Examples

This package demonstrates how to effectively combine PySpark and PyTorch for large-scale machine learning workflows.

## 📚 Examples Overview

### 1. DataFrame to Tensor Conversion (`01_dataframe_to_tensor.py`)
**Purpose:** Learn the fundamentals of converting between PySpark DataFrames and PyTorch tensors.

**What You'll Learn:**
- DataFrame → Pandas → NumPy → Tensor conversion pipeline
- Data type handling (float32, int64)
- Batch processing with tensors
- Reverse conversion (Tensor → DataFrame)
- Memory considerations

**Run:**
```bash
python 01_dataframe_to_tensor.py
```

---

### 2. Feature Engineering + Training (`02_feature_engineering_training.py`)
**Purpose:** Complete ML pipeline using PySpark for feature engineering and PyTorch for model training.

**What You'll Learn:**
- Feature engineering with PySpark SQL functions
- Feature scaling with PySpark MLlib
- Neural network training in PyTorch
- Binary classification workflow
- Model evaluation metrics

**Use Case:** Housing price classification (expensive vs not expensive)

**Run:**
```bash
python 02_feature_engineering_training.py
```

---

### 3. Distributed Inference (`03_distributed_inference.py`)
**Purpose:** Apply PyTorch models to large datasets using distributed inference patterns.

**What You'll Learn:**
- Three inference methods comparison:
  1. Pandas UDF (simple)
  2. Broadcast + Pandas UDF (better)
  3. mapPartitions (most efficient)
- Performance optimization techniques
- Model broadcasting strategies
- Batch prediction at scale

**Run:**
```bash
python 03_distributed_inference.py
```

---

### 4. Image Embeddings (`04_image_embeddings.py`)
**Purpose:** Extract image embeddings with CNN and perform similarity analysis with PySpark.

**What You'll Learn:**
- CNN-based feature extraction
- Embedding generation at scale
- Cosine similarity calculations
- Nearest neighbor search
- Clustering analysis with embeddings

**Use Case:** Image similarity search, duplicate detection, content-based recommendations

**Run:**
```bash
python 04_image_embeddings.py
```

---

### 5. Time Series Forecasting (`05_time_series_forecasting.py`)
**Purpose:** Time series preprocessing with PySpark and forecasting with PyTorch LSTM.

**What You'll Learn:**
- Window functions for rolling features
- Sequence generation for LSTM
- LSTM architecture for forecasting
- Multi-step ahead prediction
- Distributed time series processing

**Use Case:** Demand forecasting, stock prediction, sensor data analysis

**Run:**
```bash
python 05_time_series_forecasting.py
```

---

## 🚀 Quick Start

### Prerequisites

```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install pyspark torch torchvision numpy pandas scikit-learn
```

### Running Examples

Each example is self-contained and can be run independently:

```bash
cd src/pyspark_pytorch

# Run any example
python 01_dataframe_to_tensor.py
python 02_feature_engineering_training.py
python 03_distributed_inference.py
python 04_image_embeddings.py
python 05_time_series_forecasting.py
```

---

## �� Key Integration Patterns

### Pattern 1: PySpark → PyTorch
```python
# Feature engineering with PySpark
df_features = df.withColumn("new_feature", F.col("a") + F.col("b"))

# Convert to tensors
X = torch.from_numpy(df_features.toPandas().values).float()

# Train PyTorch model
model.fit(X, y)
```

### Pattern 2: PyTorch → PySpark (Inference)
```python
# Broadcast model
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# Create UDF
@pandas_udf(FloatType())
def predict_udf(features: pd.Series) -> pd.Series:
    model = load_model(broadcast_model.value)
    return pd.Series(model.predict(features))

# Apply at scale
df_predictions = df.withColumn("prediction", predict_udf(F.col("features")))
```

### Pattern 3: Full Pipeline
```
Raw Data → PySpark (ETL) → PyTorch (Train) → PySpark (Deploy) → Predictions
```

---

## 📊 When to Use What

| <sub>Task</sub> | <sub>Use PySpark</sub> | <sub>Use PyTorch</sub> | <sub>Why</sub> |
|------|-------------|-------------|-----|
| <sub>**Data Loading**</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>Distributed I/O, handles large files</sub> |
| <sub>**Feature Engineering**</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>SQL-like operations, window functions</sub> |
| <sub>**Data Aggregation**</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>Optimized for groupBy, joins</sub> |
| <sub>**Model Training**</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>Deep learning, GPU support</sub> |
| <sub>**Model Architecture**</sub> | <sub>❌</sub> | <sub>✅</sub> | <sub>Flexible neural networks</sub> |
| <sub>**Batch Inference**</sub> | <sub>✅</sub> | <sub>✅</sub> | <sub>Both (use Pandas UDF for PyTorch)</sub> |
| <sub>**Data Sampling**</sub> | <sub>✅</sub> | <sub>❌</sub> | <sub>Random sampling from billions of rows</sub> |

---

## 🔧 Performance Tips

### 1. Memory Management
```python
# Cache only what you need
df.select("important_cols").cache()

# Unpersist when done
df.unpersist()
```

### 2. Efficient Conversion
```python
# Good: Sample before converting
df_sample = df.sample(0.1)
X = torch.from_numpy(df_sample.toPandas().values)

# Bad: Convert entire large dataset
X = torch.from_numpy(df.toPandas().values)  # OOM!
```

### 3. Broadcast Models
```python
# For models < 100MB
broadcast_model = sc.broadcast(model.state_dict())

# For models > 100MB
# Save to shared storage, load in UDF
```

### 4. Partitioning
```python
# More partitions = better parallelism
df = df.repartition(200)

# But not too many (overhead)
# Rule of thumb: 2-4x number of cores
```

---

## 🏗️ Architecture Patterns

### Pattern A: Batch Training
```
1. PySpark: Load & preprocess large dataset
2. PySpark: Sample training data
3. PyTorch: Train model on sample
4. PySpark: Apply model to full dataset
```

### Pattern B: Streaming
```
1. PySpark Streaming: Ingest real-time data
2. PySpark: Feature engineering
3. PyTorch (UDF): Real-time inference
4. PySpark: Write predictions
```

### Pattern C: Transfer Learning
```
1. PyTorch: Pretrained model (ResNet, BERT)
2. PySpark: Load images/text at scale
3. PySpark + PyTorch: Extract embeddings
4. PySpark: Clustering, similarity, analytics
```

---

## 🐛 Common Issues & Solutions

### Issue 1: Out of Memory
**Problem:** `OutOfMemoryError` when converting DataFrame to Pandas
```python
# Bad
X = df.toPandas().values  # OOM on large data
```

**Solution:** Sample or partition
```python
# Good
X = df.sample(0.01).toPandas().values
# Or process in partitions with mapPartitions
```

### Issue 2: Slow UDF Execution
**Problem:** Model loaded multiple times
```python
# Bad: Model loaded for each batch
@pandas_udf(FloatType())
def predict(x):
    model = load_model()  # Slow!
    return model.predict(x)
```

**Solution:** Broadcast model
```python
# Good: Load once, broadcast
broadcast_model = sc.broadcast(model)

@pandas_udf(FloatType())
def predict(x):
    model = use_broadcast(broadcast_model)
    return model.predict(x)
```

### Issue 3: Data Skew
**Problem:** Some partitions much larger than others

**Solution:**
```python
# Repartition by column
df = df.repartition(200, "key_column")

# Or use salting for skewed keys
df = df.withColumn("salt", F.rand() * 10)
```

---

## 📈 Scaling Considerations

| <sub>Dataset Size</sub> | <sub>Approach</sub> | <sub>Tools</sub> |
|--------------|----------|-------|
| <sub>**< 1GB**</sub> | <sub>Single machine</sub> | <sub>Pandas + PyTorch</sub> |
| <sub>**1-100GB**</sub> | <sub>PySpark + PyTorch</sub> | <sub>Local cluster</sub> |
| <sub>**100GB-1TB**</sub> | <sub>Distributed</sub> | <sub>PySpark cluster + GPU</sub> |
| <sub>**> 1TB**</sub> | <sub>Partitioned training</sub> | <sub>Multiple PySpark jobs</sub> |

---

## 🔗 Additional Resources

### Documentation
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [PyTorch Documentation](https://pytorch.org/docs/stable/index.html)
- [PySpark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)

### Related Topics
- **Koalas/Pandas API on Spark:** Pandas-like API for PySpark
- **Horovod:** Distributed deep learning with PyTorch + Spark
- **Petastorm:** Parquet-based dataset for PyTorch
- **Ray on Spark:** Distributed Python framework

---

## 💡 Best Practices Summary

1. ✅ **Use PySpark for data preprocessing** - It's designed for big data
2. ✅ **Use PyTorch for model training** - Better deep learning support
3. ✅ **Broadcast models for inference** - Avoid repeated loading
4. ✅ **Sample before training** - PyTorch works best on data that fits in memory
5. ✅ **Cache intelligently** - Only cache DataFrames used multiple times
6. ✅ **Monitor Spark UI** - Identify bottlenecks and skew
7. ✅ **Test on small data first** - Validate pipeline before scaling
8. ✅ **Use appropriate data types** - float32 for features, int64 for labels
9. ✅ **Partition wisely** - 2-4x number of cores is a good starting point
10. ✅ **Profile your code** - Identify slowest operations

---

## 🎓 Learning Path

1. **Start Here:** `01_dataframe_to_tensor.py` - Understand basics
2. **Then:** `02_feature_engineering_training.py` - Complete workflow
3. **Next:** `03_distributed_inference.py` - Scale inference
4. **Advanced:** `04_image_embeddings.py` or `05_time_series_forecasting.py`

---

## 📞 Support

For issues or questions:
- Review the example code comments
- Check common issues section above
- Refer to PySpark/PyTorch documentation
- Test with smaller datasets first

---

**Happy Learning! 🚀**

*These examples demonstrate production-ready patterns for integrating PySpark and PyTorch in real-world ML pipelines.*