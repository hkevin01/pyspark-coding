# Python Ecosystem Integration with PySpark

This package demonstrates how to integrate Python's most powerful data science libraries with PySpark for distributed big data processing.

## 📚 Table of Contents

- [Overview](#overview)
- [Why This Matters](#why-this-matters)
- [Modules](#modules)
- [Quick Start](#quick-start)
- [Installation](#installation)
- [Performance](#performance)
- [Best Practices](#best-practices)
- [Examples](#examples)
- [Resources](#resources)

## 🎯 Overview

PySpark's **biggest advantage over Scala** is access to the entire Python data science ecosystem:

| Library | Purpose | Integration Method |
|---------|---------|-------------------|
| **NumPy** | Fast numerical operations | Pandas UDFs |
| **Pandas** | Rich DataFrame manipulation | Pandas UDFs + Apache Arrow |
| **Scikit-learn** | 100+ ML algorithms | Sample → Train → Broadcast |
| **PyTorch** | Deep learning | Pandas UDFs for inference |
| **Matplotlib** | Custom visualizations | Sample → Plot |
| **Seaborn** | Statistical plots | Sample → Plot |

## 🚀 Why This Matters

### Scala Advantage
- **Performance**: Compiled, JVM-native (2-5x faster for some operations)
- **Type Safety**: Compile-time type checking
- **Native Integration**: Direct access to Spark internals

### Python Advantage (THIS!)
- **Ecosystem**: NumPy, Pandas, Scikit-learn, PyTorch, Matplotlib, Seaborn
- **Community**: 10+ years of data science packages
- **Productivity**: Faster development and prototyping
- **Familiarity**: Most data scientists know Python

**Result**: Python's ecosystem advantage usually outweighs Scala's performance edge!

## 📦 Modules

### 1. NumPy Integration (`01_numpy_integration.py`)
**What it does**: Vectorized numerical operations with Pandas UDFs

**Key Features**:
- Mathematical functions (sqrt, log, exp, sin, cos)
- Statistical operations (standardization, normalization)
- Linear algebra (dot products, distances)
- Random operations (noise, sampling)
- **Performance**: 10-100x faster than pure Python

**Example**:
```python
@pandas_udf(DoubleType())
def numpy_sqrt(x: pd.Series) -> pd.Series:
    return np.sqrt(x)  # Vectorized! Super fast!

df.withColumn("sqrt_value", numpy_sqrt(col("value")))
```

### 2. Pandas Integration (`02_pandas_integration.py`)
**What it does**: Use Pandas' rich API within PySpark

**Key Features**:
- Pandas Scalar UDFs: Column-wise operations
- Pandas Grouped Map: Per-group transformations
- Pandas CoGrouped Map: Complex joins
- Apache Arrow: Zero-copy data transfer
- **Performance**: 10-20x faster than regular Python UDFs

**Example**:
```python
@pandas_udf(StringType())
def pandas_uppercase(s: pd.Series) -> pd.Series:
    return s.str.upper()  # Use Pandas string methods!

# Grouped Map: Rolling averages per store
@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def rolling_avg(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["rolling_avg"] = pdf["sales"].rolling(window=7).mean()
    return pdf

df.groupBy("store").apply(rolling_avg)
```

### 3. Scikit-learn Integration (`03_sklearn_integration.py`)
**What it does**: 100+ ML algorithms on distributed data

**Key Features**:
- Model training on sampled data
- Preprocessing (StandardScaler, encoders)
- Pipelines for reproducible workflows
- Cross-validation and hyperparameter tuning
- Distributed inference via Pandas UDFs

**Example**:
```python
# Train on sample
sample = df.sample(0.1).toPandas()
model = RandomForestClassifier()
model.fit(X_train, y_train)

# Apply to full Spark dataset
@pandas_udf(DoubleType())
def predict(*features):
    X = pd.DataFrame(dict(zip(feature_names, features)))
    return pd.Series(model.predict(X))

df.withColumn("prediction", predict(*feature_cols))
```

### 4. PyTorch Integration (`04_pytorch_integration.py`)
**What it does**: Deep learning at scale

**Key Features**:
- Neural network training on samples
- Distributed inference with Pandas UDFs
- Pre-trained models (ResNet, BERT)
- GPU acceleration
- TorchScript for production

**Example**:
```python
# Define model
class SimpleNN(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc1 = nn.Linear(10, 64)
        self.fc2 = nn.Linear(64, 1)
    
    def forward(self, x):
        x = F.relu(self.fc1(x))
        return self.fc2(x)

# Train on sample
model.fit(X_train, y_train)

# Distributed inference
@pandas_udf(DoubleType())
def pytorch_predict(*features):
    X = torch.FloatTensor(np.column_stack(features))
    with torch.no_grad():
        predictions = model(X)
    return pd.Series(predictions.numpy())
```

### 5. Visualization (`05_visualization.py`)
**What it does**: Beautiful plots from Spark data

**Key Features**:
- Matplotlib for custom plots
- Seaborn for statistical visualizations
- Time series plotting
- Heatmaps, pair plots, facet grids
- **Critical**: Always sample before plotting!

**Example**:
```python
# GOOD: Sample first!
plot_data = df.sample(0.01).toPandas()

# Matplotlib
plt.scatter(plot_data['x'], plot_data['y'])
plt.savefig('scatter.png')

# Seaborn
sns.boxplot(data=plot_data, x='category', y='value')
plt.savefig('boxplot.png')
```

### 6. Complete ML Pipeline (`06_complete_ml_pipeline.py`)
**What it does**: End-to-end ML project combining everything

**Pipeline Stages**:
1. **Data Loading**: PySpark DataFrame
2. **Feature Engineering**: NumPy + Pandas UDFs
3. **Preprocessing**: Scikit-learn (StandardScaler)
4. **Model Training**: Multiple Scikit-learn models
5. **Distributed Inference**: Pandas UDFs
6. **Visualization**: Matplotlib + Seaborn

**Scenario**: Customer churn prediction
- 10,000 customers
- 7 engineered features
- 3 models compared
- Distributed predictions
- 9 comprehensive visualizations

## 🚀 Quick Start

### Run Individual Modules

```bash
# NumPy integration
python src/python_ecosystem/01_numpy_integration.py

# Pandas integration
python src/python_ecosystem/02_pandas_integration.py

# Scikit-learn integration
python src/python_ecosystem/03_sklearn_integration.py

# PyTorch integration
python src/python_ecosystem/04_pytorch_integration.py

# Visualization
python src/python_ecosystem/05_visualization.py

# Complete ML pipeline
python src/python_ecosystem/06_complete_ml_pipeline.py
```

### Interactive Usage

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col
import pandas as pd
import numpy as np

# Enable Apache Arrow
spark = SparkSession.builder \
    .appName("Python Ecosystem") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Create data
df = spark.range(0, 1000)

# Use NumPy in Pandas UDF
@pandas_udf("double")
def sqrt_udf(x: pd.Series) -> pd.Series:
    return np.sqrt(x)

result = df.withColumn("sqrt", sqrt_udf(col("id")))
result.show()
```

## 📦 Installation

### Required Packages

```bash
# PySpark
pip install pyspark

# Data Science Stack
pip install numpy pandas scikit-learn

# Deep Learning (optional)
pip install torch torchvision

# Visualization
pip install matplotlib seaborn

# All at once
pip install pyspark numpy pandas scikit-learn torch matplotlib seaborn
```

### Apache Arrow (Important!)

Apache Arrow is required for Pandas UDFs:

```python
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
    .getOrCreate()
```

## ⚡ Performance

### Pandas UDFs vs Regular Python UDFs

| Operation | Python UDF | Pandas UDF | Speedup |
|-----------|-----------|------------|---------|
| Math operations | ~25s | ~2s | **12x faster** |
| String operations | ~30s | ~3s | **10x faster** |
| Statistical ops | ~40s | ~2s | **20x faster** |

**Why?**
- **Regular UDF**: Serializes each row individually (slow!)
- **Pandas UDF**: Batch processing with Apache Arrow (fast!)

### NumPy Vectorization

| Method | Time (100K rows) | Speedup |
|--------|-----------------|---------|
| Python loop | ~10s | 1x |
| List comprehension | ~5s | 2x |
| NumPy vectorized | ~0.1s | **100x faster** |

## 🎯 Best Practices

### 1. Always Use Pandas UDFs

```python
# ❌ BAD: Regular Python UDF
@udf(DoubleType())
def slow_sqrt(x):
    import math
    return math.sqrt(x)

# ✅ GOOD: Pandas UDF
@pandas_udf(DoubleType())
def fast_sqrt(x: pd.Series) -> pd.Series:
    return np.sqrt(x)
```

### 2. Enable Apache Arrow

```python
# Always configure Arrow!
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### 3. Sample Before Collecting

```python
# ❌ BAD: Collect everything
all_data = df.toPandas()  # Might crash!

# ✅ GOOD: Sample first
sample_data = df.sample(0.01).toPandas()
```

### 4. Train on Samples, Predict on All

```python
# Train Scikit-learn on sample
train_data = df.sample(0.1).toPandas()
model.fit(X_train, y_train)

# Apply to full dataset with Pandas UDF
@pandas_udf(DoubleType())
def predict(*features):
    X = pd.DataFrame(dict(zip(feature_names, features)))
    return pd.Series(model.predict(X))

df.withColumn("prediction", predict(*feature_cols))
```

### 5. Aggregate Before Visualizing

```python
# ✅ GOOD: Aggregate in Spark
agg_df = df.groupBy("category").agg(avg("value"), count("*"))
plot_data = agg_df.toPandas()

# Then plot
plt.bar(plot_data['category'], plot_data['avg(value)'])
```

### 6. Broadcast Models

```python
# For production: broadcast model to all workers
broadcast_model = spark.sparkContext.broadcast(trained_model)

@pandas_udf(DoubleType())
def predict(*features):
    model = broadcast_model.value  # Access broadcasted model
    # ... make predictions
```

## 📝 Examples

### Feature Engineering with NumPy

```python
@pandas_udf(ArrayType(DoubleType()))
def create_features(x: pd.Series) -> pd.Series:
    """Create multiple features at once"""
    def compute(val):
        return [
            float(val),              # original
            float(val ** 2),         # squared
            float(np.sqrt(val)),     # sqrt
            float(np.log(val + 1))   # log
        ]
    return x.apply(compute)

df.withColumn("features", create_features(col("value")))
```

### Rolling Window with Pandas

```python
from pyspark.sql.types import StructType, StructField

schema = StructType([
    StructField("date", DateType()),
    StructField("value", DoubleType()),
    StructField("rolling_avg", DoubleType())
])

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def rolling_window(pdf: pd.DataFrame) -> pd.DataFrame:
    """7-day rolling average per group"""
    pdf = pdf.sort_values("date")
    pdf["rolling_avg"] = pdf["value"].rolling(window=7).mean()
    return pdf

df.groupBy("store_id").apply(rolling_window)
```

### ML Pipeline with Scikit-learn

```python
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier

# Create pipeline
pipeline = Pipeline([
    ('scaler', StandardScaler()),
    ('model', RandomForestClassifier())
])

# Train on sample
train_df = df.sample(0.2).toPandas()
pipeline.fit(X_train, y_train)

# Distributed inference
@pandas_udf(DoubleType())
def pipeline_predict(*features):
    X = pd.DataFrame(dict(zip(feature_names, features)))
    return pd.Series(pipeline.predict(X))
```

### Deep Learning with PyTorch

```python
import torch
import torch.nn as nn

# Define model
model = nn.Sequential(
    nn.Linear(10, 64),
    nn.ReLU(),
    nn.Linear(64, 1)
)

# Train on sample
# ... training code ...

# Distributed inference
@pandas_udf(DoubleType())
def pytorch_inference(*features):
    X_tensor = torch.FloatTensor(np.column_stack(features))
    with torch.no_grad():
        predictions = model(X_tensor)
    return pd.Series(predictions.numpy().flatten())
```

## 📊 Comparison: PySpark vs Scala

| Aspect | PySpark | Scala Spark |
|--------|---------|-------------|
| **Ecosystem** | ✅✅✅ NumPy, Pandas, Scikit-learn, PyTorch | ❌ Limited libraries |
| **Performance** | ✅ Fast with Pandas UDFs | ✅✅ Native JVM performance |
| **ML Libraries** | ✅✅✅ 1000+ packages | ⚠️ Spark MLlib only |
| **Visualization** | ✅✅ Matplotlib, Seaborn | ❌ Limited options |
| **Development Speed** | ✅✅✅ Very fast | ⚠️ Slower |
| **Type Safety** | ❌ Runtime errors | ✅✅ Compile-time checks |
| **Community** | ✅✅✅ Massive data science community | ⚠️ Smaller |
| **Learning Curve** | ✅✅ Easy for data scientists | ⚠️ Steeper |

**Conclusion**: For data science and ML, Python's ecosystem advantage is decisive!

## 🔧 Common Patterns

### Pattern 1: Sample → Process → Scale

```python
# 1. Sample for development
sample = df.sample(0.01).toPandas()

# 2. Develop with Pandas/Scikit-learn
model.fit(X_sample, y_sample)

# 3. Scale with Pandas UDF
@pandas_udf(DoubleType())
def predict(*features):
    return pd.Series(model.predict(X))

df.withColumn("prediction", predict(*feature_cols))
```

### Pattern 2: Aggregate → Visualize

```python
# 1. Aggregate in Spark (handles big data)
agg_df = df.groupBy("category").agg(
    avg("value").alias("avg_val"),
    count("*").alias("count")
)

# 2. Convert to Pandas (small result)
plot_df = agg_df.toPandas()

# 3. Visualize
plt.bar(plot_df['category'], plot_df['avg_val'])
```

### Pattern 3: Broadcast → Apply

```python
# 1. Train model locally
model = train_model(sample_data)

# 2. Broadcast to all workers
broadcast_model = spark.sparkContext.broadcast(model)

# 3. Use in UDF (no re-serialization!)
@pandas_udf(DoubleType())
def predict(*features):
    model = broadcast_model.value
    return pd.Series(model.predict(X))
```

## 📚 Resources

### Official Documentation
- [PySpark Pandas UDFs](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [Apache Arrow](https://arrow.apache.org/docs/python/)
- [NumPy Documentation](https://numpy.org/doc/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Scikit-learn User Guide](https://scikit-learn.org/stable/user_guide.html)
- [PyTorch Tutorials](https://pytorch.org/tutorials/)

### Performance Tuning
- [Optimizing Pandas UDFs](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html#optimization)
- [Apache Arrow Configuration](https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html)
- [PySpark Performance Best Practices](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/best_practices.html)

### Tutorials
- Databricks blog: "New Pandas UDFs"
- Towards Data Science: "PySpark + Scikit-learn"
- Medium: "Deep Learning with PySpark"

## 🎯 Key Takeaways

1. **Python's ecosystem is PySpark's killer feature** - access to NumPy, Pandas, Scikit-learn, PyTorch
2. **Pandas UDFs are 10-20x faster** than regular Python UDFs - always use them!
3. **Apache Arrow is critical** - enable it for best performance
4. **Sample → Train → Broadcast → Apply** - the standard ML pattern
5. **Aggregate in Spark, plot in Pandas** - don't collect big data
6. **This is why PySpark beats Scala** - the ecosystem advantage is huge!

## 📬 Summary

This package demonstrates **why Python is the #1 choice for data science with Spark**:

✅ **NumPy**: 100x faster numerical operations  
✅ **Pandas**: Rich data manipulation APIs  
✅ **Scikit-learn**: 100+ ML algorithms ready to use  
✅ **PyTorch**: State-of-the-art deep learning  
✅ **Matplotlib + Seaborn**: Beautiful visualizations  
✅ **PySpark**: Distributed processing at scale  

**Result**: The entire Python data science ecosystem on distributed big data!

---

**Happy distributed data science! 🚀**
