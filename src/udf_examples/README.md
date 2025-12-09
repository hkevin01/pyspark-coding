# UDF Examples for ML Inference
## User-Defined Functions for Production ML Pipelines

---

## Overview

This package contains production-ready examples of **User-Defined Functions (UDFs)** for embedding machine learning model inference inside data processing systems.

### What is a UDF for Inference?

A UDF for inference wraps a trained ML model and applies it to data **inside** the database or data processing system, eliminating the need to:
- Export data to external Python scripts
- Use manual Excel workflows
- Introduce latency with external API calls

### Benefits

✅ **Real-time inference** - Process data as it arrives  
✅ **Lower latency** - No data movement outside the system  
✅ **Scalability** - Distributed processing with PySpark  
✅ **Production-ready** - Fault-tolerant and monitored  
✅ **SQL-friendly** - Use models in SQL queries  

---

## Examples Included

### 1. **Basic Pandas UDF for Batch Inference** (`01_basic_batch_inference.py`)
- Simple PyTorch model inference
- Batch processing pattern
- Model broadcasting

### 2. **Anomaly Detection UDF** (`02_anomaly_detection_udf.py`)
- Isolation Forest for sensor data
- Real-time anomaly detection
- Streaming-ready

### 3. **Classification UDF** (`03_classification_udf.py`)
- Multi-class classification
- Probability scores
- Confidence thresholds

### 4. **Time Series Forecasting UDF** (`04_time_series_forecast_udf.py`)
- LSTM-based forecasting
- Rolling window features
- Multi-step predictions

### 5. **NLP Sentiment Analysis UDF** (`05_sentiment_analysis_udf.py`)
- BERT-based sentiment scoring
- Text preprocessing
- Batch tokenization

### 6. **Image Classification UDF** (`06_image_classification_udf.py`)
- ResNet for image inference
- Path-based processing
- Distributed image loading

### 7. **Fraud Detection UDF** (`07_fraud_detection_udf.py`)
- Real-time fraud scoring
- Complex feature engineering
- Alert routing

### 8. **Recommendation UDF** (`08_recommendation_udf.py`)
- Collaborative filtering
- User-item embeddings
- Top-K recommendations

### 9. **Data Quality UDF** (`09_data_quality_udf.py`)
- ML-based validation
- Quality scoring
- Automatic quarantine

### 10. **Ensemble Model UDF** (`10_ensemble_inference_udf.py`)
- Multiple model voting
- Weighted averaging
- Confidence intervals

---

## Quick Start

### Installation

```bash
cd /home/kevin/Projects/pyspark-coding
source venv/bin/activate
pip install pyspark pandas numpy torch scikit-learn transformers pillow
```

### Basic Usage

```python
from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder \
    .appName("UDF_Inference") \
    .getOrCreate()

# Load data
df = spark.read.parquet("data/input.parquet")

# Import and apply UDF
from udf_examples.basic_batch_inference import create_inference_udf

predict_udf = create_inference_udf(spark, "models/model.pth")
df_predictions = df.withColumn("prediction", predict_udf(df.features))

# Save results
df_predictions.write.parquet("data/predictions.parquet")
```

---

## Architecture Patterns

### Pattern 1: Broadcast Model (Recommended)

```python
# Load model once
model = torch.load("model.pth")
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

@pandas_udf("float")
def predict_udf(features: pd.Series) -> pd.Series:
    # Each partition loads from broadcast (efficient)
    model = MyModel()
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    # Inference
    X = np.array(features.tolist())
    X_tensor = torch.from_numpy(X).float()
    
    with torch.no_grad():
        predictions = model(X_tensor).numpy()
    
    return pd.Series(predictions)
```

### Pattern 2: Iterator UDF (Large Batches)

```python
@pandas_udf("float", PandasUDFType.SCALAR_ITER)
def predict_iterator(iterator):
    # Load model once per partition
    model = torch.load("model.pth")
    model.eval()
    
    for batch in iterator:
        X = torch.from_numpy(batch.values).float()
        with torch.no_grad():
            predictions = model(X).numpy()
        yield pd.Series(predictions)
```

### Pattern 3: Streaming UDF

```python
def create_streaming_inference():
    model = torch.load("model.pth")
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    @pandas_udf("float")
    def streaming_predict(features: pd.Series) -> pd.Series:
        model = MyModel()
        model.load_state_dict(broadcast_model.value)
        # ... inference logic
        return predictions
    
    return streaming_predict

# Apply to stream
stream_df = spark.readStream.format("kafka").load()
predictions = stream_df.withColumn("pred", streaming_predict(F.col("features")))
query = predictions.writeStream.format("parquet").start()
```

---

## Use Cases

### SQL Databases (Postgres, BigQuery, Snowflake)

```sql
-- Register UDF in database
CREATE FUNCTION predict_failure(sensor_data ARRAY[FLOAT])
RETURNS FLOAT
AS $$
  -- Python UDF calling ML model
  import torch
  model = torch.load('/models/failure_predictor.pth')
  prediction = model(torch.tensor(sensor_data))
  return float(prediction)
$$ LANGUAGE plpython3u;

-- Use in queries
SELECT 
    sensor_id,
    timestamp,
    predict_failure(ARRAY[temp, pressure, vibration]) as failure_prob
FROM telemetry
WHERE failure_prob > 0.8;
```

### Spark Streaming (Real-Time)

```python
# Read from Kafka
stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Apply UDF
from udf_examples.anomaly_detection_udf import create_anomaly_detector

detect_anomaly = create_anomaly_detector(spark, "models/anomaly.pth")

stream_with_anomalies = stream.withColumn(
    "is_anomaly",
    detect_anomaly(F.col("sensor_values"))
)

# Route based on predictions
stream_with_anomalies \
    .filter(F.col("is_anomaly")) \
    .writeStream \
    .format("kafka") \
    .option("topic", "alerts") \
    .start()
```

### Production ETL Pipeline

```python
def daily_etl_with_inference(spark, date):
    # Extract
    df = spark.read.parquet(f"s3://data/raw/date={date}/")
    
    # Transform + ML Inference
    from udf_examples.classification_udf import create_classifier
    
    classify = create_classifier(spark, "models/classifier.pth")
    
    df_enriched = df.withColumn(
        "predicted_class",
        classify(F.array([F.col(c) for c in feature_cols]))
    ).withColumn(
        "confidence",
        F.col("probability").getItem(F.col("predicted_class"))
    )
    
    # Load
    df_enriched.write.partitionBy("predicted_class") \
        .parquet(f"s3://data/enriched/date={date}/")
```

---

## Performance Tips

### 1. Use Broadcasting for Models

```python
# ✅ GOOD: Load once, broadcast to all partitions
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# ❌ BAD: Load from disk in every UDF call
@pandas_udf("float")
def predict(x):
    model = torch.load("model.pth")  # Slow!
```

### 2. Batch Processing

```python
# ✅ GOOD: Process batches efficiently
@pandas_udf("float")
def predict_batch(features: pd.Series) -> pd.Series:
    X = np.array(features.tolist())  # Convert entire batch
    predictions = model(torch.from_numpy(X))
    return pd.Series(predictions)

# ❌ BAD: Row-by-row processing
@pandas_udf("float")
def predict_row(feature: float) -> float:
    return float(model(torch.tensor([feature])))  # Inefficient!
```

### 3. Repartition Data

```python
# ✅ GOOD: Increase parallelism
df = df.repartition(200)  # More partitions = more parallel inference

# ⚠️ Monitor: Too many partitions = overhead
```

### 4. Cache Intermediate Results

```python
# ✅ GOOD: Cache feature engineering results
df_features = df.select("id", "features").cache()
df_predictions = df_features.withColumn("pred", predict_udf(F.col("features")))
df_features.unpersist()
```

---

## Testing

### Unit Tests

```python
# tests/test_udf_examples.py
import pytest
from pyspark.sql import SparkSession
from udf_examples.basic_batch_inference import create_inference_udf

@pytest.fixture
def spark():
    return SparkSession.builder \
        .master("local[2]") \
        .appName("test") \
        .getOrCreate()

def test_basic_inference(spark):
    # Create test data
    df = spark.createDataFrame([
        ([1.0, 2.0, 3.0],),
        ([4.0, 5.0, 6.0],)
    ], ["features"])
    
    # Apply UDF
    predict = create_inference_udf(spark, "models/test_model.pth")
    result = df.withColumn("prediction", predict(F.col("features")))
    
    # Assert
    assert result.count() == 2
    assert "prediction" in result.columns
```

### Integration Tests

```bash
# Run all examples
cd src/udf_examples
python 01_basic_batch_inference.py
python 02_anomaly_detection_udf.py
# ... etc
```

---

## Production Deployment

### Step 1: Package Models

```bash
# Save model artifacts
python -c "
import torch
model = train_model()
torch.save(model.state_dict(), 'models/production_model.pth')
"

# Upload to S3/Cloud Storage
aws s3 cp models/production_model.pth s3://ml-models/v1/
```

### Step 2: Deploy UDF

```python
# production_etl.py
from pyspark.sql import SparkSession
from udf_examples.classification_udf import create_classifier

spark = SparkSession.builder \
    .appName("ProductionETL") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

# Load model from S3
model_path = "s3://ml-models/v1/production_model.pth"
classify = create_classifier(spark, model_path)

# Process data
df = spark.read.parquet("s3://data/input/")
df_predictions = df.withColumn("class", classify(F.col("features")))
df_predictions.write.parquet("s3://data/output/")
```

### Step 3: Monitor

```python
# Add monitoring
from pyspark.sql import functions as F

df_predictions = df_predictions \
    .withColumn("prediction_timestamp", F.current_timestamp()) \
    .withColumn("model_version", F.lit("v1.0"))

# Track metrics
df_predictions.groupBy("predicted_class") \
    .agg(F.count("*").alias("count")) \
    .show()
```

---

## Troubleshooting

### Issue: OutOfMemoryError

**Solution:**
```python
# Increase executor memory
spark = SparkSession.builder \
    .config("spark.executor.memory", "16g") \
    .getOrCreate()

# Or sample data first
df_sample = df.sample(0.1)
```

### Issue: Slow UDF Execution

**Solution:**
```python
# 1. Broadcast model
# 2. Increase partitions
df = df.repartition(200)

# 3. Use iterator UDF for large batches
```

### Issue: Model Not Found

**Solution:**
```python
# Check model path
import os
assert os.path.exists(model_path), f"Model not found: {model_path}"

# Use absolute paths
model_path = os.path.abspath("models/model.pth")
```

---

## References

- [PySpark Pandas UDF Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html)
- [PyTorch Model Deployment](https://pytorch.org/tutorials/beginner/saving_loading_models.html)
- [Production ML Best Practices](https://ml-ops.org/)

---

**Ready to deploy ML models in production? 🚀**

*Explore the examples and adapt them to your use case!*
