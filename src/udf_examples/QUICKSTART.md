# UDF Examples - Quick Start Guide 🚀

## What You've Built

A complete collection of **User-Defined Functions (UDFs) for ML Inference** inside databases and streaming systems. This eliminates data export and enables **in-database ML inference**.

### 📂 Package Contents

```
udf_examples/
├── README.md                          # Complete architecture guide
├── SQL_DATABASE_UDF_GUIDE.md         # PostgreSQL/BigQuery/Snowflake deployment
├── QUICKSTART.md                      # This file
├── run_all_examples.py                # Demo runner script
├── __init__.py                        # Package initialization
├── 01_basic_batch_inference.py        # PyTorch batch inference pattern
├── 02_anomaly_detection_udf.py        # Isolation Forest anomaly detection
├── 03_classification_udf.py           # Multi-class classification
├── 04_time_series_forecast_udf.py     # LSTM forecasting
├── 05_sentiment_analysis_udf.py       # NLP sentiment analysis
└── 07_fraud_detection_udf.py          # Real-time fraud detection
```

---

## 🏃 Quick Start (5 Minutes)

### 1. List Available Examples
```bash
cd /home/kevin/Projects/pyspark-coding/src/udf_examples

# See all examples
python run_all_examples.py --list
```

### 2. Run Single Example
```bash
# Run basic inference example
python run_all_examples.py --example 1

# Or run directly
python 01_basic_batch_inference.py
```

### 3. Run All Examples
```bash
# Sequential execution with progress tracking
python run_all_examples.py
```

---

## 📚 Examples Overview

| # | Example | Use Case | Key Technique |
|---|---------|----------|---------------|
| **1** | Basic Batch Inference | Product demand prediction | PyTorch + Pandas UDF |
| **2** | Anomaly Detection | Sensor monitoring | Isolation Forest |
| **3** | Classification | Customer segmentation | Multi-class NN with confidence |
| **4** | Time Series Forecast | Sales forecasting | LSTM (7-day ahead) |
| **5** | Sentiment Analysis | Review scoring | NLP keyword-based |
| **7** | Fraud Detection | Transaction monitoring | 4-tier risk classification |

---

## 🎯 Interview Preparation

### Core Concepts to Master

1. **What are UDFs for Inference?**
   - Embed ML models directly in databases/streaming systems
   - Avoid data export → faster, simpler pipelines
   - Example: `SELECT predict_churn(customer_id) FROM users`

2. **Why Use UDFs?**
   - **Reduced Latency**: No data movement
   - **Simplified Architecture**: No external API calls
   - **SQL-Native**: Data teams can use ML without Python scripts
   - **Real-Time**: Streaming inference with Kafka + Spark

3. **Key Patterns**

   **Pattern 1: Model Broadcasting (Most Important!)**
   ```python
   # Load model ONCE per partition, not per row
   broadcast_model = spark.sparkContext.broadcast(model)
   
   @pandas_udf(FloatType())
   def predict_udf(features: pd.Series) -> pd.Series:
       model = broadcast_model.value  # Reuse loaded model
       return model.predict(features)
   ```

   **Pattern 2: Batch Processing**
   ```python
   # Process entire partitions at once for efficiency
   @pandas_udf(StructType([...]))
   def batch_predict(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
       model = load_model()
       for batch in iterator:
           predictions = model.predict(batch)
           yield batch.assign(prediction=predictions)
   ```

   **Pattern 3: Streaming Inference**
   ```python
   # Real-time inference on Kafka streams
   stream = spark.readStream.format("kafka")...
   predictions = stream.withColumn("fraud_score", fraud_udf(col("features")))
   predictions.writeStream.format("console").start()
   ```

---

## 💼 SQL Database Deployment

### PostgreSQL Example
```sql
-- Create PL/Python UDF
CREATE OR REPLACE FUNCTION detect_anomaly(sensor_data FLOAT[])
RETURNS TABLE(is_anomaly BOOLEAN, score FLOAT) AS $$
    from sklearn.ensemble import IsolationForest
    import numpy as np
    
    model = IsolationForest()
    model.fit(np.array(sensor_data).reshape(-1, 1))
    
    score = model.score_samples(sensor_data)
    is_anomaly = model.predict(sensor_data) == -1
    
    return [(bool(ia), float(s)) for ia, s in zip(is_anomaly, score)]
$$ LANGUAGE plpython3u;

-- Use it
SELECT sensor_id, detect_anomaly(readings)
FROM sensor_telemetry
WHERE timestamp > NOW() - INTERVAL '1 hour';
```

### BigQuery Example
```sql
-- Remote Function (calls Cloud Function with PyTorch model)
CREATE FUNCTION predict_churn(customer_id STRING)
RETURNS FLOAT64
REMOTE WITH CONNECTION `project.us.connection`
OPTIONS (
  endpoint = 'https://us-central1-project.cloudfunctions.net/predict-churn'
);

-- Use it
SELECT customer_id, predict_churn(customer_id) as churn_probability
FROM `project.dataset.customers`
WHERE churn_probability > 0.7;
```

### Snowflake Example
```sql
-- Python UDF with packages
CREATE OR REPLACE FUNCTION predict_ltv(features ARRAY)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
PACKAGES = ('torch', 'numpy')
HANDLER = 'predict'
AS $$
import torch
import numpy as np

def predict(features):
    model = torch.load('/tmp/ltv_model.pt')
    return float(model(torch.tensor(features)).item())
$$;

-- Use it
SELECT customer_id, predict_ltv(ARRAY_CONSTRUCT(age, tenure, spend))
FROM customers;
```

---

## 🔥 Performance Best Practices

### 1. Model Broadcasting (Critical!)
```python
# ❌ BAD: Load model every row
@pandas_udf(FloatType())
def slow_predict(x: pd.Series) -> pd.Series:
    model = torch.load('model.pt')  # Loads 1000x times!
    return model.predict(x)

# ✅ GOOD: Load once per partition
broadcast_model = spark.sparkContext.broadcast(torch.load('model.pt'))

@pandas_udf(FloatType())
def fast_predict(x: pd.Series) -> pd.Series:
    model = broadcast_model.value
    return model.predict(x)
```

### 2. Batch Processing
```python
# Process entire batches for GPU efficiency
@pandas_udf(StructType([...]))
def batch_predict(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    model = load_model().cuda()
    
    for batch in iterator:
        # Convert entire batch to tensor
        tensor = torch.tensor(batch['features'].values).cuda()
        predictions = model(tensor).cpu().numpy()
        yield batch.assign(prediction=predictions)
```

### 3. Repartitioning
```python
# Balance workload across executors
df = df.repartition(spark.sparkContext.defaultParallelism * 2)
df = df.withColumn('prediction', predict_udf(col('features')))
```

---

## 🧪 Testing Your UDFs

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture
def spark():
    return SparkSession.builder.master("local[2]").getOrCreate()

def test_fraud_detection_udf(spark):
    # Test data
    test_df = spark.createDataFrame([
        (1, [100.0, 1, 5]),   # Normal
        (2, [9999.0, 0, 1]),  # Suspicious
    ], ['id', 'features'])
    
    # Apply UDF
    result = test_df.withColumn('fraud_score', fraud_udf(col('features')))
    
    # Assertions
    scores = result.select('fraud_score').collect()
    assert scores[0][0] < 0.3  # Normal transaction
    assert scores[1][0] > 0.7  # Fraudulent transaction
```

---

## 🚀 Production Deployment Checklist

- [ ] **Model Versioning**: Tag models with version numbers
- [ ] **Monitoring**: Log prediction counts, latencies, errors
- [ ] **Caching**: Use materialized views for frequent queries
- [ ] **Error Handling**: Graceful fallback for missing features
- [ ] **Testing**: Unit tests + integration tests on sample data
- [ ] **Documentation**: SQL examples for data analysts
- [ ] **Performance**: Benchmark with production data volumes
- [ ] **Rollback Plan**: Keep previous model version available

---

## 📖 Study Plan for Interview

### Day 1: Core Concepts
- [ ] Read `README.md` (architecture patterns)
- [ ] Run examples 1, 2, 7 (basic inference, anomaly, fraud)
- [ ] Understand model broadcasting pattern

### Day 2: SQL Database Deployment
- [ ] Read `SQL_DATABASE_UDF_GUIDE.md`
- [ ] Study PostgreSQL PL/Python examples
- [ ] Understand BigQuery Remote Functions architecture
- [ ] Review Snowflake Python UDFs

### Day 3: Streaming & Advanced
- [ ] Run examples 3, 4, 5 (classification, forecasting, NLP)
- [ ] Study Kafka + Spark Structured Streaming example
- [ ] Practice explaining benefits of in-database inference

### Day 4: Interview Prep
- [ ] Practice explaining: "What are UDFs for inference?"
- [ ] Draw architecture diagram (Spark → Broadcast Model → Executors)
- [ ] Prepare example: "At my last project, I used UDFs to..."
- [ ] Review performance optimization techniques

---

## 🎤 Interview Talking Points

### Question: "How would you deploy ML models in production?"

**Answer Structure:**
1. **Context**: "I'd use UDFs to embed models directly in the data pipeline"
2. **Example**: "For fraud detection, I created a Spark UDF that loads a PyTorch model once per partition using broadcasting"
3. **Benefits**: "This eliminates data export, reduces latency from minutes to seconds, and lets SQL users access ML predictions natively"
4. **Code**: "The key is broadcasting - load the model once, not per row: `broadcast_model = sc.broadcast(model)`"
5. **Production**: "I'd monitor with logging tables, use materialized views for common queries, and version models for rollback"

### Question: "How do you handle model performance at scale?"

**Answer Structure:**
1. **Batch Processing**: "Use Pandas UDF with iterator pattern to process entire partitions"
2. **Repartitioning**: "Balance workload: `df.repartition(num_executors * 2)`"
3. **GPU Optimization**: "For deep learning, batch predictions for GPU efficiency"
4. **Caching**: "Cache frequently-used predictions in materialized views"
5. **Monitoring**: "Track inference latency, errors, and throughput"

---

## 📞 Need Help?

### Debugging
- Check Spark logs: `spark.sparkContext.setLogLevel("INFO")`
- Validate model loading: Add print statements in UDF
- Test on small data first: `df.limit(100)`

### Resources
- PySpark UDF docs: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html
- PostgreSQL PL/Python: https://www.postgresql.org/docs/current/plpython.html
- BigQuery Remote Functions: https://cloud.google.com/bigquery/docs/remote-functions
- Snowflake Python UDFs: https://docs.snowflake.com/en/developer-guide/udf/python/udf-python.html

---

## ✅ You're Ready When...

- [ ] You can explain UDF inference concept in 30 seconds
- [ ] You understand model broadcasting pattern
- [ ] You've run all 6 examples successfully
- [ ] You can write a simple Pandas UDF from scratch
- [ ] You know when to use PostgreSQL vs BigQuery vs Snowflake UDFs
- [ ] You can describe streaming inference with Kafka + Spark
- [ ] You've reviewed the SQL examples in the guide

---

**Good luck with your ICF PySpark interview! 🎯**

Remember: **UDFs = ML Inference Where Your Data Lives**
