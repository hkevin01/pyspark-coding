# SQL Database UDF Guide
## Deploying ML Models in PostgreSQL, BigQuery, and Snowflake

---

## Overview

This guide shows how to use UDFs for ML inference **inside** SQL databases, eliminating data export workflows.

### Traditional Workflow (❌ Inefficient)

```
SQL Database → Export CSV → Python Script → Load Results → Database
   ↓              ↓              ↓               ↓             ↓
 Extract      Manual Step    Run Model      Manual Step   Reload
```

### UDF Workflow (✅ Efficient)

```
SQL Database → UDF (ML Model Inside) → Results
   ↓                  ↓                    ↓
 Extract          Automatic            Automatic
```

---

## 1. PostgreSQL UDFs (PL/Python)

### Setup

```sql
-- Enable PL/Python extension
CREATE EXTENSION plpython3u;
```

### Example 1: Anomaly Detection UDF

```sql
CREATE OR REPLACE FUNCTION detect_anomaly(
    sensor_values DOUBLE PRECISION[]
)
RETURNS TABLE(
    is_anomaly BOOLEAN,
    anomaly_score DOUBLE PRECISION
) AS $$
    import pickle
    import numpy as np
    
    # Load trained Isolation Forest model
    with open('/models/anomaly_detector.pkl', 'rb') as f:
        model = pickle.load(f)
    
    # Convert input to numpy array
    X = np.array([sensor_values])
    
    # Predict
    prediction = model.predict(X)[0]  # -1 for anomaly, 1 for normal
    score = -model.score_samples(X)[0]  # Higher = more anomalous
    
    # Return result
    return [(prediction == -1, float(score))]
$$ LANGUAGE plpython3u;
```

### Usage

```sql
-- Apply anomaly detection in SQL query
SELECT 
    sensor_id,
    timestamp,
    sensor_readings,
    (detect_anomaly(sensor_readings)).*
FROM telemetry_data
WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Find critical anomalies
SELECT 
    sensor_id,
    timestamp,
    detection.is_anomaly,
    detection.anomaly_score
FROM telemetry_data,
     LATERAL detect_anomaly(sensor_readings) AS detection
WHERE detection.is_anomaly = TRUE
  AND detection.anomaly_score > 2.0
ORDER BY detection.anomaly_score DESC;
```

### Example 2: Churn Prediction UDF

```sql
CREATE OR REPLACE FUNCTION predict_churn(
    customer_features DOUBLE PRECISION[]
)
RETURNS TABLE(
    churn_probability DOUBLE PRECISION,
    risk_level TEXT
) AS $$
    import torch
    import torch.nn as nn
    import numpy as np
    
    # Define model architecture
    class ChurnPredictor(nn.Module):
        def __init__(self):
            super().__init__()
            self.network = nn.Sequential(
                nn.Linear(10, 64),
                nn.ReLU(),
                nn.Linear(64, 32),
                nn.ReLU(),
                nn.Linear(32, 1),
                nn.Sigmoid()
            )
        
        def forward(self, x):
            return self.network(x)
    
    # Load model
    model = ChurnPredictor()
    model.load_state_dict(torch.load('/models/churn_model.pth'))
    model.eval()
    
    # Convert input
    X = torch.tensor([customer_features]).float()
    
    # Predict
    with torch.no_grad():
        prob = model(X).item()
    
    # Determine risk level
    if prob > 0.8:
        risk = 'CRITICAL'
    elif prob > 0.6:
        risk = 'HIGH'
    elif prob > 0.4:
        risk = 'MEDIUM'
    else:
        risk = 'LOW'
    
    return [(prob, risk)]
$$ LANGUAGE plpython3u;
```

### Usage

```sql
-- Identify high-risk customers
SELECT 
    customer_id,
    customer_name,
    (predict_churn(ARRAY[
        total_purchases,
        avg_order_value,
        days_since_last_order,
        support_tickets,
        -- ... other features
    ])).*
FROM customers
WHERE (predict_churn(...)).churn_probability > 0.7;

-- Daily churn risk report
CREATE MATERIALIZED VIEW daily_churn_risk AS
SELECT 
    DATE(CURRENT_DATE) as report_date,
    prediction.risk_level,
    COUNT(*) as customer_count,
    AVG(prediction.churn_probability) as avg_churn_prob
FROM customers,
     LATERAL predict_churn(customer_features) AS prediction
GROUP BY prediction.risk_level;
```

---

## 2. BigQuery UDFs (JavaScript & Remote Functions)

### Example 1: JavaScript UDF (Simple)

```sql
-- Simple sentiment scoring in JavaScript
CREATE TEMP FUNCTION sentiment_score(text STRING)
RETURNS FLOAT64
LANGUAGE js AS """
  const positive_words = ['good', 'great', 'excellent', 'love', 'best'];
  const negative_words = ['bad', 'terrible', 'awful', 'hate', 'worst'];
  
  const text_lower = text.toLowerCase();
  
  let pos_count = 0;
  let neg_count = 0;
  
  for (let word of positive_words) {
    if (text_lower.includes(word)) pos_count++;
  }
  
  for (let word of negative_words) {
    if (text_lower.includes(word)) neg_count++;
  }
  
  const total = pos_count + neg_count;
  if (total === 0) return 0.5;
  
  return pos_count / total;
""";

-- Use in query
SELECT 
  review_id,
  review_text,
  sentiment_score(review_text) as sentiment
FROM `project.dataset.reviews`
WHERE sentiment_score(review_text) > 0.7;
```

### Example 2: Remote Function (Python + Cloud Functions)

```sql
-- Create connection to Cloud Function
CREATE FUNCTION `project.dataset.predict_fraud`(
  transaction_features ARRAY<FLOAT64>
) 
RETURNS STRUCT<
  fraud_score FLOAT64,
  is_fraud BOOL,
  risk_level STRING
>
REMOTE WITH CONNECTION `project.region.connection_name`
OPTIONS (
  endpoint = 'https://us-central1-project.cloudfunctions.net/fraud-detection'
);

-- Use in query
SELECT 
  transaction_id,
  amount,
  merchant,
  prediction.fraud_score,
  prediction.is_fraud,
  prediction.risk_level
FROM `project.dataset.transactions`,
     UNNEST([predict_fraud(transaction_features)]) AS prediction
WHERE prediction.is_fraud = TRUE;
```

**Cloud Function Code (fraud-detection):**

```python
# cloud_function.py
import torch
import torch.nn as nn
import functions_framework
import numpy as np

# Load model globally (executed once per cold start)
model = torch.load('/models/fraud_detector.pth')
model.eval()

@functions_framework.http
def predict_fraud(request):
    """BigQuery Remote Function for fraud detection"""
    
    # Parse BigQuery batch request
    request_json = request.get_json()
    calls = request_json['calls']
    
    results = []
    
    for call in calls:
        features = call[0]  # transaction_features array
        
        # Convert to tensor
        X = torch.tensor([features]).float()
        
        # Predict
        with torch.no_grad():
            fraud_score = model(X).item()
        
        # Determine risk
        is_fraud = fraud_score > 0.6
        
        if fraud_score > 0.8:
            risk_level = 'CRITICAL'
        elif fraud_score > 0.6:
            risk_level = 'HIGH'
        else:
            risk_level = 'LOW'
        
        results.append({
            'fraud_score': fraud_score,
            'is_fraud': is_fraud,
            'risk_level': risk_level
        })
    
    return {'replies': results}
```

---

## 3. Snowflake UDFs (Python & Java)

### Example 1: Python UDF

```sql
-- Create Python UDF in Snowflake
CREATE OR REPLACE FUNCTION predict_customer_ltv(
    purchase_history ARRAY,
    customer_age FLOAT,
    account_tenure FLOAT
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('numpy', 'scikit-learn', 'torch')
HANDLER = 'predict_ltv'
AS $$
import numpy as np
import torch
import torch.nn as nn

def predict_ltv(purchase_history, customer_age, account_tenure):
    # Aggregate features
    total_purchases = len(purchase_history)
    avg_purchase = np.mean(purchase_history) if purchase_history else 0
    
    # Create feature vector
    features = [
        total_purchases,
        avg_purchase,
        customer_age,
        account_tenure
    ]
    
    # Simple prediction (in production, load actual model)
    ltv = avg_purchase * total_purchases * 1.5
    
    return float(ltv)
$$;
```

### Usage

```sql
-- Calculate LTV for all customers
SELECT 
    customer_id,
    customer_name,
    predict_customer_ltv(
        purchase_amounts,
        age,
        account_tenure_days
    ) as predicted_ltv
FROM customers
ORDER BY predicted_ltv DESC
LIMIT 100;

-- Segment customers by LTV
SELECT 
    CASE 
        WHEN ltv > 10000 THEN 'Platinum'
        WHEN ltv > 5000 THEN 'Gold'
        WHEN ltv > 1000 THEN 'Silver'
        ELSE 'Bronze'
    END as segment,
    COUNT(*) as customer_count,
    AVG(ltv) as avg_ltv
FROM (
    SELECT 
        customer_id,
        predict_customer_ltv(
            purchase_amounts,
            age,
            account_tenure_days
        ) as ltv
    FROM customers
)
GROUP BY segment;
```

### Example 2: Vectorized Python UDF (Batch Processing)

```sql
CREATE OR REPLACE FUNCTION predict_batch_churn(
    customer_features ARRAY
)
RETURNS TABLE(customer_id INT, churn_prob FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('numpy', 'torch')
HANDLER = 'ChurnPredictor'
AS $$
import numpy as np
import torch
import torch.nn as nn

class ChurnPredictor:
    def __init__(self):
        # Initialize model (load once per batch)
        self.model = self.load_model()
    
    def load_model(self):
        # Define and load PyTorch model
        class Model(nn.Module):
            def __init__(self):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(10, 64),
                    nn.ReLU(),
                    nn.Linear(64, 1),
                    nn.Sigmoid()
                )
            
            def forward(self, x):
                return self.network(x)
        
        model = Model()
        # In production: model.load_state_dict(torch.load('@stage/model.pth'))
        model.eval()
        return model
    
    def process(self, customer_features):
        """Process batch of customers"""
        results = []
        
        for i, features in enumerate(customer_features):
            X = torch.tensor([features]).float()
            
            with torch.no_grad():
                churn_prob = self.model(X).item()
            
            results.append((i, churn_prob))
        
        return results
$$;
```

---

## 4. Streaming Inference (Kafka + Spark UDFs)

### Real-Time Fraud Detection

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType
import torch

# Initialize Spark with Kafka
spark = SparkSession.builder \
    .appName("StreamingFraudDetection") \
    .getOrCreate()

# Load model
model = torch.load("models/fraud_detector.pth")
model.eval()
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# Define UDF
@pandas_udf(FloatType())
def detect_fraud_udf(features: pd.Series) -> pd.Series:
    local_model = FraudDetector()
    local_model.load_state_dict(broadcast_model.value)
    local_model.eval()
    
    X = torch.from_numpy(np.array(features.tolist())).float()
    
    with torch.no_grad():
        scores = local_model(X).numpy().flatten()
    
    return pd.Series(scores)

# Read from Kafka
stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse and apply UDF
transactions = stream.select(
    F.from_json(F.col("value").cast("string"), schema).alias("data")
).select("data.*")

# Apply fraud detection
fraud_scores = transactions.withColumn(
    "fraud_score",
    detect_fraud_udf(F.array([F.col(c) for c in feature_cols]))
).withColumn(
    "is_fraud",
    F.when(F.col("fraud_score") > 0.8, True).otherwise(False)
)

# Route based on fraud score
# Legitimate → Data warehouse
fraud_scores.filter(~F.col("is_fraud")) \
    .writeStream \
    .format("parquet") \
    .option("path", "s3://warehouse/transactions/") \
    .option("checkpointLocation", "s3://checkpoints/legit/") \
    .start()

# Fraudulent → Alert system
fraud_scores.filter(F.col("is_fraud")) \
    .select(F.to_json(F.struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "s3://checkpoints/fraud/") \
    .start()
```

---

## Performance Best Practices

### 1. Model Caching

```python
# ✅ GOOD: Load model once per execution context
model = load_model()  # Outside function

def predict_udf(features):
    return model.predict(features)

# ❌ BAD: Load model every call
def predict_udf(features):
    model = load_model()  # Loaded millions of times!
    return model.predict(features)
```

### 2. Batch Processing

```sql
-- ✅ GOOD: Process in batches
SELECT predict_batch(
    ARRAY_AGG(features)
) FROM table
GROUP BY batch_id;

-- ❌ BAD: Row-by-row
SELECT predict(features)
FROM table;  -- Slow for millions of rows
```

### 3. Materialized Views

```sql
-- Cache predictions for frequently queried data
CREATE MATERIALIZED VIEW customer_risk_scores AS
SELECT 
    customer_id,
    (predict_churn(features)).* 
FROM customers;

-- Refresh periodically
REFRESH MATERIALIZED VIEW customer_risk_scores;
```

---

## Monitoring & Logging

### PostgreSQL

```sql
-- Create logging table
CREATE TABLE ml_predictions_log (
    prediction_id SERIAL PRIMARY KEY,
    model_name TEXT,
    input_features JSONB,
    prediction DOUBLE PRECISION,
    timestamp TIMESTAMP DEFAULT NOW()
);

-- Modified UDF with logging
CREATE OR REPLACE FUNCTION predict_with_logging(features DOUBLE PRECISION[])
RETURNS DOUBLE PRECISION AS $$
    # ... prediction code ...
    
    # Log prediction
    plpy.execute(f"""
        INSERT INTO ml_predictions_log (model_name, input_features, prediction)
        VALUES ('churn_v1', '{features}'::jsonb, {prediction})
    """)
    
    return prediction
$$ LANGUAGE plpython3u;
```

### BigQuery

```sql
-- Log predictions to separate table
INSERT INTO `project.dataset.prediction_log`
SELECT 
    CURRENT_TIMESTAMP() as prediction_time,
    'fraud_detector_v2' as model_name,
    transaction_id,
    prediction.*
FROM transactions,
     UNNEST([predict_fraud(features)]) AS prediction;
```

---

## Summary

### When to Use Each Platform

| Platform | Best For | Limitations |
|----------|----------|-------------|
| **PostgreSQL** | Complex Python/PyTorch models, On-premise | Must install packages on server |
| **BigQuery** | Google Cloud, serverless scaling | Limited Python packages, use Remote Functions for complex models |
| **Snowflake** | Multi-cloud, large-scale batch | Cold start latency for UDFs |
| **Spark Streaming** | Real-time, high throughput | Requires cluster management |

### Key Takeaways

1. **UDFs eliminate data movement** - Process where data lives
2. **Model broadcasting** - Load once, use many times
3. **Batch when possible** - More efficient than row-by-row
4. **Monitor predictions** - Track model performance in production
5. **Cache results** - Use materialized views for frequent queries

---

**Ready to deploy ML models in your database? 🚀**

*Start with the examples in `src/udf_examples/` and adapt to your infrastructure!*
