# ETL Pipelines: Framework Comparison Guide
## Understanding ETL and Choosing the Right Tools

---

## Table of Contents
1. [What is ETL?](#what-is-etl)
2. [Why Pandas Falls Short for ETL](#why-pandas-falls-short-for-etl)
3. [Why PySpark Excels at ETL](#why-pyspark-excels-at-etl)
4. [How PyTorch Assists in ETL](#how-pytorch-assists-in-etl)
5. [Real-World ETL Patterns](#real-world-etl-patterns)
6. [Framework Decision Matrix](#framework-decision-matrix)

---

## What is ETL?

### Definition

**ETL** stands for **Extract, Transform, Load** - a fundamental data engineering process for moving and preparing data.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   EXTRACT   │────▶│  TRANSFORM  │────▶│    LOAD     │
│             │     │             │     │             │
│ Read from   │     │ Clean &     │     │ Write to    │
│ Sources     │     │ Process     │     │ Target      │
└─────────────┘     └─────────────┘     └─────────────┘
```

### The Three Phases

#### 1. **Extract** (E)
- **Purpose:** Read data from source systems
- **Sources:** 
  - Databases (PostgreSQL, MySQL, Oracle)
  - Files (CSV, JSON, Parquet, Avro)
  - APIs (REST, SOAP)
  - Streaming (Kafka, Kinesis)
  - Cloud Storage (S3, Azure Blob, GCS)

**Example:**
```python
# Extract from multiple sources
customers = read_from_database("customers")
orders = read_from_s3("s3://data/orders/")
products = read_from_api("https://api.example.com/products")
```

#### 2. **Transform** (T)
- **Purpose:** Clean, enrich, and restructure data
- **Operations:**
  - Data Cleaning (nulls, duplicates, outliers)
  - Data Validation (schema, constraints)
  - Data Enrichment (joins, lookups)
  - Aggregations (sum, count, avg)
  - Data Type Conversions
  - Feature Engineering
  - Business Logic Application
  - ML Model Predictions (with PyTorch)

**Example:**
```python
# Transform data
cleaned = remove_duplicates(customers)
enriched = join_with_demographics(cleaned)
aggregated = calculate_customer_lifetime_value(enriched)
validated = apply_business_rules(aggregated)
```

#### 3. **Load** (L)
- **Purpose:** Write transformed data to target system
- **Targets:**
  - Data Warehouses (Snowflake, Redshift, BigQuery)
  - Data Lakes (S3, HDFS)
  - Databases (PostgreSQL, MongoDB)
  - Analytics Platforms (Tableau, Power BI)
  - ML Feature Stores

**Example:**
```python
# Load to target
write_to_warehouse(validated, "analytics.customers")
write_to_s3(validated, "s3://data-lake/gold/customers/")
write_to_feature_store(validated, "customer_features")
```

---

## Why Pandas Falls Short for ETL

While Pandas is excellent for exploratory analysis, it has **critical limitations** for production ETL pipelines.

### ❌ Problem 1: Memory Constraints

**Pandas loads entire dataset into RAM**

```python
# ❌ FAILS: 100GB CSV file
import pandas as pd
df = pd.read_csv("large_file.csv")  # OutOfMemoryError!
```

**Why This Fails:**
- Single machine memory limit (typically 8-64GB)
- No automatic data distribution
- Cannot process datasets larger than RAM

**Real-World Impact:**
```
Dataset Size: 100GB
Available RAM: 32GB
Result: CRASH 💥
```

### ❌ Problem 2: No Parallelization

**Pandas is single-threaded by default**

```python
# ❌ SLOW: Processes one partition at a time
import pandas as pd

# This runs on ONE CPU core
for file in files:  # 1000 files
    df = pd.read_csv(file)
    df_transformed = transform(df)
    df_transformed.to_csv(f"output/{file}")

# Takes: 1000 files × 10 seconds = 10,000 seconds (2.8 hours)
```

**Why This is Slow:**
- No automatic parallelization
- Doesn't utilize multiple CPU cores
- Manual chunking required for large files

### ❌ Problem 3: No Fault Tolerance

**If Pandas crashes, you lose all progress**

```python
# ❌ NO RECOVERY: If crash at file 500/1000
for i, file in enumerate(files):
    df = pd.read_csv(file)
    process(df)  # Crash here? Start over from 0!
```

**Issues:**
- No checkpointing
- No automatic retry
- No lineage tracking
- Manual error handling required

### ❌ Problem 4: Inefficient Joins on Large Data

**Pandas loads both datasets into memory**

```python
# ❌ OOM: Joining two 50GB tables
customers = pd.read_csv("customers.csv")  # 50GB
orders = pd.read_csv("orders.csv")        # 50GB
result = customers.merge(orders)          # Need 100GB+ RAM!
```

**Problems:**
- No partitioned joins
- No broadcast optimization
- Hash join in memory only

### ❌ Problem 5: No Data Cataloging

**Pandas has no built-in schema management**

```python
# ❌ NO SCHEMA ENFORCEMENT
df = pd.read_csv("data.csv")  # What schema? Unknown!
df['age'] = 'invalid'  # Oops, changed type
# No validation, no history, no lineage
```

### ❌ Problem 6: Manual Scaling

**Scaling Pandas requires complex engineering**

```python
# ❌ COMPLEX: Manual distribution
from multiprocessing import Pool

def process_chunk(file):
    df = pd.read_csv(file)
    return transform(df)

# Manual parallelization
with Pool(8) as pool:
    results = pool.map(process_chunk, files)

# Still limited by single machine!
```

### Real-World Pandas ETL Failure

**Scenario:** Daily ETL for e-commerce company

```python
# Required: Process 500GB of transaction data daily
# Available: Single server with 64GB RAM

import pandas as pd

# ❌ THIS WILL FAIL
transactions = pd.read_csv("transactions.csv")  # 500GB - CRASH!

# ❌ WORKAROUND: Manual chunking
chunks = []
for chunk in pd.read_csv("transactions.csv", chunksize=10000):
    processed = transform(chunk)
    chunks.append(processed)

df = pd.concat(chunks)  # Still might OOM!

# Problems:
# - Slow (hours)
# - Error-prone
# - Not scalable
# - No monitoring
```

### When Pandas IS Appropriate

✅ **Use Pandas for:**
- Exploratory data analysis (< 10GB)
- Prototyping and testing
- Single-file processing
- Local development
- Quick scripts and notebooks
- Data that fits in memory

❌ **Don't use Pandas for:**
- Production ETL pipelines
- Data > 10GB
- Multi-table joins (large data)
- Distributed processing
- Fault-tolerant workflows
- Data lineage tracking

---

## Why PySpark Excels at ETL

PySpark is **purpose-built** for large-scale data processing and production ETL pipelines.

### ✅ Advantage 1: Distributed Processing

**PySpark automatically distributes data across cluster**

```python
from pyspark.sql import SparkSession

# ✅ SUCCESS: 100GB CSV file
spark = SparkSession.builder.appName("ETL").getOrCreate()
df = spark.read.csv("s3://data/large_file.csv")  # Distributed!

# Data automatically partitioned across workers
# Each worker processes its partition in parallel
```

**Architecture:**
```
┌──────────────┐
│    Driver    │  (Coordinates work)
└──────┬───────┘
       │
   ┌───┴────┬─────────┬─────────┐
   │        │         │         │
┌──▼───┐ ┌─▼────┐ ┌──▼───┐ ┌──▼───┐
│Worker│ │Worker│ │Worker│ │Worker│
│ 32GB │ │ 32GB │ │ 32GB │ │ 32GB │
└──────┘ └──────┘ └──────┘ └──────┘

Total Processing Power: 128GB RAM, 32 CPU cores
```

### ✅ Advantage 2: Handles Massive Datasets

**PySpark processes data in chunks (partitions)**

```python
# ✅ Can process TERABYTES
df = spark.read.parquet("s3://data/transactions/")  # 5TB dataset

# Lazy evaluation - doesn't load everything
df_filtered = df.filter(df.amount > 100)
df_aggregated = df_filtered.groupBy("customer_id").sum("amount")

# Only executes when action is called
df_aggregated.write.parquet("s3://output/customer_totals/")
```

**Memory Efficiency:**
```
Traditional (Pandas): Load ALL → Transform → Write
PySpark: Load PARTITION → Transform PARTITION → Write PARTITION
```

### ✅ Advantage 3: Automatic Parallelization

**PySpark distributes work automatically**

```python
# ✅ FAST: Processes all files in parallel
df = spark.read.csv("s3://data/files/*.csv")  # 1000 files

# Automatically:
# - Distributes files across workers
# - Each worker processes multiple files
# - Results combined automatically

# 1000 files ÷ 100 workers = 10 files per worker
# 10 files × 10 seconds = 100 seconds total (vs 10,000 with Pandas)
```

### ✅ Advantage 4: Built-in Fault Tolerance

**PySpark automatically recovers from failures**

```python
# ✅ RESILIENT: If node fails, Spark retries
df = spark.read.parquet("s3://data/input/")

result = df.filter(df.status == "active") \
           .groupBy("category") \
           .count()

result.write.parquet("s3://data/output/")

# If worker crashes:
# - Spark detects failure
# - Reassigns work to healthy worker
# - No data loss (lineage tracking)
```

**Lineage Tracking (DAG):**
```
Read → Filter → GroupBy → Count → Write
  ↓       ↓        ↓        ↓       ↓
Node1   Node2    Node3    Node4   Node5

If Node3 fails → Spark replays from Node2
```

### ✅ Advantage 5: Optimized Joins

**PySpark uses multiple join strategies**

```python
# ✅ EFFICIENT: Broadcast join for small table
customers = spark.read.parquet("s3://data/customers/")  # 1GB
orders = spark.read.parquet("s3://data/orders/")        # 100GB

# Spark automatically broadcasts small table
result = orders.join(customers, "customer_id")  # Fast!

# Strategies:
# - Broadcast Join (small + large)
# - Sort-Merge Join (large + large, sorted)
# - Shuffle Hash Join (large + large, unsorted)
```

### ✅ Advantage 6: Schema Management

**PySpark enforces schemas**

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# ✅ Define schema
schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True)
])

# Read with schema enforcement
df = spark.read.schema(schema).csv("data.csv")

# Automatic validation
# Invalid data → Error or null (configurable)
```

### ✅ Advantage 7: Lazy Evaluation

**PySpark optimizes entire pipeline before execution**

```python
# All transformations are lazy (not executed)
df = spark.read.csv("input.csv")
df_filtered = df.filter(df.age > 25)
df_selected = df_filtered.select("name", "age")
df_sorted = df_selected.orderBy("age")

# Spark builds execution plan
# Optimizes (e.g., pushdown filters, combine operations)
# Only executes on action:
df_sorted.write.parquet("output.parquet")  # NOW it runs

# Benefits:
# - Optimizes entire pipeline
# - Eliminates redundant operations
# - Predicate pushdown
# - Minimal data movement
```

### ✅ Advantage 8: Multiple Data Sources

**PySpark reads from any source**

```python
# ✅ VERSATILE: Connect to anything
# Databases
df_postgres = spark.read.jdbc(url, "table", properties)
df_mysql = spark.read.format("jdbc").load()

# Files
df_csv = spark.read.csv("s3://bucket/data.csv")
df_json = spark.read.json("s3://bucket/data.json")
df_parquet = spark.read.parquet("s3://bucket/data.parquet")
df_avro = spark.read.format("avro").load("s3://bucket/data.avro")

# Streaming
df_kafka = spark.readStream.format("kafka").load()

# Cloud
df_s3 = spark.read.csv("s3://bucket/file.csv")
df_azure = spark.read.csv("wasbs://container@account.blob.core.windows.net/file.csv")
df_gcs = spark.read.csv("gs://bucket/file.csv")
```

### ✅ Advantage 9: Monitoring and Observability

**PySpark provides built-in UI**

```
Spark UI (http://localhost:4040):
- Job progress
- Stage timings
- Task distribution
- Memory usage
- Data skew detection
- Query plans
- Executor metrics
```

### Complete ETL Pipeline with PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Initialize Spark
spark = SparkSession.builder \
    .appName("ProductionETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# ============================================================
# EXTRACT
# ============================================================
print("Extracting data from sources...")

# Define schemas
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_date", StringType(), True)
])

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_date", StringType(), False)
])

# Extract from multiple sources
customers = spark.read.schema(customer_schema).csv("s3://data/customers/")
orders = spark.read.schema(order_schema).csv("s3://data/orders/")
products = spark.read.parquet("s3://data/products/")

print(f"Extracted {customers.count():,} customers")
print(f"Extracted {orders.count():,} orders")

# ============================================================
# TRANSFORM
# ============================================================
print("Transforming data...")

# 1. Data Cleaning
customers_clean = customers \
    .dropDuplicates(["customer_id"]) \
    .filter(F.col("email").isNotNull()) \
    .withColumn("signup_date", F.to_date("signup_date", "yyyy-MM-dd"))

orders_clean = orders \
    .filter(F.col("amount") > 0) \
    .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))

# 2. Data Enrichment (Joins)
orders_enriched = orders_clean \
    .join(customers_clean, "customer_id", "inner") \
    .join(products, "product_id", "inner")

# 3. Aggregations
customer_metrics = orders_enriched \
    .groupBy("customer_id", "name") \
    .agg(
        F.count("order_id").alias("total_orders"),
        F.sum("amount").alias("total_spent"),
        F.avg("amount").alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date")
    )

# 4. Business Logic
customer_metrics = customer_metrics \
    .withColumn("customer_lifetime_value", F.col("total_spent")) \
    .withColumn("days_since_last_order", 
                F.datediff(F.current_date(), F.col("last_order_date"))) \
    .withColumn("customer_segment",
                F.when(F.col("total_spent") > 10000, "Premium")
                 .when(F.col("total_spent") > 1000, "Standard")
                 .otherwise("Basic"))

# 5. Data Quality Checks
invalid_records = customer_metrics.filter(
    (F.col("total_orders") <= 0) | 
    (F.col("total_spent") <= 0)
)

if invalid_records.count() > 0:
    print(f"WARNING: Found {invalid_records.count()} invalid records")
    invalid_records.write.parquet("s3://data/quarantine/invalid_customers/")

# Filter valid records
customer_metrics_valid = customer_metrics.filter(
    (F.col("total_orders") > 0) & 
    (F.col("total_spent") > 0)
)

# ============================================================
# LOAD
# ============================================================
print("Loading data to targets...")

# Load to Data Warehouse (partitioned for performance)
customer_metrics_valid \
    .repartition(10, "customer_segment") \
    .write \
    .mode("overwrite") \
    .partitionBy("customer_segment") \
    .parquet("s3://data-warehouse/customer_analytics/")

# Load to PostgreSQL for reporting
customer_metrics_valid.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://db.example.com:5432/analytics") \
    .option("dbtable", "customer_metrics") \
    .option("user", "etl_user") \
    .option("password", "password") \
    .mode("overwrite") \
    .save()

print("ETL Pipeline Complete!")
print(f"Processed {customer_metrics_valid.count():,} customer records")

# Cleanup
spark.stop()
```

### PySpark ETL Performance Example

**Scenario:** Process 1TB of daily transactions

```python
# Dataset: 1TB (1 billion rows)
# Cluster: 20 workers × 32GB RAM = 640GB total
# Processing time: 15 minutes

# PySpark distributes work:
# 1TB ÷ 20 workers = 50GB per worker (fits in 32GB with partitioning)
# Each worker processes its partition in parallel
# Total time: 15 minutes

# Same job with Pandas (if it could run):
# Single machine: 64GB RAM → Out of Memory
# Manual chunking: ~48 hours (if careful)
```

---

## How PyTorch Assists in ETL

PyTorch adds **machine learning intelligence** to ETL pipelines, enabling advanced transformations.

### Use Case 1: Data Quality with ML

**Problem:** Traditional rule-based validation misses complex patterns

**Solution:** Use PyTorch models for anomaly detection

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
import torch
import torch.nn as nn
import numpy as np

# Train anomaly detection model (Autoencoder)
class AnomalyDetector(nn.Module):
    def __init__(self, input_dim):
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 16)
        )
        self.decoder = nn.Sequential(
            nn.Linear(16, 32),
            nn.ReLU(),
            nn.Linear(32, input_dim)
        )
    
    def forward(self, x):
        encoded = self.encoder(x)
        decoded = self.decoder(encoded)
        return decoded

# Train model on historical clean data
model = AnomalyDetector(input_dim=10)
# ... training code ...
model.eval()

# Broadcast model to Spark workers
broadcast_model = spark.sparkContext.broadcast(model.state_dict())

# ETL with anomaly detection
@pandas_udf("double")
def detect_anomaly(features: pd.Series) -> pd.Series:
    """Detect anomalous records using PyTorch model"""
    model = AnomalyDetector(input_dim=10)
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    # Convert to tensor
    X = np.array(features.tolist())
    X_tensor = torch.from_numpy(X).float()
    
    # Calculate reconstruction error
    with torch.no_grad():
        reconstructed = model(X_tensor)
        error = torch.mean((X_tensor - reconstructed) ** 2, dim=1).numpy()
    
    return pd.Series(error)

# Apply in ETL pipeline
df = spark.read.parquet("s3://data/transactions/")

df_with_anomaly_score = df.withColumn(
    "anomaly_score",
    detect_anomaly(F.array([F.col(c) for c in feature_columns]))
)

# Separate clean vs anomalous data
df_clean = df_with_anomaly_score.filter(F.col("anomaly_score") < 0.1)
df_anomalies = df_with_anomaly_score.filter(F.col("anomaly_score") >= 0.1)

# Route to different destinations
df_clean.write.parquet("s3://data/clean/")
df_anomalies.write.parquet("s3://data/quarantine/")
```

### Use Case 2: Feature Engineering with Deep Learning

**Problem:** Manual feature engineering is time-consuming

**Solution:** Use PyTorch for automated feature extraction

```python
import torchvision.models as models

# Load pre-trained ResNet for image features
resnet = models.resnet50(pretrained=True)
resnet.eval()

# Remove final classification layer (get embeddings)
resnet = nn.Sequential(*list(resnet.children())[:-1])

broadcast_model = spark.sparkContext.broadcast(resnet.state_dict())

@pandas_udf("array<double>")
def extract_image_features(image_paths: pd.Series) -> pd.Series:
    """Extract deep learning features from images"""
    from PIL import Image
    import torchvision.transforms as transforms
    
    # Load model
    model = models.resnet50()
    model = nn.Sequential(*list(model.children())[:-1])
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406],
                           std=[0.229, 0.224, 0.225])
    ])
    
    results = []
    for path in image_paths:
        img = Image.open(path)
        img_tensor = preprocess(img).unsqueeze(0)
        
        with torch.no_grad():
            features = model(img_tensor).squeeze().numpy()
        
        results.append(features.tolist())
    
    return pd.Series(results)

# ETL Pipeline with ML Features
df_images = spark.read.parquet("s3://data/product_images/")

df_enriched = df_images.withColumn(
    "image_embedding",
    extract_image_features(F.col("image_path"))
)

# Now you can:
# - Find similar products
# - Cluster products
# - Detect duplicate images
df_enriched.write.parquet("s3://data/products_with_embeddings/")
```

### Use Case 3: Real-Time Enrichment

**Problem:** Need to add ML predictions during ETL

**Solution:** Use PyTorch for real-time scoring

```python
# Trained sentiment analysis model
class SentimentClassifier(nn.Module):
    def __init__(self, vocab_size, embedding_dim):
        super().__init__()
        self.embedding = nn.Embedding(vocab_size, embedding_dim)
        self.lstm = nn.LSTM(embedding_dim, 128, batch_first=True)
        self.fc = nn.Linear(128, 1)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        embedded = self.embedding(x)
        _, (hidden, _) = self.lstm(embedded)
        output = self.sigmoid(self.fc(hidden.squeeze(0)))
        return output

# Load trained model
model = SentimentClassifier(vocab_size=10000, embedding_dim=100)
model.load_state_dict(torch.load("sentiment_model.pth"))
model.eval()

broadcast_model = spark.sparkContext.broadcast(model.state_dict())

@pandas_udf("double")
def predict_sentiment(texts: pd.Series) -> pd.Series:
    """Predict sentiment score for text"""
    from transformers import BertTokenizer
    
    model = SentimentClassifier(vocab_size=10000, embedding_dim=100)
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
    
    results = []
    for text in texts:
        inputs = tokenizer(text, return_tensors="pt", 
                          truncation=True, max_length=128)
        
        with torch.no_grad():
            score = model(inputs['input_ids']).item()
        
        results.append(score)
    
    return pd.Series(results)

# ETL with ML enrichment
df_reviews = spark.read.json("s3://data/customer_reviews/")

df_enriched = df_reviews.withColumn(
    "sentiment_score",
    predict_sentiment(F.col("review_text"))
).withColumn(
    "sentiment_label",
    F.when(F.col("sentiment_score") > 0.7, "positive")
     .when(F.col("sentiment_score") < 0.3, "negative")
     .otherwise("neutral")
)

# Load enriched data
df_enriched.write.partitionBy("sentiment_label") \
    .parquet("s3://analytics/reviews_with_sentiment/")
```

### Use Case 4: Data Transformation with Embeddings

**Problem:** Need to convert unstructured data to structured

**Solution:** Use PyTorch embeddings

```python
# ETL: Convert text descriptions to embeddings
from transformers import BertModel, BertTokenizer

bert = BertModel.from_pretrained('bert-base-uncased')
bert.eval()
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')

broadcast_model = spark.sparkContext.broadcast(bert.state_dict())

@pandas_udf("array<double>")
def text_to_embedding(texts: pd.Series) -> pd.Series:
    """Convert text to BERT embeddings"""
    model = BertModel.from_pretrained('bert-base-uncased')
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    results = []
    for text in texts:
        inputs = tokenizer(text, return_tensors="pt", 
                          truncation=True, max_length=128)
        
        with torch.no_grad():
            outputs = model(**inputs)
            # Use [CLS] token embedding
            embedding = outputs.last_hidden_state[:, 0, :].squeeze().numpy()
        
        results.append(embedding.tolist())
    
    return pd.Series(results)

# ETL: Transform unstructured text to structured embeddings
df_products = spark.read.json("s3://data/products/")

df_with_embeddings = df_products.withColumn(
    "description_embedding",
    text_to_embedding(F.col("description"))
)

# Now can do vector similarity searches
df_with_embeddings.write.parquet("s3://data/products_searchable/")
```

### Use Case 5: Data Validation with Neural Networks

**Problem:** Complex validation rules (e.g., valid addresses, names)

**Solution:** Use trained PyTorch models for validation

```python
class AddressValidator(nn.Module):
    """Neural network to validate addresses"""
    def __init__(self):
        super().__init__()
        self.lstm = nn.LSTM(input_size=100, hidden_size=64, batch_first=True)
        self.fc = nn.Linear(64, 1)
        self.sigmoid = nn.Sigmoid()
    
    def forward(self, x):
        _, (hidden, _) = self.lstm(x)
        output = self.sigmoid(self.fc(hidden.squeeze(0)))
        return output

# Train on historical valid/invalid addresses
model = AddressValidator()
# ... training ...
model.eval()

broadcast_model = spark.sparkContext.broadcast(model.state_dict())

@pandas_udf("boolean")
def validate_address(addresses: pd.Series) -> pd.Series:
    """Validate addresses using neural network"""
    model = AddressValidator()
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    results = []
    for address in addresses:
        # Convert address to embedding
        embedding = encode_address(address)
        embedding_tensor = torch.from_numpy(embedding).float().unsqueeze(0)
        
        with torch.no_grad():
            validity_score = model(embedding_tensor).item()
        
        results.append(validity_score > 0.5)
    
    return pd.Series(results)

# ETL with ML-based validation
df = spark.read.csv("s3://data/customer_records/")

df_validated = df.withColumn(
    "is_valid_address",
    validate_address(F.col("address"))
)

# Route valid vs invalid
df_valid = df_validated.filter(F.col("is_valid_address"))
df_invalid = df_validated.filter(~F.col("is_valid_address"))

df_valid.write.parquet("s3://data/valid_customers/")
df_invalid.write.parquet("s3://data/invalid_addresses_to_review/")
```

---

## Real-World ETL Patterns

### Pattern 1: Batch ETL with ML Enrichment

**Scenario:** Nightly ETL for customer data with churn prediction

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def customer_etl_with_churn_prediction(spark, date):
    """
    Complete ETL pipeline with ML-based churn prediction
    """
    
    # ============================================================
    # EXTRACT
    # ============================================================
    customers = spark.read.parquet(f"s3://data/customers/date={date}/")
    transactions = spark.read.parquet(f"s3://data/transactions/date={date}/")
    support_tickets = spark.read.parquet(f"s3://data/support/date={date}/")
    
    # ============================================================
    # TRANSFORM (Feature Engineering)
    # ============================================================
    
    # Aggregate transaction features
    customer_tx_features = transactions \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("tx_count_30d"),
            F.sum("amount").alias("tx_total_30d"),
            F.avg("amount").alias("tx_avg_30d"),
            F.max("amount").alias("tx_max_30d"),
            F.stddev("amount").alias("tx_std_30d"),
            F.datediff(F.current_date(), F.max("tx_date")).alias("days_since_last_tx")
        )
    
    # Aggregate support features
    customer_support_features = support_tickets \
        .groupBy("customer_id") \
        .agg(
            F.count("*").alias("ticket_count_30d"),
            F.avg("resolution_time_hours").alias("avg_resolution_time"),
            F.sum(F.when(F.col("status") == "unresolved", 1).otherwise(0)).alias("unresolved_tickets")
        )
    
    # Join all features
    customer_features = customers \
        .join(customer_tx_features, "customer_id", "left") \
        .join(customer_support_features, "customer_id", "left") \
        .fillna(0)
    
    # ============================================================
    # TRANSFORM (ML Prediction)
    # ============================================================
    
    # Load trained churn prediction model
    churn_model = torch.load("models/churn_predictor.pth")
    churn_model.eval()
    broadcast_model = spark.sparkContext.broadcast(churn_model.state_dict())
    
    @pandas_udf("double")
    def predict_churn_probability(features: pd.Series) -> pd.Series:
        model = ChurnPredictor()
        model.load_state_dict(broadcast_model.value)
        model.eval()
        
        X = np.array(features.tolist())
        X_tensor = torch.from_numpy(X).float()
        
        with torch.no_grad():
            probabilities = model(X_tensor).numpy().flatten()
        
        return pd.Series(probabilities)
    
    # Add churn prediction
    feature_cols = ["tx_count_30d", "tx_total_30d", "days_since_last_tx", 
                   "ticket_count_30d", "unresolved_tickets"]
    
    customer_features = customer_features.withColumn(
        "churn_probability",
        predict_churn_probability(F.array([F.col(c) for c in feature_cols]))
    ).withColumn(
        "churn_risk",
        F.when(F.col("churn_probability") > 0.7, "high")
         .when(F.col("churn_probability") > 0.4, "medium")
         .otherwise("low")
    )
    
    # ============================================================
    # LOAD
    # ============================================================
    
    # Write to data warehouse (partitioned by risk)
    customer_features \
        .repartition("churn_risk") \
        .write \
        .mode("overwrite") \
        .partitionBy("churn_risk") \
        .parquet(f"s3://analytics/customer_churn_risk/date={date}/")
    
    # Write high-risk customers to immediate action queue
    high_risk_customers = customer_features.filter(F.col("churn_risk") == "high")
    high_risk_customers.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://crm.example.com:5432/retention") \
        .option("dbtable", "high_risk_customers") \
        .mode("append") \
        .save()
    
    return customer_features.count()

# Run ETL
spark = SparkSession.builder.appName("CustomerChurnETL").getOrCreate()
count = customer_etl_with_churn_prediction(spark, "2024-12-09")
print(f"Processed {count:,} customers")
```

### Pattern 2: Streaming ETL with Real-Time ML

**Scenario:** Real-time fraud detection on payment stream

```python
def streaming_fraud_detection_etl(spark):
    """
    Streaming ETL with real-time fraud detection using PyTorch
    """
    
    # Load fraud detection model
    fraud_model = torch.load("models/fraud_detector.pth")
    fraud_model.eval()
    broadcast_model = spark.sparkContext.broadcast(fraud_model.state_dict())
    
    # Define streaming source (Kafka)
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "payments") \
        .load()
    
    # Parse JSON
    from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
    
    schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("customer_id", StringType()) \
        .add("amount", DoubleType()) \
        .add("merchant", StringType()) \
        .add("timestamp", TimestampType())
    
    payments = stream.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Feature engineering
    payments_features = payments \
        .withColumn("hour", F.hour("timestamp")) \
        .withColumn("day_of_week", F.dayofweek("timestamp")) \
        .withColumn("is_high_amount", (F.col("amount") > 1000).cast("int"))
    
    # ML-based fraud detection
    @pandas_udf("double")
    def detect_fraud(features: pd.Series) -> pd.Series:
        model = FraudDetector()
        model.load_state_dict(broadcast_model.value)
        model.eval()
        
        X = np.array(features.tolist())
        X_tensor = torch.from_numpy(X).float()
        
        with torch.no_grad():
            fraud_scores = model(X_tensor).numpy().flatten()
        
        return pd.Series(fraud_scores)
    
    # Add fraud score
    payments_scored = payments_features.withColumn(
        "fraud_score",
        detect_fraud(F.array(["amount", "hour", "day_of_week", "is_high_amount"]))
    ).withColumn(
        "is_fraud",
        F.col("fraud_score") > 0.8
    )
    
    # Route to different sinks based on fraud detection
    
    # Legitimate transactions → Data warehouse
    legitimate_query = payments_scored \
        .filter(~F.col("is_fraud")) \
        .writeStream \
        .format("parquet") \
        .option("path", "s3://data-warehouse/payments/") \
        .option("checkpointLocation", "s3://checkpoints/legitimate/") \
        .trigger(processingTime="1 minute") \
        .start()
    
    # Fraudulent transactions → Alert system (Kafka)
    fraud_query = payments_scored \
        .filter(F.col("is_fraud")) \
        .select(F.to_json(F.struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "fraud_alerts") \
        .option("checkpointLocation", "s3://checkpoints/fraud/") \
        .start()
    
    # Wait for both streams
    legitimate_query.awaitTermination()
    fraud_query.awaitTermination()

# Run streaming ETL
spark = SparkSession.builder.appName("FraudDetectionETL").getOrCreate()
streaming_fraud_detection_etl(spark)
```

---

## Framework Decision Matrix

### Choose Your Tool Based on Requirements

| Requirement | Pandas | PySpark | PyTorch | Explanation |
|-------------|--------|---------|---------|-------------|
| **Data Size < 10GB** | ✅ | ⚠️ | ❌ | Pandas fine, PySpark overhead |
| **Data Size > 100GB** | ❌ | ✅ | ❌ | Only PySpark handles this |
| **Distributed Processing** | ❌ | ✅ | ❌ | PySpark built for clusters |
| **Fault Tolerance** | ❌ | ✅ | ❌ | PySpark auto-recovery |
| **SQL-like Operations** | ✅ | ✅ | ❌ | Both support DataFrames |
| **Machine Learning** | ⚠️ | ⚠️ | ✅ | PyTorch for neural networks |
| **Deep Learning** | ❌ | ❌ | ✅ | Only PyTorch |
| **GPU Acceleration** | ❌ | ❌ | ✅ | PyTorch supports CUDA |
| **Real-Time Streaming** | ❌ | ✅ | ⚠️ | PySpark Structured Streaming |
| **Multiple Data Sources** | ⚠️ | ✅ | ❌ | PySpark has connectors |
| **Schema Enforcement** | ❌ | ✅ | ❌ | PySpark validates schemas |
| **Monitoring/UI** | ❌ | ✅ | ❌ | Spark UI for observability |
| **Production ETL** | ❌ | ✅ | ⚠️ | PySpark + PyTorch together |

### Recommended Combinations

#### 🏆 **Best Practice: PySpark + PyTorch**

```python
# PySpark for data processing
df = spark.read.parquet("s3://large_data/")
df_processed = df.filter(...).groupBy(...).agg(...)

# PyTorch for ML predictions
@pandas_udf("double")
def predict(features):
    model = torch.load("model.pth")
    return model.predict(features)

df_final = df_processed.withColumn("prediction", predict(F.col("features")))
```

**When to Use:**
- Large-scale ETL (> 100GB)
- ML-enhanced transformations
- Production pipelines

#### ⚠️ **Use with Caution: Pandas + PyTorch**

```python
# Pandas for small data
df = pd.read_csv("small_data.csv")
df_processed = df.groupby("category").agg({"value": "sum"})

# PyTorch for ML
X = torch.from_numpy(df_processed.values).float()
predictions = model(X)
```

**When to Use:**
- Small datasets (< 10GB)
- Prototyping/development
- Single-machine workloads

#### ❌ **Avoid: Pandas-only for ETL**

```python
# ❌ Don't do this for production ETL
df = pd.read_csv("large_file.csv")  # Will crash if > RAM
```

**Why Not:**
- No fault tolerance
- Limited by single machine
- No automatic parallelization

---

## Summary

### 🎯 Key Takeaways

1. **ETL is about moving and transforming data at scale**
   - Extract from sources
   - Transform with business logic
   - Load to targets

2. **Pandas is NOT designed for production ETL**
   - Memory-bound (single machine RAM)
   - No fault tolerance
   - No automatic parallelization
   - Use only for small data or prototyping

3. **PySpark excels at ETL**
   - Distributed processing (handles TBs)
   - Automatic fault tolerance
   - Built-in parallelization
   - Schema enforcement
   - Multiple data source connectors
   - Production-ready monitoring

4. **PyTorch adds ML intelligence to ETL**
   - Anomaly detection for data quality
   - Real-time predictions during transform
   - Feature extraction with deep learning
   - Complex validation with neural networks

5. **Best Practice: PySpark + PyTorch**
   - PySpark for data engineering
   - PyTorch for ML transformations
   - Pandas as the bridge between them

### 📊 Decision Tree

```
Need to process data?
│
├─ Data < 10GB?
│  └─ YES → Use Pandas ✅
│  
└─ Data > 10GB?
   └─ YES → Use PySpark ✅
      │
      ├─ Need ML predictions?
      │  └─ YES → Add PyTorch ✅
      │
      └─ Need streaming?
         └─ YES → Use PySpark Streaming ✅
```

---

**Ready to build production ETL pipelines? 🚀**

*Check out the practical examples in `src/pyspark_pytorch/` and `src/etl/` for hands-on code.*
