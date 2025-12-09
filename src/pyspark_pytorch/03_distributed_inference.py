"""
Example 3: Distributed Inference with PySpark UDF and PyTorch
==============================================================
Demonstrates how to use PyTorch models within PySpark UDFs for distributed inference.
"""

import numpy as np
import torch
import torch.nn as nn
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, ArrayType, StructType, StructField

print("=" * 80)
print("DISTRIBUTED INFERENCE: PYSPARK UDF + PYTORCH MODEL")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("DistributedInference") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# 1. DEFINE AND TRAIN A SIMPLE MODEL
# ============================================================================
print("\n1. DEFINE AND TRAIN A SIMPLE MODEL")
print("-" * 80)

class SimpleClassifier(nn.Module):
    """Simple 2-layer neural network"""
    def __init__(self, input_size=4, hidden_size=10, num_classes=3):
        super(SimpleClassifier, self).__init__()
        self.fc1 = nn.Linear(input_size, hidden_size)
        self.relu = nn.ReLU()
        self.fc2 = nn.Linear(hidden_size, num_classes)
    
    def forward(self, x):
        out = self.fc1(x)
        out = self.relu(out)
        out = self.fc2(out)
        return out

# Create and initialize model
model = SimpleClassifier(input_size=4, hidden_size=10, num_classes=3)

# Train on synthetic data (simplified for demo)
torch.manual_seed(42)
X_train = torch.randn(100, 4)
y_train = torch.randint(0, 3, (100,))

criterion = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

print("Training simple model...")
for epoch in range(20):
    outputs = model(X_train)
    loss = criterion(outputs, y_train)
    
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

print(f"Final training loss: {loss.item():.4f}")

# Set model to evaluation mode
model.eval()

# ============================================================================
# 2. SAVE MODEL FOR DISTRIBUTED USE
# ============================================================================
print("\n2. SAVE MODEL FOR DISTRIBUTED USE")
print("-" * 80)

# Save model state dict
model_path = "/tmp/simple_classifier.pth"
torch.save(model.state_dict(), model_path)
print(f"Model saved to: {model_path}")

# ============================================================================
# 3. CREATE LARGE DATASET FOR INFERENCE
# ============================================================================
print("\n3. CREATE LARGE DATASET FOR INFERENCE")
print("-" * 80)

# Create synthetic data
np.random.seed(42)
n_samples = 1000

data = []
for i in range(n_samples):
    features = np.random.randn(4).tolist()
    data.append((i, features[0], features[1], features[2], features[3]))

df = spark.createDataFrame(data, ["id", "f1", "f2", "f3", "f4"])

print(f"Dataset size: {df.count()} rows")
df.show(10)

# ============================================================================
# 4. METHOD 1: PANDAS UDF WITH PYTORCH
# ============================================================================
print("\n4. METHOD 1: PANDAS UDF WITH PYTORCH")
print("-" * 80)

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd

# Define Pandas UDF for batch inference
@pandas_udf(FloatType())
def predict_class_pandas(f1: pd.Series, f2: pd.Series, f3: pd.Series, f4: pd.Series) -> pd.Series:
    """Predict class using PyTorch model with Pandas UDF"""
    # Load model (in real scenario, this could be broadcast)
    model = SimpleClassifier(input_size=4, hidden_size=10, num_classes=3)
    model.load_state_dict(torch.load("/tmp/simple_classifier.pth"))
    model.eval()
    
    # Stack features into numpy array
    features = np.column_stack([f1, f2, f3, f4])
    
    # Convert to tensor
    X = torch.from_numpy(features).float()
    
    # Make predictions
    with torch.no_grad():
        outputs = model(X)
        _, predictions = torch.max(outputs, 1)
    
    # Return as pandas Series
    return pd.Series(predictions.numpy())

# Apply UDF
df_with_predictions = df.withColumn(
    "predicted_class",
    predict_class_pandas(F.col("f1"), F.col("f2"), F.col("f3"), F.col("f4"))
)

print("Predictions using Pandas UDF:")
df_with_predictions.select("id", "f1", "f2", "f3", "f4", "predicted_class").show(10)

print(f"\nClass distribution:")
df_with_predictions.groupBy("predicted_class").count().orderBy("predicted_class").show()

# ============================================================================
# 5. METHOD 2: BROADCAST MODEL + PANDAS UDF
# ============================================================================
print("\n5. METHOD 2: BROADCAST MODEL + PANDAS UDF (MORE EFFICIENT)")
print("-" * 80)

# Broadcast model state dict to all workers
model_state = model.state_dict()
broadcast_model = spark.sparkContext.broadcast(model_state)

@pandas_udf(ArrayType(FloatType()))
def predict_probabilities(f1: pd.Series, f2: pd.Series, f3: pd.Series, f4: pd.Series) -> pd.Series:
    """Return class probabilities using broadcast model"""
    # Load model from broadcast variable
    model = SimpleClassifier(input_size=4, hidden_size=10, num_classes=3)
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    # Prepare features
    features = np.column_stack([f1, f2, f3, f4])
    X = torch.from_numpy(features).float()
    
    # Get probabilities
    with torch.no_grad():
        outputs = model(X)
        probabilities = torch.softmax(outputs, dim=1)
    
    # Convert to list of lists
    probs_list = probabilities.numpy().tolist()
    
    return pd.Series(probs_list)

# Apply UDF to get probabilities
df_with_probs = df.withColumn(
    "class_probabilities",
    predict_probabilities(F.col("f1"), F.col("f2"), F.col("f3"), F.col("f4"))
)

print("Predictions with probabilities:")
df_with_probs.select("id", "class_probabilities").show(10, truncate=False)

# Extract max probability class
df_with_probs = df_with_probs.withColumn(
    "predicted_class",
    F.expr("array_position(class_probabilities, array_max(class_probabilities)) - 1")
)

df_with_probs.select("id", "predicted_class", "class_probabilities").show(10, truncate=False)

# ============================================================================
# 6. METHOD 3: MAPPARTITIONS FOR EFFICIENCY
# ============================================================================
print("\n6. METHOD 3: MAPPARTITIONS (MOST EFFICIENT)")
print("-" * 80)

def predict_partition(iterator):
    """Process entire partition at once"""
    # Load model once per partition
    model = SimpleClassifier(input_size=4, hidden_size=10, num_classes=3)
    model.load_state_dict(torch.load("/tmp/simple_classifier.pth"))
    model.eval()
    
    # Collect all rows in partition
    rows = list(iterator)
    if not rows:
        return
    
    # Extract features
    features = np.array([[row.f1, row.f2, row.f3, row.f4] for row in rows])
    X = torch.from_numpy(features).float()
    
    # Batch prediction
    with torch.no_grad():
        outputs = model(X)
        _, predictions = torch.max(outputs, 1)
        probabilities = torch.softmax(outputs, dim=1)
    
    # Yield results
    for i, row in enumerate(rows):
        yield (
            row.id,
            row.f1, row.f2, row.f3, row.f4,
            int(predictions[i].item()),
            probabilities[i].numpy().tolist()
        )

# Apply mapPartitions
schema = StructType([
    StructField("id", FloatType(), True),
    StructField("f1", FloatType(), True),
    StructField("f2", FloatType(), True),
    StructField("f3", FloatType(), True),
    StructField("f4", FloatType(), True),
    StructField("predicted_class", FloatType(), True),
    StructField("probabilities", ArrayType(FloatType()), True)
])

df_predictions_rdd = df.rdd.mapPartitions(predict_partition)
df_predictions = spark.createDataFrame(df_predictions_rdd, schema)

print("Predictions using mapPartitions:")
df_predictions.show(10, truncate=False)

# ============================================================================
# 7. PERFORMANCE COMPARISON
# ============================================================================
print("\n7. PERFORMANCE COMPARISON")
print("-" * 80)

import time

# Test on larger dataset
large_df = df.sample(fraction=1.0, seed=42)

# Method 1: Pandas UDF
start = time.time()
result1 = large_df.withColumn(
    "pred",
    predict_class_pandas(F.col("f1"), F.col("f2"), F.col("f3"), F.col("f4"))
).count()
time1 = time.time() - start

# Method 2: Broadcast + Pandas UDF
start = time.time()
result2 = large_df.withColumn(
    "pred",
    predict_probabilities(F.col("f1"), F.col("f2"), F.col("f3"), F.col("f4"))
).count()
time2 = time.time() - start

# Method 3: mapPartitions
start = time.time()
result3 = large_df.rdd.mapPartitions(predict_partition)
result3 = spark.createDataFrame(result3, schema).count()
time3 = time.time() - start

print(f"Performance results (processing {result1} rows):")
print(f"  Pandas UDF:                {time1:.2f}s")
print(f"  Broadcast + Pandas UDF:    {time2:.2f}s")
print(f"  mapPartitions:             {time3:.2f}s")

# ============================================================================
# 8. AGGREGATE PREDICTIONS
# ============================================================================
print("\n8. AGGREGATE PREDICTIONS")
print("-" * 80)

# Calculate statistics
stats = df_predictions.select(
    F.count("*").alias("total_predictions"),
    F.countDistinct("predicted_class").alias("unique_classes"),
    F.avg("predicted_class").alias("avg_predicted_class")
)

print("Prediction statistics:")
stats.show()

# Class distribution
print("\nClass distribution:")
df_predictions.groupBy("predicted_class") \
    .agg(
        F.count("*").alias("count"),
        F.avg(F.element_at("probabilities", 1)).alias("avg_prob_class0"),
        F.avg(F.element_at("probabilities", 2)).alias("avg_prob_class1"),
        F.avg(F.element_at("probabilities", 3)).alias("avg_prob_class2")
    ) \
    .orderBy("predicted_class") \
    .show()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("DISTRIBUTED INFERENCE PATTERNS SUMMARY:")
print("=" * 80)
print("""
1. Pandas UDF (Simple):
   ✓ Easy to implement
   ✓ Automatic vectorization
   ✗ Model loaded per batch

2. Broadcast + Pandas UDF (Better):
   ✓ Model shared across workers
   ✓ Reduced I/O overhead
   ✓ Good for moderate-sized models

3. mapPartitions (Best):
   ✓ Model loaded once per partition
   ✓ Maximum efficiency
   ✓ Full control over batch processing
   ✓ Best for large-scale inference

4. Key Considerations:
   ✓ Model size vs broadcast overhead
   ✓ Partition size for batch efficiency
   ✓ Memory constraints per executor
   ✓ I/O bottlenecks (model loading)

5. Best Practices:
   ✓ Use broadcast for models < 100MB
   ✓ Cache models in shared storage
   ✓ Profile different methods
   ✓ Monitor executor memory usage
""")

spark.stop()
print("\n" + "=" * 80)
print("Example completed!")
print("=" * 80)
