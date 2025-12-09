"""
Example 2: Feature Engineering with PySpark + Training with PyTorch
====================================================================
Demonstrates using PySpark for feature engineering and PyTorch for model training.
"""

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline

print("=" * 80)
print("FEATURE ENGINEERING (PYSPARK) + MODEL TRAINING (PYTORCH)")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("FeatureEngineeringTraining") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# 1. CREATE SAMPLE DATA
# ============================================================================
print("\n1. CREATE SAMPLE DATA")
print("-" * 80)

# Create synthetic dataset (housing prices)
data = []
np.random.seed(42)
for i in range(200):
    bedrooms = np.random.randint(1, 6)
    bathrooms = np.random.randint(1, 4)
    sqft = np.random.randint(500, 4000)
    age = np.random.randint(0, 50)
    
    # Simple price formula with some noise
    price = (bedrooms * 50000 + bathrooms * 30000 + 
             sqft * 150 - age * 1000 + np.random.randn() * 20000)
    
    # Binary classification: expensive (>500k) or not
    label = 1 if price > 500000 else 0
    
    data.append((i, bedrooms, bathrooms, sqft, age, float(price), label))

df = spark.createDataFrame(data, 
    ["id", "bedrooms", "bathrooms", "sqft", "age", "price", "label"])

print("Sample data:")
df.show(10)

print(f"\nDataset statistics:")
df.select(
    F.count("*").alias("count"),
    F.avg("price").alias("avg_price"),
    F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("expensive_count")
).show()

# ============================================================================
# 2. FEATURE ENGINEERING WITH PYSPARK
# ============================================================================
print("\n2. FEATURE ENGINEERING WITH PYSPARK")
print("-" * 80)

# Create new features
df_engineered = df \
    .withColumn("price_per_sqft", F.col("price") / F.col("sqft")) \
    .withColumn("bathroom_bedroom_ratio", F.col("bathrooms") / F.col("bedrooms")) \
    .withColumn("total_rooms", F.col("bedrooms") + F.col("bathrooms")) \
    .withColumn("is_new", F.when(F.col("age") < 5, 1).otherwise(0)) \
    .withColumn("sqft_category", 
        F.when(F.col("sqft") < 1000, "small")
         .when(F.col("sqft") < 2500, "medium")
         .otherwise("large")
    )

print("Engineered features:")
df_engineered.select("bedrooms", "bathrooms", "sqft", "age", 
                     "price_per_sqft", "bathroom_bedroom_ratio", 
                     "total_rooms", "is_new", "label").show(5)

# ============================================================================
# 3. PREPARE FEATURES WITH PYSPARK ML
# ============================================================================
print("\n3. PREPARE FEATURES WITH PYSPARK ML")
print("-" * 80)

# Select feature columns
feature_columns = ["bedrooms", "bathrooms", "sqft", "age", 
                   "price_per_sqft", "bathroom_bedroom_ratio", 
                   "total_rooms", "is_new"]

# Create feature vector
assembler = VectorAssembler(
    inputCols=feature_columns,
    outputCol="features_raw"
)

# Scale features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features_scaled",
    withMean=True,
    withStd=True
)

# Create pipeline
pipeline = Pipeline(stages=[assembler, scaler])

# Fit and transform
model = pipeline.fit(df_engineered)
df_prepared = model.transform(df_engineered)

print("Prepared features:")
df_prepared.select("features_scaled", "label").show(5, truncate=False)

# ============================================================================
# 4. CONVERT TO PYTORCH TENSORS
# ============================================================================
print("\n4. CONVERT TO PYTORCH TENSORS")
print("-" * 80)

def spark_vector_to_numpy(spark_df, vector_col):
    """Convert Spark ML Vector column to NumPy array"""
    return np.array(spark_df.select(vector_col).rdd.map(lambda row: row[0].toArray()).collect())

# Convert features and labels
X_numpy = spark_vector_to_numpy(df_prepared, "features_scaled")
y_numpy = df_prepared.select("label").toPandas().values.flatten()

# Create tensors
X_tensor = torch.from_numpy(X_numpy).float()
y_tensor = torch.from_numpy(y_numpy).long()

print(f"Features tensor shape: {X_tensor.shape}")
print(f"Labels tensor shape: {y_tensor.shape}")
print(f"\nClass distribution:")
print(f"  Class 0: {(y_tensor == 0).sum().item()}")
print(f"  Class 1: {(y_tensor == 1).sum().item()}")

# ============================================================================
# 5. SPLIT DATA (TRAIN/TEST)
# ============================================================================
print("\n5. SPLIT DATA (TRAIN/TEST)")
print("-" * 80)

# 80-20 split
train_size = int(0.8 * len(X_tensor))
indices = torch.randperm(len(X_tensor))

train_indices = indices[:train_size]
test_indices = indices[train_size:]

X_train, X_test = X_tensor[train_indices], X_tensor[test_indices]
y_train, y_test = y_tensor[train_indices], y_tensor[test_indices]

print(f"Training set: {X_train.shape[0]} samples")
print(f"Test set: {X_test.shape[0]} samples")

# ============================================================================
# 6. DEFINE PYTORCH MODEL
# ============================================================================
print("\n6. DEFINE PYTORCH MODEL")
print("-" * 80)

class HousePriceClassifier(nn.Module):
    def __init__(self, input_size):
        super(HousePriceClassifier, self).__init__()
        self.fc1 = nn.Linear(input_size, 64)
        self.relu1 = nn.ReLU()
        self.dropout1 = nn.Dropout(0.3)
        
        self.fc2 = nn.Linear(64, 32)
        self.relu2 = nn.ReLU()
        self.dropout2 = nn.Dropout(0.2)
        
        self.fc3 = nn.Linear(32, 16)
        self.relu3 = nn.ReLU()
        
        self.fc4 = nn.Linear(16, 2)  # Binary classification
    
    def forward(self, x):
        x = self.dropout1(self.relu1(self.fc1(x)))
        x = self.dropout2(self.relu2(self.fc2(x)))
        x = self.relu3(self.fc3(x))
        x = self.fc4(x)
        return x

input_size = X_train.shape[1]
model = HousePriceClassifier(input_size)

print(f"Model architecture:")
print(model)
print(f"\nTotal parameters: {sum(p.numel() for p in model.parameters())}")

# ============================================================================
# 7. TRAIN THE MODEL
# ============================================================================
print("\n7. TRAIN THE MODEL")
print("-" * 80)

criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=0.001)

num_epochs = 50
batch_size = 32

print(f"Training configuration:")
print(f"  Epochs: {num_epochs}")
print(f"  Batch size: {batch_size}")
print(f"  Learning rate: 0.001")
print(f"  Optimizer: Adam")
print(f"\nTraining progress:")

for epoch in range(num_epochs):
    model.train()
    total_loss = 0
    
    # Mini-batch training
    for i in range(0, len(X_train), batch_size):
        batch_X = X_train[i:i+batch_size]
        batch_y = y_train[i:i+batch_size]
        
        # Forward pass
        outputs = model(batch_X)
        loss = criterion(outputs, batch_y)
        
        # Backward pass
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        total_loss += loss.item()
    
    # Print progress every 10 epochs
    if (epoch + 1) % 10 == 0:
        avg_loss = total_loss / (len(X_train) // batch_size)
        print(f"  Epoch [{epoch+1}/{num_epochs}], Loss: {avg_loss:.4f}")

# ============================================================================
# 8. EVALUATE THE MODEL
# ============================================================================
print("\n8. EVALUATE THE MODEL")
print("-" * 80)

model.eval()
with torch.no_grad():
    # Training accuracy
    train_outputs = model(X_train)
    _, train_predicted = torch.max(train_outputs, 1)
    train_accuracy = (train_predicted == y_train).float().mean()
    
    # Test accuracy
    test_outputs = model(X_test)
    _, test_predicted = torch.max(test_outputs, 1)
    test_accuracy = (test_predicted == y_test).float().mean()

print(f"Training Accuracy: {train_accuracy.item():.4f}")
print(f"Test Accuracy: {test_accuracy.item():.4f}")

# Confusion matrix
from sklearn.metrics import confusion_matrix, classification_report

y_test_np = y_test.numpy()
y_pred_np = test_predicted.numpy()

print("\nConfusion Matrix:")
print(confusion_matrix(y_test_np, y_pred_np))

print("\nClassification Report:")
print(classification_report(y_test_np, y_pred_np, 
                          target_names=["Not Expensive", "Expensive"]))

# ============================================================================
# 9. INFERENCE ON NEW DATA
# ============================================================================
print("\n9. INFERENCE ON NEW DATA")
print("-" * 80)

# Create new sample data
new_houses = [
    (1, 3, 2, 1800, 5),   # 3 bed, 2 bath, 1800 sqft, 5 years old
    (2, 5, 4, 3500, 2),   # 5 bed, 4 bath, 3500 sqft, 2 years old
    (3, 2, 1, 900, 30),   # 2 bed, 1 bath, 900 sqft, 30 years old
]

new_df = spark.createDataFrame(new_houses, 
    ["id", "bedrooms", "bathrooms", "sqft", "age"])

# Apply same feature engineering
new_df_engineered = new_df \
    .withColumn("price_per_sqft", F.lit(0.0)) \
    .withColumn("bathroom_bedroom_ratio", F.col("bathrooms") / F.col("bedrooms")) \
    .withColumn("total_rooms", F.col("bedrooms") + F.col("bathrooms")) \
    .withColumn("is_new", F.when(F.col("age") < 5, 1).otherwise(0))

# Transform using the same pipeline
new_df_prepared = model.transform(new_df_engineered)

# Convert to tensor
X_new = spark_vector_to_numpy(new_df_prepared, "features_scaled")
X_new_tensor = torch.from_numpy(X_new).float()

# Make predictions
model.eval()
with torch.no_grad():
    predictions = model(X_new_tensor)
    probabilities = torch.softmax(predictions, dim=1)
    _, predicted_classes = torch.max(predictions, 1)

print("Predictions for new houses:")
for i, house in enumerate(new_houses):
    print(f"\nHouse {house[0]}:")
    print(f"  Specs: {house[1]} bed, {house[2]} bath, {house[3]} sqft, {house[4]} years")
    print(f"  Prediction: {'Expensive' if predicted_classes[i] == 1 else 'Not Expensive'}")
    print(f"  Confidence: {probabilities[i][predicted_classes[i]].item():.2%}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY WORKFLOW SUMMARY:")
print("=" * 80)
print("""
1. PySpark Stage (Distributed Processing):
   ✓ Load large datasets efficiently
   ✓ Feature engineering with SQL functions
   ✓ Feature scaling with MLlib
   ✓ Data aggregation and statistics

2. Conversion Stage:
   ✓ Spark Vector -> NumPy array
   ✓ NumPy array -> PyTorch tensor
   ✓ Handle train/test split

3. PyTorch Stage (Deep Learning):
   ✓ Define neural network architecture
   ✓ Train with mini-batch gradient descent
   ✓ Evaluate model performance
   ✓ Make predictions on new data

4. Benefits of Integration:
   ✓ PySpark: Handles big data preprocessing
   ✓ PyTorch: Powerful deep learning capabilities
   ✓ Best of both worlds for ML pipelines
""")

spark.stop()
print("\n" + "=" * 80)
print("Example completed!")
print("=" * 80)
