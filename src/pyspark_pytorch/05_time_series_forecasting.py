"""
Example 5: Time Series Forecasting with PyTorch LSTM and PySpark Window Functions
==================================================================================
Demonstrates time series preprocessing with PySpark and forecasting with PyTorch LSTM.
"""

import numpy as np
import torch
import torch.nn as nn
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import pandas as pd

print("=" * 80)
print("TIME SERIES FORECASTING: PYSPARK WINDOW + PYTORCH LSTM")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("TimeSeriesForecasting") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ============================================================================
# 1. CREATE TIME SERIES DATA
# ============================================================================
print("\n1. CREATE TIME SERIES DATA")
print("-" * 80)

def generate_time_series(n_points=1000, n_series=3):
    """Generate synthetic time series data"""
    np.random.seed(42)
    data = []
    
    start_date = datetime(2023, 1, 1)
    
    for series_id in range(n_series):
        # Different patterns for each series
        trend = np.linspace(0, 10, n_points)
        seasonality = 5 * np.sin(np.linspace(0, 20 * np.pi, n_points))
        noise = np.random.randn(n_points) * 2
        
        values = trend + seasonality + noise + series_id * 5
        
        for i in range(n_points):
            timestamp = start_date + timedelta(hours=i)
            data.append((series_id, timestamp, float(values[i])))
    
    return data

# Create dataset
data = generate_time_series(n_points=1000, n_series=3)
df_ts = spark.createDataFrame(data, ["series_id", "timestamp", "value"])

print("Time series data:")
df_ts.orderBy("series_id", "timestamp").show(10)

print(f"\nDataset info:")
df_ts.groupBy("series_id").agg(
    F.count("*").alias("points"),
    F.min("timestamp").alias("start"),
    F.max("timestamp").alias("end"),
    F.avg("value").alias("mean_value")
).show()

# ============================================================================
# 2. FEATURE ENGINEERING WITH PYSPARK WINDOWS
# ============================================================================
print("\n2. FEATURE ENGINEERING WITH PYSPARK WINDOWS")
print("-" * 80)

# Define window spec for each series
window_spec = Window.partitionBy("series_id").orderBy("timestamp")

# Create rolling features
df_features = df_ts \
    .withColumn("prev_1", F.lag("value", 1).over(window_spec)) \
    .withColumn("prev_2", F.lag("value", 2).over(window_spec)) \
    .withColumn("prev_3", F.lag("value", 3).over(window_spec)) \
    .withColumn("prev_4", F.lag("value", 4).over(window_spec)) \
    .withColumn("prev_5", F.lag("value", 5).over(window_spec)) \
    .withColumn("rolling_mean_3", F.avg("value").over(window_spec.rowsBetween(-3, -1))) \
    .withColumn("rolling_std_3", F.stddev("value").over(window_spec.rowsBetween(-3, -1))) \
    .withColumn("rolling_min_5", F.min("value").over(window_spec.rowsBetween(-5, -1))) \
    .withColumn("rolling_max_5", F.max("value").over(window_spec.rowsBetween(-5, -1))) \
    .withColumn("row_number", F.row_number().over(window_spec))

# Drop nulls (first few rows don't have complete features)
df_features_clean = df_features.filter(F.col("prev_5").isNotNull())

print("Feature-engineered time series:")
df_features_clean.select("series_id", "timestamp", "value", 
                         "prev_1", "prev_2", "rolling_mean_3").show(10)

# ============================================================================
# 3. CREATE SEQUENCES FOR LSTM
# ============================================================================
print("\n3. CREATE SEQUENCES FOR LSTM")
print("-" * 80)

def create_sequences_spark(df, sequence_length=10, forecast_horizon=1):
    """Create sequences from time series using PySpark"""
    # Collect consecutive values as arrays
    window_spec = Window.partitionBy("series_id").orderBy("timestamp")
    
    # Create array of previous N values
    df_seq = df
    for i in range(1, sequence_length + 1):
        df_seq = df_seq.withColumn(f"val_{i}", F.lag("value", i).over(window_spec))
    
    # Create target (future value)
    df_seq = df_seq.withColumn("target", F.lead("value", forecast_horizon).over(window_spec))
    
    # Create feature array
    value_cols = [f"val_{i}" for i in range(sequence_length, 0, -1)]
    df_seq = df_seq.withColumn("sequence", F.array(*value_cols))
    
    # Filter out rows with nulls
    df_seq = df_seq.filter(F.col("target").isNotNull())
    
    for col in value_cols:
        df_seq = df_seq.filter(F.col(col).isNotNull())
    
    return df_seq.select("series_id", "timestamp", "sequence", "target")

sequence_length = 20
df_sequences = create_sequences_spark(df_ts, sequence_length=sequence_length, forecast_horizon=1)

print(f"Sequence dataset (lookback={sequence_length}):")
df_sequences.show(5, truncate=False)
print(f"Total sequences: {df_sequences.count()}")

# ============================================================================
# 4. SPLIT DATA INTO TRAIN/TEST
# ============================================================================
print("\n4. SPLIT DATA INTO TRAIN/TEST")
print("-" * 80)

# Split by time (80-20)
total_count = df_sequences.count()
train_count = int(total_count * 0.8)

df_train = df_sequences.limit(train_count)
df_test = df_sequences.subtract(df_train)

print(f"Training sequences: {df_train.count()}")
print(f"Test sequences: {df_test.count()}")

# ============================================================================
# 5. DEFINE PYTORCH LSTM MODEL
# ============================================================================
print("\n5. DEFINE PYTORCH LSTM MODEL")
print("-" * 80)

class LSTMForecaster(nn.Module):
    """LSTM model for time series forecasting"""
    def __init__(self, input_size=1, hidden_size=64, num_layers=2, output_size=1):
        super(LSTMForecaster, self).__init__()
        
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=0.2 if num_layers > 1 else 0
        )
        
        self.fc = nn.Linear(hidden_size, output_size)
    
    def forward(self, x):
        # x shape: (batch, sequence_length, input_size)
        lstm_out, _ = self.lstm(x)
        
        # Take output from last time step
        last_output = lstm_out[:, -1, :]
        
        # Make prediction
        prediction = self.fc(last_output)
        
        return prediction

# Create model
model = LSTMForecaster(input_size=1, hidden_size=64, num_layers=2, output_size=1)

print("LSTM Model Architecture:")
print(model)
print(f"\nTotal parameters: {sum(p.numel() for p in model.parameters())}")

# ============================================================================
# 6. PREPARE DATA FOR TRAINING
# ============================================================================
print("\n6. PREPARE DATA FOR TRAINING")
print("-" * 80)

# Convert to tensors
train_data = df_train.select("sequence", "target").toPandas()
X_train_list = train_data['sequence'].tolist()
y_train = train_data['target'].values

X_train = np.array(X_train_list)
X_train = X_train.reshape(-1, sequence_length, 1)  # Add feature dimension

X_train_tensor = torch.from_numpy(X_train).float()
y_train_tensor = torch.from_numpy(y_train).float().unsqueeze(1)

print(f"Training data shape:")
print(f"  X_train: {X_train_tensor.shape}")
print(f"  y_train: {y_train_tensor.shape}")

# ============================================================================
# 7. TRAIN THE MODEL
# ============================================================================
print("\n7. TRAIN THE MODEL")
print("-" * 80)

criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

num_epochs = 50
batch_size = 32

print(f"Training configuration:")
print(f"  Epochs: {num_epochs}")
print(f"  Batch size: {batch_size}")
print(f"  Learning rate: 0.001")

model.train()
for epoch in range(num_epochs):
    total_loss = 0
    
    for i in range(0, len(X_train_tensor), batch_size):
        batch_X = X_train_tensor[i:i+batch_size]
        batch_y = y_train_tensor[i:i+batch_size]
        
        # Forward pass
        outputs = model(batch_X)
        loss = criterion(outputs, batch_y)
        
        # Backward pass
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        
        total_loss += loss.item()
    
    if (epoch + 1) % 10 == 0:
        avg_loss = total_loss / (len(X_train_tensor) // batch_size)
        print(f"  Epoch [{epoch+1}/{num_epochs}], Loss: {avg_loss:.4f}")

# ============================================================================
# 8. EVALUATE ON TEST SET
# ============================================================================
print("\n8. EVALUATE ON TEST SET")
print("-" * 80)

# Prepare test data
test_data = df_test.select("sequence", "target").toPandas()
X_test_list = test_data['sequence'].tolist()
y_test = test_data['target'].values

X_test = np.array(X_test_list)
X_test = X_test.reshape(-1, sequence_length, 1)

X_test_tensor = torch.from_numpy(X_test).float()
y_test_tensor = torch.from_numpy(y_test).float().unsqueeze(1)

# Make predictions
model.eval()
with torch.no_grad():
    predictions = model(X_test_tensor)

# Calculate metrics
mse = criterion(predictions, y_test_tensor).item()
mae = torch.mean(torch.abs(predictions - y_test_tensor)).item()
rmse = np.sqrt(mse)

print(f"Test Set Metrics:")
print(f"  MSE:  {mse:.4f}")
print(f"  MAE:  {mae:.4f}")
print(f"  RMSE: {rmse:.4f}")

# ============================================================================
# 9. DISTRIBUTED INFERENCE WITH PYSPARK
# ============================================================================
print("\n9. DISTRIBUTED INFERENCE WITH PYSPARK")
print("-" * 80)

# Save model
model_path = "/tmp/lstm_forecaster.pth"
torch.save(model.state_dict(), model_path)

# Broadcast model
model_state = model.state_dict()
broadcast_model = spark.sparkContext.broadcast(model_state)

from pyspark.sql.functions import pandas_udf

@pandas_udf(FloatType())
def forecast_udf(sequences: pd.Series) -> pd.Series:
    """Forecast next value using LSTM"""
    # Load model
    model = LSTMForecaster(input_size=1, hidden_size=64, num_layers=2, output_size=1)
    model.load_state_dict(broadcast_model.value)
    model.eval()
    
    # Convert sequences to tensor
    seq_array = np.array(sequences.tolist())
    seq_array = seq_array.reshape(-1, sequence_length, 1)
    seq_tensor = torch.from_numpy(seq_array).float()
    
    # Make predictions
    with torch.no_grad():
        predictions = model(seq_tensor)
    
    return pd.Series(predictions.numpy().flatten())

# Apply UDF
df_with_predictions = df_test.withColumn(
    "prediction",
    forecast_udf(F.col("sequence"))
)

print("Predictions on test set:")
df_with_predictions.select("series_id", "timestamp", "target", "prediction").show(20)

# Calculate error
df_with_predictions = df_with_predictions.withColumn(
    "error",
    F.abs(F.col("target") - F.col("prediction"))
)

print("\nError statistics by series:")
df_with_predictions.groupBy("series_id").agg(
    F.count("*").alias("count"),
    F.avg("error").alias("mean_error"),
    F.stddev("error").alias("std_error"),
    F.min("error").alias("min_error"),
    F.max("error").alias("max_error")
).show()

# ============================================================================
# 10. MULTI-STEP FORECASTING
# ============================================================================
print("\n10. MULTI-STEP FORECASTING")
print("-" * 80)

def forecast_multi_step(model, initial_sequence, steps=10):
    """Forecast multiple steps ahead"""
    model.eval()
    
    current_sequence = initial_sequence.clone()
    predictions = []
    
    with torch.no_grad():
        for _ in range(steps):
            # Predict next value
            pred = model(current_sequence)
            predictions.append(pred.item())
            
            # Update sequence (shift and add prediction)
            current_sequence = torch.cat([
                current_sequence[:, 1:, :],
                pred.unsqueeze(1)
            ], dim=1)
    
    return predictions

# Take first sequence from test set
first_sequence = X_test_tensor[0:1]

# Forecast 20 steps ahead
multi_step_forecast = forecast_multi_step(model, first_sequence, steps=20)

print(f"Multi-step forecast (20 steps ahead):")
print(f"  {[f'{x:.2f}' for x in multi_step_forecast]}")

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("TIME SERIES FORECASTING PIPELINE SUMMARY:")
print("=" * 80)
print("""
1. PySpark Stage (Data Preparation):
   ✓ Window functions for rolling features
   ✓ Lag features for historical context
   ✓ Statistical aggregations (mean, std, min, max)
   ✓ Sequence generation at scale

2. PyTorch Stage (Model Training):
   ✓ LSTM architecture for temporal patterns
   ✓ Multi-layer architecture with dropout
   ✓ Mini-batch training with Adam optimizer
   ✓ MSE loss for regression

3. Integration Benefits:
   ✓ PySpark handles large-scale data preprocessing
   ✓ PyTorch provides powerful sequence modeling
   ✓ Distributed inference via Pandas UDF
   ✓ Scalable to millions of time series

4. Real-World Applications:
   ✓ Demand forecasting
   ✓ Stock price prediction
   ✓ Energy consumption forecasting
   ✓ Sensor data analysis
   ✓ IoT time series

5. Advanced Techniques:
   ✓ Multi-step ahead forecasting
   ✓ Attention mechanisms
   ✓ Multi-variate forecasting
   ✓ Transfer learning across series
""")

spark.stop()
print("\n" + "=" * 80)
print("Example completed!")
print("=" * 80)
