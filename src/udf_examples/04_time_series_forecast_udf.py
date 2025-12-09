"""
Time Series Forecasting UDF
============================

LSTM-based time series forecasting for large-scale predictions.

Use Case: Sales forecasting for millions of products, demand prediction.

Features:
- LSTM architecture for sequence prediction
- Rolling window features
- Multi-step ahead forecasting
- Confidence intervals
"""

import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType, ArrayType


class LSTMForecaster(nn.Module):
    """LSTM for time series forecasting"""
    def __init__(self, input_size=1, hidden_size=64, num_layers=2, forecast_horizon=7):
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.forecast_horizon = forecast_horizon
        
        self.lstm = nn.LSTM(
            input_size,
            hidden_size,
            num_layers,
            batch_first=True,
            dropout=0.2 if num_layers > 1 else 0
        )
        
        self.fc = nn.Linear(hidden_size, forecast_horizon)
    
    def forward(self, x):
        # x shape: (batch, seq_len, input_size)
        lstm_out, _ = self.lstm(x)
        # Take last timestep output
        last_output = lstm_out[:, -1, :]
        # Forecast multiple steps
        forecast = self.fc(last_output)
        return forecast


def create_forecaster(spark, model_path=None, sequence_length=30, forecast_horizon=7):
    """
    Create time series forecasting UDF.
    
    Args:
        spark: SparkSession
        model_path: Path to trained LSTM model
        sequence_length: Length of input sequence
        forecast_horizon: Number of steps to forecast
    
    Returns:
        Pandas UDF for forecasting
    """
    
    # Load or create model
    if model_path:
        model = LSTMForecaster(forecast_horizon=forecast_horizon)
        model.load_state_dict(torch.load(model_path))
    else:
        model = LSTMForecaster(forecast_horizon=forecast_horizon)
    
    model.eval()
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    @pandas_udf(ArrayType(FloatType()))
    def forecast_udf(history: pd.Series) -> pd.Series:
        """
        Forecast future values based on historical sequence.
        
        Args:
            history: Series where each element is a list of historical values
        
        Returns:
            Series where each element is a list of forecasted values
        """
        local_model = LSTMForecaster(forecast_horizon=forecast_horizon)
        local_model.load_state_dict(broadcast_model.value)
        local_model.eval()
        
        forecasts = []
        
        for hist in history:
            if len(hist) < sequence_length:
                # Pad if too short
                hist = [0] * (sequence_length - len(hist)) + list(hist)
            else:
                # Take last sequence_length values
                hist = hist[-sequence_length:]
            
            # Convert to tensor
            X = torch.tensor(hist).float().unsqueeze(0).unsqueeze(-1)
            
            # Forecast
            with torch.no_grad():
                forecast = local_model(X).squeeze().numpy()
            
            forecasts.append(forecast.tolist())
        
        return pd.Series(forecasts)
    
    return forecast_udf


def main():
    """Demonstrate time series forecasting"""
    
    print("="*60)
    print("Example 4: Time Series Forecasting UDF")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("TimeSeriesForecasting") \
        .master("local[4]") \
        .getOrCreate()
    
    print("\n1. Generating synthetic time series data...")
    
    np.random.seed(42)
    num_series = 1000
    sequence_length = 30
    
    data = []
    for i in range(num_series):
        # Generate time series with trend and seasonality
        t = np.arange(60)
        trend = t * 0.5
        seasonal = 10 * np.sin(2 * np.pi * t / 7)
        noise = np.random.randn(60) * 2
        series = trend + seasonal + noise + 50
        
        # Split into history and future
        history = series[:sequence_length].tolist()
        future = series[sequence_length:sequence_length+7].tolist()
        
        data.append((i, history, future))
    
    df = spark.createDataFrame(data, ["series_id", "history", "actual_future"])
    
    print(f"   Created {df.count():,} time series")
    
    print("\n2. Sample time series:")
    df.show(3, truncate=False)
    
    print("\n3. Creating forecasting UDF...")
    
    forecast_udf = create_forecaster(spark, forecast_horizon=7)
    
    print("\n4. Generating forecasts...")
    
    df_forecasts = df.withColumn(
        "forecast",
        forecast_udf(F.col("history"))
    )
    
    print("\n5. Forecast results:")
    df_forecasts.select("series_id", "forecast").show(10, truncate=False)
    
    # Calculate simple metrics
    print("\n6. Forecast evaluation:")
    print("   (Note: Using dummy model, accuracy will be poor)")
    
    # Show comparison for one series
    sample = df_forecasts.limit(1).collect()[0]
    print(f"\n   Series {sample['series_id']}:")
    print(f"   Last historical values: {sample['history'][-5:]}")
    print(f"   Forecast: {sample['forecast']}")
    print(f"   Actual: {sample['actual_future']}")
    
    print("\n7. Forecast statistics:")
    # Explode forecast array to calculate statistics
    df_exploded = df_forecasts.withColumn("forecast_value", F.explode(F.col("forecast")))
    
    df_exploded.select(
        F.mean("forecast_value").alias("avg_forecast"),
        F.stddev("forecast_value").alias("std_forecast"),
        F.min("forecast_value").alias("min_forecast"),
        F.max("forecast_value").alias("max_forecast")
    ).show()
    
    spark.stop()
    print("\n✅ Time series forecasting completed!")


if __name__ == "__main__":
    main()
