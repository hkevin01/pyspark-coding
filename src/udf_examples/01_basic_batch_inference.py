"""
Basic Pandas UDF for Batch Inference
=====================================

This example demonstrates the fundamental pattern for ML inference using Pandas UDFs.

Use Case: Apply a trained PyTorch model to millions of records in a distributed manner.

Key Concepts:
- Model broadcasting (load once, use many times)
- Pandas UDF for batch processing
- Efficient tensor conversions
"""

import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import FloatType


# ============================================================
# MODEL DEFINITION
# ============================================================

class SimplePredictor(nn.Module):
    """Simple neural network for demonstration"""
    def __init__(self, input_dim=10, hidden_dim=64):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(hidden_dim, 32),
            nn.ReLU(),
            nn.Linear(32, 1)
        )
    
    def forward(self, x):
        return self.network(x)


# ============================================================
# UDF CREATION
# ============================================================

def create_inference_udf(spark, model_path=None):
    """
    Create a Pandas UDF for batch inference.
    
    Args:
        spark: SparkSession
        model_path: Path to saved model (if None, creates dummy model)
    
    Returns:
        Pandas UDF function
    """
    
    # Load or create model
    if model_path:
        model = SimplePredictor()
        model.load_state_dict(torch.load(model_path))
    else:
        # Create dummy model for demonstration
        model = SimplePredictor()
    
    model.eval()
    
    # Broadcast model to all workers
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    # Define Pandas UDF
    @pandas_udf(FloatType())
    def predict_udf(features: pd.Series) -> pd.Series:
        """
        Apply model to batch of features.
        
        Args:
            features: Pandas Series where each element is a list of features
        
        Returns:
            Pandas Series of predictions
        """
        # Load model from broadcast variable
        local_model = SimplePredictor()
        local_model.load_state_dict(broadcast_model.value)
        local_model.eval()
        
        # Convert features to tensor
        # features is a Series where each element is a list
        feature_arrays = np.array(features.tolist())
        X_tensor = torch.from_numpy(feature_arrays).float()
        
        # Inference
        with torch.no_grad():
            predictions = local_model(X_tensor).numpy().flatten()
        
        return pd.Series(predictions)
    
    return predict_udf


# ============================================================
# EXAMPLE USAGE
# ============================================================

def main():
    """Demonstrate basic batch inference"""
    
    print("="*60)
    print("Example 1: Basic Batch Inference with Pandas UDF")
    print("="*60)
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("BasicBatchInference") \
        .master("local[4]") \
        .getOrCreate()
    
    print("\n1. Creating sample data...")
    
    # Create sample dataset
    np.random.seed(42)
    num_samples = 10000
    
    data = []
    for i in range(num_samples):
        features = np.random.randn(10).tolist()
        data.append((i, features))
    
    df = spark.createDataFrame(data, ["id", "features"])
    
    print(f"   Created {df.count():,} samples")
    print(f"   Schema: {df.schema}")
    
    # Show sample
    print("\n2. Sample data:")
    df.show(5, truncate=False)
    
    print("\n3. Creating inference UDF...")
    
    # Create UDF (without pre-trained model for demonstration)
    predict_udf = create_inference_udf(spark)
    
    print("   UDF created successfully")
    
    print("\n4. Applying inference to data...")
    
    # Apply UDF
    df_predictions = df.withColumn(
        "prediction",
        predict_udf(F.col("features"))
    )
    
    print(f"   Inference complete!")
    
    # Show results
    print("\n5. Prediction results:")
    df_predictions.select("id", "prediction").show(10)
    
    # Statistics
    print("\n6. Prediction statistics:")
    df_predictions.select(
        F.mean("prediction").alias("mean"),
        F.stddev("prediction").alias("stddev"),
        F.min("prediction").alias("min"),
        F.max("prediction").alias("max")
    ).show()
    
    print("\n7. Performance metrics:")
    print(f"   Total records processed: {df_predictions.count():,}")
    print(f"   Number of partitions: {df_predictions.rdd.getNumPartitions()}")
    
    # Optional: Save results
    # df_predictions.write.mode("overwrite").parquet("output/predictions")
    
    spark.stop()
    print("\n✅ Example completed successfully!")


# ============================================================
# ADVANCED USAGE PATTERNS
# ============================================================

def batch_inference_with_features(spark, df, feature_columns, model_path=None):
    """
    Advanced pattern: Apply inference to specific columns
    
    Args:
        spark: SparkSession
        df: Input DataFrame
        feature_columns: List of column names to use as features
        model_path: Path to model
    
    Returns:
        DataFrame with predictions
    """
    
    # Create feature array column
    df_features = df.withColumn(
        "feature_array",
        F.array([F.col(c) for c in feature_columns])
    )
    
    # Create and apply UDF
    predict_udf = create_inference_udf(spark, model_path)
    
    df_predictions = df_features.withColumn(
        "prediction",
        predict_udf(F.col("feature_array"))
    )
    
    return df_predictions


def batch_inference_with_confidence(spark):
    """
    Pattern: Return predictions with confidence scores
    """
    from pyspark.sql.types import StructType, StructField, FloatType
    
    model = SimplePredictor()
    model.eval()
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    # Define return schema
    schema = StructType([
        StructField("prediction", FloatType(), False),
        StructField("confidence", FloatType(), False)
    ])
    
    @pandas_udf(schema)
    def predict_with_confidence(features: pd.Series) -> pd.DataFrame:
        """Return both prediction and confidence"""
        local_model = SimplePredictor()
        local_model.load_state_dict(broadcast_model.value)
        local_model.eval()
        
        X = torch.from_numpy(np.array(features.tolist())).float()
        
        with torch.no_grad():
            predictions = local_model(X).numpy().flatten()
        
        # Simple confidence: abs(prediction)
        confidence = np.abs(predictions)
        
        return pd.DataFrame({
            'prediction': predictions,
            'confidence': confidence
        })
    
    return predict_with_confidence


if __name__ == "__main__":
    main()
