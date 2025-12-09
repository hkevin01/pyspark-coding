"""
Multi-Class Classification UDF
===============================

Production-ready classification with probability scores and confidence thresholds.

Use Case: Customer churn prediction, document classification, product categorization.

Features:
- Multi-class classification
- Probability scores for all classes
- Confidence-based filtering
- Top-K predictions
"""

import torch
import torch.nn as nn
import torch.nn.functional as F
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F_sql
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, ArrayType


class MultiClassClassifier(nn.Module):
    """Multi-class classifier"""
    def __init__(self, input_dim=20, num_classes=5):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, num_classes)
        )
    
    def forward(self, x):
        return self.network(x)


def create_classifier(spark, model_path=None, num_classes=5):
    """
    Create multi-class classification UDF.
    
    Args:
        spark: SparkSession
        model_path: Path to model
        num_classes: Number of output classes
    
    Returns:
        Pandas UDF for classification
    """
    
    # Load or create model
    if model_path:
        model = MultiClassClassifier(num_classes=num_classes)
        model.load_state_dict(torch.load(model_path))
    else:
        model = MultiClassClassifier(num_classes=num_classes)
    
    model.eval()
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    # Define return schema
    schema = StructType([
        StructField("predicted_class", IntegerType(), False),
        StructField("confidence", FloatType(), False),
        StructField("probabilities", ArrayType(FloatType()), False)
    ])
    
    @pandas_udf(schema)
    def classify(features: pd.Series) -> pd.DataFrame:
        """
        Classify features into multiple classes.
        
        Returns:
            DataFrame with predicted_class, confidence, and probabilities
        """
        local_model = MultiClassClassifier(num_classes=num_classes)
        local_model.load_state_dict(broadcast_model.value)
        local_model.eval()
        
        # Convert to tensor
        X = torch.from_numpy(np.array(features.tolist())).float()
        
        # Inference
        with torch.no_grad():
            logits = local_model(X)
            probabilities = F.softmax(logits, dim=1).numpy()
        
        # Get predictions and confidence
        predicted_classes = np.argmax(probabilities, axis=1)
        confidences = np.max(probabilities, axis=1)
        
        return pd.DataFrame({
            'predicted_class': predicted_classes,
            'confidence': confidences,
            'probabilities': list(probabilities)
        })
    
    return classify


def main():
    """Demonstrate classification UDF"""
    
    print("="*60)
    print("Example 3: Multi-Class Classification UDF")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("Classification") \
        .master("local[4]") \
        .getOrCreate()
    
    print("\n1. Creating synthetic classification data...")
    
    np.random.seed(42)
    num_samples = 5000
    num_classes = 5
    
    # Generate class-conditional data
    data = []
    for i in range(num_samples):
        true_class = i % num_classes
        # Generate features centered around class
        features = np.random.randn(20) + true_class * 2
        data.append((i, features.tolist(), true_class))
    
    df = spark.createDataFrame(data, ["id", "features", "true_class"])
    
    print(f"   Created {df.count():,} samples with {num_classes} classes")
    
    print("\n2. Creating classification UDF...")
    
    classify = create_classifier(spark, num_classes=num_classes)
    
    print("\n3. Applying classification...")
    
    df_classified = df.withColumn(
        "classification",
        classify(F_sql.col("features"))
    ).select(
        "id",
        "true_class",
        F_sql.col("classification.predicted_class").alias("predicted_class"),
        F_sql.col("classification.confidence").alias("confidence"),
        F_sql.col("classification.probabilities").alias("probabilities")
    )
    
    print("\n4. Classification results:")
    df_classified.show(10)
    
    print("\n5. Confidence distribution:")
    df_classified.select(
        F_sql.mean("confidence").alias("avg_confidence"),
        F_sql.stddev("confidence").alias("std_confidence"),
        F_sql.min("confidence").alias("min_confidence"),
        F_sql.max("confidence").alias("max_confidence")
    ).show()
    
    print("\n6. High-confidence predictions (>0.8):")
    high_conf = df_classified.filter(F_sql.col("confidence") > 0.8)
    print(f"   {high_conf.count():,} high-confidence predictions")
    
    print("\n7. Low-confidence predictions (<0.5) - needs review:")
    low_conf = df_classified.filter(F_sql.col("confidence") < 0.5)
    print(f"   {low_conf.count():,} low-confidence predictions")
    low_conf.show(5)
    
    print("\n8. Predictions per class:")
    df_classified.groupBy("predicted_class").count().orderBy("predicted_class").show()
    
    spark.stop()
    print("\n✅ Classification completed!")


if __name__ == "__main__":
    main()
