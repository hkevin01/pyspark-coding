"""
Real-Time Fraud Detection UDF
==============================

Production fraud detection for financial transactions.

Use Case: Payment processing, transaction monitoring, real-time alerts.

Features:
- Multi-factor fraud scoring
- Real-time risk assessment
- Alert routing based on risk level
- Feature engineering integrated
"""

import torch
import torch.nn as nn
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, FloatType, StringType, BooleanType


class FraudDetector(nn.Module):
    """Neural network for fraud detection"""
    def __init__(self, input_dim=15):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 128),
            nn.ReLU(),
            nn.BatchNorm1d(128),
            nn.Dropout(0.4),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.BatchNorm1d(64),
            nn.Dropout(0.3),
            nn.Linear(64, 32),
            nn.ReLU(),
            nn.Linear(32, 1),
            nn.Sigmoid()
        )
    
    def forward(self, x):
        return self.network(x)


def create_fraud_detector(spark, model_path=None):
    """
    Create fraud detection UDF.
    
    Args:
        spark: SparkSession
        model_path: Path to trained model
    
    Returns:
        Pandas UDF for fraud detection
    """
    
    # Load or create model
    if model_path:
        model = FraudDetector()
        model.load_state_dict(torch.load(model_path))
    else:
        model = FraudDetector()
    
    model.eval()
    broadcast_model = spark.sparkContext.broadcast(model.state_dict())
    
    # Define return schema
    schema = StructType([
        StructField("fraud_score", FloatType(), False),
        StructField("risk_level", StringType(), False),
        StructField("is_fraud", BooleanType(), False),
        StructField("alert_required", BooleanType(), False)
    ])
    
    @pandas_udf(schema)
    def detect_fraud(features: pd.Series) -> pd.DataFrame:
        """
        Detect fraudulent transactions.
        
        Args:
            features: Series where each element is transaction features array
        
        Returns:
            DataFrame with fraud detection results
        """
        local_model = FraudDetector()
        local_model.load_state_dict(broadcast_model.value)
        local_model.eval()
        
        # Convert to tensor
        X = torch.from_numpy(np.array(features.tolist())).float()
        
        # Inference
        with torch.no_grad():
            fraud_scores = local_model(X).numpy().flatten()
        
        results = []
        for score in fraud_scores:
            # Determine risk level
            if score > 0.8:
                risk_level = "CRITICAL"
                is_fraud = True
                alert_required = True
            elif score > 0.6:
                risk_level = "HIGH"
                is_fraud = True
                alert_required = True
            elif score > 0.4:
                risk_level = "MEDIUM"
                is_fraud = False
                alert_required = False
            else:
                risk_level = "LOW"
                is_fraud = False
                alert_required = False
            
            results.append({
                'fraud_score': float(score),
                'risk_level': risk_level,
                'is_fraud': is_fraud,
                'alert_required': alert_required
            })
        
        return pd.DataFrame(results)
    
    return detect_fraud


def main():
    """Demonstrate fraud detection"""
    
    print("="*60)
    print("Example 7: Real-Time Fraud Detection UDF")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("FraudDetection") \
        .master("local[4]") \
        .getOrCreate()
    
    print("\n1. Creating synthetic transaction data...")
    
    np.random.seed(42)
    num_transactions = 10000
    
    # Generate normal transactions
    normal_features = np.random.randn(9500, 15) * 0.5
    
    # Generate fraudulent transactions (higher values)
    fraud_features = np.random.randn(500, 15) * 2 + 3
    
    # Combine
    all_features = np.vstack([normal_features, fraud_features])
    indices = np.arange(num_transactions)
    np.random.shuffle(indices)
    all_features = all_features[indices]
    
    # Create DataFrame
    data = [(i, features.tolist()) for i, features in enumerate(all_features)]
    df = spark.createDataFrame(data, ["transaction_id", "features"])
    
    print(f"   Created {df.count():,} transactions")
    
    print("\n2. Creating fraud detection UDF...")
    
    detect_fraud = create_fraud_detector(spark)
    
    print("\n3. Detecting fraud...")
    
    df_results = df.withColumn(
        "fraud_detection",
        detect_fraud(F.col("features"))
    ).select(
        "transaction_id",
        F.col("fraud_detection.fraud_score").alias("fraud_score"),
        F.col("fraud_detection.risk_level").alias("risk_level"),
        F.col("fraud_detection.is_fraud").alias("is_fraud"),
        F.col("fraud_detection.alert_required").alias("alert_required")
    )
    
    print("\n4. Fraud detection results:")
    df_results.show(20)
    
    print("\n5. Risk level distribution:")
    df_results.groupBy("risk_level").count().orderBy("risk_level").show()
    
    print("\n6. Fraud statistics:")
    df_results.groupBy("is_fraud").count().show()
    
    print("\n7. Alerts required:")
    alerts = df_results.filter(F.col("alert_required") == True)
    print(f"   {alerts.count():,} transactions require immediate alert")
    
    print("\n8. Critical risk transactions:")
    critical = df_results.filter(F.col("risk_level") == "CRITICAL")
    critical.show(10)
    
    print("\n9. Score distribution by risk level:")
    df_results.groupBy("risk_level").agg(
        F.min("fraud_score").alias("min_score"),
        F.avg("fraud_score").alias("avg_score"),
        F.max("fraud_score").alias("max_score")
    ).orderBy("risk_level").show()
    
    print("\n10. Routing recommendations:")
    print("    - CRITICAL: Block transaction immediately + alert security team")
    print("    - HIGH: Hold for manual review + alert fraud team")
    print("    - MEDIUM: Flag for post-transaction review")
    print("    - LOW: Process normally")
    
    spark.stop()
    print("\n✅ Fraud detection completed!")


if __name__ == "__main__":
    main()
