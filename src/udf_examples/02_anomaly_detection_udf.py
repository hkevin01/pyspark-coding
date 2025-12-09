"""
Anomaly Detection UDF
=====================

Real-time anomaly detection on streaming sensor data using Isolation Forest.

Use Case: Manufacturing IoT sensors - detect equipment failures before they happen.

Key Features:
- Isolation Forest for unsupervised anomaly detection
- Streaming-ready architecture
- Anomaly score and binary classification
"""

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
import pickle
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, BooleanType, FloatType


def create_anomaly_detector(spark, model_path=None, contamination=0.1):
    """
    Create anomaly detection UDF using Isolation Forest.
    
    Args:
        spark: SparkSession
        model_path: Path to trained model (pickled sklearn model)
        contamination: Expected proportion of anomalies
    
    Returns:
        Pandas UDF for anomaly detection
    """
    
    # Load or train model
    if model_path:
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
    else:
        # Create default model
        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        # In production, fit on historical clean data
        # model.fit(X_train)
    
    # Broadcast model
    broadcast_model = spark.sparkContext.broadcast(pickle.dumps(model))
    
    # Define schema for return
    schema = StructType([
        StructField("is_anomaly", BooleanType(), False),
        StructField("anomaly_score", FloatType(), False)
    ])
    
    @pandas_udf(schema)
    def detect_anomaly(sensor_data: pd.Series) -> pd.DataFrame:
        """
        Detect anomalies in sensor data.
        
        Args:
            sensor_data: Series of sensor readings (each element is list of values)
        
        Returns:
            DataFrame with is_anomaly and anomaly_score columns
        """
        # Load model from broadcast
        local_model = pickle.loads(broadcast_model.value)
        
        # Convert to numpy array
        X = np.array(sensor_data.tolist())
        
        # Predict (-1 for anomalies, 1 for normal)
        predictions = local_model.predict(X)
        
        # Get anomaly scores (more negative = more anomalous)
        scores = local_model.score_samples(X)
        
        # Convert predictions to boolean
        is_anomaly = predictions == -1
        
        return pd.DataFrame({
            'is_anomaly': is_anomaly,
            'anomaly_score': -scores  # Negate so higher = more anomalous
        })
    
    return detect_anomaly


def main():
    """Demonstrate anomaly detection"""
    
    print("="*60)
    print("Example 2: Anomaly Detection UDF")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("AnomalyDetection") \
        .master("local[4]") \
        .getOrCreate()
    
    print("\n1. Generating synthetic sensor data...")
    
    np.random.seed(42)
    num_samples = 10000
    
    # Generate normal data
    normal_data = np.random.normal(loc=50, scale=5, size=(9000, 5))
    
    # Generate anomalies (1000 samples with extreme values)
    anomaly_data = np.random.normal(loc=50, scale=20, size=(1000, 5))
    
    # Combine
    all_data = np.vstack([normal_data, anomaly_data])
    np.random.shuffle(all_data)
    
    # Create DataFrame
    data = [(i, row.tolist()) for i, row in enumerate(all_data)]
    df = spark.createDataFrame(data, ["sensor_id", "readings"])
    
    print(f"   Created {df.count():,} sensor readings")
    
    print("\n2. Training anomaly detection model...")
    
    # Train model on a sample (in production, use historical clean data)
    sample_data = all_data[:5000]  # First 5000 samples
    model = IsolationForest(contamination=0.1, random_state=42, n_estimators=100)
    model.fit(sample_data)
    
    # Save model
    with open('/tmp/anomaly_model.pkl', 'wb') as f:
        pickle.dump(model, f)
    
    print("   Model trained and saved")
    
    print("\n3. Creating anomaly detection UDF...")
    
    detect_anomaly = create_anomaly_detector(spark, '/tmp/anomaly_model.pkl')
    
    print("\n4. Detecting anomalies...")
    
    df_results = df.withColumn(
        "anomaly_detection",
        detect_anomaly(F.col("readings"))
    ).select(
        "sensor_id",
        "readings",
        F.col("anomaly_detection.is_anomaly").alias("is_anomaly"),
        F.col("anomaly_detection.anomaly_score").alias("anomaly_score")
    )
    
    print("\n5. Anomaly detection results:")
    df_results.show(20)
    
    print("\n6. Anomaly statistics:")
    df_results.groupBy("is_anomaly").count().show()
    
    print("\n7. High-risk anomalies (score > 1.0):")
    high_risk = df_results.filter(
        (F.col("is_anomaly") == True) & (F.col("anomaly_score") > 1.0)
    )
    high_risk.select("sensor_id", "anomaly_score").show(10)
    
    print(f"\n   Found {high_risk.count()} high-risk anomalies")
    
    spark.stop()
    print("\n✅ Anomaly detection completed!")


if __name__ == "__main__":
    main()
