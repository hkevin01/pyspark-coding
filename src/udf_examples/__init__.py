"""
UDF Examples for ML Inference
==============================

Production-ready User-Defined Functions for embedding ML models in data pipelines.

Available Modules:
- 01_basic_batch_inference: Basic PyTorch model inference
- 02_anomaly_detection_udf: Isolation Forest for anomaly detection
- 03_classification_udf: Multi-class classification with confidence
- 04_time_series_forecast_udf: LSTM-based forecasting
- 05_sentiment_analysis_udf: NLP sentiment analysis
- 07_fraud_detection_udf: Real-time fraud detection

Quick Start:
    from pyspark.sql import SparkSession
    from udf_examples.basic_batch_inference import create_inference_udf
    
    spark = SparkSession.builder.getOrCreate()
    predict_udf = create_inference_udf(spark, "models/model.pth")
    df_predictions = df.withColumn("pred", predict_udf(df.features))
"""

__version__ = "1.0.0"
__author__ = "Kevin"

__all__ = [
    "create_inference_udf",
    "create_anomaly_detector",
    "create_classifier",
    "create_forecaster",
    "create_sentiment_analyzer",
    "create_fraud_detector"
]
