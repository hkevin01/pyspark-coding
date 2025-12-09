"""
Spark Session Utility Module
Handles creation and configuration of Spark sessions
"""
import os
from typing import Optional

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "PySpark ETL Application",
    master: str = "local[*]",
    config: Optional[dict] = None
) -> SparkSession:
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name of the Spark application
        master: Spark master URL (default: local[*])
        config: Additional Spark configuration as dictionary
        
    Returns:
        SparkSession: Configured Spark session
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    )
    
    # Add custom configurations
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stop the Spark session.
    
    Args:
        spark: The SparkSession to stop
    """
    if spark:
        spark.stop()


def get_spark_session() -> SparkSession:
    """
    Get or create the active Spark session.
    
    Returns:
        SparkSession: The active Spark session
    """
    return SparkSession.builder.getOrCreate()
