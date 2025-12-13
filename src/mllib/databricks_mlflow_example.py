#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
DATABRICKS + MLFLOW - Production ML Platform
================================================================================

MODULE OVERVIEW:
----------------
Databricks provides a unified analytics platform built on Apache Spark.
MLflow is an open-source platform for managing the complete ML lifecycle including:
• Experiment tracking
• Model registry
• Model deployment
• Reproducibility

This module demonstrates production ML workflows on Databricks with MLflow.

PURPOSE:
--------
Learn Databricks + MLflow patterns:
• MLflow experiment tracking
• Model registry and versioning
• Databricks-specific optimizations
• Delta Lake integration
• Production deployment patterns

DATABRICKS ARCHITECTURE:
------------------------
┌──────────────────────────────────────────────────────────────────┐
│                    DATABRICKS PLATFORM                          │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Workspace                                                      │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Notebooks  │  Jobs  │  Models  │  Data             │         │
│  └────────────────────────────────────────────────────┘         │
│                          ↓                                       │
│  Delta Lake (Storage Layer)                                     │
│  ┌────────────────────────────────────────────────────┐         │
│  │  ACID transactions                                 │         │
│  │  Time travel                                       │         │
│  │  Schema enforcement                                │         │
│  └────────────────────────────────────────────────────┘         │
│                          ↓                                       │
│  Spark Cluster (Compute)                                        │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Auto-scaling                                      │         │
│  │  Spot instances                                    │         │
│  │  GPU support                                       │         │
│  └────────────────────────────────────────────────────┘         │
│                          ↓                                       │
│  MLflow (ML Lifecycle)                                          │
│  ┌────────────────────────────────────────────────────┐         │
│  │  Tracking  │  Registry  │  Deployment              │         │
│  └────────────────────────────────────────────────────┘         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

MLFLOW WORKFLOW:
----------------
1. Track experiments → 2. Register best model → 3. Deploy to production
"""

# NOTE: These examples are designed to run on Databricks
# For local development, install: pip install mlflow databricks-cli

try:
    import mlflow
    import mlflow.spark
    from mlflow.models.signature import infer_signature
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    print("⚠️  MLflow not installed. Install with: pip install mlflow")

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import os


def create_spark():
    """Create Spark session (works locally and on Databricks)."""
    return SparkSession.builder \
        .appName("Databricks_MLflow") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def example1_mlflow_tracking():
    """
    EXAMPLE 1: MLflow Experiment Tracking
    ======================================
    
    Track experiments with parameters, metrics, and artifacts.
    """
    print("=" * 70)
    print("EXAMPLE 1: MLflow Experiment Tracking")
    print("=" * 70)
    
    if not MLFLOW_AVAILABLE:
        print("\n⚠️  MLflow not available")
        return
    
    print("""
    📊 MLFLOW TRACKING WORKFLOW:
    
    1. Start experiment
    2. Log parameters
    3. Train model
    4. Log metrics
    5. Save artifacts
    6. Compare runs
    
    🔧 Code Example:
    
    import mlflow
    import mlflow.spark
    
    # Set experiment
    mlflow.set_experiment("/Users/yourname/my-experiment")
    
    # Start run
    with mlflow.start_run(run_name="random_forest_v1"):
        
        # Log parameters
        mlflow.log_param("num_trees", 100)
        mlflow.log_param("max_depth", 10)
        mlflow.log_param("min_instances_per_node", 5)
        
        # Train model
        rf = RandomForestClassifier(
            numTrees=100,
            maxDepth=10,
            minInstancesPerNode=5
        )
        model = rf.fit(train_df)
        
        # Make predictions
        predictions = model.transform(test_df)
        
        # Evaluate
        evaluator = BinaryClassificationEvaluator()
        auc = evaluator.evaluate(predictions)
        accuracy = predictions.filter(col("prediction") == col("label")).count() / predictions.count()
        
        # Log metrics
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("accuracy", accuracy)
        
        # Log model
        mlflow.spark.log_model(
            model, 
            "model",
            signature=infer_signature(train_df, predictions)
        )
        
        # Log artifacts (plots, data, etc.)
        import matplotlib.pyplot as plt
        plt.figure()
        plt.plot([1, 2, 3], [4, 5, 6])
        plt.savefig("plot.png")
        mlflow.log_artifact("plot.png")
    
    💡 Benefits:
    ✅ Track all experiments automatically
    ✅ Compare runs side-by-side
    ✅ Reproduce any experiment
    ✅ Share results with team
    """)


def example2_model_registry():
    """
    EXAMPLE 2: MLflow Model Registry
    =================================
    
    Version and manage models through their lifecycle.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: MLflow Model Registry")
    print("=" * 70)
    
    print("""
    📦 MODEL REGISTRY WORKFLOW:
    
    ┌──────────────────────────────────────────────────────┐
    │           MODEL LIFECYCLE STAGES                     │
    ├──────────────────────────────────────────────────────┤
    │                                                      │
    │  1. None (default)                                  │
    │     ↓                                                │
    │  2. Staging (testing)                               │
    │     ↓                                                │
    │  3. Production (live)                               │
    │     ↓                                                │
    │  4. Archived (retired)                              │
    │                                                      │
    └──────────────────────────────────────────────────────┘
    
    🔧 Register Model:
    
    # After training, register model
    model_uri = f"runs:/{run.info.run_id}/model"
    
    mlflow.register_model(
        model_uri=model_uri,
        name="fraud_detection_model"
    )
    
    🔧 Transition to Staging:
    
    from mlflow.tracking import MlflowClient
    client = MlflowClient()
    
    client.transition_model_version_stage(
        name="fraud_detection_model",
        version=1,
        stage="Staging"
    )
    
    🔧 Load Model from Registry:
    
    # Load staging model
    model = mlflow.pyfunc.load_model(
        "models:/fraud_detection_model/Staging"
    )
    
    # Load production model
    model = mlflow.pyfunc.load_model(
        "models:/fraud_detection_model/Production"
    )
    
    # Load specific version
    model = mlflow.pyfunc.load_model(
        "models:/fraud_detection_model/3"
    )
    
    🔧 Production Deployment:
    
    # Test in staging
    staging_model = mlflow.spark.load_model("models:/my_model/Staging")
    test_results = evaluate_model(staging_model, test_data)
    
    # If good, promote to production
    if test_results['auc'] > 0.85:
        client.transition_model_version_stage(
            name="my_model",
            version=3,
            stage="Production"
        )
        print("Model promoted to Production!")
    
    💡 Benefits:
    ✅ Version control for models
    ✅ Staged rollout (staging → production)
    ✅ Easy rollback if issues
    ✅ Model lineage tracking
    ✅ Centralized model store
    """)


def example3_delta_lake_integration():
    """
    EXAMPLE 3: Delta Lake Integration
    ==================================
    
    ACID transactions and time travel for data.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Delta Lake Integration")
    print("=" * 70)
    
    print("""
    🗄️  DELTA LAKE FEATURES:
    
    ✅ ACID transactions
    ✅ Time travel (version history)
    ✅ Schema enforcement & evolution
    ✅ Upserts and deletes
    ✅ Streaming + batch unification
    
    🔧 Write Delta Table:
    
    # Write DataFrame to Delta
    df.write.format("delta").mode("overwrite").save("/path/to/delta")
    
    # Or create table
    df.write.format("delta").saveAsTable("my_table")
    
    🔧 Read Delta Table:
    
    # Read latest version
    df = spark.read.format("delta").load("/path/to/delta")
    
    # Or read table
    df = spark.read.table("my_table")
    
    🔧 Time Travel:
    
    # Read specific version
    df = spark.read.format("delta").option("versionAsOf", 5).load("/path/to/delta")
    
    # Read as of timestamp
    df = spark.read.format("delta") \\
        .option("timestampAsOf", "2024-01-01") \\
        .load("/path/to/delta")
    
    # Show history
    spark.sql("DESCRIBE HISTORY my_table").show()
    
    🔧 Upserts (Merge):
    
    from delta.tables import DeltaTable
    
    deltaTable = DeltaTable.forPath(spark, "/path/to/delta")
    
    deltaTable.alias("target").merge(
        updates.alias("source"),
        "target.id = source.id"
    ).whenMatchedUpdateAll() \\
     .whenNotMatchedInsertAll() \\
     .execute()
    
    🔧 Optimization:
    
    # Optimize (compaction)
    spark.sql("OPTIMIZE my_table")
    
    # Z-order (clustering)
    spark.sql("OPTIMIZE my_table ZORDER BY (user_id, date)")
    
    # Vacuum (delete old files)
    spark.sql("VACUUM my_table RETAIN 168 HOURS")  # 7 days
    
    💡 ML Workflow with Delta:
    
    # 1. Read training data from Delta
    train_df = spark.read.table("features_table")
    
    # 2. Train model
    model = train_pipeline(train_df)
    
    # 3. Score new data
    predictions = model.transform(new_data)
    
    # 4. Write predictions to Delta
    predictions.write.format("delta").mode("append") \\
        .saveAsTable("predictions_table")
    
    # 5. Track with MLflow
    with mlflow.start_run():
        mlflow.log_param("data_version", train_df.version)
        mlflow.log_model(model, "model")
    """)


def example4_databricks_automl():
    """
    EXAMPLE 4: Databricks AutoML
    =============================
    
    Automated machine learning.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Databricks AutoML")
    print("=" * 70)
    
    print("""
    🤖 AUTOML WORKFLOW:
    
    1. Load data
    2. Run AutoML
    3. Review results
    4. Deploy best model
    
    🔧 Python API:
    
    from databricks import automl
    
    # Classification
    summary = automl.classify(
        dataset=train_df,
        target_col="label",
        primary_metric="f1",
        timeout_minutes=30,
        max_trials=20
    )
    
    # Regression
    summary = automl.regress(
        dataset=train_df,
        target_col="price",
        primary_metric="rmse",
        timeout_minutes=30
    )
    
    # Get best run
    best_trial = summary.best_trial
    
    # Load best model
    model = mlflow.sklearn.load_model(f"runs:/{best_trial.mlflow_run_id}/model")
    
    📊 What AutoML Does:
    
    ✅ Feature preprocessing
    ✅ Algorithm selection
    ✅ Hyperparameter tuning
    ✅ Cross-validation
    ✅ Model explanation
    ✅ Generates notebook with code
    
    🎯 Algorithms Tried:
    
    Classification:
    • Logistic Regression
    • Decision Tree
    • Random Forest
    • XGBoost
    • LightGBM
    
    Regression:
    • Linear Regression
    • Decision Tree
    • Random Forest
    • XGBoost
    • LightGBM
    
    💡 Best Practices:
    
    ✅ Clean data beforehand
    ✅ Set timeout appropriately
    ✅ Review generated notebook
    ✅ Customize if needed
    ✅ Use as baseline
    """)


def example5_production_deployment():
    """
    EXAMPLE 5: Production Deployment Patterns
    ==========================================
    
    Deploy models to production on Databricks.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Production Deployment")
    print("=" * 70)
    
    print("""
    🚀 DEPLOYMENT OPTIONS:
    
    1. Batch Inference (Scheduled Jobs)
    2. Real-time Inference (Model Serving)
    3. Streaming Inference (Structured Streaming)
    
    📊 OPTION 1: Batch Inference Job
    
    # inference_job.py
    import mlflow
    
    # Load production model
    model = mlflow.spark.load_model("models:/my_model/Production")
    
    # Load new data
    new_data = spark.read.table("new_customers")
    
    # Make predictions
    predictions = model.transform(new_data)
    
    # Write results
    predictions.write.format("delta").mode("append") \\
        .saveAsTable("predictions")
    
    # Schedule in Databricks:
    # Jobs → Create Job → Schedule: Daily at 2 AM
    
    📊 OPTION 2: Real-time Model Serving
    
    # Enable model serving in UI
    # Or via API:
    
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    
    endpoint = w.serving_endpoints.create(
        name="fraud-detection",
        config={
            "served_models": [{
                "model_name": "fraud_detection_model",
                "model_version": "1",
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }]
        }
    )
    
    # Query endpoint:
    
    import requests
    
    response = requests.post(
        "https://<workspace>.cloud.databricks.com/serving-endpoints/fraud-detection/invocations",
        headers={"Authorization": f"Bearer {token}"},
        json={"dataframe_records": [{"feature1": 1, "feature2": 2}]}
    )
    
    📊 OPTION 3: Streaming Inference
    
    # Load model
    model = mlflow.spark.load_model("models:/my_model/Production")
    
    # Read stream
    stream_df = spark.readStream \\
        .format("delta") \\
        .table("incoming_events")
    
    # Apply model
    predictions_stream = model.transform(stream_df)
    
    # Write stream
    query = predictions_stream.writeStream \\
        .format("delta") \\
        .outputMode("append") \\
        .option("checkpointLocation", "/checkpoints/predictions") \\
        .table("real_time_predictions")
    
    query.awaitTermination()
    
    🔧 MONITORING AND ALERTING:
    
    # Track prediction metrics
    with mlflow.start_run():
        mlflow.log_metric("predictions_count", predictions.count())
        mlflow.log_metric("avg_confidence", 
            predictions.select(avg("probability")).first()[0])
    
    # Set alerts in Databricks:
    # SQL Warehouse → Create Alert
    # Example: Alert if prediction count drops below threshold
    
    💡 PRODUCTION CHECKLIST:
    
    ✅ Model versioned and tested
    ✅ Data pipeline validated
    ✅ Monitoring configured
    ✅ Alerts set up
    ✅ Rollback plan ready
    ✅ Documentation updated
    ✅ Stakeholders notified
    """)


def show_databricks_best_practices():
    """Best practices for Databricks + MLflow."""
    print("\n" + "=" * 70)
    print("DATABRICKS + MLFLOW BEST PRACTICES")
    print("=" * 70)
    
    print("""
    🎯 BEST PRACTICES:
    
    1. EXPERIMENT MANAGEMENT
       ✅ Use meaningful experiment names
       ✅ Tag runs with metadata
       ✅ Document parameters and metrics
       ✅ Save artifacts (plots, data samples)
       ❌ Don't leave experiments unorganized
    
    2. MODEL VERSIONING
       ✅ Use Model Registry
       ✅ Stage: None → Staging → Production
       ✅ Add model descriptions
       ✅ Test before promoting
       ❌ Don't skip staging
    
    3. DATA MANAGEMENT
       ✅ Use Delta Lake for all tables
       ✅ Partition large tables
       ✅ Optimize regularly
       ✅ Vacuum old files
       ❌ Don't use CSV for production
    
    4. CLUSTER CONFIGURATION
       ✅ Use autoscaling
       ✅ Spot instances for dev/test
       ✅ GPU clusters for deep learning
       ✅ Right-size executors
       ❌ Don't over-provision
    
    5. COST OPTIMIZATION
       ✅ Auto-terminate idle clusters
       ✅ Use pools for faster startup
       ✅ Spot instances (70% cheaper)
       ✅ Right-size clusters
       ✅ Cache frequently used data
    
    📊 COST SAVINGS TIPS:
    
    # 1. Auto-terminate
    spark.conf.set("spark.databricks.cluster.autoTermination.enabled", "true")
    spark.conf.set("spark.databricks.cluster.autoTermination.minutes", "30")
    
    # 2. Cache data
    df.cache()
    df.count()  # Materialize cache
    
    # 3. Optimize queries
    df.repartition(200)  # Optimal partition count
    df.coalesce(10)      # Reduce partitions for small data
    
    # 4. Use Delta caching
    spark.sql("CACHE SELECT * FROM my_table")
    
    🚀 PERFORMANCE TIPS:
    
    1. Photon Engine:
       Enable for 2-3x speedup on SQL queries
    
    2. Adaptive Query Execution:
       spark.conf.set("spark.sql.adaptive.enabled", "true")
    
    3. Z-ordering:
       OPTIMIZE my_table ZORDER BY (user_id, date)
    
    4. Broadcast joins:
       df.join(broadcast(small_df), "key")
    
    5. Predicate pushdown:
       Use filters early in pipeline
    
    📋 CHECKLIST FOR PRODUCTION:
    
    ✅ Code reviewed and tested
    ✅ Model accuracy > threshold
    ✅ Data quality checks passed
    ✅ Performance benchmarked
    ✅ Monitoring configured
    ✅ Alerts set up
    ✅ Documentation complete
    ✅ Rollback plan ready
    ✅ Stakeholders notified
    ✅ Cost estimated
    """)


def main():
    """Run all Databricks + MLflow examples."""
    print("🧱 DATABRICKS + MLFLOW PLATFORM")
    print("=" * 70)
    print("\nProduction ML platform with experiment tracking and deployment!")
    
    # Check MLflow availability
    if MLFLOW_AVAILABLE:
        print("\n✅ MLflow available")
    else:
        print("\n⚠️  MLflow not available")
        print("   Install: pip install mlflow databricks-cli")
    
    # Run examples
    example1_mlflow_tracking()
    example2_model_registry()
    example3_delta_lake_integration()
    example4_databricks_automl()
    example5_production_deployment()
    
    # Show best practices
    show_databricks_best_practices()
    
    print("\n" + "=" * 70)
    print("✅ DATABRICKS + MLFLOW EXAMPLES COMPLETE")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. MLflow tracks experiments and models")
    print("   2. Model Registry manages lifecycle")
    print("   3. Delta Lake provides ACID + time travel")
    print("   4. AutoML accelerates development")
    print("   5. Multiple deployment options")
    print("   6. Monitor and optimize costs")
    
    print("\n📚 See Also:")
    print("   • 01_ml_pipelines.py - MLlib pipelines")
    print("   • ../gpu_acceleration/ - GPU acceleration")
    
    print("\n🔗 Resources:")
    print("   • MLflow: https://mlflow.org")
    print("   • Databricks: https://docs.databricks.com")
    print("   • Delta Lake: https://delta.io")


if __name__ == "__main__":
    main()
