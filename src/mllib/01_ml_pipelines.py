#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
SPARK MLLIB #1 - Machine Learning Pipelines
================================================================================

MODULE OVERVIEW:
----------------
Spark MLlib provides scalable machine learning on distributed data.
ML Pipelines provide a high-level API for building ML workflows that include:
• Feature engineering (transformers)
• Model training (estimators)
• Model evaluation
• Hyperparameter tuning

This module demonstrates end-to-end ML pipeline construction.

PURPOSE:
--------
Learn Spark MLlib pipelines:
• Transformers (feature engineering)
• Estimators (model training)
• Pipeline construction
• Model persistence
• Production deployment patterns

ML PIPELINE ARCHITECTURE:
-------------------------
┌─────────────────────────────────────────────────────────────────┐
│                  SPARK ML PIPELINE                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Data Ingestion                                             │
│     ┌────────────────────────────────────────┐                 │
│     │ Load raw data (CSV, Parquet, etc.)     │                 │
│     └────────────────────────────────────────┘                 │
│                        ↓                                        │
│  2. Feature Engineering (Transformers)                         │
│     ┌────────────────────────────────────────┐                 │
│     │ • StringIndexer (categorical → index)  │                 │
│     │ • OneHotEncoder (index → vector)       │                 │
│     │ • VectorAssembler (combine features)   │                 │
│     │ • StandardScaler (normalization)       │                 │
│     └────────────────────────────────────────┘                 │
│                        ↓                                        │
│  3. Model Training (Estimator)                                 │
│     ┌────────────────────────────────────────┐                 │
│     │ • LogisticRegression                   │                 │
│     │ • RandomForestClassifier               │                 │
│     │ • GBTClassifier                        │                 │
│     │ • LinearRegression                     │                 │
│     └────────────────────────────────────────┘                 │
│                        ↓                                        │
│  4. Model Evaluation                                           │
│     ┌────────────────────────────────────────┐                 │
│     │ • Accuracy, Precision, Recall, F1      │                 │
│     │ • ROC-AUC, PR-AUC                      │                 │
│     │ • RMSE, R2 (regression)                │                 │
│     └────────────────────────────────────────┘                 │
│                        ↓                                        │
│  5. Model Persistence                                          │
│     ┌────────────────────────────────────────┐                 │
│     │ • Save model to disk                   │                 │
│     │ • Load for inference                   │                 │
│     └────────────────────────────────────────┘                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

KEY CONCEPTS:
-------------
• Transformer: Takes DataFrame, returns transformed DataFrame
• Estimator: Takes DataFrame, returns Model (which is a Transformer)
• Pipeline: Chain of Transformers and Estimators
• Model: Trained Estimator (can transform data)
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col, when
import os


def create_spark():
    """Create Spark session for MLlib."""
    return SparkSession.builder \
        .appName("MLlib_Pipelines") \
        .master("local[*]") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()


def example1_basic_pipeline(spark):
    """
    EXAMPLE 1: Basic Classification Pipeline
    =========================================
    
    Complete ML pipeline: Features → Training → Evaluation
    """
    print("=" * 70)
    print("EXAMPLE 1: Basic Classification Pipeline")
    print("=" * 70)
    
    # Create sample data
    data = spark.createDataFrame([
        (0, "male", 22, 1, 0),
        (1, "female", 38, 1, 1),
        (1, "female", 26, 0, 1),
        (0, "male", 35, 1, 0),
        (1, "female", 35, 0, 1),
        (0, "male", 54, 0, 0),
        (1, "female", 27, 0, 1),
        (0, "male", 28, 1, 0),
    ] * 1000, ["survived", "sex", "age", "sibsp", "label"])
    
    print(f"\n📊 Dataset: {data.count():,} rows")
    data.show(5)
    
    # Split data
    train, test = data.randomSplit([0.8, 0.2], seed=42)
    print(f"\n✂️  Train: {train.count():,} | Test: {test.count():,}")
    
    # Stage 1: Index categorical features
    indexer = StringIndexer(inputCol="sex", outputCol="sex_index")
    
    # Stage 2: One-hot encode
    encoder = OneHotEncoder(inputCol="sex_index", outputCol="sex_vec")
    
    # Stage 3: Assemble features
    assembler = VectorAssembler(
        inputCols=["sex_vec", "age", "sibsp"],
        outputCol="features"
    )
    
    # Stage 4: Train model
    lr = LogisticRegression(featuresCol="features", labelCol="label")
    
    # Create pipeline
    pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])
    
    print("\n🔧 Pipeline stages:")
    for i, stage in enumerate(pipeline.getStages()):
        print(f"   {i+1}. {stage.__class__.__name__}")
    
    # Train
    print("\n🎯 Training model...")
    model = pipeline.fit(train)
    
    # Predict
    predictions = model.transform(test)
    
    # Evaluate
    evaluator = BinaryClassificationEvaluator(labelCol="label")
    auc = evaluator.evaluate(predictions)
    
    print(f"\n📊 Model Performance:")
    print(f"   AUC-ROC: {auc:.4f}")
    
    # Show predictions
    print("\n📝 Sample Predictions:")
    predictions.select("sex", "age", "label", "prediction", "probability").show(10)
    
    print("\n💡 Pipeline Benefits:")
    print("   ✅ Reproducible workflow")
    print("   ✅ Easy to modify stages")
    print("   ✅ Single fit/transform")
    print("   ✅ Production-ready")


def example2_save_load_model(spark):
    """
    EXAMPLE 2: Model Persistence
    =============================
    
    Save and load trained models.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Model Persistence")
    print("=" * 70)
    
    print("""
    📁 MODEL SAVING PATTERNS:
    
    # Save entire pipeline model
    model.write().overwrite().save("/path/to/model")
    
    # Load model
    from pyspark.ml import PipelineModel
    loaded_model = PipelineModel.load("/path/to/model")
    
    # Use for inference
    predictions = loaded_model.transform(new_data)
    
    💡 Best Practices:
    ✅ Version your models (v1, v2, etc.)
    ✅ Save metadata (training date, metrics)
    ✅ Test loading before deploying
    ✅ Use MLflow for experiment tracking
    
    📂 Directory Structure:
    models/
    ├── logistic_regression_v1/
    │   ├── metadata/
    │   └── stages/
    ├── random_forest_v2/
    │   ├── metadata/
    │   └── stages/
    └── xgboost_v3/
        ├── metadata/
        └── stages/
    
    🔄 Model Deployment Workflow:
    
    1. Train model in development
    2. Save model to staging
    3. Test model in staging
    4. Promote to production
    5. Monitor performance
    6. Rollback if needed
    """)


def example3_hyperparameter_tuning(spark):
    """
    EXAMPLE 3: Hyperparameter Tuning with Cross-Validation
    =======================================================
    
    Automated hyperparameter search.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Hyperparameter Tuning")
    print("=" * 70)
    
    print("""
    🎛️  HYPERPARAMETER TUNING WORKFLOW:
    
    1. Define parameter grid
    2. Create cross-validator
    3. Fit on training data
    4. Get best model
    
    📊 Example: Random Forest Tuning
    
    from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
    
    # Build pipeline
    rf = RandomForestClassifier(labelCol="label", featuresCol="features")
    pipeline = Pipeline(stages=[indexer, assembler, rf])
    
    # Parameter grid
    paramGrid = ParamGridBuilder() \\
        .addGrid(rf.numTrees, [10, 20, 50]) \\
        .addGrid(rf.maxDepth, [5, 10, 15]) \\
        .addGrid(rf.minInstancesPerNode, [1, 5, 10]) \\
        .build()
    
    # Cross-validator
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=BinaryClassificationEvaluator(),
        numFolds=5,
        parallelism=4  # Run 4 folds in parallel
    )
    
    # Fit (this trains multiple models!)
    cvModel = cv.fit(train)
    
    # Best model
    bestModel = cvModel.bestModel
    
    # Best parameters
    print("Best Parameters:")
    for param, value in zip(paramGrid[cvModel.avgMetrics.index(max(cvModel.avgMetrics))].keys(),
                           paramGrid[cvModel.avgMetrics.index(max(cvModel.avgMetrics))].values()):
        print(f"  {param.name}: {value}")
    
    ⚡ OPTIMIZATION TIPS:
    
    ✅ Use parallelism parameter (4-8x speedup)
    ✅ Start with coarse grid, then refine
    ✅ Use stratified sampling for imbalanced data
    ✅ Cache training data for multiple iterations
    ✅ Monitor with Spark UI
    
    🚀 Parallelism Example:
    
    # 3 hyperparameters × 3 values each = 27 combinations
    # 5-fold CV = 135 model trainings
    # With parallelism=4: 4 models trained simultaneously
    # Time reduction: 4x faster!
    """)


def show_mllib_best_practices():
    """Best practices for MLlib pipelines."""
    print("\n" + "=" * 70)
    print("MLLIB BEST PRACTICES")
    print("=" * 70)
    
    print("""
    🎯 BEST PRACTICES:
    
    1. DATA PREPARATION
       ✅ Handle missing values
       ✅ Remove duplicates
       ✅ Balance classes (if needed)
       ✅ Feature scaling/normalization
       ❌ Don't skip EDA
    
    2. FEATURE ENGINEERING
       ✅ Domain knowledge
       ✅ Feature selection
       ✅ Handle categorical variables
       ✅ Create interaction features
       ❌ Don't overfit on training data
    
    3. MODEL TRAINING
       ✅ Use train/validation/test split
       ✅ Cross-validation for tuning
       ✅ Start with simple models
       ✅ Cache training data
       ❌ Don't train on test data
    
    4. MODEL EVALUATION
       ✅ Multiple metrics (accuracy, F1, AUC)
       ✅ Confusion matrix
       ✅ Feature importance
       ✅ Error analysis
       ❌ Don't rely on single metric
    
    5. PRODUCTION DEPLOYMENT
       ✅ Version your models
       ✅ Monitor performance
       ✅ A/B testing
       ✅ Logging and alerts
       ❌ Don't deploy without testing
    
    📊 COMMON ALGORITHMS:
    
    Classification:
    • LogisticRegression (baseline, fast)
    • RandomForestClassifier (robust, feature importance)
    • GBTClassifier (high accuracy, slower)
    • LinearSVC (large datasets)
    • NaiveBayes (text classification)
    
    Regression:
    • LinearRegression (baseline)
    • RandomForestRegressor (robust)
    • GBTRegressor (high accuracy)
    • GeneralizedLinearRegression (various distributions)
    
    Clustering:
    • KMeans (fast, spherical clusters)
    • BisectingKMeans (hierarchical)
    • GaussianMixture (probabilistic)
    
    💡 ALGORITHM SELECTION:
    
    ┌──────────────────┬────────────────┬──────────────┐
    │ Use Case         │ Algorithm      │ Why          │
    ├──────────────────┼────────────────┼──────────────┤
    │ Binary class     │ Logistic Reg   │ Baseline     │
    │ Multi-class      │ Random Forest  │ Robust       │
    │ Large dataset    │ LinearSVC      │ Scales well  │
    │ Imbalanced       │ GBT + weights  │ Handles well │
    │ Text             │ NaiveBayes     │ Fast for text│
    │ Regression       │ Linear Reg     │ Baseline     │
    │ Non-linear       │ Random Forest  │ Captures     │
    │ Time series      │ ARIMA/Prophet  │ Specialized  │
    └──────────────────┴────────────────┴──────────────┘
    
    �� PERFORMANCE OPTIMIZATION:
    
    1. Caching:
       train.cache()  # Cache training data
       
    2. Partitioning:
       df.repartition(200)  # Optimal partitions
       
    3. Broadcast joins:
       df.join(broadcast(small_df), "key")
       
    4. Parallelism:
       CrossValidator(parallelism=8)
       
    5. Resource allocation:
       --executor-memory 16g
       --executor-cores 4
       --num-executors 10
    """)


def main():
    """Run all MLlib pipeline examples."""
    spark = create_spark()
    
    print("🤖 SPARK MLLIB - MACHINE LEARNING PIPELINES")
    print("=" * 70)
    print("\nScalable machine learning on distributed data!")
    
    # Run examples
    example1_basic_pipeline(spark)
    example2_save_load_model(spark)
    example3_hyperparameter_tuning(spark)
    
    # Show best practices
    show_mllib_best_practices()
    
    print("\n" + "=" * 70)
    print("✅ MLLIB EXAMPLES COMPLETE")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Pipelines = reproducible ML workflows")
    print("   2. Transformers + Estimators pattern")
    print("   3. Save/load models for production")
    print("   4. Cross-validation for tuning")
    print("   5. Monitor and version models")
    
    print("\n📚 See Also:")
    print("   • 02_feature_engineering.py - Advanced features")
    print("   • 03_model_evaluation.py - Evaluation metrics")
    print("   • databricks_mlflow_example.py - MLflow tracking")
    
    spark.stop()


if __name__ == "__main__":
    main()
