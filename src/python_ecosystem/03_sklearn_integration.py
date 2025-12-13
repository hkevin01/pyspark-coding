"""
Scikit-learn Integration with PySpark

Scikit-learn is Python's most popular machine learning library. This module
shows how to use Scikit-learn models with PySpark for distributed ML.

WHAT IS SCIKIT-LEARN?
======================
- 100+ ML algorithms (classification, regression, clustering)
- Preprocessing and feature engineering
- Model selection and evaluation
- Pipeline API
- Consistent interface across all algorithms

WHY USE SCIKIT-LEARN WITH PYSPARK?
===================================
- Familiar API for data scientists
- Rich set of ML algorithms
- Easy experimentation and prototyping
- Use with Pandas UDFs for distributed inference
- Train on samples, predict on big data
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, struct
from pyspark.sql.types import DoubleType, ArrayType, StructType, StructField
import pandas as pd
import numpy as np

# =============================================================================
# 1. SCIKIT-LEARN MODEL TRAINING
# =============================================================================

def sklearn_model_training():
    """
    Train Scikit-learn models on sampled data.
    
    WORKFLOW:
    1. Sample data from Spark DataFrame
    2. Convert to Pandas
    3. Train Scikit-learn model
    4. Apply model to full Spark DataFrame
    """
    
    spark = SparkSession.builder \
        .appName("Sklearn Training") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate synthetic data
    from sklearn.datasets import make_classification
    
    X, y = make_classification(n_samples=10000, n_features=5, n_informative=3,
                               n_redundant=0, random_state=42)
    
    # Create Spark DataFrame
    data = [(float(X[i, 0]), float(X[i, 1]), float(X[i, 2]), 
             float(X[i, 3]), float(X[i, 4]), int(y[i])) 
            for i in range(len(y))]
    
    df = spark.createDataFrame(data, 
                               ["feature1", "feature2", "feature3", "feature4", "feature5", "label"])
    
    print("Dataset:")
    df.show(10)
    
    # ==========================================================================
    # Train Random Forest
    # ==========================================================================
    
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score, classification_report
    
    # Sample and convert to Pandas
    train_df = df.sample(0.8, seed=42).toPandas()
    test_df = df.subtract(df.sample(0.8, seed=42)).toPandas()
    
    # Prepare features
    feature_cols = ["feature1", "feature2", "feature3", "feature4", "feature5"]
    X_train = train_df[feature_cols].values
    y_train = train_df["label"].values
    X_test = test_df[feature_cols].values
    y_test = test_df["label"].values
    
    # Train model
    print("\nTraining Random Forest...")
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)
    
    # Evaluate
    y_pred = rf_model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"\nAccuracy: {accuracy:.4f}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
    # ==========================================================================
    # Distributed Inference with Pandas UDF
    # ==========================================================================
    
    @pandas_udf(DoubleType())
    def predict_udf(f1: pd.Series, f2: pd.Series, f3: pd.Series, 
                    f4: pd.Series, f5: pd.Series) -> pd.Series:
        """Apply trained model to distributed data"""
        X = pd.DataFrame({
            'f1': f1, 'f2': f2, 'f3': f3, 'f4': f4, 'f5': f5
        })
        predictions = rf_model.predict(X.values)
        return pd.Series(predictions)
    
    # Apply to full dataset
    result = df.withColumn(
        "prediction",
        predict_udf(col("feature1"), col("feature2"), col("feature3"),
                   col("feature4"), col("feature5"))
    )
    
    print("\nPredictions on full dataset:")
    result.show(10)
    
    spark.stop()

# =============================================================================
# 2. SCIKIT-LEARN PREPROCESSING
# =============================================================================

def sklearn_preprocessing():
    """
    Use Scikit-learn preprocessing with PySpark.
    
    COMMON USE CASES:
    - StandardScaler: Standardize features
    - MinMaxScaler: Normalize to [0, 1]
    - OneHotEncoder: Encode categorical features
    - LabelEncoder: Encode labels
    """
    
    spark = SparkSession.builder \
        .appName("Sklearn Preprocessing") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create test data
    df = spark.createDataFrame([
        (1, 10.0, 100.0, "A"),
        (2, 20.0, 200.0, "B"),
        (3, 30.0, 300.0, "A"),
        (4, 40.0, 400.0, "C"),
        (5, 50.0, 500.0, "B"),
    ], ["id", "feature1", "feature2", "category"])
    
    # ==========================================================================
    # Standard Scaling
    # ==========================================================================
    
    from sklearn.preprocessing import StandardScaler
    
    # Fit scaler on sample
    sample = df.select("feature1", "feature2").toPandas()
    scaler = StandardScaler()
    scaler.fit(sample)
    
    print("Fitted StandardScaler:")
    print(f"Mean: {scaler.mean_}")
    print(f"Scale: {scaler.scale_}")
    
    # Apply to full dataset
    @pandas_udf(DoubleType())
    def standardize_f1(x: pd.Series) -> pd.Series:
        """Standardize feature1"""
        return (x - scaler.mean_[0]) / scaler.scale_[0]
    
    @pandas_udf(DoubleType())
    def standardize_f2(x: pd.Series) -> pd.Series:
        """Standardize feature2"""
        return (x - scaler.mean_[1]) / scaler.scale_[1]
    
    result = df.select(
        col("id"),
        col("feature1"),
        col("feature2"),
        standardize_f1(col("feature1")).alias("scaled_f1"),
        standardize_f2(col("feature2")).alias("scaled_f2")
    )
    
    print("\nStandardized Features:")
    result.show()
    
    # ==========================================================================
    # One-Hot Encoding
    # ==========================================================================
    
    from sklearn.preprocessing import LabelEncoder
    
    # Fit encoder
    categories = df.select("category").toPandas()
    encoder = LabelEncoder()
    encoder.fit(categories["category"])
    
    print(f"\nLabel Encoder Classes: {encoder.classes_}")
    
    # Apply to full dataset
    @pandas_udf(IntegerType())
    def encode_category(x: pd.Series) -> pd.Series:
        """Encode categorical feature"""
        return pd.Series(encoder.transform(x))
    
    result2 = df.select(
        col("category"),
        encode_category(col("category")).alias("category_encoded")
    )
    
    print("\nEncoded Categories:")
    result2.show()
    
    spark.stop()

# =============================================================================
# 3. SCIKIT-LEARN PIPELINES
# =============================================================================

def sklearn_pipelines():
    """
    Use Scikit-learn Pipelines for complex workflows.
    
    BENEFITS:
    - Chain multiple transformations
    - Avoid data leakage
    - Easy cross-validation
    - Reproducible workflows
    """
    
    spark = SparkSession.builder \
        .appName("Sklearn Pipelines") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate data
    from sklearn.datasets import make_regression
    
    X, y = make_regression(n_samples=5000, n_features=10, noise=10, random_state=42)
    
    data = [(int(i),) + tuple(float(x) for x in X[i]) + (float(y[i]),)
            for i in range(len(y))]
    
    cols = ["id"] + [f"feature{i}" for i in range(10)] + ["target"]
    df = spark.createDataFrame(data, cols)
    
    # ==========================================================================
    # Build Pipeline
    # ==========================================================================
    
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import Ridge
    
    # Sample for training
    train_df = df.sample(0.8, seed=42).toPandas()
    
    feature_cols = [f"feature{i}" for i in range(10)]
    X_train = train_df[feature_cols].values
    y_train = train_df["target"].values
    
    # Create pipeline
    pipeline = Pipeline([
        ('scaler', StandardScaler()),
        ('model', Ridge(alpha=1.0))
    ])
    
    print("Training Pipeline:")
    print(pipeline)
    
    # Fit pipeline
    pipeline.fit(X_train, y_train)
    
    print("\nPipeline trained successfully!")
    
    # ==========================================================================
    # Apply Pipeline to Spark DataFrame
    # ==========================================================================
    
    @pandas_udf(DoubleType())
    def predict_pipeline(*features):
        """Apply entire pipeline"""
        X = pd.DataFrame({f'f{i}': features[i] for i in range(len(features))})
        predictions = pipeline.predict(X.values)
        return pd.Series(predictions)
    
    # Apply to full dataset
    feature_cols_spark = [col(f"feature{i}") for i in range(10)]
    result = df.withColumn("prediction", predict_pipeline(*feature_cols_spark))
    
    print("\nPipeline Predictions:")
    result.select("id", "target", "prediction").show(10)
    
    spark.stop()

# =============================================================================
# 4. CROSS-VALIDATION AND HYPERPARAMETER TUNING
# =============================================================================

def sklearn_cross_validation():
    """
    Perform cross-validation and hyperparameter tuning.
    
    WORKFLOW:
    1. Sample data for training
    2. Use GridSearchCV or RandomizedSearchCV
    3. Find best parameters
    4. Train final model
    5. Apply to full dataset
    """
    
    spark = SparkSession.builder \
        .appName("Sklearn CV") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Generate data
    from sklearn.datasets import make_classification
    
    X, y = make_classification(n_samples=5000, n_features=10, n_informative=5,
                               random_state=42)
    
    data = [(int(i),) + tuple(float(x) for x in X[i]) + (int(y[i]),)
            for i in range(len(y))]
    
    cols = ["id"] + [f"feature{i}" for i in range(10)] + ["label"]
    df = spark.createDataFrame(data, cols)
    
    # ==========================================================================
    # Grid Search
    # ==========================================================================
    
    from sklearn.model_selection import GridSearchCV
    from sklearn.ensemble import RandomForestClassifier
    
    # Sample for training
    train_df = df.sample(0.2, seed=42).toPandas()  # Smaller sample for speed
    
    feature_cols = [f"feature{i}" for i in range(10)]
    X_train = train_df[feature_cols].values
    y_train = train_df["label"].values
    
    # Define parameter grid
    param_grid = {
        'n_estimators': [50, 100],
        'max_depth': [5, 10],
        'min_samples_split': [2, 5]
    }
    
    # Grid search
    print("Running Grid Search...")
    rf = RandomForestClassifier(random_state=42)
    grid_search = GridSearchCV(rf, param_grid, cv=3, scoring='accuracy', n_jobs=-1)
    grid_search.fit(X_train, y_train)
    
    print(f"\nBest Parameters: {grid_search.best_params_}")
    print(f"Best Score: {grid_search.best_score_:.4f}")
    
    # ==========================================================================
    # Use Best Model
    # ==========================================================================
    
    best_model = grid_search.best_estimator_
    
    @pandas_udf(DoubleType())
    def predict_best(*features):
        """Apply best model"""
        X = pd.DataFrame({f'f{i}': features[i] for i in range(len(features))})
        predictions = best_model.predict(X.values)
        return pd.Series(predictions)
    
    # Apply to full dataset
    feature_cols_spark = [col(f"feature{i}") for i in range(10)]
    result = df.withColumn("prediction", predict_best(*feature_cols_spark))
    
    print("\nBest Model Predictions:")
    result.select("id", "label", "prediction").show(10)
    
    spark.stop()

# =============================================================================
# 5. MODEL PERSISTENCE
# =============================================================================

def sklearn_model_persistence():
    """
    Save and load Scikit-learn models.
    
    OPTIONS:
    - joblib: Efficient for large models
    - pickle: Standard Python serialization
    - Can broadcast to all workers
    """
    
    spark = SparkSession.builder \
        .appName("Sklearn Persistence") \
        .master("local[*]") \
        .getOrCreate()
    
    # Train a simple model
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.datasets import make_classification
    import joblib
    import tempfile
    import os
    
    X, y = make_classification(n_samples=1000, n_features=5, random_state=42)
    
    model = RandomForestClassifier(n_estimators=10, random_state=42)
    model.fit(X, y)
    
    print("Model trained successfully!")
    
    # ==========================================================================
    # Save Model
    # ==========================================================================
    
    # Create temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix='.joblib') as tmp:
        model_path = tmp.name
    
    joblib.dump(model, model_path)
    print(f"\n✅ Model saved to: {model_path}")
    
    # ==========================================================================
    # Load Model
    # ==========================================================================
    
    loaded_model = joblib.load(model_path)
    print(f"✅ Model loaded successfully!")
    
    # Test loaded model
    predictions = loaded_model.predict(X[:5])
    print(f"\nTest Predictions: {predictions}")
    
    # ==========================================================================
    # Broadcast Model (for production use)
    # ==========================================================================
    
    # Broadcast model to all workers
    broadcast_model = spark.sparkContext.broadcast(loaded_model)
    
    print("\n✅ Model broadcasted to all workers!")
    print("Now you can use it in Pandas UDFs without re-serializing")
    
    # Cleanup
    os.unlink(model_path)
    
    spark.stop()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("SCIKIT-LEARN INTEGRATION WITH PYSPARK")
    print("=" * 70)
    
    print("\n1. Scikit-learn Model Training")
    print("-" * 70)
    sklearn_model_training()
    
    print("\n2. Scikit-learn Preprocessing")
    print("-" * 70)
    sklearn_preprocessing()
    
    print("\n3. Scikit-learn Pipelines")
    print("-" * 70)
    sklearn_pipelines()
    
    print("\n4. Cross-Validation and Hyperparameter Tuning")
    print("-" * 70)
    sklearn_cross_validation()
    
    print("\n5. Model Persistence")
    print("-" * 70)
    sklearn_model_persistence()
    
    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS:")
    print("=" * 70)
    print("✅ Train Scikit-learn models on sampled data")
    print("✅ Use Pandas UDFs for distributed inference")
    print("✅ Leverage Pipelines for reproducible workflows")
    print("✅ Grid Search for hyperparameter tuning")
    print("✅ Broadcast models for efficient distributed prediction")
    print("=" * 70 + "\n")
