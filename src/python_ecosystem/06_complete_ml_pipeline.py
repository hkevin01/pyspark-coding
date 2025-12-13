"""
Complete ML Pipeline with Python Ecosystem + PySpark

This module demonstrates an end-to-end machine learning pipeline combining
all the Python ecosystem libraries with PySpark.

PIPELINE STAGES:
================
1. Data Loading & Exploration (PySpark)
2. Feature Engineering (NumPy + Pandas UDFs)
3. Preprocessing (Scikit-learn)
4. Model Training (Scikit-learn / PyTorch)
5. Distributed Inference (Pandas UDFs)
6. Visualization & Analysis (Matplotlib + Seaborn)

THIS IS THE POWER OF PYSPARK!
==============================
Use the entire Python ecosystem on distributed big data!
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, rand, when
from pyspark.sql.types import DoubleType, ArrayType, FloatType
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# =============================================================================
# COMPLETE END-TO-END ML PIPELINE
# =============================================================================

def complete_ml_pipeline():
    """
    End-to-end ML pipeline demonstrating all integrations.
    
    SCENARIO: Customer Churn Prediction
    - Load customer data
    - Engineer features
    - Train model
    - Predict on distributed data
    - Visualize results
    """
    
    spark = SparkSession.builder \
        .appName("Complete ML Pipeline") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=" * 70)
    print("COMPLETE ML PIPELINE: CUSTOMER CHURN PREDICTION")
    print("=" * 70)
    
    # ==========================================================================
    # STAGE 1: DATA LOADING & EXPLORATION
    # ==========================================================================
    
    print("\n📊 STAGE 1: Data Loading & Exploration")
    print("-" * 70)
    
    # Generate synthetic customer data
    df = spark.range(0, 10000).select(
        col("id").alias("customer_id"),
        (rand() * 12 + 1).cast("int").alias("tenure_months"),
        (rand() * 100 + 20).alias("monthly_charges"),
        (rand() * 500 + 100).alias("total_charges"),
        (rand() * 10).cast("int").alias("support_tickets"),
        (rand() > 0.7).cast("int").alias("churn")  # 30% churn rate
    )
    
    print(f"Dataset: {df.count()} customers")
    df.show(10)
    
    # Statistics
    print("\nChurn Rate:")
    df.groupBy("churn").count().show()
    
    # ==========================================================================
    # STAGE 2: FEATURE ENGINEERING (NumPy + Pandas UDFs)
    # ==========================================================================
    
    print("\n🔧 STAGE 2: Feature Engineering")
    print("-" * 70)
    
    @pandas_udf(DoubleType())
    def calculate_clv(tenure: pd.Series, monthly: pd.Series) -> pd.Series:
        """
        Customer Lifetime Value using NumPy.
        CLV = monthly_charges * tenure_months * retention_factor
        """
        retention_factor = 1.0 + np.log(tenure + 1) / 10
        return monthly * tenure * retention_factor
    
    @pandas_udf(DoubleType())
    def calculate_support_ratio(tickets: pd.Series, tenure: pd.Series) -> pd.Series:
        """Support tickets per month using NumPy"""
        return tickets / (tenure + 1)  # +1 to avoid division by zero
    
    @pandas_udf(DoubleType())
    def calculate_engagement_score(charges: pd.Series, tickets: pd.Series) -> pd.Series:
        """
        Engagement score combining charges and support.
        Higher charges + fewer tickets = higher engagement
        """
        normalized_charges = (charges - np.mean(charges)) / np.std(charges)
        normalized_tickets = (tickets - np.mean(tickets)) / np.std(tickets)
        return normalized_charges - normalized_tickets
    
    # Apply feature engineering
    df_features = df.withColumn(
        "clv", calculate_clv(col("tenure_months"), col("monthly_charges"))
    ).withColumn(
        "support_ratio", calculate_support_ratio(col("support_tickets"), col("tenure_months"))
    ).withColumn(
        "engagement_score", calculate_engagement_score(col("total_charges"), col("support_tickets"))
    )
    
    print("✅ Engineered features: CLV, Support Ratio, Engagement Score")
    df_features.select("customer_id", "clv", "support_ratio", "engagement_score").show(10)
    
    # ==========================================================================
    # STAGE 3: PREPROCESSING (Scikit-learn)
    # ==========================================================================
    
    print("\n⚙️  STAGE 3: Preprocessing with Scikit-learn")
    print("-" * 70)
    
    from sklearn.preprocessing import StandardScaler
    from sklearn.model_selection import train_test_split
    
    # Sample for training (simulate smaller dataset for training)
    train_sample = df_features.sample(0.8, seed=42).toPandas()
    
    feature_cols = ["tenure_months", "monthly_charges", "total_charges", 
                   "support_tickets", "clv", "support_ratio", "engagement_score"]
    
    X = train_sample[feature_cols].values
    y = train_sample["churn"].values
    
    # Split data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Standardize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    print(f"✅ Training set: {len(X_train)} samples")
    print(f"✅ Test set: {len(X_test)} samples")
    print(f"✅ Standardization: mean=0, std=1")
    
    # ==========================================================================
    # STAGE 4: MODEL TRAINING (Scikit-learn)
    # ==========================================================================
    
    print("\n🤖 STAGE 4: Model Training")
    print("-" * 70)
    
    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
    
    # Train multiple models
    models = {
        'Logistic Regression': LogisticRegression(random_state=42, max_iter=1000),
        'Random Forest': RandomForestClassifier(n_estimators=100, random_state=42),
        'Gradient Boosting': GradientBoostingClassifier(n_estimators=100, random_state=42)
    }
    
    results = {}
    
    for name, model in models.items():
        print(f"\nTraining {name}...")
        model.fit(X_train_scaled, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test_scaled)
        y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
        
        results[name] = {
            'model': model,
            'accuracy': accuracy_score(y_test, y_pred),
            'precision': precision_score(y_test, y_pred),
            'recall': recall_score(y_test, y_pred),
            'f1': f1_score(y_test, y_pred),
            'roc_auc': roc_auc_score(y_test, y_pred_proba)
        }
        
        print(f"  Accuracy: {results[name]['accuracy']:.4f}")
        print(f"  Precision: {results[name]['precision']:.4f}")
        print(f"  Recall: {results[name]['recall']:.4f}")
        print(f"  F1 Score: {results[name]['f1']:.4f}")
        print(f"  ROC AUC: {results[name]['roc_auc']:.4f}")
    
    # Select best model
    best_model_name = max(results, key=lambda k: results[k]['f1'])
    best_model = results[best_model_name]['model']
    
    print(f"\n✅ Best Model: {best_model_name}")
    
    # ==========================================================================
    # STAGE 5: DISTRIBUTED INFERENCE (Pandas UDFs)
    # ==========================================================================
    
    print("\n🚀 STAGE 5: Distributed Inference")
    print("-" * 70)
    
    @pandas_udf(DoubleType())
    def predict_churn(*features):
        """
        Apply trained model to distributed Spark data.
        """
        # Stack features
        X = np.column_stack([f.values for f in features])
        
        # Standardize
        X_scaled = scaler.transform(X)
        
        # Predict probability
        predictions = best_model.predict_proba(X_scaled)[:, 1]
        
        return pd.Series(predictions)
    
    # Apply to full dataset
    feature_cols_spark = [col(f) for f in feature_cols]
    df_predictions = df_features.withColumn(
        "churn_probability", 
        predict_churn(*feature_cols_spark)
    ).withColumn(
        "predicted_churn",
        when(col("churn_probability") > 0.5, 1).otherwise(0)
    )
    
    print("✅ Predictions completed on full dataset")
    df_predictions.select("customer_id", "churn", "churn_probability", "predicted_churn").show(10)
    
    # Calculate distributed metrics
    from pyspark.sql.functions import avg, sum as spark_sum
    
    metrics = df_predictions.select(
        (spark_sum(when(col("churn") == col("predicted_churn"), 1).otherwise(0)) / 
         spark_sum(col("churn") >= 0)).alias("accuracy"),
        avg(col("churn_probability")).alias("avg_churn_prob")
    ).first()
    
    print(f"\nDistributed Inference Metrics:")
    print(f"  Overall Accuracy: {metrics['accuracy']:.4f}")
    print(f"  Avg Churn Probability: {metrics['avg_churn_prob']:.4f}")
    
    # ==========================================================================
    # STAGE 6: VISUALIZATION & ANALYSIS (Matplotlib + Seaborn)
    # ==========================================================================
    
    print("\n📊 STAGE 6: Visualization & Analysis")
    print("-" * 70)
    
    # Sample for visualization
    viz_data = df_predictions.sample(0.1).toPandas()
    
    # Create comprehensive visualization
    fig = plt.figure(figsize=(16, 12))
    
    # 1. Model Performance Comparison
    plt.subplot(3, 3, 1)
    model_names = list(results.keys())
    f1_scores = [results[m]['f1'] for m in model_names]
    plt.barh(model_names, f1_scores, color='steelblue', alpha=0.8)
    plt.xlabel('F1 Score')
    plt.title('Model Performance Comparison')
    plt.grid(True, alpha=0.3, axis='x')
    
    # 2. Feature Importance (for best model)
    plt.subplot(3, 3, 2)
    if hasattr(best_model, 'feature_importances_'):
        importances = best_model.feature_importances_
        indices = np.argsort(importances)[::-1]
        plt.bar(range(len(importances)), importances[indices], alpha=0.8)
        plt.xticks(range(len(importances)), 
                  [feature_cols[i] for i in indices], rotation=45, ha='right')
        plt.ylabel('Importance')
        plt.title(f'Feature Importance ({best_model_name})')
        plt.grid(True, alpha=0.3, axis='y')
    
    # 3. Churn Probability Distribution
    plt.subplot(3, 3, 3)
    sns.histplot(data=viz_data, x='churn_probability', hue='churn', bins=30, alpha=0.6)
    plt.xlabel('Churn Probability')
    plt.ylabel('Count')
    plt.title('Churn Probability Distribution')
    
    # 4. CLV vs Churn
    plt.subplot(3, 3, 4)
    sns.boxplot(data=viz_data, x='churn', y='clv')
    plt.xlabel('Churn (0=No, 1=Yes)')
    plt.ylabel('Customer Lifetime Value')
    plt.title('CLV by Churn Status')
    
    # 5. Engagement Score vs Churn
    plt.subplot(3, 3, 5)
    sns.violinplot(data=viz_data, x='churn', y='engagement_score')
    plt.xlabel('Churn (0=No, 1=Yes)')
    plt.ylabel('Engagement Score')
    plt.title('Engagement Score by Churn Status')
    
    # 6. Support Ratio vs Churn Probability
    plt.subplot(3, 3, 6)
    plt.scatter(viz_data['support_ratio'], viz_data['churn_probability'], 
               c=viz_data['churn'], cmap='RdYlGn_r', alpha=0.5)
    plt.xlabel('Support Ratio')
    plt.ylabel('Churn Probability')
    plt.title('Support Ratio vs Churn Probability')
    plt.colorbar(label='Actual Churn')
    
    # 7. Tenure vs Monthly Charges (colored by churn)
    plt.subplot(3, 3, 7)
    scatter = plt.scatter(viz_data['tenure_months'], viz_data['monthly_charges'],
                         c=viz_data['churn_probability'], cmap='RdYlGn_r', alpha=0.6)
    plt.xlabel('Tenure (months)')
    plt.ylabel('Monthly Charges')
    plt.title('Tenure vs Charges (by Churn Prob)')
    plt.colorbar(scatter, label='Churn Probability')
    
    # 8. Confusion Matrix Heatmap
    plt.subplot(3, 3, 8)
    from sklearn.metrics import confusion_matrix
    cm = confusion_matrix(viz_data['churn'], viz_data['predicted_churn'])
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', square=True)
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.title('Confusion Matrix (Sample)')
    
    # 9. ROC Curves
    plt.subplot(3, 3, 9)
    from sklearn.metrics import roc_curve
    for name in results.keys():
        model_obj = results[name]['model']
        y_pred_proba = model_obj.predict_proba(X_test_scaled)[:, 1]
        fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
        auc = results[name]['roc_auc']
        plt.plot(fpr, tpr, label=f'{name} (AUC={auc:.3f})', linewidth=2)
    
    plt.plot([0, 1], [0, 1], 'k--', label='Random', linewidth=1)
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curves')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('/tmp/pyspark_complete_pipeline.png', dpi=150, bbox_inches='tight')
    print("\n✅ Saved comprehensive visualization: /tmp/pyspark_complete_pipeline.png")
    plt.close()
    
    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    
    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    print(f"✅ Dataset: {df.count()} customers")
    print(f"✅ Features Engineered: {len(feature_cols)}")
    print(f"✅ Models Trained: {len(models)}")
    print(f"✅ Best Model: {best_model_name}")
    print(f"✅ Best F1 Score: {results[best_model_name]['f1']:.4f}")
    print(f"✅ Predictions: Distributed across full dataset")
    print(f"✅ Visualizations: 9 comprehensive plots")
    print("=" * 70)
    
    print("\n🎯 THIS IS THE POWER OF PYSPARK + PYTHON ECOSYSTEM!")
    print("=" * 70)
    print("✅ NumPy: Fast numerical operations")
    print("✅ Pandas: Rich data manipulation via UDFs")
    print("✅ Scikit-learn: 100+ ML algorithms")
    print("✅ PyTorch: Deep learning (not used here, but available!)")
    print("✅ Matplotlib + Seaborn: Beautiful visualizations")
    print("✅ PySpark: Distributed processing at scale")
    print("=" * 70)
    
    spark.stop()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("COMPLETE ML PIPELINE")
    print("Python Ecosystem + PySpark Integration")
    print("=" * 70)
    
    complete_ml_pipeline()
    
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE!")
    print("=" * 70)
    print("\nYou've seen a complete end-to-end ML pipeline using:")
    print("- PySpark for distributed data processing")
    print("- NumPy for numerical operations")
    print("- Pandas for data manipulation")
    print("- Scikit-learn for machine learning")
    print("- Matplotlib + Seaborn for visualization")
    print("\nThis is why PySpark is so powerful - access to the")
    print("entire Python data science ecosystem at scale!")
    print("=" * 70 + "\n")
