"""
ADVANCED 03: ML Pipelines with MLlib
Build end-to-end ML pipelines - feature engineering, training, evaluation.
TIME: 45 minutes | DIFFICULTY: ⭐⭐⭐⭐⭐ (5/5)
"""

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def create_spark_session():
    return SparkSession.builder.appName("Advanced03_ML").getOrCreate()

def example_1_ml_pipeline():
    """Complete ML pipeline."""
    spark = create_spark_session()
    
    # Sample data
    data = [(0, 1.0, 2.0, 0.0), (1, 3.0, 4.0, 1.0), (2, 5.0, 6.0, 1.0)]
    df = spark.createDataFrame(data, ["id", "feature1", "feature2", "label"])
    
    # Build pipeline
    assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    lr = LogisticRegression(featuresCol="scaled_features", labelCol="label")
    
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    # Train
    model = pipeline.fit(df)
    predictions = model.transform(df)
    
    print("\n✅ ML Pipeline Predictions:")
    predictions.select("id", "label", "prediction").show()
    
    spark.stop()

def main():
    print("\n🤖" * 40)
    print("ADVANCED LESSON 3: ML PIPELINES")
    print("🤖" * 40)
    
    example_1_ml_pipeline()
    
    print("\n🎉 ML PIPELINES COMPLETE!")
    print("\n➡️  NEXT: Try advanced/04_production_patterns.py")

if __name__ == "__main__":
    main()
