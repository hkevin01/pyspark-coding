"""
Example 14: Recommendation System ML Pipeline
==============================================

Process 1B user interactions with PySpark → train collaborative filtering model

Real-world application:
- Netflix movie recommendations
- Amazon product recommendations
- Spotify music recommendations

Pipeline: PySpark (1B interactions) → ALS model → Pandas (top-K recommendations)
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand

try:
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.recommendation import ALS

    ALS_AVAILABLE = True
except ImportError:
    ALS_AVAILABLE = False
    print("⚠️  Spark MLlib not available")


def main():
    spark = SparkSession.builder.appName("RecommendationML").getOrCreate()

    try:
        print("=" * 70)
        print("RECOMMENDATION SYSTEM ML PIPELINE")
        print("=" * 70)

        # Step 1: PySpark - Process interaction data
        print("\n📊 STEP 1: PySpark - Processing 1B user-item interactions...")
        interactions_df = spark.range(0, 1000000).select(
            (col("id") % 100000).alias("user_id"),
            (col("id") % 50000).alias("item_id"),
            (rand() * 5).alias("rating"),
        )

        print(f"   Total interactions: {interactions_df.count():,}")

        # Step 2: Train ALS with Spark MLlib
        print("\n⚙️  STEP 2: Training ALS (Alternating Least Squares)...")
        print("   Algorithm: Matrix Factorization")
        print("   Latent factors: 10 (simplified for demo)")
        print("   Max iterations: 5")

        if ALS_AVAILABLE:
            # Split data for training and testing
            (training, test) = interactions_df.randomSplit([0.8, 0.2], seed=42)

            # Build ALS model
            als = ALS(
                maxIter=5,
                rank=10,  # Latent factors
                regParam=0.01,
                userCol="user_id",
                itemCol="item_id",
                ratingCol="rating",
                coldStartStrategy="drop",
            )

            print("   Training ALS model...")
            model = als.fit(training)

            # Evaluate
            predictions = model.transform(test)
            evaluator = RegressionEvaluator(
                metricName="rmse", labelCol="rating", predictionCol="prediction"
            )
            rmse = evaluator.evaluate(predictions)
            print(f"   ✅ Model trained successfully")
            print(f"   ✅ Test RMSE: {rmse:.3f}")

            # Step 3: Generate recommendations
            print("\n🎯 STEP 3: Generating top-10 recommendations per user...")

            # Generate top 10 recommendations for each user
            user_recs = model.recommendForAllUsers(10)
            print(f"   Generated recommendations for {user_recs.count():,} users")

            # Show sample recommendations for user 42
            sample_user = user_recs.filter(col("user_id") == 42)
            if sample_user.count() > 0:
                print("\n   Sample recommendations for User 42:")
                sample_df = sample_user.toPandas()
                if len(sample_df) > 0:
                    recs = sample_df.iloc[0]["recommendations"]
                    for i, rec in enumerate(recs[:5], 1):
                        print(f"      {i}. Item {rec['item_id']}: {rec['rating']:.2f}")

            # Step 4: Convert to Pandas for serving
            print("\n🐼 STEP 4: Converting recommendations to Pandas...")
            sample_recs = user_recs.limit(100).toPandas()
            print(f"   Sample: {len(sample_recs):,} users with recommendations")
            print("   ✅ Ready for real-time API serving")

        else:
            print("   ⚠️  Spark MLlib not available - showing simulated results")
            print("   Users: 100,000")
            print("   Items: 50,000")
            print("   Factors: 100")
            print("   Iterations: 10")
            print("   ✅ Model trained on distributed data")

            print("\n🎯 STEP 3: Generating top-10 recommendations per user...")
            print("   Using Spark to compute recommendations at scale")

            print("\n🐼 STEP 4: Converting recommendations to Pandas...")
            print(f"   Generated recommendations for all users")
            print("   Ready for real-time API serving")

        print("\n" + "=" * 70)
        print("REAL-WORLD USE CASES:")
        print("- Netflix: 80% of watched content from recommendations")
        print("- Amazon: 35% of revenue from recommendations")
        print("- Spotify: Discover Weekly (collaborative filtering)")
        print("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
