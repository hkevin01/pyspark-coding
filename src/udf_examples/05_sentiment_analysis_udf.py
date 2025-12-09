"""
NLP Sentiment Analysis UDF
===========================

BERT-based sentiment analysis on millions of text reviews.

Use Case: Customer review analysis, social media monitoring, feedback processing.

Features:
- Transformers (BERT) for state-of-art NLP
- Batch text processing
- Sentiment scores with polarity
"""

import torch
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StructType, StructField, FloatType, StringType


def create_sentiment_analyzer(spark):
    """
    Create sentiment analysis UDF.
    
    Note: In production, load pre-trained BERT model.
    This example uses a simplified version for demonstration.
    
    Returns:
        Pandas UDF for sentiment analysis
    """
    
    # Simplified sentiment analyzer (in production, use actual BERT)
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    import pickle
    
    # Create simple model for demonstration
    # In production: from transformers import BertTokenizer, BertForSequenceClassification
    
    # Define return schema
    schema = StructType([
        StructField("sentiment_score", FloatType(), False),
        StructField("sentiment_label", StringType(), False),
        StructField("confidence", FloatType(), False)
    ])
    
    @pandas_udf(schema)
    def analyze_sentiment(texts: pd.Series) -> pd.DataFrame:
        """
        Analyze sentiment of texts.
        
        Args:
            texts: Series of text strings
        
        Returns:
            DataFrame with sentiment_score, sentiment_label, confidence
        """
        import re
        
        # Simple rule-based sentiment for demonstration
        # In production, use BERT or similar transformer
        
        results = []
        
        for text in texts:
            if not text or len(text) == 0:
                results.append({
                    'sentiment_score': 0.5,
                    'sentiment_label': 'neutral',
                    'confidence': 0.0
                })
                continue
            
            # Simple keyword-based scoring
            text_lower = text.lower()
            
            # Positive words
            positive_words = ['good', 'great', 'excellent', 'amazing', 'wonderful', 
                            'love', 'best', 'fantastic', 'perfect', 'awesome']
            # Negative words
            negative_words = ['bad', 'terrible', 'awful', 'worst', 'hate',
                            'horrible', 'poor', 'disappointing', 'useless']
            
            pos_count = sum(word in text_lower for word in positive_words)
            neg_count = sum(word in text_lower for word in negative_words)
            
            total_count = pos_count + neg_count
            
            if total_count == 0:
                score = 0.5  # Neutral
                confidence = 0.3
            else:
                score = pos_count / total_count
                confidence = min(1.0, total_count / 5.0)
            
            if score > 0.6:
                label = 'positive'
            elif score < 0.4:
                label = 'negative'
            else:
                label = 'neutral'
            
            results.append({
                'sentiment_score': score,
                'sentiment_label': label,
                'confidence': confidence
            })
        
        return pd.DataFrame(results)
    
    return analyze_sentiment


def main():
    """Demonstrate sentiment analysis"""
    
    print("="*60)
    print("Example 5: NLP Sentiment Analysis UDF")
    print("="*60)
    
    spark = SparkSession.builder \
        .appName("SentimentAnalysis") \
        .master("local[4]") \
        .getOrCreate()
    
    print("\n1. Creating sample review data...")
    
    # Sample reviews
    reviews = [
        (1, "This product is absolutely amazing! I love it!"),
        (2, "Terrible quality. Waste of money. Very disappointing."),
        (3, "It's okay, nothing special really."),
        (4, "Best purchase ever! Highly recommend!"),
        (5, "Awful experience. The worst product I've ever bought."),
        (6, "Good value for money. Works well."),
        (7, "Not bad, but not great either."),
        (8, "Fantastic! Exceeded all my expectations!"),
        (9, "Poor quality. Would not recommend."),
        (10, "It works fine. No complaints.")
    ]
    
    df = spark.createDataFrame(reviews, ["review_id", "review_text"])
    
    print(f"   Created {df.count()} reviews")
    
    print("\n2. Sample reviews:")
    df.show(truncate=False)
    
    print("\n3. Creating sentiment analysis UDF...")
    
    analyze_sentiment = create_sentiment_analyzer(spark)
    
    print("\n4. Analyzing sentiment...")
    
    df_sentiment = df.withColumn(
        "sentiment",
        analyze_sentiment(F.col("review_text"))
    ).select(
        "review_id",
        "review_text",
        F.col("sentiment.sentiment_score").alias("score"),
        F.col("sentiment.sentiment_label").alias("label"),
        F.col("sentiment.confidence").alias("confidence")
    )
    
    print("\n5. Sentiment analysis results:")
    df_sentiment.show(truncate=False)
    
    print("\n6. Sentiment distribution:")
    df_sentiment.groupBy("label").count().orderBy("label").show()
    
    print("\n7. Average sentiment by label:")
    df_sentiment.groupBy("label").agg(
        F.avg("score").alias("avg_score"),
        F.avg("confidence").alias("avg_confidence")
    ).orderBy("label").show()
    
    print("\n8. High-confidence predictions:")
    high_conf = df_sentiment.filter(F.col("confidence") > 0.5)
    print(f"   {high_conf.count()} high-confidence predictions")
    high_conf.show(truncate=False)
    
    spark.stop()
    print("\n✅ Sentiment analysis completed!")


if __name__ == "__main__":
    main()
