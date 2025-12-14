"""
Example 10: Economics - Stock Market Analysis
==============================================

Analyze market trends from millions of trades

Real-world application:
- NYSE/NASDAQ (billions of trades)
- High-frequency trading analytics
- Market surveillance
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max as spark_max, min as spark_min


def main():
    spark = SparkSession.builder.appName("EconomicsMarket").getOrCreate()
    
    try:
        print("=" * 70)
        print("ECONOMICS: Stock Market Analysis")
        print("=" * 70)
        
        stock_data = [
            ("AAPL", 150.0, 155.0, 148.0, 152.5),
            ("GOOGL", 2800.0, 2850.0, 2780.0, 2820.0),
            ("MSFT", 300.0, 310.0, 298.0, 308.0),
        ]
        
        df = spark.createDataFrame(stock_data, ["symbol", "open", "high", "low", "close"])
        
        summary = df.agg(
            avg("close").alias("avg_close"),
            spark_max("high").alias("max_high"),
            spark_min("low").alias("min_low")
        )
        
        print("\n📈 Market Summary:")
        df.show()
        summary.show()
        print("✅ Real Use: NYSE/NASDAQ (billions of trades)")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
