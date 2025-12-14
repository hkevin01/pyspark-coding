"""
Example 4: Climate Science - Global Warming Analysis
=====================================================

Analyze global temperature trends over decades

Real-world application:
- NOAA/NASA climate records
- IPCC reports
- Climate change monitoring
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max as spark_max


def main():
    spark = SparkSession.builder.appName("ClimateWarming").getOrCreate()
    
    try:
        print("=" * 70)
        print("CLIMATE SCIENCE: Global Warming Analysis")
        print("=" * 70)
        
        temp_data = [
            (1980, 0.26), (1990, 0.45), (2000, 0.42),
            (2010, 0.72), (2020, 1.02), (2023, 1.17)
        ]
        
        df = spark.createDataFrame(temp_data, ["year", "temp_anomaly"])
        
        result = df.agg(
            spark_max("temp_anomaly").alias("max_warming"),
            avg("temp_anomaly").alias("avg_warming")
        )
        
        print("\n🌡️  Temperature Analysis:")
        result.show()
        df.show()
        print("✅ Real Use: NOAA/NASA climate records, IPCC reports")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
