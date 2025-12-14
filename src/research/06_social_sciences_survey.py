"""
Example 6: Social Sciences - Survey Analysis
=============================================

Large-scale demographic survey analysis

Real-world application:
- US Census
- World Bank surveys
- Pew Research
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


def main():
    spark = SparkSession.builder.appName("SocialSurvey").getOrCreate()
    
    try:
        print("=" * 70)
        print("SOCIAL SCIENCES: Survey Analysis")
        print("=" * 70)
        
        survey_data = [
            ("18-25", 45000, 1200000),
            ("26-35", 62000, 1500000),
            ("36-50", 75000, 2000000),
            ("51-65", 68000, 1800000),
            ("65+", 42000, 1000000),
        ]
        
        df = spark.createDataFrame(survey_data, ["age_group", "avg_income", "population"])
        
        print("\n📊 Income by Age Group:")
        df.show()
        
        total_pop = df.agg(spark_sum("population").alias("total")).collect()[0][0]
        print(f"Total surveyed: {total_pop:,} individuals")
        print("✅ Real Use: US Census, World Bank surveys, Pew Research")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
