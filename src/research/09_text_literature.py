"""
Example 9: Text Analytics - Literature Trends
==============================================

Analyze publication trends in PubMed research database

Real-world application:
- PubMed (35M+ citations)
- Google Scholar analysis
- Citation trend detection
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


def main():
    spark = SparkSession.builder.appName("TextLiterature").getOrCreate()
    
    try:
        print("=" * 70)
        print("TEXT ANALYTICS: Publication Trends")
        print("=" * 70)
        
        pubmed_data = [
            ("CRISPR", 2015, 1200),
            ("CRISPR", 2020, 4500),
            ("COVID-19", 2020, 18000),
            ("COVID-19", 2021, 25000),
        ]
        
        df = spark.createDataFrame(pubmed_data, ["keyword", "year", "publications"])
        
        print("\n📚 Research Publication Trends:")
        df.show()
        
        total = df.agg(spark_sum("publications").alias("total")).collect()[0][0]
        print(f"Total publications analyzed: {total:,}")
        print("✅ Real Use: PubMed (35M+ citations)")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
