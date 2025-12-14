"""
Example 7: Medical Research - Clinical Trial Analysis
======================================================

Measure drug efficacy in clinical trials

Real-world application:
- FDA drug approvals
- NIH All of Us program
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("MedicalTrial").getOrCreate()
    
    try:
        print("=" * 70)
        print("MEDICAL RESEARCH: Clinical Trial Results")
        print("=" * 70)
        
        trial_data = [
            ("Treatment", 5000, 3800),
            ("Placebo", 5000, 2500),
        ]
        
        df = spark.createDataFrame(trial_data, ["group", "participants", "improved"])
        result = df.withColumn("success_rate", (col("improved") / col("participants")) * 100)
        
        print("\n💊 Clinical Trial Efficacy:")
        result.show()
        print("✅ Real Use: FDA drug approvals, NIH All of Us program")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
