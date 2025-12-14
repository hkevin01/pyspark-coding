"""
ADVANCED 04: Production Patterns
Error handling, logging, testing, monitoring for production Spark jobs.
TIME: 45 minutes | DIFFICULTY: ⭐⭐⭐⭐⭐ (5/5)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
import logging

def create_spark_session():
    return (
        SparkSession.builder
        .appName("Advanced04_Production")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

def setup_logging():
    """Configure production logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def pattern_1_error_handling():
    """Robust error handling."""
    logger = setup_logging()
    spark = create_spark_session()
    
    try:
        logger.info("Starting ETL job...")
        
        # Sample data
        df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
        
        # Add audit columns
        df_audited = df.withColumn("processed_at", current_timestamp()) \
                       .withColumn("job_id", lit("job_001"))
        
        logger.info(f"Processed {df_audited.count()} records")
        df_audited.show()
        
    except Exception as e:
        logger.error(f"Job failed: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Job completed")

def main():
    print("\n🏭" * 40)
    print("ADVANCED LESSON 4: PRODUCTION PATTERNS")
    print("🏭" * 40)
    
    pattern_1_error_handling()
    
    print("\n🎉 PRODUCTION PATTERNS COMPLETE!")
    print("\n➡️  NEXT: Try advanced/05_capstone_project.py")

if __name__ == "__main__":
    main()
