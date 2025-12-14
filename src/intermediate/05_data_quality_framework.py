"""
================================================================================
INTERMEDIATE 05: Data Quality Framework
================================================================================

PURPOSE: Build a production-ready data quality validation framework.

WHAT YOU'LL LEARN:
- Schema validation
- Data quality checks
- Automated testing
- Quality metrics
- Reporting framework

WHY: Data quality is critical for production pipelines.

REAL-WORLD: Validate data before loading to warehouse, catch issues early.

TIME: 40 minutes | DIFFICULTY: ⭐⭐⭐⭐☆ (4/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


def create_spark_session():
    return SparkSession.builder.appName("Intermediate05_DataQuality").getOrCreate()


def create_test_data(spark):
    """Create test data with quality issues."""
    data = [
        (1, "Alice", "alice@email.com", 25, 1500.00),
        (2, "Bob", None, 30, 2000.00),  # Missing email
        (3, "Charlie", "invalid-email", -5, 500.00),  # Invalid email, negative age
        (1, "Alice", "alice@email.com", 25, 1500.00),  # Duplicate
        (4, None, "diana@email.com", 150, None),  # Missing name, invalid age, null revenue
        (5, "Eve", "eve@email.com", 28, 1000.00),
    ]
    
    df = spark.createDataFrame(
        data,
        ["customer_id", "name", "email", "age", "total_revenue"]
    )
    
    return df


def check_1_schema_validation(df):
    """Validate schema matches expectations."""
    print("\n" + "=" * 80)
    print("CHECK 1: SCHEMA VALIDATION")
    print("=" * 80)
    
    # Expected schema
    expected_schema = StructType([
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("total_revenue", DoubleType(), True),
    ])
    
    # Check schema
    actual_schema = df.schema
    schema_match = actual_schema == expected_schema
    
    print(f"\n✅ Schema Match: {schema_match}")
    print("\n📋 Actual Schema:")
    df.printSchema()
    
    return schema_match


def check_2_null_checks(df):
    """Check for null values."""
    print("\n" + "=" * 80)
    print("CHECK 2: NULL VALUE CHECKS")
    print("=" * 80)
    
    # Count nulls per column
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ])
    
    print("\n📊 Null counts per column:")
    null_counts.show()
    
    # Calculate null percentage
    total_rows = df.count()
    null_pct = df.select([
        (count(when(col(c).isNull(), c)) / total_rows * 100).alias(c) 
        for c in df.columns
    ])
    
    print(f"\n📊 Null percentage (total rows: {total_rows}):")
    null_pct.show()
    
    # Quality score
    total_nulls = null_counts.collect()[0].asDict()
    quality_score = 100 - (sum(total_nulls.values()) / (len(df.columns) * total_rows) * 100)
    
    print(f"\n✅ Data Completeness Score: {quality_score:.1f}%")
    
    return quality_score


def check_3_duplicate_detection(df):
    """Detect duplicate rows."""
    print("\n" + "=" * 80)
    print("CHECK 3: DUPLICATE DETECTION")
    print("=" * 80)
    
    total_rows = df.count()
    unique_rows = df.dropDuplicates().count()
    duplicate_count = total_rows - unique_rows
    
    print(f"\n📊 Duplicate Analysis:")
    print(f"   Total rows:     {total_rows}")
    print(f"   Unique rows:    {unique_rows}")
    print(f"   Duplicates:     {duplicate_count}")
    print(f"   Duplicate rate: {duplicate_count/total_rows*100:.1f}%")
    
    if duplicate_count > 0:
        print("\n⚠️  Duplicates found! Example:")
        df.groupBy(df.columns).count().filter(col("count") > 1).show()
    else:
        print("\n✅ No duplicates found!")
    
    return duplicate_count == 0


def check_4_business_rules(df):
    """Validate business rules."""
    print("\n" + "=" * 80)
    print("CHECK 4: BUSINESS RULE VALIDATION")
    print("=" * 80)
    
    # Rule 1: Age between 0 and 120
    invalid_age = df.filter((col("age") < 0) | (col("age") > 120))
    age_violations = invalid_age.count()
    
    # Rule 2: Email must contain @
    invalid_email = df.filter(
        col("email").isNotNull() & ~col("email").contains("@")
    )
    email_violations = invalid_email.count()
    
    # Rule 3: Revenue must be non-negative
    invalid_revenue = df.filter(col("total_revenue") < 0)
    revenue_violations = invalid_revenue.count()
    
    print("\n📊 Business Rule Violations:")
    print(f"   Invalid age:     {age_violations}")
    print(f"   Invalid email:   {email_violations}")
    print(f"   Invalid revenue: {revenue_violations}")
    
    if age_violations > 0:
        print("\n⚠️  Invalid age examples:")
        invalid_age.show()
    
    if email_violations > 0:
        print("\n⚠️  Invalid email examples:")
        invalid_email.show()
    
    total_violations = age_violations + email_violations + revenue_violations
    rule_compliance = 100 - (total_violations / df.count() * 100)
    
    print(f"\n✅ Business Rule Compliance: {rule_compliance:.1f}%")
    
    return rule_compliance


def check_5_generate_report(df):
    """Generate comprehensive quality report."""
    print("\n" + "=" * 80)
    print("CHECK 5: COMPREHENSIVE QUALITY REPORT")
    print("=" * 80)
    
    total_rows = df.count()
    
    # Overall metrics
    print("\n📊 DATASET SUMMARY:")
    print(f"   Total rows: {total_rows}")
    print(f"   Total columns: {len(df.columns)}")
    
    # Column-by-column analysis
    print("\n�� COLUMN QUALITY ANALYSIS:")
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        distinct_count = df.select(column).distinct().count()
        null_pct = (null_count / total_rows * 100)
        
        status = "✅" if null_pct == 0 else "⚠️ "
        print(f"   {status} {column:20s}: {null_pct:5.1f}% nulls, {distinct_count} distinct values")
    
    # Final grade
    completeness = 100 - (df.select([count(when(col(c).isNull(), c)) for c in df.columns])
                         .collect()[0].asDict().values())
    
    print("\n" + "=" * 80)
    print("📋 FINAL DATA QUALITY GRADE:")
    print("=" * 80)
    
    grade = "A" if completeness > 95 else "B" if completeness > 85 else "C" if completeness > 70 else "F"
    print(f"\n   Overall Grade: {grade}")
    print("\n   Recommendations:")
    if grade in ["C", "F"]:
        print("   ⚠️  Data quality issues detected!")
        print("   1. Review and fix null values")
        print("   2. Validate business rules")
        print("   3. Remove duplicates")
    else:
        print("   ✅ Data quality is acceptable")


def main():
    """Run data quality framework."""
    print("\n" + "🔍" * 40)
    print("INTERMEDIATE LESSON 5: DATA QUALITY FRAMEWORK")
    print("🔍" * 40)
    
    spark = create_spark_session()
    
    try:
        df = create_test_data(spark)
        print("\n📊 Test Data:")
        df.show(truncate=False)
        
        # Run all checks
        check_1_schema_validation(df)
        check_2_null_checks(df)
        check_3_duplicate_detection(df)
        check_4_business_rules(df)
        check_5_generate_report(df)
        
        print("\n" + "=" * 80)
        print("🎉 DATA QUALITY FRAMEWORK COMPLETE!")
        print("=" * 80)
        print("\n✅ You built a production framework:")
        print("   1. Schema validation")
        print("   2. Null value detection")
        print("   3. Duplicate detection")
        print("   4. Business rule validation")
        print("   5. Quality reporting")
        
        print("\n🏆 CONGRATULATIONS - INTERMEDIATE TRACK COMPLETE!")
        print("\n✨ You now know:")
        print("   ✅ Advanced transformations")
        print("   ✅ Window functions")
        print("   ✅ UDFs (Python + Pandas)")
        print("   ✅ Complex aggregations")
        print("   ✅ Data quality frameworks")
        
        print("\n➡️  NEXT LEVEL: Try advanced/01_performance_optimization.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
