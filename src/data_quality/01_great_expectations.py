"""
================================================================================
Great Expectations Data Quality with PySpark
================================================================================

PURPOSE:
--------
Implement comprehensive data quality validation using Great Expectations
to catch data issues before they impact downstream systems.

WHAT GREAT EXPECTATIONS DOES:
------------------------------
- Define expectations (rules) for data quality
- Validate data against expectations
- Generate data documentation
- Alert on quality issues
- Track data quality metrics over time

WHY DATA QUALITY MATTERS:
-------------------------
- Bad data costs US companies $3.1 trillion annually
- 1 in 3 business decisions based on poor data
- Data pipeline failures cost $1000s per hour
- Regulatory compliance requires data validation

REAL-WORLD USE CASES:
- Banking: Validate transaction amounts (no negatives)
- Healthcare: Ensure patient IDs are unique
- E-commerce: Check order totals match line items
- Insurance: Validate claim amounts within limits

KEY VALIDATIONS:
----------------
1. Column existence
2. Data types
3. Null checks
4. Uniqueness constraints
5. Value ranges
6. Custom business rules

================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# Note: Requires great_expectations package
# pip install great-expectations

def create_spark():
    """Create Spark session for data quality checks."""
    return SparkSession.builder \
        .appName("GreatExpectations_DataQuality") \
        .getOrCreate()

def example_1_basic_validations(spark):
    """
    Example 1: Basic data quality validations.
    
    EXPECTATIONS:
    - Customer ID must be unique
    - Email must not be null
    - Age must be between 18 and 120
    - Signup date must be valid date
    """
    print("\n" + "=" * 80)
    print("EXAMPLE 1: DATA QUALITY VALIDATIONS")
    print("=" * 80)
    
    # Sample data with quality issues
    customers = spark.createDataFrame([
        (1, "alice@email.com", 25, "2023-01-15"),
        (2, "bob@email.com", 30, "2023-02-20"),
        (3, None, 150, "2023-03-10"),  # Missing email, age out of range
        (4, "diana@email.com", 22, "invalid-date"),  # Invalid date
        (1, "duplicate@email.com", 28, "2023-04-05"),  # Duplicate ID
    ], ["customer_id", "email", "age", "signup_date"])
    
    # Manual validation examples (Great Expectations would automate this)
    print("\n🔍 Checking data quality...")
    
    # Check 1: Unique IDs
    duplicate_ids = customers.groupBy("customer_id").count().filter(col("count") > 1)
    print(f"\n❌ Duplicate customer_ids found: {duplicate_ids.count()}")
    if duplicate_ids.count() > 0:
        duplicate_ids.show()
    
    # Check 2: Non-null emails
    null_emails = customers.filter(col("email").isNull())
    print(f"❌ Null emails found: {null_emails.count()}")
    
    # Check 3: Age range
    invalid_age = customers.filter((col("age") < 18) | (col("age") > 120))
    print(f"❌ Ages out of range [18-120]: {invalid_age.count()}")
    
    print("\n💡 Great Expectations would:")
    print("   • Automatically run these checks")
    print("   • Generate HTML reports")
    print("   • Send alerts on failures")
    print("   • Track quality metrics over time")
    
    return customers

def main():
    """Main execution."""
    print("\n" + "✔️" * 40)
    print("GREAT EXPECTATIONS DATA QUALITY")
    print("✔️" * 40)
    
    spark = create_spark()
    example_1_basic_validations(spark)
    
    print("\n✅ Data quality examples complete!")
    spark.stop()

if __name__ == "__main__":
    main()
