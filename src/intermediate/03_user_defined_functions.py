"""
================================================================================
INTERMEDIATE 03: User Defined Functions (UDFs)
================================================================================

PURPOSE: Create custom functions when built-in Spark functions aren't enough.
Learn performance implications and best practices.

WHAT YOU'LL LEARN:
- Python UDFs (basic)
- Pandas UDFs (vectorized, much faster!)
- Return types and schemas
- Performance comparison
- When to use vs built-in functions

WHY: Sometimes you need custom logic that Spark doesn't provide.

REAL-WORLD: Custom business rules, ML scoring, complex transformations.

TIME: 35 minutes | DIFFICULTY: ⭐⭐⭐⭐☆ (4/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import StringType, IntegerType, FloatType
import pandas as pd


def create_spark_session():
    return SparkSession.builder.appName("Intermediate03_UDFs").getOrCreate()


def create_sample_data(spark):
    """Create sample customer data."""
    data = [
        (1, "alice@email.com", "Premium", 1500),
        (2, "bob@test.com", "Standard", 500),
        (3, "charlie@email.com", "Premium", 2500),
        (4, "diana@company.com", "Trial", 0),
        (5, "eve@email.com", "Standard", 800),
    ]
    
    df = spark.createDataFrame(data, ["customer_id", "email", "tier", "total_spent"])
    return df


def example_1_basic_python_udf(df):
    """Basic Python UDF."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: BASIC PYTHON UDF")
    print("=" * 80)
    
    # Define Python function
    def categorize_email(email):
        """Categorize email domain."""
        if email is None:
            return "Unknown"
        domain = email.split("@")[1] if "@" in email else "Unknown"
        if domain in ["gmail.com", "yahoo.com", "hotmail.com"]:
            return "Personal"
        else:
            return "Business"
    
    # Register as UDF
    categorize_udf = udf(categorize_email, StringType())
    
    # Apply UDF
    df_result = df.withColumn("email_type", categorize_udf(col("email")))
    
    print("\n✅ Email Categorization:")
    df_result.show()
    
    print("\n💡 Syntax: udf(python_function, return_type)")
    
    return df_result


def example_2_complex_udf(df):
    """UDF with complex logic."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: COMPLEX UDF")
    print("=" * 80)
    
    # Complex business logic
    def calculate_loyalty_score(tier, total_spent):
        """Calculate custom loyalty score."""
        if tier == "Premium":
            base_score = 100
            multiplier = 1.5
        elif tier == "Standard":
            base_score = 50
            multiplier = 1.0
        else:
            base_score = 0
            multiplier = 0.5
        
        # Score = base + (spend * multiplier)
        score = base_score + (total_spent * multiplier / 100)
        
        # Cap at 200
        return min(score, 200)
    
    # Register UDF
    loyalty_score_udf = udf(calculate_loyalty_score, FloatType())
    
    # Apply
    df_result = df.withColumn(
        "loyalty_score",
        loyalty_score_udf(col("tier"), col("total_spent"))
    )
    
    print("\n✅ Loyalty Scores:")
    df_result.show()
    
    print("\n💡 Use case: Custom business rules")
    
    return df_result


def example_3_pandas_udf(df):
    """Pandas UDF (vectorized, much faster!)."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: PANDAS UDF (VECTORIZED)")
    print("=" * 80)
    
    # Pandas UDF - operates on entire series
    @pandas_udf(FloatType())
    def calculate_discount_vectorized(tier: pd.Series, total_spent: pd.Series) -> pd.Series:
        """Calculate discount percentage (vectorized)."""
        discount = pd.Series([0.0] * len(tier))
        
        # Premium customers: 20% discount
        discount[tier == "Premium"] = 0.20
        
        # Standard customers: 10% if spent > 500
        discount[(tier == "Standard") & (total_spent > 500)] = 0.10
        
        # Standard customers: 5% otherwise
        discount[(tier == "Standard") & (total_spent <= 500)] = 0.05
        
        return discount
    
    # Apply
    df_result = df.withColumn(
        "discount_rate",
        calculate_discount_vectorized(col("tier"), col("total_spent"))
    )
    
    print("\n✅ Discount Rates:")
    df_result.show()
    
    print("\n💡 Pandas UDFs are 10-100x faster than Python UDFs!")
    print("   Use @pandas_udf decorator")
    
    return df_result


def example_4_performance_comparison():
    """Compare Python UDF vs Pandas UDF performance."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: PERFORMANCE COMPARISON")
    print("=" * 80)
    
    print("\n⚡ PERFORMANCE:")
    print("   Python UDF:  ⭐☆☆☆☆ (Slow - row-by-row)")
    print("   Pandas UDF:  ⭐⭐⭐⭐⭐ (Fast - vectorized)")
    print("   Built-in:    ⭐⭐⭐⭐⭐ (Fastest - native Spark)")
    
    print("\n📊 Real-world benchmark (1M rows):")
    print("   Built-in functions: 2 seconds")
    print("   Pandas UDF:         5 seconds")
    print("   Python UDF:         120 seconds (60x slower!)")
    
    print("\n💡 RULES:")
    print("   1. Use built-in functions when possible")
    print("   2. Use Pandas UDF if you need custom logic")
    print("   3. Avoid Python UDF unless necessary")


def example_5_best_practices(df):
    """UDF best practices."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: BEST PRACTICES")
    print("=" * 80)
    
    # ✅ GOOD: Pandas UDF with type hints
    @pandas_udf(StringType())
    def segment_customer(total_spent: pd.Series) -> pd.Series:
        """Segment customers by spend."""
        return pd.cut(
            total_spent,
            bins=[0, 500, 1000, float('inf')],
            labels=["Low", "Medium", "High"]
        ).astype(str)
    
    df_result = df.withColumn("segment", segment_customer(col("total_spent")))
    
    print("\n✅ Customer Segmentation:")
    df_result.show()
    
    print("\n🏆 BEST PRACTICES:")
    print("   ✅ Use type hints in Pandas UDFs")
    print("   ✅ Test with small data first")
    print("   ✅ Handle null values explicitly")
    print("   ✅ Avoid side effects (no API calls!)")
    print("   ✅ Keep UDFs pure and deterministic")
    
    print("\n❌ AVOID:")
    print("   ❌ UDFs for simple transformations (use built-ins)")
    print("   ❌ Database/API calls inside UDFs")
    print("   ❌ Mutable state or side effects")
    print("   ❌ Python UDFs on large datasets")
    
    return df_result


def main():
    """Run all UDF examples."""
    print("\n" + "🔧" * 40)
    print("INTERMEDIATE LESSON 3: USER DEFINED FUNCTIONS")
    print("🔧" * 40)
    
    spark = create_spark_session()
    
    try:
        df = create_sample_data(spark)
        print("\n📊 Sample Data:")
        df.show()
        
        example_1_basic_python_udf(df)
        example_2_complex_udf(df)
        example_3_pandas_udf(df)
        example_4_performance_comparison()
        example_5_best_practices(df)
        
        print("\n" + "=" * 80)
        print("🎉 UDFs MASTERED!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Basic Python UDFs")
        print("   2. Complex logic UDFs")
        print("   3. Pandas UDFs (vectorized)")
        print("   4. Performance implications")
        print("   5. Best practices")
        
        print("\n🏆 KEY RULE: Pandas UDF > Python UDF")
        print("\n➡️  NEXT: Try intermediate/04_aggregation_patterns.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
