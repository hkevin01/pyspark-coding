"""
================================================================================
INTERMEDIATE 01: Advanced Transformations
================================================================================

PURPOSE: Master complex column operations - withColumn chains, when/otherwise,
array/struct operations, and data type conversions.

WHAT YOU'LL LEARN:
- Complex withColumn operations
- Conditional logic (when/otherwise)
- Working with arrays and structs
- Type casting and conversions
- Column renaming patterns

WHY: Production pipelines need complex transformations beyond simple filters.

REAL-WORLD: Transform raw event logs into clean analytics-ready format.

TIME: 35 minutes | DIFFICULTY: ⭐⭐⭐☆☆ (3/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lit, concat, concat_ws, lower, upper, substring,
    regexp_replace, split, explode, array, struct, to_date, datediff,
    current_date, coalesce, round as spark_round
)


def create_spark_session():
    return SparkSession.builder.appName("Intermediate01_Transformations").getOrCreate()


def create_sample_data(spark):
    """Create sample user activity data."""
    print("\n" + "=" * 80)
    print("CREATING SAMPLE DATA")
    print("=" * 80)
    
    data = [
        (1, "alice@EMAIL.COM", "  Alice Smith  ", "2024-01-15", 5, "premium", 150.00),
        (2, "bob@email.com", "bob jones", "2024-01-10", 2, "STANDARD", 50.00),
        (3, "charlie@email.com", "Charlie", "2023-12-01", 15, "premium", 500.00),
        (4, None, "Diana Prince", "2024-01-20", 0, "trial", 0.00),
        (5, "eve@email.com", "Eve", "2024-01-05", 8, None, 200.00),
    ]
    
    df = spark.createDataFrame(
        data,
        ["user_id", "email", "name", "signup_date", "logins", "tier", "total_spent"]
    )
    
    print("\n📊 Raw User Data:")
    df.show(truncate=False)
    return df


def example_1_string_transformations(df):
    """Advanced string operations."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: STRING TRANSFORMATIONS")
    print("=" * 80)
    
    df_transformed = df \
        .withColumn("email_clean", lower(col("email"))) \
        .withColumn("name_standardized", 
                   concat(
                       upper(substring(col("name"), 1, 1)),
                       lower(substring(col("name"), 2, 100))
                   )) \
        .withColumn("email_domain", 
                   split(col("email_clean"), "@").getItem(1)) \
        .withColumn("initials",
                   concat_ws(".", 
                            substring(split(col("name"), " ").getItem(0), 1, 1),
                            substring(split(col("name"), " ").getItem(1), 1, 1)))
    
    print("\n✅ String Transformations:")
    df_transformed.select(
        "user_id", "email_clean", "name_standardized", "email_domain", "initials"
    ).show(truncate=False)
    
    return df_transformed


def example_2_conditional_logic(df):
    """Complex conditional transformations."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: CONDITIONAL LOGIC")
    print("=" * 80)
    
    df_transformed = df \
        .withColumn("user_segment",
                   when(col("total_spent") >= 500, "VIP")
                   .when(col("total_spent") >= 100, "High Value")
                   .when(col("total_spent") > 0, "Active")
                   .otherwise("Inactive")) \
        .withColumn("engagement_level",
                   when(col("logins") >= 10, "🔥 Power User")
                   .when(col("logins") >= 5, "💪 Active")
                   .when(col("logins") >= 1, "👍 Regular")
                   .otherwise("😴 Dormant")) \
        .withColumn("risk_flag",
                   when((col("email").isNull()) | (col("total_spent") == 0), "⚠️  HIGH")
                   .when(col("logins") < 2, "⚡ MEDIUM")
                   .otherwise("✅ LOW"))
    
    print("\n✅ Conditional Logic:")
    df_transformed.select(
        "user_id", "name", "user_segment", "engagement_level", "risk_flag"
    ).show(truncate=False)
    
    return df_transformed


def example_3_date_operations(df):
    """Date calculations and transformations."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: DATE OPERATIONS")
    print("=" * 80)
    
    df_transformed = df \
        .withColumn("signup_date_parsed", to_date(col("signup_date"), "yyyy-MM-dd")) \
        .withColumn("days_since_signup", 
                   datediff(current_date(), col("signup_date_parsed"))) \
        .withColumn("account_age_category",
                   when(col("days_since_signup") < 30, "New (< 1 month)")
                   .when(col("days_since_signup") < 90, "Recent (1-3 months)")
                   .when(col("days_since_signup") < 365, "Established (< 1 year)")
                   .otherwise("Veteran (1+ years)"))
    
    print("\n✅ Date Operations:")
    df_transformed.select(
        "user_id", "name", "signup_date_parsed", 
        "days_since_signup", "account_age_category"
    ).show(truncate=False)
    
    return df_transformed


def example_4_null_handling(df):
    """Advanced null handling patterns."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: NULL HANDLING")
    print("=" * 80)
    
    df_transformed = df \
        .withColumn("email_fixed", 
                   coalesce(col("email"), lit("no-email@unknown.com"))) \
        .withColumn("tier_fixed",
                   coalesce(lower(col("tier")), lit("free"))) \
        .withColumn("data_completeness",
                   (col("email").isNotNull().cast("int") +
                    col("tier").isNotNull().cast("int") +
                    (col("total_spent") > 0).cast("int")) / 3 * 100)
    
    print("\n✅ Null Handling:")
    df_transformed.select(
        "user_id", "email_fixed", "tier_fixed", "data_completeness"
    ).show(truncate=False)
    
    return df_transformed


def example_5_calculated_metrics(df):
    """Create derived business metrics."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: CALCULATED METRICS")
    print("=" * 80)
    
    df_transformed = df \
        .withColumn("avg_spend_per_login",
                   when(col("logins") > 0, 
                        spark_round(col("total_spent") / col("logins"), 2))
                   .otherwise(0.0)) \
        .withColumn("customer_lifetime_value",
                   spark_round(col("total_spent") * 1.5, 2)) \
        .withColumn("value_tier",
                   when(col("customer_lifetime_value") >= 750, "💎 Diamond")
                   .when(col("customer_lifetime_value") >= 300, "🥇 Gold")
                   .when(col("customer_lifetime_value") >= 100, "🥈 Silver")
                   .otherwise("�� Bronze")) \
        .withColumn("engagement_score",
                   spark_round(
                       (col("logins") * 10) + 
                       (col("total_spent") / 10) +
                       when(col("tier") == "premium", 50).otherwise(0),
                       1
                   ))
    
    print("\n✅ Calculated Metrics:")
    df_transformed.select(
        "user_id", "name", "avg_spend_per_login", 
        "customer_lifetime_value", "value_tier", "engagement_score"
    ).show(truncate=False)
    
    return df_transformed


def main():
    """Run all transformation examples."""
    print("\n" + "🔄" * 40)
    print("INTERMEDIATE LESSON 1: ADVANCED TRANSFORMATIONS")
    print("🔄" * 40)
    
    spark = create_spark_session()
    
    try:
        df = create_sample_data(spark)
        
        example_1_string_transformations(df)
        example_2_conditional_logic(df)
        example_3_date_operations(df)
        example_4_null_handling(df)
        example_5_calculated_metrics(df)
        
        print("\n" + "=" * 80)
        print("🎉 ADVANCED TRANSFORMATIONS MASTERED!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. String operations (lower, upper, substring, split)")
        print("   2. Conditional logic (when/otherwise chains)")
        print("   3. Date operations (parsing, datediff)")
        print("   4. Null handling (coalesce, defaults)")
        print("   5. Calculated metrics (CLV, scores)")
        
        print("\n🏆 KEY PATTERN: Chain withColumn for complex transforms")
        print("\n➡️  NEXT: Try intermediate/02_window_functions.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
