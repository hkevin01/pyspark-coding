"""
================================================================================
BEGINNER 03: Data Cleaning Essentials
================================================================================

PURPOSE: Learn fundamental data cleaning techniques - handle nulls, remove
duplicates, fix data types, and validate data quality.

WHAT YOU'LL LEARN:
- Handle missing values (nulls)
- Remove duplicate rows
- Fix data types
- Filter invalid data
- Rename columns

WHY: Real-world data is messy! 80% of data engineering is cleaning data.

REAL-WORLD: Clean customer database with duplicates, missing emails, invalid ages.

TIME: 25 minutes | DIFFICULTY: ⭐⭐☆☆☆ (2/5)
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, regexp_replace, when, isnan, count


def create_spark_session():
    return SparkSession.builder.appName("Beginner03_Cleaning").getOrCreate()


def create_messy_data(spark):
    """Create messy data that needs cleaning."""
    print("\n" + "=" * 80)
    print("CREATING MESSY DATA (like real-world!)")
    print("=" * 80)
    
    # Messy customer data with common problems
    data = [
        (1, "Alice Smith", "alice@email.com", 25, "NY"),
        (2, " Bob Jones ", "  bob@email.com", 30, "CA"),
        (1, "Alice Smith", "alice@email.com", 25, "NY"),  # Duplicate!
        (3, "Charlie Brown", None, 35, "TX"),  # Missing email
        (4, "Diana Prince", "diana@email.com", -5, "WA"),  # Invalid age
        (5, "Eve Wilson", "INVALID", 28, "FL"),  # Bad email
        (6, "Frank Miller", "frank@email.com", None, "NY"),  # Missing age
        (7, "  Grace Lee  ", "grace@email.com", 150, "CA"),  # Invalid age
    ]
    
    df = spark.createDataFrame(data, ["id", "name", "email", "age", "state"])
    
    print("\n🔍 MESSY DATA (notice problems):")
    df.show(truncate=False)
    
    print("\n❌ Problems:")
    print("   - Row 3: Duplicate of row 1")
    print("   - Row 4: Missing email (None)")
    print("   - Row 5: Negative age (-5)")
    print("   - Row 6: Invalid email ('INVALID')")
    print("   - Row 7: Missing age (None)")
    print("   - Row 8: Invalid age (150)")
    print("   - Multiple rows: Extra whitespace in names")
    
    return df


def example_1_check_nulls(df):
    """Check for null/missing values."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: CHECK FOR NULL VALUES")
    print("=" * 80)
    
    print("\n🔍 Null count per column:")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()
    
    print("\n💡 TIP: Always check for nulls before processing!")
    return df


def example_2_handle_nulls(df):
    """Handle missing values."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: HANDLE NULL VALUES")
    print("=" * 80)
    
    # Drop rows where id is null
    df_clean = df.dropna(subset=["id"])
    
    # Fill missing emails with default
    df_clean = df_clean.fillna({"email": "no-email@unknown.com"})
    
    # Fill missing ages with median (30)
    df_clean = df_clean.fillna({"age": 30})
    
    print("\n✅ After handling nulls:")
    df_clean.show(truncate=False)
    
    print("\n💡 Strategies:")
    print("   - dropna(): Remove rows with nulls")
    print("   - fillna(): Replace nulls with defaults")
    print("   - Could also use mean/median for numeric columns")
    
    return df_clean


def example_3_remove_duplicates(df):
    """Remove duplicate rows."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: REMOVE DUPLICATES")
    print("=" * 80)
    
    print(f"\n🔍 Before: {df.count()} rows")
    
    # Remove exact duplicates
    df_dedup = df.dropDuplicates()
    
    print(f"✅ After removing duplicates: {df_dedup.count()} rows")
    
    # Remove duplicates based on specific columns
    df_dedup_id = df.dropDuplicates(["id"])
    print(f"✅ After dedup by ID: {df_dedup_id.count()} rows")
    
    df_dedup.show(truncate=False)
    
    return df_dedup


def example_4_clean_strings(df):
    """Clean string data (whitespace, case, etc.)."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: CLEAN STRING DATA")
    print("=" * 80)
    
    # Remove extra whitespace
    df_clean = df.withColumn("name", trim(col("name")))
    
    # Standardize state codes to uppercase
    df_clean = df_clean.withColumn("state", upper(col("state")))
    
    # Clean email formatting
    df_clean = df_clean.withColumn("email", trim(col("email")))
    
    print("\n✅ After string cleaning:")
    df_clean.show(truncate=False)
    
    print("\n💡 Common string operations:")
    print("   - trim(): Remove leading/trailing whitespace")
    print("   - upper()/lower(): Standardize case")
    print("   - regexp_replace(): Pattern-based cleaning")
    
    return df_clean


def example_5_validate_data(df):
    """Validate and filter invalid data."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: VALIDATE AND FILTER DATA")
    print("=" * 80)
    
    # Filter valid ages (0-120)
    df_valid = df.filter((col("age") > 0) & (col("age") <= 120))
    
    # Filter valid emails (contains @)
    df_valid = df_valid.filter(col("email").contains("@"))
    
    # Mark data quality
    df_valid = df_valid.withColumn(
        "data_quality",
        when((col("age").isNotNull()) & (col("email").contains("@")), "GOOD")
        .otherwise("NEEDS_REVIEW")
    )
    
    print("\n✅ After validation:")
    df_valid.show(truncate=False)
    
    print(f"\n📊 Quality Summary:")
    print(f"   Input rows:  {df.count()}")
    print(f"   Valid rows:  {df_valid.count()}")
    print(f"   Removed:     {df.count() - df_valid.count()}")
    
    return df_valid


def main():
    """Run all cleaning examples."""
    print("\n" + "🧹" * 40)
    print("BEGINNER LESSON 3: DATA CLEANING")
    print("🧹" * 40)
    
    spark = create_spark_session()
    
    try:
        df = create_messy_data(spark)
        df = example_1_check_nulls(df)
        df = example_2_handle_nulls(df)
        df = example_3_remove_duplicates(df)
        df = example_4_clean_strings(df)
        df_final = example_5_validate_data(df)
        
        print("\n" + "=" * 80)
        print("🎉 DATA CLEANING COMPLETE!")
        print("=" * 80)
        print("\n✅ What you learned:")
        print("   1. Check for null values")
        print("   2. Handle missing data (drop/fill)")
        print("   3. Remove duplicates")
        print("   4. Clean string data (trim, case)")
        print("   5. Validate and filter invalid data")
        
        print("\n🏆 KEY RULE: Always validate your data!")
        print("\n➡️  NEXT: Try beginner/04_joins.py")
        print("=" * 80 + "\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
