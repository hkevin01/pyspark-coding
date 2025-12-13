"""
PySpark Undefined Behavior: Type Coercion & NULL Handling

Demonstrates dangerous patterns related to type conversions and NULLs.

UNDEFINED BEHAVIORS COVERED:
============================
1. Implicit type coercion surprises
2. NULL propagation in UDFs
3. NULL vs None confusion
4. String to number coercion edge cases
5. Division by zero behavior
6. NaN vs NULL differences
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, when
from pyspark.sql.types import IntegerType, StringType, FloatType
import math


def dangerous_implicit_coercion():
    """
    ⚠️ UNDEFINED BEHAVIOR: Implicit type coercion loses data.
    
    PROBLEM: String "123abc" cast to int becomes NULL.
    RESULT: Silent data loss, no error raised.
    """
    spark = SparkSession.builder.appName("Coercion UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: Implicit type coercion loses data")
    
    data = [("123",), ("456abc",), ("789.5",), ("not_a_number",)]
    df = spark.createDataFrame(data, ["value"])
    
    # ❌ Invalid strings become NULL (silent failure)
    df_coerced = df.withColumn("as_int", col("value").cast("int"))
    df_coerced.show()
    
    null_count = df_coerced.filter(col("as_int").isNull()).count()
    print(f"   ⚠️ {null_count} values became NULL silently!")
    
    spark.stop()


def dangerous_null_udf():
    """
    ⚠️ UNDEFINED BEHAVIOR: UDF doesn't handle NULLs properly.
    
    PROBLEM: Python None causes exceptions in UDF.
    RESULT: Executor crashes, failed jobs.
    """
    spark = SparkSession.builder.appName("NULL UDF UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: UDF doesn't handle NULLs")
    
    @udf(IntegerType())
    def dangerous_multiply(value):
        # ❌ Crashes on NULL (value is None)
        return value * 2  # TypeError: unsupported operand type(s)
    
    data = [(1,), (2,), (None,), (4,)]
    df = spark.createDataFrame(data, ["value"])
    
    try:
        result = df.withColumn("doubled", dangerous_multiply(col("value")))
        result.show()
    except Exception as e:
        print(f"   ❌ ERROR: {type(e).__name__}: {str(e)[:100]}")
    
    spark.stop()


def safe_null_handling():
    """
    ✅ SAFE: Explicit NULL handling in UDF.
    """
    spark = SparkSession.builder.appName("Safe NULL").master("local[*]").getOrCreate()
    
    print("✅ SAFE: Explicit NULL handling")
    
    @udf(IntegerType())
    def safe_multiply(value):
        # ✅ Handle NULL explicitly
        if value is None:
            return None
        return value * 2
    
    data = [(1,), (2,), (None,), (4,)]
    df = spark.createDataFrame(data, ["value"])
    result = df.withColumn("doubled", safe_multiply(col("value")))
    result.show()
    
    print("   ✅ NULLs handled gracefully!")
    
    spark.stop()


def dangerous_division_by_zero():
    """
    ⚠️ UNDEFINED BEHAVIOR: Division by zero behavior.
    
    PROBLEM: Returns Infinity, not NULL or error.
    RESULT: Downstream computations corrupted.
    """
    spark = SparkSession.builder.appName("Division UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: Division by zero")
    
    data = [(10, 2), (20, 0), (30, 5)]
    df = spark.createDataFrame(data, ["numerator", "denominator"])
    
    # ❌ Division by zero gives Infinity!
    result = df.withColumn("result", col("numerator") / col("denominator"))
    result.show()
    
    print("   ⚠️ Division by zero produces Infinity, not NULL!")
    
    spark.stop()


def safe_division():
    """
    ✅ SAFE: Check for zero before division.
    """
    spark = SparkSession.builder.appName("Safe Division").master("local[*]").getOrCreate()
    
    print("✅ SAFE: Explicit zero check")
    
    data = [(10, 2), (20, 0), (30, 5)]
    df = spark.createDataFrame(data, ["numerator", "denominator"])
    
    # ✅ Explicit check for zero
    result = df.withColumn(
        "result",
        when(col("denominator") == 0, None)
        .otherwise(col("numerator") / col("denominator"))
    )
    result.show()
    
    print("   ✅ Zero denominator becomes NULL!")
    
    spark.stop()


def dangerous_nan_vs_null():
    """
    ⚠️ UNDEFINED BEHAVIOR: NaN vs NULL confusion.
    
    PROBLEM: NaN != NaN in comparisons.
    RESULT: Filters don't work as expected.
    """
    spark = SparkSession.builder.appName("NaN UB").master("local[*]").getOrCreate()
    
    print("❌ DANGEROUS: NaN vs NULL confusion")
    
    data = [(1.0,), (float('nan'),), (None,), (3.0,)]
    df = spark.createDataFrame(data, ["value"])
    
    print("   Original data:")
    df.show()
    
    # ❌ NaN != NaN, so filter doesn't work!
    filtered = df.filter(col("value") == float('nan'))
    print(f"   Rows where value == NaN: {filtered.count()}")
    print("   ⚠️ NaN comparisons are always false!")
    
    spark.stop()


print("\n" + "=" * 80)
print("KEY TAKEAWAYS:")
print("=" * 80)
print("❌ Type coercion silently creates NULLs")
print("❌ UDFs must explicitly handle NULLs")
print("❌ Division by zero produces Infinity")
print("❌ NaN != NaN in comparisons")
print("✅ Validate data before type casting")
print("✅ Always check for None in UDFs")
print("✅ Use when() to check denominator != 0")
print("✅ Use isnan() function for NaN checks")
print("=" * 80 + "\n")
