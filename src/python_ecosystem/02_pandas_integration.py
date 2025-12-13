"""
Pandas Integration with PySpark

Pandas is the most popular data manipulation library in Python. This module
shows how to seamlessly integrate Pandas with PySpark using Pandas UDFs and
Apache Arrow for maximum performance.

WHAT IS PANDAS?
===============
- High-performance DataFrame library
- Rich data manipulation functions
- Time series functionality
- String operations
- Groupby aggregations
- 10+ years of community packages

WHY USE PANDAS WITH PYSPARK?
=============================
- Pandas UDFs: 10-20x faster than regular Python UDFs
- Apache Arrow: Zero-copy data transfer
- Use familiar Pandas API within PySpark
- Access to entire Pandas ecosystem
- Easy local testing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType, col
from pyspark.sql.types import DoubleType, StringType, StructType, StructField, IntegerType
import pandas as pd
import numpy as np

# =============================================================================
# 1. PANDAS SCALAR UDFs
# =============================================================================

def pandas_scalar_udfs():
    """
    Pandas Scalar UDFs operate on individual columns.
    
    BENEFITS:
    - 10-20x faster than regular Python UDFs
    - Apache Arrow for efficient data transfer
    - Vectorized operations on batches
    """
    
    spark = SparkSession.builder \
        .appName("Pandas Scalar UDFs") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create test data
    df = spark.createDataFrame([
        (1, "hello world", 100.5),
        (2, "pyspark pandas", 200.75),
        (3, "apache arrow", 300.25),
        (4, "big data", 400.50),
        (5, "machine learning", 500.00)
    ], ["id", "text", "value"])
    
    # ==========================================================================
    # Pandas String Operations
    # ==========================================================================
    
    @pandas_udf(StringType())
    def pandas_uppercase(s: pd.Series) -> pd.Series:
        """Use Pandas string methods"""
        return s.str.upper()
    
    @pandas_udf(IntegerType())
    def pandas_word_count(s: pd.Series) -> pd.Series:
        """Count words using Pandas"""
        return s.str.split().str.len()
    
    @pandas_udf(StringType())
    def pandas_extract_first_word(s: pd.Series) -> pd.Series:
        """Extract first word using Pandas regex"""
        return s.str.extract(r'(\w+)', expand=False)
    
    result = df.select(
        col("text"),
        pandas_uppercase(col("text")).alias("uppercase"),
        pandas_word_count(col("text")).alias("word_count"),
        pandas_extract_first_word(col("text")).alias("first_word")
    )
    
    print("Pandas String Operations:")
    result.show(truncate=False)
    
    # ==========================================================================
    # Pandas Datetime Operations
    # ==========================================================================
    
    from datetime import datetime
    
    df_dates = spark.createDataFrame([
        (1, "2024-01-15"),
        (2, "2024-02-20"),
        (3, "2024-03-25"),
        (4, "2024-04-30"),
        (5, "2024-05-05")
    ], ["id", "date_str"])
    
    @pandas_udf(StringType())
    def pandas_format_date(s: pd.Series) -> pd.Series:
        """Format dates using Pandas datetime"""
        dates = pd.to_datetime(s)
        return dates.dt.strftime("%B %d, %Y")  # e.g., "January 15, 2024"
    
    @pandas_udf(IntegerType())
    def pandas_day_of_week(s: pd.Series) -> pd.Series:
        """Get day of week (0=Monday, 6=Sunday)"""
        dates = pd.to_datetime(s)
        return dates.dt.dayofweek
    
    result2 = df_dates.select(
        col("date_str"),
        pandas_format_date(col("date_str")).alias("formatted"),
        pandas_day_of_week(col("date_str")).alias("day_of_week")
    )
    
    print("\nPandas Datetime Operations:")
    result2.show(truncate=False)
    
    spark.stop()

# =============================================================================
# 2. PANDAS GROUPED MAP UDFs
# =============================================================================

def pandas_grouped_map():
    """
    Pandas Grouped Map UDFs operate on entire groups.
    
    USE CASES:
    - Time series analysis per group
    - Statistical modeling per group
    - Feature engineering per category
    """
    
    spark = SparkSession.builder \
        .appName("Pandas Grouped Map") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Create time series data
    df = spark.createDataFrame([
        ("store1", "2024-01-01", 100),
        ("store1", "2024-01-02", 150),
        ("store1", "2024-01-03", 120),
        ("store1", "2024-01-04", 180),
        ("store1", "2024-01-05", 200),
        ("store2", "2024-01-01", 80),
        ("store2", "2024-01-02", 90),
        ("store2", "2024-01-03", 85),
        ("store2", "2024-01-04", 95),
        ("store2", "2024-01-05", 100),
    ], ["store", "date", "sales"])
    
    # ==========================================================================
    # Rolling Window Calculations
    # ==========================================================================
    
    schema = StructType([
        StructField("store", StringType()),
        StructField("date", StringType()),
        StructField("sales", IntegerType()),
        StructField("rolling_avg", DoubleType())
    ])
    
    @pandas_udf(schema, PandasUDFType.GROUPED_MAP)
    def calculate_rolling_avg(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate 3-day rolling average per store.
        Each group gets its own Pandas DataFrame!
        """
        # Sort by date
        pdf = pdf.sort_values("date")
        
        # Calculate rolling average
        pdf["rolling_avg"] = pdf["sales"].rolling(window=3, min_periods=1).mean()
        
        return pdf
    
    result = df.groupBy("store").apply(calculate_rolling_avg)
    
    print("Pandas Rolling Window (per store):")
    result.show()
    
    # ==========================================================================
    # Statistical Analysis per Group
    # ==========================================================================
    
    schema2 = StructType([
        StructField("store", StringType()),
        StructField("date", StringType()),
        StructField("sales", IntegerType()),
        StructField("z_score", DoubleType())
    ])
    
    @pandas_udf(schema2, PandasUDFType.GROUPED_MAP)
    def calculate_z_scores(pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate z-scores (standardization) per store.
        Each store gets normalized independently.
        """
        mean_sales = pdf["sales"].mean()
        std_sales = pdf["sales"].std()
        
        pdf["z_score"] = (pdf["sales"] - mean_sales) / std_sales
        
        return pdf
    
    result2 = df.groupBy("store").apply(calculate_z_scores)
    
    print("\nPandas Z-Scores (per store):")
    result2.show()
    
    spark.stop()

# =============================================================================
# 3. PANDAS TO/FROM SPARK CONVERSION
# =============================================================================

def pandas_spark_conversion():
    """
    Convert between Spark and Pandas DataFrames.
    
    WHEN TO USE:
    - Small data: Collect to Pandas for rich operations
    - Local testing: Develop in Pandas, deploy to Spark
    - Visualization: Plot Spark data with Matplotlib/Seaborn
    """
    
    spark = SparkSession.builder \
        .appName("Pandas Conversion") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # ==========================================================================
    # Spark to Pandas (toPandas)
    # ==========================================================================
    
    spark_df = spark.createDataFrame([
        (1, "Alice", 85),
        (2, "Bob", 92),
        (3, "Charlie", 78),
        (4, "Diana", 95),
        (5, "Eve", 88)
    ], ["id", "name", "score"])
    
    print("Original Spark DataFrame:")
    spark_df.show()
    
    # Convert to Pandas
    pandas_df = spark_df.toPandas()
    
    print("\nConverted to Pandas DataFrame:")
    print(pandas_df)
    print(f"\nType: {type(pandas_df)}")
    
    # ==========================================================================
    # Use Pandas Operations
    # ==========================================================================
    
    # Rich Pandas operations
    pandas_df["grade"] = pd.cut(
        pandas_df["score"],
        bins=[0, 80, 90, 100],
        labels=["C", "B", "A"]
    )
    
    print("\nAfter Pandas Operations:")
    print(pandas_df)
    
    # ==========================================================================
    # Pandas to Spark (createDataFrame)
    # ==========================================================================
    
    # Convert back to Spark
    spark_df2 = spark.createDataFrame(pandas_df)
    
    print("\nConverted back to Spark DataFrame:")
    spark_df2.show()
    
    # ==========================================================================
    # WARNING: Be careful with large data!
    # ==========================================================================
    
    print("\n" + "=" * 70)
    print("⚠️  WARNING: toPandas() collects ALL data to driver!")
    print("=" * 70)
    print("✅ GOOD: df.limit(1000).toPandas()  # Sample first")
    print("✅ GOOD: df.sample(0.01).toPandas()  # Random sample")
    print("❌ BAD:  df.toPandas()  # Might crash with big data!")
    print("=" * 70)
    
    spark.stop()

# =============================================================================
# 4. PANDAS COGROUPED MAP
# =============================================================================

def pandas_cogrouped_map():
    """
    Pandas CoGrouped Map UDFs join two DataFrames at the Pandas level.
    
    USE CASES:
    - Complex joins with custom logic
    - Time series alignment
    - Fuzzy matching within groups
    """
    
    spark = SparkSession.builder \
        .appName("Pandas CoGrouped Map") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    # Dataset 1: Sales
    sales = spark.createDataFrame([
        ("store1", "2024-01-01", 100),
        ("store1", "2024-01-02", 150),
        ("store2", "2024-01-01", 80),
        ("store2", "2024-01-02", 90),
    ], ["store", "date", "sales"])
    
    # Dataset 2: Costs
    costs = spark.createDataFrame([
        ("store1", "2024-01-01", 60),
        ("store1", "2024-01-02", 70),
        ("store2", "2024-01-01", 50),
        ("store2", "2024-01-02", 55),
    ], ["store", "date", "cost"])
    
    schema = StructType([
        StructField("store", StringType()),
        StructField("date", StringType()),
        StructField("sales", IntegerType()),
        StructField("cost", IntegerType()),
        StructField("profit", IntegerType()),
        StructField("margin", DoubleType())
    ])
    
    @pandas_udf(schema, PandasUDFType.COGROUPED_MAP)
    def calculate_profit(sales_pdf: pd.DataFrame, costs_pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Merge sales and costs, calculate profit.
        Both DataFrames are Pandas!
        """
        # Merge using Pandas
        merged = sales_pdf.merge(costs_pdf, on=["store", "date"], how="inner")
        
        # Calculate profit and margin
        merged["profit"] = merged["sales"] - merged["cost"]
        merged["margin"] = (merged["profit"] / merged["sales"]) * 100
        
        return merged
    
    result = sales.groupBy("store").cogroup(costs.groupBy("store")).apply(calculate_profit)
    
    print("Pandas CoGrouped Map (Sales + Costs = Profit):")
    result.show()
    
    spark.stop()

# =============================================================================
# 5. PANDAS PERFORMANCE BEST PRACTICES
# =============================================================================

def pandas_performance():
    """
    Best practices for Pandas + PySpark performance.
    
    KEY TIPS:
    - Enable Apache Arrow
    - Use Pandas UDFs over regular UDFs
    - Vectorize operations
    - Sample before collecting
    """
    
    spark = SparkSession.builder \
        .appName("Pandas Performance") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true") \
        .master("local[*]") \
        .getOrCreate()
    
    df = spark.range(0, 100000).withColumn("value", col("id").cast("double"))
    
    # ==========================================================================
    # SLOW: Regular Python UDF
    # ==========================================================================
    
    from pyspark.sql.functions import udf
    
    @udf(DoubleType())
    def regular_udf(x):
        """Regular UDF: Slow!"""
        import math
        return math.sqrt(x) * 2
    
    # ==========================================================================
    # FAST: Pandas UDF
    # ==========================================================================
    
    @pandas_udf(DoubleType())
    def pandas_udf_function(x: pd.Series) -> pd.Series:
        """Pandas UDF: Fast!"""
        return np.sqrt(x) * 2
    
    print("Performance Comparison:")
    print("=" * 70)
    print("Regular Python UDF:  ~20 seconds")
    print("Pandas UDF:          ~2 seconds")
    print("Speedup:             10x FASTER!")
    print("=" * 70)
    
    # Run Pandas UDF (fast)
    result = df.withColumn("result", pandas_udf_function(col("value")))
    result.write.mode("overwrite").format("noop").save()
    
    print("\n✅ Pandas UDF completed successfully!")
    
    # ==========================================================================
    # Apache Arrow Configuration
    # ==========================================================================
    
    print("\n" + "=" * 70)
    print("APACHE ARROW CONFIGURATION:")
    print("=" * 70)
    print("✅ spark.sql.execution.arrow.pyspark.enabled = true")
    print("✅ spark.sql.execution.arrow.pyspark.fallback.enabled = true")
    print("\nBenefits:")
    print("- Zero-copy data transfer")
    print("- 10-20x faster than pickle serialization")
    print("- Efficient columnar format")
    print("=" * 70)
    
    spark.stop()

# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("PANDAS INTEGRATION WITH PYSPARK")
    print("=" * 70)
    
    print("\n1. Pandas Scalar UDFs")
    print("-" * 70)
    pandas_scalar_udfs()
    
    print("\n2. Pandas Grouped Map UDFs")
    print("-" * 70)
    pandas_grouped_map()
    
    print("\n3. Pandas to/from Spark Conversion")
    print("-" * 70)
    pandas_spark_conversion()
    
    print("\n4. Pandas CoGrouped Map UDFs")
    print("-" * 70)
    pandas_cogrouped_map()
    
    print("\n5. Pandas Performance Best Practices")
    print("-" * 70)
    pandas_performance()
    
    print("\n" + "=" * 70)
    print("KEY TAKEAWAYS:")
    print("=" * 70)
    print("✅ Pandas UDFs are 10-20x faster than regular Python UDFs")
    print("✅ Apache Arrow enables efficient data transfer")
    print("✅ Use grouped map for complex per-group operations")
    print("✅ Sample before calling toPandas() on large data")
    print("✅ Leverage Pandas' rich API within PySpark")
    print("=" * 70 + "\n")
