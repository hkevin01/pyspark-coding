"""
Pandas vs PySpark: Missing Data Handling Comparison
===================================================
This script demonstrates handling null/missing values.
"""

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sample data with missing values
data = {
    "id": [1, 2, 3, 4, 5, 6, 7],
    "name": ["Alice", "Bob", None, "David", "Eve", "Frank", "Grace"],
    "age": [25, None, 35, 40, None, 50, 28],
    "salary": [50000, 60000, None, 80000, 90000, None, 70000],
    "city": ["NYC", "LA", "Chicago", None, "Phoenix", "Seattle", None]
}

print("=" * 80)
print("MISSING DATA HANDLING COMPARISON")
print("=" * 80)

# Create DataFrames
pandas_df = pd.DataFrame(data)
pandas_df = pandas_df.replace({None: np.nan})  # Ensure NaN for pandas

spark = SparkSession.builder.appName("MissingData").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
pyspark_df = spark.createDataFrame(data)

print("\nOriginal Data (with missing values):")
print(pandas_df)
print()

# ============================================================================
# 1. DETECT MISSING VALUES
# ============================================================================
print("=" * 80)
print("1. DETECT MISSING VALUES")
print("=" * 80)

# Pandas
print("\nPANDAS - df.isnull().sum() (count nulls per column):")
print(pandas_df.isnull().sum())

print("\nPANDAS - Total missing values:")
print(f"Total: {pandas_df.isnull().sum().sum()}")

# PySpark
print("\nPYSPARK - Count nulls per column:")
null_counts = pyspark_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) 
    for c in pyspark_df.columns
])
null_counts.show()

# ============================================================================
# 2. DROP ROWS WITH ANY NULL
# ============================================================================
print("\n" + "=" * 80)
print("2. DROP ROWS WITH ANY NULL VALUE")
print("=" * 80)

# Pandas
print("\nPANDAS - df.dropna():")
result_pandas = pandas_df.dropna()
print(result_pandas)
print(f"Rows remaining: {len(result_pandas)}")

# PySpark
print("\nPYSPARK - df.na.drop():")
result_pyspark = pyspark_df.na.drop()
result_pyspark.show()
print(f"Rows remaining: {result_pyspark.count()}")

# ============================================================================
# 3. DROP ROWS WITH NULL IN SPECIFIC COLUMNS
# ============================================================================
print("\n" + "=" * 80)
print("3. DROP ROWS WITH NULL IN SPECIFIC COLUMNS (name, age)")
print("=" * 80)

# Pandas
print("\nPANDAS - df.dropna(subset=['name', 'age']):")
result_pandas = pandas_df.dropna(subset=["name", "age"])
print(result_pandas)

# PySpark
print("\nPYSPARK - df.na.drop(subset=['name', 'age']):")
result_pyspark = pyspark_df.na.drop(subset=["name", "age"])
result_pyspark.show()

# ============================================================================
# 4. DROP ROWS WITH THRESHOLD
# ============================================================================
print("\n" + "=" * 80)
print("4. DROP ROWS WITH < 3 NON-NULL VALUES")
print("=" * 80)

# Pandas
print("\nPANDAS - df.dropna(thresh=3):")
result_pandas = pandas_df.dropna(thresh=3)
print(result_pandas)

# PySpark
print("\nPYSPARK - df.na.drop(thresh=3):")
result_pyspark = pyspark_df.na.drop(thresh=3)
result_pyspark.show()

# ============================================================================
# 5. FILL ALL NULLS WITH A VALUE
# ============================================================================
print("\n" + "=" * 80)
print("5. FILL ALL NULLS WITH 0 / 'Unknown'")
print("=" * 80)

# Pandas
print("\nPANDAS - df.fillna(0) for numeric:")
result_pandas = pandas_df.fillna(0)
print(result_pandas)

# PySpark
print("\nPYSPARK - df.na.fill(0) for numeric, 'Unknown' for strings:")
result_pyspark = pyspark_df.na.fill(0).na.fill("Unknown")
result_pyspark.show()

# ============================================================================
# 6. FILL SPECIFIC COLUMNS
# ============================================================================
print("\n" + "=" * 80)
print("6. FILL SPECIFIC COLUMNS WITH DIFFERENT VALUES")
print("=" * 80)

# Pandas
print("\nPANDAS - df.fillna({'age': 0, 'city': 'Unknown'}):")
result_pandas = pandas_df.fillna({"age": 0, "name": "Unknown", "city": "Unknown"})
print(result_pandas)

# PySpark
print("\nPYSPARK - df.na.fill({'age': 0, 'city': 'Unknown'}):")
result_pyspark = pyspark_df.na.fill({"age": 0, "name": "Unknown", "city": "Unknown"})
result_pyspark.show()

# ============================================================================
# 7. FILL WITH MEAN/MEDIAN
# ============================================================================
print("\n" + "=" * 80)
print("7. FILL AGE WITH MEAN VALUE")
print("=" * 80)

# Pandas
print("\nPANDAS - df['age'].fillna(df['age'].mean()):")
result_pandas = pandas_df.copy()
age_mean = pandas_df["age"].mean()
result_pandas["age"] = result_pandas["age"].fillna(age_mean)
print(f"Age mean: {age_mean:.2f}")
print(result_pandas[["id", "name", "age"]])

# PySpark
print("\nPYSPARK - Fill age with mean:")
age_mean = pyspark_df.select(F.mean("age")).first()[0]
result_pyspark = pyspark_df.na.fill({"age": age_mean})
print(f"Age mean: {age_mean:.2f}")
result_pyspark.select("id", "name", "age").show()

# ============================================================================
# 8. FORWARD/BACKWARD FILL (Pandas only)
# ============================================================================
print("\n" + "=" * 80)
print("8. FORWARD FILL (Pandas feature)")
print("=" * 80)

# Pandas
print("\nPANDAS - df.fillna(method='ffill') [forward fill]:")
result_pandas = pandas_df.fillna(method="ffill")
print(result_pandas)

print("\nNote: PySpark doesn't have built-in ffill/bfill.")
print("Would need to use window functions for similar behavior.")

# ============================================================================
# 9. REPLACE SPECIFIC VALUES
# ============================================================================
print("\n" + "=" * 80)
print("9. REPLACE SPECIFIC VALUES (Replace 'NYC' with 'New York')")
print("=" * 80)

# Pandas
print("\nPANDAS - df['city'].replace('NYC', 'New York'):")
result_pandas = pandas_df.copy()
result_pandas["city"] = result_pandas["city"].replace("NYC", "New York")
print(result_pandas[["id", "city"]])

# PySpark
print("\nPYSPARK - Use when().otherwise() or replace:")
result_pyspark = pyspark_df.withColumn(
    "city",
    F.when(F.col("city") == "NYC", "New York").otherwise(F.col("city"))
)
result_pyspark.select("id", "city").show()

# ============================================================================
# 10. FILTER NON-NULL ROWS
# ============================================================================
print("\n" + "=" * 80)
print("10. FILTER FOR NON-NULL ROWS (age is not null)")
print("=" * 80)

# Pandas
print("\nPANDAS - df[df['age'].notna()]:")
result_pandas = pandas_df[pandas_df["age"].notna()]
print(result_pandas)

# PySpark
print("\nPYSPARK - df.filter(F.col('age').isNotNull()):")
result_pyspark = pyspark_df.filter(F.col("age").isNotNull())
result_pyspark.show()

# ============================================================================
# 11. COALESCE (Use first non-null value)
# ============================================================================
print("\n" + "=" * 80)
print("11. COALESCE - Use salary, or age*1000 if salary is null")
print("=" * 80)

# Pandas
print("\nPANDAS - Using np.where or fillna:")
result_pandas = pandas_df.copy()
result_pandas["effective_salary"] = result_pandas["salary"].fillna(
    result_pandas["age"] * 1000
)
print(result_pandas[["id", "name", "age", "salary", "effective_salary"]])

# PySpark
print("\nPYSPARK - Using F.coalesce:")
result_pyspark = pyspark_df.withColumn(
    "effective_salary",
    F.coalesce(F.col("salary"), F.col("age") * 1000)
)
result_pyspark.select("id", "name", "age", "salary", "effective_salary").show()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY DIFFERENCES SUMMARY:")
print("=" * 80)
print("""
1. Detect Nulls:
   - Pandas: df.isnull() or df.isna()
   - PySpark: F.col('col').isNull()

2. Count Nulls:
   - Pandas: df.isnull().sum()
   - PySpark: Use F.count(F.when(F.col(c).isNull(), c))

3. Drop Nulls:
   - Pandas: df.dropna() or df.dropna(subset=['col'])
   - PySpark: df.na.drop() or df.na.drop(subset=['col'])

4. Fill Nulls:
   - Pandas: df.fillna(value) or df.fillna({'col': value})
   - PySpark: df.na.fill(value) or df.na.fill({'col': value})

5. Fill with Stats:
   - Pandas: df['col'].fillna(df['col'].mean())
   - PySpark: Calculate mean first, then df.na.fill({'col': mean})

6. Forward/Backward Fill:
   - Pandas: df.fillna(method='ffill') or df.fillna(method='bfill')
   - PySpark: Use Window functions (more complex)

7. Filter Non-Null:
   - Pandas: df[df['col'].notna()]
   - PySpark: df.filter(F.col('col').isNotNull())

8. Coalesce (first non-null):
   - Pandas: df['col1'].fillna(df['col2'])
   - PySpark: F.coalesce(F.col('col1'), F.col('col2'))

9. Replace Values:
   - Pandas: df['col'].replace(old, new)
   - PySpark: F.when(F.col('col') == old, new).otherwise(F.col('col'))

10. Both handle nulls well, but syntax differs!
    PySpark: Use df.na.* methods
    Pandas: Use .fillna(), .dropna(), .isna()
""")

spark.stop()
print("\n" + "=" * 80)
print("Script completed!")
print("=" * 80)
