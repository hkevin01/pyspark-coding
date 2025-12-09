"""
Pandas vs PySpark: Basic Operations Comparison
===============================================
This script demonstrates basic DataFrame operations in both Pandas and PySpark.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sample data
data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 90000],
    "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"]
}

print("=" * 80)
print("BASIC OPERATIONS COMPARISON")
print("=" * 80)

# ============================================================================
# 1. CREATE DATAFRAME
# ============================================================================
print("\n1. CREATE DATAFRAME")
print("-" * 80)

# Pandas
print("PANDAS:")
pandas_df = pd.DataFrame(data)
print(pandas_df)

# PySpark
print("\nPYSPARK:")
spark = SparkSession.builder.appName("BasicOps").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
pyspark_df = spark.createDataFrame(pd.DataFrame(data))
pyspark_df.show()

# ============================================================================
# 2. VIEW DATA
# ============================================================================
print("\n2. VIEW DATA (First 3 rows)")
print("-" * 80)

# Pandas
print("PANDAS - df.head(3):")
print(pandas_df.head(3))

# PySpark
print("\nPYSPARK - df.show(3):")
pyspark_df.show(3)

# ============================================================================
# 3. SELECT COLUMNS
# ============================================================================
print("\n3. SELECT COLUMNS (name, salary)")
print("-" * 80)

# Pandas
print("PANDAS - df[['name', 'salary']]:")
print(pandas_df[["name", "salary"]])

# PySpark
print("\nPYSPARK - df.select('name', 'salary'):")
pyspark_df.select("name", "salary").show()

# ============================================================================
# 4. FILTER ROWS
# ============================================================================
print("\n4. FILTER ROWS (age > 30)")
print("-" * 80)

# Pandas
print("PANDAS - df[df['age'] > 30]:")
print(pandas_df[pandas_df["age"] > 30])

# PySpark
print("\nPYSPARK - df.filter(F.col('age') > 30):")
pyspark_df.filter(F.col("age") > 30).show()

# ============================================================================
# 5. ADD NEW COLUMN
# ============================================================================
print("\n5. ADD NEW COLUMN (annual_bonus = salary * 0.1)")
print("-" * 80)

# Pandas
print("PANDAS - df['annual_bonus'] = df['salary'] * 0.1:")
pandas_df_copy = pandas_df.copy()
pandas_df_copy["annual_bonus"] = pandas_df_copy["salary"] * 0.1
print(pandas_df_copy[["name", "salary", "annual_bonus"]])

# PySpark
print("\nPYSPARK - df.withColumn('annual_bonus', F.col('salary') * 0.1):")
pyspark_df.withColumn("annual_bonus", F.col("salary") * 0.1).select(
    "name", "salary", "annual_bonus"
).show()

# ============================================================================
# 6. SORT DATA
# ============================================================================
print("\n6. SORT DATA (by salary descending)")
print("-" * 80)

# Pandas
print("PANDAS - df.sort_values('salary', ascending=False):")
print(pandas_df.sort_values("salary", ascending=False))

# PySpark
print("\nPYSPARK - df.orderBy(F.desc('salary')):")
pyspark_df.orderBy(F.desc("salary")).show()

# ============================================================================
# 7. COLUMN STATISTICS
# ============================================================================
print("\n7. COLUMN STATISTICS")
print("-" * 80)

# Pandas
print("PANDAS - df['salary'].describe():")
print(pandas_df["salary"].describe())

# PySpark
print("\nPYSPARK - df.select('salary').describe():")
pyspark_df.select("salary").describe().show()

# ============================================================================
# 8. COUNT ROWS
# ============================================================================
print("\n8. COUNT ROWS")
print("-" * 80)

# Pandas
print(f"PANDAS - len(df): {len(pandas_df)}")

# PySpark
print(f"PYSPARK - df.count(): {pyspark_df.count()}")

# ============================================================================
# 9. UNIQUE VALUES
# ============================================================================
print("\n9. UNIQUE VALUES (cities)")
print("-" * 80)

# Pandas
print("PANDAS - df['city'].unique():")
print(pandas_df["city"].unique())

# PySpark
print("\nPYSPARK - df.select('city').distinct():")
pyspark_df.select("city").distinct().show()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY DIFFERENCES SUMMARY:")
print("=" * 80)
print("""
1. Execution:
   - Pandas: Eager (immediate)
   - PySpark: Lazy (only executes on actions like .show())

2. Data Display:
   - Pandas: df.head() returns data
   - PySpark: df.show() displays data (doesn't return)

3. Column Selection:
   - Pandas: df['col'] or df[['col1', 'col2']]
   - PySpark: df.select('col') or df.select('col1', 'col2')

4. Filtering:
   - Pandas: df[df['col'] > value]
   - PySpark: df.filter(F.col('col') > value)

5. Adding Columns:
   - Pandas: df['new'] = expression (modifies in-place)
   - PySpark: df.withColumn('new', expression) (returns new DF)

6. Sorting:
   - Pandas: df.sort_values('col')
   - PySpark: df.orderBy('col') or df.orderBy(F.desc('col'))

7. Counting:
   - Pandas: len(df)
   - PySpark: df.count()
""")

spark.stop()
print("\n" + "=" * 80)
print("Script completed!")
print("=" * 80)
