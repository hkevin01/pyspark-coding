"""
Pandas vs PySpark: Aggregations and GroupBy Comparison
======================================================
This script demonstrates aggregation and groupby operations.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sample sales data
data = {
    "product": ["Laptop", "Mouse", "Keyboard", "Laptop", "Mouse", "Monitor", "Keyboard", "Laptop"],
    "category": ["Electronics", "Accessories", "Accessories", "Electronics", "Accessories", "Electronics", "Accessories", "Electronics"],
    "price": [1000, 25, 75, 1200, 30, 300, 80, 1100],
    "quantity": [2, 10, 5, 1, 15, 3, 8, 2],
    "store": ["NYC", "NYC", "LA", "LA", "Chicago", "Chicago", "NYC", "Houston"]
}

print("=" * 80)
print("AGGREGATIONS AND GROUPBY COMPARISON")
print("=" * 80)

# Create DataFrames
pandas_df = pd.DataFrame(data)
spark = SparkSession.builder.appName("Aggregations").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
pyspark_df = spark.createDataFrame(pandas_df)

print("\nOriginal Data:")
print(pandas_df)

# ============================================================================
# 1. BASIC AGGREGATIONS
# ============================================================================
print("\n" + "=" * 80)
print("1. BASIC AGGREGATIONS")
print("=" * 80)

# Pandas
print("\nPANDAS:")
print(f"Total Quantity: {pandas_df['quantity'].sum()}")
print(f"Average Price: ${pandas_df['price'].mean():.2f}")
print(f"Max Price: ${pandas_df['price'].max()}")
print(f"Min Price: ${pandas_df['price'].min()}")
print(f"Count: {pandas_df['price'].count()}")

# PySpark
print("\nPYSPARK:")
agg_result = pyspark_df.agg(
    F.sum("quantity").alias("total_quantity"),
    F.mean("price").alias("avg_price"),
    F.max("price").alias("max_price"),
    F.min("price").alias("min_price"),
    F.count("price").alias("count")
)
agg_result.show()

# ============================================================================
# 2. GROUPBY WITH SINGLE AGGREGATION
# ============================================================================
print("\n" + "=" * 80)
print("2. GROUPBY WITH SINGLE AGGREGATION (Total quantity by category)")
print("=" * 80)

# Pandas
print("\nPANDAS - df.groupby('category')['quantity'].sum():")
result_pandas = pandas_df.groupby("category")["quantity"].sum()
print(result_pandas)

# PySpark
print("\nPYSPARK - df.groupBy('category').agg(F.sum('quantity')):")
pyspark_df.groupBy("category").agg(
    F.sum("quantity").alias("total_quantity")
).show()

# ============================================================================
# 3. GROUPBY WITH MULTIPLE AGGREGATIONS
# ============================================================================
print("\n" + "=" * 80)
print("3. GROUPBY WITH MULTIPLE AGGREGATIONS (by product)")
print("=" * 80)

# Pandas
print("\nPANDAS:")
result_pandas = pandas_df.groupby("product").agg({
    "quantity": "sum",
    "price": ["mean", "max"]
})
print(result_pandas)

# PySpark
print("\nPYSPARK:")
pyspark_df.groupBy("product").agg(
    F.sum("quantity").alias("total_quantity"),
    F.mean("price").alias("avg_price"),
    F.max("price").alias("max_price")
).show()

# ============================================================================
# 4. GROUPBY WITH MULTIPLE COLUMNS
# ============================================================================
print("\n" + "=" * 80)
print("4. GROUPBY WITH MULTIPLE COLUMNS (category and store)")
print("=" * 80)

# Pandas
print("\nPANDAS:")
result_pandas = pandas_df.groupby(["category", "store"])["quantity"].sum()
print(result_pandas)

# PySpark
print("\nPYSPARK:")
pyspark_df.groupBy("category", "store").agg(
    F.sum("quantity").alias("total_quantity")
).orderBy("category", "store").show()

# ============================================================================
# 5. CALCULATE REVENUE (DERIVED COLUMN + AGGREGATION)
# ============================================================================
print("\n" + "=" * 80)
print("5. CALCULATE REVENUE (price * quantity) BY CATEGORY")
print("=" * 80)

# Pandas
print("\nPANDAS:")
pandas_df_copy = pandas_df.copy()
pandas_df_copy["revenue"] = pandas_df_copy["price"] * pandas_df_copy["quantity"]
revenue_by_category = pandas_df_copy.groupby("category")["revenue"].sum().sort_values(ascending=False)
print(revenue_by_category)

# PySpark
print("\nPYSPARK:")
pyspark_df.withColumn("revenue", F.col("price") * F.col("quantity")).groupBy("category").agg(
    F.sum("revenue").alias("total_revenue")
).orderBy(F.desc("total_revenue")).show()

# ============================================================================
# 6. COUNT UNIQUE PRODUCTS BY STORE
# ============================================================================
print("\n" + "=" * 80)
print("6. COUNT UNIQUE PRODUCTS BY STORE")
print("=" * 80)

# Pandas
print("\nPANDAS - df.groupby('store')['product'].nunique():")
result_pandas = pandas_df.groupby("store")["product"].nunique()
print(result_pandas)

# PySpark
print("\nPYSPARK - df.groupBy('store').agg(F.countDistinct('product')):")
pyspark_df.groupBy("store").agg(
    F.countDistinct("product").alias("unique_products")
).show()

# ============================================================================
# 7. ADVANCED: MULTIPLE AGGREGATIONS WITH RENAMED COLUMNS
# ============================================================================
print("\n" + "=" * 80)
print("7. ADVANCED: COMPLETE SALES SUMMARY BY CATEGORY")
print("=" * 80)

# Pandas
print("\nPANDAS:")
summary_pandas = pandas_df.assign(
    revenue=lambda x: x["price"] * x["quantity"]
).groupby("category").agg({
    "quantity": "sum",
    "price": "mean",
    "revenue": "sum",
    "product": "count"
}).round(2)
summary_pandas.columns = ["total_quantity", "avg_price", "total_revenue", "num_transactions"]
print(summary_pandas)

# PySpark
print("\nPYSPARK:")
pyspark_df.withColumn("revenue", F.col("price") * F.col("quantity")).groupBy("category").agg(
    F.sum("quantity").alias("total_quantity"),
    F.round(F.mean("price"), 2).alias("avg_price"),
    F.sum("revenue").alias("total_revenue"),
    F.count("product").alias("num_transactions")
).show()

# ============================================================================
# 8. TOP N ANALYSIS
# ============================================================================
print("\n" + "=" * 80)
print("8. TOP 3 PRODUCTS BY TOTAL REVENUE")
print("=" * 80)

# Pandas
print("\nPANDAS:")
pandas_df_copy = pandas_df.copy()
pandas_df_copy["revenue"] = pandas_df_copy["price"] * pandas_df_copy["quantity"]
top_products_pandas = pandas_df_copy.groupby("product")["revenue"].sum().sort_values(ascending=False).head(3)
print(top_products_pandas)

# PySpark
print("\nPYSPARK:")
pyspark_df.withColumn("revenue", F.col("price") * F.col("quantity")).groupBy("product").agg(
    F.sum("revenue").alias("total_revenue")
).orderBy(F.desc("total_revenue")).limit(3).show()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY DIFFERENCES SUMMARY:")
print("=" * 80)
print("""
1. Basic Aggregations:
   - Pandas: df['col'].sum(), df['col'].mean()
   - PySpark: df.agg(F.sum('col'), F.mean('col'))

2. GroupBy Single Agg:
   - Pandas: df.groupby('col')['value'].sum()
   - PySpark: df.groupBy('col').agg(F.sum('value'))

3. Multiple Aggregations:
   - Pandas: df.groupby('col').agg({'col1': 'sum', 'col2': 'mean'})
   - PySpark: df.groupBy('col').agg(F.sum('col1'), F.mean('col2'))

4. Column Renaming:
   - Pandas: result.columns = ['new1', 'new2']
   - PySpark: Use .alias() in aggregations

5. Top N:
   - Pandas: df.sort_values(...).head(n)
   - PySpark: df.orderBy(...).limit(n)

6. Count Distinct:
   - Pandas: df.groupby('col')['other'].nunique()
   - PySpark: df.groupBy('col').agg(F.countDistinct('other'))

7. Must use F.col() and F functions in PySpark for all operations!
""")

spark.stop()
print("\n" + "=" * 80)
print("Script completed!")
print("=" * 80)
