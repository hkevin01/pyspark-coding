"""
Pandas vs PySpark: Joins Comparison
===================================
This script demonstrates different types of joins in both frameworks.
"""

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Sample data for customers
customers_data = {
    "customer_id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
    "city": ["NYC", "LA", "Chicago", "Houston", "Phoenix"]
}

# Sample data for orders
orders_data = {
    "order_id": [101, 102, 103, 104, 105, 106],
    "customer_id": [1, 1, 2, 3, 3, 6],  # Note: customer_id 6 doesn't exist in customers
    "product": ["Laptop", "Mouse", "Keyboard", "Monitor", "Laptop", "Tablet"],
    "amount": [1000, 25, 75, 300, 1200, 500]
}

print("=" * 80)
print("JOINS COMPARISON")
print("=" * 80)

# Create DataFrames
customers_pandas = pd.DataFrame(customers_data)
orders_pandas = pd.DataFrame(orders_data)

spark = SparkSession.builder.appName("Joins").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

customers_pyspark = spark.createDataFrame(customers_pandas)
orders_pyspark = spark.createDataFrame(orders_pandas)

print("\nCUSTOMERS TABLE:")
print(customers_pandas)
print("\nORDERS TABLE:")
print(orders_pandas)

# ============================================================================
# 1. INNER JOIN
# ============================================================================
print("\n" + "=" * 80)
print("1. INNER JOIN (Only matching records)")
print("=" * 80)

# Pandas
print("\nPANDAS - pd.merge(customers, orders, on='customer_id', how='inner'):")
result_pandas = pd.merge(customers_pandas, orders_pandas, on="customer_id", how="inner")
print(result_pandas)

# PySpark
print("\nPYSPARK - customers.join(orders, on='customer_id', how='inner'):")
result_pyspark = customers_pyspark.join(orders_pyspark, on="customer_id", how="inner")
result_pyspark.show()

# ============================================================================
# 2. LEFT JOIN
# ============================================================================
print("\n" + "=" * 80)
print("2. LEFT JOIN (All customers, matching orders)")
print("=" * 80)

# Pandas
print("\nPANDAS - pd.merge(customers, orders, on='customer_id', how='left'):")
result_pandas = pd.merge(customers_pandas, orders_pandas, on="customer_id", how="left")
print(result_pandas)

# PySpark
print("\nPYSPARK - customers.join(orders, on='customer_id', how='left'):")
result_pyspark = customers_pyspark.join(orders_pyspark, on="customer_id", how="left")
result_pyspark.show()

# ============================================================================
# 3. RIGHT JOIN
# ============================================================================
print("\n" + "=" * 80)
print("3. RIGHT JOIN (All orders, matching customers)")
print("=" * 80)

# Pandas
print("\nPANDAS - pd.merge(customers, orders, on='customer_id', how='right'):")
result_pandas = pd.merge(customers_pandas, orders_pandas, on="customer_id", how="right")
print(result_pandas)

# PySpark
print("\nPYSPARK - customers.join(orders, on='customer_id', how='right'):")
result_pyspark = customers_pyspark.join(orders_pyspark, on="customer_id", how="right")
result_pyspark.show()

# ============================================================================
# 4. OUTER (FULL) JOIN
# ============================================================================
print("\n" + "=" * 80)
print("4. OUTER/FULL JOIN (All records from both tables)")
print("=" * 80)

# Pandas
print("\nPANDAS - pd.merge(customers, orders, on='customer_id', how='outer'):")
result_pandas = pd.merge(customers_pandas, orders_pandas, on="customer_id", how="outer")
print(result_pandas)

# PySpark
print("\nPYSPARK - customers.join(orders, on='customer_id', how='outer'):")
result_pyspark = customers_pyspark.join(orders_pyspark, on="customer_id", how="outer")
result_pyspark.show()

# ============================================================================
# 5. JOIN WITH DIFFERENT COLUMN NAMES
# ============================================================================
print("\n" + "=" * 80)
print("5. JOIN WITH DIFFERENT COLUMN NAMES")
print("=" * 80)

# Create tables with different column names
customers_alt_pandas = customers_pandas.rename(columns={"customer_id": "id"})
print("\nCustomers table (id column):")
print(customers_alt_pandas)

customers_alt_pyspark = customers_pyspark.withColumnRenamed("customer_id", "id")

# Pandas
print("\nPANDAS - pd.merge(customers, orders, left_on='id', right_on='customer_id'):")
result_pandas = pd.merge(
    customers_alt_pandas, 
    orders_pandas, 
    left_on="id", 
    right_on="customer_id"
)
print(result_pandas)

# PySpark
print("\nPYSPARK - customers.join(orders, customers['id'] == orders['customer_id']):")
result_pyspark = customers_alt_pyspark.join(
    orders_pyspark, 
    customers_alt_pyspark["id"] == orders_pyspark["customer_id"]
)
result_pyspark.show()

# ============================================================================
# 6. JOIN WITH AGGREGATION
# ============================================================================
print("\n" + "=" * 80)
print("6. JOIN WITH AGGREGATION (Total amount per customer)")
print("=" * 80)

# Pandas
print("\nPANDAS:")
result_pandas = pd.merge(customers_pandas, orders_pandas, on="customer_id", how="left")
summary = result_pandas.groupby(["customer_id", "name", "city"])["amount"].sum().reset_index()
summary.columns = ["customer_id", "name", "city", "total_spent"]
print(summary)

# PySpark
print("\nPYSPARK:")
result_pyspark = customers_pyspark.join(orders_pyspark, on="customer_id", how="left")
summary = result_pyspark.groupBy("customer_id", "name", "city").agg(
    F.sum("amount").alias("total_spent")
).orderBy("customer_id")
summary.show()

# ============================================================================
# 7. COUNT ORDERS PER CUSTOMER
# ============================================================================
print("\n" + "=" * 80)
print("7. COUNT ORDERS PER CUSTOMER")
print("=" * 80)

# Pandas
print("\nPANDAS:")
order_counts = orders_pandas.groupby("customer_id").size().reset_index(name="order_count")
result_pandas = pd.merge(customers_pandas, order_counts, on="customer_id", how="left")
result_pandas["order_count"] = result_pandas["order_count"].fillna(0).astype(int)
print(result_pandas)

# PySpark
print("\nPYSPARK:")
order_counts = orders_pyspark.groupBy("customer_id").agg(
    F.count("order_id").alias("order_count")
)
result_pyspark = customers_pyspark.join(order_counts, on="customer_id", how="left")
result_pyspark = result_pyspark.na.fill({"order_count": 0})
result_pyspark.show()

# ============================================================================
# 8. ANTI JOIN (Customers with NO orders)
# ============================================================================
print("\n" + "=" * 80)
print("8. ANTI JOIN (Customers with NO orders)")
print("=" * 80)

# Pandas - Using merge with indicator
print("\nPANDAS - Using merge with indicator:")
result_pandas = pd.merge(
    customers_pandas, 
    orders_pandas, 
    on="customer_id", 
    how="left", 
    indicator=True
)
no_orders = result_pandas[result_pandas["_merge"] == "left_only"][["customer_id", "name", "city"]]
print(no_orders)

# PySpark - Using left_anti join
print("\nPYSPARK - Using left_anti join:")
no_orders = customers_pyspark.join(orders_pyspark, on="customer_id", how="left_anti")
no_orders.show()

# ============================================================================
# SUMMARY
# ============================================================================
print("\n" + "=" * 80)
print("KEY DIFFERENCES SUMMARY:")
print("=" * 80)
print("""
1. Basic Join Syntax:
   - Pandas: pd.merge(df1, df2, on='key', how='inner')
   - PySpark: df1.join(df2, on='key', how='inner')

2. Join Types (same in both):
   - inner: Only matching records
   - left: All from left, matching from right
   - right: All from right, matching from left
   - outer: All records from both

3. Different Column Names:
   - Pandas: pd.merge(df1, df2, left_on='col1', right_on='col2')
   - PySpark: df1.join(df2, df1['col1'] == df2['col2'])

4. Anti Join (records in left but not in right):
   - Pandas: Use merge with indicator=True, then filter
   - PySpark: df1.join(df2, on='key', how='left_anti')

5. After Join Operations:
   - Both support groupBy, aggregations, filtering
   - Syntax follows their respective patterns

6. PySpark Advantage:
   - Built-in left_anti and left_semi joins
   - Better performance on large datasets
   - Automatic optimization of join strategy
""")

spark.stop()
print("\n" + "=" * 80)
print("Script completed!")
print("=" * 80)
