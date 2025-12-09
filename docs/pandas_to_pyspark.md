# From Pandas to PySpark: A Practical Guide

## Introduction

If you're coming from Pandas, you already have a strong foundation for PySpark! This guide shows you how your Pandas knowledge translates to PySpark, with side-by-side examples and key differences.

---

## Table of Contents
1. [Core Concepts Comparison](#core-concepts-comparison)
2. [Reading Data](#reading-data)
3. [Basic Operations](#basic-operations)
4. [Filtering and Selecting](#filtering-and-selecting)
5. [Aggregations and GroupBy](#aggregations-and-groupby)
6. [Joins](#joins)
7. [Working with Columns](#working-with-columns)
8. [Handling Missing Data](#handling-missing-data)
9. [String Operations](#string-operations)
10. [Date/Time Operations](#datetime-operations)
11. [Writing Data](#writing-data)
12. [Performance Tips](#performance-tips)
13. [Key Differences Summary](#key-differences-summary)

---

## Core Concepts Comparison

### Philosophy Differences

| Concept | Pandas | PySpark |
|---------|--------|---------|
| **Execution** | Eager (immediate) | Lazy (optimized plan) |
| **Data Structure** | DataFrame (mutable) | DataFrame (immutable) |
| **Processing** | Single machine | Distributed cluster |
| **Memory** | All data in RAM | Partitioned across nodes |
| **Modifications** | In-place possible | Always creates new DF |

### Basic Setup

```python
# Pandas
import pandas as pd
df = pd.read_csv("data.csv")

# PySpark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

**Key Difference:** PySpark requires SparkSession initialization. Import `functions as F` for transformations.

---

## Reading Data

### Reading CSV Files

```python
# Pandas - Excel files you're familiar with
df = pd.read_csv("sales.csv")
df = pd.read_csv("sales.csv", sep=";", encoding="utf-8")
df = pd.read_excel("sales.xlsx", sheet_name="Sheet1")

# PySpark - Similar but different parameters
df = spark.read.csv("sales.csv", header=True, inferSchema=True)
df = spark.read.csv("sales.csv", sep=";", encoding="utf-8", header=True)
# For Excel, need to use pandas or external libraries
# PySpark focuses on big data formats (CSV, Parquet, JSON)
```

### Reading Multiple Files

```python
# Pandas - Loop through files
import glob
dfs = []
for file in glob.glob("data/*.csv"):
    dfs.append(pd.read_csv(file))
combined = pd.concat(dfs, ignore_index=True)

# PySpark - Built-in wildcard support (much easier!)
df = spark.read.csv("data/*.csv", header=True, inferSchema=True)
# Or specific pattern
df = spark.read.csv("data/sales_*.csv", header=True, inferSchema=True)
```

### Reading Other Formats

```python
# Pandas
df = pd.read_json("data.json")
df = pd.read_parquet("data.parquet")
df = pd.read_sql("SELECT * FROM table", connection)

# PySpark - Very similar
df = spark.read.json("data.json")
df = spark.read.parquet("data.parquet")
df = spark.read.jdbc(url="jdbc:postgresql://...", table="table", properties={...})
```

---

## Basic Operations

### Viewing Data

```python
# Pandas
df.head()           # First 5 rows
df.head(10)         # First 10 rows
df.tail()           # Last 5 rows
df.sample(5)        # Random 5 rows
df.shape            # (rows, columns)
df.columns          # Column names
df.dtypes           # Data types
df.info()           # Summary info
df.describe()       # Statistics

# PySpark - Similar but different methods
df.show()           # First 20 rows (default)
df.show(10)         # First 10 rows
df.show(10, False)  # Without truncation
# No direct tail() - need to work around
df.sample(False, 0.1).show()  # 10% random sample
df.count()          # Row count (returns number)
df.columns          # Column names (same!)
df.dtypes           # Data types (same!)
df.printSchema()    # Schema info
df.describe().show() # Statistics
```

**Important:** PySpark's `show()` just displays data. Use `count()` for actual row count (triggers computation).

### Selecting Columns

```python
# Pandas - Multiple ways
df["name"]                    # Single column (Series)
df[["name", "age"]]           # Multiple columns (DataFrame)
df.loc[:, "name":"age"]       # Range of columns

# PySpark - Similar but more explicit
df.select("name")             # Single column (DataFrame)
df.select("name", "age")      # Multiple columns
df.select(F.col("name"), F.col("age"))  # Using F.col()
df.select("*")                # All columns
df.select(df.columns[0:3])    # First 3 columns by position
```

---

## Filtering and Selecting

### Simple Filtering

```python
# Pandas
df[df["age"] > 30]
df[df["name"] == "John"]
df[df["salary"] >= 50000]

# PySpark - Use filter() or where() (they're the same!)
df.filter(df["age"] > 30)
df.filter(F.col("age") > 30)     # Preferred way
df.where(df["name"] == "John")    # where() is alias for filter()
df.filter(F.col("salary") >= 50000)
```

### Multiple Conditions

```python
# Pandas
df[(df["age"] > 30) & (df["salary"] > 50000)]          # AND
df[(df["city"] == "NYC") | (df["city"] == "LA")]       # OR
df[~(df["status"] == "inactive")]                       # NOT

# PySpark - Same operators!
df.filter((F.col("age") > 30) & (F.col("salary") > 50000))     # AND
df.filter((F.col("city") == "NYC") | (F.col("city") == "LA"))  # OR
df.filter(~(F.col("status") == "inactive"))                     # NOT
```

### Filtering with Lists

```python
# Pandas
cities = ["NYC", "LA", "Chicago"]
df[df["city"].isin(cities)]

# PySpark - Exactly the same concept!
cities = ["NYC", "LA", "Chicago"]
df.filter(F.col("city").isin(cities))
```

### String Filtering

```python
# Pandas
df[df["name"].str.contains("John")]
df[df["email"].str.startswith("admin")]
df[df["product"].str.endswith(".pdf")]

# PySpark - Similar with F.col()
df.filter(F.col("name").contains("John"))
df.filter(F.col("email").startswith("admin"))
df.filter(F.col("product").endswith(".pdf"))
```

---

## Aggregations and GroupBy

### Basic Aggregations

```python
# Pandas
df["salary"].sum()
df["salary"].mean()
df["age"].max()
df["age"].min()
df["id"].count()

# PySpark - Must use agg() with F functions
from pyspark.sql.functions import sum, mean, max, min, count

df.select(F.sum("salary")).show()
df.select(F.mean("salary")).show()
df.select(F.max("age")).show()
df.select(F.min("age")).show()
df.select(F.count("id")).show()

# Or use agg() for multiple
df.agg(
    F.sum("salary").alias("total_salary"),
    F.mean("age").alias("avg_age"),
    F.count("id").alias("total_count")
).show()
```

### GroupBy Operations

```python
# Pandas - Very familiar if you've used Excel Pivot Tables!
df.groupby("department")["salary"].sum()
df.groupby("department")["salary"].mean()
df.groupby("department").size()
df.groupby(["department", "city"])["salary"].sum()

# PySpark - Similar structure, must use agg()
df.groupBy("department").agg(F.sum("salary")).show()
df.groupBy("department").agg(F.mean("salary")).show()
df.groupBy("department").count().show()
df.groupBy("department", "city").agg(F.sum("salary")).show()
```

### Multiple Aggregations

```python
# Pandas
df.groupby("department").agg({
    "salary": ["sum", "mean", "max"],
    "age": "mean",
    "id": "count"
})

# PySpark - More explicit
df.groupBy("department").agg(
    F.sum("salary").alias("total_salary"),
    F.mean("salary").alias("avg_salary"),
    F.max("salary").alias("max_salary"),
    F.mean("age").alias("avg_age"),
    F.count("id").alias("employee_count")
).show()
```

### Advanced GroupBy Example

```python
# Pandas - Sales analysis (like Excel Pivot Tables)
sales_df.groupby("product_category").agg({
    "revenue": "sum",
    "quantity": "sum",
    "order_id": "count"
}).round(2)

# PySpark - Same logic, different syntax
sales_df.groupBy("product_category").agg(
    F.round(F.sum("revenue"), 2).alias("total_revenue"),
    F.sum("quantity").alias("total_quantity"),
    F.count("order_id").alias("num_orders")
).show()
```

---

## Joins

### Inner Join (Default)

```python
# Pandas
result = pd.merge(customers, orders, on="customer_id")
# Or
result = pd.merge(customers, orders, left_on="id", right_on="customer_id")

# PySpark - Very similar!
result = customers.join(orders, on="customer_id", how="inner")
# Or more explicit
result = customers.join(orders, customers["id"] == orders["customer_id"], "inner")
```

### Different Join Types

```python
# Pandas
pd.merge(df1, df2, on="key", how="inner")    # Inner join
pd.merge(df1, df2, on="key", how="left")     # Left join
pd.merge(df1, df2, on="key", how="right")    # Right join
pd.merge(df1, df2, on="key", how="outer")    # Full outer join

# PySpark - Exactly the same logic!
df1.join(df2, on="key", how="inner")         # Inner join
df1.join(df2, on="key", how="left")          # Left join
df1.join(df2, on="key", how="right")         # Right join
df1.join(df2, on="key", how="outer")         # Full outer join
```

### Join with Different Column Names

```python
# Pandas
customers = pd.DataFrame({"customer_id": [1, 2, 3], "name": ["A", "B", "C"]})
orders = pd.DataFrame({"cust_id": [1, 1, 2], "amount": [100, 200, 150]})
result = pd.merge(customers, orders, left_on="customer_id", right_on="cust_id")

# PySpark - Same concept
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

result = customers.join(
    orders, 
    customers["customer_id"] == orders["cust_id"],
    "inner"
)
```

---

## Working with Columns

### Adding New Columns

```python
# Pandas - Familiar from Excel formulas!
df["total"] = df["price"] * df["quantity"]
df["discount_price"] = df["price"] * 0.9
df["full_name"] = df["first_name"] + " " + df["last_name"]

# PySpark - Use withColumn()
df = df.withColumn("total", F.col("price") * F.col("quantity"))
df = df.withColumn("discount_price", F.col("price") * 0.9)
df = df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
```

### Conditional Columns (Like Excel IF)

```python
# Pandas - Like IF() in Excel
df["category"] = df["age"].apply(lambda x: "Adult" if x >= 18 else "Minor")
# Or with numpy
import numpy as np
df["category"] = np.where(df["age"] >= 18, "Adult", "Minor")

# PySpark - Use when().otherwise()
df = df.withColumn(
    "category",
    F.when(F.col("age") >= 18, "Adult").otherwise("Minor")
)
```

### Multiple Conditions (Like Nested IFs)

```python
# Pandas - Like nested IF() in Excel
def categorize_age(age):
    if age < 18:
        return "Minor"
    elif age < 65:
        return "Adult"
    else:
        return "Senior"
df["age_group"] = df["age"].apply(categorize_age)

# PySpark - Chain when() statements
df = df.withColumn(
    "age_group",
    F.when(F.col("age") < 18, "Minor")
     .when(F.col("age") < 65, "Adult")
     .otherwise("Senior")
)
```

### Renaming Columns

```python
# Pandas
df.rename(columns={"old_name": "new_name"}, inplace=True)
df.rename(columns={"col1": "new1", "col2": "new2"}, inplace=True)

# PySpark - Use withColumnRenamed()
df = df.withColumnRenamed("old_name", "new_name")
# For multiple renames, chain them
df = df.withColumnRenamed("col1", "new1").withColumnRenamed("col2", "new2")
```

### Dropping Columns

```python
# Pandas
df.drop(columns=["col1", "col2"], inplace=True)
df.drop("col1", axis=1, inplace=True)

# PySpark
df = df.drop("col1", "col2")  # Can list multiple
df = df.drop("col1")
```

---

## Handling Missing Data

### Detecting Missing Values

```python
# Pandas
df.isnull()
df.isnull().sum()           # Count nulls per column
df["age"].isnull().sum()    # Nulls in specific column

# PySpark
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()
# Or simpler for one column
df.filter(F.col("age").isNull()).count()
```

### Dropping Missing Values

```python
# Pandas
df.dropna()                    # Drop rows with any null
df.dropna(subset=["age"])      # Drop if age is null
df.dropna(thresh=2)            # Drop if < 2 non-null values

# PySpark - Very similar!
df.na.drop()                   # Drop rows with any null
df.na.drop(subset=["age"])     # Drop if age is null
df.na.drop(thresh=2)           # Drop if < 2 non-null values
```

### Filling Missing Values

```python
# Pandas
df.fillna(0)                                  # Fill all nulls with 0
df.fillna({"age": 0, "name": "Unknown"})      # Different values per column
df["age"].fillna(df["age"].mean())            # Fill with mean

# PySpark
df.na.fill(0)                                 # Fill all nulls with 0
df.na.fill({"age": 0, "name": "Unknown"})     # Different values per column
# Fill with mean (more complex)
mean_age = df.select(F.mean("age")).first()[0]
df = df.na.fill({"age": mean_age})
```

---

## String Operations

### Changing Case

```python
# Pandas
df["name"].str.upper()
df["name"].str.lower()
df["name"].str.title()

# PySpark
df.withColumn("name_upper", F.upper(F.col("name")))
df.withColumn("name_lower", F.lower(F.col("name")))
df.withColumn("name_title", F.initcap(F.col("name")))  # Title case
```

### String Trimming

```python
# Pandas
df["name"].str.strip()        # Remove leading/trailing spaces
df["name"].str.lstrip()       # Remove leading spaces
df["name"].str.rstrip()       # Remove trailing spaces

# PySpark - Same concepts
df.withColumn("name_clean", F.trim(F.col("name")))
df.withColumn("name_clean", F.ltrim(F.col("name")))
df.withColumn("name_clean", F.rtrim(F.col("name")))
```

### String Replacement

```python
# Pandas
df["text"].str.replace("old", "new")
df["phone"].str.replace(r"\D", "", regex=True)  # Remove non-digits

# PySpark
df.withColumn("text_new", F.regexp_replace(F.col("text"), "old", "new"))
df.withColumn("phone_clean", F.regexp_replace(F.col("phone"), r"\D", ""))
```

### String Splitting

```python
# Pandas
df["email"].str.split("@", expand=True)
df["full_name"].str.split(" ", n=1, expand=True)

# PySpark
df.withColumn("email_parts", F.split(F.col("email"), "@"))
# To get specific parts
df.withColumn("username", F.split(F.col("email"), "@")[0])
df.withColumn("domain", F.split(F.col("email"), "@")[1])
```

---

## Date/Time Operations

### Parsing Dates

```python
# Pandas
df["date"] = pd.to_datetime(df["date_string"])
df["date"] = pd.to_datetime(df["date_string"], format="%Y-%m-%d")

# PySpark
df = df.withColumn("date", F.to_date(F.col("date_string"), "yyyy-MM-dd"))
df = df.withColumn("timestamp", F.to_timestamp(F.col("datetime_string"), "yyyy-MM-dd HH:mm:ss"))
```

### Extracting Date Parts

```python
# Pandas
df["year"] = df["date"].dt.year
df["month"] = df["date"].dt.month
df["day"] = df["date"].dt.day
df["dayofweek"] = df["date"].dt.dayofweek

# PySpark - Similar with F functions
df = df.withColumn("year", F.year(F.col("date")))
df = df.withColumn("month", F.month(F.col("date")))
df = df.withColumn("day", F.dayofmonth(F.col("date")))
df = df.withColumn("dayofweek", F.dayofweek(F.col("date")))
```

### Date Arithmetic

```python
# Pandas
df["date"] + pd.Timedelta(days=7)
df["date"] - pd.Timedelta(days=30)
(df["end_date"] - df["start_date"]).dt.days

# PySpark
df.withColumn("date_plus_7", F.date_add(F.col("date"), 7))
df.withColumn("date_minus_30", F.date_sub(F.col("date"), 30))
df.withColumn("days_diff", F.datediff(F.col("end_date"), F.col("start_date")))
```

### Current Date/Time

```python
# Pandas
pd.Timestamp.now()
pd.Timestamp.today()

# PySpark
df.withColumn("current_timestamp", F.current_timestamp())
df.withColumn("current_date", F.current_date())
```

---

## Writing Data

### Writing CSV

```python
# Pandas
df.to_csv("output.csv", index=False)
df.to_csv("output.csv", index=False, sep="|")
df.to_excel("output.xlsx", index=False)

# PySpark - No index in PySpark (distributed data)
df.write.csv("output.csv", header=True, mode="overwrite")
df.write.csv("output.csv", header=True, sep="|", mode="overwrite")
# For Excel, convert to Pandas first (only for small data!)
df.toPandas().to_excel("output.xlsx", index=False)
```

### Write Modes

```python
# Pandas - Use 'mode' parameter
df.to_csv("file.csv", mode="w")   # Overwrite
df.to_csv("file.csv", mode="a")   # Append

# PySpark - Use 'mode' parameter
df.write.csv("file.csv", mode="overwrite")  # Overwrite
df.write.csv("file.csv", mode="append")     # Append
df.write.csv("file.csv", mode="ignore")     # Skip if exists
df.write.csv("file.csv", mode="error")      # Error if exists (default)
```

### Writing Parquet (Recommended for Big Data!)

```python
# Pandas
df.to_parquet("output.parquet")

# PySpark - Much better for large data!
df.write.parquet("output.parquet", mode="overwrite")
df.write.parquet("output.parquet", mode="overwrite", partitionBy=["year", "month"])
```

### Writing with Partitions (PySpark Advantage!)

```python
# Pandas - No built-in partitioning (would need manual loops)
for year in df["year"].unique():
    year_df = df[df["year"] == year]
    year_df.to_parquet(f"output/year={year}/data.parquet")

# PySpark - Built-in partitioning!
df.write.parquet("output", mode="overwrite", partitionBy=["year", "month"])
# Creates directory structure: output/year=2024/month=12/data.parquet
```

---

## Performance Tips

### Pandas → PySpark Migration Tips

#### 1. **Avoid .toPandas() on Large Data**
```python
# ❌ BAD - Brings all data to driver, defeats purpose of PySpark
pandas_df = spark_df.toPandas()

# ✅ GOOD - Work with PySpark DataFrame
result = spark_df.filter(...).groupBy(...).agg(...)
```

#### 2. **Cache/Persist When Reusing**
```python
# Pandas - Already in memory
df.describe()
df.groupby("col").sum()

# PySpark - Cache if using multiple times
df.cache()  # Or df.persist()
df.describe()
df.groupBy("col").sum()
df.unpersist()  # Clean up when done
```

#### 3. **Use Built-in Functions, Not UDFs**
```python
# ❌ SLOW - UDF (User Defined Function)
from pyspark.sql.types import StringType
def categorize(age):
    return "Adult" if age >= 18 else "Minor"
categorize_udf = F.udf(categorize, StringType())
df = df.withColumn("category", categorize_udf(F.col("age")))

# ✅ FAST - Built-in functions
df = df.withColumn("category", F.when(F.col("age") >= 18, "Adult").otherwise("Minor"))
```

#### 4. **Avoid Collect on Large Data**
```python
# ❌ BAD - Brings all data to driver
data = df.collect()

# ✅ GOOD - Process in distributed way
result = df.filter(...).groupBy(...).agg(...).show()
# Or write to storage
result.write.parquet("output")
```

#### 5. **Leverage Partitioning**
```python
# Pandas - Single file
df.to_csv("large_file.csv")

# PySpark - Partitioned for parallel processing
df.repartition(10).write.csv("output")  # 10 partitions
df.write.partitionBy("year", "month").parquet("output")
```

---

## Key Differences Summary

### Execution Model

| Aspect | Pandas | PySpark |
|--------|--------|---------|
| **When it runs** | Immediately (eager) | When action called (lazy) |
| **Optimization** | None | Catalyst optimizer |
| **Example** | `df = df[df["age"] > 30]` runs NOW | `df = df.filter(...)` doesn't run until `.show()` |

**PySpark Lazy Evaluation Example:**
```python
# These don't execute yet (transformations)
df2 = df.filter(F.col("age") > 30)
df3 = df2.groupBy("city").count()

# This triggers execution (action)
df3.show()  # NOW everything runs with optimized plan
```

### Data Mutability

```python
# Pandas - Can modify in place
df["new_col"] = 100          # Adds column
df.drop("col", axis=1, inplace=True)  # Removes column

# PySpark - Always returns new DataFrame
df = df.withColumn("new_col", F.lit(100))  # Returns new DF
df = df.drop("col")                         # Returns new DF
```

### Memory Model

```python
# Pandas - All in memory on one machine
df = pd.read_csv("big_file.csv")  # Loads all into RAM

# PySpark - Distributed across cluster
df = spark.read.csv("big_file.csv")  # Loads partitioned across nodes
```

### Common Pitfalls for Pandas Users

#### 1. **Forgetting to Trigger Actions**
```python
# Pandas
result = df[df["age"] > 30].groupby("city").size()
print(result)  # Works immediately

# PySpark - Need action!
result = df.filter(F.col("age") > 30).groupBy("city").count()
# Nothing happened yet!
result.show()  # NOW it executes
```

#### 2. **Trying to Use .loc/.iloc**
```python
# Pandas
df.loc[0]          # First row
df.loc[0, "name"]  # Specific cell

# PySpark - No direct row indexing (distributed data!)
# Use filter instead
df.filter(F.col("id") == 1).show()
```

#### 3. **Expecting Immediate Feedback**
```python
# Pandas - See results immediately
df["age"] = df["age"] + 1
print(df.head())  # See change now

# PySpark - Chain transformations, then action
df = df.withColumn("age", F.col("age") + 1)
df.show()  # Triggers execution
```

---

## Complete Example: Sales Analysis

Let's do a complete analysis comparing both approaches.

### Scenario
Analyze sales data from Excel/CSV: calculate total revenue per category, find top products.

### Pandas Approach

```python
import pandas as pd

# 1. Read data
sales = pd.read_csv("sales.csv")
# or from Excel
# sales = pd.read_excel("sales.xlsx", sheet_name="Sales")

# 2. Data cleaning
sales = sales.dropna(subset=["product", "revenue"])
sales["revenue"] = sales["revenue"].astype(float)
sales["sale_date"] = pd.to_datetime(sales["sale_date"])

# 3. Add calculated column
sales["year"] = sales["sale_date"].dt.year

# 4. Group by category
category_revenue = sales.groupby("category").agg({
    "revenue": "sum",
    "order_id": "count"
}).round(2)
category_revenue.columns = ["total_revenue", "num_orders"]

# 5. Top 10 products
top_products = sales.groupby("product")["revenue"].sum().sort_values(ascending=False).head(10)

# 6. Save results
category_revenue.to_excel("category_summary.xlsx")
top_products.to_excel("top_products.xlsx")

print(category_revenue)
print(top_products)
```

### PySpark Approach

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 0. Initialize Spark
spark = SparkSession.builder.appName("SalesAnalysis").getOrCreate()

# 1. Read data
sales = spark.read.csv("sales.csv", header=True, inferSchema=True)
# Can read multiple files at once!
# sales = spark.read.csv("sales_*.csv", header=True, inferSchema=True)

# 2. Data cleaning
sales = sales.na.drop(subset=["product", "revenue"])
sales = sales.withColumn("revenue", F.col("revenue").cast("double"))
sales = sales.withColumn("sale_date", F.to_date(F.col("sale_date"), "yyyy-MM-dd"))

# 3. Add calculated column
sales = sales.withColumn("year", F.year(F.col("sale_date")))

# 4. Group by category
category_revenue = sales.groupBy("category").agg(
    F.round(F.sum("revenue"), 2).alias("total_revenue"),
    F.count("order_id").alias("num_orders")
)

# 5. Top 10 products
top_products = sales.groupBy("product").agg(
    F.sum("revenue").alias("total_revenue")
).orderBy(F.desc("total_revenue")).limit(10)

# 6. Save results (PySpark writes distributed files)
category_revenue.write.csv("category_summary", header=True, mode="overwrite")
top_products.write.csv("top_products", header=True, mode="overwrite")

# Or convert to Pandas for Excel (only if small!)
if category_revenue.count() < 10000:
    category_revenue.toPandas().to_excel("category_summary.xlsx", index=False)
    top_products.toPandas().to_excel("top_products.xlsx", index=False)

category_revenue.show()
top_products.show()
```

---

## When to Use Which?

### Use Pandas When:
- ✅ Data < 10 GB (fits in RAM)
- ✅ Working with Excel files
- ✅ Quick exploratory analysis
- ✅ Prototyping and testing
- ✅ Need immediate feedback
- ✅ Data science on small datasets

### Use PySpark When:
- ✅ Data > 100 GB (doesn't fit in RAM)
- ✅ Processing takes > 1 hour in Pandas
- ✅ Working with distributed storage (HDFS, S3)
- ✅ ETL pipelines on big data
- ✅ Need horizontal scalability
- ✅ Production big data workflows

### Hybrid Approach (Best Practice!)
```python
# Use PySpark for heavy lifting
large_df = spark.read.parquet("s3://big-data/")
aggregated = large_df.groupBy("category").agg(
    F.sum("revenue").alias("total")
).orderBy(F.desc("total")).limit(100)

# Convert to Pandas for visualization/Excel
small_df = aggregated.toPandas()

# Use Pandas for final formatting and Excel export
small_df.to_excel("summary.xlsx", index=False)

# Or use Pandas plotting
import matplotlib.pyplot as plt
small_df.plot(x="category", y="total", kind="bar")
plt.show()
```

---

## Quick Reference Cheat Sheet

### Common Operations Translation

| Task | Pandas | PySpark |
|------|--------|---------|
| Read CSV | `pd.read_csv()` | `spark.read.csv()` |
| View data | `.head()` | `.show()` |
| Filter rows | `df[df["age"] > 30]` | `df.filter(F.col("age") > 30)` |
| Select columns | `df[["col1", "col2"]]` | `df.select("col1", "col2")` |
| Add column | `df["new"] = df["a"] + df["b"]` | `df.withColumn("new", F.col("a") + F.col("b"))` |
| Group by | `df.groupby("col").sum()` | `df.groupBy("col").agg(F.sum(...))` |
| Sort | `df.sort_values("col")` | `df.orderBy("col")` |
| Join | `pd.merge(df1, df2, on="key")` | `df1.join(df2, on="key")` |
| Drop nulls | `df.dropna()` | `df.na.drop()` |
| Fill nulls | `df.fillna(0)` | `df.na.fill(0)` |
| Distinct | `df.drop_duplicates()` | `df.distinct()` |
| Count rows | `len(df)` | `df.count()` |
| Column names | `df.columns` | `df.columns` |
| Save CSV | `df.to_csv()` | `df.write.csv()` |

---

## Final Tips for Pandas Users

1. **Think Declarative, Not Procedural**
   - Pandas: "Do this step, then do that"
   - PySpark: "Here's what I want, you optimize how"

2. **Embrace Immutability**
   - Always reassign: `df = df.withColumn(...)`
   - No `inplace=True` in PySpark

3. **Learn to Love Lazy Evaluation**
   - Build your pipeline with transformations
   - Trigger with actions (`.show()`, `.count()`, `.write()`)
   - PySpark optimizes the entire plan

4. **Import Functions as F**
   - `from pyspark.sql import functions as F`
   - Use `F.col()`, `F.sum()`, `F.when()`, etc.

5. **Cache Strategically**
   - If reusing a DataFrame multiple times: `.cache()`
   - Don't forget to `.unpersist()` when done

6. **Avoid Python UDFs**
   - Use built-in PySpark functions when possible
   - UDFs are slow (Python ↔ JVM serialization)

7. **Start Small, Then Scale**
   - Test on `.limit(1000)` sample
   - Once working, run on full data

8. **Use Parquet, Not CSV**
   - Parquet is columnar and compressed
   - Much faster to read/write in PySpark

---

## Conclusion

You already know the concepts from Pandas! PySpark is mostly about:
- Learning the slightly different syntax
- Understanding lazy evaluation
- Working with immutable DataFrames
- Using distributed processing advantages

**Your Pandas experience is 80% of what you need for PySpark!**

The remaining 20% is:
- Learning `F.col()` and PySpark functions
- Understanding when transformations run
- Knowing performance best practices

Start practicing with the examples in the `notebooks/` folder, and you'll be productive with PySpark in no time!
