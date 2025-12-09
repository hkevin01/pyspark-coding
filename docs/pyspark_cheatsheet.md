# PySpark Quick Reference Cheat Sheet

## SparkSession

```python
from pyspark.sql import SparkSession

# Create session
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Stop session
spark.stop()
```

## Reading Data

```python
from pyspark.sql import functions as F

# CSV
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df = spark.read.option("delimiter", ";").csv("file.csv")

# JSON
df = spark.read.json("path/to/file.json")
df = spark.read.option("multiLine", True).json("file.json")

# Parquet
df = spark.read.parquet("path/to/file.parquet")

# Multiple files
df = spark.read.csv("path/to/directory/")
df = spark.read.csv(["file1.csv", "file2.csv"])

# With schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])
df = spark.read.schema(schema).csv("file.csv")
```

## Writing Data

```python
# CSV
df.write.csv("output_path", header=True, mode="overwrite")

# JSON
df.write.json("output_path", mode="overwrite")

# Parquet
df.write.parquet("output_path", mode="overwrite")

# With partitioning
df.write.partitionBy("year", "month").parquet("output_path")

# Modes: overwrite, append, error (default), ignore
df.write.mode("append").csv("output_path")
```

## DataFrame Operations

```python
# Show data
df.show(5)          # Show 5 rows
df.show(truncate=False)  # Show full content

# Schema
df.printSchema()
df.schema
df.dtypes
df.columns

# Count
df.count()

# Describe
df.describe().show()
df.describe("age", "salary").show()

# Select columns
df.select("col1", "col2")
df.select(F.col("col1"), F.col("col2"))
df.select("*")

# Drop columns
df.drop("col1", "col2")

# Rename columns
df.withColumnRenamed("old_name", "new_name")

# Add column
df.withColumn("new_col", F.col("old_col") * 2)
df.withColumn("constant", F.lit("value"))

# Filter/Where (same thing)
df.filter(F.col("age") > 25)
df.filter("age > 25")
df.where(F.col("age") > 25)

# Multiple conditions
df.filter((F.col("age") > 25) & (F.col("salary") > 50000))
df.filter((F.col("age") > 25) | (F.col("salary") > 50000))

# Distinct
df.distinct()
df.select("col1").distinct()

# Drop duplicates
df.dropDuplicates()
df.dropDuplicates(["col1", "col2"])

# Sort
df.orderBy("age")
df.orderBy(F.col("age").desc())
df.orderBy(["age", "name"], ascending=[True, False])
df.sort("age")

# Limit
df.limit(10)

# Sample
df.sample(fraction=0.1, seed=42)
```

## Handling Nulls

```python
# Check for nulls
df.filter(F.col("col1").isNull())
df.filter(F.col("col1").isNotNull())

# Count nulls
from pyspark.sql.functions import col, sum as spark_sum
df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

# Drop nulls
df.dropna()                    # Drop rows with any null
df.dropna(how="all")           # Drop rows where all values are null
df.dropna(subset=["col1"])     # Drop rows with null in specific columns
df.dropna(thresh=2)            # Keep rows with at least 2 non-null values

# Fill nulls
df.fillna(0)                   # Fill all numeric nulls with 0
df.fillna("unknown")           # Fill all string nulls
df.fillna({"col1": 0, "col2": "unknown"})  # Column-specific

# Replace values
df.replace(["old1", "old2"], ["new1", "new2"], subset=["col1"])
```

## Aggregations

```python
from pyspark.sql import functions as F

# GroupBy
df.groupBy("category").count()
df.groupBy("category").sum("amount")
df.groupBy("category").avg("amount")
df.groupBy("category").max("amount")
df.groupBy("category").min("amount")

# Multiple aggregations
df.groupBy("category").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("average"),
    F.count("*").alias("count"),
    F.min("amount").alias("minimum"),
    F.max("amount").alias("maximum")
)

# Aggregate without groupBy
df.agg(F.sum("amount"), F.avg("amount"))
```

## Common Functions

```python
from pyspark.sql import functions as F

# String functions
F.concat(F.col("first_name"), F.lit(" "), F.col("last_name"))
F.upper(F.col("name"))
F.lower(F.col("name"))
F.trim(F.col("name"))
F.substring(F.col("name"), 1, 3)
F.length(F.col("name"))
F.split(F.col("email"), "@")
F.regexp_replace(F.col("text"), "pattern", "replacement")

# Numeric functions
F.round(F.col("price"), 2)
F.ceil(F.col("value"))
F.floor(F.col("value"))
F.abs(F.col("value"))
F.sqrt(F.col("value"))

# Date/Time functions
F.current_date()
F.current_timestamp()
F.to_date(F.col("date_string"), "yyyy-MM-dd")
F.to_timestamp(F.col("timestamp_string"))
F.year(F.col("date"))
F.month(F.col("date"))
F.dayofmonth(F.col("date"))
F.dayofweek(F.col("date"))
F.datediff(F.col("end_date"), F.col("start_date"))
F.date_add(F.col("date"), 7)
F.date_sub(F.col("date"), 7)

# Conditional
F.when(F.col("age") < 18, "Minor") \
 .when(F.col("age") < 65, "Adult") \
 .otherwise("Senior")

# Null handling
F.coalesce(F.col("col1"), F.col("col2"), F.lit("default"))
F.isnan(F.col("value"))
F.isnull(F.col("value"))

# Type casting
F.col("age").cast("integer")
F.col("age").cast(IntegerType())
```

## Joins

```python
# Join types: inner, left, right, outer (full), left_semi, left_anti, cross

# Inner join (default)
df1.join(df2, df1.id == df2.id)
df1.join(df2, "id")  # If column name is same

# Left join
df1.join(df2, df1.id == df2.id, "left")

# Right join
df1.join(df2, df1.id == df2.id, "right")

# Full outer join
df1.join(df2, df1.id == df2.id, "outer")

# Multiple conditions
df1.join(df2, (df1.id == df2.id) & (df1.type == df2.type))

# Cross join
df1.crossJoin(df2)
```

## Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Define window
window = Window.partitionBy("category").orderBy("date")

# Row number
df.withColumn("row_num", F.row_number().over(window))

# Rank
df.withColumn("rank", F.rank().over(window))
df.withColumn("dense_rank", F.dense_rank().over(window))

# Aggregate over window
window_agg = Window.partitionBy("category")
df.withColumn("total", F.sum("amount").over(window_agg))
df.withColumn("avg", F.avg("amount").over(window_agg))

# Moving average
window_range = Window.partitionBy("category").orderBy("date").rowsBetween(-2, 0)
df.withColumn("moving_avg", F.avg("amount").over(window_range))

# Lag and Lead
df.withColumn("prev_value", F.lag("amount", 1).over(window))
df.withColumn("next_value", F.lead("amount", 1).over(window))

# Cumulative sum
df.withColumn("cumsum", F.sum("amount").over(window))
```

## UDFs (User Defined Functions)

```python
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# Define UDF
def upper_case(s):
    return s.upper() if s else None

upper_udf = udf(upper_case, StringType())

# Use UDF
df.withColumn("upper_name", upper_udf(F.col("name")))

# Lambda UDF
square = udf(lambda x: x * x, IntegerType())
df.withColumn("squared", square(F.col("value")))
```

## SQL Queries

```python
# Register DataFrame as temp view
df.createOrReplaceTempView("my_table")

# Run SQL query
result = spark.sql("""
    SELECT category, SUM(amount) as total
    FROM my_table
    WHERE amount > 100
    GROUP BY category
    ORDER BY total DESC
""")

result.show()
```

## Performance Tips

```python
# Cache DataFrame
df.cache()
df.persist()

# Unpersist
df.unpersist()

# Repartition
df.repartition(10)
df.repartition("col1")

# Coalesce (reduce partitions)
df.coalesce(1)

# Broadcast join for small tables
from pyspark.sql.functions import broadcast
df1.join(broadcast(df2), "id")

# Explain execution plan
df.explain()
df.explain(extended=True)
```

## Common Patterns

```python
# Pivot
df.groupBy("year").pivot("category").sum("amount")

# Unpivot (melt)
from pyspark.sql.functions import expr
df.selectExpr("id", "stack(3, 'col1', col1, 'col2', col2, 'col3', col3) as (key, value)")

# Union
df1.union(df2)
df1.unionByName(df2)  # Match by column names

# Intersect
df1.intersect(df2)

# Except (difference)
df1.subtract(df2)

# Collect results (use cautiously)
rows = df.collect()  # List of Row objects
first_row = df.first()
df.take(5)  # First 5 rows
```

## Data Quality Checks

```python
# Check schema
df.printSchema()

# Check for duplicates
df.count() - df.distinct().count()

# Check column stats
df.describe().show()

# Check null percentages
total_count = df.count()
for col in df.columns:
    null_count = df.filter(F.col(col).isNull()).count()
    print(f"{col}: {null_count/total_count*100:.2f}% nulls")

# Data profiling
df.select([F.approx_count_distinct(c).alias(c) for c in df.columns]).show()
```
