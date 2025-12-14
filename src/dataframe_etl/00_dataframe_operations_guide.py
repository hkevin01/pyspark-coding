"""
PySpark DataFrame Operations - Comprehensive Guide
==================================================

This module provides detailed examples of ALL common DataFrame operations.
Each operation includes:
• What it does
• Syntax
• Real examples
• Common use cases
• Performance notes

DATAFRAME BASICS:
----------------
A DataFrame is a distributed collection of data organized into named columns.
Think of it as a table in a relational database or a pandas DataFrame,
but distributed across multiple machines.

Key Concepts:
• Immutable: Operations create NEW DataFrames (original unchanged)
• Lazy Evaluation: Operations build execution plan, run on action
• Distributed: Data split across partitions on multiple executors
• Typed: Each column has a data type

Author: PySpark Training
Date: 2024-12
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, expr, concat, concat_ws, split, explode, array,
    substring, length, trim, upper, lower, initcap,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    round as spark_round, abs as spark_abs, sqrt, pow as spark_pow,
    to_date, to_timestamp, date_format, current_date, current_timestamp,
    datediff, date_add, date_sub, year, month, dayofmonth,
    row_number, rank, dense_rank, lag, lead, ntile,
    regexp_replace, regexp_extract, coalesce, isnull, isnan,
    struct, array_contains, size, map_keys, map_values
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, ArrayType, MapType
from pyspark.sql.window import Window


def create_sample_data():
    """
    Create sample DataFrame for demonstrating operations.
    """
    spark = SparkSession.builder \
        .appName("DataFrame_Operations") \
        .getOrCreate()
    
    # Sample employee data
    data = [
        (1, "Alice", "Engineering", 75000, "2020-01-15", 28, "alice@company.com"),
        (2, "Bob", "Sales", 65000, "2019-03-22", 35, "bob@company.com"),
        (3, "Charlie", "Engineering", 85000, "2018-07-10", 42, "charlie@company.com"),
        (4, "Diana", "Marketing", 70000, "2021-05-05", 29, "diana@company.com"),
        (5, "Eve", "Engineering", 95000, "2017-11-30", 38, "eve@company.com"),
        (6, "Frank", "Sales", 60000, "2022-02-14", 26, "frank@company.com"),
        (7, "Grace", "Marketing", 72000, "2020-09-18", 31, None),
        (8, "Henry", None, 80000, "2019-12-01", 45, "henry@company.com"),
        (9, "Ivy", "Engineering", None, "2021-08-20", 27, "ivy@company.com"),
        (10, "Jack", "Sales", 68000, "2020-04-11", None, "jack@company.com")
    ]
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("hire_date", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("email", StringType(), True)
    ])
    
    df = spark.createDataFrame(data, schema=schema)
    return df, spark


def section_1_column_operations():
    """
    COLUMN OPERATIONS: Adding, modifying, selecting columns
    """
    print("\n" + "="*70)
    print("SECTION 1: COLUMN OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    print("\n📊 Original DataFrame:")
    df.show()
    
    # ============================================================
    # 1. withColumn() - Add or replace column
    # ============================================================
    print("\n" + "-"*70)
    print("1. withColumn(col_name, expression)")
    print("-"*70)
    print("PURPOSE: Adds a new column (or replaces existing one)")
    print("RETURNS: New DataFrame with added/modified column")
    print("IMMUTABLE: Original DataFrame unchanged")
    
    # Add new column
    df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)
    print("\nExample 1a: Add bonus column (10% of salary)")
    df_with_bonus.select("name", "salary", "bonus").show(5)
    
    # Replace existing column
    df_salary_k = df.withColumn("salary", col("salary") / 1000)
    print("\nExample 1b: Replace salary (convert to thousands)")
    df_salary_k.select("name", "salary").show(5)
    
    # Chain multiple withColumn
    df_enriched = df \
        .withColumn("bonus", col("salary") * 0.1) \
        .withColumn("total_comp", col("salary") + col("bonus")) \
        .withColumn("salary_tier", when(col("salary") > 80000, "High")
                                   .when(col("salary") > 70000, "Medium")
                                   .otherwise("Low"))
    print("\nExample 1c: Chain multiple withColumn operations")
    df_enriched.select("name", "salary", "bonus", "total_comp", "salary_tier").show(5)
    
    # ============================================================
    # 2. select() - Choose specific columns
    # ============================================================
    print("\n" + "-"*70)
    print("2. select(col1, col2, ...)")
    print("-"*70)
    print("PURPOSE: Select specific columns (like SQL SELECT)")
    print("RETURNS: New DataFrame with only selected columns")
    
    df_selected = df.select("name", "department", "salary")
    print("\nExample 2a: Select specific columns")
    df_selected.show(5)
    
    # Select with expressions
    df_calculated = df.select(
        "name",
        col("salary").alias("current_salary"),
        (col("salary") * 1.05).alias("next_year_salary")
    )
    print("\nExample 2b: Select with calculations")
    df_calculated.show(5)
    
    # Select with column expressions
    df_transformed = df.select(
        "name",
        upper(col("department")).alias("dept_upper"),
        (year(current_date()) - col("age")).alias("birth_year")
    )
    print("\nExample 2c: Select with transformations")
    df_transformed.show(5)
    
    # ============================================================
    # 3. selectExpr() - Select using SQL expressions
    # ============================================================
    print("\n" + "-"*70)
    print("3. selectExpr(sql_expression)")
    print("-"*70)
    print("PURPOSE: Select columns using SQL-like expressions")
    print("RETURNS: New DataFrame with evaluated expressions")
    
    df_sql_style = df.selectExpr(
        "name",
        "salary",
        "salary * 0.1 as bonus",
        "salary * 1.1 as total_comp",
        "CASE WHEN salary > 80000 THEN 'High' ELSE 'Normal' END as tier"
    )
    print("\nExample 3: SQL-style selections")
    df_sql_style.show(5)
    
    # ============================================================
    # 4. withColumnRenamed() - Rename column
    # ============================================================
    print("\n" + "-"*70)
    print("4. withColumnRenamed(old_name, new_name)")
    print("-"*70)
    print("PURPOSE: Rename a column")
    print("RETURNS: New DataFrame with renamed column")
    
    df_renamed = df \
        .withColumnRenamed("name", "employee_name") \
        .withColumnRenamed("department", "dept") \
        .withColumnRenamed("salary", "annual_salary")
    print("\nExample 4: Rename multiple columns")
    df_renamed.select("employee_name", "dept", "annual_salary").show(5)
    
    # ============================================================
    # 5. drop() - Remove columns
    # ============================================================
    print("\n" + "-"*70)
    print("5. drop(col1, col2, ...)")
    print("-"*70)
    print("PURPOSE: Remove one or more columns")
    print("RETURNS: New DataFrame without specified columns")
    
    df_dropped = df.drop("email", "hire_date")
    print("\nExample 5: Drop unwanted columns")
    df_dropped.show(5)
    print(f"Original columns: {df.columns}")
    print(f"After drop: {df_dropped.columns}")
    
    # ============================================================
    # 6. alias() - Rename DataFrame or column
    # ============================================================
    print("\n" + "-"*70)
    print("6. alias(name)")
    print("-"*70)
    print("PURPOSE: Give DataFrame or column an alias (for joins)")
    print("RETURNS: Aliased DataFrame/Column")
    
    df1 = df.alias("employees")
    df2 = df.alias("emp")
    
    # Useful in self-joins
    df_join = df1.join(df2, col("employees.department") == col("emp.department"), "inner") \
        .select("employees.name", "emp.name", "employees.department")
    print("\nExample 6: Alias in self-join (find colleagues)")
    df_join.show(5)


def section_2_filtering_operations():
    """
    FILTERING OPERATIONS: Selecting rows based on conditions
    """
    print("\n" + "="*70)
    print("SECTION 2: FILTERING OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # 1. filter() / where() - Filter rows
    # ============================================================
    print("\n" + "-"*70)
    print("1. filter(condition) / where(condition)")
    print("-"*70)
    print("PURPOSE: Filter rows that match condition")
    print("RETURNS: New DataFrame with only matching rows")
    print("NOTE: filter() and where() are identical")
    
    # Simple filter
    df_high_salary = df.filter(col("salary") > 75000)
    print("\nExample 1a: Filter salary > 75000")
    df_high_salary.show()
    
    # Multiple conditions with &, |
    df_engineering_high = df.filter(
        (col("department") == "Engineering") & (col("salary") > 70000)
    )
    print("\nExample 1b: Filter Engineering AND salary > 70000")
    df_engineering_high.show()
    
    # OR condition
    df_sales_or_marketing = df.filter(
        (col("department") == "Sales") | (col("department") == "Marketing")
    )
    print("\nExample 1c: Filter Sales OR Marketing")
    df_sales_or_marketing.show()
    
    # String matching
    df_a_names = df.filter(col("name").startswith("A") | col("name").startswith("E"))
    print("\nExample 1d: Filter names starting with A or E")
    df_a_names.show()
    
    # ============================================================
    # 2. isNull() / isNotNull() - Handle nulls
    # ============================================================
    print("\n" + "-"*70)
    print("2. isNull() / isNotNull()")
    print("-"*70)
    print("PURPOSE: Filter based on null values")
    
    df_missing_dept = df.filter(col("department").isNull())
    print("\nExample 2a: Find rows with null department")
    df_missing_dept.show()
    
    df_has_email = df.filter(col("email").isNotNull())
    print("\nExample 2b: Find rows with email")
    df_has_email.show()
    
    # ============================================================
    # 3. isin() - Check membership
    # ============================================================
    print("\n" + "-"*70)
    print("3. isin(list_of_values)")
    print("-"*70)
    print("PURPOSE: Check if column value is in list")
    
    df_specific_depts = df.filter(col("department").isin(["Engineering", "Sales"]))
    print("\nExample 3: Filter specific departments")
    df_specific_depts.show()
    
    # ============================================================
    # 4. between() - Range check
    # ============================================================
    print("\n" + "-"*70)
    print("4. between(start, end)")
    print("-"*70)
    print("PURPOSE: Check if value is within range (inclusive)")
    
    df_mid_age = df.filter(col("age").between(28, 35))
    print("\nExample 4: Filter age between 28 and 35")
    df_mid_age.show()
    
    # ============================================================
    # 5. distinct() / dropDuplicates() - Remove duplicates
    # ============================================================
    print("\n" + "-"*70)
    print("5. distinct() / dropDuplicates()")
    print("-"*70)
    print("PURPOSE: Remove duplicate rows")
    
    df_unique_depts = df.select("department").distinct()
    print("\nExample 5a: Get unique departments")
    df_unique_depts.show()
    
    df_unique_by_dept = df.dropDuplicates(["department"])
    print("\nExample 5b: Drop duplicates by department (keep first)")
    df_unique_by_dept.select("name", "department").show()


def section_3_aggregation_operations():
    """
    AGGREGATION OPERATIONS: Summarizing data
    """
    print("\n" + "="*70)
    print("SECTION 3: AGGREGATION OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # 1. groupBy() + agg() - Group and aggregate
    # ============================================================
    print("\n" + "-"*70)
    print("1. groupBy(col).agg(aggregation_functions)")
    print("-"*70)
    print("PURPOSE: Group rows by column and calculate aggregates")
    print("RETURNS: New DataFrame with grouped results")
    
    df_by_dept = df.groupBy("department").agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        spark_max("salary").alias("max_salary"),
        spark_min("salary").alias("min_salary")
    )
    print("\nExample 1: Statistics by department")
    df_by_dept.show()
    
    # Multiple groupBy columns
    df_by_dept_age = df.groupBy("department", 
                                when(col("age") < 30, "Young")
                                .when(col("age") < 40, "Mid")
                                .otherwise("Senior").alias("age_group")) \
        .agg(count("*").alias("count"))
    print("\nExample 1b: Group by multiple columns")
    df_by_dept_age.show()
    
    # ============================================================
    # 2. count() - Count rows
    # ============================================================
    print("\n" + "-"*70)
    print("2. count()")
    print("-"*70)
    print("PURPOSE: Count total rows")
    print("RETURNS: Integer (this is an ACTION, not transformation)")
    
    total_count = df.count()
    print(f"\nExample 2a: Total row count: {total_count}")
    
    engineering_count = df.filter(col("department") == "Engineering").count()
    print(f"Example 2b: Engineering employees: {engineering_count}")
    
    # ============================================================
    # 3. describe() - Summary statistics
    # ============================================================
    print("\n" + "-"*70)
    print("3. describe()")
    print("-"*70)
    print("PURPOSE: Calculate summary statistics (count, mean, stddev, min, max)")
    print("RETURNS: DataFrame with statistics")
    
    print("\nExample 3: Describe numerical columns")
    df.describe(["salary", "age"]).show()
    
    # ============================================================
    # 4. pivot() - Reshape data
    # ============================================================
    print("\n" + "-"*70)
    print("4. pivot(column)")
    print("-"*70)
    print("PURPOSE: Rotate data from rows to columns")
    print("RETURNS: Pivoted DataFrame")
    
    df_pivot = df.groupBy("department") \
        .pivot("department") \
        .agg(avg("salary"))
    print("\nExample 4: Pivot (less useful here, better with time-series)")
    
    # Better pivot example
    df_dept_summary = df.groupBy() \
        .pivot("department") \
        .agg(count("*").alias("count"))
    print("\nExample 4b: Count employees per department (pivoted)")
    df_dept_summary.show()


def section_4_join_operations():
    """
    JOIN OPERATIONS: Combining DataFrames
    """
    print("\n" + "="*70)
    print("SECTION 4: JOIN OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # Create second DataFrame for joins
    dept_data = [
        ("Engineering", "Building A", "tech@company.com"),
        ("Sales", "Building B", "sales@company.com"),
        ("Marketing", "Building C", "marketing@company.com"),
        ("HR", "Building D", "hr@company.com")
    ]
    df_departments = spark.createDataFrame(dept_data, ["dept_name", "location", "contact"])
    
    print("\n📊 Employee DataFrame:")
    df.select("name", "department", "salary").show(5)
    
    print("\n📊 Department DataFrame:")
    df_departments.show()
    
    # ============================================================
    # 1. join() - Inner join (default)
    # ============================================================
    print("\n" + "-"*70)
    print("1. join(other_df, on=condition, how='inner')")
    print("-"*70)
    print("PURPOSE: Combine DataFrames based on common column(s)")
    print("RETURNS: New DataFrame with matched rows")
    
    df_inner = df.join(df_departments, df.department == df_departments.dept_name, "inner")
    print("\nExample 1: Inner join (only matching rows)")
    df_inner.select("name", "department", "location", "contact").show()
    
    # ============================================================
    # 2. left / right / outer joins
    # ============================================================
    print("\n" + "-"*70)
    print("2. join(how='left' / 'right' / 'outer' / 'full')")
    print("-"*70)
    
    df_left = df.join(df_departments, df.department == df_departments.dept_name, "left")
    print("\nExample 2a: Left join (all employees, even without dept info)")
    df_left.select("name", "department", "location").show()
    
    df_right = df.join(df_departments, df.department == df_departments.dept_name, "right")
    print("\nExample 2b: Right join (all departments, even without employees)")
    df_right.select("name", "dept_name", "location").show()
    
    df_outer = df.join(df_departments, df.department == df_departments.dept_name, "outer")
    print("\nExample 2c: Outer join (all from both, with nulls)")
    df_outer.select("name", "department", "dept_name", "location").show()
    
    # ============================================================
    # 3. cross join - Cartesian product
    # ============================================================
    print("\n" + "-"*70)
    print("3. crossJoin(other_df)")
    print("-"*70)
    print("PURPOSE: Every row from left × every row from right")
    print("WARNING: Can be very large! n × m rows")
    
    small_df = df.select("name").limit(2)
    dept_small = df_departments.select("dept_name").limit(2)
    df_cross = small_df.crossJoin(dept_small)
    print("\nExample 3: Cross join (small sample)")
    df_cross.show()


def section_5_sorting_operations():
    """
    SORTING OPERATIONS: Ordering rows
    """
    print("\n" + "="*70)
    print("SECTION 5: SORTING OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # 1. orderBy() / sort() - Sort rows
    # ============================================================
    print("\n" + "-"*70)
    print("1. orderBy(col) / sort(col)")
    print("-"*70)
    print("PURPOSE: Sort DataFrame by one or more columns")
    print("NOTE: orderBy() and sort() are identical")
    
    df_sorted = df.orderBy("salary", ascending=False)
    print("\nExample 1a: Sort by salary (descending)")
    df_sorted.select("name", "salary").show()
    
    df_multi_sort = df.orderBy(col("department"), col("salary").desc())
    print("\nExample 1b: Sort by department, then salary (descending)")
    df_multi_sort.select("name", "department", "salary").show()
    
    # ============================================================
    # 2. limit() - Limit number of rows
    # ============================================================
    print("\n" + "-"*70)
    print("2. limit(n)")
    print("-"*70)
    print("PURPOSE: Return only first n rows")
    
    df_top5 = df.orderBy(col("salary").desc()).limit(5)
    print("\nExample 2: Top 5 highest salaries")
    df_top5.select("name", "salary").show()


def section_6_string_operations():
    """
    STRING OPERATIONS: Manipulating text columns
    """
    print("\n" + "="*70)
    print("SECTION 6: STRING OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # String functions
    # ============================================================
    print("\n" + "-"*70)
    print("STRING FUNCTIONS")
    print("-"*70)
    
    df_strings = df.select(
        "name",
        upper(col("name")).alias("name_upper"),
        lower(col("name")).alias("name_lower"),
        length(col("name")).alias("name_length"),
        substring(col("name"), 1, 3).alias("first_3_chars"),
        concat(col("name"), lit(" - "), col("department")).alias("name_dept")
    )
    print("\nExample: Various string operations")
    df_strings.show(5)
    
    # Email domain extraction
    df_email = df.filter(col("email").isNotNull()) \
        .select(
            "name",
            "email",
            regexp_extract(col("email"), r"@(.+)", 1).alias("email_domain")
        )
    print("\nExample: Extract email domain")
    df_email.show()


def section_7_date_operations():
    """
    DATE/TIME OPERATIONS: Working with temporal data
    """
    print("\n" + "="*70)
    print("SECTION 7: DATE/TIME OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # Date functions
    # ============================================================
    print("\n" + "-"*70)
    print("DATE FUNCTIONS")
    print("-"*70)
    
    df_dates = df.select(
        "name",
        "hire_date",
        to_date(col("hire_date")).alias("hire_date_typed"),
        year(to_date(col("hire_date"))).alias("hire_year"),
        month(to_date(col("hire_date"))).alias("hire_month"),
        datediff(current_date(), to_date(col("hire_date"))).alias("days_employed"),
        (datediff(current_date(), to_date(col("hire_date"))) / 365).cast("int").alias("years_employed")
    )
    print("\nExample: Date calculations")
    df_dates.show()


def section_8_null_handling():
    """
    NULL HANDLING: Dealing with missing data
    """
    print("\n" + "="*70)
    print("SECTION 8: NULL HANDLING")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # 1. fillna() - Fill null values
    # ============================================================
    print("\n" + "-"*70)
    print("1. fillna(value) / fillna({col: value})")
    print("-"*70)
    print("PURPOSE: Replace null values")
    
    df_filled = df.fillna({
        "department": "Unknown",
        "salary": 0,
        "age": 30,
        "email": "no-email@company.com"
    })
    print("\nExample 1: Fill nulls with defaults")
    df_filled.show()
    
    # ============================================================
    # 2. dropna() - Drop rows with nulls
    # ============================================================
    print("\n" + "-"*70)
    print("2. dropna(how='any'/'all', subset=[cols])")
    print("-"*70)
    print("PURPOSE: Remove rows with null values")
    
    df_no_nulls = df.dropna()
    print(f"\nExample 2a: Drop any row with ANY null")
    print(f"Original: {df.count()} rows")
    print(f"After dropna: {df_no_nulls.count()} rows")
    df_no_nulls.show()
    
    df_dept_only = df.dropna(subset=["department"])
    print(f"\nExample 2b: Drop only if department is null")
    print(f"After: {df_dept_only.count()} rows")
    df_dept_only.show()
    
    # ============================================================
    # 3. coalesce() - First non-null value
    # ============================================================
    print("\n" + "-"*70)
    print("3. coalesce(col1, col2, ..., default)")
    print("-"*70)
    print("PURPOSE: Return first non-null value from columns")
    
    df_coalesce = df.select(
        "name",
        "department",
        coalesce(col("department"), lit("Unknown")).alias("dept_or_unknown")
    )
    print("\nExample 3: Use coalesce for default values")
    df_coalesce.show()


def section_9_window_functions():
    """
    WINDOW FUNCTIONS: Advanced analytics over partitions
    """
    print("\n" + "="*70)
    print("SECTION 9: WINDOW FUNCTIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # Window functions
    # ============================================================
    print("\n" + "-"*70)
    print("WINDOW FUNCTIONS - Ranking, Analytics, Aggregates")
    print("-"*70)
    print("PURPOSE: Perform calculations across sets of rows")
    print("KEY: Define window spec (partitionBy, orderBy, rowsBetween)")
    
    # Define windows
    window_dept = Window.partitionBy("department").orderBy(col("salary").desc())
    window_full = Window.orderBy(col("salary").desc())
    
    df_windowed = df.filter(col("salary").isNotNull()).select(
        "name",
        "department",
        "salary",
        # Ranking functions
        row_number().over(window_dept).alias("rank_in_dept"),
        rank().over(window_full).alias("company_rank"),
        # Analytical functions
        lag(col("salary"), 1).over(window_dept).alias("next_lower_salary"),
        lead(col("salary"), 1).over(window_dept).alias("next_higher_salary"),
        # Aggregate over window
        avg("salary").over(Window.partitionBy("department")).alias("dept_avg_salary")
    )
    
    print("\nExample: Window functions (ranking, lag/lead, moving avg)")
    df_windowed.show()
    
    print("\n💡 KEY INSIGHTS:")
    print("• row_number(): Unique rank (1, 2, 3, 4...)")
    print("• rank(): Ties get same rank, gaps after (1, 2, 2, 4...)")
    print("• lag/lead(): Access previous/next row in window")
    print("• Window aggregates: Calculate over partition without collapsing")


def section_10_advanced_operations():
    """
    ADVANCED OPERATIONS: Complex transformations
    """
    print("\n" + "="*70)
    print("SECTION 10: ADVANCED OPERATIONS")
    print("="*70)
    
    df, spark = create_sample_data()
    
    # ============================================================
    # 1. explode() - Expand arrays
    # ============================================================
    print("\n" + "-"*70)
    print("1. explode(array_column)")
    print("-"*70)
    print("PURPOSE: Convert array column into multiple rows")
    
    # Create data with arrays
    data_arrays = [
        (1, "Alice", ["Python", "Spark", "SQL"]),
        (2, "Bob", ["Java", "Scala"]),
        (3, "Charlie", ["Python", "R", "Julia", "SQL"])
    ]
    df_skills = spark.createDataFrame(data_arrays, ["id", "name", "skills"])
    
    print("\nBefore explode:")
    df_skills.show(truncate=False)
    
    df_exploded = df_skills.select("name", explode("skills").alias("skill"))
    print("\nAfter explode (one row per skill):")
    df_exploded.show()
    
    # ============================================================
    # 2. union() - Combine DataFrames vertically
    # ============================================================
    print("\n" + "-"*70)
    print("2. union(other_df)")
    print("-"*70)
    print("PURPOSE: Stack DataFrames (append rows)")
    
    df1 = df.limit(3)
    df2 = df.limit(3).withColumn("salary", col("salary") + 10000)
    df_union = df1.union(df2)
    print("\nExample: Union two DataFrames")
    df_union.select("name", "salary").show()
    
    # ============================================================
    # 3. repartition() / coalesce()
    # ============================================================
    print("\n" + "-"*70)
    print("3. repartition(n) / coalesce(n)")
    print("-"*70)
    print("PURPOSE: Control partitioning for parallelism")
    
    print(f"\nOriginal partitions: {df.rdd.getNumPartitions()}")
    
    df_repartition = df.repartition(10)
    print(f"After repartition(10): {df_repartition.rdd.getNumPartitions()}")
    
    df_coalesce = df.coalesce(1)
    print(f"After coalesce(1): {df_coalesce.rdd.getNumPartitions()}")
    
    print("\n💡 Difference:")
    print("• repartition(): Full shuffle, can increase or decrease")
    print("• coalesce(): No shuffle, only decrease partitions")


def main():
    """
    Run all DataFrame operation examples.
    """
    print("\n" + "="*70)
    print(" PYSPARK DATAFRAME OPERATIONS - COMPLETE GUIDE ")
    print("="*70)
    
    print("""
This comprehensive guide covers ALL common DataFrame operations:

1. Column Operations (withColumn, select, selectExpr, rename, drop)
2. Filtering (filter, where, isNull, isin, between, distinct)
3. Aggregations (groupBy, agg, count, describe, pivot)
4. Joins (inner, left, right, outer, cross)
5. Sorting (orderBy, sort, limit)
6. String Operations (upper, lower, concat, substring, regexp)
7. Date Operations (to_date, year, month, datediff)
8. Null Handling (fillna, dropna, coalesce)
9. Window Functions (row_number, rank, lag, lead)
10. Advanced (explode, union, repartition)

Each operation includes:
• What it does
• Syntax
• Real examples
• Common use cases
    """)
    
    try:
        section_1_column_operations()
        section_2_filtering_operations()
        section_3_aggregation_operations()
        section_4_join_operations()
        section_5_sorting_operations()
        section_6_string_operations()
        section_7_date_operations()
        section_8_null_handling()
        section_9_window_functions()
        section_10_advanced_operations()
        
        print("\n" + "="*70)
        print("✅ ALL DATAFRAME OPERATIONS DEMONSTRATED")
        print("="*70)
        
        print("\n📚 QUICK REFERENCE:")
        print("""
TRANSFORMATIONS (Lazy - build execution plan):
• withColumn(), select(), filter(), join(), groupBy()
• orderBy(), distinct(), drop(), union()

ACTIONS (Eager - trigger execution):
• show(), count(), collect(), take()
• write.parquet(), write.csv()

KEY PRINCIPLE: Operations are immutable and lazy!
DataFrame operations create NEW DataFrames.
Execution happens only when action is called.
        """)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
