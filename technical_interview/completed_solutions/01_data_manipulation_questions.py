#!/usr/bin/env python3
"""
PySpark Technical Interview Questions - Data Manipulation
==========================================================

Common interview questions focusing on DataFrame operations, 
transformations, and data manipulation.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, count, sum as _sum, avg, max as _max, min as _min,
    row_number, rank, dense_rank, lag, lead, ntile,
    explode, split, concat, concat_ws, lower, upper, trim,
    year, month, dayofmonth, date_format, datediff, to_date,
    regexp_replace, regexp_extract, substring
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys


def create_spark_session():
    """Create Spark session for interview practice."""
    return SparkSession.builder \
        .appName("PySpark_Interview_DataManipulation") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()


# ============================================================================
# QUESTION 1: Find Duplicate Records
# ============================================================================
def question_1_find_duplicates(spark):
    """
    Given a dataset of employees, find all duplicate records based on email.
    Return the email and count of duplicates.
    
    Expected Output: Emails that appear more than once with their counts.
    """
    print("\n" + "="*70)
    print("QUESTION 1: Find Duplicate Records")
    print("="*70)
    
    # Sample data
    data = [
        (1, "John Doe", "john@company.com"),
        (2, "Jane Smith", "jane@company.com"),
        (3, "John Doe", "john@company.com"),  # Duplicate
        (4, "Bob Wilson", "bob@company.com"),
        (5, "Jane Smith", "jane@company.com"),  # Duplicate
        (6, "Alice Brown", "alice@company.com"),
    ]
    
    df = spark.createDataFrame(data, ["id", "name", "email"])
    
    print("\nOriginal Data:")
    df.show()
    
    # SOLUTION
    duplicates = df.groupBy("email") \
        .agg(count("*").alias("count")) \
        .filter(col("count") > 1) \
        .orderBy(col("count").desc())
    
    print("\nDuplicates Found:")
    duplicates.show()
    
    # Alternative: Show all duplicate rows
    duplicate_emails = duplicates.select("email")
    all_duplicates = df.join(duplicate_emails, "email", "inner")
    
    print("\nAll Duplicate Rows:")
    all_duplicates.show()
    
    return duplicates


# ============================================================================
# QUESTION 2: Second Highest Salary
# ============================================================================
def question_2_second_highest_salary(spark):
    """
    Find the second highest salary from employees table.
    If there's no second highest, return NULL.
    
    Common SQL interview question adapted for PySpark.
    """
    print("\n" + "="*70)
    print("QUESTION 2: Second Highest Salary")
    print("="*70)
    
    data = [
        ("John", "Engineering", 95000),
        ("Jane", "Engineering", 88000),
        ("Bob", "Sales", 75000),
        ("Alice", "Engineering", 95000),  # Same as highest
        ("Charlie", "Sales", 82000),
    ]
    
    df = spark.createDataFrame(data, ["name", "department", "salary"])
    
    print("\nEmployee Data:")
    df.show()
    
    # SOLUTION 1: Using dense_rank
    window_spec = Window.orderBy(col("salary").desc())
    
    result = df.select("salary").distinct() \
        .withColumn("rank", dense_rank().over(window_spec)) \
        .filter(col("rank") == 2) \
        .select("salary")
    
    print("\nSecond Highest Salary (Method 1 - dense_rank):")
    result.show()
    
    # SOLUTION 2: Using offset
    distinct_salaries = df.select("salary").distinct().orderBy(col("salary").desc())
    
    if distinct_salaries.count() >= 2:
        second_highest = distinct_salaries.collect()[1][0]
        print(f"\nSecond Highest Salary (Method 2): ${second_highest:,}")
    else:
        print("\nSecond Highest Salary: NULL (not enough distinct salaries)")
    
    return result


# ============================================================================
# QUESTION 3: Running Total
# ============================================================================
def question_3_running_total(spark):
    """
    Calculate running total of sales for each product over time.
    
    Tests understanding of window functions and cumulative aggregations.
    """
    print("\n" + "="*70)
    print("QUESTION 3: Running Total (Cumulative Sum)")
    print("="*70)
    
    data = [
        ("Product_A", "2024-01-01", 100),
        ("Product_A", "2024-01-02", 150),
        ("Product_A", "2024-01-03", 200),
        ("Product_B", "2024-01-01", 80),
        ("Product_B", "2024-01-02", 120),
        ("Product_B", "2024-01-03", 90),
    ]
    
    df = spark.createDataFrame(data, ["product", "date", "sales"])
    
    print("\nDaily Sales:")
    df.show()
    
    # SOLUTION
    window_spec = Window.partitionBy("product") \
        .orderBy("date") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    result = df.withColumn("running_total", _sum("sales").over(window_spec))
    
    print("\nRunning Total by Product:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 4: Pivot Table - Sales by Region and Quarter
# ============================================================================
def question_4_pivot_table(spark):
    """
    Create a pivot table showing sales by region and quarter.
    
    Tests pivot operations and aggregations.
    """
    print("\n" + "="*70)
    print("QUESTION 4: Pivot Table - Sales by Region and Quarter")
    print("="*70)
    
    data = [
        ("North", "Q1", 10000),
        ("North", "Q2", 15000),
        ("North", "Q3", 12000),
        ("South", "Q1", 8000),
        ("South", "Q2", 9000),
        ("South", "Q3", 11000),
        ("East", "Q1", 13000),
        ("East", "Q2", 14000),
        ("West", "Q1", 7000),
        ("West", "Q2", 8500),
    ]
    
    df = spark.createDataFrame(data, ["region", "quarter", "sales"])
    
    print("\nOriginal Data:")
    df.show()
    
    # SOLUTION
    pivot_df = df.groupBy("region") \
        .pivot("quarter", ["Q1", "Q2", "Q3", "Q4"]) \
        .agg(_sum("sales")) \
        .fillna(0)
    
    print("\nPivot Table:")
    pivot_df.show()
    
    return pivot_df


# ============================================================================
# QUESTION 5: Remove Consecutive Duplicates
# ============================================================================
def question_5_remove_consecutive_duplicates(spark):
    """
    Remove consecutive duplicate rows based on a column value.
    Keep only the first occurrence.
    
    Tests window functions with lag/lead.
    """
    print("\n" + "="*70)
    print("QUESTION 5: Remove Consecutive Duplicates")
    print("="*70)
    
    data = [
        (1, "A", "2024-01-01"),
        (2, "A", "2024-01-02"),  # Consecutive duplicate
        (3, "B", "2024-01-03"),
        (4, "B", "2024-01-04"),  # Consecutive duplicate
        (5, "B", "2024-01-05"),  # Consecutive duplicate
        (6, "A", "2024-01-06"),  # Not consecutive with previous A
        (7, "C", "2024-01-07"),
    ]
    
    df = spark.createDataFrame(data, ["id", "status", "date"])
    
    print("\nOriginal Data:")
    df.show()
    
    # SOLUTION
    window_spec = Window.orderBy("id")
    
    result = df.withColumn("prev_status", lag("status").over(window_spec)) \
        .filter((col("status") != col("prev_status")) | col("prev_status").isNull()) \
        .drop("prev_status")
    
    print("\nAfter Removing Consecutive Duplicates:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 6: Top N Per Group
# ============================================================================
def question_6_top_n_per_group(spark):
    """
    Find top 3 highest-paid employees in each department.
    
    Very common interview question testing window functions.
    """
    print("\n" + "="*70)
    print("QUESTION 6: Top 3 Highest Paid Employees Per Department")
    print("="*70)
    
    data = [
        ("John", "Engineering", 95000),
        ("Jane", "Engineering", 88000),
        ("Bob", "Engineering", 92000),
        ("Alice", "Engineering", 78000),
        ("Charlie", "Sales", 82000),
        ("David", "Sales", 75000),
        ("Eve", "Sales", 88000),
        ("Frank", "Sales", 79000),
        ("Grace", "HR", 72000),
        ("Henry", "HR", 68000),
    ]
    
    df = spark.createDataFrame(data, ["name", "department", "salary"])
    
    print("\nAll Employees:")
    df.show()
    
    # SOLUTION
    window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
    
    result = df.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .orderBy("department", "rank")
    
    print("\nTop 3 Per Department:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 7: Self Join - Manager Hierarchy
# ============================================================================
def question_7_manager_hierarchy(spark):
    """
    Join employees table with itself to show employee-manager relationships.
    
    Tests self-join understanding.
    """
    print("\n" + "="*70)
    print("QUESTION 7: Employee-Manager Hierarchy (Self Join)")
    print("="*70)
    
    data = [
        (1, "Alice", None),      # CEO, no manager
        (2, "Bob", 1),           # Reports to Alice
        (3, "Charlie", 1),       # Reports to Alice
        (4, "David", 2),         # Reports to Bob
        (5, "Eve", 2),           # Reports to Bob
        (6, "Frank", 3),         # Reports to Charlie
    ]
    
    df = spark.createDataFrame(data, ["emp_id", "emp_name", "manager_id"])
    
    print("\nEmployee Data:")
    df.show()
    
    # SOLUTION
    managers = df.select(
        col("emp_id").alias("mgr_id"),
        col("emp_name").alias("manager_name")
    )
    
    result = df.join(managers, df.manager_id == managers.mgr_id, "left") \
        .select("emp_id", "emp_name", "manager_name") \
        .orderBy("emp_id")
    
    print("\nEmployee-Manager Relationships:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 8: Explode Nested Arrays
# ============================================================================
def question_8_explode_arrays(spark):
    """
    Flatten nested array structures.
    
    Tests understanding of explode and array handling.
    """
    print("\n" + "="*70)
    print("QUESTION 8: Explode Nested Arrays")
    print("="*70)
    
    data = [
        (1, "Alice", ["Python", "Scala", "SQL"]),
        (2, "Bob", ["Java", "Python"]),
        (3, "Charlie", ["R", "Python", "Spark"]),
    ]
    
    df = spark.createDataFrame(data, ["id", "name", "skills"])
    
    print("\nOriginal Data (with arrays):")
    df.show(truncate=False)
    
    # SOLUTION
    result = df.select("id", "name", explode("skills").alias("skill"))
    
    print("\nExploded Data:")
    result.show()
    
    # Count skills per person
    skill_count = df.select("name", "skills") \
        .withColumn("skill_count", count(explode("skills")).over(Window.partitionBy("name")))
    
    return result


# ============================================================================
# QUESTION 9: Moving Average
# ============================================================================
def question_9_moving_average(spark):
    """
    Calculate 3-day moving average of stock prices.
    
    Tests window functions with frame specifications.
    """
    print("\n" + "="*70)
    print("QUESTION 9: 3-Day Moving Average")
    print("="*70)
    
    data = [
        ("2024-01-01", 100.0),
        ("2024-01-02", 102.5),
        ("2024-01-03", 98.0),
        ("2024-01-04", 105.0),
        ("2024-01-05", 103.0),
        ("2024-01-06", 107.5),
        ("2024-01-07", 106.0),
    ]
    
    df = spark.createDataFrame(data, ["date", "price"])
    
    print("\nDaily Prices:")
    df.show()
    
    # SOLUTION: 3-day moving average (current + 2 previous days)
    window_spec = Window.orderBy("date").rowsBetween(-2, 0)
    
    result = df.withColumn("moving_avg_3day", avg("price").over(window_spec)) \
        .withColumn("moving_avg_3day", col("moving_avg_3day").cast("decimal(10,2)"))
    
    print("\n3-Day Moving Average:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 10: Gap Analysis - Missing Dates
# ============================================================================
def question_10_find_gaps(spark):
    """
    Find missing dates in a time series.
    
    Tests understanding of date operations and joins.
    """
    print("\n" + "="*70)
    print("QUESTION 10: Find Missing Dates")
    print("="*70)
    
    from datetime import datetime, timedelta
    
    # Data with missing dates
    data = [
        ("2024-01-01",),
        ("2024-01-02",),
        ("2024-01-04",),  # Missing 01-03
        ("2024-01-05",),
        ("2024-01-08",),  # Missing 01-06, 01-07
    ]
    
    df = spark.createDataFrame(data, ["date"])
    df = df.withColumn("date", to_date(col("date")))
    
    print("\nRecords with Dates:")
    df.show()
    
    # SOLUTION
    # Create complete date range
    min_date = df.agg(_min("date")).collect()[0][0]
    max_date = df.agg(_max("date")).collect()[0][0]
    
    # Generate all dates
    date_range = []
    current_date = min_date
    while current_date <= max_date:
        date_range.append((current_date,))
        current_date += timedelta(days=1)
    
    all_dates = spark.createDataFrame(date_range, ["date"])
    
    # Find missing dates
    missing = all_dates.join(df, "date", "left_anti")
    
    print("\nMissing Dates:")
    missing.show()
    
    return missing


# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Run all interview questions."""
    print("\n" + "="*70)
    print("PYSPARK TECHNICAL INTERVIEW - DATA MANIPULATION QUESTIONS")
    print("="*70)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        # Run each question
        question_1_find_duplicates(spark)
        question_2_second_highest_salary(spark)
        question_3_running_total(spark)
        question_4_pivot_table(spark)
        question_5_remove_consecutive_duplicates(spark)
        question_6_top_n_per_group(spark)
        question_7_manager_hierarchy(spark)
        question_8_explode_arrays(spark)
        question_9_moving_average(spark)
        question_10_find_gaps(spark)
        
        print("\n" + "="*70)
        print("✅ All questions completed successfully!")
        print("="*70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
