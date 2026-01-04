#!/usr/bin/env python3
"""
PySpark Technical Interview Questions - SQL-Style Problems
==========================================================

Classic SQL interview questions adapted for PySpark.
Common in technical interviews at FAANG and data companies.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType


def create_spark_session():
    """Create Spark session."""
    return SparkSession.builder \
        .appName("PySpark_Interview_SQL") \
        .master("local[*]") \
        .getOrCreate()


# ============================================================================
# QUESTION 1: Employee Salary Higher Than Manager
# ============================================================================
def question_1_salary_higher_than_manager(spark):
    """
    Find employees who earn more than their managers.
    Classic SQL interview question.
    """
    print("\n" + "="*70)
    print("QUESTION 1: Employees Earning More Than Their Managers")
    print("="*70)
    
    data = [
        (1, "John", 50000, 3),
        (2, "Jane", 60000, 3),
        (3, "Bob", 55000, None),   # Manager
        (4, "Alice", 70000, 5),
        (5, "Charlie", 65000, None), # Manager
    ]
    
    df = spark.createDataFrame(data, ["id", "name", "salary", "manager_id"])
    df.show()
    
    # SOLUTION
    managers = df.select(
        col("id").alias("mgr_id"),
        col("salary").alias("mgr_salary")
    )
    
    result = df.join(managers, df.manager_id == managers.mgr_id) \
        .filter(col("salary") > col("mgr_salary")) \
        .select("name", "salary", "mgr_salary")
    
    print("\nEmployees earning more than their manager:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 2: Nth Highest Salary
# ============================================================================
def question_2_nth_highest_salary(spark, n=3):
    """
    Find the Nth highest salary.
    Very common in interviews - especially 2nd highest.
    """
    print("\n" + "="*70)
    print(f"QUESTION 2: Find {n}th Highest Salary")
    print("="*70)
    
    data = [
        ("John", 95000),
        ("Jane", 88000),
        ("Bob", 92000),
        ("Alice", 95000),  # Tied for highest
        ("Charlie", 78000),
        ("David", 82000),
    ]
    
    df = spark.createDataFrame(data, ["name", "salary"])
    df.show()
    
    # SOLUTION: Using dense_rank to handle ties
    window_spec = Window.orderBy(col("salary").desc())
    
    result = df.select("salary").distinct() \
        .withColumn("rank", dense_rank().over(window_spec)) \
        .filter(col("rank") == n) \
        .select("salary")
    
    print(f"\n{n}th Highest Salary:")
    if result.count() > 0:
        result.show()
    else:
        print(f"NULL (less than {n} distinct salaries)")
    
    return result


# ============================================================================
# QUESTION 3: Department Top 3 Salaries
# ============================================================================
def question_3_department_top_3(spark):
    """
    Find top 3 salaries in each department.
    Tests window functions and ranking.
    """
    print("\n" + "="*70)
    print("QUESTION 3: Top 3 Salaries Per Department")
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
    ]
    
    df = spark.createDataFrame(data, ["name", "department", "salary"])
    df.show()
    
    # SOLUTION
    window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
    
    result = df.withColumn("rank", dense_rank().over(window_spec)) \
        .filter(col("rank") <= 3) \
        .orderBy("department", "rank")
    
    print("\nTop 3 per department:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 4: Consecutive Numbers
# ============================================================================
def question_4_consecutive_numbers(spark):
    """
    Find numbers that appear at least 3 times consecutively.
    Tests window functions with lag/lead.
    """
    print("\n" + "="*70)
    print("QUESTION 4: Find Consecutive Numbers (3+ times)")
    print("="*70)
    
    data = [(1, 1), (2, 1), (3, 1), (4, 2), (5, 1), (6, 2), (7, 2)]
    df = spark.createDataFrame(data, ["id", "num"])
    df.show()
    
    # SOLUTION
    window_spec = Window.orderBy("id")
    
    result = df.withColumn("prev1", lag("num", 1).over(window_spec)) \
        .withColumn("prev2", lag("num", 2).over(window_spec)) \
        .filter((col("num") == col("prev1")) & (col("num") == col("prev2"))) \
        .select("num").distinct()
    
    print("\nNumbers appearing 3+ times consecutively:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 5: Customers Who Never Order
# ============================================================================
def question_5_customers_never_order(spark):
    """
    Find customers who never placed an order.
    Tests left anti join.
    """
    print("\n" + "="*70)
    print("QUESTION 5: Customers Who Never Ordered")
    print("="*70)
    
    customers = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David"),
    ]
    
    orders = [
        (1, 1, 100),
        (2, 1, 150),
        (3, 3, 200),
    ]
    
    customers_df = spark.createDataFrame(customers, ["cust_id", "name"])
    orders_df = spark.createDataFrame(orders, ["order_id", "cust_id", "amount"])
    
    print("Customers:")
    customers_df.show()
    
    print("Orders:")
    orders_df.show()
    
    # SOLUTION: Left anti join
    result = customers_df.join(orders_df, "cust_id", "left_anti")
    
    print("\nCustomers who never ordered:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 6: Trips and Users (Cancellation Rate)
# ============================================================================
def question_6_trip_cancellation_rate(spark):
    """
    Calculate cancellation rate for trips.
    Tests groupBy, conditionals, and aggregation.
    """
    print("\n" + "="*70)
    print("QUESTION 6: Trip Cancellation Rate")
    print("="*70)
    
    data = [
        (1, "2024-01-01", "completed"),
        (2, "2024-01-01", "cancelled_by_driver"),
        (3, "2024-01-01", "completed"),
        (4, "2024-01-02", "cancelled_by_client"),
        (5, "2024-01-02", "completed"),
        (6, "2024-01-02", "completed"),
        (7, "2024-01-03", "completed"),
    ]
    
    df = spark.createDataFrame(data, ["trip_id", "date", "status"])
    df.show()
    
    # SOLUTION
    result = df.groupBy("date").agg(
        count("*").alias("total_trips"),
        sum(when(col("status").contains("cancelled"), 1).otherwise(0)).alias("cancelled_trips")
    ).withColumn(
        "cancellation_rate",
        round(col("cancelled_trips") / col("total_trips"), 2)
    )
    
    print("\nCancellation rate by date:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 7: Rising Temperature
# ============================================================================
def question_7_rising_temperature(spark):
    """
    Find dates where temperature was higher than previous day.
    Tests date operations and window functions.
    """
    print("\n" + "="*70)
    print("QUESTION 7: Rising Temperature")
    print("="*70)
    
    data = [
        ("2024-01-01", 30),
        ("2024-01-02", 35),  # Higher than previous
        ("2024-01-03", 32),
        ("2024-01-04", 38),  # Higher than previous
        ("2024-01-05", 36),
    ]
    
    df = spark.createDataFrame(data, ["date", "temperature"])
    df = df.withColumn("date", to_date(col("date")))
    df.show()
    
    # SOLUTION
    window_spec = Window.orderBy("date")
    
    result = df.withColumn("prev_temp", lag("temperature").over(window_spec)) \
        .withColumn("prev_date", lag("date").over(window_spec)) \
        .filter((col("temperature") > col("prev_temp")) & 
                (datediff(col("date"), col("prev_date")) == 1)) \
        .select("date", "temperature", "prev_temp")
    
    print("\nDates with rising temperature:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 8: Exchange Seats
# ============================================================================
def question_8_exchange_seats(spark):
    """
    Swap seat IDs of consecutive students.
    Tests conditional logic with when/otherwise.
    """
    print("\n" + "="*70)
    print("QUESTION 8: Exchange Seats")
    print("="*70)
    
    data = [
        (1, "Alice"),
        (2, "Bob"),
        (3, "Charlie"),
        (4, "David"),
        (5, "Eve"),
    ]
    
    df = spark.createDataFrame(data, ["id", "student"])
    print("Original seating:")
    df.show()
    
    # SOLUTION
    max_id = df.agg(max("id")).collect()[0][0]
    
    result = df.withColumn("new_id",
        when(col("id") % 2 == 1, 
            when(col("id") == max_id, col("id")).otherwise(col("id") + 1)
        ).otherwise(col("id") - 1)
    ).select(
        col("new_id").alias("id"),
        col("student")
    ).orderBy("id")
    
    print("\nAfter swapping seats:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 9: Game Play Analysis
# ============================================================================
def question_9_first_login_date(spark):
    """
    Find the first login date for each player.
    Tests groupBy with min aggregation.
    """
    print("\n" + "="*70)
    print("QUESTION 9: First Login Date Per Player")
    print("="*70)
    
    data = [
        (1, "2024-01-01", "Quest_1"),
        (1, "2024-01-02", "Quest_2"),
        (2, "2024-01-01", "Quest_1"),
        (3, "2024-01-02", "Quest_1"),
        (3, "2024-01-03", "Quest_2"),
    ]
    
    df = spark.createDataFrame(data, ["player_id", "event_date", "games_played"])
    df = df.withColumn("event_date", to_date(col("event_date")))
    df.show()
    
    # SOLUTION
    result = df.groupBy("player_id").agg(
        min("event_date").alias("first_login")
    )
    
    print("\nFirst login date per player:")
    result.show()
    
    return result


# ============================================================================
# QUESTION 10: Active Users
# ============================================================================
def question_10_active_users(spark):
    """
    Find users who logged in for 5 consecutive days.
    Tests complex window functions.
    """
    print("\n" + "="*70)
    print("QUESTION 10: Users Active for 5+ Consecutive Days")
    print("="*70)
    
    data = [
        (1, "2024-01-01"),
        (1, "2024-01-02"),
        (1, "2024-01-03"),
        (1, "2024-01-04"),
        (1, "2024-01-05"),  # 5 consecutive
        (2, "2024-01-01"),
        (2, "2024-01-03"),  # Not consecutive
        (3, "2024-01-01"),
        (3, "2024-01-02"),
        (3, "2024-01-03"),
        (3, "2024-01-05"),  # Gap
    ]
    
    df = spark.createDataFrame(data, ["user_id", "login_date"])
    df = df.withColumn("login_date", to_date(col("login_date"))).distinct()
    df.show()
    
    # SOLUTION: Check if 4 days after current date also exists
    window_spec = Window.partitionBy("user_id").orderBy("login_date")
    
    result = df.withColumn("date_plus_4", date_add(col("login_date"), 4)) \
        .join(
            df.select(col("user_id").alias("u2"), col("login_date").alias("check_date")),
            (col("user_id") == col("u2")) & (col("date_plus_4") == col("check_date")),
            "inner"
        ).select("user_id").distinct()
    
    print("\nUsers active 5+ consecutive days:")
    result.show()
    
    return result


# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Run all SQL-style interview questions."""
    print("\n" + "="*70)
    print("PYSPARK TECHNICAL INTERVIEW - SQL-STYLE QUESTIONS")
    print("="*70)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    try:
        question_1_salary_higher_than_manager(spark)
        question_2_nth_highest_salary(spark, n=3)
        question_3_department_top_3(spark)
        question_4_consecutive_numbers(spark)
        question_5_customers_never_order(spark)
        question_6_trip_cancellation_rate(spark)
        question_7_rising_temperature(spark)
        question_8_exchange_seats(spark)
        question_9_first_login_date(spark)
        question_10_active_users(spark)
        
        print("\n" + "="*70)
        print("✅ All SQL-style questions completed!")
        print("="*70)
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
