"""
01_session_basics.py
====================

Spark Session Basics - Entry Point for PySpark Applications

Covers:
- Creating SparkSession
- Configuration options
- Reading data
- SQL operations
- Stop and cleanup
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time


def create_basic_session():
    """
    Create a basic SparkSession with minimal configuration.
    """
    print("=" * 80)
    print("CREATING BASIC SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("BasicSparkSession") \
        .master("local[*]") \
        .getOrCreate()
    
    print(f"✅ Spark Version: {spark.version}")
    print(f"✅ Application Name: {spark.sparkContext.appName}")
    print(f"✅ Master: {spark.sparkContext.master}")
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")
    
    return spark


def create_configured_session():
    """
    Create SparkSession with comprehensive configuration.
    """
    print("\n" + "=" * 80)
    print("CREATING CONFIGURED SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("ConfiguredSparkSession") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print("\n🔧 Configuration Summary:")
    conf = spark.sparkContext.getConf()
    
    important_configs = [
        'spark.sql.shuffle.partitions',
        'spark.default.parallelism',
        'spark.sql.adaptive.enabled',
        'spark.driver.memory',
        'spark.executor.memory',
        'spark.sql.autoBroadcastJoinThreshold'
    ]
    
    for config in important_configs:
        value = conf.get(config, 'Not Set')
        print(f"   {config}: {value}")
    
    return spark


def demonstrate_createdataframe(spark):
    """
    Demonstrate various ways to create DataFrames.
    """
    print("\n" + "=" * 80)
    print("CREATEDATAFRAME EXAMPLES")
    print("=" * 80)
    
    # Method 1: From list of tuples
    print("\n1️⃣  From List of Tuples:")
    data = [
        ("Alice", 25, 50000),
        ("Bob", 30, 60000),
        ("Charlie", 35, 70000)
    ]
    df1 = spark.createDataFrame(data, ["name", "age", "salary"])
    df1.show()
    
    # Method 2: From list of dictionaries
    print("\n2️⃣  From List of Dictionaries:")
    data = [
        {"name": "Alice", "age": 25, "salary": 50000},
        {"name": "Bob", "age": 30, "salary": 60000},
        {"name": "Charlie", "age": 35, "salary": 70000}
    ]
    df2 = spark.createDataFrame(data)
    df2.show()
    
    # Method 3: With explicit schema
    print("\n3️⃣  With Explicit Schema:")
    schema = StructType([
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("salary", DoubleType(), False)
    ])
    
    data = [
        ("Alice", 25, 50000.0),
        ("Bob", 30, 60000.0),
        ("Charlie", 35, 70000.0)
    ]
    df3 = spark.createDataFrame(data, schema)
    print("\nSchema:")
    df3.printSchema()
    df3.show()
    
    # Method 4: From range
    print("\n4️⃣  From Range:")
    df4 = spark.range(1, 11).toDF("id")
    df4.show(5)
    
    return df1


def demonstrate_read_operations(spark):
    """
    Demonstrate various read operations.
    """
    print("\n" + "=" * 80)
    print("READ OPERATIONS")
    print("=" * 80)
    
    # Create sample data first
    sample_data = [
        ("Alice", 25, "New York", 50000),
        ("Bob", 30, "San Francisco", 60000),
        ("Charlie", 35, "Los Angeles", 70000),
        ("David", 28, "Chicago", 55000),
        ("Eve", 32, "Boston", 65000)
    ]
    
    df = spark.createDataFrame(sample_data, ["name", "age", "city", "salary"])
    
    # Write in different formats
    print("\n📝 Writing sample data in multiple formats...")
    
    # CSV
    df.write.mode("overwrite").csv("/tmp/spark_test_csv", header=True)
    print("   ✅ Wrote CSV")
    
    # JSON
    df.write.mode("overwrite").json("/tmp/spark_test_json")
    print("   ✅ Wrote JSON")
    
    # Parquet
    df.write.mode("overwrite").parquet("/tmp/spark_test_parquet")
    print("   ✅ Wrote Parquet")
    
    # Now read them back
    print("\n📖 Reading data back:")
    
    # Read CSV
    print("\n1️⃣  Read CSV:")
    csv_df = spark.read.csv("/tmp/spark_test_csv", header=True, inferSchema=True)
    csv_df.show(3)
    
    # Read JSON
    print("2️⃣  Read JSON:")
    json_df = spark.read.json("/tmp/spark_test_json")
    json_df.show(3)
    
    # Read Parquet
    print("3️⃣  Read Parquet:")
    parquet_df = spark.read.parquet("/tmp/spark_test_parquet")
    parquet_df.show(3)
    
    # Read with options
    print("4️⃣  Read CSV with Options:")
    csv_options_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("mode", "DROPMALFORMED") \
        .csv("/tmp/spark_test_csv")
    csv_options_df.show(3)
    
    return df


def demonstrate_sql_operations(spark, df):
    """
    Demonstrate SQL operations on DataFrames.
    """
    print("\n" + "=" * 80)
    print("SQL OPERATIONS")
    print("=" * 80)
    
    # Register DataFrame as temp view
    df.createOrReplaceTempView("employees")
    print("✅ Registered DataFrame as 'employees' table")
    
    # SQL Query 1: Simple SELECT
    print("\n1️⃣  Simple SELECT:")
    result1 = spark.sql("""
        SELECT name, age, salary
        FROM employees
        WHERE age > 28
    """)
    result1.show()
    
    # SQL Query 2: Aggregation
    print("2️⃣  Aggregation:")
    result2 = spark.sql("""
        SELECT 
            city,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY city
        ORDER BY avg_salary DESC
    """)
    result2.show()
    
    # SQL Query 3: Complex query
    print("3️⃣  Complex Query with Subquery:")
    result3 = spark.sql("""
        SELECT name, age, salary,
               salary - avg_salary as salary_diff
        FROM employees
        CROSS JOIN (
            SELECT AVG(salary) as avg_salary FROM employees
        )
        WHERE salary > (SELECT AVG(salary) FROM employees)
    """)
    result3.show()
    
    # SQL Query 4: Window function
    print("4️⃣  Window Function:")
    result4 = spark.sql("""
        SELECT 
            name, 
            age, 
            salary,
            RANK() OVER (ORDER BY salary DESC) as salary_rank
        FROM employees
    """)
    result4.show()


def demonstrate_catalog_operations(spark):
    """
    Demonstrate Catalog operations.
    """
    print("\n" + "=" * 80)
    print("CATALOG OPERATIONS")
    print("=" * 80)
    
    # List databases
    print("\n📊 Databases:")
    databases = spark.catalog.listDatabases()
    for db in databases:
        print(f"   - {db.name}")
    
    # List tables
    print("\n📋 Tables:")
    tables = spark.catalog.listTables()
    if tables:
        for table in tables:
            print(f"   - {table.name} (type: {table.tableType})")
    else:
        print("   No tables found")
    
    # Current database
    print(f"\n🎯 Current Database: {spark.catalog.currentDatabase()}")
    
    # Check if table exists
    print(f"\n🔍 Table 'employees' exists: {spark.catalog.tableExists('employees')}")
    
    # Get table details
    if spark.catalog.tableExists('employees'):
        print("\n📄 'employees' Table Columns:")
        columns = spark.catalog.listColumns('employees')
        for col in columns:
            print(f"   - {col.name}: {col.dataType}")


def demonstrate_udf_registration(spark):
    """
    Demonstrate UDF registration and usage.
    """
    print("\n" + "=" * 80)
    print("UDF REGISTRATION")
    print("=" * 80)
    
    # Define UDF function
    def categorize_salary(salary):
        if salary < 55000:
            return "Low"
        elif salary < 65000:
            return "Medium"
        else:
            return "High"
    
    # Register UDF
    spark.udf.register("categorize_salary", categorize_salary, StringType())
    print("✅ Registered UDF: categorize_salary")
    
    # Use in SQL
    print("\n📊 Using UDF in SQL:")
    result = spark.sql("""
        SELECT 
            name, 
            salary,
            categorize_salary(salary) as salary_category
        FROM employees
    """)
    result.show()
    
    # Use with DataFrame API
    from pyspark.sql.functions import udf
    categorize_udf = udf(categorize_salary, StringType())
    
    print("📊 Using UDF in DataFrame API:")
    df = spark.table("employees")
    df.withColumn("salary_category", categorize_udf(col("salary"))).show()


def demonstrate_session_management(spark):
    """
    Demonstrate session management operations.
    """
    print("\n" + "=" * 80)
    print("SESSION MANAGEMENT")
    print("=" * 80)
    
    # Get active session
    print("\n1️⃣  Get Active Session:")
    active_spark = SparkSession.getActiveSession()
    if active_spark:
        print(f"   ✅ Active session exists: {active_spark.sparkContext.appName}")
    
    # Get or create
    print("\n2️⃣  Get Or Create (reuses existing):")
    spark2 = SparkSession.builder.appName("AnotherApp").getOrCreate()
    print(f"   App Name: {spark2.sparkContext.appName}")
    print(f"   Same session: {spark == spark2}")
    
    # Create new session
    print("\n3️⃣  Create New Session:")
    new_spark = spark.newSession()
    print(f"   New App Name: {new_spark.sparkContext.appName}")
    print(f"   Same SparkContext: {spark.sparkContext == new_spark.sparkContext}")
    print(f"   Different Session: {spark != new_spark}")
    
    # Version info
    print("\n4️⃣  Version Information:")
    print(f"   Spark Version: {spark.version}")
    print(f"   Python Version: {spark.sparkContext.pythonVer}")
    
    # Configuration access
    print("\n5️⃣  Access Configuration:")
    conf = spark.conf
    print(f"   Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions')}")
    
    # Set runtime config
    print("\n6️⃣  Set Runtime Configuration:")
    spark.conf.set("spark.sql.shuffle.partitions", "16")
    print(f"   Updated Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")
    
    # Stop new session
    new_spark.stop()
    print("\n   ✅ Stopped new session")


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("SPARK SESSION - COMPREHENSIVE EXAMPLES")
    print("🎯" * 40)
    
    # Create session
    spark = create_configured_session()
    
    # Demonstrate operations
    df = demonstrate_createdataframe(spark)
    df = demonstrate_read_operations(spark)
    demonstrate_sql_operations(spark, df)
    demonstrate_catalog_operations(spark)
    demonstrate_udf_registration(spark)
    demonstrate_session_management(spark)
    
    print("\n" + "=" * 80)
    print("✅ SPARK SESSION EXAMPLES COMPLETE")
    print("=" * 80)
    print("\n📚 Key Takeaways:")
    print("   1. SparkSession is the entry point for all PySpark operations")
    print("   2. Configure session for optimal performance")
    print("   3. createDataFrame() has multiple convenient overloads")
    print("   4. Read/write support many formats (CSV, JSON, Parquet, etc.)")
    print("   5. SQL operations work seamlessly with DataFrames")
    print("   6. Catalog provides metadata operations")
    print("   7. UDFs extend built-in functionality")
    print("   8. Session management allows multiple contexts")
    
    # Cleanup
    spark.stop()
    print("\n✅ SparkSession stopped")


if __name__ == "__main__":
    main()
