"""
Hive Integration with PySpark Example

This module demonstrates how to use Apache Hive with PySpark, showcasing:
• Hive's SQL-to-MapReduce/Tez translation
• Schema-on-read flexibility
• HBase integration
• Batch processing patterns
• Data warehousing workflows
• Hive metastore integration

Hive makes big data analytics accessible to SQL users without requiring
extensive programming skills.

Author: PySpark Learning Series
Date: December 2024
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_hive_enabled_spark_session():
    """
    Create Spark session with Hive support enabled.

    CONNECTING PYSPARK TO HIVE - REQUIREMENTS:
    =========================================
    To connect PySpark to Hive, you need:

    1. HIVE INSTALLED AND CONFIGURED:
       • Hive binaries installed
       • hive-site.xml configuration file
       • Location: $HIVE_HOME/conf/hive-site.xml

       Key configurations in hive-site.xml:
       <configuration>
         <property>
           <name>hive.metastore.uris</name>
           <value>thrift://localhost:9083</value>
         </property>
         <property>
           <name>hive.metastore.warehouse.dir</name>
           <value>/user/hive/warehouse</value>
         </property>
       </configuration>

    2. RUNNING HIVE METASTORE:
       • Start metastore service:
         $ hive --service metastore

       • Metastore stores metadata (schemas, tables, partitions)
       • Default port: 9083
       • Database backend: Derby, MySQL, or PostgreSQL

    3. SPARK WITH HIVE SUPPORT:
       • Option A: Spark built with Hive support
         Check: spark-submit --version (should show "with Hive")

       • Option B: Add Hive jars to Spark classpath
         spark.jars = /path/to/hive-metastore.jar,
                      /path/to/hive-exec.jar,
                      /path/to/hive-common.jar

       • Copy hive-site.xml to $SPARK_HOME/conf/

    CONNECTION VERIFICATION:
    =======================
    # Check if Hive support is available
    spark.conf.get("spark.sql.catalogImplementation")
    # Returns: "hive" (if enabled) or "in-memory" (if not)

    TROUBLESHOOTING:
    ===============
    Error: "java.lang.ClassNotFoundException: org.apache.hadoop.hive.conf.HiveConf"
    Solution: Add Hive jars to Spark classpath

    Error: "Could not connect to meta store"
    Solution: Start Hive metastore service

    Error: "Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient"
    Solution: Check hive-site.xml configuration

    WHY YOU MIGHT NOT ALWAYS ENABLE HIVE:
    =====================================
    1. EXTRA DEPENDENCIES:
       • Hive support requires Hive libraries and a metastore
       • If you don't need them, enabling Hive adds unnecessary overhead
       • Increases JAR file size and deployment complexity

    2. STARTUP COST:
       • SparkSession with Hive support takes longer to initialize
       • Must connect to Hive metastore on startup
       • Added latency for each Spark application start

    3. COMPLEXITY:
       • Hive introduces schema enforcement
       • Requires permissions and access control setup
       • Needs configuration files (hive-site.xml)
       • For quick, lightweight jobs, this can be overkill

    4. PORTABILITY:
       • Running Spark in environments without Hive installed causes errors
       • Local development environments may not have Hive
       • Lightweight clusters may not need full Hive infrastructure
       • Makes code less portable across different environments

    5. RESOURCE USAGE:
       • Hive metastore queries add latency
       • Extra network calls to metastore for metadata
       • If you only need temporary Spark tables, Hive is slower
       • Spark's in-memory catalog is faster for ephemeral data

    WHEN TO USE HIVE:
    • Need persistent tables across sessions
    • Sharing data with other Hive/SQL users
    • Enterprise data warehouse integration
    • Complex schema management requirements

    WHEN NOT TO USE HIVE:
    • One-off data processing jobs
    • Local development and testing
    • Temporary transformations
    • Simple ETL without persistent storage

    WHAT IS HIVE?
    =============
    Apache Hive is a data warehouse system built on top of Hadoop that provides:
    • SQL interface (HiveQL) for querying data
    • Translation of SQL → MapReduce/Tez/Spark jobs
    • Schema-on-read (flexible data structures)
    • Metastore (centralized metadata catalog)
    • Integration with Hadoop ecosystem

    KEY CONCEPT: SQL-to-MapReduce Translation
    =========================================
    HiveQL Query:
    SELECT department, AVG(salary)
    FROM employees
    WHERE year = 2024
    GROUP BY department;

    Hive translates this to:
    ┌─────────────────────────────────────────┐
    │ Step 1: MAP Phase                       │
    │ • Read employees data from HDFS         │
    │ • Filter: year = 2024                   │
    │ • Emit: (department, salary)            │
    └─────────────────────────────────────────┘
                    ↓
    ┌─────────────────────────────────────────┐
    │ Step 2: SHUFFLE & SORT                  │
    │ • Group by department                   │
    │ • Engineering: [50K, 60K, 55K]         │
    │ • Sales: [45K, 48K]                    │
    └─────────────────────────────────────────┘
                    ↓
    ┌─────────────────────────────────────────┐
    │ Step 3: REDUCE Phase                    │
    │ • Calculate AVG per department          │
    │ • Engineering: 55K                      │
    │ • Sales: 46.5K                         │
    └─────────────────────────────────────────┘

    NO PROGRAMMING REQUIRED - Just write SQL!

    Returns:
        SparkSession with Hive support
    """
    print("\n" + "=" * 70)
    print("CREATING HIVE-ENABLED SPARK SESSION")
    print("=" * 70)

    spark = (
        SparkSession.builder.appName("Hive Integration Example")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )

    print("\n✅ Spark session created with Hive support")
    print(f"   Warehouse location: {spark.conf.get('spark.sql.warehouse.dir')}")
    print(f"   Catalog: {spark.conf.get('spark.sql.catalogImplementation')}")

    return spark


def example_1_basic_hive_table_operations(spark):
    """
    Demonstrate basic Hive table operations.

    HIVE TABLE TYPES:
    ================
    1. MANAGED TABLES (Internal):
       • Hive manages both data and metadata
       • DROP TABLE deletes data and metadata
       • Stored in Hive warehouse directory

    2. EXTERNAL TABLES:
       • Hive manages only metadata
       • DROP TABLE deletes only metadata (data preserved)
       • Data stored in user-specified location

    SCHEMA-ON-READ:
    ==============
    Traditional databases: Schema-on-write (enforce schema on insert)
    Hive: Schema-on-read (apply schema when querying)

    Benefit: Flexibility!
    • Store raw data in any format
    • Define schema later
    • Multiple schemas for same data

    Example:
    File: /data/logs.txt (CSV)
    timestamp,user,action,duration
    2024-01-01,Alice,login,10
    2024-01-01,Bob,purchase,30

    Schema 1 (Analysis):
    CREATE TABLE user_actions (ts STRING, user STRING, action STRING, duration INT);

    Schema 2 (Audit):
    CREATE TABLE audit_log (timestamp STRING, username STRING, event STRING);

    Same data, different schemas - applied at read time!
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 1: Basic Hive Table Operations")
    print("=" * 70)

    # Create database
    print("\n📁 Creating Hive database...")
    spark.sql("CREATE DATABASE IF NOT EXISTS company")
    spark.sql("USE company")
    print("   ✅ Database 'company' created")

    # Show current database
    current_db = spark.sql("SELECT current_database()").collect()[0][0]
    print(f"   Current database: {current_db}")

    # Create sample employee data
    print("\n📊 Creating employee dataset...")
    employees_data = [
        (1, "Alice", "Engineering", 50000, 30, 2024),
        (2, "Bob", "Sales", 45000, 35, 2024),
        (3, "Charlie", "Engineering", 60000, 28, 2024),
        (4, "Diana", "Marketing", 48000, 32, 2024),
        (5, "Eve", "Engineering", 55000, 29, 2024),
        (6, "Frank", "Sales", 47000, 33, 2024),
        (7, "Grace", "Engineering", 62000, 27, 2024),
        (8, "Henry", "Marketing", 50000, 31, 2024),
    ]

    employees_df = spark.createDataFrame(
        employees_data, ["id", "name", "department", "salary", "age", "year"]
    )

    print(f"   Created {employees_df.count()} employee records")
    employees_df.show()

    # Create managed table
    print("\n💾 Creating MANAGED table...")
    employees_df.write.mode("overwrite").saveAsTable("employees")
    print("   ✅ Managed table 'employees' created")

    # Query table using HiveQL
    print("\n🔍 Querying with HiveQL (SQL-like syntax)...")
    print("   Query: SELECT department, AVG(salary) GROUP BY department")

    result = spark.sql(
        """
        SELECT department, 
               ROUND(AVG(salary), 2) as avg_salary,
               COUNT(*) as employee_count
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """
    )
    result.show()

    # Show table metadata
    print("\n📋 Table metadata:")
    spark.sql("DESCRIBE FORMATTED employees").show(50, truncate=False)

    # Create partitioned table
    print("\n📂 Creating PARTITIONED table...")
    print("   Partitioning by 'department' for efficient queries")

    employees_df.write.mode("overwrite").partitionBy("department").saveAsTable(
        "employees_partitioned"
    )

    print("   ✅ Partitioned table created")
    print("\n   Directory structure:")
    print("   /warehouse/company.db/employees_partitioned/")
    print("   ├── department=Engineering/")
    print("   │   └── part-00000.snappy.parquet")
    print("   ├── department=Sales/")
    print("   │   └── part-00000.snappy.parquet")
    print("   └── department=Marketing/")
    print("       └── part-00000.snappy.parquet")

    # Query partitioned table
    print("\n🚀 Querying partitioned table (faster!)...")
    print("   Query: SELECT * WHERE department = 'Engineering'")
    print("   Benefit: Only reads Engineering partition!")

    eng_result = spark.sql(
        """
        SELECT * FROM employees_partitioned 
        WHERE department = 'Engineering'
    """
    )
    eng_result.show()

    # Show all databases
    print("\n🗂️  All databases:")
    spark.sql("SHOW DATABASES").show()

    # Show all tables
    print("\n📋 All tables in 'company' database:")
    spark.sql("SHOW TABLES IN company").show()


def example_2_schema_on_read_flexibility(spark):
    """
    Demonstrate Hive's schema-on-read flexibility.

    SCHEMA-ON-READ vs SCHEMA-ON-WRITE:
    ==================================

    SCHEMA-ON-WRITE (Traditional databases):
    ┌─────────────────────────────────────────┐
    │ INSERT INTO users VALUES (...)          │
    │          ↓                              │
    │ Validate schema (fail if mismatch)      │
    │          ↓                              │
    │ Write to disk                           │
    └─────────────────────────────────────────┘

    Problem: Must know schema upfront, hard to change

    SCHEMA-ON-READ (Hive):
    ┌─────────────────────────────────────────┐
    │ Store raw data (any format)             │
    │          ↓                              │
    │ Define schema when reading              │
    │          ↓                              │
    │ Apply schema to data                    │
    └─────────────────────────────────────────┘

    Benefit: Flexibility! Change schema anytime.

    REAL-WORLD SCENARIO:
    ===================
    You have log files from 3 systems with different formats:

    System A logs: timestamp,user,action
    System B logs: time,username,event,status
    System C logs: date,user_id,activity,duration

    Solution: Store all as-is, define schemas when querying!
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 2: Schema-on-Read Flexibility")
    print("=" * 70)

    spark.sql("USE company")

    # Create external table from CSV
    print("\n📄 Creating EXTERNAL table from CSV...")

    # Write sample CSV data
    csv_data = [
        ("2024-01-01", "Alice", "login", 10),
        ("2024-01-01", "Bob", "purchase", 30),
        ("2024-01-02", "Alice", "logout", 5),
        ("2024-01-02", "Charlie", "browse", 120),
    ]

    logs_df = spark.createDataFrame(
        csv_data, ["timestamp", "user", "action", "duration"]
    )

    # Write as CSV
    csv_path = "/tmp/raw_logs"
    logs_df.write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"   ✅ CSV data written to {csv_path}")

    # Create external table (schema defined, data stays in original location)
    print("\n🔗 Creating external table pointing to CSV...")

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS user_logs (
            timestamp STRING,
            user STRING,
            action STRING,
            duration INT
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{csv_path}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """
    )

    print("   ✅ External table 'user_logs' created")
    print("   Note: Data stays at original location")

    # Query external table
    print("\n🔍 Querying external table...")
    spark.sql("SELECT * FROM user_logs").show()

    # Now change schema interpretation (schema-on-read!)
    print("\n🔄 Creating ALTERNATE schema for same data...")
    print("   Same data, different interpretation!")

    spark.sql(
        f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS audit_trail (
            event_time STRING,
            username STRING,
            event_type STRING,
            event_duration_seconds INT
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        LOCATION '{csv_path}'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """
    )

    print("   ✅ Alternate table 'audit_trail' created")
    print("   Note: Same data, different column names!")

    # Query alternate schema
    print("\n📊 Querying with alternate schema...")
    spark.sql("SELECT * FROM audit_trail").show()

    # Drop external table (data preserved)
    print("\n🗑️  Dropping external table...")
    spark.sql("DROP TABLE IF EXISTS audit_trail")
    print("   ✅ Table dropped, but CSV data still exists!")

    # Verify data still exists
    print("\n✅ Verifying original CSV data still exists...")
    df_check = spark.read.option("header", "true").csv(csv_path)
    print(f"   Records in CSV: {df_check.count()}")

    print("\n💡 KEY INSIGHT:")
    print("   External tables: DROP TABLE only removes metadata")
    print("   Managed tables: DROP TABLE removes data AND metadata")


def example_3_hive_query_execution(spark):
    """
    Demonstrate how Hive translates SQL to execution plans.

    HIVE EXECUTION ENGINES:
    ======================
    1. MapReduce (Original):
       • Slower (writes intermediate data to disk)
       • Good for batch processing
       • Fault-tolerant

    2. Apache Tez (Faster):
       • DAG-based execution (Directed Acyclic Graph)
       • In-memory intermediate data
       • 5-10× faster than MapReduce

    3. Spark (Fastest):
       • In-memory computation
       • Optimized query planning
       • 10-100× faster than MapReduce

    QUERY EXECUTION FLOW:
    ====================
    User writes HiveQL:
    SELECT department, AVG(salary)
    FROM employees
    WHERE year = 2024
    GROUP BY department;

    Step 1: PARSE
    ├─ Parse SQL into Abstract Syntax Tree (AST)
    └─ Validate syntax

    Step 2: ANALYZE
    ├─ Check table exists in metastore
    ├─ Check column names valid
    └─ Infer data types

    Step 3: OPTIMIZE
    ├─ Predicate pushdown (filter early)
    ├─ Column pruning (read only needed columns)
    └─ Join optimization

    Step 4: EXECUTE
    ├─ Generate MapReduce/Tez/Spark job
    ├─ Submit to YARN
    └─ Monitor execution

    Step 5: RETURN RESULTS
    └─ Collect results and display
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 3: Hive Query Execution")
    print("=" * 70)

    spark.sql("USE company")

    # Complex query demonstrating SQL-to-execution translation
    print("\n🔍 Complex analytical query...")
    print("   Hive translates this SQL to optimized execution plan:")

    query = """
        SELECT 
            department,
            COUNT(*) as employee_count,
            ROUND(AVG(salary), 2) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary,
            ROUND(STDDEV(salary), 2) as salary_stddev
        FROM employees
        WHERE year = 2024 AND salary > 40000
        GROUP BY department
        HAVING employee_count > 1
        ORDER BY avg_salary DESC
    """

    print(f"\n   Query:\n{query}")

    # Show execution plan
    print("\n📋 EXECUTION PLAN:")
    print("   (What Hive does behind the scenes)")
    spark.sql(f"EXPLAIN EXTENDED {query}").show(truncate=False)

    # Execute query
    print("\n⚡ Executing query...")
    start_time = time.time()
    result = spark.sql(query)
    result.show()
    execution_time = time.time() - start_time

    print(f"\n   ⏱️  Execution time: {execution_time:.3f} seconds")
    print("   Note: Hive handled all the complexity!")

    # Show query stages
    print("\n🎯 WHAT HIVE DID:")
    print(
        """
   1. FILTER: WHERE year = 2024 AND salary > 40000
      └─ Read only needed rows (predicate pushdown)
   
   2. PROJECT: SELECT department, salary
      └─ Read only needed columns (column pruning)
   
   3. AGGREGATE: GROUP BY department
      └─ Calculate COUNT, AVG, MIN, MAX, STDDEV
   
   4. FILTER: HAVING employee_count > 1
      └─ Filter aggregated results
   
   5. SORT: ORDER BY avg_salary DESC
      └─ Sort final results
   
   No MapReduce programming needed - just SQL!
    """
    )


def example_4_batch_processing_patterns(spark):
    """
    Demonstrate Hive batch processing and ETL patterns.

    BATCH PROCESSING:
    ================
    Hive is optimized for batch processing:
    • Process large volumes of data
    • Not real-time (latency: seconds to hours)
    • Cost-effective for big data

    COMMON BATCH PATTERNS:
    =====================
    1. Daily Aggregation:
       • Summarize yesterday's transactions
       • Generate daily reports

    2. ETL (Extract, Transform, Load):
       • Extract: Read from source systems
       • Transform: Clean, join, aggregate
       • Load: Write to target warehouse

    3. Data Summarization:
       • Hourly → Daily → Monthly rollups
       • Dimension tables for OLAP

    4. Historical Analysis:
       • Year-over-year comparisons
       • Trend analysis
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 4: Batch Processing Patterns")
    print("=" * 70)

    spark.sql("USE company")

    # Create fact table (large, transactional)
    print("\n📊 Creating fact table (sales transactions)...")

    sales_data = []
    for i in range(1, 101):
        sales_data.append(
            (
                i,
                f"2024-01-{(i % 30) + 1:02d}",
                f"Product_{i % 10}",
                i % 8 + 1,  # employee_id
                (i * 37) % 500 + 100,  # amount
                2024,
            )
        )

    sales_df = spark.createDataFrame(
        sales_data, ["sale_id", "sale_date", "product", "employee_id", "amount", "year"]
    )

    print(f"   Created {sales_df.count()} sales records")

    # Write partitioned by year and month
    sales_df_with_month = sales_df.withColumn(
        "month", substring(col("sale_date"), 6, 2)
    )

    sales_df_with_month.write.mode("overwrite").partitionBy(
        "year", "month"
    ).saveAsTable("sales_fact")

    print("   ✅ Fact table 'sales_fact' created (partitioned by year, month)")

    # ETL Pattern 1: Daily aggregation
    print("\n📈 ETL Pattern 1: Daily Sales Aggregation")

    daily_agg = spark.sql(
        """
        SELECT 
            sale_date,
            COUNT(*) as transaction_count,
            ROUND(SUM(amount), 2) as total_sales,
            ROUND(AVG(amount), 2) as avg_sale
        FROM sales_fact
        WHERE year = 2024
        GROUP BY sale_date
        ORDER BY sale_date
    """
    )

    print("\n   Creating 'daily_sales_summary' table...")
    daily_agg.write.mode("overwrite").saveAsTable("daily_sales_summary")
    print("   ✅ Daily summary created")
    daily_agg.show(10)

    # ETL Pattern 2: Join with dimension table
    print("\n🔗 ETL Pattern 2: Join Fact with Dimension")

    enriched = spark.sql(
        """
        SELECT 
            s.sale_date,
            e.name as employee_name,
            e.department,
            s.product,
            s.amount
        FROM sales_fact s
        JOIN employees e ON s.employee_id = e.id
        WHERE s.year = 2024
        ORDER BY s.sale_date, s.amount DESC
    """
    )

    print("\n   Enriched sales data (with employee info):")
    enriched.show(10)

    # ETL Pattern 3: Department performance
    print("\n🏆 ETL Pattern 3: Department Performance Summary")

    dept_performance = spark.sql(
        """
        SELECT 
            e.department,
            COUNT(DISTINCT s.employee_id) as num_sellers,
            COUNT(*) as total_sales,
            ROUND(SUM(s.amount), 2) as revenue,
            ROUND(AVG(s.amount), 2) as avg_sale_amount
        FROM sales_fact s
        JOIN employees e ON s.employee_id = e.id
        GROUP BY e.department
        ORDER BY revenue DESC
    """
    )

    print("\n   Department performance:")
    dept_performance.show()

    # Create summary table
    dept_performance.write.mode("overwrite").saveAsTable("department_performance")
    print("\n   ✅ 'department_performance' table created")

    print("\n�� BATCH PROCESSING BENEFITS:")
    print("   ✅ Process large volumes efficiently")
    print("   ✅ Cost-effective (batch vs real-time)")
    print("   ✅ Easy to write (just SQL)")
    print("   ✅ Automatic optimization by Hive")
    print("   ✅ Fault-tolerant execution")


def example_5_hive_accessibility(spark):
    """
    Demonstrate how Hive makes big data accessible to SQL users.

    WHO USES HIVE?
    =============
    1. Data Analysts:
       • Know SQL, not Java/Scala/Python
       • Need to query big data
       • Use familiar SQL syntax

    2. Business Analysts:
       • Generate reports
       • Create dashboards
       • Ad-hoc analysis

    3. Data Scientists:
       • Exploratory data analysis
       • Feature engineering
       • Model training data preparation

    4. BI Tools:
       • Tableau, Power BI connect to Hive
       • Query big data like a database
       • Drag-and-drop interface

    NO PROGRAMMING SKILLS REQUIRED:
    ==============================
    Instead of writing:

    // Java MapReduce (100+ lines)
    public class SalesMapper extends Mapper<...> {
        public void map(...) { ... }
    }
    public class SalesReducer extends Reducer<...> {
        public void reduce(...) { ... }
    }
    // Configure job, handle I/O, error handling, etc.

    Write this in Hive:

    SELECT department, SUM(amount)
    FROM sales
    GROUP BY department;

    That's it! 3 lines vs 100+ lines of code.
    """
    print("\n" + "=" * 70)
    print("EXAMPLE 5: Hive Accessibility")
    print("=" * 70)

    spark.sql("USE company")

    print("\n👥 HIVE DEMOCRATIZES BIG DATA:")
    print("   Anyone who knows SQL can analyze big data!")

    # Typical analyst queries
    print("\n📊 Common Analyst Queries:")

    queries = [
        (
            "Top performing employees",
            """
            SELECT e.name, e.department, COUNT(*) as sales_count, 
                   SUM(s.amount) as total_revenue
            FROM sales_fact s
            JOIN employees e ON s.employee_id = e.id
            GROUP BY e.name, e.department
            ORDER BY total_revenue DESC
            LIMIT 5
        """,
        ),
        (
            "Monthly trends",
            """
            SELECT month, 
                   COUNT(*) as transaction_count,
                   ROUND(SUM(amount), 2) as revenue
            FROM sales_fact
            WHERE year = 2024
            GROUP BY month
            ORDER BY month
        """,
        ),
        (
            "Product performance",
            """
            SELECT product, 
                   COUNT(*) as units_sold,
                   ROUND(AVG(amount), 2) as avg_price
            FROM sales_fact
            GROUP BY product
            ORDER BY units_sold DESC
        """,
        ),
    ]

    for title, query in queries:
        print(f"\n   {title.upper()}:")
        print(f"   {query.strip()}")
        result = spark.sql(query)
        result.show(5)

    # Show metadata (helpful for analysts)
    print("\n🔍 HELPFUL FOR ANALYSTS:")
    print("   View available tables:")
    spark.sql("SHOW TABLES").show()

    print("\n   View table structure:")
    spark.sql("DESCRIBE employees").show()

    print("\n   View table statistics:")
    spark.sql(
        """
        SELECT 
            'employees' as table_name,
            COUNT(*) as row_count
        FROM employees
    """
    ).show()

    print("\n💡 KEY BENEFITS FOR NON-PROGRAMMERS:")
    print(
        """
   1. FAMILIAR SYNTAX:
      • Standard SQL (similar to MySQL, PostgreSQL)
      • No need to learn MapReduce/Spark programming
   
   2. INTERACTIVE ANALYSIS:
      • Write query, get results
      • No compilation, no deployment
   
   3. TOOL INTEGRATION:
      • Excel, Tableau, Power BI can connect
      • Use drag-and-drop interfaces
   
   4. DOCUMENTATION:
      • DESCRIBE tables
      • SHOW tables
      • View partitions, statistics
   
   5. SCALABILITY:
      • Same SQL works on 1 GB or 1 PB
      • Hive handles distribution automatically
    """
    )


def main():
    """
    Run all Hive integration examples.
    """
    print("\n" + "=" * 70)
    print(" HIVE INTEGRATION WITH PYSPARK - COMPLETE GUIDE ")
    print("=" * 70)

    print(
        """
Apache Hive makes big data analytics accessible by providing:

1. SQL Interface:       Write HiveQL (SQL-like) instead of MapReduce code
2. Schema-on-Read:      Flexible data structures, define schema at query time
3. Query Translation:   Automatically converts SQL to MapReduce/Tez/Spark jobs
4. Batch Processing:    Optimized for large-scale data processing
5. Accessibility:       No programming skills required - just SQL

This module demonstrates all key Hive concepts with working examples.
    """
    )

    try:
        # Create Hive-enabled Spark session
        spark = create_hive_enabled_spark_session()

        # Run all examples
        example_1_basic_hive_table_operations(spark)
        example_2_schema_on_read_flexibility(spark)
        example_3_hive_query_execution(spark)
        example_4_batch_processing_patterns(spark)
        example_5_hive_accessibility(spark)

        print("\n" + "=" * 70)
        print("✅ ALL HIVE EXAMPLES COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print("\n📚 KEY TAKEAWAYS:")
        print(
            """
   1. HIVE TRANSLATES SQL TO JOBS:
      • Write HiveQL (SQL-like queries)
      • Hive converts to MapReduce/Tez/Spark
      • No MapReduce programming needed
   
   2. SCHEMA-ON-READ FLEXIBILITY:
      • Store data in any format
      • Define schema when querying
      • Change schema without reprocessing
   
   3. BATCH PROCESSING:
      • Optimized for large-scale analytics
      • Cost-effective for big data
      • ETL and data warehouse patterns
   
   4. ACCESSIBILITY:
      • SQL users can analyze big data
      • Integrates with BI tools
      • No programming skills required
   
   5. HADOOP ECOSYSTEM:
      • Hive Metastore: Centralized metadata
      • HiveQL: SQL dialect
      • HDFS: Distributed storage
      • YARN: Resource management
        """
        )

        # Clean up
        print("\n🧹 Cleaning up demo databases...")
        spark.sql("DROP DATABASE IF EXISTS company CASCADE")
        print("   ✅ Cleanup complete")

        spark.stop()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
