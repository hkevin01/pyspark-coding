# 🐝 PySpark + Hive Integration

Complete guide to using Apache Hive with PySpark for SQL-based data warehousing and analytics.

## 📋 Overview

This folder contains comprehensive examples of integrating PySpark with Apache Hive for:
- SQL-based data processing
- Metastore management
- Table creation and management
- Advanced analytics with HiveQL
- Performance optimization

## 📁 Files

### Configuration
- **hive-site.xml** - Hive configuration for PySpark integration
  - Metastore settings (Derby embedded)
  - Warehouse location configuration
  - Dynamic partitioning setup
  - Performance tuning parameters

### Examples

#### 01_hive_basics.py
**Difficulty:** ⭐⭐⭐☆☆ (3/5)  
**Time:** 25 minutes

Learn the fundamentals:
- Enable Hive support in SparkSession
- Create databases and managed tables
- Run HiveQL queries
- Understand partitioning
- Create external tables
- Insert and manage data

```bash
python 01_hive_basics.py
```

#### 02_advanced_hive.py
**Difficulty:** ⭐⭐⭐⭐☆ (4/5)  
**Time:** 35 minutes

Master advanced features:
- Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- File formats (ORC, Parquet)
- Bucketing for performance
- Views and CTEs
- Dynamic partitioning
- Complex joins
- Performance optimization

```bash
python 02_advanced_hive.py
```

## 🚀 Quick Start

### 1. Basic Setup

```python
from pyspark.sql import SparkSession

# Create Spark session with Hive support
spark = SparkSession.builder \
    .appName("HiveExample") \
    .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
    .enableHiveSupport() \
    .getOrCreate()
```

### 2. Create Database and Table

```python
# Create database
spark.sql("CREATE DATABASE IF NOT EXISTS my_db")
spark.sql("USE my_db")

# Create table
spark.sql("""
    CREATE TABLE IF NOT EXISTS users (
        id INT,
        name STRING,
        age INT
    )
    PARTITIONED BY (country STRING)
    STORED AS PARQUET
""")
```

### 3. Run HiveQL Queries

```python
# Insert data
spark.sql("""
    INSERT INTO users PARTITION(country='US')
    VALUES (1, 'Alice', 30), (2, 'Bob', 25)
""")

# Query data
result = spark.sql("""
    SELECT name, age 
    FROM users 
    WHERE country = 'US' AND age > 20
""")
result.show()
```

## 🎯 Key Concepts

### Managed vs External Tables

**Managed Tables:**
- Hive manages both data and metadata
- Dropping table deletes data
- Use for Hive-only data

```sql
CREATE TABLE managed_table (...) STORED AS PARQUET;
```

**External Tables:**
- Hive manages only metadata
- Dropping table keeps data
- Use for shared data

```sql
CREATE EXTERNAL TABLE external_table (...)
LOCATION '/path/to/data';
```

### Partitioning

Organize data for faster queries:

```python
# Static partitioning
df.write.mode("overwrite") \
    .partitionBy("year", "month") \
    .saveAsTable("partitioned_table")

# Query specific partition
spark.sql("""
    SELECT * FROM partitioned_table 
    WHERE year = 2024 AND month = 1
""")
```

### File Formats

**ORC (Optimized Row Columnar):**
- Best for Hive workloads
- Excellent compression
- Built-in indexes

**Parquet:**
- Cross-platform compatibility
- Great for analytics
- Wide ecosystem support

```python
# ORC table
df.write.format("orc").saveAsTable("orc_table")

# Parquet table
df.write.format("parquet").saveAsTable("parquet_table")
```

## 🔧 Configuration

### hive-site.xml Key Settings

```xml
<!-- Warehouse location -->
<property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp/spark-warehouse</value>
</property>

<!-- Dynamic partitioning -->
<property>
    <name>hive.exec.dynamic.partition.mode</name>
    <value>nonstrict</value>
</property>

<!-- Execution engine -->
<property>
    <name>hive.execution.engine</name>
    <value>spark</value>
</property>
```

## 💡 Best Practices

### 1. Partitioning Strategy
- Partition by frequently filtered columns
- Keep partition size around 1GB
- Don't over-partition (too many small files)

### 2. File Format Selection
- Use **ORC** for pure Hive workloads
- Use **Parquet** for multi-tool environments
- Enable compression for storage savings

### 3. Performance Optimization
```python
# Collect statistics
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")

# Cache frequently used tables
spark.sql("CACHE TABLE my_table")

# Use bucketing for large tables
df.write.bucketBy(10, "id").saveAsTable("bucketed_table")
```

### 4. Query Optimization
- Use predicate pushdown (filter early)
- Avoid SELECT * (specify columns)
- Use appropriate join strategies
- Leverage partitioning in WHERE clauses

## 📊 Common Operations

### Create Database
```sql
CREATE DATABASE IF NOT EXISTS sales_db;
USE sales_db;
```

### Create Partitioned Table
```sql
CREATE TABLE sales (
    order_id INT,
    amount DOUBLE,
    product STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;
```

### Insert Data
```sql
INSERT INTO sales PARTITION(year=2024, month=1)
VALUES (1, 150.00, 'Laptop'), (2, 25.00, 'Mouse');
```

### Query with Window Function
```sql
SELECT 
    product,
    amount,
    ROW_NUMBER() OVER (PARTITION BY product ORDER BY amount DESC) as rank
FROM sales
WHERE year = 2024;
```

### Create View
```sql
CREATE VIEW high_value_sales AS
SELECT * FROM sales
WHERE amount > 1000;
```

## 🐛 Troubleshooting

### Issue: Metastore Connection Error
**Solution:** Check Derby database permissions
```bash
rm -rf /tmp/metastore_db
# Restart Spark session
```

### Issue: Partition Not Found
**Solution:** Refresh partition metadata
```sql
MSCK REPAIR TABLE my_table;
```

### Issue: Slow Queries
**Solution:** Collect statistics and optimize
```sql
ANALYZE TABLE my_table COMPUTE STATISTICS;
ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS;
```

## 📚 Additional Resources

- [Apache Hive Documentation](https://hive.apache.org/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Hive Performance Tuning](https://cwiki.apache.org/confluence/display/Hive/LanguageManual)

## 🎓 Learning Path

1. **Start:** `01_hive_basics.py` - Learn fundamentals
2. **Advance:** `02_advanced_hive.py` - Master complex operations
3. **Practice:** Create your own data warehouse project
4. **Master:** Optimize production workloads

## ✅ Prerequisites

- PySpark installed
- Basic SQL knowledge
- Understanding of DataFrames
- Familiarity with data warehousing concepts

## 🎯 Real-World Use Cases

1. **Data Warehouse:** Centralized analytics repository
2. **ETL Pipelines:** Transform and load data
3. **Reporting:** SQL-based business intelligence
4. **Data Lakes:** Metadata management for big data
5. **Analytics:** Complex queries and aggregations

---

**Ready to start?** Run `python 01_hive_basics.py` and begin your Hive journey! 🚀
