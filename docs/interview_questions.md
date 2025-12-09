# PySpark Interview Questions & Answers
## Prepared for ICF Senior Data Engineer Position

---

## Table of Contents
1. [Core PySpark Concepts](#core-pyspark-concepts)
2. [ETL & Data Processing](#etl--data-processing)
3. [Performance Optimization](#performance-optimization)
4. [Data Quality & Governance](#data-quality--governance)
5. [Architecture & Design](#architecture--design)
6. [Real-World Scenarios](#real-world-scenarios)
7. [ICF-Specific Topics](#icf-specific-topics)

---

## Core PySpark Concepts

### Q1: What's the difference between transformation and action in PySpark?

**Answer:**

**Transformations** are lazy operations that define a computation but don't execute immediately:
- Return a new DataFrame/RDD
- Build up a DAG (Directed Acyclic Graph) of operations
- Examples: `map()`, `filter()`, `select()`, `join()`, `groupBy()`, `withColumn()`

**Actions** trigger actual execution of the computation:
- Return results to the driver or write to storage
- Trigger the execution of all pending transformations
- Examples: `count()`, `collect()`, `show()`, `take()`, `write()`, `first()`

**Example:**
```python
# Transformations (not executed yet)
df2 = df.filter(F.col("age") > 30)  # Lazy
df3 = df2.select("name", "salary")   # Lazy

# Action (triggers execution of filter + select)
df3.show()  # Executes entire pipeline
```

**Why it matters:** Understanding lazy evaluation helps you optimize by chaining transformations before triggering actions, reducing the number of passes over data.

---

### Q2: Explain the difference between `map()` and `mapPartitions()`

**Answer:**

**`map()`:**
- Applies function to each element individually
- Creates iterator for each element
- More overhead due to function calls per element

**`mapPartitions()`:**
- Applies function to each partition (batch of rows)
- Processes entire partition at once
- More efficient for batch operations
- Better for operations with setup/teardown costs

**Example:**
```python
# map - called for each row
rdd.map(lambda x: expensive_function(x))

# mapPartitions - called once per partition
def process_partition(iterator):
    # Setup once per partition
    connection = create_connection()
    results = [process(row, connection) for row in iterator]
    connection.close()
    return iter(results)

rdd.mapPartitions(process_partition)
```

**Best for:** `mapPartitions()` is ideal for database connections, API calls, or ML model inference where setup cost is high.

---

### Q3: What's the difference between DataFrame, Dataset, and RDD?

**Answer:**

| Feature | RDD | DataFrame | Dataset |
|---------|-----|-----------|---------|
| **Type Safety** | No (runtime) | No (runtime) | Yes (compile-time) |
| **Optimization** | No Catalyst | Catalyst optimizer | Catalyst optimizer |
| **API** | Low-level | High-level SQL-like | Type-safe + SQL-like |
| **Language** | All (Python, Scala, Java) | All | Scala/Java only |
| **Performance** | Slower | Fast | Fastest |
| **Schema** | No schema | Schema enforced | Strongly typed |

**When to use:**
- **RDD**: Low-level control, complex computations, legacy code
- **DataFrame**: Most common use case, SQL-like operations, Python (no Dataset support)
- **Dataset**: Scala/Java with type safety requirements

**In Python:** Only DataFrames and RDDs are available (no Dataset API).

---

## ETL & Data Processing

### Q4: How do you handle null values in PySpark? Provide multiple strategies.

**Answer:**

**1. Detect Nulls:**
```python
# Count nulls per column
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) 
           for c in df.columns]).show()

# Filter rows with nulls
df.filter(F.col("age").isNull())
```

**2. Drop Nulls:**
```python
# Drop rows with any null
df.dropna()  # or df.na.drop()

# Drop if specific columns have nulls
df.na.drop(subset=["age", "salary"])

# Drop if fewer than N non-null values
df.na.drop(thresh=3)  # Keep rows with at least 3 non-nulls
```

**3. Fill Nulls:**
```python
# Fill all nulls with a value
df.na.fill(0)  # Numeric
df.na.fill("Unknown")  # String

# Fill specific columns
df.na.fill({"age": 0, "city": "Unknown", "salary": 50000})

# Fill with mean/median
mean_salary = df.select(F.mean("salary")).first()[0]
df.na.fill({"salary": mean_salary})
```

**4. Coalesce (use first non-null):**
```python
# Use salary, or estimate from age if null
df.withColumn("effective_salary", 
    F.coalesce(F.col("salary"), F.col("age") * 1000)
)
```

**5. Replace specific values:**
```python
# Replace specific value
df.withColumn("status", 
    F.when(F.col("status").isNull(), "pending").otherwise(F.col("status"))
)
```

**Best Practice:** Document your null-handling strategy in data quality rules and apply consistently across ETL pipelines.

---

### Q5: How do you read and write different file formats? What are the trade-offs?

**Answer:**

**CSV:**
```python
# Read
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Write
df.write.csv("output.csv", header=True, mode="overwrite")
```
- ✅ **Pros:** Human-readable, universally supported, Excel-compatible
- ❌ **Cons:** No schema enforcement, slow, no compression, no columnar access

**Parquet (Recommended for production):**
```python
# Read
df = spark.read.parquet("data.parquet")

# Write with partitioning
df.write.partitionBy("year", "month").parquet("output.parquet")
```
- ✅ **Pros:** Columnar storage, compressed, schema embedded, predicate pushdown, fast
- ❌ **Cons:** Not human-readable, requires compatible tools

**JSON:**
```python
# Read
df = spark.read.json("data.json")

# Write
df.write.json("output.json", mode="overwrite")
```
- ✅ **Pros:** Flexible schema, nested data support, human-readable
- ❌ **Cons:** Larger files, slower than Parquet, no columnar benefits

**Avro:**
```python
# Read
df = spark.read.format("avro").load("data.avro")
```
- ✅ **Pros:** Schema evolution, compact, good for streaming
- ❌ **Cons:** Not as fast as Parquet for analytics

**Delta Lake (Modern choice):**
```python
# Read
df = spark.read.format("delta").load("data")

# Write
df.write.format("delta").mode("overwrite").save("output")
```
- ✅ **Pros:** ACID transactions, time travel, schema enforcement, upserts/deletes
- ❌ **Cons:** Requires Delta Lake library

**Recommendation for ICF projects:**
- **Raw ingestion:** Parquet (preserves data types, fast)
- **Data lake:** Parquet with partitioning
- **Data warehouse:** Delta Lake (ACID, versioning)
- **External sharing:** CSV (compatibility) or Parquet (technical teams)

---

### Q6: How do you handle schema evolution in production ETL pipelines?

**Answer:**

**1. Schema Inference (Development):**
```python
df = spark.read.option("inferSchema", "true").csv("data.csv")
```

**2. Explicit Schema (Production - Recommended):**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("age", IntegerType(), True)
])

df = spark.read.schema(schema).csv("data.csv")
```

**3. Schema Validation:**
```python
def validate_schema(df, expected_schema):
    """Validate DataFrame matches expected schema"""
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}
    
    missing = set(expected_fields.keys()) - set(actual_fields.keys())
    extra = set(actual_fields.keys()) - set(expected_fields.keys())
    
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if extra:
        print(f"Warning: Extra columns: {extra}")
    
    return True
```

**4. Handling Schema Changes:**
```python
# Add new columns with defaults
df = df.withColumn("new_column", F.lit(None).cast(StringType()))

# Handle missing columns
required_cols = ["id", "name", "email"]
for col in required_cols:
    if col not in df.columns:
        df = df.withColumn(col, F.lit(None))

# Reorder columns to match expected schema
df = df.select(*expected_column_order)
```

**5. Using Delta Lake for Schema Evolution:**
```python
# Allow schema merging
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("output")
```

**Best Practice:** Use Delta Lake in production for automatic schema evolution with versioning and rollback capabilities.

---

## Performance Optimization

### Q7: How do you optimize Spark jobs for better performance?

**Answer:**

**1. Partitioning:**
```python
# Repartition for better parallelism
df = df.repartition(200)  # Balance parallelism

# Partition by column for filtering
df.write.partitionBy("year", "month").parquet("output")

# Coalesce to reduce partitions (fewer files)
df.coalesce(10).write.parquet("output")
```

**2. Caching/Persistence:**
```python
# Cache frequently used DataFrames
df.cache()  # MEMORY_AND_DISK (default)
df.persist(StorageLevel.MEMORY_ONLY)

# Use cached DataFrame multiple times
df.count()
df.groupBy("category").count().show()

# Unpersist when done
df.unpersist()
```

**3. Avoid Shuffles:**
```python
# Bad: Multiple shuffles
df.groupBy("col1").count().groupBy("col2").sum()

# Good: Single groupBy
df.groupBy("col1", "col2").agg(F.count("*"), F.sum("value"))

# Use window functions instead of self-joins
from pyspark.sql.window import Window
window = Window.partitionBy("category").orderBy("date")
df.withColumn("running_total", F.sum("amount").over(window))
```

**4. Broadcast Joins (Small Tables):**
```python
from pyspark.sql.functions import broadcast

# Broadcast small lookup table (<10MB)
result = large_df.join(broadcast(small_df), "key")
```

**5. Predicate Pushdown:**
```python
# Filter before joins (pushdown to storage layer)
df = spark.read.parquet("data") \
    .filter(F.col("date") >= "2024-01-01") \
    .filter(F.col("status") == "active")
```

**6. Use Appropriate File Formats:**
```python
# Parquet with partitioning and column pruning
df = spark.read.parquet("data") \
    .select("id", "name", "amount")  # Column pruning
    .filter(F.col("year") == 2024)    # Partition pruning
```

**7. Optimize Data Types:**
```python
# Use appropriate types to reduce memory
df = df.withColumn("id", F.col("id").cast(IntegerType()))  # Not BigInt if not needed
```

**8. Avoid UDFs (Use Built-in Functions):**
```python
# Bad: Python UDF (slow)
@udf(returnType=StringType())
def categorize(age):
    return "Adult" if age >= 18 else "Minor"

df.withColumn("category", categorize(F.col("age")))

# Good: Built-in functions (fast)
df.withColumn("category", 
    F.when(F.col("age") >= 18, "Adult").otherwise("Minor")
)
```

**9. Tune Spark Configs:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

**10. Monitor with Spark UI:**
- Check DAG visualization
- Identify skewed partitions
- Monitor shuffle read/write
- Check GC time

---

### Q8: What causes data skew and how do you handle it?

**Answer:**

**What is Data Skew?**
Uneven distribution of data across partitions, causing some tasks to take much longer.

**Causes:**
1. Skewed join keys (e.g., many nulls, popular values)
2. Uneven partition sizes
3. Hot keys in aggregations

**Detection:**
```python
# Check partition sizes
df.rdd.glom().map(len).collect()

# Check value distribution
df.groupBy("key").count().orderBy(F.desc("count")).show()
```

**Solutions:**

**1. Salting (for joins):**
```python
# Add random salt to skewed keys
salt_range = 10
df1 = df1.withColumn("salt", (F.rand() * salt_range).cast(IntegerType()))
df2 = df2.withColumn("salt", F.explode(F.array([F.lit(i) for i in range(salt_range)])))

# Join on salted key
result = df1.join(df2, ["key", "salt"])
```

**2. Adaptive Query Execution (AQE):**
```python
# Enable AQE (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

**3. Repartition by column:**
```python
# Better distribution
df = df.repartition(200, "key")
```

**4. Broadcast smaller side:**
```python
# If one side is small enough
result = large_skewed_df.join(broadcast(small_df), "key")
```

**5. Filter nulls separately:**
```python
# Handle nulls separately if they're the skew
df_with_key = df.filter(F.col("key").isNotNull())
df_null_key = df.filter(F.col("key").isNull())

result = df_with_key.join(lookup, "key").union(df_null_key)
```

---

### Q9: Explain the difference between `cache()`, `persist()`, and checkpointing.

**Answer:**

**`cache()`:**
```python
df.cache()  # Default: MEMORY_AND_DISK
```
- Stores DataFrame in memory and disk
- Lineage preserved (can recompute if lost)
- Cleared when SparkSession ends
- Good for: DataFrames used 2-3 times

**`persist()`:**
```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)      # Memory only
df.persist(StorageLevel.DISK_ONLY)        # Disk only
df.persist(StorageLevel.MEMORY_AND_DISK)  # Both (like cache)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # Serialized
```
- More control over storage level
- Choose based on memory constraints
- Lineage preserved

**Checkpointing:**
```python
spark.sparkContext.setCheckpointDir("hdfs://checkpoint")
df.checkpoint()  # Truncates lineage
```
- Saves to reliable storage (HDFS, S3)
- Truncates lineage (breaks dependency chain)
- Survives SparkSession restart
- Good for: Very long lineage chains, iterative algorithms

**Comparison:**

| Feature | cache() | persist() | checkpoint() |
|---------|---------|-----------|--------------|
| **Storage** | Memory + Disk | Configurable | HDFS/S3 |
| **Lineage** | Preserved | Preserved | Truncated |
| **Durability** | Session-scoped | Session-scoped | Persistent |
| **Use Case** | Quick reuse | Memory control | Long lineage |

**When to use:**
- **Cache**: DataFrame used 2-3 times in same job
- **Persist MEMORY_ONLY**: Enough memory, fast access
- **Persist DISK_ONLY**: Limited memory, can afford slower access
- **Checkpoint**: Very long lineage (>100 transformations), iterative ML

---

## Data Quality & Governance

### Q10: How do you implement data quality checks in a PySpark ETL pipeline?

**Answer:**

**1. Schema Validation:**
```python
def validate_schema(df, expected_schema):
    """Ensure DataFrame has expected schema"""
    assert df.schema == expected_schema, "Schema mismatch"
    return df
```

**2. Null Checks:**
```python
def check_nulls(df, non_nullable_cols):
    """Check for nulls in critical columns"""
    for col in non_nullable_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            raise ValueError(f"Found {null_count} nulls in {col}")
    return df
```

**3. Duplicate Detection:**
```python
def check_duplicates(df, key_cols):
    """Check for duplicate keys"""
    total = df.count()
    distinct = df.select(key_cols).distinct().count()
    duplicates = total - distinct
    
    if duplicates > 0:
        print(f"Warning: Found {duplicates} duplicate rows")
        # Log duplicates for investigation
        df.groupBy(key_cols).count() \
          .filter(F.col("count") > 1) \
          .show()
    return df
```

**4. Range Validation:**
```python
def validate_ranges(df):
    """Check values are within expected ranges"""
    invalid = df.filter(
        (F.col("age") < 0) | (F.col("age") > 120) |
        (F.col("salary") < 0)
    )
    
    if invalid.count() > 0:
        print(f"Warning: Found {invalid.count()} rows with invalid ranges")
        invalid.show(5)
    
    return df
```

**5. Referential Integrity:**
```python
def check_foreign_keys(df, lookup_df, key_col):
    """Ensure all keys exist in lookup table"""
    orphans = df.join(lookup_df, key_col, "left_anti")
    orphan_count = orphans.count()
    
    if orphan_count > 0:
        raise ValueError(f"Found {orphan_count} orphaned records")
    
    return df
```

**6. Completeness Checks:**
```python
def check_completeness(df, required_cols):
    """Check required columns have sufficient data"""
    thresholds = {"email": 0.95, "phone": 0.80}  # 95% emails, 80% phones
    
    for col in required_cols:
        non_null_pct = df.filter(F.col(col).isNotNull()).count() / df.count()
        threshold = thresholds.get(col, 1.0)
        
        if non_null_pct < threshold:
            print(f"Warning: {col} only {non_null_pct:.1%} complete (expected {threshold:.1%})")
    
    return df
```

**7. Data Quality Framework:**
```python
class DataQualityChecker:
    def __init__(self, df):
        self.df = df
        self.issues = []
    
    def check_schema(self, expected_schema):
        if self.df.schema != expected_schema:
            self.issues.append("Schema mismatch")
        return self
    
    def check_nulls(self, cols):
        for col in cols:
            null_count = self.df.filter(F.col(col).isNull()).count()
            if null_count > 0:
                self.issues.append(f"{col}: {null_count} nulls")
        return self
    
    def check_duplicates(self, key_cols):
        total = self.df.count()
        distinct = self.df.select(key_cols).distinct().count()
        if total != distinct:
            self.issues.append(f"Found {total - distinct} duplicates")
        return self
    
    def report(self):
        if self.issues:
            print("Data Quality Issues Found:")
            for issue in self.issues:
                print(f"  - {issue}")
            return False
        else:
            print("✓ All data quality checks passed")
            return True

# Usage
checker = DataQualityChecker(df)
checker.check_schema(expected_schema) \
       .check_nulls(["id", "email"]) \
       .check_duplicates(["customer_id"]) \
       .report()
```

**Best Practice for ICF:**
- Log all quality checks to monitoring system
- Quarantine bad records for investigation
- Alert on quality threshold violations
- Document quality rules in data dictionary

---

### Q11: How do you handle PII (Personally Identifiable Information) in data pipelines?

**Answer:**

**1. Masking:**
```python
# Mask email addresses
df = df.withColumn("email_masked", 
    F.regexp_replace(F.col("email"), r"(.{2}).*(@.*)", r"\1***\2")
)
# Result: jo***@example.com

# Mask phone numbers
df = df.withColumn("phone_masked",
    F.concat(F.lit("XXX-XXX-"), F.substring(F.col("phone"), -4, 4))
)
# Result: XXX-XXX-1234
```

**2. Hashing (One-way):**
```python
from pyspark.sql.functions import sha2, md5

# Hash SSN or other sensitive IDs
df = df.withColumn("ssn_hash", sha2(F.col("ssn"), 256))

# Add salt for better security
df = df.withColumn("ssn_hash", 
    sha2(F.concat(F.col("ssn"), F.lit("SECRET_SALT")), 256)
)
```

**3. Tokenization:**
```python
def tokenize_column(df, col_name):
    """Replace sensitive values with tokens"""
    # Create lookup table
    tokens = df.select(col_name).distinct() \
        .withColumn("token", F.monotonically_increasing_id())
    
    # Save lookup securely
    tokens.write.mode("overwrite").parquet("secure/tokens")
    
    # Replace with tokens
    return df.join(tokens, col_name).drop(col_name)
```

**4. Encryption (for storage):**
```python
# Note: Do this at rest, not in Spark processing
# Use cloud provider encryption (S3 SSE, Azure Storage Encryption)
df.write.option("encryption", "AES256").parquet("encrypted_data")
```

**5. Access Control:**
```python
# Separate PII into different tables with stricter access
pii_df = df.select("customer_id", "ssn", "email", "phone")
non_pii_df = df.drop("ssn", "email", "phone")

# Write to different locations with different permissions
pii_df.write.parquet("secure/pii")  # Restricted access
non_pii_df.write.parquet("data/public")  # Broader access
```

**6. Audit Logging:**
```python
def log_pii_access(user, table, action):
    """Log all PII data access"""
    log_entry = {
        "timestamp": datetime.now(),
        "user": user,
        "table": table,
        "action": action
    }
    # Write to audit log
    spark.createDataFrame([log_entry]).write.mode("append").parquet("audit/pii_access")
```

**ICF Best Practices:**
- Follow NIST guidelines for government data
- Implement role-based access control (RBAC)
- Encrypt data at rest and in transit
- Regular PII audits and compliance checks
- Document data lineage for PII fields

---

## Architecture & Design

### Q12: Design a scalable ETL pipeline architecture for a data lake.

**Answer:**

**Architecture Layers:**

```
┌─────────────────────────────────────────────────────┐
│                  Data Sources                        │
│  (APIs, Databases, Files, Streaming, IoT)           │
└─────────────────────────────────────────────────────┘
                        │
                        ↓
┌─────────────────────────────────────────────────────┐
│              Ingestion Layer                         │
│  - Apache Kafka (streaming)                          │
│  - AWS Kinesis / Azure Event Hubs                    │
│  - Batch: S3, ADLS, HDFS                            │
└─────────────────────────────────────────────────────┘
                        │
                        ↓
┌─────────────────────────────────────────────────────┐
│          Bronze Layer (Raw Data)                     │
│  - Parquet/Avro format                              │
│  - Partitioned by date                               │
│  - No transformations                                │
│  - Full audit trail                                  │
└─────────────────────────────────────────────────────┘
                        │
                        ↓
┌─────────────────────────────────────────────────────┐
│          Silver Layer (Cleaned Data)                 │
│  - Data quality checks                               │
│  - Schema validation                                 │
│  - Deduplication                                     │
│  - Type casting                                      │
│  - Delta Lake format                                 │
└─────────────────────────────────────────────────────┘
                        │
                        ↓
┌─────────────────────────────────────────────────────┐
│          Gold Layer (Business Logic)                 │
│  - Aggregations                                      │
│  - Business rules applied                            │
│  - Star schema / dimensional model                   │
│  - Ready for analytics                               │
└─────────────────────────────────────────────────────┘
                        │
                        ↓
┌─────────────────────────────────────────────────────┐
│              Consumption Layer                       │
│  - BI Tools (Tableau, Power BI)                     │
│  - ML Models                                         │
│  - APIs / Applications                               │
└─────────────────────────────────────────────────────┘
```

**Implementation:**

```python
class DataLakePipeline:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def ingest_to_bronze(self, source_path, table_name):
        """Raw ingestion without transformation"""
        df = self.spark.read.format("parquet").load(source_path)
        
        # Add metadata
        df = df.withColumn("ingestion_timestamp", F.current_timestamp()) \
               .withColumn("source_file", F.input_file_name())
        
        # Write to bronze with partitioning
        bronze_path = f"{self.config['bronze_path']}/{table_name}"
        df.write.mode("append") \
          .partitionBy("ingestion_date") \
          .parquet(bronze_path)
        
        return df
    
    def bronze_to_silver(self, table_name):
        """Clean and validate data"""
        bronze_path = f"{self.config['bronze_path']}/{table_name}"
        df = self.spark.read.parquet(bronze_path)
        
        # Data quality checks
        df = self.validate_schema(df)
        df = self.remove_duplicates(df)
        df = self.handle_nulls(df)
        df = self.standardize_formats(df)
        
        # Write to silver with Delta Lake
        silver_path = f"{self.config['silver_path']}/{table_name}"
        df.write.format("delta") \
          .mode("overwrite") \
          .option("mergeSchema", "true") \
          .save(silver_path)
        
        return df
    
    def silver_to_gold(self, table_name, business_logic):
        """Apply business transformations"""
        silver_path = f"{self.config['silver_path']}/{table_name}"
        df = self.spark.read.format("delta").load(silver_path)
        
        # Apply business logic
        df = business_logic(df)
        
        # Write to gold
        gold_path = f"{self.config['gold_path']}/{table_name}"
        df.write.format("delta") \
          .mode("overwrite") \
          .save(gold_path)
        
        return df
```

**Key Design Principles:**
1. **Separation of concerns** - Bronze (raw), Silver (clean), Gold (business)
2. **Idempotency** - Can rerun without side effects
3. **Incremental processing** - Process only new data
4. **Schema evolution** - Handle schema changes gracefully
5. **Data lineage** - Track data from source to consumption
6. **Monitoring** - Log metrics and quality checks

---

### Q13: How would you implement a slowly changing dimension (SCD) Type 2 in PySpark?

**Answer:**

**SCD Type 2** maintains historical versions of records with effective dates.

```python
from pyspark.sql import Window
from datetime import datetime

def implement_scd_type2(new_df, existing_df, key_cols, compare_cols):
    """
    Implement SCD Type 2 logic
    
    Args:
        new_df: New data from source
        existing_df: Current dimension table
        key_cols: Business key columns (e.g., customer_id)
        compare_cols: Columns to track for changes
    """
    
    # Add SCD columns to new data
    new_df = new_df.withColumn("effective_date", F.lit(datetime.now())) \
                   .withColumn("end_date", F.lit(None).cast("timestamp")) \
                   .withColumn("is_current", F.lit(True))
    
    # Find changed records
    comparison = " OR ".join([f"a.{col} != b.{col}" for col in compare_cols])
    
    changed_records = existing_df.alias("a") \
        .join(new_df.alias("b"), key_cols) \
        .where(comparison) \
        .select("a.*")
    
    # Expire old records
    expired = changed_records \
        .withColumn("end_date", F.lit(datetime.now())) \
        .withColumn("is_current", F.lit(False))
    
    # New records (inserts)
    new_keys = new_df.select(key_cols).distinct()
    existing_keys = existing_df.filter(F.col("is_current") == True).select(key_cols).distinct()
    
    insert_keys = new_keys.join(existing_keys, key_cols, "left_anti")
    inserts = new_df.join(insert_keys, key_cols)
    
    # Updates (new versions of changed records)
    update_keys = changed_records.select(key_cols).distinct()
    updates = new_df.join(update_keys, key_cols)
    
    # Unchanged records
    unchanged = existing_df.join(changed_records.select(key_cols), key_cols, "left_anti")
    
    # Combine all
    result = unchanged.union(expired).union(inserts).union(updates)
    
    return result

# Usage
dimension_table = implement_scd_type2(
    new_df=source_df,
    existing_df=customer_dim,
    key_cols=["customer_id"],
    compare_cols=["name", "email", "address"]
)

# Write back
dimension_table.write.format("delta").mode("overwrite").save("gold/customer_dim")
```

**Result:**
```
customer_id | name  | email         | effective_date | end_date   | is_current
------------|-------|---------------|----------------|------------|------------
1           | Alice | old@email.com | 2024-01-01     | 2024-06-01 | False
1           | Alice | new@email.com | 2024-06-01     | NULL       | True
2           | Bob   | bob@email.com | 2024-01-01     | NULL       | True
```

---

## Real-World Scenarios

### Q14: You have a 10TB dataset join with a 100MB lookup table. How do you optimize this?

**Answer:**

**Problem:** Large-large join causes shuffle, even though one side is small.

**Solution: Broadcast Join**

```python
from pyspark.sql.functions import broadcast

# Read large dataset
large_df = spark.read.parquet("s3://data/transactions")  # 10TB

# Read small lookup table
small_df = spark.read.parquet("s3://data/products")  # 100MB

# Broadcast small table to all executors
result = large_df.join(broadcast(small_df), "product_id")

# No shuffle on large dataset!
result.write.parquet("s3://output/enriched_transactions")
```

**Why it works:**
- Broadcast copies small table to all executors (memory)
- No shuffle needed on large dataset
- Each partition processes locally
- Massive performance improvement

**Broadcast threshold:**
```python
# Auto-broadcast tables < 10MB (default)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

# Increase for your 100MB table
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")
```

**When NOT to broadcast:**
- Table > 1GB (too much driver/executor memory)
- Executors have limited memory
- Both tables are large

**Alternative: Replicated Join (for very small tables)**
```python
# For tiny lookup tables (<1MB), collect to driver
lookup_dict = small_df.rdd.collectAsMap()

# Use in map function
large_df.rdd.map(lambda row: enrich(row, lookup_dict))
```

---

### Q15: Your Spark job is failing with "OutOfMemoryError". How do you troubleshoot and fix it?

**Answer:**

**Troubleshooting Steps:**

**1. Check Spark UI:**
- Task metrics → Shuffle read/write sizes
- Storage tab → Cached data size
- Executors tab → Memory usage per executor

**2. Identify the cause:**

**a) Too many cached DataFrames:**
```python
# Check what's cached
spark.catalog.listTables()

# Unpersist unused DataFrames
df.unpersist()
```

**b) Data skew:**
```python
# Check partition sizes
df.rdd.glom().map(len).collect()

# Fix with repartitioning
df = df.repartition(400)  # More partitions = less data per partition
```

**c) Large broadcast:**
```python
# Check broadcast size in Spark UI
# If > executor memory, don't broadcast
# Use regular join instead
result = large_df.join(medium_df, "key")  # No broadcast
```

**d) Collecting large dataset:**
```python
# Bad: Brings all data to driver
data = df.collect()  # OOM if data > driver memory

# Good: Sample or write to storage
sample = df.limit(1000).collect()
# Or
df.write.parquet("output")
```

**e) Complex UDFs:**
```python
# UDFs can cause serialization overhead
# Replace with built-in functions when possible
```

**Solutions:**

**1. Increase executor memory:**
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memoryOverhead", "1g") \
    .getOrCreate()
```

**2. Increase partitions:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

**3. Use disk spillover:**
```python
# Allow spilling to disk
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "2g")
```

**4. Process in batches:**
```python
# Process date ranges separately
for date in date_range:
    df_batch = spark.read.parquet(f"data/date={date}")
    process(df_batch)
```

**5. Optimize data types:**
```python
# Use smaller types
df = df.withColumn("id", F.col("id").cast(IntegerType()))  # Not LongType
```

**6. Avoid wide transformations:**
```python
# Bad: Many wide transforms
df.repartition(1000).groupBy(...).join(...).distinct()

# Good: Minimize transformations
df.groupBy(...).join(...).distinct()  # Let Spark optimize
```

---

## ICF-Specific Topics

### Q16: How would you process and analyze sensitive government data (e.g., census, healthcare) while ensuring compliance?

**Answer:**

**Compliance Requirements:**
- FISMA (Federal Information Security Management Act)
- NIST 800-53 controls
- HIPAA (for healthcare data)
- PII handling per OMB memorandums

**Implementation:**

**1. Data Classification:**
```python
def classify_data(df):
    """Tag columns by sensitivity level"""
    pii_cols = ["ssn", "dob", "email", "phone"]
    phi_cols = ["diagnosis", "medication", "doctor"]
    
    # Add metadata
    for col in pii_cols:
        df = df.withColumn(f"_{col}_class", F.lit("PII"))
    for col in phi_cols:
        df = df.withColumn(f"_{col}_class", F.lit("PHI"))
    
    return df
```

**2. Access Control:**
```python
class SecureDataAccess:
    def __init__(self, user_role):
        self.user_role = user_role
        self.permissions = {
            "analyst": ["age", "zip", "income_range"],  # Aggregated only
            "researcher": ["age", "zip", "income_range", "education"],
            "admin": "all"
        }
    
    def get_approved_columns(self):
        return self.permissions.get(self.user_role, [])
    
    def read_data(self, path):
        df = spark.read.parquet(path)
        
        if self.user_role != "admin":
            allowed_cols = self.get_approved_columns()
            df = df.select(*allowed_cols)
        
        # Log access
        self.log_access(path)
        
        return df
```

**3. Anonymization:**
```python
def anonymize_census_data(df):
    """K-anonymity for census data"""
    
    # Generalize age to ranges
    df = df.withColumn("age_range",
        F.when(F.col("age") < 25, "18-24")
         .when(F.col("age") < 35, "25-34")
         .when(F.col("age") < 50, "35-49")
         .otherwise("50+")
    ).drop("age")
    
    # Generalize ZIP to 3-digit
    df = df.withColumn("zip_3", F.substring(F.col("zip"), 1, 3)).drop("zip")
    
    # Remove quasi-identifiers if group size < k
    k = 5
    df = df.groupBy("age_range", "zip_3").agg(
        F.count("*").alias("count"),
        F.mean("income").alias("avg_income")
    ).filter(F.col("count") >= k)
    
    return df
```

**4. Audit Trail:**
```python
def audit_log(user, action, data_accessed, pii_accessed=False):
    """Comprehensive audit logging"""
    log_entry = {
        "timestamp": datetime.now(),
        "user": user,
        "action": action,
        "data_accessed": data_accessed,
        "pii_accessed": pii_accessed,
        "ip_address": get_client_ip(),
        "session_id": get_session_id()
    }
    
    # Write to immutable audit log
    spark.createDataFrame([log_entry]) \
         .write.mode("append") \
         .format("delta") \
         .save("s3://audit-logs/data-access")
```

**5. Encryption:**
```python
# At rest (S3)
df.write \
  .option("encryption", "SSE-S3") \
  .parquet("s3://secure-bucket/data")

# In transit (use SSL)
spark.conf.set("spark.ssl.enabled", "true")
```

**6. Data Masking for Non-Production:**
```python
def mask_for_dev(df):
    """Mask PII for dev/test environments"""
    if os.getenv("ENV") != "production":
        df = df.withColumn("ssn", F.lit("XXX-XX-XXXX"))
        df = df.withColumn("email", F.regexp_replace("email", "@.*", "@example.com"))
    return df
```

---

### Q17: Design a CDC (Change Data Capture) pipeline for a large government database.

**Answer:**

**Scenario:** Capture changes from Oracle/SQL Server database and stream to data lake.

**Architecture:**

```
Oracle DB → Debezium/GoldenGate → Kafka → PySpark Streaming → Delta Lake
```

**Implementation:**

```python
from pyspark.sql.streaming import StreamingQuery

def cdc_pipeline(spark):
    """Process CDC events from Kafka"""
    
    # Read from Kafka
    cdc_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "db.schema.table") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse CDC events
    parsed = cdc_stream.selectExpr("CAST(value AS STRING) as json") \
        .select(F.from_json("json", cdc_schema).alias("data")) \
        .select("data.*")
    
    # Extract operation type
    parsed = parsed.withColumn("op_type", F.col("__op")) \
                   .withColumn("ts", F.col("__ts_ms"))
    
    # Process by operation type
    def process_cdc(batch_df, batch_id):
        """Process each micro-batch"""
        
        # Inserts
        inserts = batch_df.filter(F.col("op_type") == "c")  # Create
        if inserts.count() > 0:
            inserts.write.format("delta").mode("append").save("delta/table")
        
        # Updates
        updates = batch_df.filter(F.col("op_type") == "u")  # Update
        if updates.count() > 0:
            # Merge logic
            delta_table = DeltaTable.forPath(spark, "delta/table")
            delta_table.alias("target") \
                .merge(updates.alias("source"), "target.id = source.id") \
                .whenMatchedUpdateAll() \
                .execute()
        
        # Deletes
        deletes = batch_df.filter(F.col("op_type") == "d")  # Delete
        if deletes.count() > 0:
            delta_table = DeltaTable.forPath(spark, "delta/table")
            delta_table.alias("target") \
                .merge(deletes.alias("source"), "target.id = source.id") \
                .whenMatchedDelete() \
                .execute()
    
    # Write stream with CDC logic
    query = parsed.writeStream \
        .foreachBatch(process_cdc) \
        .option("checkpointLocation", "s3://checkpoints/cdc") \
        .start()
    
    return query

# Run pipeline
query = cdc_pipeline(spark)
query.awaitTermination()
```

**Key Features:**
1. **Exactly-once processing** - Checkpointing ensures no duplicates
2. **Schema evolution** - Delta Lake handles schema changes
3. **Audit trail** - Delta Lake maintains version history
4. **Recovery** - Can restart from checkpoint after failures

---

### Q18: How would you implement data lineage tracking in a complex ETL pipeline?

**Answer:**

```python
class DataLineageTracker:
    """Track data lineage for compliance and debugging"""
    
    def __init__(self, spark, job_name):
        self.spark = spark
        self.job_name = job_name
        self.lineage_events = []
    
    def track_read(self, source_path, source_type, record_count):
        """Track data source"""
        event = {
            "job_name": self.job_name,
            "event_type": "READ",
            "source_path": source_path,
            "source_type": source_type,
            "record_count": record_count,
            "timestamp": datetime.now()
        }
        self.lineage_events.append(event)
    
    def track_transform(self, transform_name, input_count, output_count, logic):
        """Track transformation"""
        event = {
            "job_name": self.job_name,
            "event_type": "TRANSFORM",
            "transform_name": transform_name,
            "input_count": input_count,
            "output_count": output_count,
            "logic": logic,
            "timestamp": datetime.now()
        }
        self.lineage_events.append(event)
    
    def track_write(self, dest_path, dest_type, record_count):
        """Track data destination"""
        event = {
            "job_name": self.job_name,
            "event_type": "WRITE",
            "dest_path": dest_path,
            "dest_type": dest_type,
            "record_count": record_count,
            "timestamp": datetime.now()
        }
        self.lineage_events.append(event)
    
    def persist_lineage(self):
        """Save lineage metadata"""
        lineage_df = self.spark.createDataFrame(self.lineage_events)
        lineage_df.write.mode("append") \
            .format("delta") \
            .save("s3://metadata/lineage")

# Usage in ETL
lineage = DataLineageTracker(spark, "customer_etl")

# Read
source_df = spark.read.parquet("bronze/customers")
lineage.track_read("bronze/customers", "parquet", source_df.count())

# Transform
cleaned_df = source_df.filter(F.col("age").isNotNull())
lineage.track_transform("null_filter", source_df.count(), cleaned_df.count(), "filter age is not null")

# Write
cleaned_df.write.parquet("silver/customers")
lineage.track_write("silver/customers", "parquet", cleaned_df.count())

# Persist lineage
lineage.persist_lineage()
```

---

### Q19: What metrics would you monitor for a production Spark job?

**Answer:**

**1. Performance Metrics:**
```python
# Job duration
job_start = datetime.now()
# ... run job ...
job_duration = (datetime.now() - job_start).seconds

# Records processed
input_count = df.count()
output_count = result.count()
throughput = input_count / job_duration

# Shuffle metrics
shuffle_read = sc._jsc.sc().getExecutorMemoryStatus()
```

**2. Resource Metrics:**
```python
# Executor memory usage
executor_memory_used = spark.sparkContext._jvm.java.lang.Runtime.getRuntime().totalMemory()

# GC time
gc_time = sc._jsc.sc().getConf().get("spark.executor.memory")
```

**3. Data Quality Metrics:**
```python
def calculate_quality_metrics(df):
    total_records = df.count()
    
    metrics = {
        "total_records": total_records,
        "null_percentage": df.filter(F.col("key").isNull()).count() / total_records * 100,
        "duplicate_percentage": (total_records - df.distinct().count()) / total_records * 100,
        "completeness": df.filter(F.col("required_field").isNotNull()).count() / total_records * 100
    }
    
    return metrics
```

**4. Alerting Thresholds:**
```python
def check_alerts(metrics):
    alerts = []
    
    if metrics["job_duration"] > 3600:  # > 1 hour
        alerts.append("Job duration exceeded threshold")
    
    if metrics["failure_rate"] > 0.01:  # > 1%
        alerts.append("High task failure rate")
    
    if metrics["data_quality"]["null_percentage"] > 5:
        alerts.append("High null percentage detected")
    
    return alerts
```

**5. Dashboard Metrics:**
- Job success/failure rate
- Average job duration
- Data volume processed
- Cost per GB processed
- SLA compliance

---

### Q20: How do you handle late-arriving data in a streaming pipeline?

**Answer:**

```python
from pyspark.sql.functions import window

def handle_late_data(spark):
    """Process streaming data with watermarking"""
    
    # Read streaming data
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "events") \
        .load()
    
    # Parse with event timestamp
    parsed = stream_df.select(
        F.from_json(F.col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")
    
    # Set watermark (allow 1 hour late data)
    windowed = parsed \
        .withWatermark("event_time", "1 hour") \
        .groupBy(
            window(F.col("event_time"), "10 minutes"),
            F.col("user_id")
        ) \
        .agg(F.count("*").alias("event_count"))
    
    # Write with append mode
    query = windowed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3://checkpoints/windowed") \
        .start("s3://output/windowed_events")
    
    return query
```

**Watermark Behavior:**
- Data arriving within 1 hour of watermark: Processed
- Data arriving > 1 hour late: Dropped
- Trade-off between latency and completeness

---

## Behavioral Questions

### Q21: Tell me about a time when you optimized a slow Spark job.

**STAR Format Answer:**

**Situation:** Production ETL job processing 500GB daily data was taking 6 hours, missing SLA of 4 hours.

**Task:** Optimize job to meet SLA while maintaining data quality.

**Action:**
1. Analyzed Spark UI - found massive data skew (one partition had 80% of data)
2. Enabled AQE and skew join optimization
3. Added salting to distribute skewed keys
4. Changed from CSV to Parquet format (10x faster reads)
5. Implemented broadcast join for lookup tables
6. Increased executor memory and tuned partition count

**Result:**
- Job duration: 6 hours → 2.5 hours (58% improvement)
- Cost reduction: $200/day → $80/day (60% savings)
- Met SLA consistently with buffer for data growth
- Documented optimization patterns for team

---

## Quick Reference

### Common Spark Configs for Production

```python
spark = SparkSession.builder \
    .appName("ProductionETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memoryOverhead", "1g") \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

### Must-Know PySpark Functions

- **Aggregations:** `F.sum()`, `F.mean()`, `F.count()`, `F.countDistinct()`, `F.max()`, `F.min()`
- **String:** `F.concat()`, `F.split()`, `F.regexp_replace()`, `F.upper()`, `F.lower()`, `F.trim()`
- **Date:** `F.current_date()`, `F.date_add()`, `F.datediff()`, `F.year()`, `F.month()`
- **Conditional:** `F.when()`, `F.otherwise()`, `F.coalesce()`
- **Window:** `F.row_number()`, `F.rank()`, `F.lag()`, `F.lead()`
- **Array:** `F.explode()`, `F.array_contains()`, `F.size()`

---

**Good luck with your ICF interview! 🚀**

*Remember: Be ready to discuss real-world scenarios, demonstrate problem-solving, and show understanding of government data compliance requirements.*
