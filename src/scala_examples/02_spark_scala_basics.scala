/*
 * SPARK WITH SCALA - Basic Operations
 * 
 * This file shows how to use Apache Spark with Scala.
 * Spark is natively written in Scala, so this is the most natural API.
 * 
 * COMPARISON WITH PYSPARK:
 * ========================
 * - Scala code compiles to JVM bytecode (no serialization overhead)
 * - Direct access to Spark internals
 * - Type-safe DataFrame operations with Datasets
 * - 5-10% faster for native operations
 * - 2-5x faster for UDFs
 * 
 * SPARK SCALA API SIMILAR TO:
 * ============================
 * - PySpark API (95% identical syntax)
 * - Pandas (DataFrame operations)
 * - SQL (query-like transformations)
 */

import org.apache.spark.sql.{SparkSession, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// =============================================================================
// 1. CREATE SPARK SESSION
// =============================================================================

object SparkScalaBasics {
  
  def main(args: Array[String]): Unit = {
    
    // Create SparkSession (entry point to Spark)
    val spark = SparkSession.builder()
      .appName("Spark Scala Basics")           // Application name
      .master("local[*]")                      // Run locally with all cores
      .config("spark.sql.shuffle.partitions", "8")  // Optimize for local
      .getOrCreate()
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // spark = SparkSession.builder \
    //     .appName("My App") \
    //     .master("local[*]") \
    //     .getOrCreate()
    // 
    // Scala:
    // val spark = SparkSession.builder()
    //     .appName("My App")
    //     .master("local[*]")
    //     .getOrCreate()
    // 
    // NEARLY IDENTICAL! Only syntax difference (. vs \)
    
    // Import spark implicits for implicit conversions
    import spark.implicits._
    
    // Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    println("✅ SparkSession created successfully")
  }
}

// =============================================================================
// 2. READING DATA (DataFrames)
// =============================================================================

object ReadData {
  
  def readExamples(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Read CSV file
    val csvDF = spark.read
      .option("header", "true")              // First row is header
      .option("inferSchema", "true")         // Infer data types
      .csv("data/sample/customers.csv")
    
    // Read JSON file
    val jsonDF = spark.read
      .json("data/input.json")
    
    // Read Parquet file (columnar format, best for Spark)
    val parquetDF = spark.read
      .parquet("data/input.parquet")
    
    // Read with explicit schema (best practice for production)
    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = true),
      StructField("salary", DoubleType, nullable = true)
    ))
    
    val dfWithSchema = spark.read
      .schema(schema)
      .option("header", "true")
      .csv("data/input.csv")
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // df = spark.read.option("header", "true").csv("data.csv")
    // 
    // Scala:
    // val df = spark.read.option("header", "true").csv("data.csv")
    // 
    // IDENTICAL API!
  }
}

// =============================================================================
// 3. BASIC DATAFRAME OPERATIONS
// =============================================================================

object DataFrameOperations {
  
  def basicOperations(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Create sample DataFrame
    val data = Seq(
      ("Alice", 30, 75000.0, "Engineering"),
      ("Bob", 25, 60000.0, "Sales"),
      ("Charlie", 35, 85000.0, "Engineering"),
      ("Diana", 28, 70000.0, "Marketing"),
      ("Eve", 32, 90000.0, "Engineering")
    )
    
    val df = data.toDF("name", "age", "salary", "department")
    
    // Show data (like df.show() in Pandas or PySpark)
    df.show()
    
    // Print schema
    df.printSchema()
    
    // Select columns
    val selected = df.select("name", "salary")
    val selected2 = df.select($"name", $"salary")  // Using $ for column reference
    val selected3 = df.select(col("name"), col("salary"))  // Using col()
    
    // Filter rows
    val highEarners = df.filter($"salary" > 70000)
    val engineeringHigh = df.filter($"department" === "Engineering" && $"salary" > 70000)
    
    // Add new column
    val withBonus = df.withColumn("bonus", $"salary" * 0.1)
    
    // Rename column
    val renamed = df.withColumnRenamed("name", "employee_name")
    
    // Drop column
    val dropped = df.drop("age")
    
    // Sort data
    val sorted = df.orderBy($"salary".desc)
    val sorted2 = df.sort(desc("salary"))
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // df.filter(col("salary") > 70000)
    // df.withColumn("bonus", col("salary") * 0.1)
    // 
    // Scala:
    // df.filter($"salary" > 70000)
    // df.withColumn("bonus", $"salary" * 0.1)
    // 
    // VERY SIMILAR! Scala uses $ or col(), Python uses col()
  }
}

// =============================================================================
// 4. AGGREGATIONS
// =============================================================================

object Aggregations {
  
  def aggregationExamples(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val df = Seq(
      ("Engineering", "Alice", 75000),
      ("Sales", "Bob", 60000),
      ("Engineering", "Charlie", 85000),
      ("Marketing", "Diana", 70000),
      ("Engineering", "Eve", 90000),
      ("Sales", "Frank", 65000)
    ).toDF("department", "name", "salary")
    
    // Simple aggregation
    val avgSalary = df.agg(avg("salary").as("average_salary"))
    val totalSalary = df.agg(sum("salary").as("total_salary"))
    
    // Group by aggregation
    val deptStats = df.groupBy("department").agg(
      count("*").as("employee_count"),
      avg("salary").as("avg_salary"),
      max("salary").as("max_salary"),
      min("salary").as("min_salary")
    )
    
    deptStats.show()
    
    // Multiple aggregations
    val stats = df.groupBy("department").agg(
      sum("salary").as("total_salary"),
      avg("salary").as("avg_salary"),
      count("name").as("count")
    ).orderBy(desc("total_salary"))
    
    stats.show()
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // df.groupBy("department").agg(
    //     sum("salary").alias("total"),
    //     avg("salary").alias("average")
    // )
    // 
    // Scala:
    // df.groupBy("department").agg(
    //     sum("salary").as("total"),
    //     avg("salary").as("average")
    // )
    // 
    // NEARLY IDENTICAL! .alias() vs .as()
  }
}

// =============================================================================
// 5. JOINS
// =============================================================================

object Joins {
  
  def joinExamples(spark: SparkSession): Unit = {
    import spark.implicits._
    
    // Employees DataFrame
    val employees = Seq(
      (1, "Alice", 101),
      (2, "Bob", 102),
      (3, "Charlie", 101),
      (4, "Diana", 103)
    ).toDF("emp_id", "name", "dept_id")
    
    // Departments DataFrame
    val departments = Seq(
      (101, "Engineering"),
      (102, "Sales"),
      (103, "Marketing")
    ).toDF("dept_id", "dept_name")
    
    // Inner join (default)
    val innerJoin = employees.join(departments, "dept_id")
    
    // Left outer join
    val leftJoin = employees.join(departments, Seq("dept_id"), "left")
    
    // Right outer join
    val rightJoin = employees.join(departments, Seq("dept_id"), "right")
    
    // Full outer join
    val fullJoin = employees.join(departments, Seq("dept_id"), "outer")
    
    // Join with different column names
    val joinDiffCols = employees.join(
      departments,
      employees("dept_id") === departments("dept_id"),
      "inner"
    ).drop(departments("dept_id"))  // Drop duplicate column
    
    innerJoin.show()
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // employees.join(departments, "dept_id", "inner")
    // 
    // Scala:
    // employees.join(departments, Seq("dept_id"), "inner")
    // 
    // VERY SIMILAR! Scala uses Seq() for multiple join keys
  }
}

// =============================================================================
// 6. WINDOW FUNCTIONS
// =============================================================================

object WindowFunctions {
  
  def windowExamples(spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window
    
    val sales = Seq(
      ("Alice", "Engineering", 75000),
      ("Bob", "Sales", 60000),
      ("Charlie", "Engineering", 85000),
      ("Diana", "Marketing", 70000),
      ("Eve", "Engineering", 90000),
      ("Frank", "Sales", 65000)
    ).toDF("name", "department", "salary")
    
    // Window specification
    val windowSpec = Window
      .partitionBy("department")
      .orderBy(desc("salary"))
    
    // Rank within department
    val ranked = sales.withColumn("rank", rank().over(windowSpec))
    
    // Dense rank (no gaps in ranking)
    val denseRanked = sales.withColumn("dense_rank", dense_rank().over(windowSpec))
    
    // Row number
    val numbered = sales.withColumn("row_num", row_number().over(windowSpec))
    
    // Running total within department
    val windowSpecUnbounded = Window
      .partitionBy("department")
      .orderBy("salary")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    val runningTotal = sales.withColumn(
      "running_total",
      sum("salary").over(windowSpecUnbounded)
    )
    
    ranked.show()
    runningTotal.show()
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // from pyspark.sql.window import Window
    // windowSpec = Window.partitionBy("department").orderBy(desc("salary"))
    // df.withColumn("rank", rank().over(windowSpec))
    // 
    // Scala:
    // import org.apache.spark.sql.expressions.Window
    // val windowSpec = Window.partitionBy("department").orderBy(desc("salary"))
    // df.withColumn("rank", rank().over(windowSpec))
    // 
    // IDENTICAL API!
  }
}

// =============================================================================
// 7. SQL QUERIES
// =============================================================================

object SQLQueries {
  
  def sqlExamples(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val employees = Seq(
      ("Alice", 30, 75000, "Engineering"),
      ("Bob", 25, 60000, "Sales"),
      ("Charlie", 35, 85000, "Engineering")
    ).toDF("name", "age", "salary", "department")
    
    // Register as temporary view
    employees.createOrReplaceTempView("employees")
    
    // Execute SQL query
    val result = spark.sql("""
      SELECT department,
             COUNT(*) as emp_count,
             AVG(salary) as avg_salary,
             MAX(salary) as max_salary
      FROM employees
      WHERE salary > 60000
      GROUP BY department
      ORDER BY avg_salary DESC
    """)
    
    result.show()
    
    // Mix SQL and DataFrame API
    val engineeringDF = spark.sql("SELECT * FROM employees WHERE department = 'Engineering'")
    val highEarners = engineeringDF.filter($"salary" > 70000)
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // df.createOrReplaceTempView("employees")
    // result = spark.sql("SELECT * FROM employees")
    // 
    // Scala:
    // df.createOrReplaceTempView("employees")
    // val result = spark.sql("SELECT * FROM employees")
    // 
    // IDENTICAL!
  }
}

// =============================================================================
// 8. WRITING DATA
// =============================================================================

object WriteData {
  
  def writeExamples(spark: SparkSession, df: DataFrame): Unit = {
    
    // Write to Parquet (recommended format)
    df.write
      .mode("overwrite")  // overwrite, append, ignore, errorIfExists
      .parquet("output/data.parquet")
    
    // Write to CSV
    df.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/data.csv")
    
    // Write to JSON
    df.write
      .mode("overwrite")
      .json("output/data.json")
    
    // Write partitioned data
    df.write
      .mode("overwrite")
      .partitionBy("department", "year")
      .parquet("output/partitioned/")
    
    // Write with compression
    df.write
      .mode("overwrite")
      .option("compression", "snappy")  // snappy, gzip, lz4
      .parquet("output/compressed.parquet")
    
    // Write to single file (coalesce)
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/single_file.csv")
    
    // COMPARISON WITH PYSPARK:
    // Python:
    // df.write.mode("overwrite").parquet("output/")
    // 
    // Scala:
    // df.write.mode("overwrite").parquet("output/")
    // 
    // IDENTICAL!
  }
}

// =============================================================================
// 9. PERFORMANCE OPTIMIZATIONS
// =============================================================================

object Optimizations {
  
  def optimizationExamples(spark: SparkSession): Unit = {
    import spark.implicits._
    
    val df = spark.read.parquet("data/large_dataset.parquet")
    
    // Cache/Persist (store in memory)
    df.cache()  // Equivalent to persist(MEMORY_ONLY)
    df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    
    // Repartition (increase partitions for parallelism)
    val repartitioned = df.repartition(100)
    val repartitionedByKey = df.repartition(100, $"department")
    
    // Coalesce (decrease partitions, more efficient than repartition)
    val coalesced = df.coalesce(10)
    
    // Broadcast join (for small tables)
    val small = spark.read.csv("data/small.csv")
    val large = spark.read.parquet("data/large.parquet")
    
    val joined = large.join(broadcast(small), "id")
    
    // Explain query plan
    df.filter($"amount" > 1000).groupBy("category").count().explain(true)
    
    // PERFORMANCE NOTES:
    // - Scala compiles to JVM bytecode (no Python serialization overhead)
    // - Same execution plan as PySpark for native operations
    // - UDFs are 2-5x faster in Scala (no Python interpreter)
    // - Type safety catches errors at compile time
  }
}

// =============================================================================
// SUMMARY: SPARK SCALA API
// =============================================================================

/*
 * KEY TAKEAWAYS:
 * ==============
 * 
 * 1. SPARK SCALA API ≈ PYSPARK API (95% identical)
 *    - Same DataFrame operations
 *    - Same function names
 *    - Same query execution engine
 * 
 * 2. SYNTAX DIFFERENCES:
 *    Scala                          | PySpark
 *    -------------------------------|-------------------------------
 *    val df = spark.read.csv()     | df = spark.read.csv()
 *    $"column" or col("column")    | col("column")
 *    .as("alias")                  | .alias("alias")
 *    Seq("col1", "col2")           | ["col1", "col2"]
 *    df.filter($"x" > 10)          | df.filter(col("x") > 10)
 * 
 * 3. PERFORMANCE:
 *    - Native operations: ~5% faster in Scala
 *    - UDFs: 2-5x faster in Scala
 *    - Pandas UDFs in Python close the gap
 * 
 * 4. TYPE SAFETY:
 *    - Scala catches type errors at compile time
 *    - Python catches errors at runtime
 *    - Scala Datasets provide full type safety
 * 
 * 5. WHEN TO USE SCALA:
 *    ✓ Performance-critical applications
 *    ✓ Production streaming jobs
 *    ✓ Low-latency requirements
 *    ✓ Custom Spark development
 *    ✓ Type safety needed
 * 
 * 6. WHEN TO USE PYSPARK:
 *    ✓ Data science and ML focus
 *    ✓ Rapid prototyping
 *    ✓ Python ecosystem (NumPy, Pandas, Scikit-learn)
 *    ✓ Easier learning curve
 *    ✓ Jupyter notebooks
 * 
 * NEXT STEPS:
 * ===========
 * - See 03_scala_udfs.scala for custom function examples
 * - See 05_scala_datasets.scala for type-safe operations
 * - See 07_performance_comparison.scala for benchmarks
 */
