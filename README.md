# PySpark ETL Interview Preparation Environment

Professional development environment for PySpark ETL interviews and practice.

## 📋 Table of Contents
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Quick Start](#quick-start)
- [Extensions Installed](#extensions-installed)
- [Practice Materials](#practice-materials)
- [Running Examples](#running-examples)
- [Interview Tips](#interview-tips)

## 🗂️ Project Structure

```
pyspark-coding/
├── src/                      # Source code modules
│   ├── etl/                  # ETL pipeline implementations
│   ├── readers/              # Data reading utilities
│   ├── writers/              # Data writing utilities
│   ├── transformations/      # Transformation functions
│   └── utils/                # Utility functions (Spark session, etc.)
├── tests/                    # Unit and integration tests
│   ├── unit/                 # Unit tests
│   └── integration/          # Integration tests
├── notebooks/                # Jupyter notebooks
│   ├── examples/             # Example notebooks
│   └── practice/             # Practice notebooks for interview prep
├── data/                     # Data directory
│   ├── raw/                  # Raw input data
│   ├── processed/            # Processed output data
│   └── sample/               # Sample datasets
├── docs/                     # Documentation
├── config/                   # Configuration files
├── docker/                   # Docker configurations
├── logs/                     # Application logs
├── requirements.txt          # Python dependencies
├── .env.template             # Environment variable template
└── README.md                 # This file
```

## 🚀 Setup Instructions

### 1. Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Linux/Mac
# venv\Scripts\activate  # On Windows
```

### 2. Install Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install all dependencies
pip install -r requirements.txt
```

### 3. Set Up Environment Variables

```bash
# Copy template to .env and configure
cp .env.template .env

# Edit .env with your specific settings
nano .env  # or use your preferred editor
```

### 4. Verify Installation

```bash
# Test PySpark installation
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); print(f'Spark {spark.version} is ready!'); spark.stop()"
```

## ⚡ Quick Start

### Run Example ETL Pipeline

```bash
# From project root
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
python src/etl/basic_etl_pipeline.py
```

### Start Jupyter Notebook

```bash
# Start Jupyter
jupyter notebook

# Navigate to notebooks/practice/ and open any notebook
```

### Run Tests

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest --cov=src tests/

# Run specific test file
pytest tests/unit/test_transformations.py
```

## 🔧 Extensions Installed

The following VSCode extensions are installed for optimal PySpark development:

- **Python** (ms-python.python) - Core Python support
- **Pylance** - Advanced Python IntelliSense
- **Python Debugger** - Debugging support
- **Jupyter** - Notebook support
- **Code Runner** - Quick code execution
- **isort** - Import sorting
- **Python Indent** - Smart indentation
- **Data Wrangler** - Visual data exploration
- **Prettier SQL** - SQL formatting
- **Databricks** - Databricks integration (optional)

## 📚 Practice Materials

### Notebooks

1. **01_pyspark_basics.ipynb** - Fundamental PySpark operations
   - SparkSession creation
   - Reading data
   - DataFrame operations
   - Filtering and selecting
   - Aggregations

2. **02_etl_transformations.ipynb** - ETL transformation patterns
   - Data cleaning (nulls, duplicates)
   - Type conversions
   - String manipulations
   - Date/time operations
   - Joins and unions
   - Window functions

### Sample Data

- `data/sample/customers.csv` - Customer data with duplicates and nulls
- `data/sample/orders.csv` - Order transactions data

### Code Modules

All modules in `src/` are well-documented and can be used as reference:
- `utils/spark_session.py` - Spark session management
- `readers/data_reader.py` - Data reading utilities
- `writers/data_writer.py` - Data writing utilities
- `transformations/common_transforms.py` - Common transformations
- `etl/basic_etl_pipeline.py` - Complete ETL pipeline example

## 🎯 Running Examples

### Example 1: Basic DataFrame Operations

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Example").getOrCreate()

# Read CSV
df = spark.read.csv("data/sample/customers.csv", header=True, inferSchema=True)

# Basic operations
df.show()
df.printSchema()
df.describe().show()

# Filter and select
df.filter(F.col("age") > 30).select("first_name", "last_name", "age").show()

spark.stop()
```

### Example 2: ETL Pipeline

```python
from src.etl.basic_etl_pipeline import ETLPipeline

# Run complete ETL pipeline
pipeline = ETLPipeline()
pipeline.run(
    input_path="data/sample/customers.csv",
    output_path="data/processed/customers_cleaned.parquet"
)
```

### Example 3: Using Utility Classes

```python
from src.utils.spark_session import create_spark_session
from src.readers.data_reader import DataReader
from src.transformations.common_transforms import CommonTransforms

# Create Spark session
spark = create_spark_session("My ETL Job")

# Read data
reader = DataReader(spark)
df = reader.read_csv("data/sample/customers.csv")

# Apply transformations
transforms = CommonTransforms()
df = transforms.remove_duplicates(df, ["customer_id"])
df = transforms.add_timestamp_column(df)
df = transforms.fill_nulls(df, {"email": "unknown@example.com"})

df.show()
spark.stop()
```

## 💡 Interview Tips

### Before the Interview

1. **Test Your Setup**
   - Run all sample notebooks
   - Verify Spark is working
   - Test screen sharing with code execution

2. **Review Key Concepts**
   - DataFrame operations (select, filter, groupBy, join)
   - Transformations vs Actions
   - Data cleaning (nulls, duplicates)
   - Aggregations and window functions
   - Reading/writing different formats (CSV, Parquet, JSON)

3. **Practice Common Scenarios**
   - Reading data from CSV/JSON
   - Handling missing values
   - Joining datasets
   - Aggregating data
   - Writing results to Parquet

### During the Interview

1. **Communication**
   - Explain your thought process
   - Ask clarifying questions
   - Discuss trade-offs

2. **Code Organization**
   - Write clean, readable code
   - Use meaningful variable names
   - Add comments for complex logic
   - Structure code logically

3. **Best Practices**
   - Use schema inference appropriately
   - Handle nulls explicitly
   - Use appropriate data types
   - Consider partitioning for large datasets
   - Cache DataFrames when reused

4. **Common Operations to Know**

```python
# Reading data
df = spark.read.csv("path", header=True, inferSchema=True)
df = spark.read.json("path")
df = spark.read.parquet("path")

# Selecting and filtering
df.select("col1", "col2")
df.filter(F.col("age") > 25)
df.where((F.col("age") > 25) & (F.col("country") == "USA"))

# Transformations
df.withColumn("new_col", F.col("old_col") * 2)
df.withColumnRenamed("old_name", "new_name")
df.drop("col1", "col2")
df.dropDuplicates(["id"])
df.fillna({"col1": 0, "col2": "unknown"})

# Aggregations
df.groupBy("category").agg(
    F.sum("amount").alias("total"),
    F.avg("amount").alias("average"),
    F.count("*").alias("count")
)

# Joins
df1.join(df2, df1.id == df2.id, "inner")
df1.join(df2, "id", "left")

# Window functions
from pyspark.sql.window import Window
window = Window.partitionBy("category").orderBy("date")
df.withColumn("rank", F.row_number().over(window))

# Writing data
df.write.csv("path", header=True, mode="overwrite")
df.write.parquet("path", mode="overwrite", partitionBy=["year"])
```

### Common Interview Questions

1. **What's the difference between transformation and action?**
   - Transformations are lazy (map, filter, select)
   - Actions trigger execution (count, collect, show)

2. **How do you handle null values?**
   - Filter: `df.filter(F.col("col").isNotNull())`
   - Fill: `df.fillna({"col": value})`
   - Drop: `df.dropna(subset=["col"])`

3. **How do you optimize Spark jobs?**
   - Use appropriate partitioning
   - Cache/persist frequently used DataFrames
   - Avoid shuffles when possible
   - Use broadcast joins for small tables
   - Use columnar formats (Parquet)

4. **What file formats do you use and why?**
   - CSV: Human-readable, but slower
   - Parquet: Columnar, compressed, efficient
   - JSON: Flexible schema, nested data

## 🐳 Docker Setup (Optional)

If you prefer using Docker for Spark:

```bash
# Navigate to docker directory
cd docker

# Build and start containers
docker-compose up -d

# Access Jupyter at http://localhost:8888
```

## 📝 Additional Resources

- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- Sample datasets in `data/sample/`

## 🤝 Support

For issues or questions:
1. Check notebook examples in `notebooks/practice/`
2. Review code examples in `src/`
3. Test with sample data in `data/sample/`

## 📄 License

This project is for interview preparation and educational purposes.

---

**Good luck with your interview! 🚀**
