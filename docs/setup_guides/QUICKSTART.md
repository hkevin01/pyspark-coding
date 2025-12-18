# Quick Start Guide

## Initial Setup (5 minutes)

### Option 1: Automated Setup (Recommended)

```bash
cd /home/kevin/Projects/pyspark-coding
./setup.sh
```

### Option 2: Manual Setup

```bash
# 1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy environment template
cp .env.template .env

# 4. Test installation
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.getOrCreate(); print(f'Spark {spark.version} ready!'); spark.stop()"
```

## Your First PySpark Code (2 minutes)

### Test with Sample Data

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create Spark session
spark = SparkSession.builder.appName("QuickTest").getOrCreate()

# Read sample data
df = spark.read.csv("data/sample/customers.csv", header=True, inferSchema=True)

# Display data
print(f"Total customers: {df.count()}")
df.show(5)

# Basic transformation
df.filter(F.col("age") > 30).select("first_name", "last_name", "age").show()

spark.stop()
```

Save this as `quick_test.py` and run:
```bash
python quick_test.py
```

## Practice Notebooks (Recommended for Interview Prep)

```bash
# Start Jupyter
jupyter notebook

# Navigate to:
# notebooks/practice/01_pyspark_basics.ipynb
# notebooks/practice/02_etl_transformations.ipynb
```

## Run Example ETL Pipeline

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
python src/etl/basic_etl_pipeline.py
```

## Key Resources

1. **Cheat Sheet**: `docs/pyspark_cheatsheet.md`
2. **Sample Data**: `data/sample/`
3. **Code Examples**: `src/`
4. **Practice Notebooks**: `notebooks/practice/`

## Common Commands

```bash
# Activate environment
source venv/bin/activate

# Start Jupyter
jupyter notebook

# Run Python script
python script.py

# Run tests (after writing them)
pytest tests/

# Deactivate environment
deactivate
```

## Interview Day Checklist

- [ ] Test Spark session creation
- [ ] Verify screen sharing works with code execution
- [ ] Have sample data ready (`data/sample/`)
- [ ] Review cheat sheet (`docs/pyspark_cheatsheet.md`)
- [ ] Practice common operations in notebooks
- [ ] Test reading CSV, JSON, Parquet files
- [ ] Practice joins and aggregations
- [ ] Review null handling techniques

## Troubleshooting

### Issue: "pyspark not found"
```bash
pip install pyspark
```

### Issue: "No module named 'src'"
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
```

### Issue: Java not found
```bash
# Install OpenJDK
sudo apt install openjdk-11-jdk  # Ubuntu/Debian
# or
brew install openjdk@11  # macOS
```

## Next Steps

1. Run through both practice notebooks
2. Modify examples with your own logic
3. Create sample ETL scenarios
4. Practice explaining your code out loud
5. Time yourself on common tasks

**Good luck! 🎯**
