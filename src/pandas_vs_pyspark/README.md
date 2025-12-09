# Pandas vs PySpark Comparison Scripts

This folder contains executable Python scripts that demonstrate the differences between Pandas and PySpark with side-by-side code examples.

## 📁 Scripts Overview

### 1. `01_basic_operations.py`
**Basic DataFrame Operations**
- Creating DataFrames
- Viewing data (head, show)
- Selecting columns
- Filtering rows
- Adding columns
- Sorting
- Statistics
- Counting and unique values

**Run:** `python 01_basic_operations.py`

---

### 2. `02_aggregations_groupby.py`
**Aggregations and GroupBy**
- Basic aggregations (sum, mean, max, min)
- Single column groupby
- Multiple aggregations
- GroupBy with multiple columns
- Revenue calculations
- Count distinct
- Top N analysis

**Run:** `python 02_aggregations_groupby.py`

---

### 3. `03_joins.py`
**Join Operations**
- Inner join
- Left join
- Right join
- Outer/Full join
- Joins with different column names
- Join with aggregation
- Anti join (left_anti)
- Count after joins

**Run:** `python 03_joins.py`

---

### 4. `04_string_operations.py`
**String Manipulation**
- Case conversion (upper, lower, title)
- Trimming whitespace
- String length
- Contains/like patterns
- Starts with / ends with
- String replacement
- String splitting
- Concatenation
- Regex extraction
- Substring operations

**Run:** `python 04_string_operations.py`

---

### 5. `05_missing_data.py`
**Handling Missing Data**
- Detect missing values
- Drop rows with nulls
- Drop with thresholds
- Fill nulls with values
- Fill specific columns
- Fill with mean/median
- Forward/backward fill (Pandas)
- Replace values
- Filter non-null rows
- Coalesce (first non-null)

**Run:** `python 05_missing_data.py`

---

## 🚀 Quick Start

### Prerequisites
```bash
# Ensure you're in the virtual environment
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies (if not already done)
pip install pyspark pandas numpy
```

### Run Individual Scripts
```bash
cd src/pandas_vs_pyspark

# Run any script
python 01_basic_operations.py
python 02_aggregations_groupby.py
python 03_joins.py
python 04_string_operations.py
python 05_missing_data.py
```

### Run All Scripts
```bash
# Linux/Mac
for script in *.py; do echo "Running $script..."; python $script; echo ""; done

# Or individually
python 01_basic_operations.py && \
python 02_aggregations_groupby.py && \
python 03_joins.py && \
python 04_string_operations.py && \
python 05_missing_data.py
```

---

## 📊 What You'll Learn

### Key Differences Highlighted

| Concept | Pandas | PySpark |
|---------|--------|---------|
| **Execution** | Eager (immediate) | Lazy (optimized) |
| **Import** | `import pandas as pd` | `from pyspark.sql import functions as F` |
| **View Data** | `df.head()` | `df.show()` |
| **Filter** | `df[df['col'] > 5]` | `df.filter(F.col('col') > 5)` |
| **Select** | `df[['col1', 'col2']]` | `df.select('col1', 'col2')` |
| **Add Column** | `df['new'] = ...` | `df.withColumn('new', ...)` |
| **GroupBy** | `df.groupby('col').sum()` | `df.groupBy('col').agg(F.sum(...))` |
| **Join** | `pd.merge(df1, df2)` | `df1.join(df2)` |
| **Null Handling** | `df.fillna()` | `df.na.fill()` |
| **String Ops** | `df['col'].str.upper()` | `F.upper('col')` |

---

## 💡 Usage Tips

### 1. **Read the Output Carefully**
Each script prints side-by-side comparisons showing:
- Pandas approach first
- PySpark approach second
- Summary of key differences at the end

### 2. **Experiment**
Modify the sample data in each script to see how operations work with different data.

### 3. **Copy-Paste Patterns**
Use these scripts as templates for your own Pandas-to-PySpark migrations.

### 4. **Compare Syntax**
Notice the patterns:
- Pandas: Direct column access, in-place operations
- PySpark: `F` functions, immutable DataFrames

---

## 🎯 Common Patterns to Remember

### Pandas → PySpark Translation

```python
# FILTERING
# Pandas
df[df['age'] > 30]
# PySpark
df.filter(F.col('age') > 30)

# COLUMN OPERATIONS
# Pandas
df['total'] = df['price'] * df['quantity']
# PySpark
df = df.withColumn('total', F.col('price') * F.col('quantity'))

# AGGREGATION
# Pandas
df.groupby('category')['sales'].sum()
# PySpark
df.groupBy('category').agg(F.sum('sales'))

# STRING OPERATIONS
# Pandas
df['name'].str.upper()
# PySpark
F.upper('name')

# JOINS
# Pandas
pd.merge(df1, df2, on='key')
# PySpark
df1.join(df2, on='key')
```

---

## 🔍 Understanding Lazy Evaluation

**Important PySpark Concept:**

```python
# These DON'T execute yet (transformations)
df2 = df.filter(F.col('age') > 30)
df3 = df2.groupBy('city').count()

# This TRIGGERS execution (action)
df3.show()  # NOW everything runs with optimized plan
```

**Actions that trigger execution:**
- `.show()`
- `.count()`
- `.collect()`
- `.write.*()`
- `.first()`, `.take(n)`

---

## 📖 Further Reading

After running these scripts, refer to:
- **Documentation:** `docs/pandas_to_pyspark.md` - Comprehensive guide
- **Notebooks:** `notebooks/practice/` - Interactive exercises
- **Examples:** `notebooks/examples/` - Real-world scenarios

---

## 🐛 Troubleshooting

### Script won't run?
```bash
# Check if PySpark is installed
python -c "import pyspark; print(pyspark.__version__)"

# Check if pandas is installed
python -c "import pandas; print(pandas.__version__)"

# Reinstall if needed
pip install --upgrade pyspark pandas
```

### Too much Spark logging?
The scripts set `setLogLevel("ERROR")` to reduce noise, but you can change it:
```python
spark.sparkContext.setLogLevel("WARN")  # or "INFO", "DEBUG"
```

---

## 🎓 Learning Path

**Recommended order:**
1. Start with `01_basic_operations.py` - Foundation
2. Then `02_aggregations_groupby.py` - Most common operations
3. Next `03_joins.py` - Combining data
4. Follow with `04_string_operations.py` - Text processing
5. Finally `05_missing_data.py` - Data cleaning

**After completing these:**
- Read `docs/pandas_to_pyspark.md` for detailed explanations
- Work through practice notebooks in `notebooks/practice/`
- Try interview examples in `notebooks/examples/`

---

## 📝 Notes

- **All scripts are self-contained** - they create their own sample data
- **Scripts are safe to run multiple times** - they don't modify external files
- **Output is verbose** - showing both approaches clearly
- **PySpark sessions are stopped** - cleanup happens at the end of each script

---

## 🤝 Contributing

Found improvements or want to add more comparisons? These scripts are meant to be learning tools!

Potential additions:
- Date/time operations comparison
- Window functions comparison
- Performance benchmarking
- Reading/writing files comparison
- UDF vs built-in functions

---

**Happy Learning! 🚀**

Your Pandas knowledge is 80% of what you need for PySpark. These scripts show you the remaining 20%!
