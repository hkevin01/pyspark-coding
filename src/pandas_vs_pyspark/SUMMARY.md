# Pandas vs PySpark Comparison Scripts - Summary

## ✅ Created Successfully

I've created a comprehensive comparison folder with **5 executable Python scripts** that demonstrate Pandas vs PySpark side-by-side.

## 📂 File Structure

```
src/pandas_vs_pyspark/
├── __init__.py                    # Package initialization
├── README.md                      # Comprehensive guide (6.7KB)
├── 01_basic_operations.py         # Basic operations (5.7KB)
├── 02_aggregations_groupby.py     # Aggregations (7.9KB)
├── 03_joins.py                    # Join operations (8.6KB)
├── 04_string_operations.py        # String manipulation (9.0KB)
└── 05_missing_data.py             # Missing data handling (9.4KB)
```

**Total: 7 files, ~47KB of comparison code**

---

## 📋 What Each Script Covers

### 1. **01_basic_operations.py** (9 comparisons)
- Creating DataFrames
- Viewing data (head vs show)
- Selecting columns
- Filtering rows
- Adding new columns
- Sorting data
- Column statistics
- Counting rows
- Getting unique values

**Example Output:**
```
PANDAS - df.head(3):
   id     name  age  salary     city
0   1    Alice   25   50000      NYC
1   2      Bob   30   60000       LA
2   3  Charlie   35   75000  Chicago

PYSPARK - df.show(3):
+---+-------+---+------+-------+
| id|   name|age|salary|   city|
+---+-------+---+------+-------+
|  1|  Alice| 25| 50000|    NYC|
|  2|    Bob| 30| 60000|     LA|
|  3|Charlie| 35| 75000|Chicago|
+---+-------+---+------+-------+
```

---

### 2. **02_aggregations_groupby.py** (8 comparisons)
- Basic aggregations (sum, mean, max, min)
- Single column groupby
- Multiple aggregations
- GroupBy with multiple columns
- Revenue calculations
- Count distinct values
- Complete sales summaries
- Top N analysis

**Key Pattern:**
```python
# Pandas
df.groupby('category')['sales'].sum()

# PySpark
df.groupBy('category').agg(F.sum('sales'))
```

---

### 3. **03_joins.py** (8 join types)
- Inner join
- Left join
- Right join
- Outer/Full join
- Joins with different column names
- Join with aggregation
- Count orders per customer
- Anti join (customers with no orders)

**Key Pattern:**
```python
# Pandas
pd.merge(customers, orders, on='customer_id', how='inner')

# PySpark
customers.join(orders, on='customer_id', how='inner')
```

---

### 4. **04_string_operations.py** (10 operations)
- Case conversion (upper, lower, title)
- Trimming whitespace
- String length
- Contains/like patterns
- Starts with / ends with
- String replacement (including regex)
- String splitting
- Concatenation
- Pattern extraction
- Substring operations

**Key Pattern:**
```python
# Pandas uses .str accessor
df['name'].str.upper()
df['email'].str.split('@')

# PySpark uses F functions
F.upper('name')
F.split('email', '@')
```

---

### 5. **05_missing_data.py** (11 techniques)
- Detect missing values
- Count nulls per column
- Drop rows with nulls
- Drop with threshold
- Fill nulls with values
- Fill specific columns differently
- Fill with mean/median
- Forward/backward fill (Pandas)
- Replace specific values
- Filter non-null rows
- Coalesce (first non-null)

**Key Pattern:**
```python
# Pandas
df.dropna()
df.fillna(0)
df.isnull().sum()

# PySpark
df.na.drop()
df.na.fill(0)
df.select([F.count(F.when(F.col(c).isNull(), c)) ...])
```

---

## 🚀 How to Use

### Step 1: Setup Environment (if not done)
```bash
cd /home/kevin/Projects/pyspark-coding
./setup.sh  # Or manually: python -m venv venv && source venv/bin/activate
pip install pyspark pandas numpy
```

### Step 2: Run Scripts
```bash
cd src/pandas_vs_pyspark

# Run individual scripts
python 01_basic_operations.py
python 02_aggregations_groupby.py
python 03_joins.py
python 04_string_operations.py
python 05_missing_data.py

# Or run all at once
for script in 0*.py; do 
    echo "=========================================="
    echo "Running $script"
    echo "=========================================="
    python $script
    echo ""
done
```

### Step 3: Study the Output
Each script shows:
1. **Original data** - The sample dataset
2. **Pandas approach** - How to do it in Pandas
3. **PySpark approach** - The PySpark equivalent
4. **Side-by-side comparison** - Both outputs
5. **Summary** - Key differences highlighted

---

## 💡 Key Takeaways

### Main Differences

| Aspect | Pandas | PySpark |
|--------|--------|---------|
| **Execution** | Eager (immediate) | Lazy (when action called) |
| **Mutability** | Mutable (in-place) | Immutable (new DF) |
| **API Style** | `df['col']`, `.str`, direct | `F.col()`, `F.functions`, explicit |
| **Method Names** | `.head()`, `.merge()`, `.dropna()` | `.show()`, `.join()`, `.na.drop()` |
| **Column Ops** | `df['new'] = ...` | `df.withColumn('new', ...)` |
| **Strings** | `.str.upper()` | `F.upper()` |
| **Nulls** | `.fillna()`, `.dropna()` | `.na.fill()`, `.na.drop()` |

### The 80/20 Rule
**You already know 80% of PySpark from Pandas!**

The 20% difference:
1. Import `functions as F`
2. Use `F.col()` and F functions
3. Remember DataFrames are immutable
4. Understand lazy evaluation
5. Use `.show()` instead of `.head()`

---

## 📖 Related Documentation

Complement these scripts with:
- **`docs/pandas_to_pyspark.md`** - 1000+ line comprehensive guide
- **`docs/pyspark_overview.md`** - Technology comparison
- **`notebooks/practice/`** - Interactive practice
- **`notebooks/examples/`** - Real-world examples

---

## 🎯 Learning Path

**Start here:** Run each script in order (01 → 05)

**Then:** Read the full documentation at `docs/pandas_to_pyspark.md`

**Practice:** Work through notebooks in `notebooks/practice/`

**Interview Prep:** Study examples in `notebooks/examples/`

---

## 🎓 Perfect for:

✅ **Interview Preparation** - ICF technical interview coming up!
✅ **Quick Reference** - Side-by-side syntax comparisons
✅ **Learning PySpark** - If you already know Pandas
✅ **Migration Projects** - Converting Pandas code to PySpark
✅ **Teaching Others** - Clear, runnable examples

---

## 📊 Statistics

- **5 comparison scripts**
- **46 individual comparisons**
- **~200+ lines of side-by-side code examples**
- **Self-contained** - No external data files needed
- **Executable** - Run and see the differences immediately
- **Documented** - Extensive comments and summaries

---

## 🔥 Quick Demo Commands

```bash
# Navigate to folder
cd /home/kevin/Projects/pyspark-coding/src/pandas_vs_pyspark

# Quick test (after environment setup)
python 01_basic_operations.py | grep -A5 "FILTER ROWS"

# Run all and save output
for f in 0*.py; do python $f > "${f%.py}_output.txt"; done

# Count total comparisons
grep -c "^print.*=" *.py
```

---

## ✨ Next Steps

1. **Activate virtual environment:**
   ```bash
   cd /home/kevin/Projects/pyspark-coding
   source venv/bin/activate  # Or run ./setup.sh if not created
   ```

2. **Install dependencies:**
   ```bash
   pip install pyspark pandas numpy
   ```

3. **Run your first comparison:**
   ```bash
   cd src/pandas_vs_pyspark
   python 01_basic_operations.py
   ```

4. **Study the output and patterns**

5. **Move on to the comprehensive guide:**
   ```bash
   cat docs/pandas_to_pyspark.md
   ```

---

**You're all set! Happy learning! 🚀**

These scripts give you hands-on, executable examples of every major difference between Pandas and PySpark. Perfect preparation for your ICF technical interview!
