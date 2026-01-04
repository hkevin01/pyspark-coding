# PySpark Technical Interview Questions

Comprehensive collection of real-world PySpark interview questions commonly asked at FAANG, unicorns, and data-focused companies.

## � Available Formats

- **📓 Jupyter Notebooks** (Recommended) - Interactive learning with markdown explanations
- **🐍 Python Scripts** - Quick reference and command-line execution

## 🚀 Quick Start

### Interactive Learning (Recommended)
```bash
# Open notebooks in VS Code
code 01_data_manipulation_questions.ipynb

# Or use Jupyter
jupyter notebook 01_data_manipulation_questions.ipynb
```

### Command Line Execution
```bash
python 01_data_manipulation_questions.py
```

## 📚 Question Categories

### 1. Data Manipulation Questions

**Files:** 
- `01_data_manipulation_questions.ipynb` (Interactive Notebook)
- `01_data_manipulation_questions.py` (Script)

**Topics Covered:**
- Finding duplicate records
- N-th highest value problems
- Running totals and cumulative sums
- Pivot tables and data reshaping
- Consecutive duplicate removal
- Top N per group (window functions)
- Self-joins and hierarchies
- Exploding nested structures
- Moving averages
- Gap analysis and missing data

**Key Concepts:**
- Window functions (row_number, rank, dense_rank, lag, lead)
- GroupBy aggregations
- Joins (inner, left, right, self)
- Array/nested data handling
- Date operations

---

### 2. Performance & Optimization Questions

**Files:**
- `02_performance_optimization_questions.ipynb` (Interactive Notebook)
- `02_performance_optimization_questions.py` (Script)

**Topics Covered:**
- Broadcast joins vs regular joins
- Caching and persistence strategies
- Partition optimization
- Filter pushdown
- Shuffle minimization
- Column pruning
- Predicate pushdown with Parquet
- Avoiding UDFs
- Data skew handling
- Memory management

**Key Concepts:**
- Broadcast joins for small tables (<10MB)
- Cache when DataFrame used multiple times
- Aim for 128-200MB per partition
- Filter early, aggregate late
- Use built-in functions instead of UDFs
- Salt keys to handle skew
- Memory vs execution trade-offs

---

### 3. SQL-Style Questions

**Files:**
- `03_sql_style_questions.ipynb` (Interactive Notebook)
- `03_sql_style_questions.py` (Script)

**Topics Covered:**
- Employee salary comparisons
- N-th highest salary (2nd, 3rd, etc.)
- Department top 3 salaries
- Consecutive numbers detection
- Customers who never ordered
- Cancellation rates
- Rising temperature problems
- Seat swapping logic
- First login analysis
- Active users detection

**Key Concepts:**
- Classic SQL interview patterns in PySpark
- Self-joins for hierarchies
- Anti-joins for "never" conditions
- Window functions for ranking
- Date arithmetic
- Conditional aggregations

**Run:**
```bash
python 03_sql_style_questions.py
```

---

## 🎯 How to Use

### Run Individual Question Files

```bash
cd technical_interview

# Data manipulation
python 01_data_manipulation_questions.py

# Performance optimization
python 02_performance_optimization_questions.py

# SQL-style problems
python 03_sql_style_questions.py
```

### Run All Questions

```bash
# Run all interview questions sequentially
for file in 0*.py; do python "$file"; done
```

### Study Individual Questions

Each file has 10 questions that can be run independently. Open the file and uncomment specific questions in the `main()` function.

---

## 💡 Interview Tips

### Before the Interview

1. **Practice explaining** - Don't just code, explain your thought process
2. **Know time complexity** - Understand shuffle operations and their cost
3. **Memorize key functions** - Window functions, aggregations, joins
4. **Understand execution plans** - Use `.explain()` to show optimization knowledge

### During the Interview

1. **Ask clarifying questions**:
   - Data size and format?
   - Performance requirements?
   - Acceptable latency?
   - Memory constraints?

2. **Think out loud**:
   - "This will cause a shuffle because..."
   - "I'll use broadcast join since the lookup table is small..."
   - "Let me optimize by filtering early..."

3. **Consider edge cases**:
   - Empty DataFrames
   - NULL values
   - Duplicate keys
   - Data skew

4. **Optimize progressively**:
   - Get working solution first
   - Then optimize for performance
   - Explain trade-offs

### Common Interview Question Patterns

#### Pattern 1: Top N Per Group
```python
window_spec = Window.partitionBy("category").orderBy(col("value").desc())
df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") <= N)
```

#### Pattern 2: Running Total
```python
window_spec = Window.partitionBy("group").orderBy("date")\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", sum("amount").over(window_spec))
```

#### Pattern 3: Lag/Lead Comparison
```python
window_spec = Window.partitionBy("id").orderBy("date")
df.withColumn("prev_value", lag("value").over(window_spec))\
  .filter(col("value") > col("prev_value"))
```

#### Pattern 4: Broadcast Join
```python
large_df.join(broadcast(small_df), "key")
```

#### Pattern 5: Data Skew - Salting
```python
df.withColumn("salt", (rand() * 10).cast("int"))\
  .withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))
```

---

## 📊 Performance Benchmarks

### Broadcast Join Speedup
- Small table (<10MB): **2-10x faster**
- No shuffle on large table side

### Caching Speedup
- Multiple actions on same DataFrame: **5-20x faster**
- Trade-off: Memory usage

### Filter Pushdown
- Filtering before aggregation: **10-100x faster**
- Reduces data processed

### UDF vs Built-in
- Built-in functions: **5-50x faster**
- UDFs break Catalyst optimization

---

## 🔍 Common Interview Topics

### Must Know (Asked 90%+ of time)
- ✅ Window functions (row_number, rank, lag, lead)
- ✅ Broadcast joins
- ✅ Partition optimization
- ✅ Caching strategies
- ✅ N-th highest value
- ✅ Top N per group
- ✅ Self-joins

### Frequently Asked (50-70%)
- ✅ Data skew handling
- ✅ UDF performance
- ✅ Filter pushdown
- ✅ Shuffle operations
- ✅ Running totals
- ✅ Gap analysis
- ✅ Consecutive detection

### Advanced Topics (20-40%)
- ✅ Memory management
- ✅ Predicate pushdown
- ✅ Execution plan reading
- ✅ Custom partitioners
- ✅ Catalyst optimizer
- ✅ Tungsten execution

---

## 🏆 Company-Specific Focus

### FAANG (Meta, Amazon, Apple, Netflix, Google)
- Heavy focus on **optimization** and **performance**
- Must explain `.explain()` output
- Expect data skew scenarios
- Large scale considerations (PB data)

### Fintech (JP Morgan, Goldman Sachs, Bloomberg)
- **SQL-style questions** very common
- Window functions heavily tested
- Data quality and accuracy critical
- Regulatory compliance questions

### Unicorns (Uber, Airbnb, DoorDash)
- Real-time streaming scenarios
- **Practical ETL problems**
- Architecture discussions
- Production debugging

### Data Companies (Databricks, Snowflake, Confluent)
- Deep dive into **Spark internals**
- Optimization techniques
- Benchmark comparisons
- Format-specific knowledge (Delta, Iceberg)

---

## 📖 Additional Resources

### Documentation
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Window Functions Guide](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

### Practice Platforms
- LeetCode (Database section - adapt to PySpark)
- HackerRank (SQL questions)
- StrataScratch (Real interview questions)

### Books
- "Spark: The Definitive Guide" by Chambers & Zaharia
- "High Performance Spark" by Holden Karau
- "Learning Spark" 2nd Edition

---

## ✅ Checklist for Interview Prep

- [ ] Run all 30 questions successfully
- [ ] Understand every window function example
- [ ] Can explain shuffle operations
- [ ] Know when to use broadcast join
- [ ] Understand caching strategies
- [ ] Can optimize queries independently
- [ ] Practiced explaining code out loud
- [ ] Reviewed Spark UI interpretation
- [ ] Know common data skew solutions
- [ ] Can write UDFs (but know when not to)

---

## 🚀 Quick Reference

### Most Used Functions
```python
# Window functions
row_number(), rank(), dense_rank(), lag(), lead(), ntile()

# Aggregations
count(), sum(), avg(), min(), max(), first(), last()

# Joins
inner, left, right, full, left_semi, left_anti, cross

# Conditionals
when(), otherwise(), coalesce(), isnull(), isnotnull()

# Date functions
to_date(), date_add(), date_sub(), datediff(), year(), month()

# String functions
concat(), concat_ws(), substring(), regexp_replace(), split()

# Array functions
explode(), array(), collect_list(), collect_set()
```

### Performance Commands
```python
df.explain()          # See execution plan
df.explain(True)      # Detailed plan with optimizations
df.cache()            # Cache in memory
df.repartition(N)     # Shuffle to N partitions
df.coalesce(N)        # Reduce partitions (no shuffle)
broadcast(df)         # Broadcast join
df.unpersist()        # Release cache
```

---

## 🎓 Contributing

Found a better solution? Want to add more questions? PRs welcome!

---

**Good luck with your PySpark interviews! 🚀**
