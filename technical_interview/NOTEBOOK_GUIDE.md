# Jupyter Notebook Guide - PySpark Interview Questions

## 📓 Available Notebooks

All technical interview questions are now available as **interactive Jupyter notebooks** with extensive markdown explanations and detailed code comments.

### Files

1. **[01_data_manipulation_questions.ipynb](01_data_manipulation_questions.ipynb)** (16KB)
   - Find duplicates, Nth highest, running totals
   - Window functions masterclass
   - Top N per group patterns

2. **[02_performance_optimization_questions.ipynb](02_performance_optimization_questions.ipynb)** (17KB)
   - Broadcast joins with benchmarks
   - Caching strategies comparison
   - Query plan analysis

3. **[03_sql_style_questions.ipynb](03_sql_style_questions.ipynb)** (18KB)
   - Classic LeetCode-style problems
   - Self-joins and rankings
   - Consecutive pattern detection

## 🚀 Quick Start

### VS Code (Recommended)

```bash
# Open a notebook
code 01_data_manipulation_questions.ipynb

# Or open all notebooks
code *.ipynb
```

**Features in VS Code:**
- Inline output rendering
- Variable explorer
- Integrated debugger
- Git integration
- Easy cell execution (Shift+Enter)

### Jupyter Notebook/Lab

```bash
# Start Jupyter Notebook
jupyter notebook

# Or Jupyter Lab (modern interface)
jupyter lab
```

**Navigate to:** `technical_interview/` → Select notebook

### Command Line (Legacy)

```bash
# Original Python scripts still work
python 01_data_manipulation_questions.py
```

## 📚 Notebook Structure

Each notebook follows this pattern:

```
┌─────────────────────────────────────────┐
│ 1. TITLE & OVERVIEW                     │
│    - Topics covered                      │
│    - Interview frequency                 │
│    - Study tips                          │
├─────────────────────────────────────────┤
│ 2. SETUP CELL                           │
│    - Import statements                   │
│    - Spark session creation              │
│    - Configuration                       │
├─────────────────────────────────────────┤
│ 3. QUESTION 1                           │
│    ┌──────────────────────────────┐    │
│    │ Markdown: Problem Statement   │    │
│    │ - 📝 Description              │    │
│    │ - 🎯 Interview Focus          │    │
│    │ - 💡 Key Concepts             │    │
│    └──────────────────────────────┘    │
│    ┌──────────────────────────────┐    │
│    │ Code: Sample Data             │    │
│    │ # Create example dataset      │    │
│    │ # With inline comments        │    │
│    └──────────────────────────────┘    │
│    ┌──────────────────────────────┐    │
│    │ Code: Solution                │    │
│    │ # Step 1: Detailed comment    │    │
│    │ # Step 2: Detailed comment    │    │
│    │ result = ...                  │    │
│    └──────────────────────────────┘    │
│    ┌──────────────────────────────┐    │
│    │ Markdown: Key Takeaways       │    │
│    │ - ✅ Pattern learned          │    │
│    │ - 🎤 Interview answers        │    │
│    │ - 🔥 Common follow-ups        │    │
│    └──────────────────────────────┘    │
├─────────────────────────────────────────┤
│ 4. QUESTION 2...                        │
│ 5. QUESTION 3...                        │
└─────────────────────────────────────────┘
```

## 💡 How to Use for Interview Prep

### Week 1: Data Manipulation

```bash
code 01_data_manipulation_questions.ipynb
```

**Study Plan:**
1. Read the overview and study tips
2. Run each question cell-by-cell
3. **Modify the code** - change values, try variations
4. Take notes in new markdown cells
5. Practice explaining solutions out loud

**Key Focus:**
- Window functions (row_number, rank, dense_rank)
- LAG/LEAD for accessing previous/next rows
- Top N per group pattern (most common!)

### Week 2: Performance Optimization

```bash
code 02_performance_optimization_questions.ipynb
```

**Study Plan:**
1. Run benchmarks and compare performance
2. Use `.explain()` to see query plans
3. Understand **why** optimizations work
4. Practice explaining trade-offs

**Key Focus:**
- Broadcast join threshold (< 10MB)
- When to cache (2+ uses of DataFrame)
- Partition sizing (128-200MB ideal)

### Week 3: SQL-Style Problems

```bash
code 03_sql_style_questions.ipynb
```

**Study Plan:**
1. Compare SQL solution with PySpark
2. Master self-joins and rankings
3. Focus on **consecutive pattern** detection
4. Time yourself (15-20 min per question)

**Key Focus:**
- Self-joins for hierarchies
- dense_rank() for tied values
- LAG for sequence detection

### Week 4: Mock Interviews

**Practice Routine:**
1. Pick a random question
2. Close the solution
3. Solve from scratch (no peeking!)
4. Time yourself: 15-20 minutes
5. Compare your solution
6. Practice explaining out loud

## 🎯 Cell Execution Tips

### Keyboard Shortcuts (VS Code & Jupyter)

```
Shift + Enter   → Run cell, move to next
Ctrl + Enter    → Run cell, stay in place
Alt + Enter     → Run cell, insert new cell below

A               → Insert cell above (Jupyter)
B               → Insert cell below (Jupyter)
M               → Change to markdown (Jupyter)
Y               → Change to code (Jupyter)
```

### Running Multiple Cells

```python
# Run all cells above current
# Cell → Run All Above (VS Code menu)

# Run all cells
# Run → Run All Cells

# Restart kernel and run all
# Kernel → Restart & Run All
```

### Debugging Tips

```python
# Add to any cell to see DataFrame structure
df.printSchema()
df.show(5)

# Check query plan
df.explain(mode="simple")     # Basic
df.explain(mode="extended")   # Detailed
df.explain(mode="formatted")  # Pretty print

# See what's cached
spark.sparkContext._jsc.sc().getPersistentRDDs()
```

## 🔥 Top 5 Questions to Master

Based on interview frequency across FAANG, fintech, and unicorns:

### 1. Top N Per Group ⭐⭐⭐⭐⭐
**Location:** 01_data_manipulation_questions.ipynb, Question 6
**Why:** Asked in 90%+ of interviews
**Pattern:** 
```python
Window.partitionBy("group").orderBy(col("value").desc())
dense_rank().over(window)
```

### 2. Second/Nth Highest ⭐⭐⭐⭐⭐
**Location:** 01_data_manipulation_questions.ipynb, Question 2
**Why:** Classic warm-up question
**Pattern:**
```python
distinct().withColumn("rank", dense_rank().over(window))
.filter(col("rank") == N)
```

### 3. Broadcast Join ⭐⭐⭐⭐⭐
**Location:** 02_performance_optimization_questions.ipynb, Question 1
**Why:** FAANG companies care about performance
**Pattern:**
```python
large_df.join(broadcast(small_df), "key")
```

### 4. Running Total ⭐⭐⭐⭐
**Location:** 01_data_manipulation_questions.ipynb, Question 3
**Why:** Common in time-series analysis
**Pattern:**
```python
Window.partitionBy("group").orderBy("date")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
```

### 5. Consecutive Detection ⭐⭐⭐⭐
**Location:** 03_sql_style_questions.ipynb, Question 3
**Why:** Tests LAG/LEAD understanding
**Pattern:**
```python
lag("value", 1).over(Window.orderBy("id"))
filter(current == prev)
```

## 📖 Additional Resources

### Within This Project

- [README.md](README.md) - Full guide with all topics
- [INTERVIEW_PREP_SUMMARY.md](INTERVIEW_PREP_SUMMARY.md) - 30-day study plan
- Original Python files (*.py) - Quick reference

### External Resources

**PySpark Documentation:**
- [Window Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Built-in Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)

**Practice Problems:**
- LeetCode (SQL section - adapt to PySpark)
- HackerRank (SQL + PySpark)
- StrataScratch (Real interview questions)

## ✅ Checklist Before Interview

```markdown
- [ ] Can solve "Top N per group" in < 5 minutes
- [ ] Understand rank vs dense_rank vs row_number
- [ ] Know when to use broadcast join
- [ ] Can explain window frames (rowsBetween)
- [ ] Comfortable with LAG/LEAD functions
- [ ] Know caching trade-offs
- [ ] Can read query plans (.explain())
- [ ] Practice explaining solutions out loud
- [ ] Timed yourself on 5+ questions
- [ ] Understand data skew and solutions
```

## 🎤 Interview Communication Tips

### When Solving Problems:

1. **Clarify Requirements**
   - "Should we handle null values?"
   - "What if there are ties?"
   - "Any constraints on data size?"

2. **Explain Your Approach**
   - "I'll use a window function partitioned by..."
   - "We need dense_rank instead of rank because..."
   - "This will cause a shuffle, which is unavoidable for..."

3. **Discuss Trade-offs**
   - "Broadcast join is faster but limited to small tables"
   - "Caching helps if we reuse the DataFrame multiple times"
   - "More partitions = more parallelism but higher overhead"

4. **Optimize Incrementally**
   - "First, let me get a working solution"
   - "Now let's optimize the join with broadcast"
   - "We could reduce shuffles by partitioning differently"

## 🆘 Troubleshooting

### Notebook Won't Open

```bash
# Check Jupyter is installed
pip install jupyter

# Check notebook exists
ls -la *.ipynb

# Try opening with Jupyter directly
jupyter notebook 01_data_manipulation_questions.ipynb
```

### Kernel Errors

```bash
# Restart kernel
# Kernel → Restart

# Clear all outputs
# Kernel → Restart & Clear Output

# Check Python environment
python --version
pip list | grep pyspark
```

### Spark Session Issues

```python
# Stop existing session
spark.stop()

# Create new session with fresh config
spark = SparkSession.builder \
    .appName("Interview") \
    .master("local[*]") \
    .getOrCreate()
```

## 📝 Taking Notes

Add your own markdown cells:

```markdown
### My Notes - Question 2

Key learnings:
- dense_rank handles ties better than rank
- Need .distinct() before ranking
- Window spec must have orderBy

Common mistakes I make:
- Forgetting to partition when needed
- Using rank instead of dense_rank
- Not handling null values

Interview tips:
- Explain WHY using dense_rank
- Mention shuffle implications
- Discuss edge cases (no 2nd highest)
```

## 🚀 Ready to Start!

1. Open your first notebook:
   ```bash
   code 01_data_manipulation_questions.ipynb
   ```

2. Run the setup cell

3. Start with Question 1

4. Run each cell and understand the output

5. Modify and experiment!

Good luck with your interviews! 🎯
