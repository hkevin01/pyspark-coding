# PySpark Technical Interview Preparation - Complete Summary

## 📦 What's Included

### ✅ 30 Fully Working Interview Questions

1. **Data Manipulation (10 questions)** - `01_data_manipulation_questions.py`
   - Find duplicates, Nth highest, running totals, pivot tables, top N per group, self-joins, explode arrays, moving averages, gap analysis

2. **Performance & Optimization (10 questions)** - `02_performance_optimization_questions.py`
   - Broadcast joins, caching, partitioning, filter pushdown, shuffle avoidance, column pruning, UDF optimization, data skew, memory management

3. **SQL-Style Problems (10 questions)** - `03_sql_style_questions.py`
   - Classic SQL patterns: employee hierarchies, consecutive numbers, cancellation rates, active users, seat swapping

---

## 🎯 Quick Start

```bash
cd technical_interview

# Run all data manipulation questions
python 01_data_manipulation_questions.py

# Run performance questions
python 02_performance_optimization_questions.py

# Run SQL-style questions
python 03_sql_style_questions.py
```

---

## 🔥 Most Common Interview Questions (Must Practice!)

### Top 5 - Asked 90%+ of Interviews

1. **Top N per Group**
   ```python
   window = Window.partitionBy("dept").orderBy(col("salary").desc())
   df.withColumn("rank", row_number().over(window)).filter(col("rank") <= 3)
   ```
   **Files**: Q6 in file 01, Q3 in file 03

2. **Second/Nth Highest Value**
   ```python
   window = Window.orderBy(col("salary").desc())
   df.select("salary").distinct()
     .withColumn("rank", dense_rank().over(window))
     .filter(col("rank") == 2)
   ```
   **Files**: Q2 in file 01, Q2 in file 03

3. **Broadcast Join for Performance**
   ```python
   large_df.join(broadcast(small_df), "key")
   ```
   **Files**: Q1 in file 02

4. **Running Total/Cumulative Sum**
   ```python
   window = Window.partitionBy("product").orderBy("date")
             .rowsBetween(Window.unboundedPreceding, Window.currentRow)
   df.withColumn("running_total", sum("amount").over(window))
   ```
   **Files**: Q3 in file 01

5. **Self-Join (Employee-Manager)**
   ```python
   df.join(df.alias("mgr"), df.manager_id == col("mgr.id"))
   ```
   **Files**: Q7 in file 01, Q1 in file 03

---

## 💡 Interview Strategy

### Phase 1: Understand (2-3 minutes)
- [ ] Read problem carefully
- [ ] Ask clarifying questions
- [ ] Discuss data size and format
- [ ] Identify edge cases

### Phase 2: Plan (3-5 minutes)
- [ ] Explain your approach
- [ ] Identify required operations (joins, aggregations, windows)
- [ ] Discuss optimization opportunities
- [ ] Mention potential issues (shuffles, skew, memory)

### Phase 3: Code (10-15 minutes)
- [ ] Start with working solution
- [ ] Test with sample data
- [ ] Handle NULL values and edge cases
- [ ] Add comments for complex logic

### Phase 4: Optimize (5-10 minutes)
- [ ] Identify bottlenecks
- [ ] Add broadcast hints for small tables
- [ ] Filter early
- [ ] Consider partitioning strategy
- [ ] Discuss caching if multiple actions

### Phase 5: Test & Explain (3-5 minutes)
- [ ] Show sample output
- [ ] Explain time/space complexity
- [ ] Discuss scalability
- [ ] Use `.explain()` to show understanding

---

## 🎓 Key Concepts to Master

### Window Functions (Critical!)
```python
# Ranking
row_number()  # 1, 2, 3, 4...
rank()        # 1, 2, 2, 4...
dense_rank()  # 1, 2, 2, 3...

# Offset
lag(col, n)   # Previous row value
lead(col, n)  # Next row value

# Frames
rowsBetween(start, end)
rangeBetween(start, end)

# Common window specs
Window.partitionBy("dept").orderBy("salary")
Window.orderBy("date").rowsBetween(-2, 0)  # 3-day window
```

### Joins
```python
inner      # Only matching rows
left       # All left + matching right
right      # All right + matching left
full       # All rows from both
left_semi  # Left rows that have match (no right columns)
left_anti  # Left rows that DON'T have match
cross      # Cartesian product (avoid!)
```

### Aggregations
```python
# Basic
count(), sum(), avg(), min(), max(), stddev()

# Advanced
collect_list()  # Array of all values
collect_set()   # Array of distinct values
first(), last() # First/last value in group
```

### Performance Keys
```python
# Good
.filter().groupBy()           # Filter early
broadcast(small_df)            # Avoid shuffle
df.select(cols).filter()       # Column pruning
col("x") * 2                   # Built-in functions

# Bad
.groupBy().filter()            # Filter late
regular join on small table    # Unnecessary shuffle
df.filter().select()           # Read all columns first
udf(lambda x: x * 2)           # Slow UDF
```

---

## 📊 Performance Cheat Sheet

| Operation | Causes Shuffle? | When to Use |
|-----------|----------------|-------------|
| `filter()` | ❌ No | Always filter early |
| `select()` | ❌ No | Column pruning |
| `groupBy()` | ✅ Yes | Aggregations |
| `join()` | ✅ Yes (usually) | Combining datasets |
| `broadcast(df)` | ❌ No | Small table joins (<10MB) |
| `repartition()` | ✅ Yes | Increase/change partitions |
| `coalesce()` | ⚠️ Sometimes | Decrease partitions only |
| `distinct()` | ✅ Yes | Remove duplicates |
| `orderBy()` | ✅ Yes | Global sorting |

---

## 🏢 Company-Specific Tips

### FAANG (Meta, Amazon, Google, Netflix, Apple)
**Focus**: Performance, scale, optimization
- Expect: "How would this scale to 1PB data?"
- Must know: Broadcast joins, data skew solutions, partitioning
- Common: Live coding with optimization discussion

**Sample Questions**:
- How to handle skewed data in joins?
- Optimize slow-running Spark job
- Design ETL for billions of records

### Fintech (Goldman Sachs, JP Morgan, Citadel)
**Focus**: Accuracy, SQL patterns, window functions
- Expect: Complex SQL-style problems
- Must know: Window functions, joins, aggregations
- Common: Whiteboard coding + explain logic

**Sample Questions**:
- Calculate rolling 7-day average
- Find gaps in transaction sequences
- Detect fraud patterns

### Startups/Unicorns (Uber, Airbnb, Stripe)
**Focus**: Practical problems, real-world scenarios
- Expect: "Build X feature" questions
- Must know: End-to-end ETL, data modeling
- Common: Take-home assignments

**Sample Questions**:
- Design surge pricing calculation
- Build user activity metrics
- ETL for recommendation system

---

## ✅ Pre-Interview Checklist

### Day Before Interview
- [ ] Run all 30 questions successfully
- [ ] Review window function examples
- [ ] Practice explaining code out loud
- [ ] Review company's tech blog (data engineering posts)
- [ ] Prepare questions to ask interviewer

### 1 Hour Before Interview
- [ ] Test your setup (if remote)
- [ ] Have PySpark documentation open
- [ ] Review your resume (know every project)
- [ ] Relax and stay confident

### During Interview
- [ ] Listen carefully to requirements
- [ ] Ask clarifying questions
- [ ] Think out loud
- [ ] Start simple, then optimize
- [ ] Test your code mentally
- [ ] Be honest about what you don't know

---

## 🚀 30-Day Study Plan

### Week 1: Fundamentals
- Day 1-2: Window functions (row_number, rank, lag, lead)
- Day 3-4: Joins (all types, especially anti/semi)
- Day 5-6: GroupBy and aggregations
- Day 7: Review + practice problems

### Week 2: Performance
- Day 8-9: Broadcast joins and shuffle optimization
- Day 10-11: Caching and partitioning
- Day 12-13: Filter pushdown and column pruning
- Day 14: Review + optimization problems

### Week 3: Advanced Topics
- Day 15-16: Data skew handling
- Day 17-18: UDFs and optimization
- Day 19-20: Memory management
- Day 21: Review + mixed problems

### Week 4: Interview Simulation
- Day 22-25: Solve 2-3 questions daily under time pressure
- Day 26-27: Mock interviews (record yourself)
- Day 28-29: Review mistakes and weak areas
- Day 30: Light review + confidence boost

---

## 📚 Quick Reference Card (Print This!)

```python
# TOP 10 PATTERNS FOR INTERVIEWS

# 1. Top N per group
Window.partitionBy("dept").orderBy(desc("salary"))
df.withColumn("rank", row_number().over(w)).filter("rank <= 3")

# 2. Running total
Window.partitionBy("id").orderBy("date").rowsBetween(-inf, 0)
df.withColumn("total", sum("amount").over(w))

# 3. Lag/Lead comparison
df.withColumn("prev", lag("value", 1).over(w))
  .filter(col("value") > col("prev"))

# 4. Find duplicates
df.groupBy("email").agg(count("*").alias("cnt"))
  .filter("cnt > 1")

# 5. Broadcast join
large.join(broadcast(small), "key")

# 6. Self-join
df.alias("a").join(df.alias("b"), col("a.mgr_id") == col("b.id"))

# 7. Anti-join (NOT IN)
df1.join(df2, "key", "left_anti")

# 8. Pivot
df.groupBy("category").pivot("region").agg(sum("sales"))

# 9. Explode arrays
df.select("id", explode("items").alias("item"))

# 10. Window with frame
Window.orderBy("date").rowsBetween(-2, 0)  # 3-day window
```

---

## 🎯 Success Metrics

Track your progress:
- [ ] Can solve 80%+ of questions independently
- [ ] Explain optimization techniques confidently
- [ ] Complete typical question in 15-20 minutes
- [ ] Handle follow-up questions easily
- [ ] Comfortable with live coding
- [ ] Can read and explain execution plans

---

## 💪 You Got This!

Remember:
- **Practice > Theory**: Code every question yourself
- **Explain out loud**: Helps you think clearly
- **Time yourself**: Simulate real pressure
- **Review mistakes**: Learn from each error
- **Stay confident**: You know more than you think!

Good luck! 🚀

---

## 📞 Additional Resources

- [PySpark SQL Docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html)
- [Window Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
- [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- LeetCode Database section (adapt to PySpark)
- StrataScratch for real interview questions

---

**Last Updated**: January 2026
**Total Questions**: 30 working examples
**Estimated Study Time**: 30-40 hours for complete mastery
