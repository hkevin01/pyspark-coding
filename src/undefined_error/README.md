# PySpark Undefined Behavior Examples

⚠️ **WARNING: Educational Anti-Patterns Ahead** ⚠️

This directory contains **intentionally dangerous code** demonstrating PySpark undefined behaviors and pitfalls that cause production failures.

## 🎯 Purpose

Learn by seeing what **NOT to do** in PySpark:
- Closure serialization failures that crash executors
- Lazy evaluation gotchas that waste resources
- Data skew patterns that cause OOM errors
- Type coercion bugs that lose data silently
- NULL handling mistakes that corrupt pipelines

Each example shows the **dangerous pattern (❌)** alongside the **safe alternative (✅)**.

## 📁 Structure

```
undefined_error/
└── pyspark/                     # PySpark-specific pitfalls
    ├── 01_closure_serialization.py   # Serialization failures (527 lines)
    ├── 02_lazy_evaluation.py         # Lazy eval gotchas (665 lines)
    ├── 03_data_skew_partitions.py    # Data distribution (160 lines)
    ├── 04_type_coercion_null.py      # Type safety bugs (210 lines)
    ├── README.md                     # Comprehensive guide
    ├── run_all.sh                    # Execute all examples
    └── __init__.py                   # Package metadata
```

## 🔥 Quick Start

```bash
# Navigate to PySpark examples
cd pyspark/

# Run all examples (see 50+ dangerous patterns + safe alternatives)
./run_all.sh

# Or run individual files
python3 01_closure_serialization.py
python3 02_lazy_evaluation.py
python3 03_data_skew_partitions.py
python3 04_type_coercion_null.py
```

## 🚨 Top 5 Production-Breaking Bugs

### 1. **Non-Serializable Objects** 
```python
# ❌ Crashes executor
log_file = open('log.txt', 'w')
@udf(StringType())
def log_udf(value):
    log_file.write(value)  # File handle not serializable!
```

### 2. **Data Skew OOM**
```python
# ❌ One executor gets 99% of data
df.withColumn("key", lit("hot_key")).groupBy("key").count()
# Executor crashes with OutOfMemory
```

### 3. **Type Coercion Data Loss**
```python
# ❌ Silent NULL creation
df.withColumn("as_int", col("string_col").cast("int"))
# "123abc" → NULL (no error, data lost!)
```

### 4. **Accumulator Double-Counting**
```python
# ❌ Wrong results
counter = spark.sparkContext.accumulator(0)
transformed = df.rdd.map(lambda x: counter.add(1) or x).toDF()
transformed.count()  # Counter = 100
transformed.count()  # Counter = 200 (WRONG!)
```

### 5. **Multiple Recomputations**
```python
# ❌ 10x slower
expensive_df = df.withColumn("expensive", expensive_computation())
expensive_df.count()  # Full computation
expensive_df.sum()    # Full computation AGAIN!
```

## 📚 Learning Resources

- **Full Documentation**: See `pyspark/README.md` for comprehensive guide
- **50+ Examples**: Each file contains 10-15 dangerous patterns + solutions
- **Performance Data**: Impact analysis for each anti-pattern
- **Defensive Patterns**: Best practices to prevent bugs

## 🛡️ Defensive Checklist

Before deploying PySpark to production:

- [ ] No file handles, locks, or sockets in closures
- [ ] All expensive DataFrames are cached before multiple actions
- [ ] UDFs explicitly handle NULL/None values
- [ ] No instance methods used as UDFs (use static methods)
- [ ] Data skew monitored (check partition sizes)
- [ ] Type casting validated before conversion
- [ ] Accumulators only used with cached data
- [ ] Random operations use seed + cache
- [ ] Partition count appropriate (2-3x CPU cores)
- [ ] No global variable modifications in UDFs

## 🔗 Related Undefined Behavior Collections

This follows the pattern of:
- **C Undefined Behavior**: Sequence points, pointer arithmetic, strict aliasing
- **Java Pitfalls**: Integer overflow, equals/hashCode, concurrency
- **Python Gotchas**: Mutable defaults, late binding, GIL issues
- **PySpark Anti-Patterns** (this collection): Distributed computing edge cases

## 📊 Statistics

- **Files**: 4 Python modules
- **Examples**: 50+ dangerous patterns
- **Lines of Code**: 1,562 lines
- **Safe Alternatives**: Every dangerous pattern has a fix
- **Real-World Impact**: All examples based on production incidents

## ⚠️ Disclaimer

These are **intentionally dangerous patterns** for educational purposes only:
- Do NOT copy dangerous (❌) patterns to production
- DO use safe (✅) alternatives
- Run examples in isolated development environments
- Expect errors, crashes, and warnings (that's the point!)

## 🤝 Contributing

Found another PySpark pitfall? Contributions welcome:
1. Add example to appropriate file (or create new file)
2. Follow pattern: dangerous (❌) → explanation → safe (✅)
3. Include docstrings and inline comments
4. Test that dangerous pattern actually fails

## 📝 License

Educational use only. MIT License for safe alternatives.
