# PySpark Undefined Behavior & Pitfalls

Comprehensive collection of dangerous patterns, edge cases, and undefined behaviors in PySpark that can cause production failures.

## 🎯 Purpose

This collection demonstrates **real-world PySpark pitfalls** that silently break production systems:
- Closure serialization failures
- Lazy evaluation gotchas
- Data skew causing OOM errors
- Type coercion data loss
- NULL handling bugs

Each example shows both the **dangerous pattern** (❌) and the **safe alternative** (✅).

## 📁 Files Overview

| <sub>File</sub> | <sub>Lines</sub> | <sub>Topics Covered</sub> |
|------|-------|----------------|
| <sub>`01_closure_serialization.py`</sub> | <sub>527</sub> | <sub>Non-serializable objects, mutable state, instance methods, late binding</sub> |
| <sub>`02_lazy_evaluation.py`</sub> | <sub>665</sub> | <sub>Multiple recomputations, side effects, accumulator double-counting, random values</sub> |
| <sub>`03_data_skew_partitions.py`</sub> | <sub>160</sub> | <sub>Data skew, hot keys, partition imbalance, salting techniques</sub> |
| <sub>`04_type_coercion_null.py`</sub> | <sub>210</sub> | <sub>Implicit coercion, NULL propagation, division by zero, NaN vs NULL</sub> |

**Total: 1,562 lines of production-ready anti-patterns and solutions**

## 🔥 Top 10 Deadly PySpark Mistakes

### 1. **Non-Serializable Objects in Closures**
```python
# ❌ DANGER: File handles cannot be serialized
log_file = open('log.txt', 'w')

@udf(StringType())
def dangerous_udf(value):
    log_file.write(value)  # CRASH!
    return value

# ✅ SAFE: Create resources on executors
@udf(StringType())
def safe_udf(value):
    with open('log.txt', 'a') as f:  # Create inside UDF
        f.write(value)
    return value
```

### 2. **Mutable State Modifications**
```python
# ❌ DANGER: Counter modifications lost on executors
counter = [0]

@udf(IntegerType())
def increment(value):
    counter[0] += 1  # Lost! Executors have copies
    return counter[0]

# ✅ SAFE: Use Spark accumulators
counter = spark.sparkContext.accumulator(0)
```

### 3. **Multiple Recomputations Without Caching**
```python
# ❌ DANGER: Each action recomputes entire DAG
expensive_df = df.withColumn("expensive", expensive_computation())
expensive_df.count()  # Computation 1
expensive_df.sum()    # Computation 2 (full recompute!)

# ✅ SAFE: Cache before multiple actions
expensive_df.cache()
expensive_df.count()  # Computation 1
expensive_df.sum()    # Uses cache!
```

### 4. **Accumulator Double-Counting**
```python
# ❌ DANGER: Accumulator incremented multiple times
counter = spark.sparkContext.accumulator(0)

def increment(row):
    counter.add(1)
    return row

transformed = df.rdd.map(increment).toDF()
transformed.count()  # Counter = 100
transformed.count()  # Counter = 200 (WRONG!)

# ✅ SAFE: Cache to prevent recomputation
transformed.cache()
```

### 5. **Data Skew (Hot Keys)**
```python
# ❌ DANGER: 99% of data has same key
df.withColumn("key", lit("hot_key")).groupBy("key").count()
# One executor OOMs, others idle

# ✅ SAFE: Salting technique
df.withColumn("salt", (rand() * 10).cast("int")) \
  .withColumn("salted_key", concat(col("key"), col("salt"))) \
  .groupBy("salted_key").count()
```

### 6. **Type Coercion Data Loss**
```python
# ❌ DANGER: Invalid strings become NULL silently
df.withColumn("as_int", col("string_col").cast("int"))
# "123abc" → NULL (no error!)

# ✅ SAFE: Validate before casting
df.withColumn("valid", col("string_col").rlike("^[0-9]+$")) \
  .withColumn("as_int", when(col("valid"), col("string_col").cast("int")))
```

### 7. **UDFs Not Handling NULLs**
```python
# ❌ DANGER: Crashes on NULL
@udf(IntegerType())
def multiply(value):
    return value * 2  # TypeError when value is None!

# ✅ SAFE: Explicit NULL check
@udf(IntegerType())
def multiply(value):
    if value is None:
        return None
    return value * 2
```

### 8. **Random Values Changing on Recomputation**
```python
# ❌ DANGER: Different results each time
df = df.withColumn("random", rand())
df.show()  # Random values X
df.show()  # Random values Y (DIFFERENT!)

# ✅ SAFE: Seed + cache
df = df.withColumn("random", rand(seed=42))
df.cache()
```

### 9. **Instance Methods in UDFs**
```python
# ❌ DANGER: Captures entire object (with locks!)
class Processor:
    def __init__(self):
        self.lock = threading.Lock()  # Non-serializable!
    
    def process(self, value):
        with self.lock:  # CRASH!
            return value * 2

processor = Processor()
udf_func = udf(processor.process)  # Tries to serialize lock!

# ✅ SAFE: Static methods
class Processor:
    @staticmethod
    def process(value):
        return value * 2  # No self capture
```

### 10. **Python Late Binding in Loops**
```python
# ❌ DANGER: All UDFs use final loop value
udfs = [udf(lambda x: x * i) for i in range(5)]
# All UDFs multiply by 4! (final i value)

# ✅ SAFE: Default argument for early binding
udfs = [udf(lambda x, i=i: x * i) for i in range(5)]
```

## 🏃 Running Examples

### Run Individual Files
```bash
# Closure serialization pitfalls
python3 01_closure_serialization.py

# Lazy evaluation issues
python3 02_lazy_evaluation.py

# Data skew problems
python3 03_data_skew_partitions.py

# Type coercion dangers
python3 04_type_coercion_null.py
```

### Run All Examples
```bash
# Execute all tests
./run_all.sh

# Or manually
for file in 0*.py; do python3 "$file"; done
```

## 📊 Pattern Categories

### Serialization Issues (File 01)
- Non-serializable objects (files, locks, sockets)
- Mutable state in closures
- Instance methods capturing self
- Global variable modifications
- Late binding in loops
- Broadcast variable misuse

### Lazy Evaluation (File 02)
- Multiple recomputations without cache
- Transformations without actions
- Side effects in transformations
- Accumulator double-counting
- Random/time-dependent operations
- Execution order assumptions
- Checkpoint vs persist confusion

### Data Distribution (File 03)
- Severe data skew (hot keys)
- Single partition bottlenecks
- Too many/too few partitions
- Unbalanced joins
- Salting techniques
- Partition sizing

### Type Safety (File 04)
- Implicit type coercion
- NULL propagation in UDFs
- NULL vs None confusion
- Division by zero
- NaN vs NULL differences
- String to number conversion

## 🎓 Learning Approach

Each file follows this structure:
1. **Dangerous Pattern** (❌) - Shows the bug
2. **Problem Explanation** - Why it fails
3. **Expected Result** - What goes wrong
4. **Safe Alternative** (✅) - Correct approach
5. **Key Takeaways** - Summary of lessons

## ⚠️ Warning Signs

Watch for these red flags in production code:

```python
# 🚨 RED FLAGS 🚨
open('file.txt')              # File handles in closures
threading.Lock()              # Locks in UDF classes
counter[0] += 1               # Mutable state modification
processor.method              # Instance methods as UDFs
df.count(); df.count()        # Multiple actions without cache
.coalesce(1)                  # Single partition bottleneck
.cast("int")                  # Type coercion without validation
value * 2                     # UDF without NULL check
rand()                        # Random without seed + cache
lambda x: x * i               # Late binding in loops
```

## 📈 Performance Impact

| <sub>Pattern</sub> | <sub>Performance Impact</sub> | <sub>Severity</sub> |
|---------|-------------------|----------|
| <sub>No caching + multiple actions</sub> | <sub>2-10x slower</sub> | <sub>🔴 Critical</sub> |
| <sub>Data skew</sub> | <sub>OOM crashes</sub> | <sub>🔴 Critical</sub> |
| <sub>Single partition</sub> | <sub>No parallelism</sub> | <sub>🔴 Critical</sub> |
| <sub>Too many partitions</sub> | <sub>50-200% overhead</sub> | <sub>🟠 High</sub> |
| <sub>Regular UDF vs Pandas UDF</sub> | <sub>10-100x slower</sub> | <sub>🟠 High</sub> |
| <sub>Accumulator double-count</sub> | <sub>Wrong results</sub> | <sub>🔴 Critical</sub> |
| <sub>Type coercion data loss</sub> | <sub>Silent corruption</sub> | <sub>🔴 Critical</sub> |

## 🛠️ Defensive Patterns

### Always Do This
```python
# 1. Cache expensive repeated computations
expensive_df.cache()

# 2. Use static methods, not instance methods
@staticmethod
def process(value):
    ...

# 3. Handle NULLs explicitly in UDFs
if value is None:
    return None

# 4. Use Spark accumulators, not global variables
counter = spark.sparkContext.accumulator(0)

# 5. Use salting for hot keys
.withColumn("salt", (rand() * 10).cast("int"))

# 6. Validate before type casting
.withColumn("valid", col("str").rlike("^[0-9]+$"))

# 7. Use seed for reproducible random data
rand(seed=42)

# 8. Check partition counts
df.rdd.getNumPartitions()

# 9. Monitor data skew
df.groupBy("key").count().describe()

# 10. Use Pandas UDFs for vectorized operations
@pandas_udf(IntegerType())
def vectorized_udf(s: pd.Series) -> pd.Series:
    return s * 2
```

## 🔗 Related Resources

- [Spark Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Spark Performance Tuning](https://spark.apache.org/docs/latest/tuning.html)

## 🤝 Contributing

Found another undefined behavior? Add it to the collection:
1. Create new file: `05_your_topic.py`
2. Follow the pattern: dangerous (❌) → safe (✅)
3. Include 5-10 examples per file
4. Add comprehensive docstrings

## 📝 License

Educational use. These are ANTI-PATTERNS - do NOT use dangerous patterns in production!