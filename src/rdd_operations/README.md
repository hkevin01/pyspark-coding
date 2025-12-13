# RDD Operations - Comprehensive Guide 🔷

This folder contains comprehensive examples of **RDD (Resilient Distributed Dataset) operations** in PySpark.

RDDs are the low-level API in Spark - the foundation beneath DataFrames. Understanding RDDs is essential for:
- Performance optimization
- Complex distributed algorithms
- Legacy Spark code
- Technical interviews

---

## 📚 What is an RDD?

**RDD (Resilient Distributed Dataset)** is:
- **Resilient**: Fault-tolerant through lineage
- **Distributed**: Partitioned across cluster nodes
- **Dataset**: Collection of elements

### RDD vs DataFrame

| Feature | RDD | DataFrame |
|---------|-----|-----------|
| API Level | Low-level | High-level |
| Type Safety | Compile-time (Scala/Java) | Runtime |
| Optimization | Manual | Automatic (Catalyst) |
| Performance | Slower (no optimizer) | Faster (optimized) |
| Use Case | Complex algorithms, control | SQL-like operations |

---

## 📦 Examples in This Folder

### **01_transformations_lowlevel_part1.py**
Core transformation operations:
- `map()`: 1-to-1 element transformation
- `flatMap()`: 1-to-N transformation with flattening
- `filter()`: Select elements by condition
- `mapPartitions()`: Transform entire partitions (efficient for expensive operations)
- `mapPartitionsWithIndex()`: Partition transformation with index

**When to run**: Learning RDD basics, understanding transformation vs action

### **02_transformations_lowlevel_part2.py**
Additional transformations:
- `distinct()`: Remove duplicates
- `union()`: Combine RDDs (keeps duplicates)
- `intersection()`: Common elements
- `subtract()`: Set difference
- `cartesian()`: Cartesian product

**When to run**: Set operations, data deduplication

### **03_transformations_joins.py**
Join operations for pair RDDs:
- `join()`: Inner join
- `leftOuterJoin()`: Left outer join
- `rightOuterJoin()`: Right outer join
- `fullOuterJoin()`: Full outer join
- `cogroup()`: Group both RDDs by key

**When to run**: Combining datasets, understanding join types

### **04_actions_aggregations.py**
Actions that trigger execution:
- `reduce()`, `fold()`: Combine elements
- `aggregate()`: Flexible aggregation
- `collect()`, `take()`, `top()`: Retrieve results
- `count()`, `sum()`, `mean()`, `stdev()`: Statistics

**When to run**: Understanding actions vs transformations, aggregation patterns

### **05_shuffle_and_key_operations.py** ⭐
Critical shuffle and key operations:
- **Shuffle explanation**: Why it's expensive
- **Combiner optimization**: `reduceByKey` vs `groupByKey`
- `reduceByKey()`: Aggregate by key (with combiner)
- `groupByKey()`: Group values by key (no combiner - slower!)
- `aggregateByKey()`: Flexible key aggregation
- `combineByKey()`: Most flexible combiner
- `countByKey()`: Count per key

**When to run**: Performance optimization, understanding shuffle

### **06_partitions_sorting_ranking.py** ⭐
Partitioning, sorting, and ranking:
- **Partition concepts**: What they are and why they matter
- `repartition()`: Change partition count (shuffle)
- `coalesce()`: Reduce partitions (no shuffle when reducing)
- `repartitionAndSortWithinPartitions()`: Efficient sorting
- `sortByKey()`, `sortBy()`: Sorting operations
- `top()`, `takeOrdered()`: Ranking operations
- Set operations and sampling

**When to run**: Performance tuning, understanding parallelism

---

## 🚀 Quick Start

### Run All Examples

```bash
cd src/rdd_operations

# Run each example
python 01_transformations_lowlevel_part1.py
python 02_transformations_lowlevel_part2.py
python 03_transformations_joins.py
python 04_actions_aggregations.py
python 05_shuffle_and_key_operations.py
python 06_partitions_sorting_ranking.py
```

### Interactive Learning

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("RDD_Learning") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Create RDD
rdd = sc.parallelize([1, 2, 3, 4, 5])

# Transformation (lazy)
squared = rdd.map(lambda x: x ** 2)

# Action (triggers execution)
result = squared.collect()
print(result)  # [1, 4, 9, 16, 25]
```

---

## 🎯 Key Concepts

### **Transformations (Lazy)**
Build a DAG without executing:
- `map`, `flatMap`, `filter`
- `reduceByKey`, `groupByKey`
- `join`, `union`, `distinct`
- `sortByKey`, `repartition`

### **Actions (Eager)**
Trigger execution and return results:
- `collect`, `count`, `take`
- `reduce`, `fold`, `aggregate`
- `saveAsTextFile`, `foreach`

### **Narrow vs Wide Transformations**

**Narrow** (no shuffle, fast):
- map, filter, flatMap
- union, mapPartitions
- Each output partition depends on single input partition

**Wide** (shuffle, expensive):
- groupByKey, reduceByKey
- join, cogroup, distinct
- sortByKey, repartition
- Output partition depends on multiple input partitions

---

## ⚡ Performance Tips

### 1. **Avoid collect() on Large RDDs**
```python
# ❌ BAD: Brings all data to driver
all_data = huge_rdd.collect()  # OOM!

# ✅ GOOD: Take sample or aggregate
sample = huge_rdd.take(100)
count = huge_rdd.count()
```

### 2. **Use reduceByKey over groupByKey**
```python
word_pairs = rdd.map(lambda x: (x, 1))

# ❌ BAD: No combiner, shuffles all values
word_counts = word_pairs.groupByKey().mapValues(sum)

# ✅ GOOD: Combiner reduces shuffle data
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
```

### 3. **Use mapPartitions for Expensive Operations**
```python
# ❌ BAD: Creates DB connection per element
rdd.map(lambda x: expensive_db_call(x))

# ✅ GOOD: Creates DB connection per partition
def process_partition(iterator):
    connection = create_db_connection()
    results = [connection.query(x) for x in iterator]
    connection.close()
    return results

rdd.mapPartitions(process_partition)
```

### 4. **Partition Wisely**
```python
# Rule of thumb: 2-4x number of cores
num_cores = 8
optimal_partitions = num_cores * 3  # 24 partitions

# Repartition large RDD
rdd = rdd.repartition(optimal_partitions)

# Reduce partitions after filter
filtered = rdd.filter(lambda x: x > 100)
filtered = filtered.coalesce(8)  # No shuffle when reducing
```

### 5. **Cache Reused RDDs**
```python
# RDD used multiple times
frequent_rdd = expensive_transformation(rdd)
frequent_rdd.cache()  # or .persist(StorageLevel.MEMORY_AND_DISK)

# Now these are fast
result1 = frequent_rdd.count()
result2 = frequent_rdd.take(10)
result3 = frequent_rdd.filter(lambda x: x > 50).count()
```

---

## 📊 Operation Cheat Sheet

### Transformations (Lazy)

| Operation | Input → Output | Description | Shuffle? |
|-----------|----------------|-------------|----------|
| `map(f)` | RDD[T] → RDD[U] | Apply f to each element | No |
| `flatMap(f)` | RDD[T] → RDD[U] | Apply f and flatten | No |
| `filter(f)` | RDD[T] → RDD[T] | Keep elements where f(x) = true | No |
| `mapPartitions(f)` | RDD[T] → RDD[U] | Apply f to entire partition | No |
| `distinct()` | RDD[T] → RDD[T] | Remove duplicates | Yes |
| `union(rdd2)` | RDD[T] → RDD[T] | Combine RDDs | No |
| `intersection(rdd2)` | RDD[T] → RDD[T] | Common elements | Yes |
| `subtract(rdd2)` | RDD[T] → RDD[T] | Elements in RDD1 not in RDD2 | Yes |
| `reduceByKey(f)` | RDD[(K,V)] → RDD[(K,V)] | Merge values per key | Yes |
| `groupByKey()` | RDD[(K,V)] → RDD[(K,Iterable[V])] | Group values per key | Yes |
| `sortByKey()` | RDD[(K,V)] → RDD[(K,V)] | Sort by key | Yes |
| `join(rdd2)` | RDD[(K,V)] → RDD[(K,(V,W))] | Inner join | Yes |
| `cogroup(rdd2)` | RDD[(K,V)] → RDD[(K,(Iter[V],Iter[W]))] | Group both RDDs | Yes |
| `repartition(n)` | RDD[T] → RDD[T] | Change partitions | Yes |
| `coalesce(n)` | RDD[T] → RDD[T] | Reduce partitions | Maybe |

### Actions (Eager)

| Operation | Output | Description |
|-----------|--------|-------------|
| `collect()` | Array[T] | Return all elements to driver |
| `count()` | Long | Count elements |
| `take(n)` | Array[T] | Return first n elements |
| `top(n)` | Array[T] | Return largest n elements |
| `takeOrdered(n)` | Array[T] | Return smallest n elements |
| `reduce(f)` | T | Combine elements with f |
| `fold(zero, f)` | T | Like reduce with initial value |
| `aggregate(zero, seq, comb)` | U | Flexible aggregation |
| `foreach(f)` | Unit | Apply f to each element |
| `saveAsTextFile(path)` | Unit | Write to text file |

---

## �� Interview Topics Covered

### Basic Concepts ✅
- What is an RDD?
- Transformations vs Actions
- Lazy evaluation
- DAG (Directed Acyclic Graph)

### Intermediate ✅
- Narrow vs Wide transformations
- Shuffling and why it's expensive
- Partitioning strategies
- Caching and persistence

### Advanced ✅
- Combiner optimization (reduceByKey vs groupByKey)
- repartition vs coalesce
- mapPartitions for efficiency
- aggregateByKey vs combineByKey

### Performance ✅
- Minimizing shuffles
- Optimal partition sizing
- When to cache
- Avoiding collect() OOMs

---

## 🔄 Transformation Workflow

```
1. Create RDD
   ↓
2. Apply Transformations (lazy)
   - map, filter, flatMap
   - reduceByKey, join
   - Build DAG
   ↓
3. Trigger Action (eager)
   - collect, count, save
   - Execute entire DAG
   ↓
4. Return Results
```

---

## 💡 Common Patterns

### Word Count (Classic)
```python
lines = sc.textFile("file.txt")
words = lines.flatMap(lambda line: line.split())
word_pairs = words.map(lambda word: (word, 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
result = word_counts.collect()
```

### Average by Key
```python
# Using aggregateByKey
data = sc.parallelize([("A", 10), ("B", 20), ("A", 15)])

zero_value = (0, 0)  # (sum, count)
seq_func = lambda acc, val: (acc[0] + val, acc[1] + 1)
comb_func = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])

avg = data.aggregateByKey(zero_value, seq_func, comb_func) \
    .mapValues(lambda x: x[0] / x[1])
```

### Top N per Group
```python
data = sc.parallelize([
    ("A", 10), ("A", 20), ("A", 5),
    ("B", 15), ("B", 25), ("B", 10)
])

# Get top 2 per key
top_n = data.groupByKey() \
    .mapValues(lambda vals: sorted(vals, reverse=True)[:2])
```

---

## 📚 Additional Resources

- [Spark RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [RDD API Documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.html)
- Example notebooks in `notebooks/examples/`

---

## ✅ Checklist

After completing these examples, you should be able to:

- [ ] Explain what an RDD is
- [ ] Differentiate transformations from actions
- [ ] Understand lazy evaluation and DAG
- [ ] Use map, flatMap, and filter effectively
- [ ] Perform joins on RDDs
- [ ] Explain why shuffle is expensive
- [ ] Choose reduceByKey over groupByKey
- [ ] Optimize partitioning for performance
- [ ] Use mapPartitions for expensive operations
- [ ] Cache RDDs appropriately
- [ ] Avoid common pitfalls (collect() OOM, excessive shuffles)

---

**Status**: ✅ Complete comprehensive RDD operations guide
**Examples**: 6 files covering all major RDD operations
**Interview Ready**: Yes - covers all common interview questions

For DataFrame operations, see `src/pandas_vs_pyspark/` directory.
