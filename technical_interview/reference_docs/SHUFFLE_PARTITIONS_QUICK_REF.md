# Shuffle Partitions - Quick Reference Card

## 🎯 What Is It?

```python
spark.conf.set("spark.sql.shuffle.partitions", N)
```

**Controls:** Number of partitions when data is redistributed across cluster

## 📊 When It Matters

| Operation | Shuffle? |
|-----------|----------|
| JOIN | ✅ Yes |
| GROUP BY | ✅ Yes |
| Window PARTITION BY | ✅ Yes |
| DISTINCT | ✅ Yes |
| ORDER BY | ✅ Yes |
| filter, select, map | ❌ No |

## 📏 Quick Formula

```
Partitions = Data Size (MB) / 128 MB
```

## 🎓 Cheat Sheet

| Data Size | Partitions |
|-----------|------------|
| < 1 GB (local) | 2-10 |
| 10 GB | ~80 |
| 100 GB | ~800 |
| 1 TB | ~8,000 |

## ⚠️ Common Mistakes

### Too Few ❌
```python
# 100GB with 4 partitions = 25GB each
spark.conf.set("spark.sql.shuffle.partitions", "4")
# Result: OOM, slow
```

### Too Many ❌
```python
# 1GB with 10,000 partitions = 100KB each  
spark.conf.set("spark.sql.shuffle.partitions", "10000")
# Result: Overhead, slow
```

### Just Right ✅
```python
# 100GB with 800 partitions = 128MB each
spark.conf.set("spark.sql.shuffle.partitions", "800")
# Result: Fast!
```

## 💡 Interview One-Liner

**Q:** "What are shuffle partitions?"

**A:** "Number of partitions created when Spark redistributes data during joins, group by, or window functions. Set via `spark.sql.shuffle.partitions`. Target 128-200MB per partition."

## 🚀 Production Settings

```python
# Local development
.config("spark.sql.shuffle.partitions", "4")

# Production (10-100GB)
.config("spark.sql.shuffle.partitions", "200")

# Big Data (100GB+)
.config("spark.sql.shuffle.partitions", "1000")
.config("spark.sql.adaptive.enabled", "true")
```

## 🔍 Check Current Value

```python
spark.conf.get("spark.sql.shuffle.partitions")
df.rdd.getNumPartitions()
```

---

📖 **Full guide:** [shuffle_partitions_explanation.md](shuffle_partitions_explanation.md)
