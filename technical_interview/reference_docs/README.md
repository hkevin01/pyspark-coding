# Reference Documentation

Quick reference guides for common PySpark interview topics.

## Available References

### shuffle_partitions_explanation.md
Complete guide to shuffle partitions with line-by-line code explanations.

### SHUFFLE_PARTITIONS_QUICK_REF.md
Quick reference card for shuffle partition optimization.

### shuffle_section.json
JSON data structure with shuffle examples.

## When to Use During Interview

**Before coding:**
- Review shuffle concepts if the problem involves joins/groupBy
- Check partition optimization strategies
- Refresh memory on when shuffles occur

**During interview:**
- These are quick glance references
- Focus on understanding, not memorization
- Explain trade-offs to the interviewer

## Key Concepts

1. **Shuffle** - Data movement across nodes (expensive)
2. **Partitions** - How data is divided across workers
3. **spark.sql.shuffle.partitions** - Default: 200
4. **Optimization** - Reduce shuffles, right-size partitions
