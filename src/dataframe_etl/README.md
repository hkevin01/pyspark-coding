# DataFrame ETL Operations

Complete guide for DataFrame transformations and ETL.

## Quick Reference

```python
from pyspark.sql.functions import *

# Select
df.select("name", "age")

# Filter
df.filter(col("age") > 25)

# Sort
df.orderBy(col("age").desc())

# Join
df1.join(df2, "key")

# GroupBy
df.groupBy("city").count()

# Aggregation
df.agg(sum("salary"), avg("age"))

# Window
from pyspark.sql.window import Window
window = Window.partitionBy("id").orderBy("date")
df.withColumn("rank", rank().over(window))
```

## Built-in Functions

- **String**: upper, lower, trim, concat, split
- **Date**: current_date, date_add, datediff
- **Math**: abs, ceil, floor, round, sqrt
- **Null**: isNull, fillna, dropna, coalesce
- **Collection**: array, explode, size

## See Also

- **PYSPARK_MASTER_CURRICULUM.md** - Complete ETL section
- **src/etl/** - ETL pipeline examples
- **notebooks/examples/02_sales_analysis.ipynb** - Sales ETL example
