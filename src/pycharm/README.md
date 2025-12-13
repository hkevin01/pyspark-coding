# PyCharm for PySpark Development

Complete guide for using PyCharm IDE with PySpark projects.

## Quick Start

```python
# 1. Install PyCharm
brew install --cask pycharm-ce  # macOS
# or download from jetbrains.com

# 2. Create project
File → New Project → pyspark-project

# 3. Install PySpark
pip install pyspark

# 4. Create first app
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.range(100)
df.show()
```

## Key Features

- ✅ Intelligent code completion
- ✅ Debugging support
- ✅ Git integration
- ✅ Database tools
- ✅ Remote development
- ✅ Jupyter notebook support

## See Also

- **PYSPARK_MASTER_CURRICULUM.md** - Complete PyCharm section
- **notebooks/** - Example Jupyter notebooks
