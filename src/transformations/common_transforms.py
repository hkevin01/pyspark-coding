"""
================================================================================
Common Transformations Module
================================================================================

PURPOSE:
--------
Reusable library of production-ready transformation functions for PySpark
DataFrames, enabling clean, maintainable, and DRY (Don't Repeat Yourself)
data pipelines.

WHAT THIS DOES:
---------------
Provides battle-tested transformation functions:
- Data quality (remove duplicates, handle nulls)
- Schema operations (rename columns, select, cast types)
- Data cleaning (trim strings, standardize formats)
- Feature engineering (binning, encoding, scaling)
- Time operations (date parsing, timezone conversion)

WHY REUSABLE TRANSFORMS:
------------------------
- DRY PRINCIPLE: Write once, use everywhere
- CONSISTENCY: Same logic across all pipelines
- TESTABILITY: Unit test once, trust everywhere
- MAINTAINABILITY: Fix bugs in one place
- DOCUMENTATION: Centralized function docs

HOW TO USE:
-----------
```python
from transformations.common_transforms import CommonTransforms

# Remove duplicates
df_clean = CommonTransforms.remove_duplicates(df, subset=['id'])

# Handle nulls
df_filled = CommonTransforms.fill_nulls(df, {'age': 0, 'name': 'Unknown'})

# Rename columns
df_renamed = CommonTransforms.rename_columns(df, {'old_name': 'new_name'})

# Chain transformations
df_transformed = (df
    .transform(lambda x: CommonTransforms.remove_duplicates(x, ['id']))
    .transform(lambda x: CommonTransforms.filter_nulls(x, ['email']))
    .transform(lambda x: CommonTransforms.rename_columns(x, name_mapping))
)
```

BEST PRACTICES:
---------------
1. PURE FUNCTIONS:
   - Take DataFrame, return DataFrame
   - No side effects (no writing, no globals)
   - Deterministic (same input → same output)

2. TYPE HINTS:
   - Specify input/output types
   - Enable IDE autocomplete
   - Catch errors at development time

3. DOCUMENTATION:
   - Clear docstrings with Args/Returns
   - Examples for complex functions
   - Explain WHY, not just WHAT

4. ERROR HANDLING:
   - Validate inputs (check column exists)
   - Provide clear error messages
   - Handle edge cases gracefully

5. PERFORMANCE:
   - Minimize shuffles (use DataFrame API)
   - Cache intermediate results if reused
   - Broadcast small lookup tables

DESIGN PATTERNS:
----------------

TRANSFORM PATTERN:
```python
def my_transform(df: DataFrame, param: str) -> DataFrame:
    \"\"\"Brief description.\"\"\"
    return df.withColumn("new_col", F.col("old_col") + 1)

# Usage with transform()
df.transform(my_transform, param="value")
```

BUILDER PATTERN:
```python
class TransformBuilder:
    def __init__(self, df):
        self.df = df

    def remove_duplicates(self, subset=None):
        self.df = self.df.dropDuplicates(subset)
        return self

    def build(self):
        return self.df

# Usage
result = (TransformBuilder(df)
    .remove_duplicates(['id'])
    .fill_nulls({'age': 0})
    .build())
```

WHEN TO USE:
------------
✅ Transformation used in 2+ pipelines
✅ Complex logic that benefits from abstraction
✅ Domain-specific operations (e.g., address parsing)
✅ Standardization needed across team
❌ Simple one-liners (df.select("col"))
❌ Pipeline-specific logic

================================================================================
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType


class CommonTransforms:
    """Common data transformation operations."""

    @staticmethod
    def remove_duplicates(df: DataFrame, subset: List[str] = None) -> DataFrame:
        """
        Remove duplicate rows from DataFrame.

        Args:
            df: Input DataFrame
            subset: Column names to consider for duplicates

        Returns:
            DataFrame without duplicates
        """
        return df.dropDuplicates(subset) if subset else df.dropDuplicates()

    @staticmethod
    def filter_nulls(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Filter out rows with null values in specified columns.

        Args:
            df: Input DataFrame
            columns: List of column names

        Returns:
            Filtered DataFrame
        """
        for col in columns:
            df = df.filter(F.col(col).isNotNull())
        return df

    @staticmethod
    def fill_nulls(df: DataFrame, value_dict: dict) -> DataFrame:
        """
        Fill null values with specified values.

        Args:
            df: Input DataFrame
            value_dict: Dictionary mapping column names to fill values

        Returns:
            DataFrame with nulls filled
        """
        return df.fillna(value_dict)

    @staticmethod
    def rename_columns(df: DataFrame, name_mapping: dict) -> DataFrame:
        """
        Rename columns based on provided mapping.

        Args:
            df: Input DataFrame
            name_mapping: Dictionary mapping old names to new names

        Returns:
            DataFrame with renamed columns
        """
        for old_name, new_name in name_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    @staticmethod
    def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Select specific columns from DataFrame.

        Args:
            df: Input DataFrame
            columns: List of column names to select

        Returns:
            DataFrame with selected columns
        """
        return df.select(columns)

    @staticmethod
    def add_timestamp_column(
        df: DataFrame, col_name: str = "processed_at"
    ) -> DataFrame:
        """
        Add a timestamp column with current timestamp.

        Args:
            df: Input DataFrame
            col_name: Name for the timestamp column

        Returns:
            DataFrame with timestamp column added
        """
        return df.withColumn(col_name, F.current_timestamp())

    @staticmethod
    def cast_column_types(df: DataFrame, type_mapping: dict) -> DataFrame:
        """
        Cast columns to specified data types.

        Args:
            df: Input DataFrame
            type_mapping: Dictionary mapping column names to data types

        Returns:
            DataFrame with casted columns
        """
        for col_name, data_type in type_mapping.items():
            df = df.withColumn(col_name, F.col(col_name).cast(data_type))
        return df

    @staticmethod
    def trim_string_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Trim whitespace from string columns.

        Args:
            df: Input DataFrame
            columns: List of column names to trim

        Returns:
            DataFrame with trimmed columns
        """
        for col in columns:
            df = df.withColumn(col, F.trim(F.col(col)))
        return df

    @staticmethod
    def lowercase_columns(df: DataFrame, columns: List[str]) -> DataFrame:
        """
        Convert string columns to lowercase.

        Args:
            df: Input DataFrame
            columns: List of column names to convert

        Returns:
            DataFrame with lowercase columns
        """
        for col in columns:
            df = df.withColumn(col, F.lower(F.col(col)))
        return df

    @staticmethod
    def add_derived_column(df: DataFrame, new_col_name: str, expression) -> DataFrame:
        """
        Add a new column based on an expression.

        Args:
            df: Input DataFrame
            new_col_name: Name for the new column
            expression: PySpark column expression

        Returns:
            DataFrame with new column
        """
        return df.withColumn(new_col_name, expression)
