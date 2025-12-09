"""
Pandas vs PySpark Comparison Package
====================================

This package contains executable comparison scripts demonstrating
the differences between Pandas and PySpark DataFrame operations.

Scripts:
    01_basic_operations.py - Basic DataFrame operations
    02_aggregations_groupby.py - Aggregations and groupby operations  
    03_joins.py - Join operations (inner, left, right, outer, anti)
    04_string_operations.py - String manipulation functions
    05_missing_data.py - Handling null/missing values

Usage:
    Run any script directly:
    $ python 01_basic_operations.py
    
    Or from the project root:
    $ python src/pandas_vs_pyspark/01_basic_operations.py

Each script is self-contained and creates its own sample data.
"""

__version__ = "1.0.0"
__author__ = "PySpark Learning Project"
__all__ = [
    "01_basic_operations",
    "02_aggregations_groupby",
    "03_joins",
    "04_string_operations",
    "05_missing_data",
]
