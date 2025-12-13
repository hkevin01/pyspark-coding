"""
Python Ecosystem Integration with PySpark

This package demonstrates how to integrate the powerful Python data science
ecosystem with Apache Spark for distributed big data processing.

WHY THIS MATTERS:
=================
PySpark gives you access to the ENTIRE Python ecosystem:
- NumPy: Fast numerical operations
- Pandas: Rich DataFrame manipulation
- Scikit-learn: Machine learning algorithms
- PyTorch: Deep learning and neural networks
- Matplotlib: Publication-quality plots
- Seaborn: Statistical data visualization

This is PySpark's BIGGEST ADVANTAGE over Scala!

Modules:
--------
1. 01_numpy_integration.py - NumPy arrays with PySpark
2. 02_pandas_integration.py - Pandas DataFrames and Pandas UDFs
3. 03_sklearn_integration.py - Scikit-learn ML pipelines
4. 04_pytorch_integration.py - PyTorch deep learning with Spark
5. 05_visualization.py - Matplotlib and Seaborn with PySpark
6. 06_complete_ml_pipeline.py - End-to-end ML project

PERFORMANCE NOTES:
==================
- Pandas UDFs: 10-20x faster than standard Python UDFs
- Use Apache Arrow for efficient data transfer
- Convert to Pandas for advanced operations
- Leverage distributed computing for big data
- Use single-node libraries for post-processing

BEST PRACTICES:
===============
1. Use PySpark for distributed data processing
2. Use Pandas UDFs for vectorized operations
3. Use Scikit-learn for small-to-medium ML tasks
4. Use PyTorch for deep learning on samples
5. Use Matplotlib/Seaborn for visualization
6. Sample large datasets before plotting
"""

__version__ = "1.0.0"
__author__ = "PySpark Learning Project"

# Package exports
__all__ = [
    'numpy_integration',
    'pandas_integration',
    'sklearn_integration',
    'pytorch_integration',
    'visualization',
    'complete_ml_pipeline'
]
