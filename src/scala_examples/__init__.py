"""
Scala Examples Package

This package contains Scala code examples and Scala-PySpark integration patterns.

Scala is a JVM language that combines object-oriented and functional programming.
Apache Spark is natively written in Scala (~400K lines), making Scala the most
performant language for Spark applications.

Modules:
--------
1. 01_scala_basics.scala - Pure Scala syntax and concepts
2. 02_spark_scala_basics.scala - Basic Spark operations in Scala
3. 03_scala_udfs.scala - Scala UDFs (User-Defined Functions)
4. 04_scala_udafs.scala - Scala UDAFs (User-Defined Aggregate Functions)
5. 05_scala_datasets.scala - Type-safe Datasets (Scala-only feature)
6. 06_scala_pyspark_integration.scala - Scala + PySpark integration
7. 07_performance_comparison.scala - Scala vs PySpark performance
8. 08_advanced_patterns.scala - Advanced Scala patterns for Spark

Language Comparison:
-------------------
Scala is similar to:
- Java (runs on JVM, uses Java libraries)
- Python (concise syntax, functional programming)
- Kotlin (modern JVM language)
- Rust (type safety, pattern matching)

Key Differences from Python:
- Statically typed (types checked at compile time)
- Functional programming paradigm
- Immutability by default
- Pattern matching
- Case classes
- Traits (like interfaces)

When to Use Scala:
- Performance-critical applications (TB+ data)
- Low-latency requirements (<100ms)
- Type safety needed
- Custom Spark development
- Team has JVM expertise
"""

__version__ = "1.0.0"
__author__ = "PySpark Learning Project"

# Note: Scala files (.scala) cannot be directly imported in Python
# They need to be compiled and packaged as JARs
