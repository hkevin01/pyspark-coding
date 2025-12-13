#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PySpark Undefined Behavior: Closure Serialization Issues
================================================================================

MODULE OVERVIEW:
----------------
This educational module demonstrates dangerous patterns related to Python
closures in distributed PySpark environments. Each example shows both the
problematic code (❌) and the correct implementation (✅).

PURPOSE:
--------
Learn to identify and avoid common serialization pitfalls that cause:
- Executor crashes due to non-serializable objects
- Silent data loss from mutable state modifications
- Performance degradation from improper resource management
- Race conditions in distributed environments

TARGET AUDIENCE:
----------------
- PySpark developers transitioning to production environments
- Data engineers debugging serialization failures
- Teams establishing PySpark coding standards
- Interview candidates learning distributed computing gotchas

UNDEFINED BEHAVIORS COVERED:
============================
1. Non-serializable objects in closures (files, locks, sockets)
2. Mutable state captured in UDFs (counters, lists, dicts)
3. Class instance methods in UDFs (capturing self with non-serializable attrs)
4. Global variable modifications (changes lost on executors)
5. Module capture with non-serializable state (loggers, connections)
6. Python late binding in loops (all lambdas use final value)
7. Broadcast variable misuse (read-only violations)

KEY LEARNING OUTCOMES:
======================
After studying this module, you will:
- Recognize serialization failures before deployment
- Understand the driver-executor execution model
- Know when to use accumulators vs global variables
- Write serialization-safe UDFs automatically
- Debug "cannot pickle" errors quickly

USAGE:
------
Run this file directly to see all examples:
    $ python3 01_closure_serialization.py

Or import specific functions for targeted learning:
    from closure_serialization import dangerous_non_serializable
    dangerous_non_serializable()

RELATED RESOURCES:
------------------
- PySpark Programming Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Python Pickle Documentation: https://docs.python.org/3/library/pickle.html
- Spark UDF Best Practices: https://spark.apache.org/docs/latest/sql-pyspark-pandas-with-arrow.html

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0
CREATED: 2025
LAST MODIFIED: 2025

================================================================================
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import pickle  # Used for demonstrating serialization concepts

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType, StringType

# ============================================================================
# DANGEROUS PATTERN 1: Non-Serializable Objects
# ============================================================================


def dangerous_non_serializable():
    """
    ============================================================================
    ⚠️ DANGEROUS: Capturing Non-Serializable Objects in UDF Closures
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    This function shows what happens when you try to use file handles, database
    connections, or other non-serializable objects inside a UDF. When Spark
    tries to send the UDF to executors, Python's pickle mechanism fails because
    these objects cannot be serialized.

    THE PROBLEM:
    ------------
    - File handles maintain OS-level file descriptors (integers)
    - Database connections have socket connections and state
    - Thread locks contain system-level synchronization primitives
    - These cannot be pickled and transmitted across network

    WHY IT FAILS:
    -------------
    1. Driver creates UDF with closure capturing file handle
    2. Spark tries to pickle UDF to send to executors
    3. Pickle encounters file handle (non-serializable)
    4. Either: Serialization error OR executor receives broken object
    5. Result: Job crashes or unpredictable behavior

    REAL-WORLD SCENARIO:
    --------------------
    Imagine logging each processed record to a file. If you open the file
    on the driver and try to write from executors, it will fail because:
    - The file handle is driver-local
    - Executors run on different machines
    - File descriptor doesn't exist on executor nodes

    EXPECTED BEHAVIOR:
    ------------------
    - TypeError or PicklingError during job submission
    - Or: Silent failure with no file writes
    - Or: Executor crash with "AttributeError" or "IOError"

    SYMPTOMS IN PRODUCTION:
    -----------------------
    - Error: "cannot pickle '_io.TextIOWrapper' object"
    - Error: "cannot pickle 'thread.lock' object"
    - Executors crash during task execution
    - Logs show serialization-related stack traces

    SEE ALSO:
    ---------
    - safe_resource_creation() - Shows correct approach
    - Python pickle limitations: https://docs.python.org/3/library/pickle.html

    ============================================================================
    """
    # ========================================================================
    # STEP 1: Create Spark session
    # ========================================================================
    spark = (
        SparkSession.builder.appName("Non-Serializable UB")
        .master("local[*]")
        .getOrCreate()
    )

    # ========================================================================
    # STEP 2: Open file handle on DRIVER (this is the problem!)
    # ========================================================================
    # ❌ DANGER: File handle in closure - cannot be serialized!
    # This file is opened on the driver node, but executors will try to use it
    log_file = open("/tmp/log.txt", "w")

    # ========================================================================
    # STEP 3: Define UDF that captures file handle in closure
    # ========================================================================
    @udf(StringType())
    def log_and_process(value):
        """
        ❌ PROBLEMATIC UDF: Tries to write to driver's file handle from executor.

        The closure of this UDF captures 'log_file' from the outer scope.
        When Spark tries to serialize this UDF to send to executors,
        it will fail because file handles are not serializable.
        """
        try:
            # This line will NEVER work on executors because:
            # 1. log_file is a driver-local file descriptor
            # 2. Executors run on different machines
            # 3. File descriptor doesn't exist on executor nodes
            log_file.write(f"Processing: {value}\n")
            return value.upper()
        except Exception as e:
            return f"ERROR: {str(e)}"

    # ========================================================================
    # STEP 4: Create test DataFrame
    # ========================================================================
    df = spark.range(10).withColumn("id_str", col("id").cast("string"))

    # ========================================================================
    # STEP 5: Try to execute UDF (will fail!)
    # ========================================================================
    try:
        result = df.withColumn("processed", log_and_process(col("id_str")))
        result.show()  # ❌ Will crash or behave unpredictably
    except Exception as e:
        print(f"❌ EXPECTED ERROR: {type(e).__name__}: {str(e)[:100]}")

    # ========================================================================
    # STEP 6: Cleanup
    # ========================================================================
    log_file.close()
    spark.stop()


# ============================================================================
# SAFE ALTERNATIVE 1: Create Resources on Executors
# ============================================================================


def safe_resource_creation():
    """
    ============================================================================
    ✅ SAFE: Create Resources Inside UDF (On Each Executor)
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    This function shows the CORRECT way to use file handles or other resources
    in distributed PySpark environments. Instead of creating resources on the
    driver, create them inside the UDF so they exist locally on each executor.

    THE SOLUTION:
    -------------
    - Don't create file handles/connections on driver
    - Create them INSIDE the UDF function body
    - Each executor creates its own local resource
    - No serialization needed (resource is executor-local)

    WHY IT WORKS:
    -------------
    1. Driver serializes UDF CODE (not resources)
    2. Executors receive the UDF code
    3. Each executor runs the UDF
    4. Each UDF execution creates its own local file handle
    5. File writes happen on executor's local filesystem

    KEY PRINCIPLE:
    --------------
    "Resources should be created where they are used, not where the code is defined"

    TRADE-OFFS:
    -----------
    ✅ Pros:
    - Works correctly in distributed environment
    - No serialization errors
    - Each executor has its own resource

    ⚠️  Cons:
    - Each executor creates separate log file
    - Driver doesn't have access to executor logs
    - Need log aggregation for central logging

    BEST PRACTICES:
    ---------------
    1. For logging: Use Spark's built-in logging or centralized log aggregation
    2. For databases: Use connection pooling per executor
    3. For files: Write to distributed filesystem (HDFS/S3)
    4. For temp data: Use executor-local disk with cleanup

    PRODUCTION RECOMMENDATIONS:
    ---------------------------
    - Use context managers (with statements) for automatic cleanup
    - Handle resource creation failures gracefully
    - Consider resource pooling to avoid creation overhead
    - Monitor resource usage per executor

    ============================================================================
    """
    # ========================================================================
    # STEP 1: Create Spark session
    # ========================================================================
    spark = (
        SparkSession.builder.appName("Safe Resources").master("local[*]").getOrCreate()
    )

    # ========================================================================
    # STEP 2: Define UDF that creates resources INSIDE function body
    # ========================================================================
    @udf(StringType())
    def safe_log_and_process(value):
        """
        ✅ SAFE UDF: Creates file handle locally on each executor.

        Key differences from dangerous version:
        1. No closure capturing external file handle
        2. File created inside UDF (executor-local)
        3. Context manager ensures cleanup
        4. Each executor writes to its own local file
        """
        # ✅ CORRECT: Create file handle INSIDE UDF (on executor)
        # This file is opened on the executor node where this UDF runs
        # Each executor will have its own /tmp/executor_log.txt file
        with open("/tmp/executor_log.txt", "a") as f:
            f.write(f"Processing: {value}\n")
        return value.upper()

    # ========================================================================
    # STEP 3: Create test DataFrame
    # ========================================================================
    df = spark.range(10).withColumn("id_str", col("id").cast("string"))

    # ========================================================================
    # STEP 4: Execute UDF (works correctly!)
    # ========================================================================
    result = df.withColumn("processed", safe_log_and_process(col("id_str")))
    result.show()

    print("✅ SAFE: Resources created on executors")

    # ========================================================================
    # STEP 5: Cleanup
    # ========================================================================
    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 2: Mutable State in Closures
# =============================================================================


def dangerous_mutable_state():
    """
    ⚠️ UNDEFINED BEHAVIOR: Mutable state captured in UDF.

    PROBLEM: Counter modifications don't propagate back to driver.
    RESULT: Counter remains 0, modifications lost in distributed execution.
    """
    spark = (
        SparkSession.builder.appName("Mutable State UB")
        .master("local[*]")
        .getOrCreate()
    )

    # ❌ DANGER: Mutable counter captured in closure
    counter = [0]  # Mutable list

    @udf(IntegerType())
    def increment_counter(value):
        # Modifications happen on executors, NOT visible to driver
        counter[0] += 1
        return counter[0]

    df = spark.range(100)
    result = df.withColumn("count", increment_counter(col("id")))

    result.show(10)

    # ❌ BUG: Counter is still 0 on driver!
    print(f"❌ UNDEFINED: Counter value on driver: {counter[0]}")
    print(f"   Expected: 100, Got: {counter[0]}")
    print(f"   Modifications on executors are LOST!")

    spark.stop()


# =============================================================================
# SAFE ALTERNATIVE 2: Use accumulators
# =============================================================================


def safe_accumulator():
    """
    ✅ SAFE: Use Spark accumulators for distributed counters.
    """
    spark = (
        SparkSession.builder.appName("Safe Accumulator")
        .master("local[*]")
        .getOrCreate()
    )

    # ✅ CORRECT: Use Spark accumulator
    counter = spark.sparkContext.accumulator(0)

    def process_with_counter(iterator):
        for row in iterator:
            counter.add(1)
            yield row

    df = spark.range(100)
    rdd = df.rdd.mapPartitions(process_with_counter)
    result = rdd.toDF()

    result.count()  # Trigger execution

    print(f"✅ SAFE: Counter value: {counter.value}")

    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 3: Class Instance Methods in UDFs
# =============================================================================


class DataProcessor:
    def __init__(self):
        self.config = {"multiplier": 10}
        self.cache = {}  # Non-serializable cache
        self.lock = __import__("threading").Lock()  # Non-serializable!

    def process(self, value):
        """Instance method - captures entire self!"""
        with self.lock:  # ❌ Lock cannot be serialized!
            return value * self.config["multiplier"]


def dangerous_instance_methods():
    """
    ⚠️ UNDEFINED BEHAVIOR: Using instance methods as UDFs.

    PROBLEM: Instance methods capture entire object (self).
    RESULT: Non-serializable attributes cause crashes.
    """
    spark = (
        SparkSession.builder.appName("Instance Method UB")
        .master("local[*]")
        .getOrCreate()
    )

    processor = DataProcessor()

    # ❌ DANGER: Instance method captures self with non-serializable lock
    process_udf = udf(processor.process, IntegerType())

    df = spark.range(10)

    try:
        result = df.withColumn("processed", process_udf(col("id")))
        result.show()
    except Exception as e:
        print(f"❌ EXPECTED ERROR: {type(e).__name__}: {str(e)[:100]}")

    spark.stop()


# =============================================================================
# SAFE ALTERNATIVE 3: Static methods or module-level functions
# =============================================================================


class SafeDataProcessor:
    MULTIPLIER = 10

    @staticmethod
    def process(value):
        """Static method - no self capture"""
        return value * SafeDataProcessor.MULTIPLIER


def safe_static_methods():
    """
    ✅ SAFE: Use static methods or module-level functions.
    """
    spark = (
        SparkSession.builder.appName("Safe Static Method")
        .master("local[*]")
        .getOrCreate()
    )

    # ✅ CORRECT: Static method doesn't capture instance
    process_udf = udf(SafeDataProcessor.process, IntegerType())

    df = spark.range(10)
    result = df.withColumn("processed", process_udf(col("id")))
    result.show()

    print("✅ SAFE: Static method works correctly")
    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 4: Global Variable Modifications
# =============================================================================

GLOBAL_COUNTER = 0


def dangerous_global_modifications():
    """
    ⚠️ UNDEFINED BEHAVIOR: Modifying global variables in UDFs.

    PROBLEM: Global modifications on executors don't affect driver.
    RESULT: Silent data loss, race conditions.
    """
    spark = (
        SparkSession.builder.appName("Global Mod UB").master("local[*]").getOrCreate()
    )

    global GLOBAL_COUNTER

    @udf(IntegerType())
    def modify_global(value):
        global GLOBAL_COUNTER
        GLOBAL_COUNTER += 1  # ❌ Modification on executor, not visible to driver
        return value * 2

    df = spark.range(100)
    result = df.withColumn("doubled", modify_global(col("id")))

    result.show(10)

    print(f"❌ UNDEFINED: GLOBAL_COUNTER on driver: {GLOBAL_COUNTER}")
    print(f"   Expected: 100, Got: {GLOBAL_COUNTER}")

    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 5: Capturing Entire Modules
# =============================================================================


def dangerous_module_capture():
    """
    ⚠️ UNDEFINED BEHAVIOR: Capturing modules with non-serializable state.

    PROBLEM: Some modules contain non-serializable state.
    RESULT: Serialization failures, undefined behavior.
    """
    spark = (
        SparkSession.builder.appName("Module Capture UB")
        .master("local[*]")
        .getOrCreate()
    )

    import logging

    logger = logging.getLogger(__name__)  # May have non-serializable handlers

    @udf(StringType())
    def log_process(value):
        # ❌ Logger may not serialize correctly
        try:
            logger.info(f"Processing {value}")
            return value.upper()
        except Exception as e:
            return f"ERROR: {str(e)}"

    df = spark.range(10).withColumn("id_str", col("id").cast("string"))

    try:
        result = df.withColumn("processed", log_process(col("id_str")))
        result.show()
    except Exception as e:
        print(f"❌ POTENTIAL ERROR: {type(e).__name__}: {str(e)[:100]}")

    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 6: Late Binding Issues
# =============================================================================


def dangerous_late_binding():
    """
    ⚠️ UNDEFINED BEHAVIOR: Python late binding in list comprehensions.

    PROBLEM: Closures capture variable reference, not value.
    RESULT: All UDFs use final loop value!
    """
    spark = (
        SparkSession.builder.appName("Late Binding UB").master("local[*]").getOrCreate()
    )

    # ❌ DANGER: Late binding - all UDFs will use i=4
    udfs = [udf(lambda x: x * i, IntegerType()) for i in range(5)]

    df = spark.range(10)

    # All columns will have same values (multiplied by 4)!
    for idx, multiplier_udf in enumerate(udfs):
        df = df.withColumn(f"mult_{idx}", multiplier_udf(col("id")))

    df.show()

    print("❌ UNDEFINED: All multipliers use i=4 due to late binding!")
    print("   Expected: Different multipliers (0,1,2,3,4)")
    print("   Got: All use final value (4)")

    spark.stop()


# =============================================================================
# SAFE ALTERNATIVE 6: Early binding with default arguments
# =============================================================================


def safe_early_binding():
    """
    ✅ SAFE: Use default arguments for early binding.
    """
    spark = (
        SparkSession.builder.appName("Safe Early Binding")
        .master("local[*]")
        .getOrCreate()
    )

    # ✅ CORRECT: Early binding with default argument
    udfs = [udf(lambda x, i=i: x * i, IntegerType()) for i in range(5)]

    df = spark.range(10)

    for idx, multiplier_udf in enumerate(udfs):
        df = df.withColumn(f"mult_{idx}", multiplier_udf(col("id")))

    df.show()

    print("✅ SAFE: Each UDF has correct multiplier")

    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 7: Broadcast Variable Misuse
# =============================================================================


def dangerous_broadcast_misuse():
    """
    ⚠️ UNDEFINED BEHAVIOR: Modifying broadcast variables.

    PROBLEM: Broadcast variables are read-only.
    RESULT: Modifications not visible, silent failures.
    """
    spark = (
        SparkSession.builder.appName("Broadcast Misuse UB")
        .master("local[*]")
        .getOrCreate()
    )

    # ❌ DANGER: Mutable object in broadcast
    config = {"count": 0}
    broadcast_config = spark.sparkContext.broadcast(config)

    @udf(IntegerType())
    def modify_broadcast(value):
        # ❌ Modifications to broadcast variables are UNDEFINED
        broadcast_config.value["count"] += 1
        return broadcast_config.value["count"]

    df = spark.range(10)
    result = df.withColumn("count", modify_broadcast(col("id")))

    result.show()

    print(f"❌ UNDEFINED: Broadcast modifications are not reliable!")
    print(f"   Original config: {config}")
    print(f"   Broadcast config: {broadcast_config.value}")

    spark.stop()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("PYSPARK UNDEFINED BEHAVIOR: CLOSURE SERIALIZATION")
    print("=" * 80)

    print("\n1. ❌ Dangerous: Non-Serializable Objects")
    print("-" * 80)
    dangerous_non_serializable()

    print("\n2. ✅ Safe: Resource Creation on Executors")
    print("-" * 80)
    safe_resource_creation()

    print("\n3. ❌ Dangerous: Mutable State in Closures")
    print("-" * 80)
    dangerous_mutable_state()

    print("\n4. ✅ Safe: Using Accumulators")
    print("-" * 80)
    safe_accumulator()

    print("\n5. ❌ Dangerous: Instance Methods in UDFs")
    print("-" * 80)
    dangerous_instance_methods()

    print("\n6. ✅ Safe: Static Methods")
    print("-" * 80)
    safe_static_methods()

    print("\n7. ❌ Dangerous: Global Variable Modifications")
    print("-" * 80)
    dangerous_global_modifications()

    print("\n8. ❌ Dangerous: Late Binding Issues")
    print("-" * 80)
    dangerous_late_binding()

    print("\n9. ✅ Safe: Early Binding with Default Arguments")
    print("-" * 80)
    safe_early_binding()

    print("\n10. ❌ Dangerous: Broadcast Variable Misuse")
    print("-" * 80)
    dangerous_broadcast_misuse()

    print("\n" + "=" * 80)
    print("KEY TAKEAWAYS:")
    print("=" * 80)
    print("❌ Never capture non-serializable objects (files, locks, sockets)")
    print("❌ Don't modify mutable state in UDFs (use accumulators)")
    print("❌ Avoid instance methods in UDFs (use static methods)")
    print("❌ Don't modify global variables in UDFs")
    print("❌ Watch out for Python late binding in loops")
    print("❌ Never modify broadcast variables")
    print("✅ Create resources inside UDFs on executors")
    print("✅ Use Spark accumulators for counters")
    print("✅ Use static methods or module-level functions")
    print("✅ Use default arguments for early binding")
    print("=" * 80 + "\n")
