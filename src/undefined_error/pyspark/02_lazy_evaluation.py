#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
PySpark Undefined Behavior: Lazy Evaluation Pitfalls
================================================================================

MODULE OVERVIEW:
----------------
This educational module demonstrates dangerous patterns that arise from Spark's
lazy evaluation model. Lazy evaluation means transformations don't execute
immediately - they're only computed when an action is triggered. This powerful
optimization can lead to subtle bugs if not understood properly.

PURPOSE:
--------
Learn to recognize and prevent common lazy evaluation pitfalls that cause:
- Performance degradation from unnecessary recomputations
- Silent failures when transformations never execute
- Data inconsistencies from side effects in UDFs
- Inflated accumulator values from DAG recomputation
- Non-deterministic behavior from random/time-dependent operations

TARGET AUDIENCE:
----------------
- PySpark developers optimizing query performance
- Data engineers debugging cache-related issues
- Teams establishing lazy evaluation best practices
- Anyone experiencing mysterious recomputation problems

UNDEFINED BEHAVIORS COVERED:
============================
1. Multiple recomputations without caching (severe performance impact)
2. Action-less transformations never executed (silent failures)
3. Side effects in transformations (unpredictable execution)
4. Accumulator double-counting (inflated metrics)
5. Random values changing on recomputation (non-deterministic results)
6. Time-dependent operations (data inconsistency)
7. Execution order assumptions (optimization breaks expectations)
8. Checkpoint vs persist confusion (lineage truncation issues)

KEY LEARNING OUTCOMES:
======================
After studying this module, you will:
- Understand when and why to use .cache() or .persist()
- Recognize transformations that need actions to execute
- Avoid side effects that break in lazy evaluation
- Use accumulators correctly with caching
- Handle random/time-dependent operations safely
- Choose between checkpoint and persist appropriately

USAGE:
------
Run this file directly to see all examples:
    $ python3 02_lazy_evaluation.py

Or import specific functions for targeted learning:
    from lazy_evaluation import dangerous_no_caching, safe_with_caching
    dangerous_no_caching()
    safe_with_caching()

PERFORMANCE IMPACT:
-------------------
⚠️ WARNING: Without proper caching, a DataFrame with 3 actions can take 3x
longer to execute. Cache your expensive transformations!

RELATED RESOURCES:
------------------
- Spark Lazy Evaluation Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations
- Caching Best Practices: https://spark.apache.org/docs/latest/rdd-programming-guide.html#which-storage-level-to-choose
- DAG Visualization: Use Spark UI to see actual execution plans

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0
CREATED: 2025
LAST MODIFIED: December 13, 2025

================================================================================
"""

# ============================================================================
# STANDARD LIBRARY IMPORTS
# ============================================================================
import time  # For measuring performance differences
from datetime import datetime  # For time-dependent operation examples

# ============================================================================
# THIRD-PARTY IMPORTS
# ============================================================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, rand, udf
from pyspark.sql.types import IntegerType, StringType

# ============================================================================
# DANGEROUS PATTERN 1: Multiple Recomputations Without Caching
# ============================================================================


def dangerous_no_caching():
    """
    ============================================================================
    ❌ DANGEROUS: Multiple Actions on Uncached DataFrame
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    Shows the severe performance penalty when multiple actions are executed
    on an expensive DataFrame without caching. Each action recomputes the
    entire DAG from scratch, multiplying the computation time.

    THE PROBLEM:
    ------------
    Spark's lazy evaluation means transformations aren't executed until an
    action (count, collect, show) is triggered. Without caching, EVERY action
    recomputes ALL transformations from the beginning:

    Action 1: Read data → Transform → Count    (Full computation)
    Action 2: Read data → Transform → Sum      (Full computation AGAIN)
    Action 3: Read data → Transform → Max      (Full computation AGAIN)

    WHY IT FAILS:
    -------------
    1. First action triggers full DAG execution
    2. Results are not stored in memory
    3. Second action sees no cached data, recomputes everything
    4. Third action also recomputes everything
    5. Total time = N actions × computation time

    REAL-WORLD SCENARIO:
    --------------------
    You have an expensive ETL pipeline with multiple aggregations:
    - Read 1TB of data from S3
    - Apply complex transformations (joins, filters, window functions)
    - Calculate 10 different metrics (each is an action)
    Without caching: 10 × 1TB reads from S3 = $$$$ in costs!

    SYMPTOMS IN PRODUCTION:
    -----------------------
    - Job takes hours when it should take minutes
    - Spark UI shows repeated identical stages
    - Executors repeatedly processing same partitions
    - Excessive S3/HDFS read metrics
    - High network transfer costs

    SEE ALSO:
    ---------
    - safe_with_caching() - Correct implementation with .cache()

    ============================================================================
    """
    # ========================================================================
    # STEP 1: Create SparkSession
    # ========================================================================
    spark = (
        SparkSession.builder.appName("No Caching UB").master("local[*]").getOrCreate()
    )

    print("❌ DANGEROUS: Computing expensive operation multiple times")

    # ========================================================================
    # STEP 2: Create expensive DataFrame (NOT cached)
    # ========================================================================
    # ❌ DANGER: This DataFrame has expensive transformations but NO cache
    # When we run multiple actions, ALL these transformations will be
    # recomputed from scratch each time
    df = spark.range(1000000)
    expensive_df = (
        df.withColumn("squared", col("id") * col("id"))
        .withColumn("cubed", col("id") * col("id") * col("id"))
        .withColumn("quartic", col("id") * col("id") * col("id") * col("id"))
    )

    # ========================================================================
    # STEP 3: Execute multiple actions WITHOUT caching
    # ========================================================================
    # ❌ DANGER: Each action below recomputes the ENTIRE DAG:
    #   - spark.range(1000000)
    #   - All withColumn transformations
    #   - Then the specific action

    # Action 1: Count rows (triggers full computation)
    start = time.time()
    count = expensive_df.count()
    time1 = time.time() - start

    # Action 2: Sum of squared values (FULL recomputation happens here!)
    start = time.time()
    total = expensive_df.agg({"squared": "sum"}).collect()[0][0]
    time2 = time.time() - start

    # Action 3: Max of cubed values (FULL recomputation happens AGAIN!)
    start = time.time()
    max_val = expensive_df.agg({"cubed": "max"}).collect()[0][0]
    time3 = time.time() - start

    # ========================================================================
    # STEP 4: Display performance metrics
    # ========================================================================
    print(f"   Count: {count} (took {time1:.2f}s)")
    print(f"   Sum: {total} (took {time2:.2f}s)")
    print(f"   Max: {max_val} (took {time3:.2f}s)")
    print(f"   Total time: {time1 + time2 + time3:.2f}s")
    print("   ⚠️ Same computation done 3 times!")
    print("   ⚠️ Check Spark UI - you'll see 3 separate jobs with identical stages")

    spark.stop()


def safe_with_caching():
    """
    ============================================================================
    ✅ SAFE: Cache DataFrame to Prevent Recomputation
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    Shows the correct way to handle multiple actions on an expensive DataFrame
    by using .cache() to store computed results in memory.

    THE SOLUTION:
    -------------
    Call .cache() on the DataFrame BEFORE executing any actions. The first
    action will compute and store results in memory. Subsequent actions will
    read from the cache instead of recomputing:

    Action 1: Read → Transform → Cache → Count   (Full computation + cache)
    Action 2: Read from cache → Sum              (Fast!)
    Action 3: Read from cache → Max              (Fast!)

    WHY IT WORKS:
    -------------
    1. .cache() marks the DataFrame for caching
    2. First action triggers computation and stores result in memory
    3. Subsequent actions detect cached data and skip recomputation
    4. Only the specific action logic (count, sum, max) is executed
    5. Total time = 1 × computation time + N × (fast cache reads)

    KEY PRINCIPLE:
    --------------
    Cache when: (Number of actions > 1) AND (Computation is expensive)
    Don't cache when: Single action OR cheap transformations

    TRADE-OFFS:
    -----------
    ✅ Pros:
    - Massive performance improvement for multiple actions
    - Reduces network/disk I/O
    - Lowers cloud costs (fewer data reads)
    - Enables iterative algorithms (ML training)

    ⚠️ Cons:
    - Consumes executor memory
    - Cache eviction can cause recomputation
    - Requires manual unpersist() for memory management

    BEST PRACTICES:
    ---------------
    1. Always .cache() before executing first action
    2. Use .persist(StorageLevel.X) for fine-grained control
    3. Call .unpersist() when done to free memory
    4. Monitor cache hit rates in Spark UI Storage tab

    SEE ALSO:
    ---------
    - dangerous_no_caching() - Shows the problem this solves

    ============================================================================
    """
    # ========================================================================
    # STEP 1: Create SparkSession
    # ========================================================================
    spark = (
        SparkSession.builder.appName("Safe Caching").master("local[*]").getOrCreate()
    )

    print("✅ SAFE: Computing once and caching")

    # ========================================================================
    # STEP 2: Create expensive DataFrame
    # ========================================================================
    df = spark.range(1000000)
    expensive_df = (
        df.withColumn("squared", col("id") * col("id"))
        .withColumn("cubed", col("id") * col("id") * col("id"))
        .withColumn("quartic", col("id") * col("id") * col("id") * col("id"))
    )

    # ========================================================================
    # STEP 3: Cache BEFORE executing any actions
    # ========================================================================
    # ✅ CORRECT: This marks the DataFrame for caching
    # The first action will trigger computation AND caching
    expensive_df.cache()

    # ========================================================================
    # STEP 4: Execute multiple actions WITH caching
    # ========================================================================
    # Action 1: Triggers full computation AND stores result in cache
    start = time.time()
    count = expensive_df.count()
    time1 = time.time() - start

    # Action 2: Reads from cache (fast!)
    start = time.time()
    total = expensive_df.agg({"squared": "sum"}).collect()[0][0]
    time2 = time.time() - start

    # Action 3: Also reads from cache (fast!)
    start = time.time()
    max_val = expensive_df.agg({"cubed": "max"}).collect()[0][0]
    time3 = time.time() - start

    # ========================================================================
    # STEP 5: Display performance metrics
    # ========================================================================
    print(f"   Count: {count} (took {time1:.2f}s)")
    print(f"   Sum: {total} (took {time2:.2f}s)")
    print(f"   Max: {max_val} (took {time3:.2f}s)")
    print(f"   Total time: {time1 + time2 + time3:.2f}s")
    print("   ✅ Computation done once, cached for subsequent actions!")
    print("   ✅ Check Spark UI Storage tab to see cached DataFrame")

    # ========================================================================
    # STEP 6: Cleanup - free memory
    # ========================================================================
    # 📝 NOTE: Always unpersist when done to free executor memory
    expensive_df.unpersist()
    spark.stop()


# ============================================================================
# DANGEROUS PATTERN 2: Transformations Without Actions
# ============================================================================


def dangerous_no_action():
    """
    ============================================================================
    ❌ DANGEROUS: Transformations Without Triggering Actions
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    Shows how Spark's lazy evaluation can lead to silent failures when
    transformations are defined but never executed. Code runs without errors
    but performs NO actual computation.

    THE PROBLEM:
    ------------
    Transformations (filter, withColumn, select, join, groupBy) are LAZY:
    - They don't execute immediately
    - They only build an execution plan (DAG)
    - Without an action, the plan is never executed

    This means: df.filter(...).select(...) does absolutely NOTHING by itself!

    WHY IT FAILS:
    -------------
    1. User writes transformation code
    2. Code executes instantly (no errors)
    3. User assumes work is done
    4. But NO computation happened - only plan was created
    5. Results are never materialized

    REAL-WORLD SCENARIO:
    --------------------
    You write a data cleaning pipeline:
    - Remove nulls
    - Filter invalid records
    - Transform columns
    But forget to call .write() or .count() at the end.
    Result: Data is never cleaned, downstream systems use dirty data!

    SYMPTOMS IN PRODUCTION:
    -----------------------
    - Code runs in seconds (suspiciously fast)
    - No errors or warnings
    - Spark UI shows no jobs executed
    - Output files are empty or missing
    - Downstream processes fail with unexpected data

    SEE ALSO:
    ---------
    - safe_with_action() - Correct implementation with action
    - Spark Lazy Evaluation: https://spark.apache.org/docs/latest/rdd-programming-guide.html

    ============================================================================
    """
    # ========================================================================
    # STEP 1: Create SparkSession
    # ========================================================================
    spark = (
        SparkSession.builder.appName("No Action UB").master("local[*]").getOrCreate()
    )

    print("❌ DANGEROUS: Creating DataFrame but never executing")

    # ========================================================================
    # STEP 2: Define transformations (NO execution happens!)
    # ========================================================================
    df = spark.range(1000000)

    # ❌ DANGER: All of these do NOTHING without an action!
    # These only build an execution plan - no actual data processing occurs
    result = df.filter(col("id") > 500000)
    result = result.withColumn("doubled", col("id") * 2)
    result = result.select("doubled")

    # ========================================================================
    # STEP 3: Code completes with no errors, but NO WORK was done!
    # ========================================================================
    print("   Code executed, but NO ACTUAL COMPUTATION happened!")
    print("   Transformations are lazy - need .count(), .show(), .collect(), etc.")
    print("   ❌ Check Spark UI - you'll see ZERO jobs executed")

    spark.stop()


def safe_with_action():
    """
    ============================================================================
    ✅ SAFE: Trigger Action to Execute Transformations
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    Shows the correct way to execute transformations by triggering an action.
    Actions (count, collect, show, write, etc.) force Spark to actually
    execute the transformation pipeline.

    THE SOLUTION:
    -------------
    Always end your transformation chain with an action:
    - .count() - Count rows
    - .collect() - Bring all data to driver
    - .show() - Display sample rows
    - .write.X() - Save to storage
    - .foreach() - Process each row

    WHY IT WORKS:
    -------------
    1. Transformations build execution plan (DAG)
    2. Action triggers DAG optimization
    3. Spark executes optimized plan
    4. Results are materialized and returned

    BEST PRACTICES:
    ---------------
    1. Always end pipelines with an action
    2. Use .count() for validation
    3. Use .write() for persistence
    4. Avoid .collect() on large datasets (OOM risk)

    ============================================================================
    """
    # ========================================================================
    # STEP 1: Create SparkSession
    # ========================================================================
    spark = SparkSession.builder.appName("Safe Action").master("local[*]").getOrCreate()

    print("✅ SAFE: Triggering action to execute transformations")

    # ========================================================================
    # STEP 2: Define transformations
    # ========================================================================
    df = spark.range(1000000)
    result = (
        df.filter(col("id") > 500000)
        .withColumn("doubled", col("id") * 2)
        .select("doubled")
    )

    # ========================================================================
    # STEP 3: Trigger action to execute the plan
    # ========================================================================
    # ✅ CORRECT: .count() is an action that triggers execution
    count = result.count()
    print(f"   Processed {count} rows")
    print("   ✅ Check Spark UI - you'll see 1 job with all transformations executed")

    spark.stop()


# ============================================================================
# DANGEROUS PATTERN 3: Side Effects in Transformations
# ============================================================================

# Global variable to demonstrate side effects problem
SIDE_EFFECT_COUNTER = 0


def dangerous_side_effects():
    """
    ============================================================================
    ❌ DANGEROUS: Side Effects in Transformations
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    Shows why you should NEVER have side effects (global variable modifications,
    file writes, database updates) inside UDFs or transformations. Due to lazy
    evaluation and potential recomputation, side effects may execute 0 times,
    1 time, or many times unpredictably.

    THE PROBLEM:
    ------------
    Transformations are:
    - Lazy (may not execute at all)
    - Potentially recomputed (may execute multiple times)
    - Executed on executors (global state not shared with driver)

    Result: Global variable changes are lost or multiplied unpredictably!

    WHY IT FAILS:
    -------------
    1. UDF with side effect defined on driver
    2. UDF serialized and sent to executors
    3. Executors modify their LOCAL copy of global variable
    4. Driver's global variable remains unchanged
    5. Without caching, each action recomputes and re-executes side effects
    6. Counter increments unpredictably

    REAL-WORLD SCENARIO:
    --------------------
    - Writing to a log file inside a UDF → File corrupted with duplicates
    - Incrementing a counter inside a transformation → Count is wrong
    - Inserting into database inside UDF → Duplicate inserts on recomputation
    - Sending emails inside UDF → Users receive multiple copies!

    SYMPTOMS IN PRODUCTION:
    -----------------------
    - Counters show wrong values
    - Files contain duplicate entries
    - Database has redundant records
    - External APIs receive duplicate requests
    - Behavior changes between runs

    SEE ALSO:
    ---------
    - Use spark.sparkContext.accumulator() for counters
    - Use foreachPartition() for write operations

    ============================================================================
    """
    global SIDE_EFFECT_COUNTER
    SIDE_EFFECT_COUNTER = 0

    # ========================================================================
    # STEP 1: Create SparkSession
    # ========================================================================
    spark = (
        SparkSession.builder.appName("Side Effects UB").master("local[*]").getOrCreate()
    )

    print("❌ DANGEROUS: Side effects in transformations")

    # ========================================================================
    # STEP 2: Define UDF with dangerous side effect
    # ========================================================================
    @udf(IntegerType())
    def dangerous_increment(value):
        global SIDE_EFFECT_COUNTER
        # ❌ DANGER: Modifying global state inside UDF!
        # This executes on executors, not the driver
        # Changes are NOT visible to driver
        # May execute 0, 1, or many times depending on caching/recomputation
        SIDE_EFFECT_COUNTER += 1
        return value * 2

    # ========================================================================
    # STEP 3: Create transformation with side effect
    # ========================================================================
    df = spark.range(10)

    # ❌ DANGER: This transformation contains a side effect
    # It won't execute until an action is triggered
    transformed = df.withColumn("doubled", dangerous_increment(col("id")))

    # ========================================================================
    # STEP 4: Observe unpredictable side effect behavior
    # ========================================================================
    # No action yet - counter still 0 because transformation didn't execute
    print(f"   Counter after transformation: {SIDE_EFFECT_COUNTER}")  # Still 0!

    # First action - triggers execution, counter incremented
    transformed.count()
    print(f"   Counter after count(): {SIDE_EFFECT_COUNTER}")

    # Second action - may recompute! Counter incremented AGAIN
    transformed.count()
    print(f"   Counter after second count(): {SIDE_EFFECT_COUNTER}")

    print("   ⚠️ Counter value is UNPREDICTABLE!")
    print("   ⚠️ Without caching, count is likely doubled (2x actual rows)")

    spark.stop()


# ============================================================================
# DANGEROUS PATTERN 4: Accumulator Double-Counting
# ============================================================================


def dangerous_accumulator_double_counting():
    """
    ============================================================================
    ❌ DANGEROUS: Accumulator Double-Counting on DAG Recomputation
    ============================================================================

    WHAT THIS DEMONSTRATES:
    -----------------------
    Shows a critical pitfall with Spark accumulators: when used with uncached
    DataFrames that have multiple actions, accumulators increment MULTIPLE
    times as the DAG is recomputed for each action.

    THE PROBLEM:
    ------------
    Accumulators are designed to aggregate values across executors:
    - Executors add to accumulator during computation
    - Driver reads the aggregated value
    - BUT: If DAG recomputes, accumulator increments AGAIN

    Without caching:
    Action 1: Compute DAG + increment accumulator → accumulator = 100
    Action 2: RECOMPUTE DAG + increment accumulator AGAIN → accumulator = 200!

    WHY IT FAILS:
    -------------
    1. Accumulator increments during DAG execution
    2. First action executes DAG → accumulator = N
    3. No caching, so DataFrame is not materialized
    4. Second action recomputes DAG → accumulator = 2N
    5. Third action recomputes again → accumulator = 3N
    6. Final value = (number_of_actions × N) instead of N

    REAL-WORLD SCENARIO:
    --------------------
    Tracking metrics in production:
    - Count processed records using accumulator
    - Run multiple quality checks (each is an action)
    - Accumulator shows 300 records processed
    - But only 100 records actually exist!
    - Monitoring dashboards show inflated metrics

    SYMPTOMS IN PRODUCTION:
    -----------------------
    - Accumulator values grow unexpectedly
    - Metrics don't match actual data counts
    - Values multiply with each action
    - Monitoring dashboards show wrong statistics

    SEE ALSO:
    ---------
    - safe_accumulator_with_cache() - Fix with caching
    - Spark Accumulators Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators

    ============================================================================
    """
    spark = (
        SparkSession.builder.appName("Accumulator UB").master("local[*]").getOrCreate()
    )

    print("❌ DANGEROUS: Accumulator double-counting")

    counter = spark.sparkContext.accumulator(0)

    def increment_accumulator(iterator):
        for row in iterator:
            counter.add(1)  # ❌ Will be counted multiple times!
            yield row

    df = spark.range(100)
    transformed = df.rdd.mapPartitions(increment_accumulator).toDF()

    # First action
    count1 = transformed.count()
    print(f"   After first count(): accumulator = {counter.value}")

    # Second action - DAG recomputed, accumulator incremented again!
    count2 = transformed.count()
    print(f"   After second count(): accumulator = {counter.value}")

    print(f"   ⚠️ Expected: 100, Got: {counter.value}")
    print("   Accumulator counted each row TWICE!")

    spark.stop()


def safe_accumulator_with_cache():
    """
    ✅ SAFE: Cache to prevent accumulator double-counting.
    """
    spark = (
        SparkSession.builder.appName("Safe Accumulator")
        .master("local[*]")
        .getOrCreate()
    )

    print("✅ SAFE: Accumulator with caching")

    counter = spark.sparkContext.accumulator(0)

    def increment_accumulator(iterator):
        for row in iterator:
            counter.add(1)
            yield row

    df = spark.range(100)
    transformed = df.rdd.mapPartitions(increment_accumulator).toDF()

    # ✅ CACHE to prevent recomputation
    transformed.cache()

    count1 = transformed.count()
    print(f"   After first count(): accumulator = {counter.value}")

    count2 = transformed.count()
    print(f"   After second count(): accumulator = {counter.value}")

    print(f"   ✅ Accumulator value correct: {counter.value}")

    transformed.unpersist()
    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 5: Random Values Changing on Recomputation
# =============================================================================


def dangerous_random_recomputation():
    """
    ⚠️ UNDEFINED BEHAVIOR: Random values change on each recomputation.

    PROBLEM: rand() is reevaluated each time DAG is computed.
    RESULT: Different results for same query, data inconsistency.
    """
    spark = SparkSession.builder.appName("Random UB").master("local[*]").getOrCreate()

    print("❌ DANGEROUS: Random values change on recomputation")

    # Add random column
    df = spark.range(10).withColumn("random", rand())

    # First execution
    print("\n   First execution:")
    df.show(5)

    # Second execution - DIFFERENT random values!
    print("\n   Second execution (DIFFERENT results):")
    df.show(5)

    print("   ⚠️ Random column has DIFFERENT values each time!")

    spark.stop()


def safe_random_with_seed_and_cache():
    """
    ✅ SAFE: Use seed and cache for consistent random values.
    """
    spark = SparkSession.builder.appName("Safe Random").master("local[*]").getOrCreate()

    print("✅ SAFE: Random values with seed and caching")

    # Use seed for reproducibility
    df = spark.range(10).withColumn("random", rand(seed=42))

    # ✅ CACHE to prevent recomputation
    df.cache()
    df.count()  # Materialize cache

    print("\n   First execution:")
    df.show(5)

    print("\n   Second execution (SAME results):")
    df.show(5)

    print("   ✅ Random column has CONSISTENT values!")

    df.unpersist()
    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 6: Time-Dependent Operations
# =============================================================================


def dangerous_time_dependent():
    """
    ⚠️ UNDEFINED BEHAVIOR: Time-dependent operations in transformations.

    PROBLEM: Timestamps change on each recomputation.
    RESULT: Inconsistent data, time travel bugs.
    """
    spark = SparkSession.builder.appName("Time UB").master("local[*]").getOrCreate()

    print("❌ DANGEROUS: Time-dependent transformations")

    @udf(StringType())
    def get_timestamp(value):
        return datetime.now().isoformat()

    df = spark.range(5)
    timestamped = df.withColumn("timestamp", get_timestamp(col("id")))

    print("\n   First execution:")
    timestamped.show(truncate=False)

    time.sleep(1)

    print("\n   Second execution after 1 second (DIFFERENT timestamps):")
    timestamped.show(truncate=False)

    print("   ⚠️ Timestamps are DIFFERENT on each execution!")

    spark.stop()


def safe_time_with_cache():
    """
    ✅ SAFE: Cache to freeze time-dependent values.
    """
    spark = SparkSession.builder.appName("Safe Time").master("local[*]").getOrCreate()

    print("✅ SAFE: Time-dependent with caching")

    @udf(StringType())
    def get_timestamp(value):
        return datetime.now().isoformat()

    df = spark.range(5)
    timestamped = df.withColumn("timestamp", get_timestamp(col("id")))

    # ✅ CACHE to freeze timestamps
    timestamped.cache()
    timestamped.count()  # Materialize

    print("\n   First execution:")
    timestamped.show(truncate=False)

    time.sleep(1)

    print("\n   Second execution after 1 second (SAME timestamps):")
    timestamped.show(truncate=False)

    print("   ✅ Timestamps are CONSISTENT!")

    timestamped.unpersist()
    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 7: Expecting Execution Order
# =============================================================================


def dangerous_execution_order():
    """
    ⚠️ UNDEFINED BEHAVIOR: Assuming transformation execution order.

    PROBLEM: Spark can reorder/optimize transformations.
    RESULT: Unexpected execution order, wrong assumptions.
    """
    spark = SparkSession.builder.appName("Order UB").master("local[*]").getOrCreate()

    print("❌ DANGEROUS: Assuming transformation order matters for side effects")

    counter = spark.sparkContext.accumulator(0)

    @udf(IntegerType())
    def increment_and_return(value):
        counter.add(1)
        print(f"   Processing: {value}")
        return value * 2

    df = spark.range(5)

    # ❌ Assuming sequential execution
    result = (
        df.filter(col("id") > 2)
        .withColumn("doubled", increment_and_return(col("id")))
        .filter(col("doubled") > 5)
    )

    print("   Triggering execution...")
    result.count()

    print(f"   Counter: {counter.value}")
    print("   ⚠️ Execution order may be optimized/reordered by Spark!")

    spark.stop()


# =============================================================================
# DANGEROUS PATTERN 8: Checkpoint vs Persist Confusion
# =============================================================================


def dangerous_checkpoint_confusion():
    """
    ⚠️ UNDEFINED BEHAVIOR: Misunderstanding checkpoint vs persist.

    PROBLEM: Checkpoint truncates lineage, persist doesn't.
    RESULT: Lost lineage can cause issues with fault tolerance.
    """
    spark = (
        SparkSession.builder.appName("Checkpoint UB").master("local[*]").getOrCreate()
    )
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoint")

    print("❌ DANGEROUS: Checkpoint truncates lineage")

    df = spark.range(1000)
    expensive = df.withColumn("squared", col("id") * col("id"))

    # ❌ Checkpoint truncates lineage - can't recover from parent RDD failures
    expensive.checkpoint()
    expensive.count()  # Trigger checkpoint

    print("   Lineage after checkpoint:")
    # 📝 NOTE: toDebugString() may return bytes, str, or None depending on Spark version
    lineage_str = expensive.rdd.toDebugString()
    if lineage_str:
        if isinstance(lineage_str, bytes):
            lineage_str = lineage_str.decode()
        print(f"   RDD: {lineage_str[:200]}...")
    print("   ⚠️ Lineage truncated - parent RDD info lost!")

    spark.stop()


def safe_persist_explanation():
    """
    ✅ SAFE: Understand persist vs checkpoint tradeoffs.
    """
    spark = (
        SparkSession.builder.appName("Safe Persist").master("local[*]").getOrCreate()
    )

    print("✅ SAFE: Persist maintains lineage for fault tolerance")

    df = spark.range(1000)
    expensive = df.withColumn("squared", col("id") * col("id"))

    # ✅ Persist keeps lineage - can recover from failures
    expensive.persist()
    expensive.count()

    print("   Lineage after persist:")
    # 📝 NOTE: Handle both bytes and str return types, and None
    lineage_str = expensive.rdd.toDebugString()
    if lineage_str:
        if isinstance(lineage_str, bytes):
            lineage_str = lineage_str.decode()
        print(f"   RDD: {lineage_str[:200]}...")
    print("   ✅ Full lineage maintained for fault tolerance!")

    expensive.unpersist()
    spark.stop()


# =============================================================================
# MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("PYSPARK UNDEFINED BEHAVIOR: LAZY EVALUATION")
    print("=" * 80)

    print("\n1. ❌ Dangerous: No Caching (Multiple Recomputations)")
    print("-" * 80)
    dangerous_no_caching()

    print("\n2. ✅ Safe: With Caching")
    print("-" * 80)
    safe_with_caching()

    print("\n3. ❌ Dangerous: Transformations Without Actions")
    print("-" * 80)
    dangerous_no_action()

    print("\n4. ✅ Safe: With Actions")
    print("-" * 80)
    safe_with_action()

    print("\n5. ❌ Dangerous: Side Effects in Transformations")
    print("-" * 80)
    dangerous_side_effects()

    print("\n6. ❌ Dangerous: Accumulator Double-Counting")
    print("-" * 80)
    dangerous_accumulator_double_counting()

    print("\n7. ✅ Safe: Accumulator with Caching")
    print("-" * 80)
    safe_accumulator_with_cache()

    print("\n8. ❌ Dangerous: Random Values Changing")
    print("-" * 80)
    dangerous_random_recomputation()

    print("\n9. ✅ Safe: Random with Seed and Cache")
    print("-" * 80)
    safe_random_with_seed_and_cache()

    print("\n10. ❌ Dangerous: Time-Dependent Operations")
    print("-" * 80)
    dangerous_time_dependent()

    print("\n11. ✅ Safe: Time with Caching")
    print("-" * 80)
    safe_time_with_cache()

    print("\n12. ❌ Dangerous: Execution Order Assumptions")
    print("-" * 80)
    dangerous_execution_order()

    print("\n13. ❌ Dangerous: Checkpoint Lineage Truncation")
    print("-" * 80)
    dangerous_checkpoint_confusion()

    print("\n14. ✅ Safe: Persist Maintains Lineage")
    print("-" * 80)
    safe_persist_explanation()

    print("\n" + "=" * 80)
    print("KEY TAKEAWAYS:")
    print("=" * 80)
    print("❌ Never rely on side effects in transformations")
    print("❌ Don't assume transformations execute without actions")
    print("❌ Avoid multiple actions on expensive uncached DataFrames")
    print("❌ Watch out for accumulator double-counting")
    print("❌ Random/time-dependent ops need caching for consistency")
    print("❌ Don't assume transformation execution order")
    print("❌ Understand checkpoint truncates lineage")
    print("✅ Use .cache() for expensive repeated operations")
    print("✅ Always trigger actions to execute work")
    print("✅ Use accumulators only with cached data")
    print("✅ Use rand(seed=X) + cache for reproducible random data")
    print("✅ Cache time-dependent transformations")
    print("✅ Prefer persist() over checkpoint() for fault tolerance")
    print("=" * 80 + "\n")
