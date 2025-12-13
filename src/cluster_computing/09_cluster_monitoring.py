"""
09_cluster_monitoring.py
========================

Master cluster monitoring: Spark UI, metrics, and performance debugging.

Learn how to monitor executor utilization, identify bottlenecks, and
debug performance issues in distributed clusters.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand
import time


def create_spark():
    return SparkSession.builder \
        .appName("ClusterMonitoring") \
        .master("local[4]") \
        .config("spark.ui.port", "4040") \
        .config("spark.eventLog.enabled", "true") \
        .getOrCreate()


def demonstrate_spark_ui_overview():
    """Overview of Spark UI tabs."""
    print("=" * 70)
    print("1. SPARK UI OVERVIEW")
    print("=" * 70)
    
    print("""
🖥️  Spark UI: Your monitoring dashboard

Access:
-------
Local: http://localhost:4040
Driver: http://<driver-ip>:4040
History Server: http://<history-server>:18080

🗂️  Main Tabs:

1. **Jobs Tab**
   - Overview of all Spark jobs
   - Success/failure status
   - Duration of each job
   - Stages per job
   
   Use for: High-level job monitoring

2. **Stages Tab** ⭐ MOST IMPORTANT
   - Detailed stage metrics
   - Task distribution
   - Shuffle read/write
   - Spill to disk
   - Data skew detection
   
   Use for: Performance debugging

3. **Storage Tab**
   - Cached RDDs/DataFrames
   - Memory usage per partition
   - Storage levels (MEMORY_ONLY, DISK, etc.)
   
   Use for: Memory management

4. **Environment Tab**
   - Spark configuration
   - System properties
   - Classpath entries
   
   Use for: Configuration verification

5. **Executors Tab**
   - Executor memory usage
   - Task distribution
   - Shuffle read/write per executor
   - GC time
   
   Use for: Resource utilization

6. **SQL Tab**
   - DataFrame/SQL query plans
   - Physical vs logical plans
   - DAG visualization
   
   Use for: Query optimization

7. **Streaming Tab** (if using Structured Streaming)
   - Batch processing time
   - Input rate
   - Processing rate
   
   Use for: Streaming performance
    """)


def demonstrate_stages_tab_metrics(spark):
    """Key metrics in Stages tab."""
    print("\n" + "=" * 70)
    print("2. STAGES TAB METRICS")
    print("=" * 70)
    
    print("""
📊 Key Metrics to Monitor:

1. **Duration**
   Total time for stage
   Look for: Stages taking much longer than others

2. **Input Size / Records**
   Data read into stage
   Look for: Very large input causing slowdowns

3. **Shuffle Read / Write**
   Data moved between nodes
   Look for: Large shuffle sizes (> 1 GB per task)

4. **Spill (Memory / Disk)**
   Data that didn't fit in memory
   Look for: Any spill indicates memory pressure
   
   ⚠️  Spill to Memory: Moderate issue
   ❌ Spill to Disk: Serious performance problem

5. **Task Time Distribution**
   Min, 25%, Median, 75%, Max task times
   Look for: Wide spread = data skew
   
   Example:
   Min: 1s, Median: 2s, Max: 60s ← SKEW!

6. **GC Time**
   Time spent in garbage collection
   Look for: > 10% of task time = memory issues


🔍 Reading the Stages Table:

Stage ID | Description      | Duration | Input   | Shuffle R | Shuffle W | Spill
---------|------------------|----------|---------|-----------|-----------|-------
0        | map              | 2s       | 1 GB    | 0 B       | 500 MB    | 0 B
1        | reduceByKey      | 10s      | 0 B     | 500 MB    | 100 MB    | 2 GB ⚠️
2        | collect          | 1s       | 100 MB  | 0 B       | 0 B       | 0 B

Analysis:
---------
✅ Stage 0: Clean (no spill)
❌ Stage 1: 2 GB spill to disk! Increase memory or partitions
✅ Stage 2: Clean
    """)
    
    # Generate some data to monitor
    print("\n📊 Generating sample workload to monitor...")
    data = spark.range(1, 5000001).toDF("id") \
        .withColumn("value", rand() * 1000)
    
    result = data.groupBy((col("id") % 100).alias("bucket")).count()
    result.collect()
    
    print("\n✅ Check Spark UI at http://localhost:4040")
    print("   → Go to 'Stages' tab")
    print("   → Click on latest stage")
    print("   → Review metrics: Duration, Shuffle, Spill")


def demonstrate_task_metrics():
    """Understanding task-level metrics."""
    print("\n" + "=" * 70)
    print("3. TASK-LEVEL METRICS")
    print("=" * 70)
    
    print("""
🔬 Task Details (Click on stage → Task table):

Columns to Watch:
-----------------
1. **Task ID**
   Unique identifier for each task

2. **Index**
   Partition number being processed

3. **Status**
   SUCCESS, RUNNING, FAILED
   Look for: Failed tasks

4. **Duration**
   Total time for task
   Look for: Tasks taking 10x longer than median

5. **GC Time**
   Garbage collection time
   Look for: > 10% of duration

6. **Shuffle Read / Write**
   Data read/written per task
   Look for: Imbalanced sizes across tasks

7. **Spill (Memory / Disk)**
   Per-task spill
   Look for: Any spill

8. **Executor ID / Host**
   Which executor ran the task
   Look for: All tasks on same executor = poor distribution


🎯 Identifying Data Skew:

Task Durations:
---------------
Task 0:  2s  |■■
Task 1:  2s  |■■
Task 2:  2s  |■■
Task 3:  45s |■■■■■■■■■■■■■■■■■■■■■  ← SKEWED PARTITION!
Task 4:  2s  |■■
Task 5:  2s  |■■

Diagnosis: Partition 3 has 20x more data
Solution: Use salting or repartition


📊 Shuffle Metrics:

Good Distribution:
-----------------
Task | Shuffle Read
-----|-------------
0    | 100 MB
1    | 105 MB
2    | 98 MB
3    | 102 MB
✅ Balanced!

Bad Distribution (Skew):
------------------------
Task | Shuffle Read
-----|-------------
0    | 50 MB
1    | 60 MB
2    | 2 GB  ← 90% of data in one partition!
3    | 55 MB
❌ Skewed! Use salting technique
    """)


def demonstrate_executor_metrics(spark):
    """Monitor executor utilization."""
    print("\n" + "=" * 70)
    print("4. EXECUTOR METRICS")
    print("=" * 70)
    
    print("""
🖥️  Executors Tab Metrics:

Key Columns:
------------
1. **Executor ID**
   Unique identifier (0 = driver)

2. **Address**
   Host:port where executor runs

3. **Status**
   Active, Dead, Lost
   Look for: Dead executors

4. **RDD Blocks**
   Number of cached RDD partitions

5. **Storage Memory**
   Memory used for cached data
   Look for: Nearing storage memory limit

6. **Disk Used**
   Disk space for spilled data
   Look for: Large disk usage

7. **Cores**
   CPU cores allocated

8. **Active Tasks / Total Tasks**
   Current and historical task counts
   Look for: Imbalanced task distribution

9. **Failed Tasks**
   Number of failed tasks
   Look for: > 0 (indicates issues)

10. **GC Time / Duration**
    GC time as % of execution time
    Look for: > 10%

11. **Shuffle Read / Write**
    Total shuffle data per executor
    Look for: Imbalanced shuffles


📊 Example Executor Dashboard:

Executor | Cores | Memory | Tasks | Failed | GC Time | Shuffle R | Shuffle W
---------|-------|--------|-------|--------|---------|-----------|----------
0        | 4     | 4 GB   | 120   | 0      | 5%      | 2 GB      | 1 GB
1        | 4     | 4 GB   | 115   | 0      | 6%      | 1.9 GB    | 950 MB
2        | 4     | 4 GB   | 10    | 5 ❌   | 45% ❌  | 200 MB    | 100 MB
3        | 4     | 4 GB   | 118   | 0      | 5%      | 2.1 GB    | 1.1 GB

Analysis:
---------
❌ Executor 2 problems:
   - Only 10 tasks (underutilized)
   - 5 failed tasks
   - 45% GC time (memory pressure)
   
Action: Check executor logs, may need restart or more memory
    """)


def demonstrate_sql_query_plan(spark):
    """Understand SQL query plans."""
    print("\n" + "=" * 70)
    print("5. SQL QUERY PLANS")
    print("=" * 70)
    
    print("""
🗺️  Query Plan Visualization:

SQL Tab shows:
--------------
1. **Logical Plan**
   High-level operations (filter, join, aggregation)

2. **Optimized Logical Plan**
   After Catalyst optimizer

3. **Physical Plan**
   Actual execution (Exchange = shuffle)

4. **DAG Visualization**
   Visual graph of stages


🔍 Reading Physical Plan:

Key Operations:
---------------
- **FileScan**: Read from source
- **Filter**: Filter rows (no shuffle)
- **Project**: Select columns (no shuffle)
- **Exchange**: SHUFFLE POINT! ⚠️
- **HashAggregate**: Group by aggregation
- **SortMergeJoin**: Join with sort
- **BroadcastHashJoin**: Broadcast join ✅


Example Plan:
-------------
== Physical Plan ==
*(3) HashAggregate(keys=[category], functions=[count(1)])
+- Exchange hashpartitioning(category, 200) ← SHUFFLE!
   +- *(2) HashAggregate(keys=[category], functions=[partial_count(1)])
      +- *(2) Project [category]
         +- *(2) Filter (value > 100)
            +- *(1) FileScan parquet [value, category]

Reading the plan:
-----------------
1. FileScan: Read parquet
2. Filter: Filter value > 100 (no shuffle)
3. Partial aggregation (map side)
4. Exchange: SHUFFLE on category key
5. Final aggregation (reduce side)


🎯 Optimization Opportunities:

Before:
-------
Exchange hashpartitioning (200 partitions) ← Expensive!

After tuning:
-------------
Exchange hashpartitioning (50 partitions) ← Better!

Or even better:
---------------
BroadcastHashJoin ← No shuffle at all!
    """)
    
    # Demonstrate explain
    print("\n💻 Example: Using .explain()")
    data = spark.range(10000).toDF("id") \
        .withColumn("category", (col("id") % 10).cast("int"))
    
    result = data.filter(col("id") > 5000).groupBy("category").count()
    
    print("\nPhysical Plan:")
    result.explain()


def demonstrate_monitoring_best_practices():
    """Best practices for cluster monitoring."""
    print("\n" + "=" * 70)
    print("6. MONITORING BEST PRACTICES")
    print("=" * 70)
    
    print("""
🎯 Monitoring Checklist:

Daily Checks:
-------------
1. ✅ Job success rate
   Target: > 99%

2. ✅ Average job duration
   Look for: Increasing trends

3. ✅ Executor utilization
   Target: > 80% task distribution

4. ✅ Memory usage
   Target: < 90% to avoid spills

5. ✅ GC time percentage
   Target: < 10% of execution time


Per-Job Analysis:
-----------------
1. ✅ Check Stages tab for slowest stage
2. ✅ Identify shuffle sizes (should be < 1 GB/task)
3. ✅ Look for spill to disk (should be 0)
4. ✅ Check task time distribution for skew
5. ✅ Verify all executors are being used


🚨 Red Flags:

1. ❌ Spill to disk > 0
   → Increase executor memory or partitions

2. ❌ Task time max >> median
   → Data skew, use salting

3. ❌ GC time > 10%
   → Memory pressure, increase executor memory

4. ❌ Shuffle read > 10 GB per partition
   → Increase spark.sql.shuffle.partitions

5. ❌ Failed tasks > 0
   → Check executor logs

6. ❌ One executor with most tasks
   → Poor data distribution

7. ❌ Long shuffle read time
   → Network or disk bottleneck


📊 Performance Metrics to Track:

Metric                    Good      Warning   Critical
------                    ----      -------   --------
GC Time %                 < 5%      5-10%     > 10%
Spill to Memory          0         < 1 GB    > 1 GB
Spill to Disk            0         > 0       > 5 GB
Task time variance       < 2x      2-5x      > 5x
Shuffle per partition    < 1 GB    1-5 GB    > 5 GB
Executor utilization     > 80%     60-80%    < 60%


🔧 Debugging Workflow:

1. Job is slow → Check Stages tab
2. Find slowest stage → Click stage ID
3. Check metrics:
   - Large shuffle? → Use broadcast or pre-partition
   - Spill to disk? → Increase memory or partitions
   - Task skew? → Use salting technique
   - High GC time? → Increase executor memory
4. Apply fix and re-run
5. Compare metrics before/after


💡 Pro Tips:

1. Enable event log for history
   spark.conf.set("spark.eventLog.enabled", "true")
   spark.conf.set("spark.eventLog.dir", "hdfs://logs")

2. Use Spark History Server for past jobs
   spark-history-server.sh start

3. Export metrics to monitoring systems
   - Prometheus + Grafana
   - Datadog
   - CloudWatch (AWS)

4. Set up alerts
   - Job failure alerts
   - Long-running job alerts
   - Resource usage alerts

5. Regular log review
   - Executor logs for errors
   - Driver logs for application issues


📈 Grafana Dashboard Metrics:

Panel 1: Job Success Rate
- Line chart of % successful jobs

Panel 2: Average Job Duration
- Track trends over time

Panel 3: Executor Memory Usage
- Gauge showing % used

Panel 4: Active Executors
- Count of active vs total

Panel 5: Shuffle Read/Write
- Bar chart per job

Panel 6: GC Time %
- Line chart with 10% threshold


🔍 Log Analysis Commands:

# Find OOM errors
grep "OutOfMemoryError" executor-*.log

# Find failed tasks
grep "Task.*FAILED" executor-*.log

# Check GC logs
grep "GC" executor-*.log | tail -100

# Find slow tasks
grep "Task.*took" executor-*.log | sort -k4 -n | tail -10

# Check shuffle errors
grep "shuffle" executor-*.log | grep -i error
    """)


def demonstrate_common_issues():
    """Common issues and solutions."""
    print("\n" + "=" * 70)
    print("7. COMMON ISSUES & SOLUTIONS")
    print("=" * 70)
    
    print("""
🐛 Issue 1: Job Stuck / Very Slow

Symptoms:
---------
- Job runs for hours
- Progress bar stuck at same %
- Few active tasks

Diagnosis:
----------
1. Check Stages tab → Look for long-running stage
2. Check task distribution → All tasks on 1-2 executors?
3. Check shuffle sizes → > 10 GB per partition?

Solutions:
----------
✅ Increase spark.sql.shuffle.partitions
✅ Check for data skew (use salting)
✅ Add more executors


🐛 Issue 2: OutOfMemoryError

Symptoms:
---------
- Job fails with OOM
- Executor lost
- Container killed by YARN

Diagnosis:
----------
1. Check Stages tab → Spill to disk?
2. Check Executors tab → Memory usage near limit?
3. Check task size → Processing huge partition?

Solutions:
----------
✅ Increase executor memory: --executor-memory 16g
✅ Increase memory overhead: --executor-memoryOverhead 4g
✅ Increase partitions to reduce per-task data
✅ Don't cache too much data
✅ Use broadcast for small tables


🐛 Issue 3: Data Skew

Symptoms:
---------
- Most tasks finish quickly
- 1-2 tasks take 10x longer
- Median task time: 2s, Max: 60s

Diagnosis:
----------
1. Check Stages tab → Task time distribution
2. Click stage → Look at task table
3. Identify which partition is large

Solutions:
----------
✅ Use salting technique for skewed keys
✅ Broadcast join if one side is small
✅ Increase partitions


🐛 Issue 4: Shuffle Performance

Symptoms:
---------
- Long shuffle read/write times
- Large shuffle sizes (> 1 GB/task)

Diagnosis:
----------
1. Check Stages tab → Shuffle Read/Write columns
2. Check if shuffle is necessary

Solutions:
----------
✅ Filter before shuffle
✅ Use broadcast join for small tables
✅ Pre-partition on join key
✅ Increase spark.sql.shuffle.partitions
✅ Enable compression (default on)


🐛 Issue 5: Executor Lost

Symptoms:
---------
- Executor becomes unresponsive
- Tasks fail and retry
- "Executor lost" in logs

Diagnosis:
----------
1. Check executor logs
2. Look for OOM, killed by YARN, network timeout

Solutions:
----------
✅ Increase executor memory
✅ Increase network timeout: spark.network.timeout=800s
✅ Check node health (disk, memory, network)
✅ Increase heartbeat: spark.executor.heartbeatInterval=30s


🐛 Issue 6: High GC Time

Symptoms:
---------
- GC time > 10% of task time
- Slow task execution
- Frequent GC pauses

Diagnosis:
----------
1. Check Executors tab → GC Time column
2. Check memory usage near limit

Solutions:
----------
✅ Increase executor memory
✅ Reduce cached data
✅ Use off-heap memory
✅ Tune GC settings (G1GC recommended)
✅ Increase partitions to reduce per-task memory
    """)


def main():
    spark = create_spark()
    
    print("📊 CLUSTER MONITORING")
    print("=" * 70)
    print("\nMaster Spark UI and performance debugging!")
    print()
    
    demonstrate_spark_ui_overview()
    demonstrate_stages_tab_metrics(spark)
    demonstrate_task_metrics()
    demonstrate_executor_metrics(spark)
    demonstrate_sql_query_plan(spark)
    demonstrate_monitoring_best_practices()
    demonstrate_common_issues()
    
    print("\n" + "=" * 70)
    print("✅ CLUSTER MONITORING DEMO COMPLETE!")
    print("=" * 70)
    print("\n📝 Key Takeaways:")
    print("   1. Always check Stages tab first for bottlenecks")
    print("   2. Look for: Spill, Skew, Large shuffles")
    print("   3. Monitor: GC time < 10%, No spill to disk")
    print("   4. Use .explain() to understand query plans")
    print("   5. Enable event logs for history analysis")
    print("   6. Set up alerts for failures and slow jobs")
    print("   7. Regular log review prevents issues")
    print("\n🖥️  Access Spark UI: http://localhost:4040")
    
    spark.stop()


if __name__ == "__main__":
    main()
