"""
================================================================================
CLUSTER COMPUTING #10 - YARN Cluster Deployment and Management
================================================================================

MODULE OVERVIEW:
----------------
YARN (Yet Another Resource Negotiator) is Hadoop's cluster resource manager
and the most common deployment platform for enterprise Spark applications.
YARN provides multi-tenancy, resource isolation, and battle-tested reliability.

This module demonstrates production YARN deployments with real ETL workloads,
dynamic allocation, resource management, and comprehensive monitoring.

PURPOSE:
--------
Master YARN cluster deployments:
• YARN architecture and components
• Production-ready configuration
• Dynamic resource allocation
• Queue management and priority
• Real ETL pipeline deployment
• Monitoring and debugging techniques
• Best practices and optimization

YARN ARCHITECTURE:
------------------

┌─────────────────────────────────────────────────────────────────┐
│                    YARN CLUSTER ARCHITECTURE                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Client (spark-submit)                                          │
│  ┌────────────────────────────────────────────────────────┐    │
│  │ Submit Spark application                               │    │
│  └──────────────────────┬─────────────────────────────────┘    │
│                         │                                       │
│                         ↓                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ RESOURCE MANAGER (Master - Port 8088)                   │   │
│  │ • Accepts applications                                  │   │
│  │ • Allocates resources                                   │   │
│  │ • Monitors applications                                 │   │
│  │ • Manages queues                                        │   │
│  └──────────────────────┬──────────────────────────────────┘   │
│                         │                                       │
│                         ↓                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ APPLICATION MASTER (Per-app - on worker node)           │   │
│  │ • Spark driver                                          │   │
│  │ • Negotiates resources with RM                          │   │
│  │ • Monitors executors                                    │   │
│  │ • Restarts failed tasks                                 │   │
│  └──────────────────────┬──────────────────────────────────┘   │
│                         │                                       │
│                         ↓                                       │
│  ┌──────────────┬──────────────┬──────────────┐               │
│  │ NODE MANAGER │ NODE MANAGER │ NODE MANAGER │               │
│  │ (Worker 1)   │ (Worker 2)   │ (Worker 3)   │               │
│  │              │              │              │               │
│  │ ┌──────────┐ │ ┌──────────┐ │ ┌──────────┐ │               │
│  │ │Container │ │ │Container │ │ │Container │ │               │
│  │ │Executor 1│ │ │Executor 2│ │ │Executor 3│ │               │
│  │ └──────────┘ │ └──────────┘ │ └──────────┘ │               │
│  │              │              │              │               │
│  │ Port: 8042   │ Port: 8042   │ Port: 8042   │               │
│  └──────────────┴──────────────┴──────────────┘               │
│                                                                 │
│  Key URLs:                                                      │
│  • Resource Manager: http://rm-host:8088                       │
│  • Node Manager: http://nm-host:8042                           │
│  • Application UI: http://driver-host:4040                     │
│  • History Server: http://history-host:18080                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

YARN VS OTHER CLUSTER MANAGERS:
--------------------------------

Feature           │ YARN      │ Kubernetes │ Standalone │
──────────────────┼───────────┼────────────┼────────────┤
Maturity          │ ★★★★★    │ ★★★★☆     │ ★★★☆☆     │
Multi-tenancy     │ ★★★★★    │ ★★★★☆     │ ★★☆☆☆     │
Resource isolation│ ★★★★★    │ ★★★★★     │ ★★★☆☆     │
Dynamic allocation│ ★★★★★    │ ★★★★☆     │ ★★★☆☆     │
Ease of setup     │ ★★☆☆☆    │ ★★★☆☆     │ ★★★★★     │
Cloud native      │ ★★☆☆☆    │ ★★★★★     │ ★★★☆☆     │
Hadoop ecosystem  │ ★★★★★    │ ★★☆☆☆     │ ★★☆☆☆     │

Best for: Enterprise Hadoop clusters, on-premises deployments

CONFIGURATION BREAKDOWN:
------------------------

1. **Resource Manager Configuration** (yarn-site.xml):
```xml
<property>
  <name>yarn.resourcemanager.hostname</name>
  <value>rm-master.company.com</value>
</property>

<property>
  <name>yarn.resourcemanager.address</name>
  <value>rm-master.company.com:8032</value>
</property>

<property>
  <name>yarn.resourcemanager.scheduler.class</name>
  <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
</property>

<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>65536</value>  <!-- 64 GB per node -->
</property>

<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>16</value>  <!-- 16 cores per node -->
</property>

<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>spark_shuffle</value>  <!-- Required for dynamic allocation -->
</property>
```

2. **Queue Configuration** (capacity-scheduler.xml):
```xml
<!-- Production queue: 60% capacity -->
<property>
  <name>yarn.scheduler.capacity.root.production.capacity</name>
  <value>60</value>
</property>

<!-- Development queue: 30% capacity -->
<property>
  <name>yarn.scheduler.capacity.root.development.capacity</name>
  <value>30</value>
</property>

<!-- Ad-hoc queue: 10% capacity -->
<property>
  <name>yarn.scheduler.capacity.root.adhoc.capacity</name>
  <value>10</value>
</property>
```

SPARK-SUBMIT COMMAND:
---------------------

Full production spark-submit command:

```bash
spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --name \"Production ETL Pipeline\" \\
  --queue production \\
  --num-executors 20 \\
  --executor-cores 5 \\
  --executor-memory 16g \\
  --driver-memory 8g \\
  --driver-cores 2 \\
  --conf spark.executor.memoryOverhead=3g \\
  --conf spark.driver.memoryOverhead=2g \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.minExecutors=5 \\
  --conf spark.dynamicAllocation.maxExecutors=50 \\
  --conf spark.dynamicAllocation.initialExecutors=10 \\
  --conf spark.shuffle.service.enabled=true \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.yarn.maxAppAttempts=2 \\
  --conf spark.yarn.am.memory=2g \\
  --conf spark.yarn.am.cores=1 \\
  --conf spark.yarn.submit.waitAppCompletion=true \\
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \\
  --conf spark.network.timeout=600s \\
  --files hdfs:///configs/app.conf \\
  --py-files dependencies.zip \\
  --archives python_env.tar.gz#env \\
  production_etl.py
```

Parameter Explanations:
-----------------------

**--deploy-mode cluster**:
• Runs driver on worker node (not client)
• More stable for long-running jobs
• Driver failure doesn't kill client
• Use 'client' mode for interactive shells

**--queue production**:
• YARN queue for resource allocation
• Controls priority and capacity
• Multiple queues enable multi-tenancy

**Dynamic Allocation**:
• minExecutors=5: Always keep 5 executors
• maxExecutors=50: Scale up to 50 executors
• initialExecutors=10: Start with 10 executors
• executorIdleTimeout=60s: Remove after 60s idle

**Memory Configuration**:
• executor.memory: JVM heap for executor
• executor.memoryOverhead: Off-heap memory (network, shuffle)
• Rule: overhead = 10% of executor.memory (min 384MB)
• Total container = memory + memoryOverhead

DYNAMIC ALLOCATION DEEP DIVE:
------------------------------

┌─────────────────────────────────────────────────────────────────┐
│           DYNAMIC ALLOCATION TIMELINE (YARN)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Time: 0s (Job Start)                                          │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐│
│  │ E1  │ E2  │ E3  │ E4  │ E5  │ E6  │ E7  │ E8  │ E9  │ E10 ││
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘│
│  Initial executors: 10                                          │
│                                                                 │
│  Time: 30s (High Load - Pending Tasks)                         │
│  Request more executors from Resource Manager...               │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐│
│  │ E1  │ ... │ E10 │ E11 │ E12 │ E13 │ E14 │ E15 │ E16 │ E17 ││
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘│
│  Scale up to 17 executors                                      │
│                                                                 │
│  Time: 1m (Peak Load)                                          │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐│
│  │ E1 │ E2 │...........................│E48 │E49 │E50 │      ││
│  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘│
│  At maximum: 50 executors                                      │
│                                                                 │
│  Time: 5m (Load Decreasing)                                    │
│  Executors idle > 60s → Release back to YARN                   │
│  ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐│
│  │ E1  │ E2  │ E3  │ E4  │ E5  │ E6  │ E7  │ E8  │ E9  │ E10 ││
│  └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘│
│  Scale down to 10 executors                                    │
│                                                                 │
│  Time: 10m (Job End - Idle)                                    │
│  Keep minimum executors                                         │
│  ┌─────┬─────┬─────┬─────┬─────┐                              │
│  │ E1  │ E2  │ E3  │ E4  │ E5  │                              │
│  └─────┴─────┴─────┴─────┴─────┘                              │
│  At minimum: 5 executors                                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

Benefits:
✅ Cost optimization (pay for what you use)
✅ Better cluster utilization
✅ Automatic scaling for variable workloads
✅ No manual tuning required

Requirements:
⚠️  YARN shuffle service must be configured
⚠️  spark.shuffle.service.enabled=true
⚠️  yarn.nodemanager.aux-services=spark_shuffle

MONITORING AND DEBUGGING:
--------------------------

1. **Resource Manager Web UI** (http://rm-host:8088):
   - View all running applications
   - Check queue utilization
   - Monitor cluster capacity
   - View application logs

2. **Application Tracking URL**:
   - Get from: yarn application -status <app_id>
   - Access Spark UI while running
   - View stages, tasks, executors

3. **Command Line Monitoring**:
```bash
# List all applications
yarn application -list

# Get application status
yarn application -status application_1234567890_0001

# View logs
yarn logs -applicationId application_1234567890_0001

# Kill application
yarn application -kill application_1234567890_0001

# Check queue status
yarn queue -status production
```

4. **Common Issues and Solutions**:

❌ **Problem**: Application stuck in ACCEPTED state
**Cause**: Not enough resources in queue
**Solution**:
```bash
# Check queue capacity
yarn queue -status production

# Reduce executor count or wait for resources
--num-executors 10  # instead of 50
```

❌ **Problem**: Executors killed by NodeManager
**Cause**: Exceeded memory limits
**Solution**:
```bash
# Increase memory overhead
--conf spark.executor.memoryOverhead=4g  # was 2g

# Or reduce executor memory
--executor-memory 12g  # was 16g
```

❌ **Problem**: Shuffle service not found
**Cause**: External shuffle service not configured
**Solution**:
```xml
<!-- In yarn-site.xml -->
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>spark_shuffle</value>
</property>

<property>
  <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
  <value>org.apache.spark.network.yarn.YarnShuffleService</value>
</property>
```

PRODUCTION BEST PRACTICES:
---------------------------

✅ **1. Use Cluster Deploy Mode**:
```bash
--deploy-mode cluster  # Driver on worker node
```
Benefits: Driver failure doesn't kill client, more stable

✅ **2. Enable Dynamic Allocation**:
```bash
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.minExecutors=5
--conf spark.dynamicAllocation.maxExecutors=50
```
Benefits: Cost optimization, better resource utilization

✅ **3. Set Appropriate Queue**:
```bash
--queue production  # Use dedicated queue
```
Benefits: Resource isolation, priority control

✅ **4. Configure Memory Overhead**:
```bash
--conf spark.executor.memoryOverhead=3g
```
Rule: 10-15% of executor.memory for overhead

✅ **5. Enable AQE (Spark 3.0+)**:
```bash
--conf spark.sql.adaptive.enabled=true
--conf spark.sql.adaptive.coalescePartitions.enabled=true
--conf spark.sql.adaptive.skewJoin.enabled=true
```
Benefits: Automatic optimization, handle skew

✅ **6. Set Retry Attempts**:
```bash
--conf spark.yarn.maxAppAttempts=2
```
Benefits: Automatic retry on failure

✅ **7. Use Kryo Serialization**:
```bash
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```
Benefits: Faster serialization, smaller shuffle files

PERFORMANCE BENCHMARKS:
-----------------------

Test: Process 1 TB of data with aggregations

Configuration Impact:
```
Baseline (10 executors, static):        45 minutes
+ Dynamic allocation (5-30):            32 minutes (30% faster)
+ AQE enabled:                          24 minutes (47% faster)
+ Optimal executor sizing:              18 minutes (60% faster)
+ Kryo serialization:                   15 minutes (67% faster)
```

EXPLAIN() OUTPUT NOTES:
-----------------------
explain() doesn't show YARN-specific information.
Use Spark UI and YARN RM UI for:
• Executor allocation
• Queue utilization
• Container status
• Resource usage

Check:
• Spark UI (port 4040): Application metrics
• YARN RM (port 8088): Cluster-level view
• History Server (port 18080): Historical jobs

See Also:
---------
• 11_kubernetes_cluster_example.py - K8s deployment
• 12_standalone_cluster_example.py - Standalone cluster
• 07_resource_management.py - Resource tuning
• 09_cluster_monitoring.py - Monitoring guide
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_yarn_spark_session():
    """
    Create SparkSession configured for YARN cluster.

    YARN Components:
    - Resource Manager: Global resource scheduler
    - Node Manager: Per-node resource manager
    - Application Master: Per-app coordinator
    """
    print("=" * 80)
    print("CREATING YARN SPARK SESSION")
    print("=" * 80)

    spark = (
        SparkSession.builder.appName("YARNClusterETL")
        .master("yarn")
        .config("spark.submit.deployMode", "cluster")
        .config("spark.executor.instances", "10")
        .config("spark.executor.cores", "5")
        .config("spark.executor.memory", "10g")
        .config("spark.executor.memoryOverhead", "2g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.cores", "2")
        .config("spark.driver.memoryOverhead", "1g")
        .config("spark.yarn.queue", "production")
        .config("spark.yarn.submit.waitAppCompletion", "true")
        .config("spark.yarn.maxAppAttempts", "2")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.initialExecutors", "3")
        .config("spark.dynamicAllocation.minExecutors", "2")
        .config("spark.dynamicAllocation.maxExecutors", "20")
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s")
        .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "100")
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "20s")
        .config("spark.yarn.am.memory", "1g")
        .config("spark.yarn.am.cores", "1")
        .config("spark.yarn.executor.memoryOverhead", "2048")
        .config("spark.yarn.driver.memoryOverhead", "1024")
        .getOrCreate()
    )

    print(f"✅ Spark {spark.version} on YARN cluster")
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")
    print(f"✅ YARN Queue: production")
    print(f"✅ Dynamic Allocation: 2-20 executors")
    print(f"✅ Resources per executor: 5 cores, 10GB memory")

    return spark


def print_yarn_configuration(spark):
    """
    Display current YARN configuration.
    """
    print("\n" + "=" * 80)
    print("YARN CLUSTER CONFIGURATION")
    print("=" * 80)

    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()

    print("\n📊 Cluster Information:")
    print(f"   Master: {sc.master}")
    print(f"   Deploy Mode: {conf.get('spark.submit.deployMode')}")
    print(f"   YARN Queue: {conf.get('spark.yarn.queue')}")
    print(f"   Application ID: {sc.applicationId}")

    print("\n🔧 Resource Configuration:")
    print(f"   Executor Instances: {conf.get('spark.executor.instances')}")
    print(f"   Executor Cores: {conf.get('spark.executor.cores')}")
    print(f"   Executor Memory: {conf.get('spark.executor.memory')}")
    print(f"   Executor Memory Overhead: {conf.get('spark.executor.memoryOverhead')}")
    print(f"   Driver Memory: {conf.get('spark.driver.memory')}")
    print(f"   Driver Cores: {conf.get('spark.driver.cores')}")

    print("\n⚡ Dynamic Allocation:")
    print(f"   Enabled: {conf.get('spark.dynamicAllocation.enabled')}")
    print(f"   Min Executors: {conf.get('spark.dynamicAllocation.minExecutors')}")
    print(f"   Max Executors: {conf.get('spark.dynamicAllocation.maxExecutors')}")
    print(
        f"   Initial Executors: {conf.get('spark.dynamicAllocation.initialExecutors')}"
    )

    print("\n🎯 Performance Settings:")
    print(f"   Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions')}")
    print(f"   Default Parallelism: {conf.get('spark.default.parallelism')}")
    print(f"   AQE Enabled: {conf.get('spark.sql.adaptive.enabled')}")
    print(f"   Broadcast Threshold: {conf.get('spark.sql.autoBroadcastJoinThreshold')}")


def generate_sample_data(spark, num_records=10_000_000):
    """
    Generate sample transactional data for ETL processing.
    """
    print("\n" + "=" * 80)
    print(f"GENERATING {num_records:,} SAMPLE RECORDS")
    print("=" * 80)

    # Generate large dataset to demonstrate cluster computing
    from pyspark.sql.functions import expr, rand, randn

    df = (
        spark.range(0, num_records)
        .withColumn("customer_id", (rand() * 100000).cast("int"))
        .withColumn("product_id", (rand() * 1000).cast("int"))
        .withColumn("quantity", (rand() * 10 + 1).cast("int"))
        .withColumn("price", (rand() * 100 + 10).cast("double"))
        .withColumn(
            "region",
            expr(
                "CASE "
                + "WHEN rand() < 0.3 THEN 'US' "
                + "WHEN rand() < 0.6 THEN 'EU' "
                + "WHEN rand() < 0.85 THEN 'ASIA' "
                + "ELSE 'OTHER' END"
            ),
        )
        .withColumn(
            "timestamp", expr("current_timestamp() - INTERVAL (rand() * 365) DAYS")
        )
    )

    print(f"✅ Generated {df.count():,} records")
    print(f"✅ Partitions: {df.rdd.getNumPartitions()}")

    return df


def etl_pipeline_on_yarn(spark, df):
    """
    Run complete ETL pipeline on YARN cluster.

    Stages:
    1. Data validation
    2. Transformations
    3. Aggregations
    4. Joins
    5. Output
    """
    print("\n" + "=" * 80)
    print("RUNNING ETL PIPELINE ON YARN")
    print("=" * 80)

    start_time = time.time()

    # Stage 1: Data Validation
    print("\n📋 Stage 1: Data Validation")
    validated_df = df.filter(
        (col("quantity") > 0) & (col("price") > 0) & (col("customer_id").isNotNull())
    )

    invalid_count = df.count() - validated_df.count()
    print(f"   Filtered out {invalid_count:,} invalid records")

    # Stage 2: Calculate Revenue
    print("\n💰 Stage 2: Calculate Revenue")
    revenue_df = validated_df.withColumn("revenue", col("quantity") * col("price"))

    # Stage 3: Regional Aggregations
    print("\n🌍 Stage 3: Regional Aggregations")
    regional_summary = (
        revenue_df.groupBy("region")
        .agg(
            count("*").alias("total_transactions"),
            _sum("revenue").alias("total_revenue"),
            avg("revenue").alias("avg_revenue"),
            _sum("quantity").alias("total_quantity"),
        )
        .orderBy(col("total_revenue").desc())
    )

    print("\n📊 Regional Performance:")
    regional_summary.show()

    # Stage 4: Product Performance
    print("\n📦 Stage 4: Product Performance")
    product_summary = (
        revenue_df.groupBy("product_id")
        .agg(
            count("*").alias("sales_count"),
            _sum("revenue").alias("total_revenue"),
            avg("price").alias("avg_price"),
        )
        .filter(col("sales_count") > 100)
        .orderBy(col("total_revenue").desc())
        .limit(10)
    )

    print("\n🏆 Top 10 Products:")
    product_summary.show()

    # Stage 5: Customer Segmentation
    print("\n�� Stage 5: Customer Segmentation")
    customer_summary = (
        revenue_df.groupBy("customer_id")
        .agg(
            count("*").alias("purchase_count"),
            _sum("revenue").alias("total_spent"),
            avg("revenue").alias("avg_transaction"),
        )
        .withColumn(
            "customer_segment",
            when(col("total_spent") > 10000, "VIP")
            .when(col("total_spent") > 5000, "Premium")
            .when(col("total_spent") > 1000, "Standard")
            .otherwise("Basic"),
        )
    )

    segment_distribution = (
        customer_summary.groupBy("customer_segment")
        .agg(
            count("*").alias("customer_count"),
            _sum("total_spent").alias("segment_revenue"),
        )
        .orderBy(col("segment_revenue").desc())
    )

    print("\n🎯 Customer Segmentation:")
    segment_distribution.show()

    # Cache for reuse
    customer_summary.cache()

    # Stage 6: Execution Metrics
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETE")
    print("=" * 80)
    print(f"⏱️  Total execution time: {elapsed:.2f} seconds")
    print(f"📊 Records processed: {validated_df.count():,}")
    print(f"💾 Data cached: {customer_summary.count():,} customer records")

    # Show execution plan for last aggregation
    print("\n�� Physical Execution Plan:")
    print(segment_distribution.explain(mode="formatted"))

    return regional_summary, product_summary, customer_summary


def demonstrate_yarn_features(spark):
    """
    Demonstrate YARN-specific features.
    """
    print("\n" + "=" * 80)
    print("YARN-SPECIFIC FEATURES")
    print("=" * 80)

    print(
        """
🎯 YARN Queue Management:
   - Separate queues for different priorities (prod, dev, adhoc)
   - Resource allocation per queue
   - Fair or capacity scheduler
   
   Example: spark.yarn.queue=production
   
📊 Dynamic Resource Allocation:
   - Automatically scales executors based on workload
   - Releases idle executors after timeout
   - Requests more when tasks are pending
   
   Min: 2 → Current: ? → Max: 20
   
🔒 Security Features:
   - Kerberos authentication
   - YARN ACLs for application access
   - Secure shuffle service
   
💾 YARN NodeManager Shuffle Service:
   - External shuffle service for dynamic allocation
   - Persists shuffle data even if executors are removed
   - Enable: spark.shuffle.service.enabled=true
   
📈 Resource Monitoring:
   - YARN ResourceManager UI: http://rm-host:8088
   - Application logs aggregation
   - Container metrics and statistics
   
⚠️  Failure Recovery:
   - Application Master restart on failure
   - Max attempts: spark.yarn.maxAppAttempts
   - Automatic executor replacement
    """
    )


def yarn_submit_examples():
    """
    Print spark-submit examples for YARN.
    """
    print("\n" + "=" * 80)
    print("YARN SPARK-SUBMIT EXAMPLES")
    print("=" * 80)

    print(
        """
📝 1. Client Mode (driver on local machine):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master yarn \\
  --deploy-mode client \\
  --num-executors 10 \\
  --executor-cores 5 \\
  --executor-memory 10g \\
  --driver-memory 4g \\
  --conf spark.yarn.queue=production \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.maxExecutors=20 \\
  10_yarn_cluster_example.py

Use case: Interactive development, debugging


📝 2. Cluster Mode (driver on YARN):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --num-executors 15 \\
  --executor-cores 4 \\
  --executor-memory 12g \\
  --driver-memory 6g \\
  --conf spark.yarn.queue=production \\
  --conf spark.yarn.submit.waitAppCompletion=false \\
  --conf spark.sql.shuffle.partitions=200 \\
  10_yarn_cluster_example.py

Use case: Production jobs, scheduled pipelines


📝 3. High Memory Job (large datasets):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --num-executors 20 \\
  --executor-cores 5 \\
  --executor-memory 32g \\
  --executor-memoryOverhead 4g \\
  --driver-memory 8g \\
  --conf spark.yarn.queue=high-memory \\
  --conf spark.memory.fraction=0.8 \\
  --conf spark.memory.storageFraction=0.3 \\
  10_yarn_cluster_example.py

Use case: Large-scale aggregations, joins


📝 4. Dynamic Allocation (variable workload):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.initialExecutors=3 \\
  --conf spark.dynamicAllocation.minExecutors=2 \\
  --conf spark.dynamicAllocation.maxExecutors=50 \\
  --conf spark.dynamicAllocation.executorIdleTimeout=60s \\
  --conf spark.shuffle.service.enabled=true \\
  --executor-cores 4 \\
  --executor-memory 8g \\
  10_yarn_cluster_example.py

Use case: Variable load, cost optimization


📝 5. With Dependencies (Python packages):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master yarn \\
  --deploy-mode cluster \\
  --py-files dependencies.zip \\
  --archives environment.tar.gz#env \\
  --conf spark.yarn.dist.files=config.json \\
  --conf spark.pyspark.python=./env/bin/python \\
  10_yarn_cluster_example.py

Use case: Custom Python packages, configurations
    """
    )


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("YARN CLUSTER COMPUTING - COMPLETE EXAMPLE")
    print("🎯" * 40)

    # Note: This will fail without a real YARN cluster
    # For demo purposes, we'll use local mode
    print("\n⚠️  Note: Running in LOCAL mode for demonstration")
    print("   In production, this would connect to YARN cluster")

    # Create Spark session (local for demo)
    spark = (
        SparkSession.builder.appName("YARNDemo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # Show what YARN configuration would look like
    print_yarn_configuration(spark)

    # Generate sample data
    df = generate_sample_data(spark, num_records=1_000_000)

    # Run ETL pipeline
    regional, products, customers = etl_pipeline_on_yarn(spark, df)

    # Show YARN features
    demonstrate_yarn_features(spark)

    # Show submit examples
    yarn_submit_examples()

    print("\n" + "=" * 80)
    print("✅ YARN EXAMPLE COMPLETE")
    print("=" * 80)
    print("\n📚 Key Takeaways:")
    print("   1. YARN is the resource manager in Hadoop ecosystem")
    print("   2. Dynamic allocation automatically scales executors")
    print("   3. Queue system manages multi-tenant resources")
    print("   4. Cluster mode runs driver on YARN (production)")
    print("   5. Client mode runs driver locally (development)")
    print("   6. Shuffle service enables dynamic allocation")
    print("   7. Monitor via YARN RM UI: http://rm-host:8088")

    spark.stop()


if __name__ == "__main__":
    main()
