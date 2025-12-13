"""
12_standalone_cluster_example.py
=================================

Complete Spark Standalone cluster deployment example.

Spark Standalone is the simplest cluster manager built into Spark.
No external dependencies (unlike YARN/K8s) - just Spark itself.

This example shows:
1. Standalone cluster setup
2. Master and worker configuration
3. Web UI monitoring
4. Application deployment
5. Real ETL pipeline on Standalone
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, expr, broadcast
import time


def create_standalone_spark_session():
    """
    Create SparkSession configured for Standalone cluster.
    
    Standalone Components:
    - Master: Cluster coordinator (spark://host:7077)
    - Workers: Run executors on worker nodes
    - Web UI: Master UI (8080), Worker UI (8081), App UI (4040)
    """
    print("=" * 80)
    print("CREATING STANDALONE SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("StandaloneClusterETL") \
        .master("spark://master-node:7077") \
        .config("spark.submit.deployMode", "cluster") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "8g") \
        .config("spark.cores.max", "40") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.cores", "2") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "80") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.rpc.message.maxSize", "256") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "20s") \
        .config("spark.worker.cleanup.enabled", "true") \
        .config("spark.worker.cleanup.interval", "1800") \
        .config("spark.worker.cleanup.appDataTtl", "86400") \
        .getOrCreate()
    
    print(f"✅ Spark {spark.version} on Standalone cluster")
    print(f"✅ Master: spark://master-node:7077")
    print(f"✅ Total cores: 40")
    print(f"✅ Executor memory: 8GB")
    
    return spark


def print_standalone_configuration(spark):
    """
    Display current Standalone configuration.
    """
    print("\n" + "=" * 80)
    print("STANDALONE CLUSTER CONFIGURATION")
    print("=" * 80)
    
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    
    print("\n⚡ Cluster Information:")
    print(f"   Master URL: {sc.master}")
    print(f"   Application ID: {sc.applicationId}")
    print(f"   Deploy Mode: {conf.get('spark.submit.deployMode', 'client')}")
    
    print("\n💾 Resource Configuration:")
    print(f"   Total Max Cores: {conf.get('spark.cores.max', 'unlimited')}")
    print(f"   Executor Cores: {conf.get('spark.executor.cores', '1')}")
    print(f"   Executor Memory: {conf.get('spark.executor.memory', '1g')}")
    print(f"   Driver Memory: {conf.get('spark.driver.memory', '1g')}")
    print(f"   Driver Cores: {conf.get('spark.driver.cores', '1')}")
    
    print("\n🎯 Performance Settings:")
    print(f"   Shuffle Partitions: {conf.get('spark.sql.shuffle.partitions', '200')}")
    print(f"   Default Parallelism: {conf.get('spark.default.parallelism', 'auto')}")
    print(f"   AQE Enabled: {conf.get('spark.sql.adaptive.enabled', 'false')}")
    print(f"   Serializer: {conf.get('spark.serializer', 'Java serializer')}")


def generate_standalone_setup_scripts():
    """
    Generate scripts to set up Standalone cluster.
    """
    print("\n" + "=" * 80)
    print("STANDALONE CLUSTER SETUP SCRIPTS")
    print("=" * 80)
    
    print("""
📝 1. Master Node Setup (setup-master.sh):
──────────────────────────────────────────────────────────────────
#!/bin/bash
# Run on master node

# Set environment variables
export SPARK_HOME=/opt/spark
export SPARK_MASTER_HOST=master-node
export SPARK_MASTER_PORT=7077
export SPARK_MASTER_WEBUI_PORT=8080
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOG_DIR=/var/spark/logs

# Start master
$SPARK_HOME/sbin/start-master.sh

# Verify master is running
curl http://localhost:8080
──────────────────────────────────────────────────────────────────


📝 2. Worker Node Setup (setup-worker.sh):
──────────────────────────────────────────────────────────────────
#!/bin/bash
# Run on each worker node

# Set environment variables
export SPARK_HOME=/opt/spark
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=32g
export SPARK_WORKER_DIR=/var/spark/work
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_WORKER_WEBUI_PORT=8081

# Start worker (connect to master)
$SPARK_HOME/sbin/start-worker.sh spark://master-node:7077

# Verify worker is running
curl http://localhost:8081
──────────────────────────────────────────────────────────────────


📝 3. Configuration File (spark-env.sh):
──────────────────────────────────────────────────────────────────
#!/usr/bin/env bash
# Place in $SPARK_HOME/conf/spark-env.sh

# Java options
export SPARK_DAEMON_JAVA_OPTS="-Xms2g -Xmx4g"

# Master configuration
export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=4"

# Worker configuration  
export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true"

# Logging
export SPARK_LOG_DIR=/var/spark/logs
export SPARK_PID_DIR=/var/spark/run

# Python
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
──────────────────────────────────────────────────────────────────


📝 4. Worker Configuration (spark-defaults.conf):
──────────────────────────────────────────────────────────────────
# Place in $SPARK_HOME/conf/spark-defaults.conf

spark.master                     spark://master-node:7077
spark.executor.memory            8g
spark.executor.cores             4
spark.cores.max                  40
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.sql.adaptive.enabled       true
spark.sql.shuffle.partitions     200
spark.worker.cleanup.enabled     true
spark.worker.cleanup.interval    1800
──────────────────────────────────────────────────────────────────


📝 5. Cluster Start/Stop Scripts:
──────────────────────────────────────────────────────────────────

# Start entire cluster (run on master)
$SPARK_HOME/sbin/start-all.sh

# Stop entire cluster (run on master)
$SPARK_HOME/sbin/stop-all.sh

# Start master only
$SPARK_HOME/sbin/start-master.sh

# Stop master
$SPARK_HOME/sbin/stop-master.sh

# Start worker on specific node
$SPARK_HOME/sbin/start-worker.sh spark://master-node:7077

# Stop worker
$SPARK_HOME/sbin/stop-worker.sh
──────────────────────────────────────────────────────────────────


📝 6. Cluster Status Check:
──────────────────────────────────────────────────────────────────

# Check master status
curl http://master-node:8080/json/ | jq .

# Check worker status  
curl http://worker-node:8081/json/ | jq .

# List running applications
curl http://master-node:8080/json/applications/

# Check cluster resources
curl http://master-node:8080/json/status/
──────────────────────────────────────────────────────────────────
    """)


def run_standalone_etl_pipeline(spark):
    """
    Run comprehensive ETL pipeline on Standalone cluster.
    """
    print("\n" + "=" * 80)
    print("RUNNING ETL PIPELINE ON STANDALONE CLUSTER")
    print("=" * 80)
    
    start_time = time.time()
    
    # Generate large dataset
    print("\n📊 Generating sample e-commerce data...")
    from pyspark.sql.functions import rand, current_timestamp, date_sub
    
    # Orders data (10M records)
    orders_df = spark.range(0, 10_000_000) \
        .withColumn("order_id", col("id")) \
        .withColumn("customer_id", (rand() * 100000).cast("int")) \
        .withColumn("product_id", (rand() * 1000).cast("int")) \
        .withColumn("quantity", (rand() * 5 + 1).cast("int")) \
        .withColumn("unit_price", (rand() * 200 + 10).cast("double")) \
        .withColumn("discount", (rand() * 0.3).cast("double")) \
        .withColumn("region", expr("CASE " +
                                    "WHEN rand() < 0.4 THEN 'North America' " +
                                    "WHEN rand() < 0.7 THEN 'Europe' " +
                                    "WHEN rand() < 0.9 THEN 'Asia Pacific' " +
                                    "ELSE 'Latin America' END")) \
        .withColumn("order_date", 
                   date_sub(current_timestamp(), (rand() * 730).cast("int")))
    
    print(f"✅ Generated {orders_df.count():,} orders")
    print(f"✅ Partitions: {orders_df.rdd.getNumPartitions()}")
    
    # Calculate revenue
    print("\n💰 Stage 1: Revenue Calculation")
    revenue_df = orders_df.withColumn(
        "gross_amount",
        col("quantity") * col("unit_price")
    ).withColumn(
        "net_amount",
        col("gross_amount") * (1 - col("discount"))
    )
    
    # Regional performance
    print("\n🌍 Stage 2: Regional Performance Analysis")
    regional_perf = revenue_df.groupBy("region").agg(
        count("*").alias("order_count"),
        _sum("net_amount").alias("total_revenue"),
        avg("net_amount").alias("avg_order_value"),
        _sum("quantity").alias("total_units")
    ).orderBy(col("total_revenue").desc())
    
    print("\n📊 Regional Performance:")
    regional_perf.show()
    
    # Product analytics
    print("\n📦 Stage 3: Product Performance")
    product_perf = revenue_df.groupBy("product_id").agg(
        count("*").alias("order_count"),
        _sum("quantity").alias("units_sold"),
        _sum("net_amount").alias("revenue"),
        avg("net_amount").alias("avg_revenue")
    ).filter(col("order_count") > 5000) \
     .orderBy(col("revenue").desc()) \
     .limit(20)
    
    print("\n🏆 Top 20 Products:")
    product_perf.show()
    
    # Customer segmentation
    print("\n👥 Stage 4: Customer Segmentation")
    customer_metrics = revenue_df.groupBy("customer_id").agg(
        count("*").alias("order_count"),
        _sum("net_amount").alias("lifetime_value"),
        avg("net_amount").alias("avg_order_value"),
        _sum("quantity").alias("total_items")
    ).withColumn(
        "segment",
        expr("CASE " +
             "WHEN lifetime_value > 50000 THEN 'Platinum' " +
             "WHEN lifetime_value > 20000 THEN 'Gold' " +
             "WHEN lifetime_value > 5000 THEN 'Silver' " +
             "ELSE 'Bronze' END")
    )
    
    segment_summary = customer_metrics.groupBy("segment").agg(
        count("*").alias("customer_count"),
        _sum("lifetime_value").alias("segment_revenue"),
        avg("lifetime_value").alias("avg_customer_ltv"),
        avg("order_count").alias("avg_orders_per_customer")
    ).orderBy(col("segment_revenue").desc())
    
    print("\n💎 Customer Segments:")
    segment_summary.show()
    
    # Time-based analysis
    print("\n📅 Stage 5: Time-Based Trends")
    from pyspark.sql.functions import year, month, quarter
    
    time_trends = revenue_df.withColumn("year", year("order_date")) \
        .withColumn("quarter", quarter("order_date")) \
        .groupBy("year", "quarter").agg(
            count("*").alias("orders"),
            _sum("net_amount").alias("revenue")
        ).orderBy("year", "quarter")
    
    print("\n📈 Quarterly Trends:")
    time_trends.show(8)
    
    # Cache frequently used data
    customer_metrics.cache()
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 80)
    print("ETL PIPELINE COMPLETE")
    print("=" * 80)
    print(f"⏱️  Total execution time: {elapsed:.2f} seconds")
    print(f"📊 Records processed: {orders_df.count():,}")
    print(f"💾 Cached records: {customer_metrics.count():,}")
    
    return revenue_df, customer_metrics


def demonstrate_standalone_features():
    """
    Demonstrate Standalone-specific features.
    """
    print("\n" + "=" * 80)
    print("STANDALONE CLUSTER FEATURES")
    print("=" * 80)
    
    print("""
⚡ Simplicity:
   - No external dependencies (no Hadoop, no K8s)
   - Just master + workers running Spark
   - Easy to set up for development or small production clusters
   - Built into Spark distribution
   
🎯 Resource Management:
   - spark.cores.max: Total cores across all executors
   - Resources allocated FCFS (First Come First Served)
   - No queue management (unlike YARN)
   - No sophisticated resource scheduling
   
📊 Web UIs:
   - Master UI: http://master-node:8080
     Shows: workers, running apps, completed apps
   - Worker UI: http://worker-node:8081  
     Shows: executor processes, resources used
   - Application UI: http://driver:4040
     Shows: jobs, stages, storage, executors
   
💪 High Availability:
   - Single master = single point of failure
   - HA mode: Multiple masters with ZooKeeper
   - Standby masters take over on failure
   - Worker recovery: restart failed workers
   
🔧 Deploy Modes:
   - Client mode: driver runs on client machine
     Good for: interactive development, debugging
   - Cluster mode: driver runs on worker node
     Good for: production jobs, scheduled tasks
   
⚙️  Executor Allocation:
   - Static allocation only (no dynamic allocation)
   - Executors requested at application start
   - Stay alive until application completes
   - Can't scale up/down during execution
   
🌐 Use Cases:
   - Small to medium clusters (< 100 nodes)
   - Development and testing environments
   - Simple production deployments
   - On-premises clusters without Hadoop
   - Educational environments
   
⚠️  Limitations vs YARN/K8s:
   - No multi-tenancy features
   - Limited resource scheduling
   - No queue management
   - No dynamic allocation support
   - Manual cluster management required
    """)


def standalone_submit_examples():
    """
    Print spark-submit examples for Standalone cluster.
    """
    print("\n" + "=" * 80)
    print("STANDALONE SPARK-SUBMIT EXAMPLES")
    print("=" * 80)
    
    print("""
📝 1. Client Mode (driver on local machine):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master spark://master-node:7077 \\
  --deploy-mode client \\
  --executor-memory 8g \\
  --executor-cores 4 \\
  --total-executor-cores 40 \\
  --conf spark.sql.shuffle.partitions=200 \\
  12_standalone_cluster_example.py

Use case: Interactive development, immediate feedback
──────────────────────────────────────────────────────────────────


📝 2. Cluster Mode (driver on worker):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master spark://master-node:7077 \\
  --deploy-mode cluster \\
  --driver-memory 4g \\
  --driver-cores 2 \\
  --executor-memory 8g \\
  --executor-cores 4 \\
  --total-executor-cores 40 \\
  --supervise \\
  12_standalone_cluster_example.py

--supervise: Automatically restart driver on failure
Use case: Production jobs, scheduled pipelines
──────────────────────────────────────────────────────────────────


📝 3. High Memory Configuration:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master spark://master-node:7077 \\
  --deploy-mode cluster \\
  --executor-memory 32g \\
  --executor-cores 8 \\
  --total-executor-cores 64 \\
  --driver-memory 8g \\
  --conf spark.memory.fraction=0.8 \\
  --conf spark.memory.storageFraction=0.3 \\
  --conf spark.kryoserializer.buffer.max=512m \\
  12_standalone_cluster_example.py

Use case: Large datasets, memory-intensive operations
──────────────────────────────────────────────────────────────────


📝 4. With Dependencies:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master spark://master-node:7077 \\
  --deploy-mode cluster \\
  --py-files libs.zip \\
  --files config.json,data.csv \\
  --jars external-lib.jar \\
  --executor-memory 8g \\
  --total-executor-cores 40 \\
  12_standalone_cluster_example.py

Use case: Applications with external dependencies
──────────────────────────────────────────────────────────────────


📝 5. With Spark Properties File:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master spark://master-node:7077 \\
  --deploy-mode cluster \\
  --properties-file spark-prod.conf \\
  12_standalone_cluster_example.py

# spark-prod.conf:
spark.executor.memory           8g
spark.executor.cores            4
spark.cores.max                 40
spark.sql.adaptive.enabled      true
──────────────────────────────────────────────────────────────────


📝 6. Multiple Master URLs (HA mode):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master spark://master1:7077,master2:7077,master3:7077 \\
  --deploy-mode cluster \\
  --executor-memory 8g \\
  --total-executor-cores 40 \\
  12_standalone_cluster_example.py

Use case: High availability deployments with ZooKeeper
──────────────────────────────────────────────────────────────────
    """)


def standalone_monitoring_commands():
    """
    Print monitoring commands for Standalone cluster.
    """
    print("\n" + "=" * 80)
    print("STANDALONE CLUSTER MONITORING")
    print("=" * 80)
    
    print("""
📊 Web UI Access:
──────────────────────────────────────────────────────────────────

# Master UI (cluster overview)
http://master-node:8080

Shows:
- List of workers and their resources
- Running applications
- Completed applications  
- Cluster resources (cores, memory)


# Worker UI (per worker)
http://worker-node:8081

Shows:
- Running executors on this worker
- Resource utilization
- Logs for executors


# Application UI (per running app)
http://driver-node:4040

Shows:
- Jobs, stages, tasks
- SQL query execution
- Storage/caching
- Environment configuration
──────────────────────────────────────────────────────────────────


📝 REST API Queries:
──────────────────────────────────────────────────────────────────

# Get cluster info
curl http://master-node:8080/json/

# Get applications
curl http://master-node:8080/api/v1/applications

# Get specific application
curl http://master-node:8080/api/v1/applications/[app-id]

# Get workers
curl http://master-node:8080/api/v1/applications/[app-id]/executors

# Kill application
curl -X POST http://master-node:8080/app/kill/?id=[app-id]
──────────────────────────────────────────────────────────────────


🔍 Log Files:
──────────────────────────────────────────────────────────────────

# Master logs
tail -f $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master.Master-*.out

# Worker logs
tail -f $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out

# Application logs (on worker)
tail -f $SPARK_WORKER_DIR/app-[timestamp]/[executor-id]/stdout
──────────────────────────────────────────────────────────────────


🛠️ Cluster Management:
──────────────────────────────────────────────────────────────────

# Check master process
ps aux | grep org.apache.spark.deploy.master.Master

# Check worker processes
ps aux | grep org.apache.spark.deploy.worker.Worker

# Check executor processes
ps aux | grep CoarseGrainedExecutorBackend

# Check cluster resources
$SPARK_HOME/sbin/slaves.sh hostname
$SPARK_HOME/sbin/slaves.sh free -h
$SPARK_HOME/sbin/slaves.sh nproc
──────────────────────────────────────────────────────────────────
    """)


def main():
    """
    Main execution function.
    """
    print("\n" + "⚡ " * 40)
    print("STANDALONE CLUSTER COMPUTING - COMPLETE EXAMPLE")
    print("⚡ " * 40)
    
    # Note: This will fail without a real Standalone cluster
    # For demo purposes, we'll use local mode
    print("\n⚠️  Note: Running in LOCAL mode for demonstration")
    print("   In production, this would connect to Standalone cluster")
    print("   Master URL would be: spark://master-node:7077")
    
    # Create Spark session (local for demo)
    spark = SparkSession.builder \
        .appName("StandaloneDemo") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    # Show what Standalone configuration would look like
    print_standalone_configuration(spark)
    
    # Generate setup scripts
    generate_standalone_setup_scripts()
    
    # Run ETL pipeline
    revenue_df, customer_metrics = run_standalone_etl_pipeline(spark)
    
    # Show Standalone features
    demonstrate_standalone_features()
    
    # Show submit examples
    standalone_submit_examples()
    
    # Show monitoring commands
    standalone_monitoring_commands()
    
    print("\n" + "=" * 80)
    print("✅ STANDALONE EXAMPLE COMPLETE")
    print("=" * 80)
    print("\n📚 Key Takeaways:")
    print("   1. Standalone is simplest cluster manager - no dependencies")
    print("   2. Just master + workers running Spark processes")
    print("   3. Easy setup: start-master.sh + start-worker.sh")
    print("   4. Static resource allocation only (no dynamic)")
    print("   5. Client mode: driver on local machine (development)")
    print("   6. Cluster mode: driver on worker (production)")
    print("   7. HA mode available with ZooKeeper")
    print("   8. Best for: small clusters, development, simple deployments")
    print("   9. Limitations: no multi-tenancy, queue management")
    print("   10. Monitor via Web UI: http://master:8080")
    
    spark.stop()


if __name__ == "__main__":
    main()
