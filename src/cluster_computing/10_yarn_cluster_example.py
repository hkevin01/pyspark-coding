"""
10_yarn_cluster_example.py
===========================

Complete YARN cluster deployment example with real ETL workload.

YARN (Yet Another Resource Negotiator) is the resource manager in Hadoop ecosystem.
Most enterprise big data clusters use YARN.

This example shows:
1. YARN cluster configuration
2. Real ETL pipeline on YARN
3. Dynamic allocation
4. Resource management
5. Monitoring and debugging
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import time


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
    
    spark = SparkSession.builder \
        .appName("YARNClusterETL") \
        .master("yarn") \
        .config("spark.submit.deployMode", "cluster") \
        .config("spark.executor.instances", "10") \
        .config("spark.executor.cores", "5") \
        .config("spark.executor.memory", "10g") \
        .config("spark.executor.memoryOverhead", "2g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memoryOverhead", "1g") \
        .config("spark.yarn.queue", "production") \
        .config("spark.yarn.submit.waitAppCompletion", "true") \
        .config("spark.yarn.maxAppAttempts", "2") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.initialExecutors", "3") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "20") \
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
        .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "300s") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.default.parallelism", "100") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "512m") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "20s") \
        .config("spark.yarn.am.memory", "1g") \
        .config("spark.yarn.am.cores", "1") \
        .config("spark.yarn.executor.memoryOverhead", "2048") \
        .config("spark.yarn.driver.memoryOverhead", "1024") \
        .getOrCreate()
    
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
    print(f"   Initial Executors: {conf.get('spark.dynamicAllocation.initialExecutors')}")
    
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
    from pyspark.sql.functions import rand, randn, expr
    
    df = spark.range(0, num_records) \
        .withColumn("customer_id", (rand() * 100000).cast("int")) \
        .withColumn("product_id", (rand() * 1000).cast("int")) \
        .withColumn("quantity", (rand() * 10 + 1).cast("int")) \
        .withColumn("price", (rand() * 100 + 10).cast("double")) \
        .withColumn("region", expr("CASE " +
                                    "WHEN rand() < 0.3 THEN 'US' " +
                                    "WHEN rand() < 0.6 THEN 'EU' " +
                                    "WHEN rand() < 0.85 THEN 'ASIA' " +
                                    "ELSE 'OTHER' END")) \
        .withColumn("timestamp", expr("current_timestamp() - INTERVAL (rand() * 365) DAYS"))
    
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
        (col("quantity") > 0) &
        (col("price") > 0) &
        (col("customer_id").isNotNull())
    )
    
    invalid_count = df.count() - validated_df.count()
    print(f"   Filtered out {invalid_count:,} invalid records")
    
    # Stage 2: Calculate Revenue
    print("\n💰 Stage 2: Calculate Revenue")
    revenue_df = validated_df.withColumn(
        "revenue",
        col("quantity") * col("price")
    )
    
    # Stage 3: Regional Aggregations
    print("\n🌍 Stage 3: Regional Aggregations")
    regional_summary = revenue_df.groupBy("region").agg(
        count("*").alias("total_transactions"),
        _sum("revenue").alias("total_revenue"),
        avg("revenue").alias("avg_revenue"),
        _sum("quantity").alias("total_quantity")
    ).orderBy(col("total_revenue").desc())
    
    print("\n📊 Regional Performance:")
    regional_summary.show()
    
    # Stage 4: Product Performance
    print("\n📦 Stage 4: Product Performance")
    product_summary = revenue_df.groupBy("product_id").agg(
        count("*").alias("sales_count"),
        _sum("revenue").alias("total_revenue"),
        avg("price").alias("avg_price")
    ).filter(col("sales_count") > 100) \
     .orderBy(col("total_revenue").desc()) \
     .limit(10)
    
    print("\n🏆 Top 10 Products:")
    product_summary.show()
    
    # Stage 5: Customer Segmentation
    print("\n�� Stage 5: Customer Segmentation")
    customer_summary = revenue_df.groupBy("customer_id").agg(
        count("*").alias("purchase_count"),
        _sum("revenue").alias("total_spent"),
        avg("revenue").alias("avg_transaction")
    ).withColumn(
        "customer_segment",
        when(col("total_spent") > 10000, "VIP")
        .when(col("total_spent") > 5000, "Premium")
        .when(col("total_spent") > 1000, "Standard")
        .otherwise("Basic")
    )
    
    segment_distribution = customer_summary.groupBy("customer_segment").agg(
        count("*").alias("customer_count"),
        _sum("total_spent").alias("segment_revenue")
    ).orderBy(col("segment_revenue").desc())
    
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
    
    print("""
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
    """)


def yarn_submit_examples():
    """
    Print spark-submit examples for YARN.
    """
    print("\n" + "=" * 80)
    print("YARN SPARK-SUBMIT EXAMPLES")
    print("=" * 80)
    
    print("""
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
    """)


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
    spark = SparkSession.builder \
        .appName("YARNDemo") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
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
