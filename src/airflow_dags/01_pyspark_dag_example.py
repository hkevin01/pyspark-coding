"""
================================================================================
Apache Airflow DAG Examples for PySpark
================================================================================

PURPOSE:
--------
Orchestrate PySpark data pipelines using Apache Airflow for scheduling,
monitoring, and dependency management of complex workflows.

WHAT AIRFLOW PROVIDES:
-----------------------
- Schedule PySpark jobs (hourly, daily, custom)
- Manage dependencies (job A → job B → job C)
- Retry failed tasks automatically
- Monitor pipeline health (dashboards, alerts)
- Backfill historical data

WHY AIRFLOW:
------------
- Replace cron jobs with visual DAGs
- Handle failures gracefully (retries, alerts)
- Scale to 1000s of workflows
- Integrate with AWS, GCP, Azure
- Track SLAs and data lineage

REAL-WORLD USE CASES:
- Airbnb: 10,000+ DAGs in production
- Lyft: Real-time data pipelines
- Twitter: ETL orchestration
- Robinhood: Financial data processing

DAG COMPONENTS:
---------------
1. TASKS: Individual units of work (PySpark job, SQL, etc.)
2. DEPENDENCIES: Task A must complete before Task B
3. SCHEDULE: Cron expression or timedelta
4. OPERATORS: SparkSubmitOperator, PythonOperator, etc.

EXAMPLE DAG:
------------
extract_data >> transform_data >> load_data
      ↓                               ↓
  validate_data                  send_notification

================================================================================
"""

from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG example (requires Airflow installation)
example_dag_config = {
    "dag_id": "pyspark_etl_pipeline",
    "description": "Daily ETL pipeline with PySpark",
    "schedule_interval": "0 2 * * *",  # 2 AM daily
    "start_date": datetime(2023, 1, 1),
    "catchup": False,
    "default_args": {
        "owner": "data-team",
        "depends_on_past": False,
        "email": ["alerts@company.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
}


def print_dag_example():
    """Print example DAG structure."""
    print("\n" + "=" * 80)
    print("AIRFLOW DAG EXAMPLE")
    print("=" * 80)

    print("\n📅 DAG Schedule: Daily at 2 AM")
    print("\n📊 Task Flow:")
    print(
        """
    ┌─────────────────┐
    │  extract_data   │ ← Read from source database
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │ transform_data  │ ← PySpark transformations
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │   load_data     │ ← Write to data warehouse
    └────────┬────────┘
             │
             ↓
    ┌─────────────────┐
    │  send_success   │ ← Email notification
    │  notification   │
    └─────────────────┘
    """
    )

    print("\n⚙️  Configuration:")
    for key, value in example_dag_config.items():
        if key != "default_args":
            print(f"   {key}: {value}")

    print("\n🔄 Retry Policy:")
    print("   • Retries: 3 attempts")
    print("   • Delay: 5 minutes between retries")
    print("   • Alerts: Email on failure")

    print("\n💡 Airflow Benefits:")
    print("   • Visual DAG in UI")
    print("   • Automatic retries")
    print("   • Historical run tracking")
    print("   • SLA monitoring")
    print("   • Easy backfilling")


# ================================================================================
# REAL DAG IMPLEMENTATION (commented to avoid Airflow dependency)
# ================================================================================
"""
# Uncomment when Airflow is installed

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago

# Define DAG
dag = DAG(
    dag_id='pyspark_etl_pipeline',
    default_args={
        'owner': 'data-engineering',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': ['data-team@company.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    description='Daily ETL pipeline using PySpark',
    schedule_interval='0 2 * * *',  # 2 AM daily
    catchup=False,
    tags=['pyspark', 'etl', 'production'],
)

# Task 1: Extract data from source
extract_task = SparkSubmitOperator(
    task_id='extract_data',
    application='/opt/spark/jobs/extract_data.py',
    name='extract_daily_data',
    conn_id='spark_default',
    conf={
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.num.executors': '4',
    },
    application_args=[
        '--source', 'jdbc:postgresql://prod-db:5432/source',
        '--table', 'transactions',
        '--output', 's3://data-lake/raw/transactions/{{ ds }}',
    ],
    dag=dag,
)

# Task 2: Transform data
transform_task = SparkSubmitOperator(
    task_id='transform_data',
    application='/opt/spark/jobs/transform_data.py',
    name='transform_daily_data',
    conn_id='spark_default',
    conf={
        'spark.executor.memory': '8g',
        'spark.executor.cores': '4',
        'spark.num.executors': '8',
        'spark.sql.adaptive.enabled': 'true',
    },
    application_args=[
        '--input', 's3://data-lake/raw/transactions/{{ ds }}',
        '--output', 's3://data-lake/processed/transactions/{{ ds }}',
        '--date', '{{ ds }}',
    ],
    dag=dag,
)

# Task 3: Load to data warehouse
load_task = SparkSubmitOperator(
    task_id='load_data',
    application='/opt/spark/jobs/load_data.py',
    name='load_to_warehouse',
    conn_id='spark_default',
    conf={
        'spark.executor.memory': '4g',
        'spark.executor.cores': '2',
        'spark.num.executors': '4',
    },
    application_args=[
        '--input', 's3://data-lake/processed/transactions/{{ ds }}',
        '--warehouse', 'jdbc:postgresql://warehouse:5432/analytics',
        '--table', 'fact_transactions',
        '--mode', 'append',
    ],
    dag=dag,
)

# Task 4: Data quality validation
def validate_data(**context):
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("DataValidation").getOrCreate()
    
    # Read processed data
    df = spark.read.parquet(f"s3://data-lake/processed/transactions/{context['ds']}")
    
    # Validation checks
    record_count = df.count()
    null_count = df.filter(df['amount'].isNull()).count()
    negative_count = df.filter(df['amount'] < 0).count()
    
    # Assert data quality
    assert record_count > 0, "No records found!"
    assert null_count == 0, f"Found {null_count} null amounts"
    assert negative_count == 0, f"Found {negative_count} negative amounts"
    
    print(f"✅ Validation passed: {record_count} records, {null_count} nulls, {negative_count} negatives")
    
    return {'record_count': record_count}

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    provide_context=True,
    dag=dag,
)

# Task 5: Send success notification
success_email = EmailOperator(
    task_id='send_success_notification',
    to=['data-team@company.com'],
    subject='ETL Pipeline Success - {{ ds }}',
    html_content='''
    <h3>ETL Pipeline Completed Successfully</h3>
    <p><strong>Date:</strong> {{ ds }}</p>
    <p><strong>Records Processed:</strong> {{ task_instance.xcom_pull(task_ids='validate_data')['record_count'] }}</p>
    <p><strong>Duration:</strong> {{ task_instance.duration }} seconds</p>
    ''',
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> validate_task >> success_email
"""


# ================================================================================
# STANDALONE PYSPARK JOBS (for Airflow to submit)
# ================================================================================


def example_extract_job():
    """
    Example: Extract job that Airflow would submit.

    WHAT: Read from source database
    WHY: Ingest daily data for processing
    HOW: JDBC read with partitioning
    """
    print("\n" + "=" * 80)
    print("EXAMPLE EXTRACT JOB (Airflow Task 1)")
    print("=" * 80)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("Extract_Daily_Data")
        .config("spark.jars", "postgresql-42.2.18.jar")
        .getOrCreate()
    )

    # Simulate reading from database
    # In production: JDBC read with partitioning
    print("\n📥 Extracting data from source database...")

    # Example: JDBC read (commented as it needs real database)
    """
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://prod-db:5432/source") \
        .option("dbtable", "transactions") \
        .option("user", "readonly_user") \
        .option("password", "secret") \
        .option("numPartitions", "10") \
        .option("partitionColumn", "id") \
        .option("lowerBound", "0") \
        .option("upperBound", "1000000") \
        .load()
    """

    # Simulate with sample data
    df = spark.createDataFrame(
        [
            (1, "2024-01-15", 1001, 150.50, "completed"),
            (2, "2024-01-15", 1002, 275.00, "completed"),
            (3, "2024-01-15", 1003, 89.99, "pending"),
            (4, "2024-01-15", 1001, 320.00, "completed"),
            (5, "2024-01-15", 1004, 45.50, "failed"),
        ],
        ["transaction_id", "date", "customer_id", "amount", "status"],
    )

    print(f"   ✅ Extracted {df.count()} records")
    df.show()

    # Write to data lake
    output_path = "/tmp/data-lake/raw/transactions/2024-01-15"
    print(f"\n💾 Writing to data lake: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print("   ✅ Write complete")

    spark.stop()
    return output_path


def example_transform_job(input_path):
    """
    Example: Transform job that Airflow would submit.

    WHAT: Apply business logic and transformations
    WHY: Prepare data for analytics
    HOW: PySpark DataFrame operations
    """
    print("\n" + "=" * 80)
    print("EXAMPLE TRANSFORM JOB (Airflow Task 2)")
    print("=" * 80)

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, col, count
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.functions import when

    spark = (
        SparkSession.builder.appName("Transform_Daily_Data")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    print(f"\n📂 Reading from: {input_path}")
    df = spark.read.parquet(input_path)

    # TRANSFORMATION 1: Filter completed transactions only
    print("\n🔄 Applying transformations...")
    print("   1. Filtering completed transactions...")
    df_completed = df.filter(col("status") == "completed")
    print(f"      ✅ {df_completed.count()} completed transactions")

    # TRANSFORMATION 2: Add derived columns
    print("   2. Adding derived columns...")
    df_enriched = (
        df_completed.withColumn(
            "amount_category",
            when(col("amount") < 100, "small")
            .when(col("amount") < 300, "medium")
            .otherwise("large"),
        )
        .withColumn("processing_fee", col("amount") * 0.029)  # 2.9% fee
        .withColumn("net_amount", col("amount") - col("processing_fee"))
    )

    print("   3. Sample transformed data:")
    df_enriched.select(
        "transaction_id", "amount", "amount_category", "net_amount"
    ).show()

    # TRANSFORMATION 3: Aggregate by customer
    print("   4. Aggregating by customer...")
    df_customer_summary = df_enriched.groupBy("customer_id").agg(
        count("transaction_id").alias("transaction_count"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        spark_sum("processing_fee").alias("total_fees"),
    )

    print("\n📊 Customer summary:")
    df_customer_summary.show()

    # Write transformed data
    output_path = "/tmp/data-lake/processed/transactions/2024-01-15"
    print(f"\n💾 Writing transformed data to: {output_path}")
    df_enriched.write.mode("overwrite").partitionBy("date").parquet(output_path)
    print("   ✅ Transform complete")

    spark.stop()
    return output_path


def example_load_job(input_path):
    """
    Example: Load job that Airflow would submit.

    WHAT: Load to data warehouse
    WHY: Make data available for analytics
    HOW: JDBC write with batching
    """
    print("\n" + "=" * 80)
    print("EXAMPLE LOAD JOB (Airflow Task 3)")
    print("=" * 80)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("Load_To_Warehouse")
        .config("spark.jars", "postgresql-42.2.18.jar")
        .getOrCreate()
    )

    print(f"\n📂 Reading processed data from: {input_path}")
    df = spark.read.parquet(input_path)

    print(f"   ✅ Loaded {df.count()} records")

    # Write to data warehouse (simulated)
    print("\n💾 Loading to data warehouse...")

    # Example: JDBC write (commented as it needs real warehouse)
    """
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://warehouse:5432/analytics") \
        .option("dbtable", "fact_transactions") \
        .option("user", "etl_user") \
        .option("password", "secret") \
        .option("batchsize", "10000") \
        .mode("append") \
        .save()
    """

    # Simulate write
    print("   ✅ Loaded to fact_transactions table")
    print(f"   Records loaded: {df.count()}")

    # Data quality checks
    print("\n🔍 Data quality checks:")
    null_count = df.filter(df["amount"].isNull()).count()
    negative_count = df.filter(df["amount"] < 0).count()

    print(f"   Null amounts: {null_count}")
    print(f"   Negative amounts: {negative_count}")

    if null_count == 0 and negative_count == 0:
        print("   ✅ All quality checks passed!")
    else:
        print("   ⚠️  Quality issues detected!")

    spark.stop()


def example_complete_pipeline():
    """
    Example: Run complete pipeline (simulates Airflow orchestration).

    WHAT: Execute all ETL steps in sequence
    WHY: Show complete workflow
    HOW: Call each job function in dependency order
    """
    print("\n" + "🌊" * 40)
    print("COMPLETE ETL PIPELINE EXECUTION")
    print("🌊" * 40)

    import time

    start_time = time.time()

    try:
        # Task 1: Extract
        print("\n" + "=" * 80)
        print("TASK 1: EXTRACT DATA")
        print("=" * 80)
        raw_path = example_extract_job()

        # Task 2: Transform
        print("\n" + "=" * 80)
        print("TASK 2: TRANSFORM DATA")
        print("=" * 80)
        processed_path = example_transform_job(raw_path)

        # Task 3: Load
        print("\n" + "=" * 80)
        print("TASK 3: LOAD TO WAREHOUSE")
        print("=" * 80)
        example_load_job(processed_path)

        duration = time.time() - start_time

        print("\n" + "=" * 80)
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print("=" * 80)
        print(f"\n📊 Pipeline Statistics:")
        print(f"   Total duration: {duration:.2f} seconds")
        print(f"   Tasks completed: 3/3")
        print(f"   Success rate: 100%")

        print("\n💡 In Production with Airflow:")
        print("   • Each task runs as separate Spark job")
        print("   • Automatic retries on failure")
        print("   • Email alerts on completion/failure")
        print("   • Visual DAG in Airflow UI")
        print("   • Historical run tracking")
        print("   • Easy backfilling for date ranges")

    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        raise


def main():
    """Main execution."""
    print("\n" + "🌊" * 40)
    print("APACHE AIRFLOW DAG EXAMPLES")
    print("🌊" * 40)

    print_dag_example()

    print("\n" + "=" * 80)
    print("RUNNING PIPELINE SIMULATION")
    print("=" * 80)

    example_complete_pipeline()

    print("\n✅ Airflow DAG examples complete!")


if __name__ == "__main__":
    main()
