#!/usr/bin/env python3
"""
================================================================================
EXAMPLE 3: SPARK UI REST API - PROGRAMMATIC ACCESS
================================================================================

PURPOSE:
    Access Spark UI metrics programmatically using REST API

WHAT YOU'LL LEARN:
    • Query Spark UI via REST API
    • Extract metrics programmatically
    • Automate monitoring
    • Build custom dashboards
    • Export metrics for analysis

API ENDPOINTS:
    • /api/v1/applications - List all applications
    • /api/v1/applications/{appId}/jobs - Job information
    • /api/v1/applications/{appId}/stages - Stage details
    • /api/v1/applications/{appId}/executors - Executor stats
    • /api/v1/applications/{appId}/storage/rdd - Cached RDDs

TIME: 20 minutes | DIFFICULTY: ⭐⭐⭐⭐☆
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, count
import requests
import json
import time
import logging
from typing import Dict, List

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class SparkUIClient:
    """Client for accessing Spark UI REST API."""
    
    def __init__(self, base_url="http://localhost:4040"):
        self.base_url = base_url
        self.api_base = f"{base_url}/api/v1"
    
    def get_application_info(self) -> Dict:
        """Get application information."""
        try:
            response = requests.get(f"{self.api_base}/applications")
            return response.json()[0] if response.ok else None
        except Exception as e:
            logger.error(f"Error fetching app info: {e}")
            return None
    
    def get_all_jobs(self, app_id: str) -> List[Dict]:
        """Get all jobs for an application."""
        try:
            response = requests.get(f"{self.api_base}/applications/{app_id}/jobs")
            return response.json() if response.ok else []
        except Exception as e:
            logger.error(f"Error fetching jobs: {e}")
            return []
    
    def get_job_details(self, app_id: str, job_id: int) -> Dict:
        """Get detailed information about a specific job."""
        try:
            response = requests.get(f"{self.api_base}/applications/{app_id}/jobs/{job_id}")
            return response.json() if response.ok else {}
        except Exception as e:
            logger.error(f"Error fetching job {job_id}: {e}")
            return {}
    
    def get_all_stages(self, app_id: str) -> List[Dict]:
        """Get all stages for an application."""
        try:
            response = requests.get(f"{self.api_base}/applications/{app_id}/stages")
            return response.json() if response.ok else []
        except Exception as e:
            logger.error(f"Error fetching stages: {e}")
            return []
    
    def get_executors(self, app_id: str) -> List[Dict]:
        """Get executor information."""
        try:
            response = requests.get(f"{self.api_base}/applications/{app_id}/executors")
            return response.json() if response.ok else []
        except Exception as e:
            logger.error(f"Error fetching executors: {e}")
            return []
    
    def get_storage_info(self, app_id: str) -> List[Dict]:
        """Get cached RDD information."""
        try:
            response = requests.get(f"{self.api_base}/applications/{app_id}/storage/rdd")
            return response.json() if response.ok else []
        except Exception as e:
            logger.error(f"Error fetching storage: {e}")
            return []


def create_spark_session():
    """Create SparkSession with REST API enabled."""
    spark = SparkSession.builder \
        .appName("REST_API_Demo") \
        .master("local[4]") \
        .config("spark.ui.enabled", "true") \
        .config("spark.ui.port", "4040") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_sample_workload(spark):
    """Run sample workload to generate metrics."""
    logger.info("🚀 Running sample workload...")
    
    # Job 1: Simple count
    df1 = spark.range(0, 5000000).toDF("id")
    count1 = df1.count()
    logger.info(f"   Job 1: Counted {count1:,} records")
    
    # Job 2: Transformation and aggregation
    df2 = spark.range(0, 3000000).toDF("id")
    df2 = df2.withColumn("category", (col("id") % 10).cast("int"))
    df2 = df2.withColumn("value", (rand() * 100).cast("int"))
    
    result = df2.groupBy("category").agg(count("id").alias("count"))
    result.show()
    
    # Job 3: With caching
    df3 = spark.range(0, 2000000).toDF("id")
    df3 = df3.withColumn("data", (rand() * 1000).cast("int"))
    df3.cache()
    
    count3a = df3.count()
    count3b = df3.filter(col("data") > 500).count()
    
    logger.info(f"   Job 3: Total {count3a:,}, Filtered {count3b:,}")
    
    df3.unpersist()
    
    logger.info("✅ Workload completed")


def example_1_application_info(client: SparkUIClient):
    """Example 1: Get application information."""
    print("\n" + "="*80)
    print("EXAMPLE 1: Application Information")
    print("="*80)
    
    app_info = client.get_application_info()
    
    if app_info:
        print(f"\n📊 Application Details:")
        print(f"   ID: {app_info.get('id', 'N/A')}")
        print(f"   Name: {app_info.get('name', 'N/A')}")
        print(f"   Attempts: {len(app_info.get('attempts', []))}")
        
        if app_info.get('attempts'):
            attempt = app_info['attempts'][0]
            print(f"\n   Current Attempt:")
            print(f"   • Start Time: {attempt.get('startTime', 'N/A')}")
            print(f"   • App UI: {attempt.get('appSparkVersion', 'N/A')}")
            print(f"   • Completed: {attempt.get('completed', False)}")
    else:
        print("❌ Could not fetch application info")
    
    return app_info


def example_2_job_metrics(client: SparkUIClient, app_id: str):
    """Example 2: Analyze job metrics."""
    print("\n" + "="*80)
    print("EXAMPLE 2: Job Metrics Analysis")
    print("="*80)
    
    jobs = client.get_all_jobs(app_id)
    
    if jobs:
        print(f"\n📊 Total Jobs: {len(jobs)}")
        print(f"\n{'Job ID':<10} {'Status':<12} {'Tasks':<10} {'Duration (ms)':<15}")
        print("-" * 50)
        
        for job in jobs:
            job_id = job.get('jobId', 'N/A')
            status = job.get('status', 'N/A')
            num_tasks = job.get('numTasks', 0)
            
            # Calculate duration
            start = job.get('submissionTime')
            end = job.get('completionTime')
            duration = (end - start) if (start and end) else 0
            
            print(f"{job_id:<10} {status:<12} {num_tasks:<10} {duration:<15}")
        
        # Detailed analysis of first job
        if len(jobs) > 0:
            print(f"\n🔍 Detailed Analysis of Job {jobs[0].get('jobId')}:")
            job_detail = client.get_job_details(app_id, jobs[0].get('jobId'))
            
            if job_detail:
                print(f"   • Stage IDs: {job_detail.get('stageIds', [])}")
                print(f"   • Num Tasks: {job_detail.get('numTasks', 0)}")
                print(f"   • Num Active Tasks: {job_detail.get('numActiveTasks', 0)}")
                print(f"   • Num Completed Tasks: {job_detail.get('numCompletedTasks', 0)}")
                print(f"   • Num Failed Tasks: {job_detail.get('numFailedTasks', 0)}")
    else:
        print("❌ No jobs found")


def example_3_stage_analysis(client: SparkUIClient, app_id: str):
    """Example 3: Analyze stage performance."""
    print("\n" + "="*80)
    print("EXAMPLE 3: Stage Performance Analysis")
    print("="*80)
    
    stages = client.get_all_stages(app_id)
    
    if stages:
        print(f"\n📊 Total Stages: {len(stages)}")
        print(f"\n{'Stage ID':<10} {'Status':<12} {'Tasks':<10} {'Input':<15} {'Output':<15}")
        print("-" * 65)
        
        for stage in stages:
            stage_id = stage.get('stageId', 'N/A')
            status = stage.get('status', 'N/A')
            num_tasks = stage.get('numTasks', 0)
            input_bytes = stage.get('inputBytes', 0)
            output_bytes = stage.get('outputBytes', 0)
            
            # Format bytes
            input_mb = input_bytes / 1024 / 1024 if input_bytes else 0
            output_mb = output_bytes / 1024 / 1024 if output_bytes else 0
            
            print(f"{stage_id:<10} {status:<12} {num_tasks:<10} {input_mb:<14.2f}MB {output_mb:<14.2f}MB")
        
        # Find slowest stage
        completed_stages = [s for s in stages if s.get('status') == 'COMPLETE']
        if completed_stages:
            slowest = max(completed_stages, key=lambda s: s.get('executorRunTime', 0))
            
            print(f"\n⚠️  Slowest Stage:")
            print(f"   Stage ID: {slowest.get('stageId')}")
            print(f"   Executor Run Time: {slowest.get('executorRunTime', 0)}ms")
            print(f"   Shuffle Read: {slowest.get('shuffleReadBytes', 0) / 1024 / 1024:.2f}MB")
            print(f"   Shuffle Write: {slowest.get('shuffleWriteBytes', 0) / 1024 / 1024:.2f}MB")
    else:
        print("❌ No stages found")


def example_4_executor_stats(client: SparkUIClient, app_id: str):
    """Example 4: Executor statistics."""
    print("\n" + "="*80)
    print("EXAMPLE 4: Executor Statistics")
    print("="*80)
    
    executors = client.get_executors(app_id)
    
    if executors:
        print(f"\n📊 Total Executors: {len(executors)}")
        print(f"\n{'Executor ID':<15} {'Active':<10} {'Tasks':<10} {'Failed':<10} {'Memory':<15}")
        print("-" * 65)
        
        total_memory = 0
        total_tasks = 0
        
        for executor in executors:
            exec_id = executor.get('id', 'N/A')
            is_active = executor.get('isActive', False)
            total_t = executor.get('totalTasks', 0)
            failed_t = executor.get('failedTasks', 0)
            max_mem = executor.get('maxMemory', 0)
            
            total_memory += max_mem
            total_tasks += total_t
            
            mem_gb = max_mem / 1024 / 1024 / 1024
            
            print(f"{exec_id:<15} {str(is_active):<10} {total_t:<10} {failed_t:<10} {mem_gb:<14.2f}GB")
        
        print(f"\n📊 Summary:")
        print(f"   Total Memory: {total_memory / 1024 / 1024 / 1024:.2f}GB")
        print(f"   Total Tasks Executed: {total_tasks}")
    else:
        print("❌ No executor info found")


def example_5_storage_info(client: SparkUIClient, app_id: str):
    """Example 5: Storage and caching information."""
    print("\n" + "="*80)
    print("EXAMPLE 5: Storage & Caching")
    print("="*80)
    
    storage = client.get_storage_info(app_id)
    
    if storage:
        print(f"\n📊 Cached RDDs/DataFrames: {len(storage)}")
        
        for rdd in storage:
            print(f"\n   RDD ID: {rdd.get('id')}")
            print(f"   Name: {rdd.get('name', 'N/A')}")
            print(f"   Storage Level: {rdd.get('storageLevel', 'N/A')}")
            print(f"   Partitions: {rdd.get('numPartitions', 0)}")
            print(f"   Cached Partitions: {rdd.get('numCachedPartitions', 0)}")
            print(f"   Memory Size: {rdd.get('memoryUsed', 0) / 1024 / 1024:.2f}MB")
            print(f"   Disk Size: {rdd.get('diskUsed', 0) / 1024 / 1024:.2f}MB")
    else:
        print("\n💡 No cached data found")
        print("   (This is normal if caching wasn't used)")


def main():
    """Run REST API demonstrations."""
    
    print("\n" + "="*80)
    print("🎯 SPARK UI REST API ACCESS")
    print("="*80)
    
    print(f"\n📋 What We'll Do:")
    print(f"   1. Start Spark application")
    print(f"   2. Run sample workload")
    print(f"   3. Query REST API programmatically")
    print(f"   4. Extract and analyze metrics")
    print(f"   5. Build custom monitoring")
    
    # Start Spark
    spark = create_spark_session()
    
    print(f"\n🌐 Spark UI: http://localhost:4040")
    print(f"📡 REST API: http://localhost:4040/api/v1")
    
    time.sleep(2)
    
    # Run workload
    print(f"\n" + "="*80)
    print("GENERATING METRICS (Running Workload)")
    print("="*80)
    
    run_sample_workload(spark)
    
    time.sleep(2)
    
    # Create API client
    client = SparkUIClient()
    
    # Get application info
    app_info = example_1_application_info(client)
    
    if not app_info:
        print("\n❌ Could not connect to Spark UI API")
        print("   Make sure Spark UI is running on http://localhost:4040")
        spark.stop()
        return
    
    app_id = app_info.get('id')
    
    # Run examples
    example_2_job_metrics(client, app_id)
    example_3_stage_analysis(client, app_id)
    example_4_executor_stats(client, app_id)
    example_5_storage_info(client, app_id)
    
    # Summary
    print("\n" + "="*80)
    print("✅ REST API DEMONSTRATION COMPLETE")
    print("="*80)
    
    print(f"\n📡 Available API Endpoints:")
    print(f"   GET /api/v1/applications")
    print(f"   GET /api/v1/applications/{{appId}}/jobs")
    print(f"   GET /api/v1/applications/{{appId}}/jobs/{{jobId}}")
    print(f"   GET /api/v1/applications/{{appId}}/stages")
    print(f"   GET /api/v1/applications/{{appId}}/stages/{{stageId}}")
    print(f"   GET /api/v1/applications/{{appId}}/executors")
    print(f"   GET /api/v1/applications/{{appId}}/storage/rdd")
    
    print(f"\n💡 Use Cases:")
    print(f"   • Custom monitoring dashboards")
    print(f"   • Automated performance analysis")
    print(f"   • Integration with external tools")
    print(f"   • Alerting based on metrics")
    print(f"   • Historical trend analysis")
    print(f"   • CI/CD performance testing")
    
    print(f"\n🔧 Example Integration:")
    print(f"""
   import requests
   
   # Get all jobs
   response = requests.get('http://localhost:4040/api/v1/applications')
   apps = response.json()
   
   # Extract metrics
   app_id = apps[0]['id']
   jobs = requests.get(f'http://localhost:4040/api/v1/applications/{app_id}/jobs')
   
   # Analyze performance
   for job in jobs.json():
       print(f"Job {job['jobId']}: {job['status']}")
   """)
    
    print(f"\n📖 Next Steps:")
    print(f"   1. Explore API endpoints in browser")
    print(f"   2. Build custom monitoring scripts")
    print(f"   3. Integrate with dashboards (Grafana)")
    print(f"   4. Set up automated alerts")
    print(f"   5. Export metrics to time-series DB")
    
    time.sleep(5)
    
    spark.stop()
    logger.info("✅ Done!")


if __name__ == "__main__":
    main()
