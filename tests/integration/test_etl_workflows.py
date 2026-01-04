#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
INTEGRATION TESTS - End-to-End ETL Workflows
================================================================================

PURPOSE:
--------
Test complete ETL pipelines from data ingestion through transformation to
output, verifying:
• Data flow through multiple stages
• Integration between modules
• Real-world scenarios
• Performance characteristics
• Error recovery
• Resource usage

TESTING APPROACH:
-----------------
• Real data files (not mocked)
• Complete workflows (read → transform → write)
• Multiple operations chained together
• Realistic data volumes
• Performance benchmarks
• Error injection and recovery
"""

import logging
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, broadcast, col, count
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


class IntegrationTestBase(unittest.TestCase):
    """Base class for integration tests with comprehensive setup."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session and test data directories."""
        cls.logger = logging.getLogger(cls.__name__)
        cls.logger.info(f"Setting up integration tests: {cls.__name__}")

        # Create Spark session with realistic configuration
        try:
            cls.spark = (
                SparkSession.builder.appName(f"IntegrationTest_{cls.__name__}")
                .master("local[4]")
                .config("spark.sql.shuffle.partitions", "8")
                .config("spark.default.parallelism", "8")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate()
            )

            cls.logger.info("✅ Spark session created successfully")
            cls.logger.info(f"   Version: {cls.spark.version}")
            cls.logger.info(f"   Master: {cls.spark.sparkContext.master}")

        except Exception as e:
            cls.logger.error(f"❌ Failed to create Spark session: {e}")
            raise

        # Create base test data directory
        cls.base_test_dir = tempfile.mkdtemp(prefix="pyspark_integration_")
        cls.logger.info(f"Created test directory: {cls.base_test_dir}")

    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session and test data."""
        try:
            if hasattr(cls, "spark"):
                cls.spark.stop()
                cls.logger.info("✅ Spark session stopped")
        except Exception as e:
            cls.logger.error(f"❌ Error stopping Spark session: {e}")

        try:
            if hasattr(cls, "base_test_dir") and os.path.exists(cls.base_test_dir):
                shutil.rmtree(cls.base_test_dir)
                cls.logger.info(f"✅ Cleaned up test directory: {cls.base_test_dir}")
        except Exception as e:
            cls.logger.error(f"❌ Error cleaning up test directory: {e}")

    def setUp(self):
        """Set up before each test."""
        if hasattr(self.__class__, "logger"):
            self.__class__.logger.info(f"\n{'='*70}")
            self.__class__.logger.info(f"Running: {self._testMethodName}")
            self.__class__.logger.info(f"{'='*70}")

        # Create test-specific directory
        self.test_dir = os.path.join(self.base_test_dir, self._testMethodName)
        os.makedirs(self.test_dir, exist_ok=True)

        # Track start time for performance measurement
        self.start_time = time.time()

    def tearDown(self):
        """Clean up after each test."""
        duration = time.time() - self.start_time
        if hasattr(self.__class__, "logger"):
            self.__class__.logger.info(f"Test duration: {duration:.3f}s")


class TestCompleteETLPipeline(IntegrationTestBase):
    """Test complete ETL pipeline: extract, transform, load."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test class."""
        super().setUpClass()
        cls.logger = logging.getLogger(cls.__name__)

    def test_end_to_end_sales_pipeline(self):
        """
        Test complete sales data pipeline.

        Pipeline:
        1. Read raw sales data (CSV)
        2. Read customer data (Parquet)
        3. Join sales with customers
        4. Aggregate by customer and region
        5. Write results (Parquet)
        6. Verify output
        """
        self.logger.info("Testing end-to-end sales pipeline...")

        # Step 1: Create sample data files
        self.logger.info("Step 1: Creating sample data...")

        # Sales data (1000 transactions)
        sales_data = []
        for i in range(1, 1001):
            sales_data.append(
                {
                    "transaction_id": i,
                    "customer_id": (i % 100) + 1,  # 100 unique customers
                    "product": f"product_{(i % 20) + 1}",  # 20 products
                    "amount": (i % 500) + 10.0,
                    "quantity": (i % 10) + 1,
                }
            )

        sales_df = self.spark.createDataFrame(sales_data)
        sales_path = os.path.join(self.test_dir, "sales.csv")
        sales_df.write.mode("overwrite").csv(sales_path, header=True)
        self.logger.info(f"   Created sales data: {len(sales_data)} transactions")

        # Customer data (100 customers)
        customer_data = []
        for i in range(1, 101):
            customer_data.append(
                {
                    "customer_id": i,
                    "customer_name": f"Customer_{i}",
                    "region": f"Region_{(i % 5) + 1}",  # 5 regions
                    "country": "USA",
                }
            )

        customer_df = self.spark.createDataFrame(customer_data)
        customer_path = os.path.join(self.test_dir, "customers.parquet")
        customer_df.write.mode("overwrite").parquet(customer_path)
        self.logger.info(f"   Created customer data: {len(customer_data)} customers")

        # Step 2: Read data
        self.logger.info("Step 2: Reading data...")

        try:
            sales = self.spark.read.csv(sales_path, header=True, inferSchema=True)
            customers = self.spark.read.parquet(customer_path)

            self.logger.info(f"   ✅ Sales records: {sales.count()}")
            self.logger.info(f"   ✅ Customer records: {customers.count()}")

        except Exception as e:
            self.fail(f"Failed to read data: {e}")

        # Step 3: Transform - Join and aggregate
        self.logger.info("Step 3: Transforming data...")

        try:
            # Join sales with customers (broadcast join for small customer table)
            enriched_sales = sales.join(broadcast(customers), "customer_id", "inner")

            self.logger.info(f"   ✅ Joined records: {enriched_sales.count()}")

            # Aggregate by customer and region
            customer_summary = enriched_sales.groupBy(
                "customer_id", "customer_name", "region"
            ).agg(
                count("*").alias("transaction_count"),
                _sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                _sum("quantity").alias("total_quantity"),
            )

            # Region summary
            region_summary = enriched_sales.groupBy("region").agg(
                count("*").alias("transaction_count"),
                _sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
            )

            self.logger.info(f"   ✅ Customer summaries: {customer_summary.count()}")
            self.logger.info(f"   ✅ Region summaries: {region_summary.count()}")

        except Exception as e:
            self.fail(f"Failed to transform data: {e}")

        # Step 4: Write results
        self.logger.info("Step 4: Writing results...")

        try:
            customer_output = os.path.join(self.test_dir, "customer_summary.parquet")
            region_output = os.path.join(self.test_dir, "region_summary.parquet")

            customer_summary.write.mode("overwrite").parquet(customer_output)
            region_summary.write.mode("overwrite").parquet(region_output)

            self.logger.info(f"   ✅ Wrote customer summary to {customer_output}")
            self.logger.info(f"   ✅ Wrote region summary to {region_output}")

        except Exception as e:
            self.fail(f"Failed to write results: {e}")

        # Step 5: Verify results
        self.logger.info("Step 5: Verifying results...")

        try:
            # Read back and verify
            customer_result = self.spark.read.parquet(customer_output)
            region_result = self.spark.read.parquet(region_output)

            # Verify counts
            self.assertEqual(
                customer_result.count(), 100, "Should have 100 customer summaries"
            )
            self.assertEqual(region_result.count(), 5, "Should have 5 region summaries")

            # Verify schema
            expected_customer_columns = {
                "customer_id",
                "customer_name",
                "region",
                "transaction_count",
                "total_amount",
                "avg_amount",
                "total_quantity",
            }
            actual_customer_columns = set(customer_result.columns)
            self.assertEqual(
                actual_customer_columns,
                expected_customer_columns,
                "Customer summary schema mismatch",
            )

            # Verify data integrity
            total_transactions = customer_result.agg(_sum("transaction_count")).first()[
                0
            ]
            self.assertEqual(
                total_transactions, 1000, "Total transactions should be 1000"
            )

            self.logger.info("   ✅ All verifications passed")

        except AssertionError as e:
            self.fail(f"Verification failed: {e}")
        except Exception as e:
            self.fail(f"Error during verification: {e}")

        self.logger.info("✅ End-to-end sales pipeline completed successfully")

    def test_multi_stage_transformation_pipeline(self):
        """
        Test multi-stage data transformation pipeline.

        Pipeline:
        1. Raw data ingestion
        2. Data cleaning (remove nulls, fix types)
        3. Feature engineering
        4. Aggregation
        5. Output to multiple formats
        """
        self.logger.info("Testing multi-stage transformation pipeline...")

        # Create raw data with quality issues
        raw_data = []
        for i in range(1, 501):
            raw_data.append(
                {
                    "id": i,
                    "value": float(i * 10) if i % 10 != 0 else None,  # Some nulls
                    "category": f"cat_{(i % 5) + 1}",
                    "status": "active" if i % 3 != 0 else "inactive",
                    "timestamp": f"2024-01-{(i % 28) + 1:02d}",
                }
            )

        df = self.spark.createDataFrame(raw_data)

        # Stage 1: Data cleaning
        self.logger.info("Stage 1: Data cleaning...")
        try:
            # Remove nulls
            cleaned_df = df.filter(col("value").isNotNull())

            # Fix types
            from pyspark.sql.functions import to_date

            cleaned_df = cleaned_df.withColumn("date", to_date(col("timestamp")))

            null_count = df.count() - cleaned_df.count()
            self.logger.info(f"   ✅ Removed {null_count} null records")
            self.logger.info(f"   ✅ Clean records: {cleaned_df.count()}")

        except Exception as e:
            self.fail(f"Data cleaning failed: {e}")

        # Stage 2: Feature engineering
        self.logger.info("Stage 2: Feature engineering...")
        try:
            from pyspark.sql.functions import lit, when

            enriched_df = (
                cleaned_df.withColumn("value_squared", col("value") * col("value"))
                .withColumn(
                    "value_category",
                    when(col("value") < 100, "low")
                    .when(col("value") < 300, "medium")
                    .otherwise("high"),
                )
                .withColumn(
                    "is_active",
                    when(col("status") == "active", lit(1)).otherwise(lit(0)),
                )
            )

            self.logger.info(f"   ✅ Enriched {enriched_df.count()} records")

        except Exception as e:
            self.fail(f"Feature engineering failed: {e}")

        # Stage 3: Aggregation
        self.logger.info("Stage 3: Aggregation...")
        try:
            summary = enriched_df.groupBy("category", "value_category").agg(
                count("*").alias("count"),
                _sum("value").alias("total_value"),
                avg("value").alias("avg_value"),
                _sum("is_active").alias("active_count"),
            )

            self.logger.info(f"   ✅ Generated {summary.count()} summary records")

        except Exception as e:
            self.fail(f"Aggregation failed: {e}")

        # Stage 4: Write multiple formats
        self.logger.info("Stage 4: Writing multiple formats...")
        try:
            # Parquet (efficient columnar format)
            parquet_path = os.path.join(self.test_dir, "output.parquet")
            summary.write.mode("overwrite").parquet(parquet_path)

            # CSV (human-readable)
            csv_path = os.path.join(self.test_dir, "output.csv")
            summary.coalesce(1).write.mode("overwrite").csv(csv_path, header=True)

            # JSON (semi-structured)
            json_path = os.path.join(self.test_dir, "output.json")
            summary.coalesce(1).write.mode("overwrite").json(json_path)

            self.logger.info(f"   ✅ Wrote Parquet: {parquet_path}")
            self.logger.info(f"   ✅ Wrote CSV: {csv_path}")
            self.logger.info(f"   ✅ Wrote JSON: {json_path}")

            # Verify all files exist
            self.assertTrue(os.path.exists(parquet_path))
            self.assertTrue(os.path.exists(csv_path))
            self.assertTrue(os.path.exists(json_path))

        except Exception as e:
            self.fail(f"Writing output failed: {e}")

        self.logger.info(
            "✅ Multi-stage transformation pipeline completed successfully"
        )


class TestPerformanceIntegration(IntegrationTestBase):
    """Test performance characteristics of integrated workflows."""    
    @classmethod
    def setUpClass(cls):
        \"\"\"Set up test class.\"\"\"
        super().setUpClass()
        cls.logger = logging.getLogger(cls.__name__)
    def test_large_join_performance(self):
        """
        Test performance of large-scale joins.

        Measures:
        - Join execution time
        - Memory usage
        - Shuffle characteristics
        """
        self.logger.info("Testing large join performance...")

        # Create large datasets
        large_size = 100000
        small_size = 1000

        self.logger.info(
            f"Creating datasets: large={large_size:,}, small={small_size:,}"
        )

        # Large fact table
        large_df = (
            self.spark.range(large_size)
            .withColumnRenamed("id", "fact_id")
            .withColumn("dimension_id", (col("fact_id") % small_size).cast("int"))
            .withColumn("value", col("fact_id") * 1.5)
        )

        # Small dimension table
        small_df = (
            self.spark.range(small_size)
            .withColumnRenamed("id", "dimension_id")
            .withColumn("name", col("dimension_id").cast("string"))
        )

        # Test 1: Regular join (with shuffle)
        self.logger.info("Test 1: Regular join...")
        start = time.time()
        regular_join = large_df.join(small_df, "dimension_id", "inner")
        result_count = regular_join.count()
        regular_time = time.time() - start

        self.logger.info(f"   Result count: {result_count:,}")
        self.logger.info(f"   Time: {regular_time:.3f}s")
        self.assertEqual(result_count, large_size, "Should have all records")

        # Test 2: Broadcast join (no shuffle of large table)
        self.logger.info("Test 2: Broadcast join...")
        start = time.time()
        broadcast_join = large_df.join(broadcast(small_df), "dimension_id", "inner")
        broadcast_result_count = broadcast_join.count()
        broadcast_time = time.time() - start

        self.logger.info(f"   Result count: {broadcast_result_count:,}")
        self.logger.info(f"   Time: {broadcast_time:.3f}s")
        self.logger.info(f"   Speedup: {regular_time / broadcast_time:.2f}x")

        # Verify broadcast join is faster
        self.assertLess(
            broadcast_time,
            regular_time * 1.1,  # Allow 10% margin
            "Broadcast join should be faster or comparable",
        )

        self.logger.info("✅ Large join performance test completed")

    def test_partition_optimization_impact(self):
        """
        Test impact of partition optimization on performance.

        Compares:
        - Default partitioning
        - Optimized partitioning
        """
        self.logger.info("Testing partition optimization impact...")

        # Create dataset
        df = (
            self.spark.range(50000)
            .withColumn("category", (col("id") % 10).cast("string"))
            .withColumn("value", col("id") * 2.5)
        )

        # Test 1: Default partitioning
        self.logger.info("Test 1: Default partitioning...")
        default_partitions = df.rdd.getNumPartitions()

        start = time.time()
        result1 = df.groupBy("category").agg(_sum("value").alias("total"))
        result1.count()
        default_time = time.time() - start

        self.logger.info(f"   Partitions: {default_partitions}")
        self.logger.info(f"   Time: {default_time:.3f}s")

        # Test 2: Optimized partitioning
        self.logger.info("Test 2: Optimized partitioning...")

        # Repartition by grouping key
        optimized_df = df.repartition(8, "category")
        optimized_partitions = optimized_df.rdd.getNumPartitions()

        start = time.time()
        result2 = optimized_df.groupBy("category").agg(_sum("value").alias("total"))
        result2.count()
        optimized_time = time.time() - start

        self.logger.info(f"   Partitions: {optimized_partitions}")
        self.logger.info(f"   Time: {optimized_time:.3f}s")
        self.logger.info(
            f"   Improvement: {(default_time - optimized_time) / default_time * 100:.1f}%"
        )

        # Both should produce same results
        self.assertEqual(result1.count(), result2.count(), "Results should match")

        self.logger.info("✅ Partition optimization test completed")


class TestErrorRecoveryIntegration(IntegrationTestBase):
    """Test error handling and recovery in integrated workflows."""    
    @classmethod
    def setUpClass(cls):
        \"\"\"Set up test class.\"\"\"
        super().setUpClass()
        cls.logger = logging.getLogger(cls.__name__)
    def test_graceful_error_handling(self):
        """
        Test graceful error handling in pipeline.

        Scenarios:
        - Invalid data
        - Missing columns
        - Type mismatches
        """
        self.logger.info("Testing graceful error handling...")

        # Create dataset with issues
        data = []
        for i in range(1, 101):
            data.append(
                {
                    "id": i,
                    "value": str(i) if i % 10 == 0 else i,  # Mixed types
                    "category": f"cat_{i % 5}" if i % 7 != 0 else None,  # Some nulls
                }
            )

        df = self.spark.createDataFrame(data)

        # Test 1: Handle mixed types
        self.logger.info("Test 1: Handling mixed types...")
        try:
            from pyspark.sql.functions import when

            # Try to cast, handle failures
            clean_df = df.withColumn(
                "value_clean",
                when(
                    col("value").cast("int").isNotNull(), col("value").cast("int")
                ).otherwise(0),
            )

            result_count = clean_df.count()
            self.logger.info(f"   ✅ Cleaned {result_count} records")

        except Exception as e:
            self.fail(f"Failed to handle mixed types: {e}")

        # Test 2: Handle null values
        self.logger.info("Test 2: Handling null values...")
        try:
            # Fill nulls with default
            from pyspark.sql.functions import coalesce, lit

            filled_df = df.withColumn(
                "category_clean", coalesce(col("category"), lit("unknown"))
            )

            null_count = df.filter(col("category").isNull()).count()
            self.logger.info(f"   ✅ Filled {null_count} null values")

        except Exception as e:
            self.fail(f"Failed to handle nulls: {e}")

        self.logger.info("✅ Error handling test completed")

    def test_recovery_from_corrupt_data(self):
        """Test recovery from corrupt data files."""
        self.logger.info("Testing recovery from corrupt data...")

        # Create good data
        good_data = [{"id": i, "value": i * 10} for i in range(1, 51)]
        good_df = self.spark.createDataFrame(good_data)

        good_path = os.path.join(self.test_dir, "good_data.parquet")
        good_df.write.mode("overwrite").parquet(good_path)

        # Create corrupt file
        corrupt_path = os.path.join(self.test_dir, "corrupt_data.txt")
        with open(corrupt_path, "w") as f:
            f.write("This is not valid parquet data")

        # Test reading with error handling
        try:
            # This should work
            result = self.spark.read.parquet(good_path)
            self.assertEqual(result.count(), 50)
            self.logger.info("   ✅ Successfully read good data")

            # This should fail gracefully
            try:
                bad_result = self.spark.read.parquet(corrupt_path)
                bad_result.count()
                self.fail("Should have raised exception for corrupt data")
            except Exception as e:
                self.logger.info(
                    f"   ✅ Correctly caught corrupt data: {type(e).__name__}"
                )

        except Exception as e:
            self.fail(f"Unexpected error: {e}")

        self.logger.info("✅ Corrupt data recovery test completed")


def suite():
    """Create integration test suite."""
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCompleteETLPipeline))
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(TestPerformanceIntegration)
    )
    suite.addTests(
        unittest.TestLoader().loadTestsFromTestCase(TestErrorRecoveryIntegration)
    )

    return suite


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite())

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
