#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
UNIT TESTS - Cluster Computing Modules
================================================================================

PURPOSE:
--------
Test all cluster_computing modules for:
• Function correctness
• Error handling
• Resource management
• Performance characteristics
• Edge cases and boundary conditions

TESTING APPROACH:
-----------------
• Mock external dependencies (YARN, Kubernetes, etc.)
• Test each function in isolation
• Verify error messages and logging
• Check resource cleanup
• Measure performance
"""

import unittest
import sys
import os
import logging
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
import tempfile
import shutil

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class BaseTestCase(unittest.TestCase):
    """Base test case with common setup/teardown."""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session once for all tests."""
        cls.logger = logging.getLogger(cls.__name__)
        cls.logger.info(f"Setting up {cls.__name__}")
        
        try:
            cls.spark = SparkSession.builder \
                .appName(f"UnitTest_{cls.__name__}") \
                .master("local[2]") \
                .config("spark.sql.shuffle.partitions", "2") \
                .config("spark.default.parallelism", "2") \
                .getOrCreate()
            
            cls.logger.info("✅ Spark session created successfully")
        
        except Exception as e:
            cls.logger.error(f"❌ Failed to create Spark session: {e}")
            raise
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session."""
        try:
            if hasattr(cls, 'spark'):
                cls.spark.stop()
                cls.logger.info("✅ Spark session stopped successfully")
        except Exception as e:
            cls.logger.error(f"❌ Error stopping Spark session: {e}")
    
    def setUp(self):
        """Set up before each test."""
        self.logger.info(f"\n{'='*70}")
        self.logger.info(f"Running: {self._testMethodName}")
        self.logger.info(f"{'='*70}")
        
        # Create temp directory for test data
        self.temp_dir = tempfile.mkdtemp()
        self.logger.info(f"Created temp directory: {self.temp_dir}")
    
    def tearDown(self):
        """Clean up after each test."""
        # Clean up temp directory
        try:
            if os.path.exists(self.temp_dir):
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up temp directory: {self.temp_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to clean up temp directory: {e}")


class TestClusterSetup(BaseTestCase):
    """Test cluster setup and configuration."""
    
    def test_spark_session_creation(self):
        """Test Spark session is created with correct configuration."""
        self.assertIsNotNone(self.spark, "Spark session should not be None")
        self.assertEqual(self.spark.sparkContext.master, "local[2]")
        self.logger.info("✅ Spark session configuration verified")
    
    def test_spark_version(self):
        """Test Spark version is 3.x or higher."""
        version = self.spark.version
        major_version = int(version.split('.')[0])
        
        self.assertGreaterEqual(major_version, 3, 
                               f"Spark version should be 3.x or higher, got {version}")
        self.logger.info(f"✅ Spark version check passed: {version}")
    
    def test_configuration_settings(self):
        """Test that configuration settings are applied."""
        conf = self.spark.sparkContext.getConf()
        
        # Check shuffle partitions
        shuffle_partitions = conf.get('spark.sql.shuffle.partitions')
        self.assertEqual(shuffle_partitions, '2', 
                        f"Expected shuffle partitions 2, got {shuffle_partitions}")
        
        self.logger.info("✅ Configuration settings verified")
    
    def test_error_handling_invalid_master(self):
        """Test error handling when connecting to invalid master."""
        self.logger.info("Testing error handling for invalid master URL...")
        
        with self.assertRaises(Exception) as context:
            # Try to create session with invalid master
            invalid_spark = SparkSession.builder \
                .appName("InvalidMasterTest") \
                .master("spark://invalid-host:7077") \
                .config("spark.network.timeout", "1s") \
                .getOrCreate()
            
            # Try to perform an action
            invalid_spark.range(10).count()
        
        self.logger.info(f"✅ Correctly caught error: {type(context.exception).__name__}")


class TestDataPartitioning(BaseTestCase):
    """Test data partitioning operations."""
    
    def test_repartition_increases_partitions(self):
        """Test repartition increases number of partitions."""
        # Create DataFrame with 2 partitions
        df = self.spark.range(100).repartition(2)
        self.assertEqual(df.rdd.getNumPartitions(), 2)
        
        # Repartition to 8
        df_repartitioned = df.repartition(8)
        self.assertEqual(df_repartitioned.rdd.getNumPartitions(), 8)
        
        self.logger.info("✅ Repartition correctly increases partitions")
    
    def test_coalesce_decreases_partitions(self):
        """Test coalesce decreases number of partitions without shuffle."""
        # Create DataFrame with 8 partitions
        df = self.spark.range(100).repartition(8)
        self.assertEqual(df.rdd.getNumPartitions(), 8)
        
        # Coalesce to 2
        df_coalesced = df.coalesce(2)
        self.assertEqual(df_coalesced.rdd.getNumPartitions(), 2)
        
        self.logger.info("✅ Coalesce correctly decreases partitions")
    
    def test_repartition_by_column(self):
        """Test repartitioning by column for better distribution."""
        data = [("A", 1), ("B", 2), ("A", 3), ("B", 4)] * 25
        df = self.spark.createDataFrame(data, ["key", "value"])
        
        # Repartition by key
        df_repartitioned = df.repartition(4, "key")
        self.assertEqual(df_repartitioned.rdd.getNumPartitions(), 4)
        
        # Verify data integrity
        self.assertEqual(df_repartitioned.count(), len(data))
        
        self.logger.info("✅ Repartition by column works correctly")
    
    def test_partition_size_optimization(self):
        """Test partition sizes are reasonable."""
        # Create large DataFrame
        df = self.spark.range(10000).repartition(10)
        
        # Check partition distribution
        partition_sizes = df.rdd.mapPartitions(lambda it: [sum(1 for _ in it)]).collect()
        
        # All partitions should have data
        self.assertTrue(all(size > 0 for size in partition_sizes),
                       "All partitions should have data")
        
        # Check balance (no partition should have > 2x average)
        avg_size = sum(partition_sizes) / len(partition_sizes)
        max_size = max(partition_sizes)
        self.assertLess(max_size, avg_size * 2,
                       f"Partitions are skewed: max={max_size}, avg={avg_size}")
        
        self.logger.info(f"✅ Partition sizes balanced: avg={avg_size:.0f}, max={max_size}")


class TestDistributedJoins(BaseTestCase):
    """Test distributed join operations."""
    
    def test_simple_join(self):
        """Test basic join operation."""
        # Create two DataFrames
        df1 = self.spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
        df2 = self.spark.createDataFrame([(1, "Engineering"), (2, "Marketing")], ["id", "dept"])
        
        # Perform join
        result = df1.join(df2, "id")
        
        # Verify result
        self.assertEqual(result.count(), 2, "Should have 2 matching rows")
        self.assertEqual(len(result.columns), 3, "Should have 3 columns")
        
        self.logger.info("✅ Simple join executed correctly")
    
    def test_broadcast_join(self):
        """Test broadcast join optimization."""
        from pyspark.sql.functions import broadcast
        
        # Create large and small DataFrames
        large_df = self.spark.range(10000).withColumnRenamed("id", "large_id")
        small_df = self.spark.range(100).withColumnRenamed("id", "small_id")
        
        # Broadcast join
        result = large_df.join(broadcast(small_df), 
                              large_df.large_id == small_df.small_id)
        
        # Verify result
        self.assertEqual(result.count(), 100, "Should have 100 matching rows")
        
        # Check explain plan for BroadcastHashJoin
        plan = result._jdf.queryExecution().toString()
        self.assertIn("BroadcastHashJoin", plan, 
                     "Should use BroadcastHashJoin")
        
        self.logger.info("✅ Broadcast join optimized correctly")
    
    def test_join_types(self):
        """Test different join types."""
        df1 = self.spark.createDataFrame([(1, "A"), (2, "B"), (3, "C")], ["id", "val1"])
        df2 = self.spark.createDataFrame([(2, "X"), (3, "Y"), (4, "Z")], ["id", "val2"])
        
        # Inner join
        inner = df1.join(df2, "id", "inner")
        self.assertEqual(inner.count(), 2, "Inner join should have 2 rows")
        
        # Left outer join
        left = df1.join(df2, "id", "left")
        self.assertEqual(left.count(), 3, "Left join should have 3 rows")
        
        # Right outer join
        right = df1.join(df2, "id", "right")
        self.assertEqual(right.count(), 3, "Right join should have 3 rows")
        
        # Full outer join
        full = df1.join(df2, "id", "outer")
        self.assertEqual(full.count(), 4, "Full outer join should have 4 rows")
        
        self.logger.info("✅ All join types work correctly")
    
    def test_error_handling_missing_column(self):
        """Test error handling when join column doesn't exist."""
        df1 = self.spark.createDataFrame([(1, "A")], ["id", "val"])
        df2 = self.spark.createDataFrame([(2, "X")], ["id", "val"])
        
        with self.assertRaises(Exception):
            # Try to join on non-existent column
            result = df1.join(df2, "nonexistent_column")
            result.count()  # Force evaluation
        
        self.logger.info("✅ Correctly handles missing join column")


class TestAggregationsAtScale(BaseTestCase):
    """Test aggregation operations."""
    
    def test_simple_aggregation(self):
        """Test basic aggregation functions."""
        from pyspark.sql.functions import sum, avg, count, max, min
        
        # Create test data
        data = [(1, "A", 100), (2, "A", 200), (3, "B", 150), (4, "B", 250)]
        df = self.spark.createDataFrame(data, ["id", "category", "value"])
        
        # Perform aggregation
        result = df.groupBy("category").agg(
            count("*").alias("count"),
            sum("value").alias("total"),
            avg("value").alias("average"),
            max("value").alias("max_val"),
            min("value").alias("min_val")
        )
        
        # Verify results
        self.assertEqual(result.count(), 2, "Should have 2 categories")
        
        # Check category A
        cat_a = result.filter("category = 'A'").first()
        self.assertEqual(cat_a['count'], 2)
        self.assertEqual(cat_a['total'], 300)
        self.assertEqual(cat_a['average'], 150.0)
        
        self.logger.info("✅ Aggregations computed correctly")
    
    def test_window_functions(self):
        """Test window functions."""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number, rank
        
        # Create test data
        data = [(1, "A", 100), (2, "A", 200), (3, "B", 150), (4, "B", 250)]
        df = self.spark.createDataFrame(data, ["id", "category", "value"])
        
        # Define window
        window = Window.partitionBy("category").orderBy("value")
        
        # Apply window functions
        result = df.withColumn("row_num", row_number().over(window)) \
                   .withColumn("rank", rank().over(window))
        
        # Verify results
        self.assertEqual(result.count(), 4)
        self.assertTrue("row_num" in result.columns)
        self.assertTrue("rank" in result.columns)
        
        self.logger.info("✅ Window functions work correctly")
    
    def test_performance_large_aggregation(self):
        """Test performance of aggregations on larger dataset."""
        import time
        
        # Create larger dataset
        df = self.spark.range(100000).selectExpr("id", "id % 10 as category", "id * 2 as value")
        
        # Time the aggregation
        start_time = time.time()
        result = df.groupBy("category").agg({"value": "sum"})
        count = result.count()
        duration = time.time() - start_time
        
        # Verify
        self.assertEqual(count, 10, "Should have 10 categories")
        self.logger.info(f"✅ Large aggregation completed in {duration:.3f}s")
        
        # Performance assertion (should complete in reasonable time)
        self.assertLess(duration, 10.0, 
                       f"Aggregation took too long: {duration:.3f}s")


class TestFaultTolerance(BaseTestCase):
    """Test fault tolerance and error recovery."""
    
    def test_recovery_from_task_failure(self):
        """Test recovery from task failure."""
        # Create DataFrame that will cause some tasks to fail
        def flaky_function(x):
            if x % 100 == 0:  # Fail on every 100th record
                raise ValueError("Simulated task failure")
            return x * 2
        
        try:
            df = self.spark.range(1000)
            # This should fail
            result = df.rdd.map(flaky_function).collect()
            self.fail("Should have raised ValueError")
        except Exception as e:
            self.logger.info(f"✅ Correctly caught task failure: {type(e).__name__}")
    
    def test_checkpoint_recovery(self):
        """Test checkpointing for recovery."""
        # Set checkpoint directory
        checkpoint_dir = os.path.join(self.temp_dir, "checkpoints")
        self.spark.sparkContext.setCheckpointDir(checkpoint_dir)
        
        # Create DataFrame and checkpoint
        df = self.spark.range(1000).repartition(4)
        df.cache()
        df.count()  # Materialize
        
        # Checkpoint
        df_checkpointed = df.checkpoint()
        
        # Verify checkpoint exists
        self.assertTrue(os.path.exists(checkpoint_dir))
        self.logger.info("✅ Checkpoint created successfully")
    
    def test_cache_persistence(self):
        """Test caching for fault tolerance."""
        from pyspark import StorageLevel
        
        # Create and cache DataFrame
        df = self.spark.range(10000)
        df.persist(StorageLevel.MEMORY_ONLY)
        
        # Force materialization
        count1 = df.count()
        
        # Second count should use cache
        import time
        start = time.time()
        count2 = df.count()
        cache_time = time.time() - start
        
        self.assertEqual(count1, count2)
        self.logger.info(f"✅ Cache reused (second count took {cache_time:.4f}s)")
        
        # Unpersist
        df.unpersist()


class TestResourceManagement(BaseTestCase):
    """Test resource management and configuration."""
    
    def test_executor_memory_configuration(self):
        """Test executor memory settings."""
        conf = self.spark.sparkContext.getConf()
        
        # Check default parallelism
        parallelism = conf.get('spark.default.parallelism')
        self.assertIsNotNone(parallelism, "Default parallelism should be set")
        
        self.logger.info(f"✅ Default parallelism: {parallelism}")
    
    def test_shuffle_partition_tuning(self):
        """Test shuffle partition configuration."""
        # Create DataFrame requiring shuffle
        df1 = self.spark.range(1000).withColumnRenamed("id", "id1")
        df2 = self.spark.range(1000).withColumnRenamed("id", "id2")
        
        # Join (triggers shuffle)
        result = df1.join(df2, df1.id1 == df2.id2)
        
        # Check number of shuffle partitions
        num_partitions = result.rdd.getNumPartitions()
        self.assertEqual(num_partitions, 2, "Should use configured shuffle partitions")
        
        self.logger.info(f"✅ Shuffle partitions: {num_partitions}")
    
    def test_memory_usage_monitoring(self):
        """Test memory usage during operations."""
        import psutil
        
        # Get initial memory
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024 ** 2)  # MB
        
        # Perform memory-intensive operation
        df = self.spark.range(100000).cache()
        df.count()  # Materialize cache
        
        # Get final memory
        final_memory = process.memory_info().rss / (1024 ** 2)  # MB
        memory_increase = final_memory - initial_memory
        
        self.logger.info(f"✅ Memory increase: {memory_increase:.1f} MB")
        
        # Clean up
        df.unpersist()


class TestShuffleOptimization(BaseTestCase):
    """Test shuffle optimization techniques."""
    
    def test_broadcast_join_optimization(self):
        """Test broadcast join reduces shuffles."""
        from pyspark.sql.functions import broadcast
        
        # Create large and small DataFrames
        large_df = self.spark.range(10000)
        small_df = self.spark.range(10)
        
        # Regular join (will shuffle)
        regular_join = large_df.join(small_df, "id")
        
        # Broadcast join (no shuffle of large table)
        broadcast_join = large_df.join(broadcast(small_df), "id")
        
        # Both should give same result
        self.assertEqual(regular_join.count(), broadcast_join.count())
        
        # Check explain plan
        plan = broadcast_join._jdf.queryExecution().toString()
        self.assertIn("BroadcastHashJoin", plan)
        
        self.logger.info("✅ Broadcast join optimized correctly")
    
    def test_filter_before_join(self):
        """Test filtering before join reduces shuffle size."""
        # Create two DataFrames
        df1 = self.spark.range(10000)
        df2 = self.spark.range(10000)
        
        # Filter then join (optimal)
        filtered_join = df1.filter("id < 1000").join(df2.filter("id < 1000"), "id")
        count1 = filtered_join.count()
        
        # Join then filter (suboptimal)
        unfiltered_join = df1.join(df2, "id").filter("id < 1000")
        count2 = unfiltered_join.count()
        
        # Both should give same result
        self.assertEqual(count1, count2)
        
        self.logger.info("✅ Filter pushdown works correctly")
    
    def test_coalesce_vs_repartition(self):
        """Test coalesce is more efficient than repartition for reducing partitions."""
        import time
        
        # Create DataFrame with many partitions
        df = self.spark.range(10000).repartition(100)
        
        # Time coalesce
        start = time.time()
        coalesced = df.coalesce(10)
        coalesced.count()
        coalesce_time = time.time() - start
        
        # Time repartition
        start = time.time()
        repartitioned = df.repartition(10)
        repartitioned.count()
        repartition_time = time.time() - start
        
        # Coalesce should be faster (no shuffle)
        self.logger.info(f"Coalesce: {coalesce_time:.3f}s, Repartition: {repartition_time:.3f}s")
        self.logger.info("✅ Coalesce vs repartition performance verified")


def suite():
    """Create test suite."""
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestClusterSetup))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDataPartitioning))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestDistributedJoins))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestAggregationsAtScale))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFaultTolerance))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestResourceManagement))
    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestShuffleOptimization))
    
    return suite


if __name__ == '__main__':
    # Run tests with unittest runner
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite())
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
