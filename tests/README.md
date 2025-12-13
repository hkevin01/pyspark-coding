# PySpark Project Test Suite

## Overview

Comprehensive test suite with robust error handling, resource monitoring, crash recovery, and detailed reporting for the entire PySpark project.

## Features

✅ **Robust Error Handling**
- Try-catch blocks for all test operations
- Descriptive error messages with timestamps
- Centralized error logging
- Crash data preservation

✅ **Resource Monitoring**
- CPU, memory, and disk usage tracking
- Real-time threshold alerts
- Performance metrics per test
- Resource usage trends

✅ **Crash Recovery**
- Automatic crash data capture
- Recovery mechanisms
- Crash log preservation
- Debugging information storage

✅ **Detailed Reporting**
- JSON, HTML, and Markdown formats
- Test execution metrics
- Resource usage statistics
- Success/failure analysis

## Directory Structure

```
tests/
├── README.md                    # This file
├── __init__.py                  # Package initialization
├── test_runner.py              # Comprehensive test runner
├── unit/                       # Unit tests
│   ├── __init__.py
│   └── test_cluster_computing.py  # Cluster computing tests
└── integration/                # Integration tests
    └── __init__.py
```

## Installation

### Required Dependencies

```bash
# Core testing dependencies
pip install psutil pytest pytest-cov

# PySpark (already installed)
pip install pyspark

# Optional: For advanced reporting
pip install pytest-html pytest-json-report
```

## Usage

### Run All Tests

```bash
# Using test runner (recommended)
python tests/test_runner.py

# Using unittest
python -m unittest discover tests

# Using pytest
pytest tests/
```

### Run Specific Test Suites

```bash
# Unit tests only
python tests/test_runner.py --suite unit

# Integration tests only
python tests/test_runner.py --suite integration

# Specific test file
python tests/unit/test_cluster_computing.py
```

### Generate Reports

```bash
# JSON report
python tests/test_runner.py --report json

# HTML report (visual dashboard)
python tests/test_runner.py --report html

# Markdown report
python tests/test_runner.py --report markdown
```

### Monitor Resources

```bash
# Enable resource monitoring (default: enabled)
python tests/test_runner.py --monitor-resources

# With custom thresholds
# Edit test_runner.py to adjust thresholds
```

### Verbose Mode

```bash
# Detailed logging output
python tests/test_runner.py --verbose
```

## Test Categories

### 1. Cluster Computing Tests (`test_cluster_computing.py`)

#### TestClusterSetup
- ✅ Spark session creation
- ✅ Version verification
- ✅ Configuration validation
- ✅ Error handling for invalid masters

#### TestDataPartitioning
- ✅ Repartition operations
- ✅ Coalesce operations
- ✅ Column-based partitioning
- ✅ Partition size optimization
- ✅ Data distribution balance

#### TestDistributedJoins
- ✅ Basic joins (inner, left, right, outer)
- ✅ Broadcast join optimization
- ✅ Join performance comparison
- ✅ Error handling for missing columns

#### TestAggregationsAtScale
- ✅ Basic aggregations (sum, avg, count, min, max)
- ✅ Window functions
- ✅ Performance on large datasets
- ✅ Group by operations

#### TestFaultTolerance
- ✅ Task failure recovery
- ✅ Checkpointing
- ✅ Cache persistence
- ✅ Error recovery mechanisms

#### TestResourceManagement
- ✅ Executor memory configuration
- ✅ Shuffle partition tuning
- ✅ Memory usage monitoring
- ✅ Resource allocation

#### TestShuffleOptimization
- ✅ Broadcast join optimization
- ✅ Filter pushdown
- ✅ Coalesce vs repartition performance
- ✅ Shuffle reduction techniques

## Error Handling

### Centralized Logging

All errors are logged with:
- **Timestamp**: Exact time of error
- **Method name**: Which test failed
- **Input data**: Data that caused the error
- **Stack trace**: Full error traceback
- **System metrics**: CPU/memory at time of error

### Example Log Entry

```
2025-12-13 10:45:23 | TestRunner | ERROR | Test error: TestDistributedJoins.test_broadcast_join
2025-12-13 10:45:23 | TestRunner | ERROR |   Status: error
2025-12-13 10:45:23 | TestRunner | ERROR |   Duration: 2.345s
2025-12-13 10:45:23 | TestRunner | ERROR |   Error: Column 'id' not found
2025-12-13 10:45:23 | TestRunner | ERROR |   CPU: 45.2%, Memory: 62.1%
2025-12-13 10:45:23 | TestRunner | ERROR |   Traceback:
  File "test_cluster_computing.py", line 234, in test_broadcast_join
    result = df1.join(df2, "nonexistent_id")
  AnalysisException: Column 'nonexistent_id' not found
```

### Crash Recovery

When a test crashes:
1. **Crash data saved** to `logs/tests/crash_recovery.json`
2. **System state captured**: CPU, memory, disk usage
3. **Full traceback preserved** for debugging
4. **Test continues** with next test
5. **Summary includes crash info**

### Crash Recovery File Format

```json
{
  "timestamp": "2025-12-13T10:45:23.456789",
  "test_name": "TestDistributedJoins.test_broadcast_join",
  "error_type": "AnalysisException",
  "error_message": "Column 'id' not found",
  "traceback": "Full traceback here...",
  "system_info": {
    "cpu_percent": 45.2,
    "memory_percent": 62.1,
    "disk_percent": 78.5
  }
}
```

## Resource Monitoring

### Monitored Metrics

- **CPU Usage**: Percentage utilization
- **Memory Usage**: Used/available RAM
- **Disk Usage**: Used/free disk space
- **Per-Test Metrics**: Resource usage per test

### Alert Thresholds

Default thresholds (configurable):
- CPU: 90%
- Memory: 85%
- Disk: 90%

### Example Alert

```
2025-12-13 10:45:23 | TestRunner | WARNING | RESOURCE ALERT:
2025-12-13 10:45:23 | TestRunner | WARNING |   ⚠️  CPU usage: 92.5% (threshold: 90.0%)
2025-12-13 10:45:23 | TestRunner | WARNING |   ⚠️  Memory usage: 87.3% (threshold: 85.0%)
```

## Reports

### JSON Report

Machine-readable format with all test data:

```json
{
  "timestamp": "2025-12-13T10:45:23.456789",
  "duration_seconds": 125.67,
  "summary": {
    "total": 50,
    "passed": 47,
    "failed": 2,
    "errors": 1,
    "skipped": 0
  },
  "test_results": [...],
  "resource_metrics": [...]
}
```

### HTML Report

Visual dashboard with:
- Summary statistics
- Test results table
- Color-coded status
- Resource usage graphs

### Markdown Report

Readable format for documentation:

| Test Name | Status | Duration | CPU % | Memory % |
|-----------|--------|----------|-------|----------|
| test_spark_session_creation | ✅ passed | 0.123 | 25.3 | 45.2 |
| test_broadcast_join | ✅ passed | 2.456 | 67.8 | 72.1 |

## Best Practices

### 1. Run Tests Before Committing

```bash
# Quick check
python tests/test_runner.py --suite unit

# Full test suite
python tests/test_runner.py
```

### 2. Monitor Resource Usage

```bash
# Check resource usage trends
python tests/test_runner.py --monitor-resources --report html
# Open logs/tests/report_*.html
```

### 3. Review Error Logs

```bash
# Check latest test run
cat logs/tests/test_run_*.log

# Check crash recovery data
cat logs/tests/crash_recovery.json
```

### 4. Clean Up Test Data

```bash
# Remove old logs (keep last 7 days)
find logs/tests -name "test_run_*.log" -mtime +7 -delete

# Clear crash recovery logs
rm logs/tests/crash_recovery.json
```

## Continuous Integration

### GitHub Actions Example

```yaml
name: Run Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install psutil pytest
      - name: Run tests
        run: |
          python tests/test_runner.py --report json
      - name: Upload test results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: logs/tests/
```

## Troubleshooting

### Issue: Tests Fail Due to Memory

**Solution**: Increase Spark memory or reduce test data size

```python
# In test setup
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```

### Issue: Slow Test Execution

**Solution**: Use fewer partitions in tests

```python
# Reduce parallelism for unit tests
spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()
```

### Issue: Crash Recovery Not Working

**Solution**: Check log directory permissions

```bash
# Ensure log directory is writable
mkdir -p logs/tests
chmod 755 logs/tests
```

## Contributing

### Adding New Tests

1. Create test file in `tests/unit/` or `tests/integration/`
2. Inherit from `BaseTestCase` for common setup
3. Use descriptive test names: `test_<feature>_<scenario>`
4. Include error handling tests
5. Add docstrings explaining the test
6. Run the full suite to ensure no regressions

### Test Naming Convention

```python
def test_<module>_<function>_<scenario>():
    """
    Test description.
    
    Tests that <module>.<function> correctly handles <scenario>.
    Verifies <expected behavior>.
    """
    # Test implementation
```

## Performance Benchmarks

Target execution times (on standard hardware):
- Unit test suite: < 5 minutes
- Integration test suite: < 15 minutes
- Full test suite: < 20 minutes

## License

Same as main project.

## Support

For issues or questions:
1. Check logs in `logs/tests/`
2. Review crash recovery data
3. Enable verbose mode for detailed output
4. Check resource monitoring for bottlenecks

## See Also

- Main project README: `../README.md`
- PySpark documentation: https://spark.apache.org/docs/latest/
- Python unittest: https://docs.python.org/3/library/unittest.html
