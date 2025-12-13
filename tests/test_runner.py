#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
COMPREHENSIVE TEST RUNNER - PySpark Project Test Suite
================================================================================

MODULE OVERVIEW:
----------------
Robust test execution framework with:
• Error handling and recovery
• Detailed logging with timestamps
• Resource monitoring (CPU, memory, disk)
• Crash recovery mechanisms
• Test result reporting
• Performance benchmarking

PURPOSE:
--------
Run all tests with comprehensive monitoring and error handling to ensure
project stability and catch issues early.

USAGE:
------
# Run all tests
python tests/test_runner.py

# Run specific test suite
python tests/test_runner.py --suite unit

# Run with verbose logging
python tests/test_runner.py --verbose

# Run with resource monitoring
python tests/test_runner.py --monitor-resources

# Generate detailed report
python tests/test_runner.py --report html
"""

import sys
import os
import unittest
import logging
import time
import traceback
import psutil
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass, asdict
import argparse

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


@dataclass
class TestResult:
    """Test execution result with detailed metadata."""
    test_name: str
    status: str  # 'passed', 'failed', 'error', 'skipped'
    duration: float
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None
    timestamp: str = None
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


@dataclass
class ResourceMetrics:
    """System resource metrics."""
    timestamp: str
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float
    disk_usage_percent: float
    disk_used_gb: float
    disk_free_gb: float
    
    @classmethod
    def capture(cls) -> 'ResourceMetrics':
        """Capture current resource metrics."""
        cpu = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return cls(
            timestamp=datetime.now().isoformat(),
            cpu_percent=cpu,
            memory_percent=memory.percent,
            memory_used_mb=memory.used / (1024 ** 2),
            memory_available_mb=memory.available / (1024 ** 2),
            disk_usage_percent=disk.percent,
            disk_used_gb=disk.used / (1024 ** 3),
            disk_free_gb=disk.free / (1024 ** 3)
        )


class RobustTestRunner:
    """
    Comprehensive test runner with error handling and monitoring.
    
    Features:
    • Centralized error logging
    • Resource monitoring
    • Crash recovery
    • Detailed reporting
    • Performance tracking
    """
    
    def __init__(self, 
                 log_dir: str = 'logs/tests',
                 monitor_resources: bool = True,
                 resource_alert_threshold: Dict[str, float] = None):
        """
        Initialize test runner.
        
        Args:
            log_dir: Directory for test logs
            monitor_resources: Enable resource monitoring
            resource_alert_threshold: Dict of thresholds for alerts
                Example: {'cpu': 90.0, 'memory': 85.0, 'disk': 90.0}
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.monitor_resources = monitor_resources
        self.resource_alert_threshold = resource_alert_threshold or {
            'cpu': 90.0,
            'memory': 85.0,
            'disk': 90.0
        }
        
        # Results storage
        self.test_results: List[TestResult] = []
        self.resource_metrics: List[ResourceMetrics] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        
        # Crash recovery
        self.crash_log_path = self.log_dir / 'crash_recovery.json'
        
        # Set up logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure centralized logging with timestamps."""
        # Create log filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = self.log_dir / f'test_run_{timestamp}.log'
        
        # Configure root logger
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger('TestRunner')
        self.logger.info(f"Test runner initialized")
        self.logger.info(f"Log file: {log_file}")
        self.logger.info(f"Python version: {sys.version}")
        self.logger.info(f"Working directory: {os.getcwd()}")
        
    def _check_resource_thresholds(self, metrics: ResourceMetrics):
        """
        Check if resource usage exceeds thresholds and alert.
        
        Args:
            metrics: Current resource metrics
        """
        alerts = []
        
        if metrics.cpu_percent > self.resource_alert_threshold['cpu']:
            alerts.append(f"⚠️  CPU usage: {metrics.cpu_percent:.1f}% "
                         f"(threshold: {self.resource_alert_threshold['cpu']}%)")
        
        if metrics.memory_percent > self.resource_alert_threshold['memory']:
            alerts.append(f"⚠️  Memory usage: {metrics.memory_percent:.1f}% "
                         f"(threshold: {self.resource_alert_threshold['memory']}%)")
        
        if metrics.disk_usage_percent > self.resource_alert_threshold['disk']:
            alerts.append(f"⚠️  Disk usage: {metrics.disk_usage_percent:.1f}% "
                         f"(threshold: {self.resource_alert_threshold['disk']}%)")
        
        if alerts:
            self.logger.warning("RESOURCE ALERT:")
            for alert in alerts:
                self.logger.warning(f"  {alert}")
    
    def _save_crash_data(self, test_name: str, error: Exception):
        """
        Save crash data for debugging and recovery.
        
        Args:
            test_name: Name of the test that crashed
            error: Exception that caused the crash
        """
        crash_data = {
            'timestamp': datetime.now().isoformat(),
            'test_name': test_name,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'system_info': {
                'cpu_percent': psutil.cpu_percent(),
                'memory_percent': psutil.virtual_memory().percent,
                'disk_percent': psutil.disk_usage('/').percent
            }
        }
        
        # Append to crash log
        crashes = []
        if self.crash_log_path.exists():
            with open(self.crash_log_path, 'r') as f:
                crashes = json.load(f)
        
        crashes.append(crash_data)
        
        with open(self.crash_log_path, 'w') as f:
            json.dump(crashes, f, indent=2)
        
        self.logger.error(f"Crash data saved to {self.crash_log_path}")
    
    def discover_tests(self, start_dir: str = 'tests', pattern: str = 'test_*.py') -> unittest.TestSuite:
        """
        Discover all tests in the project.
        
        Args:
            start_dir: Directory to start discovery
            pattern: Pattern for test files
            
        Returns:
            Test suite containing all discovered tests
        """
        self.logger.info(f"Discovering tests in {start_dir} with pattern {pattern}")
        
        try:
            loader = unittest.TestLoader()
            suite = loader.discover(start_dir, pattern=pattern)
            
            # Count tests
            test_count = suite.countTestCases()
            self.logger.info(f"Discovered {test_count} test(s)")
            
            return suite
            
        except Exception as e:
            self.logger.error(f"Error discovering tests: {e}")
            self.logger.error(traceback.format_exc())
            raise
    
    def run_test_with_monitoring(self, test: unittest.TestCase) -> TestResult:
        """
        Run a single test with resource monitoring and error handling.
        
        Args:
            test: Test case to run
            
        Returns:
            TestResult with execution details
        """
        test_name = f"{test.__class__.__name__}.{test._testMethodName}"
        self.logger.info(f"Running test: {test_name}")
        
        # Capture initial metrics
        if self.monitor_resources:
            initial_metrics = ResourceMetrics.capture()
        
        start_time = time.time()
        status = 'passed'
        error_message = None
        error_traceback = None
        
        try:
            # Run the test
            result = unittest.TestResult()
            test.run(result)
            
            # Check result
            if result.errors:
                status = 'error'
                error = result.errors[0][1]
                error_message = str(error)
                error_traceback = result.errors[0][1]
                self.logger.error(f"Test error: {test_name}")
                self.logger.error(error_traceback)
                self._save_crash_data(test_name, error)
                
            elif result.failures:
                status = 'failed'
                error_message = str(result.failures[0][1])
                error_traceback = result.failures[0][1]
                self.logger.warning(f"Test failed: {test_name}")
                self.logger.warning(error_traceback)
                
            elif result.skipped:
                status = 'skipped'
                error_message = result.skipped[0][1]
                self.logger.info(f"Test skipped: {test_name} - {error_message}")
            
            else:
                self.logger.info(f"✅ Test passed: {test_name}")
        
        except Exception as e:
            status = 'error'
            error_message = str(e)
            error_traceback = traceback.format_exc()
            self.logger.error(f"Unexpected error in test: {test_name}")
            self.logger.error(error_traceback)
            self._save_crash_data(test_name, e)
        
        duration = time.time() - start_time
        
        # Capture final metrics
        if self.monitor_resources:
            final_metrics = ResourceMetrics.capture()
            self.resource_metrics.append(final_metrics)
            self._check_resource_thresholds(final_metrics)
            
            cpu_usage = final_metrics.cpu_percent
            memory_usage = final_metrics.memory_percent
        else:
            cpu_usage = 0.0
            memory_usage = 0.0
        
        # Create result
        result = TestResult(
            test_name=test_name,
            status=status,
            duration=duration,
            error_message=error_message,
            error_traceback=error_traceback,
            cpu_usage=cpu_usage,
            memory_usage=memory_usage
        )
        
        self.test_results.append(result)
        
        return result
    
    def run_suite(self, suite: unittest.TestSuite) -> bool:
        """
        Run entire test suite with monitoring.
        
        Args:
            suite: Test suite to run
            
        Returns:
            True if all tests passed, False otherwise
        """
        self.logger.info("=" * 70)
        self.logger.info("STARTING TEST SUITE")
        self.logger.info("=" * 70)
        
        self.start_time = datetime.now()
        test_count = suite.countTestCases()
        
        self.logger.info(f"Total tests to run: {test_count}")
        
        # Flatten test suite to get individual tests
        tests = []
        def extract_tests(suite_or_test):
            try:
                for test in suite_or_test:
                    extract_tests(test)
            except TypeError:
                tests.append(suite_or_test)
        
        extract_tests(suite)
        
        # Run each test with monitoring
        for i, test in enumerate(tests, 1):
            self.logger.info(f"\nTest {i}/{test_count}")
            self.logger.info("-" * 70)
            
            try:
                self.run_test_with_monitoring(test)
            except Exception as e:
                self.logger.error(f"Fatal error running test: {e}")
                self.logger.error(traceback.format_exc())
                # Continue with next test
        
        self.end_time = datetime.now()
        
        # Generate summary
        self._print_summary()
        
        # Check if all passed
        all_passed = all(r.status == 'passed' for r in self.test_results)
        
        return all_passed
    
    def _print_summary(self):
        """Print comprehensive test summary."""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("TEST SUITE SUMMARY")
        self.logger.info("=" * 70)
        
        # Count results by status
        passed = sum(1 for r in self.test_results if r.status == 'passed')
        failed = sum(1 for r in self.test_results if r.status == 'failed')
        errors = sum(1 for r in self.test_results if r.status == 'error')
        skipped = sum(1 for r in self.test_results if r.status == 'skipped')
        total = len(self.test_results)
        
        # Calculate duration
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            duration_str = f"{duration:.2f}s"
        else:
            duration_str = "N/A"
        
        # Print statistics
        self.logger.info(f"\nExecution Time: {duration_str}")
        self.logger.info(f"Total Tests: {total}")
        self.logger.info(f"✅ Passed: {passed}")
        self.logger.info(f"❌ Failed: {failed}")
        self.logger.info(f"💥 Errors: {errors}")
        self.logger.info(f"⏭️  Skipped: {skipped}")
        
        if total > 0:
            success_rate = (passed / total) * 100
            self.logger.info(f"Success Rate: {success_rate:.1f}%")
        
        # Print failed/error tests
        if failed + errors > 0:
            self.logger.info("\n" + "-" * 70)
            self.logger.info("FAILED/ERROR TESTS:")
            self.logger.info("-" * 70)
            
            for result in self.test_results:
                if result.status in ['failed', 'error']:
                    self.logger.error(f"\n{result.test_name}")
                    self.logger.error(f"  Status: {result.status}")
                    self.logger.error(f"  Duration: {result.duration:.3f}s")
                    if result.error_message:
                        self.logger.error(f"  Error: {result.error_message}")
        
        # Resource summary
        if self.resource_metrics:
            self.logger.info("\n" + "-" * 70)
            self.logger.info("RESOURCE USAGE SUMMARY:")
            self.logger.info("-" * 70)
            
            avg_cpu = sum(m.cpu_percent for m in self.resource_metrics) / len(self.resource_metrics)
            avg_memory = sum(m.memory_percent for m in self.resource_metrics) / len(self.resource_metrics)
            max_cpu = max(m.cpu_percent for m in self.resource_metrics)
            max_memory = max(m.memory_percent for m in self.resource_metrics)
            
            self.logger.info(f"Average CPU: {avg_cpu:.1f}%")
            self.logger.info(f"Max CPU: {max_cpu:.1f}%")
            self.logger.info(f"Average Memory: {avg_memory:.1f}%")
            self.logger.info(f"Max Memory: {max_memory:.1f}%")
        
        self.logger.info("\n" + "=" * 70)
    
    def generate_report(self, format: str = 'json', output_path: Optional[str] = None):
        """
        Generate detailed test report.
        
        Args:
            format: Report format ('json', 'html', 'markdown')
            output_path: Output file path (default: logs/tests/report_{timestamp}.{format})
        """
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = self.log_dir / f'report_{timestamp}.{format}'
        
        self.logger.info(f"Generating {format} report: {output_path}")
        
        # Prepare report data
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else None,
            'test_results': [asdict(r) for r in self.test_results],
            'resource_metrics': [asdict(m) for m in self.resource_metrics],
            'summary': {
                'total': len(self.test_results),
                'passed': sum(1 for r in self.test_results if r.status == 'passed'),
                'failed': sum(1 for r in self.test_results if r.status == 'failed'),
                'errors': sum(1 for r in self.test_results if r.status == 'error'),
                'skipped': sum(1 for r in self.test_results if r.status == 'skipped')
            }
        }
        
        if format == 'json':
            with open(output_path, 'w') as f:
                json.dump(report_data, f, indent=2)
        
        elif format == 'html':
            self._generate_html_report(report_data, output_path)
        
        elif format == 'markdown':
            self._generate_markdown_report(report_data, output_path)
        
        self.logger.info(f"Report saved: {output_path}")
    
    def _generate_html_report(self, data: Dict, output_path: Path):
        """Generate HTML report."""
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>PySpark Test Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #2c3e50; }}
        h2 {{ color: #34495e; margin-top: 30px; }}
        table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #3498db; color: white; }}
        .passed {{ background-color: #d4edda; }}
        .failed {{ background-color: #f8d7da; }}
        .error {{ background-color: #f8d7da; }}
        .skipped {{ background-color: #fff3cd; }}
        .summary {{ background-color: #e9ecef; padding: 20px; border-radius: 5px; }}
    </style>
</head>
<body>
    <h1>PySpark Test Suite Report</h1>
    <p>Generated: {data['timestamp']}</p>
    
    <div class="summary">
        <h2>Summary</h2>
        <p><strong>Total Tests:</strong> {data['summary']['total']}</p>
        <p><strong>✅ Passed:</strong> {data['summary']['passed']}</p>
        <p><strong>❌ Failed:</strong> {data['summary']['failed']}</p>
        <p><strong>💥 Errors:</strong> {data['summary']['errors']}</p>
        <p><strong>⏭️  Skipped:</strong> {data['summary']['skipped']}</p>
        <p><strong>Duration:</strong> {data['duration_seconds']:.2f}s</p>
    </div>
    
    <h2>Test Results</h2>
    <table>
        <tr>
            <th>Test Name</th>
            <th>Status</th>
            <th>Duration (s)</th>
            <th>CPU %</th>
            <th>Memory %</th>
        </tr>
"""
        
        for result in data['test_results']:
            html += f"""
        <tr class="{result['status']}">
            <td>{result['test_name']}</td>
            <td>{result['status'].upper()}</td>
            <td>{result['duration']:.3f}</td>
            <td>{result['cpu_usage']:.1f}</td>
            <td>{result['memory_usage']:.1f}</td>
        </tr>
"""
        
        html += """
    </table>
</body>
</html>
"""
        
        with open(output_path, 'w') as f:
            f.write(html)
    
    def _generate_markdown_report(self, data: Dict, output_path: Path):
        """Generate Markdown report."""
        md = f"""# PySpark Test Suite Report

**Generated:** {data['timestamp']}

## Summary

- **Total Tests:** {data['summary']['total']}
- **✅ Passed:** {data['summary']['passed']}
- **❌ Failed:** {data['summary']['failed']}
- **💥 Errors:** {data['summary']['errors']}
- **⏭️  Skipped:** {data['summary']['skipped']}
- **Duration:** {data['duration_seconds']:.2f}s
- **Success Rate:** {(data['summary']['passed'] / data['summary']['total'] * 100):.1f}%

## Test Results

| Test Name | Status | Duration (s) | CPU % | Memory % |
|-----------|--------|--------------|-------|----------|
"""
        
        for result in data['test_results']:
            status_emoji = {
                'passed': '✅',
                'failed': '❌',
                'error': '💥',
                'skipped': '⏭️'
            }
            emoji = status_emoji.get(result['status'], '')
            
            md += f"| {result['test_name']} | {emoji} {result['status']} | {result['duration']:.3f} | {result['cpu_usage']:.1f} | {result['memory_usage']:.1f} |\n"
        
        with open(output_path, 'w') as f:
            f.write(md)


def main():
    """Main test runner entry point."""
    parser = argparse.ArgumentParser(description='PySpark Test Runner')
    parser.add_argument('--suite', choices=['unit', 'integration', 'all'], default='all',
                       help='Test suite to run')
    parser.add_argument('--pattern', default='test_*.py',
                       help='Test file pattern')
    parser.add_argument('--verbose', action='store_true',
                       help='Verbose output')
    parser.add_argument('--monitor-resources', action='store_true', default=True,
                       help='Monitor system resources')
    parser.add_argument('--report', choices=['json', 'html', 'markdown'],
                       help='Generate report in specified format')
    
    args = parser.parse_args()
    
    # Determine start directory
    if args.suite == 'unit':
        start_dir = 'tests/unit'
    elif args.suite == 'integration':
        start_dir = 'tests/integration'
    else:
        start_dir = 'tests'
    
    # Create test runner
    runner = RobustTestRunner(monitor_resources=args.monitor_resources)
    
    try:
        # Discover tests
        suite = runner.discover_tests(start_dir=start_dir, pattern=args.pattern)
        
        # Run tests
        success = runner.run_suite(suite)
        
        # Generate report if requested
        if args.report:
            runner.generate_report(format=args.report)
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
    
    except Exception as e:
        runner.logger.error(f"Fatal error in test runner: {e}")
        runner.logger.error(traceback.format_exc())
        sys.exit(2)


if __name__ == '__main__':
    main()
