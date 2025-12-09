#!/usr/bin/env python3
"""
Run All UDF Examples
====================

Quick demo script to run all UDF inference examples.

Usage:
    python run_all_examples.py
    
    # Or run individually:
    python run_all_examples.py --example 1
"""

import sys
import argparse
import subprocess


EXAMPLES = {
    1: {
        'name': 'Basic Batch Inference',
        'file': '01_basic_batch_inference.py',
        'description': 'Simple PyTorch model inference with Pandas UDF'
    },
    2: {
        'name': 'Anomaly Detection',
        'file': '02_anomaly_detection_udf.py',
        'description': 'Isolation Forest for sensor anomaly detection'
    },
    3: {
        'name': 'Multi-Class Classification',
        'file': '03_classification_udf.py',
        'description': 'Classification with confidence scores'
    },
    4: {
        'name': 'Time Series Forecasting',
        'file': '04_time_series_forecast_udf.py',
        'description': 'LSTM-based forecasting'
    },
    5: {
        'name': 'Sentiment Analysis',
        'file': '05_sentiment_analysis_udf.py',
        'description': 'NLP sentiment scoring on text reviews'
    },
    7: {
        'name': 'Fraud Detection',
        'file': '07_fraud_detection_udf.py',
        'description': 'Real-time transaction fraud detection'
    }
}


def print_banner(text):
    """Print formatted banner"""
    width = 70
    print("\n" + "="*width)
    print(f" {text}")
    print("="*width + "\n")


def run_example(example_num):
    """Run a specific example"""
    if example_num not in EXAMPLES:
        print(f"❌ Example {example_num} not found!")
        return False
    
    example = EXAMPLES[example_num]
    
    print_banner(f"Example {example_num}: {example['name']}")
    print(f"Description: {example['description']}")
    print(f"File: {example['file']}\n")
    
    try:
        result = subprocess.run(
            ['python', example['file']],
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            print(f"\n✅ Example {example_num} completed successfully!\n")
            return True
        else:
            print(f"\n❌ Example {example_num} failed with code {result.returncode}\n")
            return False
    
    except Exception as e:
        print(f"\n❌ Error running example {example_num}: {e}\n")
        return False


def run_all_examples():
    """Run all examples sequentially"""
    print_banner("Running All UDF Inference Examples")
    
    results = {}
    
    for num in sorted(EXAMPLES.keys()):
        success = run_example(num)
        results[num] = success
        
        if not success:
            print("\n⚠️  Example failed. Continue? (y/n): ", end='')
            response = input().strip().lower()
            if response != 'y':
                print("\n🛑 Stopping execution.")
                break
    
    # Summary
    print_banner("Summary")
    
    total = len(results)
    passed = sum(1 for success in results.values() if success)
    
    print(f"Total examples run: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {total - passed}\n")
    
    for num, success in results.items():
        status = "✅" if success else "❌"
        print(f"{status} Example {num}: {EXAMPLES[num]['name']}")
    
    print()


def list_examples():
    """List all available examples"""
    print_banner("Available UDF Examples")
    
    for num in sorted(EXAMPLES.keys()):
        example = EXAMPLES[num]
        print(f"{num}. {example['name']}")
        print(f"   {example['description']}")
        print(f"   File: {example['file']}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Run UDF inference examples',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_all_examples.py               # Run all examples
  python run_all_examples.py --example 1   # Run example 1 only
  python run_all_examples.py --list        # List all examples
        """
    )
    
    parser.add_argument(
        '--example', '-e',
        type=int,
        help='Run specific example number (1-7)'
    )
    
    parser.add_argument(
        '--list', '-l',
        action='store_true',
        help='List all available examples'
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_examples()
    elif args.example:
        run_example(args.example)
    else:
        run_all_examples()


if __name__ == "__main__":
    main()
