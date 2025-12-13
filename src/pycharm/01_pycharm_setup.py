"""
01_pycharm_setup.py
===================

PyCharm Configuration for PySpark Development

This file demonstrates how to set up and configure PyCharm
for optimal PySpark development experience.
"""

import sys
import os
from pathlib import Path


def check_pycharm_environment():
    """
    Check if running in PyCharm and display environment info.
    """
    print("=" * 80)
    print("PYCHARM ENVIRONMENT CHECK")
    print("=" * 80)
    
    # Check if running in PyCharm
    is_pycharm = any('pycharm' in arg.lower() for arg in sys.argv) or \
                 'PYCHARM_HOSTED' in os.environ
    
    print(f"\n✓ Running in PyCharm: {is_pycharm}")
    print(f"✓ Python Version: {sys.version}")
    print(f"✓ Python Path: {sys.executable}")
    print(f"✓ Working Directory: {os.getcwd()}")
    
    # Display Python path
    print(f"\n📦 Python Path (sys.path):")
    for i, path in enumerate(sys.path[:5], 1):
        print(f"   {i}. {path}")
    if len(sys.path) > 5:
        print(f"   ... and {len(sys.path) - 5} more")


def configure_spark_in_pycharm():
    """
    Show how to configure Spark environment variables in PyCharm.
    
    In PyCharm:
    1. Run → Edit Configurations
    2. Select your configuration
    3. Environment Variables → Add:
       SPARK_HOME=/path/to/spark
       PYSPARK_PYTHON=python3
       PYSPARK_DRIVER_PYTHON=python3
    """
    print("\n" + "=" * 80)
    print("SPARK ENVIRONMENT CONFIGURATION")
    print("=" * 80)
    
    spark_vars = {
        'SPARK_HOME': os.environ.get('SPARK_HOME', 'NOT SET'),
        'PYSPARK_PYTHON': os.environ.get('PYSPARK_PYTHON', 'NOT SET'),
        'PYSPARK_DRIVER_PYTHON': os.environ.get('PYSPARK_DRIVER_PYTHON', 'NOT SET'),
        'JAVA_HOME': os.environ.get('JAVA_HOME', 'NOT SET')
    }
    
    print("\n🔧 Current Spark Environment Variables:")
    for var, value in spark_vars.items():
        status = "✅" if value != "NOT SET" else "❌"
        print(f"   {status} {var}: {value}")
    
    # Recommendations
    print("\n💡 PyCharm Configuration Guide:")
    print("""
    1. Run → Edit Configurations
    2. Select your Python configuration
    3. Click 'Environment Variables' (folder icon)
    4. Add the following variables:
       
       SPARK_HOME          = /path/to/spark-3.5.0
       PYSPARK_PYTHON      = python3
       PYSPARK_DRIVER_PYTHON = python3
       JAVA_HOME           = /path/to/java
    
    5. Click OK to save
    6. Restart your run configuration
    """)


def setup_runtime_arguments():
    """
    Demonstrate how to use runtime arguments in PyCharm.
    
    In PyCharm:
    1. Run → Edit Configurations
    2. Parameters field: --master local[*] --executor-memory 2g
    """
    print("\n" + "=" * 80)
    print("RUNTIME ARGUMENTS EXAMPLES")
    print("=" * 80)
    
    print("\n📋 Common PyCharm Runtime Arguments:")
    
    arguments = [
        ("--master local[*]", "Run Spark locally using all cores"),
        ("--master local[4]", "Run Spark locally with 4 cores"),
        ("--executor-memory 2g", "Allocate 2GB per executor"),
        ("--driver-memory 4g", "Allocate 4GB for driver"),
        ("--conf spark.sql.shuffle.partitions=8", "Set shuffle partitions"),
        ("--input data/input.csv", "Specify input file"),
        ("--output data/output/", "Specify output directory"),
    ]
    
    print("\nAdd these to 'Parameters' field in Run Configuration:\n")
    for arg, description in arguments:
        print(f"   {arg}")
        print(f"   └─ {description}\n")
    
    # Show how to access arguments
    print("📝 Access Arguments in Code:")
    print("""
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--master', default='local[*]')
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    
    # Use arguments
    spark = SparkSession.builder.master(args.master).getOrCreate()
    df = spark.read.csv(args.input)
    df.write.parquet(args.output)
    """)


def pycharm_debugging_tips():
    """
    Tips for debugging PySpark code in PyCharm.
    """
    print("\n" + "=" * 80)
    print("PYCHARM DEBUGGING TIPS FOR PYSPARK")
    print("=" * 80)
    
    print("""
    🐛 Debugging Techniques:
    
    1. Set Breakpoints:
       - Click left gutter next to line number
       - Or press Ctrl+F8 (Windows/Linux) / Cmd+F8 (Mac)
    
    2. Debug Mode:
       - Click bug icon or press Shift+F9
       - Step through transformations
       - Inspect DataFrame contents
    
    3. Evaluate Expressions:
       - While debugging, select code
       - Alt+F8 to evaluate
       - Check DataFrame schemas, counts
    
    4. Conditional Breakpoints:
       - Right-click breakpoint
       - Add condition (e.g., count > 100)
       - Only stops when condition is true
    
    5. DataFrame Inspection:
       # Add these lines while debugging
       df.printSchema()      # See structure
       df.show(5)            # See sample data
       df.count()            # Get row count
       df.explain()          # See execution plan
    
    6. Log Points:
       - Right-click breakpoint
       - Check "Evaluate and log"
       - No need to stop execution
    
    7. Remote Debugging (Cluster):
       - Add pydevd to cluster
       - Connect PyCharm debugger
       - Debug remote Spark jobs
    """)


def pycharm_productivity_shortcuts():
    """
    Essential PyCharm shortcuts for PySpark development.
    """
    print("\n" + "=" * 80)
    print("PYCHARM PRODUCTIVITY SHORTCUTS")
    print("=" * 80)
    
    shortcuts = {
        "Navigation": [
            ("Ctrl+N", "Find class"),
            ("Ctrl+Shift+N", "Find file"),
            ("Ctrl+B", "Go to declaration"),
            ("Alt+F7", "Find usages"),
        ],
        "Editing": [
            ("Ctrl+Space", "Code completion"),
            ("Ctrl+Shift+Enter", "Complete statement"),
            ("Ctrl+Alt+L", "Reformat code"),
            ("Ctrl+/", "Comment/uncomment line"),
        ],
        "Running & Debugging": [
            ("Shift+F10", "Run"),
            ("Shift+F9", "Debug"),
            ("Ctrl+F8", "Toggle breakpoint"),
            ("F8", "Step over"),
        ],
        "PySpark Specific": [
            ("Ctrl+Q", "Quick documentation (hover over function)"),
            ("Ctrl+P", "Parameter info (inside function call)"),
            ("Alt+Enter", "Show intention actions/quick fixes"),
        ]
    }
    
    for category, items in shortcuts.items():
        print(f"\n{category}:")
        for shortcut, description in items:
            print(f"   {shortcut:<20} → {description}")


def create_pycharm_project_structure():
    """
    Recommended PyCharm project structure for PySpark.
    """
    print("\n" + "=" * 80)
    print("RECOMMENDED PYCHARM PROJECT STRUCTURE")
    print("=" * 80)
    
    structure = """
    pyspark-project/
    │
    ├── .idea/                    # PyCharm project settings (auto-generated)
    │   ├── workspace.xml
    │   └── misc.xml
    │
    ├── src/                      # Source code
    │   ├── __init__.py
    │   ├── jobs/                 # Spark job scripts
    │   │   ├── etl_pipeline.py
    │   │   └── ml_inference.py
    │   ├── transformations/      # Reusable transformations
    │   │   └── data_cleaning.py
    │   └── utils/                # Utility functions
    │       └── spark_session.py
    │
    ├── tests/                    # Unit tests
    │   ├── test_transformations.py
    │   └── test_utils.py
    │
    ├── notebooks/                # Jupyter notebooks
    │   └── exploration.ipynb
    │
    ├── data/                     # Local data (add to .gitignore)
    │   ├── input/
    │   └── output/
    │
    ├── config/                   # Configuration files
    │   ├── dev.yaml
    │   └── prod.yaml
    │
    ├── requirements.txt          # Python dependencies
    ├── setup.py                  # Package setup
    └── README.md                 # Project documentation
    
    💡 PyCharm Tips:
    - Mark 'src' as Sources Root (right-click → Mark Directory as)
    - Mark 'tests' as Test Sources Root
    - Add data/ to .gitignore
    - Use Project View (Alt+1) to navigate
    """
    print(structure)


def main():
    """
    Main execution function.
    """
    print("\n" + "🎯" * 40)
    print("PYCHARM FOR PYSPARK DEVELOPMENT")
    print("🎯" * 40)
    
    # Run all demonstrations
    check_pycharm_environment()
    configure_spark_in_pycharm()
    setup_runtime_arguments()
    pycharm_debugging_tips()
    pycharm_productivity_shortcuts()
    create_pycharm_project_structure()
    
    print("\n" + "=" * 80)
    print("✅ PYCHARM SETUP COMPLETE")
    print("=" * 80)
    print("\n📚 Next Steps:")
    print("   1. Configure environment variables")
    print("   2. Set up runtime arguments")
    print("   3. Practice debugging with breakpoints")
    print("   4. Use shortcuts to boost productivity")
    print("   5. Create proper project structure")


if __name__ == "__main__":
    main()
