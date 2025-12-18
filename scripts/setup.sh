#!/bin/bash

# PySpark ETL Environment Setup Script (Local Environment)

echo "=========================================="
echo "PySpark ETL Environment Setup"
echo "=========================================="

# Check Python version
echo ""
echo "Checking Python version..."
python3 --version

# Check if PySpark is installed
echo ""
echo "Checking PySpark installation..."
if python3 -c "import pyspark" 2>/dev/null; then
    echo "✓ PySpark is installed"
    python3 -c "from pyspark.sql import SparkSession; print('  Version:', SparkSession.builder.getOrCreate().version); SparkSession.builder.getOrCreate().stop()"
else
    echo "⚠️  PySpark not found. Install with:"
    echo "   pip install pyspark"
    exit 1
fi

# Check for required Python packages
echo ""
echo "Checking required packages..."
PACKAGES=("pandas" "numpy" "matplotlib" "jupyter")
MISSING=()

for pkg in "${PACKAGES[@]}"; do
    if python3 -c "import $pkg" 2>/dev/null; then
        echo "✓ $pkg installed"
    else
        echo "✗ $pkg not installed"
        MISSING+=("$pkg")
    fi
done

if [ ${#MISSING[@]} -gt 0 ]; then
    echo ""
    echo "⚠️  Missing packages: ${MISSING[*]}"
    echo "   Install with: pip install ${MISSING[*]}"
fi

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo ""
    echo "Creating .env file from template..."
    cp .env.template .env
    echo "✓ .env file created - please configure it"
else
    echo "✓ .env file already exists"
fi

# Create necessary directories
echo ""
echo "Checking directory structure..."
DIRS=("data/raw" "data/processed" "data/sample" "logs" "notebooks/examples" "notebooks/practice")

for dir in "${DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "✓ Created $dir"
    fi
done

# Test Spark session
echo ""
echo "Testing Spark session..."
python3 -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[*]').appName('test').getOrCreate(); print('✓ Spark', spark.version, 'is ready!'); spark.stop()"

# Display next steps
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Configure your .env file with any needed settings"
echo ""
echo "2. Start practicing with notebooks:"
echo "   jupyter notebook"
echo "   # Then navigate to notebooks/practice/"
echo ""
echo "3. Run example scripts:"
echo "   spark-submit src/parquet/01_basic_parquet_operations.py"
echo "   spark-submit src/orc/01_basic_orc_operations.py"
echo "   spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.0 src/avro/01_basic_avro_operations.py"
echo ""
echo "4. Run ETL examples:"
echo "   export PYTHONPATH=\"\${PYTHONPATH}:\$(pwd)/src\""
echo "   python src/etl/basic_etl_pipeline.py"
echo ""
echo "5. Explore file format examples:"
echo "   cd src/parquet && cat README.md"
echo "   cd src/avro && cat README.md"
echo "   cd src/orc && cat README.md"
echo ""
echo "Happy coding! 🚀"
