#!/bin/bash

# PySpark ETL Environment Setup Script

echo "=========================================="
echo "PySpark ETL Environment Setup"
echo "=========================================="

# Check Python version
echo ""
echo "Checking Python version..."
python3 --version

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo ""
    echo "Creating virtual environment..."
    python3 -m venv venv
    echo "✓ Virtual environment created"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo ""
echo "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -r requirements.txt

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo ""
    echo "Creating .env file from template..."
    cp .env.template .env
    echo "✓ .env file created - please configure it"
else
    echo "✓ .env file already exists"
fi

# Test PySpark installation
echo ""
echo "Testing PySpark installation..."
python -c "from pyspark.sql import SparkSession; spark = SparkSession.builder.master('local[1]').appName('test').getOrCreate(); print('✓ Spark', spark.version, 'is ready!'); spark.stop()"

# Display next steps
echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Activate the virtual environment:"
echo "   source venv/bin/activate"
echo ""
echo "2. Configure your .env file with any needed settings"
echo ""
echo "3. Start practicing:"
echo "   jupyter notebook"
echo "   # Then navigate to notebooks/practice/"
echo ""
echo "4. Or run the example ETL pipeline:"
echo "   export PYTHONPATH=\"\${PYTHONPATH}:\$(pwd)/src\""
echo "   python src/etl/basic_etl_pipeline.py"
echo ""
echo "Happy coding! 🚀"
