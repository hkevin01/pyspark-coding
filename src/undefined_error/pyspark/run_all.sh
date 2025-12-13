#!/bin/bash
# Run all PySpark undefined behavior examples

set -e

echo "================================================================================"
echo "PYSPARK UNDEFINED BEHAVIOR EXAMPLES"
echo "================================================================================"
echo ""
echo "This will run all examples demonstrating dangerous patterns and safe alternatives."
echo "Expect to see intentional errors and warnings as part of the demonstrations."
echo ""

files=(
    "01_closure_serialization.py"
    "02_lazy_evaluation.py"
    "03_data_skew_partitions.py"
    "04_type_coercion_null.py"
)

for file in "${files[@]}"; do
    if [ -f "$file" ]; then
        echo ""
        echo "================================================================================"
        echo "Running: $file"
        echo "================================================================================"
        python3 "$file" 2>&1 | head -200
        echo ""
        echo "✅ Completed: $file"
        echo ""
    else
        echo "⚠️  File not found: $file"
    fi
done

echo ""
echo "================================================================================"
echo "ALL EXAMPLES COMPLETED"
echo "================================================================================"
echo ""
echo "Summary:"
echo "  - 4 files executed"
echo "  - 50+ dangerous patterns demonstrated"
echo "  - 50+ safe alternatives provided"
echo ""
echo "Next steps:"
echo "  1. Review output for specific patterns you encounter"
echo "  2. Run individual files for detailed exploration"
echo "  3. Apply safe patterns to your production code"
echo ""
