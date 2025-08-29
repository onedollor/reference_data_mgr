#!/bin/bash

# Reference Data Manager - Test Runner Script
# Runs comprehensive test suite with coverage reporting

echo "🧪 Starting Reference Data Manager Test Suite..."
echo "================================================="

# Check if pytest is installed
if ! python3 -c "import pytest" 2>/dev/null; then
    echo "❌ pytest not found. Installing test dependencies..."
    pip3 install -r requirements.txt
fi

# Set PYTHONPATH to include current directory
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# Clean previous coverage data
echo "🧹 Cleaning previous coverage data..."
rm -rf coverage_html/
rm -f .coverage
rm -f coverage.xml

# Run tests with coverage
echo "🚀 Running tests with coverage..."
python3 -m pytest tests/ \
    --cov=. \
    --cov-report=html:coverage_html \
    --cov-report=term-missing \
    --cov-report=xml \
    --cov-exclude=tests/* \
    --cov-exclude=venv/* \
    --cov-exclude=*/venv/* \
    -v \
    --tb=short

TEST_RESULT=$?

# Display results
echo ""
echo "================================================="
if [ $TEST_RESULT -eq 0 ]; then
    echo "✅ All tests passed!"
else
    echo "❌ Some tests failed. Check output above."
fi

echo ""
echo "📊 Coverage Report Generated:"
echo "   - HTML Report: coverage_html/index.html"
echo "   - XML Report: coverage.xml"
echo ""

# Open coverage report if available
if command -v xdg-open > /dev/null && [ -f coverage_html/index.html ]; then
    echo "🌐 Opening coverage report in browser..."
    xdg-open coverage_html/index.html
elif command -v open > /dev/null && [ -f coverage_html/index.html ]; then
    echo "🌐 Opening coverage report in browser..."
    open coverage_html/index.html
fi

exit $TEST_RESULT