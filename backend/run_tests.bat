@echo off
REM Reference Data Manager - Test Runner Script (Windows)
REM Runs comprehensive test suite with coverage reporting

echo 🧪 Starting Reference Data Manager Test Suite...
echo =================================================

REM Check if pytest is installed
python -c "import pytest" 2>nul
if %errorlevel% neq 0 (
    echo ❌ pytest not found. Installing test dependencies...
    pip install -r requirements.txt
)

REM Set PYTHONPATH to include current directory
set PYTHONPATH=%PYTHONPATH%;%CD%

REM Clean previous coverage data
echo 🧹 Cleaning previous coverage data...
if exist coverage_html rmdir /s /q coverage_html
if exist .coverage del .coverage
if exist coverage.xml del coverage.xml

REM Run tests with coverage
echo 🚀 Running tests with coverage...
python -m pytest tests/ ^
    --cov=. ^
    --cov-report=html:coverage_html ^
    --cov-report=term-missing ^
    --cov-report=xml ^
    --cov-exclude=tests/* ^
    --cov-exclude=venv/* ^
    --cov-exclude=*/venv/* ^
    -v ^
    --tb=short

set TEST_RESULT=%errorlevel%

REM Display results
echo.
echo =================================================
if %TEST_RESULT% equ 0 (
    echo ✅ All tests passed!
) else (
    echo ❌ Some tests failed. Check output above.
)

echo.
echo 📊 Coverage Report Generated:
echo    - HTML Report: coverage_html\index.html
echo    - XML Report: coverage.xml
echo.

REM Open coverage report if available
if exist coverage_html\index.html (
    echo 🌐 Opening coverage report in browser...
    start "" coverage_html\index.html
)

exit /b %TEST_RESULT%