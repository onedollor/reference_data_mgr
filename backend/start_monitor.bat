@echo off
REM Reference Data Manager - File Monitor Startup Script (Windows)
REM Starts the file monitoring service for automatic CSV processing

echo 📁 Starting Reference Data File Monitor...
echo ============================================

REM Check if required Python packages are installed
echo 🔍 Checking dependencies...
python -c "import pyodbc, pandas, dotenv" 2>nul
if %errorlevel% neq 0 (
    echo ❌ Required dependencies not found. Installing...
    pip install -r requirements.txt
    if %errorlevel% neq 0 (
        echo ❌ Failed to install dependencies. Please check requirements.txt
        pause
        exit /b 1
    )
)

REM Check for .env file
if not exist .env (
    echo ⚠️  Warning: .env file not found. Please ensure database configuration is set.
    echo    Required environment variables:
    echo    - db_host
    echo    - db_name  
    echo    - db_user
    echo    - db_password
    echo.
    set /p continue="Continue anyway? (y/N): "
    if /i not "%continue%"=="y" (
        echo Exiting...
        exit /b 1
    )
)

REM Set PYTHONPATH to include current directory
set PYTHONPATH=%PYTHONPATH%;%CD%

REM Create logs directory if it doesn't exist
if not exist logs mkdir logs

REM Create data directories if they don't exist
echo 📂 Setting up data directories...
if not exist data\reference_data\dropoff\reference_data_table\fullload mkdir data\reference_data\dropoff\reference_data_table\fullload
if not exist data\reference_data\dropoff\reference_data_table\append mkdir data\reference_data\dropoff\reference_data_table\append
if not exist data\reference_data\dropoff\none_reference_data_table\fullload mkdir data\reference_data\dropoff\none_reference_data_table\fullload
if not exist data\reference_data\dropoff\none_reference_data_table\append mkdir data\reference_data\dropoff\none_reference_data_table\append

REM Create processed and error subdirectories
for /d %%d in (data\reference_data\dropoff\*\*) do (
    if not exist "%%d\processed" mkdir "%%d\processed"
    if not exist "%%d\error" mkdir "%%d\error"
)

echo ✅ Directory structure verified.
echo.

REM Test database connection
echo 🔧 Testing database connection...
python -c "from utils.database import DatabaseManager; db = DatabaseManager(); result = db.test_connection(); print('✅ Database connection successful!' if result.get('status') == 'success' else f'❌ Database connection failed: {result.get(\"error\", \"Unknown error\")}')"

if %errorlevel% neq 0 (
    echo ❌ Database connection test failed. Please check your .env configuration.
    set /p continue="Start monitor anyway? (y/N): "
    if /i not "%continue%"=="y" (
        echo Exiting...
        exit /b 1
    )
)

echo.
echo 🚀 Starting File Monitor Service...
echo ===================================
echo 📋 Monitor Configuration:
echo    - Dropoff Path: data\reference_data\dropoff
echo    - Log File: logs\file_monitor.log  
echo    - Check Interval: 15 seconds
echo    - Stability Checks: 6
echo.
echo 💡 Tips:
echo    - Place CSV files in appropriate dropoff folders
echo    - Use Ctrl+C to stop the monitor
echo    - Check logs\file_monitor.log for detailed processing logs
echo.

REM Start the file monitor
python file_monitor.py

REM Capture exit code
set MONITOR_EXIT_CODE=%errorlevel%

echo.
echo ============================================
if %MONITOR_EXIT_CODE% equ 0 (
    echo ✅ File Monitor stopped normally.
) else (
    if %MONITOR_EXIT_CODE% equ 130 (
        echo 🛑 File Monitor stopped by user (Ctrl+C).
    ) else (
        echo ❌ File Monitor exited with error code: %MONITOR_EXIT_CODE%
        echo    Check logs\file_monitor.log for details.
    )
)

echo.
pause
exit /b %MONITOR_EXIT_CODE%