@echo off
REM Startup script for Simplified Reference Data Dropoff System
REM Manages both the file monitor and Excel approval monitor processes

setlocal enabledelayedexpansion

REM Configuration
set SCRIPT_DIR=%~dp0
set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%
set BACKEND_DIR=%SCRIPT_DIR%\backend
set LOG_DIR=%SCRIPT_DIR%\logs
set PID_DIR=%SCRIPT_DIR%\pids

REM Process names and scripts
set FILE_MONITOR_SCRIPT=%BACKEND_DIR%\simplified_file_monitor.py
set APPROVAL_MONITOR_SCRIPT=%BACKEND_DIR%\excel_approval_monitor.py

set FILE_MONITOR_PID=%PID_DIR%\simplified_file_monitor.pid
set APPROVAL_MONITOR_PID=%PID_DIR%\excel_approval_monitor.pid

REM Log files
set FILE_MONITOR_LOG=%LOG_DIR%\simplified_file_monitor.log
set APPROVAL_MONITOR_LOG=%LOG_DIR%\excel_approval_monitor.log
set STARTUP_LOG=%LOG_DIR%\simplified_system_startup.log

REM Colors for output (Windows doesn't support ANSI colors in standard CMD)
REM Using echo for colored output would require additional tools

REM Initialize logging
call :log "=== Simplified Reference Data System Startup Script ==="
call :log "Command: %~nx0 %*"

REM Main script logic
if "%1"=="" goto show_usage
if "%1"=="start" goto start_all
if "%1"=="stop" goto stop_all
if "%1"=="restart" goto restart_all
if "%1"=="status" goto get_status
if "%1"=="logs" goto show_logs
goto show_usage

:start_all
call :print_status "Starting Simplified Reference Data System..."

call :setup_directories
call :check_prerequisites

set file_monitor_ok=false
set approval_monitor_ok=false

REM Start file monitor
call :start_monitor "File Monitor" "%FILE_MONITOR_SCRIPT%" "%FILE_MONITOR_PID%" "%FILE_MONITOR_LOG%"
if !errorlevel! equ 0 set file_monitor_ok=true

REM Start approval monitor
call :start_monitor "Approval Monitor" "%APPROVAL_MONITOR_SCRIPT%" "%APPROVAL_MONITOR_PID%" "%APPROVAL_MONITOR_LOG%"
if !errorlevel! equ 0 set approval_monitor_ok=true

echo.
if "!file_monitor_ok!"=="true" if "!approval_monitor_ok!"=="true" (
    call :print_success "Simplified Reference Data System started successfully!"
    echo.
    call :print_status "System is ready:"
    echo   • Drop CSV files into: %SCRIPT_DIR%\data\reference_data\dropoff
    echo   • Excel forms will be generated automatically
    echo   • Review and approve Excel forms to trigger processing
    echo.
    echo Use '%~nx0 status' to check system status
    echo Use '%~nx0 logs' to view recent log entries
) else (
    call :print_error "Some components failed to start - check logs"
    exit /b 1
)
goto :eof

:stop_all
call :print_status "Stopping Simplified Reference Data System..."

call :stop_monitor "File Monitor" "%FILE_MONITOR_PID%"
call :stop_monitor "Approval Monitor" "%APPROVAL_MONITOR_PID%"

call :print_success "Simplified Reference Data System stopped"
goto :eof

:restart_all
call :print_status "Restarting Simplified Reference Data System..."
call :stop_all
timeout /t 2 /nobreak >nul
call :start_all
goto :eof

:get_status
call :print_status "Checking system status..."

echo === Simplified Reference Data System Status ===

REM File Monitor Status
call :is_process_running "%FILE_MONITOR_PID%"
if !errorlevel! equ 0 (
    set /p file_monitor_pid=<"%FILE_MONITOR_PID%"
    echo File Monitor:     RUNNING ^(PID: !file_monitor_pid!^)
) else (
    echo File Monitor:     STOPPED
)

REM Approval Monitor Status
call :is_process_running "%APPROVAL_MONITOR_PID%"
if !errorlevel! equ 0 (
    set /p approval_monitor_pid=<"%APPROVAL_MONITOR_PID%"
    echo Approval Monitor: RUNNING ^(PID: !approval_monitor_pid!^)
) else (
    echo Approval Monitor: STOPPED
)

echo.
echo Log files:
echo   File Monitor:     %FILE_MONITOR_LOG%
echo   Approval Monitor: %APPROVAL_MONITOR_LOG%
echo   Startup Log:      %STARTUP_LOG%

echo.
echo Dropoff Directory: %SCRIPT_DIR%\data\reference_data\dropoff
goto :eof

:show_logs
set lines=%2
if "%lines%"=="" set lines=50

echo === Recent File Monitor Log Entries ===
if exist "%FILE_MONITOR_LOG%" (
    powershell -command "Get-Content '%FILE_MONITOR_LOG%' -Tail %lines%"
) else (
    echo Log file not found: %FILE_MONITOR_LOG%
)

echo.
echo === Recent Approval Monitor Log Entries ===
if exist "%APPROVAL_MONITOR_LOG%" (
    powershell -command "Get-Content '%APPROVAL_MONITOR_LOG%' -Tail %lines%"
) else (
    echo Log file not found: %APPROVAL_MONITOR_LOG%
)
goto :eof

:show_usage
echo Usage: %~nx0 {start^|stop^|restart^|status^|logs}
echo.
echo Commands:
echo   start    - Start the simplified dropoff system
echo   stop     - Stop the simplified dropoff system
echo   restart  - Restart the simplified dropoff system
echo   status   - Show current system status
echo   logs     - Show recent log entries
echo.
echo Example:
echo   %~nx0 start    # Start the system
echo   %~nx0 status   # Check if running
echo   %~nx0 logs     # View recent activity
goto :eof

:setup_directories
call :print_status "Setting up directories..."

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%PID_DIR%" mkdir "%PID_DIR%"

call :print_success "Directories created successfully"
goto :eof

:check_prerequisites
call :print_status "Checking prerequisites..."

REM Check Python
python --version >nul 2>&1
if !errorlevel! neq 0 (
    call :print_error "Python is required but not installed or not in PATH"
    exit /b 1
)

REM Check if backend scripts exist
if not exist "%FILE_MONITOR_SCRIPT%" (
    call :print_error "File monitor script not found: %FILE_MONITOR_SCRIPT%"
    exit /b 1
)

if not exist "%APPROVAL_MONITOR_SCRIPT%" (
    call :print_error "Approval monitor script not found: %APPROVAL_MONITOR_SCRIPT%"
    exit /b 1
)

REM Check Python dependencies
python -c "import openpyxl" 2>nul
if !errorlevel! neq 0 (
    call :print_warning "Some Python dependencies may be missing (openpyxl)"
    call :print_warning "Consider running: pip install openpyxl"
)

call :print_success "Prerequisites check completed"
goto :eof

:is_process_running
set pid_file=%~1
if exist "%pid_file%" (
    set /p pid=<"%pid_file%"
    tasklist /fi "PID eq !pid!" 2>nul | find "!pid!" >nul
    if !errorlevel! equ 0 (
        exit /b 0
    ) else (
        REM PID file exists but process is not running - clean up
        del /f /q "%pid_file%" 2>nul
        exit /b 1
    )
) else (
    exit /b 1
)

:start_monitor
set name=%~1
set script=%~2
set pid_file=%~3
set log_file=%~4

call :print_status "Starting %name%..."

call :is_process_running "%pid_file%"
if !errorlevel! equ 0 (
    set /p pid=<"%pid_file%"
    call :print_warning "%name% is already running (PID: !pid!)"
    exit /b 0
)

REM Start the process in background
cd /d "%BACKEND_DIR%"
start /b python "%script%" >"%log_file%" 2>&1
set pid=!errorlevel!

REM Get the actual PID (this is a limitation of Windows batch - we'll use a workaround)
for /f "tokens=2 delims=," %%i in ('tasklist /fi "imagename eq python.exe" /fo csv ^| find "python.exe"') do (
    set "temp_pid=%%i"
    set "temp_pid=!temp_pid:"=!"
    echo !temp_pid! > "%pid_file%"
    goto pid_found
)
:pid_found

REM Give it a moment to start
timeout /t 2 /nobreak >nul

REM Check if it's still running
call :is_process_running "%pid_file%"
if !errorlevel! equ 0 (
    set /p pid=<"%pid_file%"
    call :print_success "%name% started successfully (PID: !pid!)"
    exit /b 0
) else (
    call :print_error "%name% failed to start - check log: %log_file%"
    exit /b 1
)

:stop_monitor
set name=%~1
set pid_file=%~2

call :print_status "Stopping %name%..."

call :is_process_running "%pid_file%"
if !errorlevel! equ 0 (
    set /p pid=<"%pid_file%"
    taskkill /pid !pid! /f >nul 2>&1
    
    REM Wait for process to stop
    set timeout=10
    :stop_wait_loop
    if !timeout! gtr 0 (
        tasklist /fi "PID eq !pid!" 2>nul | find "!pid!" >nul
        if !errorlevel! equ 0 (
            timeout /t 1 /nobreak >nul
            set /a timeout-=1
            goto stop_wait_loop
        )
    )
    
    del /f /q "%pid_file%" 2>nul
    call :print_success "%name% stopped successfully"
) else (
    call :print_warning "%name% is not running"
)
goto :eof

:log
echo %date% %time% - %~1 >> "%STARTUP_LOG%"
echo %date% %time% - %~1
goto :eof

:print_status
echo [STATUS] %~1
call :log "STATUS: %~1"
goto :eof

:print_success
echo [SUCCESS] %~1
call :log "SUCCESS: %~1"
goto :eof

:print_warning
echo [WARNING] %~1
call :log "WARNING: %~1"
goto :eof

:print_error
echo [ERROR] %~1
call :log "ERROR: %~1"
goto :eof