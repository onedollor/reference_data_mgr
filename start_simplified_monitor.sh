#!/bin/bash
# Startup script for Simplified Reference Data Dropoff System
# Manages both the file monitor and Excel approval monitor processes

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
LOG_DIR="$SCRIPT_DIR/logs"
PID_DIR="$SCRIPT_DIR/pids"

# Process names and scripts
FILE_MONITOR_SCRIPT="$BACKEND_DIR/simplified_file_monitor.py"
APPROVAL_MONITOR_SCRIPT="$BACKEND_DIR/excel_approval_monitor.py"

FILE_MONITOR_PID="$PID_DIR/simplified_file_monitor.pid"
APPROVAL_MONITOR_PID="$PID_DIR/excel_approval_monitor.pid"

# Log files
FILE_MONITOR_LOG="$LOG_DIR/simplified_file_monitor.log"
APPROVAL_MONITOR_LOG="$LOG_DIR/excel_approval_monitor.log"
STARTUP_LOG="$LOG_DIR/simplified_system_startup.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$STARTUP_LOG"
}

# Print colored messages
print_status() {
    echo -e "${BLUE}[STATUS]${NC} $1"
    log "STATUS: $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    log "SUCCESS: $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    log "WARNING: $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    log "ERROR: $1"
}

# Setup directories
setup_directories() {
    print_status "Setting up directories..."
    
    # Create necessary directories
    mkdir -p "$LOG_DIR"
    mkdir -p "$PID_DIR"
    
    print_success "Directories created successfully"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        print_error "Python3 is required but not installed"
        exit 1
    fi
    
    # Check if backend scripts exist
    if [[ ! -f "$FILE_MONITOR_SCRIPT" ]]; then
        print_error "File monitor script not found: $FILE_MONITOR_SCRIPT"
        exit 1
    fi
    
    if [[ ! -f "$APPROVAL_MONITOR_SCRIPT" ]]; then
        print_error "Approval monitor script not found: $APPROVAL_MONITOR_SCRIPT"
        exit 1
    fi
    
    # Check Python dependencies
    python3 -c "import openpyxl" 2>/dev/null || {
        print_warning "Some Python dependencies may be missing (openpyxl)"
        print_warning "Consider running: pip install openpyxl"
    }
    
    print_success "Prerequisites check completed"
}

# Check if process is running
is_process_running() {
    local pid_file="$1"
    if [[ -f "$pid_file" ]]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            return 0  # Process is running
        else
            # PID file exists but process is not running - clean up
            rm -f "$pid_file"
            return 1  # Process is not running
        fi
    fi
    return 1  # PID file doesn't exist
}

# Start a monitor process
start_monitor() {
    local name="$1"
    local script="$2"
    local pid_file="$3"
    local log_file="$4"
    
    print_status "Starting $name..."
    
    if is_process_running "$pid_file"; then
        print_warning "$name is already running (PID: $(cat "$pid_file"))"
        return 0
    fi
    
    # Start the process in background
    cd "$BACKEND_DIR"
    nohup python3 "$script" > "$log_file" 2>&1 &
    local pid=$!
    
    # Save PID
    echo $pid > "$pid_file"
    
    # Give it a moment to start
    sleep 2
    
    # Check if it's still running
    if is_process_running "$pid_file"; then
        print_success "$name started successfully (PID: $pid)"
        return 0
    else
        print_error "$name failed to start - check log: $log_file"
        return 1
    fi
}

# Stop a monitor process
stop_monitor() {
    local name="$1"
    local pid_file="$2"
    
    print_status "Stopping $name..."
    
    if is_process_running "$pid_file"; then
        local pid=$(cat "$pid_file")
        kill "$pid" 2>/dev/null || true
        
        # Wait for process to stop
        local timeout=10
        while [[ $timeout -gt 0 ]] && ps -p "$pid" > /dev/null 2>&1; do
            sleep 1
            ((timeout--))
        done
        
        if ps -p "$pid" > /dev/null 2>&1; then
            print_warning "$name didn't stop gracefully, forcing..."
            kill -9 "$pid" 2>/dev/null || true
        fi
        
        rm -f "$pid_file"
        print_success "$name stopped successfully"
    else
        print_warning "$name is not running"
    fi
}

# Get status of monitors
get_status() {
    print_status "Checking system status..."
    
    echo "=== Simplified Reference Data System Status ==="
    
    # File Monitor Status
    if is_process_running "$FILE_MONITOR_PID"; then
        echo -e "File Monitor:     ${GREEN}RUNNING${NC} (PID: $(cat "$FILE_MONITOR_PID"))"
    else
        echo -e "File Monitor:     ${RED}STOPPED${NC}"
    fi
    
    # Approval Monitor Status  
    if is_process_running "$APPROVAL_MONITOR_PID"; then
        echo -e "Approval Monitor: ${GREEN}RUNNING${NC} (PID: $(cat "$APPROVAL_MONITOR_PID"))"
    else
        echo -e "Approval Monitor: ${RED}STOPPED${NC}"
    fi
    
    echo ""
    echo "Log files:"
    echo "  File Monitor:     $FILE_MONITOR_LOG"
    echo "  Approval Monitor: $APPROVAL_MONITOR_LOG"
    echo "  Startup Log:      $STARTUP_LOG"
    
    echo ""
    echo "Dropoff Directory: $SCRIPT_DIR/data/reference_data/dropoff"
}

# Start all monitors
start_all() {
    print_status "Starting Simplified Reference Data System..."
    
    setup_directories
    check_prerequisites
    
    local file_monitor_ok=false
    local approval_monitor_ok=false
    
    # Start file monitor
    if start_monitor "File Monitor" "$FILE_MONITOR_SCRIPT" "$FILE_MONITOR_PID" "$FILE_MONITOR_LOG"; then
        file_monitor_ok=true
    fi
    
    # Start approval monitor
    if start_monitor "Approval Monitor" "$APPROVAL_MONITOR_SCRIPT" "$APPROVAL_MONITOR_PID" "$APPROVAL_MONITOR_LOG"; then
        approval_monitor_ok=true
    fi
    
    echo ""
    if [[ "$file_monitor_ok" == true && "$approval_monitor_ok" == true ]]; then
        print_success "Simplified Reference Data System started successfully!"
        echo ""
        print_status "System is ready:"
        echo "  • Drop CSV files into: $SCRIPT_DIR/data/reference_data/dropoff"
        echo "  • Excel forms will be generated automatically"
        echo "  • Review and approve Excel forms to trigger processing"
        echo ""
        echo "Use '$0 status' to check system status"
        echo "Use '$0 logs' to view recent log entries"
    else
        print_error "Some components failed to start - check logs"
        exit 1
    fi
}

# Stop all monitors
stop_all() {
    print_status "Stopping Simplified Reference Data System..."
    
    stop_monitor "File Monitor" "$FILE_MONITOR_PID"
    stop_monitor "Approval Monitor" "$APPROVAL_MONITOR_PID"
    
    print_success "Simplified Reference Data System stopped"
}

# Restart all monitors
restart_all() {
    print_status "Restarting Simplified Reference Data System..."
    stop_all
    sleep 2
    start_all
}

# Show recent log entries
show_logs() {
    local lines=${1:-50}
    
    echo "=== Recent File Monitor Log Entries ==="
    if [[ -f "$FILE_MONITOR_LOG" ]]; then
        tail -n "$lines" "$FILE_MONITOR_LOG"
    else
        echo "Log file not found: $FILE_MONITOR_LOG"
    fi
    
    echo ""
    echo "=== Recent Approval Monitor Log Entries ==="
    if [[ -f "$APPROVAL_MONITOR_LOG" ]]; then
        tail -n "$lines" "$APPROVAL_MONITOR_LOG"
    else
        echo "Log file not found: $APPROVAL_MONITOR_LOG"
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 {start|stop|restart|status|logs}"
    echo ""
    echo "Commands:"
    echo "  start    - Start the simplified dropoff system"
    echo "  stop     - Stop the simplified dropoff system"  
    echo "  restart  - Restart the simplified dropoff system"
    echo "  status   - Show current system status"
    echo "  logs     - Show recent log entries"
    echo ""
    echo "Example:"
    echo "  $0 start    # Start the system"
    echo "  $0 status   # Check if running"
    echo "  $0 logs     # View recent activity"
}

# Main script logic
main() {
    case "$1" in
        start)
            start_all
            ;;
        stop)
            stop_all
            ;;
        restart)
            restart_all
            ;;
        status)
            get_status
            ;;
        logs)
            show_logs "${2:-50}"
            ;;
        *)
            show_usage
            exit 1
            ;;
    esac
}

# Initialize logging
log "=== Simplified Reference Data System Startup Script ==="
log "Command: $0 $*"

# Run main function
main "$@"