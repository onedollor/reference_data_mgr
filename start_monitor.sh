#!/bin/bash
# Start the file monitor process

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Reference Data Management - File Monitor"
echo "========================================"

# Run integration test first
echo "Running integration test..."
python3 test_integration.py

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Integration test passed. Starting file monitor..."
    echo ""
    echo "Monitoring directories:"
    echo "  - Fullload: /home/lin/repo/reference_data_mgr/data/reference_data/dropoff/fullload/"
    echo "  - Append:   /home/lin/repo/reference_data_mgr/data/reference_data/dropoff/append/"
    echo ""
    echo "Logs: logs/file_monitor.log"
    echo "Database tracking: data/file_tracking.db"
    echo ""
    echo "Press Ctrl+C to stop"
    echo "----------------------------------------"
    
    python3 file_monitor.py
else
    echo ""
    echo "❌ Integration test failed. Please fix the issues before starting the monitor."
    echo "You can run the test separately: ./test_integration.py"
    exit 1
fi