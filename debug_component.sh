#!/bin/bash
# Debug individual components of the system

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"

# Debug environment
export DEBUG=true
export PYTHONWARNINGS=default
export PYTHONUNBUFFERED=1

usage() {
    echo "Usage: $0 <component> [options]"
    echo ""
    echo "Components:"
    echo "  file-monitor     - Debug the file monitor component"
    echo "  approval-monitor - Debug the Excel approval monitor component"
    echo "  excel-generator  - Test Excel form generation"
    echo "  excel-processor  - Test Excel form processing"
    echo "  workflow-manager - Test workflow management"
    echo "  csv-detector     - Test CSV format detection"
    echo ""
    echo "Options:"
    echo "  --test-file FILE - Use specific CSV file for testing"
    echo "  --interactive    - Run in interactive Python mode"
    echo "  --pdb           - Run with Python debugger"
    echo ""
    echo "Examples:"
    echo "  $0 file-monitor"
    echo "  $0 excel-generator --test-file data/test.csv"
    echo "  $0 workflow-manager --interactive"
    echo "  $0 approval-monitor --pdb"
}

debug_file_monitor() {
    echo "🐛 Debugging File Monitor..."
    cd "$BACKEND_DIR"
    
    if [[ "$1" == "--pdb" ]]; then
        python3 -m pdb simplified_file_monitor.py
    elif [[ "$1" == "--interactive" ]]; then
        python3 -i simplified_file_monitor.py
    else
        python3 simplified_file_monitor.py
    fi
}

debug_approval_monitor() {
    echo "🐛 Debugging Approval Monitor..."
    cd "$BACKEND_DIR"
    
    if [[ "$1" == "--pdb" ]]; then
        python3 -m pdb excel_approval_monitor.py
    elif [[ "$1" == "--interactive" ]]; then
        python3 -i excel_approval_monitor.py
    else
        python3 excel_approval_monitor.py
    fi
}

test_excel_generator() {
    local test_file="$1"
    echo "🐛 Testing Excel Generator..."
    cd "$BACKEND_DIR"
    
    cat > debug_excel_generator.py << 'EOF'
import sys
import os
from utils.excel_generator import ExcelFormGenerator
from utils.csv_detector import CSVFormatDetector

def test_excel_generation(csv_file):
    try:
        print(f"Testing Excel generation for: {csv_file}")
        
        # Initialize components
        excel_gen = ExcelFormGenerator()
        csv_detector = CSVFormatDetector()
        
        # Detect CSV format
        print("Detecting CSV format...")
        format_data = csv_detector.detect_format(csv_file)
        print(f"Format data: {format_data}")
        
        # Generate Excel form
        print("Generating Excel form...")
        excel_path = excel_gen.generate_form(csv_file, format_data)
        print(f"Excel form generated: {excel_path}")
        
        # Check if file exists
        if os.path.exists(excel_path):
            file_size = os.path.getsize(excel_path)
            print(f"Excel file size: {file_size:,} bytes")
            print("✅ Excel generation successful!")
        else:
            print("❌ Excel file not created")
            
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        print("Full traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_file = sys.argv[1] if len(sys.argv) > 1 else "../data/reference_data/dropoff/test_excel.csv"
    test_excel_generation(test_file)
EOF
    
    python3 debug_excel_generator.py "$test_file"
    rm -f debug_excel_generator.py
}

test_excel_processor() {
    echo "🐛 Testing Excel Processor..."
    cd "$BACKEND_DIR"
    
    cat > debug_excel_processor.py << 'EOF'
import sys
import os
from utils.excel_processor import ExcelProcessor

def test_excel_processing(excel_file):
    try:
        print(f"Testing Excel processing for: {excel_file}")
        
        # Initialize processor
        excel_processor = ExcelProcessor()
        
        # Check if file is ready
        print("Checking if Excel is ready for processing...")
        is_ready = excel_processor.is_excel_ready_for_processing(excel_file)
        print(f"Excel ready: {is_ready}")
        
        if is_ready:
            # Process Excel form
            print("Processing Excel form...")
            is_valid, config, errors = excel_processor.process_form(excel_file)
            print(f"Valid: {is_valid}")
            print(f"Config: {config}")
            print(f"Errors: {errors}")
            
            if is_valid:
                print("✅ Excel processing successful!")
            else:
                print(f"❌ Excel processing failed: {errors}")
        
    except Exception as e:
        import traceback
        print(f"❌ Error: {str(e)}")
        print("Full traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_file = sys.argv[1] if len(sys.argv) > 1 else "../data/reference_data/dropoff/test_excel_config.xlsx"
    test_excel_processing(test_file)
EOF
    
    python3 debug_excel_processor.py "$1"
    rm -f debug_excel_processor.py
}

test_workflow_manager() {
    echo "🐛 Testing Workflow Manager..."
    cd "$BACKEND_DIR"
    
    if [[ "$1" == "--interactive" ]]; then
        python3 -i -c "
from utils.workflow_manager import WorkflowManager
from utils.database import DatabaseManager
print('Workflow Manager loaded. Available objects: WorkflowManager, DatabaseManager')
wm = WorkflowManager()
print('Workflow manager instance created as: wm')
print('Available methods:', [m for m in dir(wm) if not m.startswith('_')])
"
    else
        cat > debug_workflow_manager.py << 'EOF'
from utils.workflow_manager import WorkflowManager
from utils.database import DatabaseManager
import traceback

def test_workflow_manager():
    try:
        print("Testing Workflow Manager...")
        
        # Initialize workflow manager
        wm = WorkflowManager()
        print("✅ Workflow manager initialized")
        
        # Test getting pending workflows
        print("Getting pending workflows...")
        pending = wm.get_pending_workflows()
        print(f"Found {len(pending)} pending workflows")
        for workflow in pending:
            print(f"  - {workflow['workflow_id']}: {workflow['status']}")
        
        print("✅ Workflow manager test completed!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("Full traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_workflow_manager()
EOF
        python3 debug_workflow_manager.py
        rm -f debug_workflow_manager.py
    fi
}

test_csv_detector() {
    local test_file="$1"
    echo "🐛 Testing CSV Detector..."
    cd "$BACKEND_DIR"
    
    cat > debug_csv_detector.py << 'EOF'
import sys
from utils.csv_detector import CSVFormatDetector
import traceback

def test_csv_detection(csv_file):
    try:
        print(f"Testing CSV detection for: {csv_file}")
        
        # Initialize detector
        detector = CSVFormatDetector()
        
        # Detect format
        print("Detecting CSV format...")
        format_data = detector.detect_format(csv_file)
        print(f"Format data: {format_data}")
        
        print("✅ CSV detection successful!")
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        print("Full traceback:")
        traceback.print_exc()

if __name__ == "__main__":
    test_file = sys.argv[1] if len(sys.argv) > 1 else "../data/reference_data/dropoff/test_excel.csv"
    test_csv_detection(test_file)
EOF
    
    python3 debug_csv_detector.py "$test_file"
    rm -f debug_csv_detector.py
}

# Main script logic
case "$1" in
    file-monitor)
        shift
        debug_file_monitor "$@"
        ;;
    approval-monitor)
        shift
        debug_approval_monitor "$@"
        ;;
    excel-generator)
        shift
        test_file="$1"
        if [[ "$1" == "--test-file" ]]; then
            test_file="$2"
        fi
        test_excel_generator "$test_file"
        ;;
    excel-processor)
        shift
        test_excel_processor "$1"
        ;;
    workflow-manager)
        shift
        test_workflow_manager "$@"
        ;;
    csv-detector)
        shift
        test_file="$1"
        if [[ "$1" == "--test-file" ]]; then
            test_file="$2"
        fi
        test_csv_detector "$test_file"
        ;;
    *)
        usage
        exit 1
        ;;
esac