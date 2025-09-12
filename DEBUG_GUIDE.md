# 🐛 Debug Guide for Simplified Reference Data Management System

## Quick Start Debug Commands

### 🚀 **1. Start Full System in Debug Mode**
```bash
# Linux/Mac
./start_debug.sh start

# Windows  
set DEBUG=true && start_simplified_monitor.cmd start
```

### 🔍 **2. Debug Individual Components**
```bash
# Debug file monitor
./debug_component.sh file-monitor

# Debug approval monitor  
./debug_component.sh approval-monitor

# Test Excel generation
./debug_component.sh excel-generator --test-file data/test.csv

# Test Excel processing
./debug_component.sh excel-processor path/to/excel_file.xlsx

# Test workflow manager (interactive mode)
./debug_component.sh workflow-manager --interactive

# Test CSV detection
./debug_component.sh csv-detector --test-file data/test.csv
```

### 🐍 **3. Python Debugger (PDB)**
```bash
# Run with Python debugger
./debug_component.sh file-monitor --pdb
./debug_component.sh approval-monitor --pdb
```

## 📊 **Debug Features Enabled**

### **Enhanced Logging**
- **Function names** and **line numbers** in all log messages
- **DEBUG level** messages when `DEBUG=true`
- **File stability tracking** debug messages
- **Workflow state transitions** debug messages

### **Debug Environment Variables**
```bash
export DEBUG=true              # Enable debug logging
export PYTHONWARNINGS=default  # Show Python warnings
export PYTHONUNBUFFERED=1     # Immediate output (no buffering)
```

### **Log Format**
```
2025-09-12 18:37:36,405 - INFO - setup_logging:74 - Simplified file monitor logging initialized
                                  ↑             ↑
                               Function    Line Number
```

## 🔧 **Debug Scenarios**

### **1. File Not Being Processed**
```bash
# Check file detection
./debug_component.sh file-monitor --pdb

# In PDB:
# b handle_detected_file     # Set breakpoint
# c                         # Continue
# Drop a CSV file and watch the debugger
```

### **2. Excel Generation Issues**
```bash
# Test Excel generation with specific file
./debug_component.sh excel-generator --test-file /path/to/your.csv

# Check logs for detailed error messages
tail -f logs/simplified_file_monitor.log
```

### **3. Database Connection Issues**
```bash
# Test workflow manager
./debug_component.sh workflow-manager --interactive

# In Python shell:
# wm.get_pending_workflows()  # Test database connection
# Check for SQL errors
```

### **4. Excel Form Processing Issues**
```bash
# Test Excel processing
./debug_component.sh excel-processor /path/to/generated_form.xlsx

# Check validation errors and form parsing
```

## 📝 **Log File Locations**

```
logs/
├── simplified_file_monitor.log     # File detection and Excel generation
├── excel_approval_monitor.log      # Excel form processing and approval  
└── simplified_system_startup.log   # System startup and shutdown events
```

## 🚨 **Common Debug Steps**

### **Step 1: Check System Status**
```bash
./start_simplified_monitor.sh status
```

### **Step 2: Enable Debug Mode**
```bash
# Stop system
./start_simplified_monitor.sh stop

# Start in debug mode  
./start_debug.sh start
```

### **Step 3: Monitor Debug Logs**
```bash
# Watch all logs in real-time
tail -f logs/*.log

# Or specific component
tail -f logs/simplified_file_monitor.log
```

### **Step 4: Test Individual Components**
```bash
# Test the component that's failing
./debug_component.sh <component-name>
```

### **Step 5: Use Interactive Debugging**
```bash
# For complex issues, use PDB
./debug_component.sh file-monitor --pdb

# Or interactive Python
./debug_component.sh workflow-manager --interactive
```

## 🐞 **Debug-Specific Features**

### **File Stability Debug Messages**
When `DEBUG=true`, you'll see:
```
DEBUG - File /path/to/file.csv not yet stable, waiting...
DEBUG - File /path/to/file.csv already being processed, skipping
```

### **Workflow State Transitions**
Debug mode shows detailed workflow state changes:
```
DEBUG - Creating workflow for file: /path/to/file.csv
DEBUG - Updating workflow abc123 to status: excel_generated
```

### **Database Query Debug**
All SQL queries are logged with parameters in debug mode.

## 🛠️ **Advanced Debugging**

### **1. Database Debugging**
```bash
# Connect to database directly
# (connection details in your environment)
sqlcmd -S your-server -d your-database

# Check workflow table
SELECT * FROM ref.Excel_Workflow_Tracking ORDER BY created_at DESC;
```

### **2. Python Interactive Debugging**
```python
# In debug_component.sh workflow-manager --interactive
import pdb; pdb.set_trace()  # Add breakpoint
wm.create_workflow('/path/to/test.csv')  # Test workflow creation
```

### **3. Network/API Debugging**
```bash
# Check if backend API is accessible
cd backend
python3 -c "from backend_lib import ReferenceDataAPI; api = ReferenceDataAPI(); print('API OK')"
```

## 📞 **Debug Output Examples**

### **Normal Mode:**
```
2025-09-12 18:37:36 - INFO - Starting simplified file monitor...
```

### **Debug Mode:**
```  
2025-09-12 18:37:36 - DEBUG - scan_simplified_directory:125 - Scanning directory: /path/to/dropoff
2025-09-12 18:37:36 - DEBUG - handle_detected_file:145 - File /path/to/test.csv not yet stable, waiting...
2025-09-12 18:37:51 - INFO - handle_detected_file:149 - Stable file detected: /path/to/test.csv  
```

This debug system gives you complete visibility into the Excel workflow process! 🎯