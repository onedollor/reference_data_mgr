# Reference Data Management System

An enterprise-grade automated solution for CSV file processing with intelligent format detection, interactive Excel approval workflow, and comprehensive audit trails.

## 🚀 Quick Start

### Start the Simplified Dropoff System
```bash
./start_simplified_monitor.sh start
```

### Drop CSV Files
Place CSV files in: `data/reference_data/dropoff/`

### Approve Processing
1. Excel forms are generated automatically
2. Review and modify the Excel configuration form
3. Check the final confirmation box and save the Excel file
4. Processing begins automatically

### Check Status
```bash
./start_simplified_monitor.sh status
```

## 📋 Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)  
- [Installation](#installation)
- [Usage Guide](#usage-guide)
- [Configuration](#configuration)
- [Monitoring & Logs](#monitoring--logs)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Development](#development)

## 🏗️ Overview

### Simplified Dropoff System

The Reference Data Management System now features a **Simplified Dropoff System** that transforms CSV processing from fully automated to human-in-the-loop validation:

**Key Features:**
- **Single Dropoff Directory** - No complex subfolder structures
- **Automatic CSV Analysis** - Intelligent format detection (delimiters, encoding, headers)
- **Interactive Excel Forms** - Review and modify processing settings
- **Human Approval Workflow** - Explicit confirmation required before processing
- **Complete Audit Trail** - Full tracking of all workflow steps
- **Backward Compatibility** - Uses existing processing engine

**Processing Flow:**
```
CSV File → Format Detection → Excel Form Generation → User Review → Approval → Processing → Archive
```

### Legacy System

The original automated system with subfolder-based processing is still available in the existing `file_monitor.py` for environments requiring full automation.

## 🏛️ System Architecture

### Simplified Dropoff Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   CSV File      │───▶│  Format          │───▶│  Excel Form     │
│   Dropoff       │    │  Detection       │    │  Generation     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   File          │◀───│  Processing      │◀───│  User           │
│   Archive       │    │  Engine          │    │  Approval       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Core Components

- **SimplifiedFileMonitor** - Monitors single dropoff directory
- **ExcelFormGenerator** - Creates interactive Excel configuration forms
- **ExcelApprovalMonitor** - Watches for approved Excel files and triggers processing
- **WorkflowManager** - Orchestrates workflow states and database tracking
- **ExcelProcessor** - Validates and extracts configuration from completed forms

### Database Schema

- **ref.Excel_Workflow_Tracking** - Workflow state and audit trail
- **ref.Excel_Processing_Stats** - Performance metrics and statistics
- **ref.File_Monitor_Tracking** - Legacy file processing tracking (preserved)

## 🛠️ Installation

### Prerequisites

- Python 3.12+
- SQL Server 2017+
- ODBC Driver 17+ for SQL Server

### Install Dependencies

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or: venv\Scripts\activate  # Windows

# Install required packages
pip install -r requirements.txt
```

### Core Dependencies
- `pandas` - Data processing and CSV handling
- `pyodbc` - SQL Server database connectivity
- `openpyxl` - Excel form generation and processing
- `fastapi` - Web framework for API endpoints
- `PyYAML` - Configuration file handling
- `chardet` - Character encoding detection
- `aiofiles` - Asynchronous file operations
- `reportlab` - PDF report generation

### Database Setup

1. **Create Database Schema:**
   ```bash
   # Execute the database schema creation
   sqlcmd -S your_server -d your_database -i sql/excel_workflow_schema.sql
   ```

2. **Configure Connection:**
   ```bash
   # Copy and edit environment file
   cp .env.example .env
   # Edit .env with your database connection details
   ```

### Directory Structure

The system will automatically create required directories:
```
data/reference_data/dropoff/
├── processed/          # Successfully processed files
└── error/             # Files that encountered errors
```

## 📖 Usage Guide

### Basic Operation

1. **Start the System:**
   ```bash
   ./start_simplified_monitor.sh start
   ```

2. **Drop CSV Files:**
   Place your CSV files directly in:
   ```
   data/reference_data/dropoff/
   ```

3. **Review Excel Forms:**
   - Excel forms are generated automatically with detected settings
   - Open the Excel form (same name as CSV with `_config.xlsx` suffix)
   - Review and modify settings as needed:
     - CSV format settings (delimiter, encoding, headers)
     - Processing mode (fullload or append)
     - Reference data table creation (Yes/No)
     - Table name (optional)

4. **Approve Processing:**
   - Check the final confirmation checkbox
   - Fill in "Processed By" and "Date/Time" fields
   - Save the Excel file
   - Processing begins automatically within 30 seconds

### Excel Form Fields

**CSV Format Settings:**
- **Delimiter:** Character separating CSV columns (`,`, `;`, `|`, `\t`)
- **Encoding:** File character encoding (`utf-8`, `utf-16`, `iso-8859-1`)
- **Headers:** Whether first row contains column names
- **Text Qualifier:** Character wrapping text values (`"`, `'`)

**Processing Options:**
- **fullload:** Truncate existing table data and reload completely
- **append:** Add new data to existing table without truncation
- **Reference Data Table:** Create configuration record in `ref.Reference_Data_Cfg`
- **Table Name:** Custom table name (defaults to filename)

**Confirmation:**
- **Final Confirmation:** Required checkbox to authorize processing
- **Processed By:** User identification for audit trail
- **Date/Time:** Processing timestamp

### Command Line Operations

```bash
# System management
./start_simplified_monitor.sh start     # Start both monitors
./start_simplified_monitor.sh stop      # Stop all processes  
./start_simplified_monitor.sh restart   # Restart system
./start_simplified_monitor.sh status    # Check running status
./start_simplified_monitor.sh logs      # View recent log entries

# Testing
python3 run_tests.py                    # Run all tests
python3 run_tests.py --test test_pdf_generator  # Run specific test
python3 run_tests.py --check-deps       # Check dependencies
```

## ⚙️ Configuration

### Environment Variables (.env)

```bash
# Database Configuration
DB_SERVER=your_sql_server
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_DRIVER={ODBC Driver 17 for SQL Server}

# System Configuration  
DROPOFF_PATH=/path/to/dropoff/directory
LOG_LEVEL=INFO
MONITOR_INTERVAL=15
STABILITY_CHECKS=6

# Processing Configuration
MAX_CONCURRENT_PROCESSING=3
EXCEL_CHECK_INTERVAL=30
CLEANUP_DAYS=7
```

### System Parameters

**File Monitor Settings:**
- `MONITOR_INTERVAL`: Seconds between directory scans (default: 15)
- `STABILITY_CHECKS`: File stability verification cycles (default: 6)

**Excel Approval Settings:**
- `APPROVAL_CHECK_INTERVAL`: Seconds between Excel approval checks (default: 30)
- `EXCEL_MODIFICATION_CHECK_INTERVAL`: Excel file modification check frequency (default: 60)
- `MAX_CONCURRENT_PROCESSING`: Maximum simultaneous file processing (default: 3)

## 📊 Monitoring & Logs

### Log Files

- **File Monitor:** `logs/simplified_file_monitor.log`
- **Approval Monitor:** `logs/excel_approval_monitor.log`
- **Startup System:** `logs/simplified_system_startup.log`

### Monitoring Commands

```bash
# View recent activity
./start_simplified_monitor.sh logs

# Monitor live logs
tail -f logs/simplified_file_monitor.log
tail -f logs/excel_approval_monitor.log

# Check workflow status in database
SELECT status, COUNT(*) as count
FROM ref.Excel_Workflow_Tracking
GROUP BY status;

# View processing statistics
SELECT * FROM ref.VW_Excel_Workflow_Summary
ORDER BY created_at DESC;
```

### Performance Metrics

- **File Detection:** <15 seconds from dropoff to Excel generation
- **Excel Generation:** <30 seconds for form creation
- **Processing Throughput:** >1,000 rows/second (inherited from existing engine)
- **Memory Usage:** <512MB per 100MB file processed

## 🧪 Testing

### Run All Tests
```bash
python3 run_tests.py
```

### Test Categories

**Unit Tests:**
- Excel form generation and processing
- Workflow state management
- Configuration validation
- Error handling

**Integration Tests:**
- End-to-end workflow simulation
- Component interaction testing
- Database operations
- File system operations

**Performance Tests:**
- CSV detection performance (10k+ rows)
- Excel generation timing
- Concurrent workflow handling
- Database operation efficiency

### Test Coverage

```bash
# Run tests with coverage
python3 run_tests.py --verbose --pattern "test_*.py"

# Run specific test module
python3 run_tests.py --test test_pdf_generator

# Check dependencies
python3 run_tests.py --check-deps
```

## 🔧 Troubleshooting

### Common Issues

**Excel Generation Fails:**
```
Error: Failed to generate Excel form
Solution: Install openpyxl: pip install openpyxl
```

**Excel Processing Errors:**
```
Error: Cannot extract Excel configuration
Solution: Install openpyxl: pip install openpyxl
```

**Database Connection Issues:**
```
Error: Database connection failed
Solution: Check .env file and database credentials
        Verify ODBC driver installation
```

**File Not Processing:**
```
Issue: CSV file detected but no Excel generated
Check: File permissions and stability
       Monitor logs for error messages
```

### Diagnostic Commands

```bash
# Check system status
./start_simplified_monitor.sh status

# View recent errors
grep -i error logs/simplified_file_monitor.log | tail -20

# Test database connection
python3 -c "from backend.utils.database import DatabaseManager; DatabaseManager().get_connection()"

# Validate dependencies
python3 run_tests.py --check-deps
```

### Recovery Procedures

**Restart Failed Workflows:**
```sql
-- Reset failed workflows to retry
UPDATE ref.Excel_Workflow_Tracking
SET status = 'pending_excel', retry_count = retry_count + 1
WHERE status = 'error' AND retry_count < 3;
```

**Clean Up Old Workflows:**
```sql
-- Use stored procedure for cleanup
EXEC ref.SP_Cleanup_Completed_Workflows @DaysToKeep = 7
```

**Manual File Processing:**
```bash
# Process file directly with existing API
cd backend
python3 -c "
from backend_lib import ReferenceDataAPI
api = ReferenceDataAPI()
result = api.process_file('/path/to/file.csv', 'fullload', True)
print(result)
"
```

## 👥 Development

### Project Structure

```
reference_data_mgr/
├── backend/                    # Core application
│   ├── simplified_file_monitor.py      # New file monitoring
│   ├── excel_approval_monitor.py       # Excel approval monitoring
│   ├── backend_lib.py                  # Existing processing API
│   ├── file_monitor.py                 # Legacy file monitoring
│   └── utils/                          # Utility modules
│       ├── excel_generator.py          # Excel form generation
│       ├── excel_processor.py          # Excel form processing
│       ├── workflow_manager.py         # Workflow orchestration
│       ├── csv_detector.py             # CSV format detection
│       ├── database.py                 # Database management
│       └── logger.py                   # Logging utilities
├── sql/                        # Database schemas
│   └── excel_workflow_schema.sql       # Workflow tracking tables
├── logs/                       # Application logs
├── data/reference_data/dropoff/        # File dropoff locations
└── tests/                      # Test suites
```

### Adding New Features

1. **Follow Existing Patterns:**
   - Use dependency injection for database and logging
   - Follow single responsibility principle
   - Add comprehensive error handling
   - Include unit tests

2. **Database Changes:**
   - Update `sql/excel_workflow_schema.sql`
   - Add migration scripts for existing deployments
   - Update workflow manager for schema changes

3. **Excel Form Modifications:**
   - Extend `ExcelFormGenerator` for new form fields
   - Update `ExcelProcessor` for new validation rules
   - Add corresponding test cases

### Code Style

- **Naming:** `snake_case` for functions/variables, `PascalCase` for classes
- **Documentation:** Comprehensive docstrings for all public methods
- **Error Handling:** Explicit exception handling with logging
- **Testing:** Unit tests for all new functionality

### Contributing

1. Create feature branch from main
2. Implement changes following project patterns
3. Add comprehensive tests
4. Update documentation
5. Submit pull request with clear description

---

## 📞 Support

For issues, questions, or feature requests:

1. **Check Logs:** Review log files in `logs/` directory
2. **Run Diagnostics:** Use troubleshooting commands above
3. **Test Dependencies:** Verify all required packages installed
4. **Database Connectivity:** Confirm database connection and permissions

**System Requirements:**
- Python 3.12+
- SQL Server 2017+
- openpyxl and pandas packages
- Appropriate file system permissions

**Performance Expectations:**
- File detection: <15 seconds
- Excel generation: <30 seconds
- Processing: >1,000 rows/second
- Memory usage: <512MB per 100MB file