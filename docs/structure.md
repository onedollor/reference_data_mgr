# Reference Data Management System - Codebase Structure

## Project Organization Philosophy

The Reference Data Management System follows a modular, domain-driven architecture that emphasizes separation of concerns, testability, and maintainability. The codebase is organized around business domains and technical layers, making it easy to understand, modify, and extend.

## Directory Structure Overview

```
reference_data_mgr/
├── .spec-workflow/              # Specification and project management
│   ├── requirements.md         # Requirements specification
│   ├── design.md              # Design specification  
│   ├── tasks.md               # Implementation tasks
│   ├── product.md             # Product specification
│   ├── tech.md                # Technical architecture
│   ├── structure.md           # This file - codebase structure
│   ├── session.json           # Workflow session data
│   └── approvals/             # Approval workflow data
├── backend/                    # Core application backend
│   ├── backend_lib.py         # Main API interface
│   ├── file_monitor.py        # File monitoring service
│   └── utils/                 # Utility modules
│       ├── __init__.py        # Package initialization
│       ├── database.py        # Database management
│       ├── ingest.py          # Data ingestion engine
│       ├── csv_detector.py    # CSV format detection
│       ├── file_handler.py    # File operations
│       ├── logger.py          # Logging utilities
│       └── progress.py        # Progress tracking
├── config/                     # Configuration files
├── data/                       # Data directories
│   └── reference_data/         # Reference data storage
│       └── dropoff/            # File dropoff structure
│           ├── reference_data_table/
│           │   ├── fullload/   # Full load files
│           │   │   ├── processed/
│           │   │   └── error/
│           │   └── append/     # Append files
│           │       ├── processed/
│           │       └── error/
│           └── none_reference_data_table/
│               ├── fullload/
│               │   ├── processed/
│               │   └── error/
│               └── append/
│                   ├── processed/
│                   └── error/
├── logs/                       # Application logs
├── sql/                        # SQL scripts and schemas
├── tests/                      # Test files (future)
├── docs/                       # Documentation (future)
├── .env                        # Environment configuration
├── .gitignore                  # Git ignore rules
├── README.md                   # Project overview
├── start_monitor.sh            # Monitor startup script
├── test_integration.py         # Integration tests
└── requirements.txt            # Python dependencies (future)
```

## Core Modules Architecture

### 1. Backend API Layer (`backend/backend_lib.py`)

**Purpose**: Unified interface for all system operations
**Responsibilities**:
- File format detection and analysis
- Data processing orchestration
- Schema matching and validation
- Health monitoring and status reporting
- Configuration management

**Key Classes and Functions**:
```python
class ReferenceDataAPI:
    """Main API class providing all system operations"""
    
    def detect_format(self, file_path: str) -> Dict[str, Any]:
        """Detect CSV format and structure"""
    
    def analyze_schema_match(self, file_path: str, headers: List[str]) -> Dict[str, Any]:
        """Analyze file schema against existing tables"""
    
    def process_file_sync(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """Synchronous file processing"""
    
    async def process_file_async(self, file_path: str, **kwargs) -> Dict[str, Any]:
        """Asynchronous file processing with progress tracking"""
    
    def health_check(self) -> Dict[str, Any]:
        """System health verification"""

# Convenience functions for direct usage
def detect_format(file_path: str) -> Dict[str, Any]
def process_file(file_path: str, load_type: str = "fullload", **kwargs) -> Dict[str, Any]
def get_table_info(table_name: str, schema: str = "ref") -> Dict[str, Any]
def health_check() -> Dict[str, Any]
```

**Integration Points**:
- Direct integration with all utility modules
- Database operations through DatabaseManager
- File processing through DataIngester
- Format detection through CSVFormatDetector

### 2. File Monitor Service (`backend/file_monitor.py`)

**Purpose**: Continuous monitoring and processing orchestration
**Responsibilities**:
- Folder scanning and file detection
- File stability verification (6-check mechanism)
- Processing workflow orchestration
- File lifecycle management (move to processed/error folders)
- Database tracking and audit logging

**Key Classes and Functions**:
```python
class FileMonitor:
    """Main file monitoring and processing orchestration"""
    
    def __init__(self):
        """Initialize monitoring components and configuration"""
    
    def scan_folders(self) -> List[Tuple[str, str, bool]]:
        """Scan dropoff folders for CSV files"""
    
    def check_file_stability(self, file_path: str) -> bool:
        """Implement 6-check stability mechanism"""
    
    def process_file(self, file_path: str, load_type: str, is_reference_data: bool):
        """Process a stable CSV file using backend API"""
    
    def run(self):
        """Main monitoring loop"""
```

**Configuration**:
```python
MONITOR_INTERVAL = 15  # seconds between scans
STABILITY_CHECKS = 6   # consecutive checks required
DROPOFF_BASE_PATH = "/path/to/dropoff"
TRACKING_TABLE = "File_Monitor_Tracking"
TRACKING_SCHEMA = "ref"
```

### 3. Database Management (`backend/utils/database.py`)

**Purpose**: SQL Server connection management and database operations
**Responsibilities**:
- Connection pooling and lifecycle management
- Transaction management with retry logic
- Schema management and validation
- Table metadata operations
- Database health monitoring

**Key Classes and Functions**:
```python
class DatabaseManager:
    """Handles all database operations with connection pooling"""
    
    def __init__(self):
        """Initialize connection parameters and pool"""
    
    def get_connection(self) -> pyodbc.Connection:
        """Get direct connection with retry logic"""
    
    def get_pooled_connection(self) -> pyodbc.Connection:
        """Get pooled connection for efficiency"""
    
    def ensure_schemas_exist(self, connection):
        """Create required schemas if they don't exist"""
    
    def table_exists(self, table_name: str, schema: str) -> bool:
        """Check if table exists in specified schema"""
    
    def get_table_columns(self, table_name: str, schema: str) -> List[Dict]:
        """Get table column metadata"""
    
    def get_all_tables(self) -> List[Dict]:
        """Get list of all accessible tables"""
```

**Connection Management Features**:
- Connection pooling with configurable pool size
- Exponential backoff retry logic
- Health checking and connection validation
- Automatic schema creation and management
- Transaction management with rollback support

### 4. Data Ingestion Engine (`backend/utils/ingest.py`)

**Purpose**: High-performance CSV data processing and database loading
**Responsibilities**:
- Batch processing with configurable batch sizes (990 rows default)
- Progress tracking and cancellation support
- Memory-efficient large file processing
- Transaction management and error handling
- Type inference and data validation

**Key Classes and Functions**:
```python
class DataIngester:
    """Handles CSV data ingestion into SQL Server database"""
    
    def __init__(self, db_manager: DatabaseManager, logger: Logger):
        """Initialize ingestion components"""
    
    async def ingest_data(
        self, 
        file_path: str, 
        fmt_file_path: str, 
        load_mode: str,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """Main ingestion process with progress tracking"""
```

**Processing Features**:
- Stream processing for memory efficiency
- Configurable batch sizes (990 rows per INSERT)
- Progress reporting with cancellation support
- Type inference based on sample data
- Comprehensive error handling and recovery

### 5. CSV Format Detection (`backend/utils/csv_detector.py`)

**Purpose**: Intelligent CSV format detection and parsing
**Responsibilities**:
- Delimiter detection (comma, semicolon, pipe, tab)
- Header identification and extraction
- Encoding detection and handling
- Sample data extraction for analysis
- Format validation and consistency checking

**Key Classes and Functions**:
```python
class CSVFormatDetector:
    """Intelligent CSV format detection and parsing"""
    
    def detect_format(self, file_path: str) -> Dict[str, Any]:
        """Main format detection algorithm"""
    
    def _analyze_delimiters(self, sample_lines: List[str]) -> str:
        """Score different delimiter candidates"""
    
    def _detect_headers(self, first_line: str, delimiter: str) -> bool:
        """Determine if first line contains headers"""
    
    def _extract_sample_data(self, file_path: str, delimiter: str) -> List[List[str]]:
        """Extract sample data for analysis"""
```

**Detection Algorithm**:
1. Sample file content (first 10-20 lines)
2. Score delimiter candidates based on consistency
3. Analyze first line for header patterns
4. Extract sample data for validation
5. Return comprehensive format information

### 6. File Operations (`backend/utils/file_handler.py`)

**Purpose**: File system operations and naming utilities
**Responsibilities**:
- Table name extraction from filenames
- File movement and organization
- File integrity verification
- Naming convention enforcement
- File metadata management

**Key Classes and Functions**:
```python
class FileHandler:
    """File operations and naming utilities"""
    
    def extract_table_base_name(self, filename: str) -> str:
        """Extract clean table name from filename"""
    
    def move_to_processed(self, source_path: str, target_dir: str):
        """Move successfully processed files"""
    
    def move_to_error(self, source_path: str, target_dir: str, error_msg: str):
        """Move failed files with error information"""
    
    def calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash for file integrity"""
```

**Naming Convention Rules**:
1. Remove file extension (.csv)
2. Strip date patterns (YYYYMMDD, YYYY-MM-DD)
3. Convert hyphens to underscores
4. Remove special characters except underscores
5. Collapse multiple underscores to single
6. Convert to lowercase
7. Trim leading/trailing underscores

### 7. Logging System (`backend/utils/logger.py`)

**Purpose**: Comprehensive logging and audit trail management
**Responsibilities**:
- Multi-level logging (DEBUG, INFO, WARNING, ERROR)
- File and console output management
- Log rotation and cleanup
- Performance metrics logging
- Audit trail for compliance

**Key Classes and Functions**:
```python
class Logger:
    """Comprehensive logging system"""
    
    def __init__(self):
        """Initialize logging configuration"""
    
    def log_processing_event(self, event: str, metadata: dict):
        """Log processing events with structured metadata"""
    
    def log_performance_metric(self, metric: str, value: float, metadata: dict):
        """Log performance metrics for analysis"""
    
    def get_recent_logs(self, limit: int) -> List[Dict]:
        """Retrieve recent log entries for monitoring"""
```

### 8. Progress Tracking (`backend/utils/progress.py`)

**Purpose**: Real-time progress monitoring and cancellation support
**Responsibilities**:
- Progress state management
- Cancellation signal handling
- Progress reporting and visualization
- State persistence and recovery
- Memory cleanup and garbage collection

**Key Functions**:
```python
def init_progress(progress_key: str):
    """Initialize progress tracking for an operation"""

def update_progress(progress_key: str, current: int, total: int, message: str):
    """Update progress information"""

def get_progress(progress_key: str) -> Dict[str, Any]:
    """Get current progress status"""

def cancel_progress(progress_key: str):
    """Signal cancellation for an operation"""

def is_canceled(progress_key: str) -> bool:
    """Check if operation has been canceled"""

def cleanup_progress(progress_key: str):
    """Clean up progress tracking resources"""
```

## Configuration Management

### Environment Variables (`.env`)
```bash
# Database Configuration
db_host=localhost
db_name=reference_data
db_user=ingest_user
db_password=secure_password
db_odbc_driver=ODBC Driver 17 for SQL Server

# Schema Configuration  
data_schema=ref
backup_schema=bkp
validation_sp_schema=ref

# Processing Configuration
INGEST_BATCH_SIZE=990
DB_POOL_SIZE=5
DB_MAX_RETRIES=3
DB_RETRY_BACKOFF=0.5

# Monitoring Configuration
MONITOR_INTERVAL=15
STABILITY_CHECKS=6
```

### Runtime Configuration
**Locations**:
- Environment variables for sensitive configuration
- Configuration files for static settings
- Database configuration for dynamic settings
- Command-line arguments for runtime overrides

## Data Flow Architecture

### File Processing Pipeline
```
1. File Detection (file_monitor.py)
   ├── Folder scanning every 15 seconds
   ├── New file registration in tracking
   └── Classification (reference vs non-reference)

2. Stability Monitoring (file_monitor.py)
   ├── Size and timestamp tracking
   ├── 6-check stability verification
   └── Ready-for-processing notification

3. Format Detection (csv_detector.py)
   ├── Delimiter and encoding detection
   ├── Header identification
   └── Sample data extraction

4. Data Processing (ingest.py)
   ├── Batch processing (990 rows)
   ├── Progress tracking
   ├── Transaction management
   └── Error handling

5. File Management (file_handler.py)
   ├── Success: Move to processed/
   ├── Failure: Move to error/
   └── Database tracking update

6. Configuration Management (backend_lib.py)
   ├── Reference data: Insert config record
   ├── Non-reference data: Skip config
   └── Audit trail completion
```

### Database Schema Integration
```sql
-- File tracking table in ref schema
ref.File_Monitor_Tracking
├── Processing status and metadata
├── File classification and load type
├── Error information and recovery
└── Audit trail and timestamps

-- Reference data configuration
ref.Reference_Data_Cfg
├── Table configuration records
├── Processing parameters
└── Metadata and lineage

-- Target data tables (dynamic)
ref.[table_name]
├── Actual data from CSV files
├── Load timestamps and audit columns
└── Backup copies in bkp schema
```

## Testing Strategy

### Unit Testing Structure (Future)
```
tests/
├── unit/
│   ├── test_database_manager.py
│   ├── test_csv_detector.py
│   ├── test_data_ingester.py
│   ├── test_file_handler.py
│   └── test_backend_api.py
├── integration/
│   ├── test_file_processing_pipeline.py
│   ├── test_database_integration.py
│   └── test_error_scenarios.py
├── performance/
│   ├── test_load_performance.py
│   └── test_memory_usage.py
└── fixtures/
    ├── sample_csv_files/
    └── test_databases/
```

### Current Testing
- **Integration Test**: `test_integration.py` - End-to-end system validation
- **Monitor Test**: `test_monitor.py` - File monitoring functionality
- **Manual Testing**: Comprehensive test cases documented in `file_monitor_test.md`

## Development Workflow

### Code Organization Principles
1. **Single Responsibility**: Each module has one clear purpose
2. **Dependency Injection**: Components receive dependencies rather than creating them
3. **Interface Segregation**: Clear interfaces between modules
4. **Configuration Externalization**: All configuration through environment variables
5. **Error Handling**: Comprehensive error handling at all levels

### Code Quality Standards
- Type hints throughout the codebase
- Comprehensive docstrings for all public interfaces  
- Consistent naming conventions (snake_case for Python)
- Error handling with specific exception types
- Logging at appropriate levels throughout

### Deployment Structure
```
production/
├── backend/              # Application code
├── logs/                 # Log files (mounted volume)
├── data/                 # Data directories (mounted volume)  
├── config/               # Configuration files
├── .env                  # Environment configuration
└── start_monitor.sh      # Service startup script
```

## Extension Points and Modularity

### Adding New File Formats
1. Extend `CSVFormatDetector` with new format detection logic
2. Update `DataIngester` to handle new format processing
3. Add configuration options for format-specific parameters
4. Update tests to validate new format support

### Adding New Data Sources
1. Create new folder structure in `data/reference_data/dropoff/`
2. Update `FileMonitor` to recognize new folder patterns
3. Add classification logic for new data types
4. Update configuration and documentation

### Adding New Processing Modes
1. Extend `DataIngester` with new processing logic
2. Update `FileMonitor` to handle new mode classification
3. Add database schema support for new modes
4. Update API interfaces to expose new modes

### Integration Extensions
1. Add new utility modules in `backend/utils/`
2. Extend `ReferenceDataAPI` with new endpoints
3. Add new configuration options and validation
4. Update health checks and monitoring

This codebase structure provides a solid foundation for the Reference Data Management System, emphasizing maintainability, extensibility, and clear separation of concerns while supporting the complex requirements of enterprise data processing.