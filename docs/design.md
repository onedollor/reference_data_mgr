# Reference Data Management System - Design Specification

## System Architecture Overview

### High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   File Monitor  │────│   Backend API    │────│ Database Utils  │
│  (file_monitor) │    │ (backend_lib)    │    │   (database)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                        │
         │                        │                        │
    ┌────────────┐         ┌──────────────┐         ┌─────────────┐
    │File Handler│         │CSV Detector  │         │SQL Server DB│
    │   Utils    │         │Format Utils  │         │  (ref/bkp)  │
    └────────────┘         └──────────────┘         └─────────────┘
         │                        │                        │
         │                        └──────────────┐         │
    ┌────────────┐                              │         │
    │  Progress  │         ┌──────────────┐     │         │
    │  Tracker   │         │   Logger     │     │         │
    └────────────┘         └──────────────┘     │         │
                                               │         │
                           ┌──────────────┐    │         │
                           │ Data Ingester│────┘         │
                           │   (ingest)   │──────────────┘
                           └──────────────┘
```

### Component Architecture

#### Core Processing Layer
- **File Monitor**: Continuous folder monitoring and file lifecycle management
- **Backend API**: Unified interface for all system operations
- **Data Ingester**: Batch processing engine for CSV data loading

#### Utility Layer
- **Database Manager**: Connection pooling and SQL Server operations
- **CSV Format Detector**: Intelligent format detection and parsing
- **File Handler**: File operations and naming utilities
- **Progress Tracker**: Real-time progress monitoring and cancellation
- **Logger**: Multi-level logging and audit trails

#### Data Layer
- **SQL Server Database**: Primary data storage with ref and bkp schemas
- **File System**: Structured dropoff, processed, and error folders

## Detailed Component Design

### 1. File Monitor Component

#### Class: FileMonitor
**Purpose**: Orchestrates the entire file processing workflow

```python
class FileMonitor:
    def __init__(self):
        # Initialize components and configuration
        
    def scan_folders(self) -> List[Tuple[str, str, bool]]:
        # Scan dropoff folders for CSV files
        # Returns: [(file_path, load_type, is_reference_data)]
        
    def check_file_stability(self, file_path: str) -> bool:
        # Implement 6-check stability mechanism
        
    def process_file(self, file_path: str, load_type: str, is_reference_data: bool):
        # Main file processing orchestration
        
    def run(self):
        # Main monitoring loop
```

#### Key Features
- **Monitoring Intervals**: 15-second scan cycles
- **Stability Detection**: 6 consecutive unchanged checks (90 seconds)
- **File Tracking**: In-memory dictionary with persistent database backup
- **Error Recovery**: Graceful handling of file system issues

#### Configuration Parameters
```python
MONITOR_INTERVAL = 15  # seconds
STABILITY_CHECKS = 6   # consecutive checks
DROPOFF_BASE_PATH = "/path/to/dropoff"
LOG_FILE = "/path/to/logs/file_monitor.log"
```

### 2. Backend API Component

#### Class: ReferenceDataAPI
**Purpose**: Provides unified interface for all data operations

```python
class ReferenceDataAPI:
    def detect_format(self, file_path: str) -> Dict[str, Any]:
        # CSV format detection and analysis
        
    def analyze_schema_match(self, file_path: str, headers: List[str]) -> Dict[str, Any]:
        # Schema matching against existing tables
        
    def process_file_sync(self, file_path: str, **kwargs) -> Dict[str, Any]:
        # Synchronous file processing
        
    async def process_file_async(self, file_path: str, **kwargs) -> Dict[str, Any]:
        # Asynchronous file processing with progress
        
    def health_check(self) -> Dict[str, Any]:
        # System health verification
```

#### API Response Format
```python
{
    "success": bool,
    "data": Optional[Any],
    "error": Optional[str],
    "metadata": {
        "timestamp": str,
        "processing_time": float,
        "file_path": str
    }
}
```

### 3. Database Manager Component

#### Class: DatabaseManager
**Purpose**: Manages all database operations with connection pooling

```python
class DatabaseManager:
    def __init__(self):
        # Connection string and pool initialization
        
    def get_connection(self) -> pyodbc.Connection:
        # Direct connection with retry logic
        
    def get_pooled_connection(self) -> pyodbc.Connection:
        # Pooled connection management
        
    def ensure_schemas_exist(self, connection):
        # Create ref and bkp schemas if needed
        
    def table_exists(self, table_name: str, schema: str) -> bool:
        # Check table existence
```

#### Connection Pool Design
```python
Pool Configuration:
- Default Size: 5 connections
- Max Retries: 3 attempts
- Retry Backoff: Exponential (0.5s base)
- Connection Timeout: 30 seconds
- Auto-commit: Enabled by default
```

### 4. Data Ingester Component

#### Class: DataIngester
**Purpose**: Handles CSV data processing and database loading

```python
class DataIngester:
    async def ingest_data(
        self, 
        file_path: str, 
        fmt_file_path: str,
        load_mode: str,
        **kwargs
    ) -> AsyncGenerator[str, None]:
        # Main ingestion process with progress tracking
```

#### Processing Flow
1. **File Validation**: Check file accessibility and format
2. **Format Detection**: Analyze CSV structure and headers
3. **Schema Preparation**: Create or validate target table
4. **Data Processing**: Batch processing with progress tracking
5. **Transaction Management**: Commit or rollback based on results

#### Batch Processing Design
```python
Batch Configuration:
- Default Batch Size: 990 rows per INSERT statement
- Progress Intervals: Every 5 batches
- Memory Management: Stream processing for large files
- Error Handling: Stop on first batch error
```

### 5. CSV Format Detector Component

#### Class: CSVFormatDetector
**Purpose**: Intelligent CSV format detection and parsing

```python
class CSVFormatDetector:
    def detect_format(self, file_path: str) -> Dict[str, Any]:
        # Primary format detection
        
    def _analyze_delimiters(self, sample_lines: List[str]) -> str:
        # Delimiter detection logic
        
    def _detect_headers(self, first_line: str, delimiter: str) -> List[str]:
        # Header detection and extraction
```

#### Detection Algorithm
1. **Sample Reading**: Read first 10-20 lines for analysis
2. **Delimiter Detection**: Score comma, semicolon, pipe, tab usage
3. **Header Analysis**: Pattern recognition for header vs data
4. **Encoding Detection**: UTF-8, Latin-1, Windows-1252 support
5. **Validation**: Consistency checks across sample lines

### 6. File Handler Component

#### Class: FileHandler
**Purpose**: File operations and naming utilities

```python
class FileHandler:
    def extract_table_base_name(self, filename: str) -> str:
        # Extract clean table name from filename
        
    def move_to_processed(self, source_path: str, target_dir: str):
        # Move successfully processed files
        
    def move_to_error(self, source_path: str, target_dir: str, error_msg: str):
        # Move failed files with error information
```

#### Naming Conventions
```python
Table Name Extraction Rules:
1. Remove file extension (.csv)
2. Strip date patterns (YYYYMMDD, YYYY-MM-DD)
3. Convert hyphens to underscores
4. Remove special characters except underscores
5. Collapse multiple underscores
6. Convert to lowercase
7. Trim leading/trailing underscores
```

## Data Flow Design

### Primary Processing Flow

```
1. File Detection
   ├── Folder Scan (every 15s)
   ├── CSV File Identification
   └── New File Registration

2. Stability Monitoring
   ├── Size/Timestamp Tracking
   ├── 6-Check Verification
   └── Stability Confirmation

3. Format Analysis
   ├── CSV Format Detection
   ├── Header Extraction
   ├── Schema Analysis
   └── Table Name Derivation

4. Data Processing
   ├── Database Connection
   ├── Transaction Start
   ├── Batch Processing (990 rows)
   ├── Progress Tracking
   └── Transaction Commit/Rollback

5. File Management
   ├── Success: Move to processed/
   ├── Failure: Move to error/
   └── Database Tracking Update

6. Configuration Management
   ├── Reference Data: Insert config record
   ├── Non-Reference Data: Skip config
   └── Audit Trail Update
```

### Error Handling Flow

```
Error Detection
├── Connection Errors
│   ├── Retry with Backoff
│   ├── Alternative Connection
│   └── Failure Notification
│
├── Processing Errors
│   ├── Transaction Rollback
│   ├── Error File Movement
│   └── Error Logging
│
├── Format Errors
│   ├── Fallback Detection
│   ├── Manual Override Options
│   └── Error Documentation
│
└── System Errors
    ├── Graceful Degradation
    ├── Resource Cleanup
    └── Recovery Procedures
```

## Database Design

### File Tracking Schema

#### Table: ref.File_Monitor_Tracking
```sql
CREATE TABLE [ref].[File_Monitor_Tracking] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    file_path NVARCHAR(500) UNIQUE NOT NULL,
    file_name NVARCHAR(255),
    file_size BIGINT,
    file_hash NVARCHAR(64),           -- MD5 hash for duplicate detection
    load_type NVARCHAR(50),           -- 'fullload' or 'append'
    table_name NVARCHAR(255),
    detected_delimiter NVARCHAR(5),   -- Detected CSV delimiter
    detected_headers NVARCHAR(MAX),   -- JSON array of headers
    is_reference_data BIT,            -- TRUE for reference data
    reference_config_inserted BIT DEFAULT 0,
    status NVARCHAR(50),              -- 'pending', 'processing', 'completed', 'error'
    created_at DATETIME2 DEFAULT GETDATE(),
    processed_at DATETIME2,
    error_message NVARCHAR(MAX)
);
```

#### Indexes
```sql
-- Performance indexes
CREATE INDEX IX_File_Monitor_Tracking_Status ON [ref].[File_Monitor_Tracking](status);
CREATE INDEX IX_File_Monitor_Tracking_Created ON [ref].[File_Monitor_Tracking](created_at);
CREATE INDEX IX_File_Monitor_Tracking_Table ON [ref].[File_Monitor_Tracking](table_name);
```

### Reference Data Configuration

#### Table: ref.Reference_Data_Cfg
```sql
-- Assumed structure based on system behavior
CREATE TABLE [ref].[Reference_Data_Cfg] (
    id INT IDENTITY(1,1) PRIMARY KEY,
    table_name NVARCHAR(255) UNIQUE NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    created_by NVARCHAR(100) DEFAULT 'AutoIngest',
    is_active BIT DEFAULT 1
);
```

## Interface Design

### File System Interface

#### Directory Structure
```
/dropoff/
├── reference_data_table/
│   ├── fullload/
│   │   ├── processed/
│   │   └── error/
│   └── append/
│       ├── processed/
│       └── error/
└── none_reference_data_table/
    ├── fullload/
    │   ├── processed/
    │   └── error/
    └── append/
        ├── processed/
        └── error/
```

#### File Naming Conventions
```
Input Files:
- airports.csv
- airport_frequencies_20241201.csv
- customer-data.csv

Processed Files:
- 20241201_120000_airports.csv
- 20241201_120001_airport_frequencies_20241201.csv

Error Files:
- ERROR_20241201_120000_airports.csv
- ERROR_20241201_120000_processing_failed.txt
```

### Configuration Interface

#### Environment Variables
```bash
# Database Configuration
db_host=localhost
db_name=production
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

## Performance Design

### Scalability Considerations

#### Horizontal Scaling
- Multiple monitor instances with folder partitioning
- Database connection pooling across instances
- Shared file system with atomic operations
- Distributed progress tracking

#### Vertical Scaling
- Memory optimization for large file processing
- CPU-efficient batch processing
- I/O optimization for file operations
- Database query optimization

### Performance Metrics

#### Key Performance Indicators
```python
Performance Targets:
- File Detection Latency: < 15 seconds
- Processing Throughput: > 1000 rows/second
- Memory Usage: < 512MB per 100MB file
- Database Connection Pool Utilization: < 80%
- Error Rate: < 1% of processed files
```

#### Monitoring Points
```python
Metrics Collection:
- File processing times by size
- Database operation latencies
- Memory usage patterns
- Error frequency and types
- System resource utilization
```

## Security Design

### Data Security
- Database connection encryption (TLS)
- Secure credential management
- SQL injection prevention through parameterized queries
- File access permission validation

### System Security
- Process isolation and resource limits
- Secure temporary file handling
- Audit logging for all operations
- Access control for system directories

## Disaster Recovery Design

### Backup Strategy
- Database backup integration
- Processed file archival
- Configuration backup
- System state snapshots

### Recovery Procedures
- Automatic service restart on failure
- Database connection failover
- File processing resumption
- State reconstruction from tracking data

This design specification provides the architectural foundation for implementing a robust, scalable, and maintainable reference data management system that meets all identified requirements while supporting future growth and enhancement needs.