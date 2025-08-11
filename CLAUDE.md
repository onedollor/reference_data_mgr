# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Reference Data Auto Ingest System is a production-ready data management application designed to automate the ingestion of CSV reference data into SQL Server databases. The system features a FastAPI backend with React frontend, providing both web interface uploads, automated file processing capabilities, comprehensive backup/rollback functionality, and intelligent load type management.

## Current Architecture

### Backend (Python FastAPI)
- **Location**: `/backend/app/main.py`
- **Port**: 8000 (default)
- **Framework**: FastAPI with async support
- **Database**: SQL Server via pyodbc with connection pooling
- **Key Features**:
  - CSV format auto-detection with configurable parameters
  - Real-time progress streaming via Server-Sent Events
  - Parameterized SQL queries (security hardened)
  - Comprehensive error handling with full tracebacks
  - Background task processing for large files
  - Database schema synchronization and migration
  - **Load type verification and override system**
  - **Backup and rollback management with version control**
  - **CSV export functionality for main tables and backup versions**

### Frontend (React)
- **Location**: `/frontend/src/`
- **Port**: 3000 (development)
- **Framework**: React 18 with Material-UI components
- **Key Components**:
  - `FileUploadComponent.js`: Drag-and-drop file upload with load type verification
  - `LoadTypeWarningDialog.js`: **NEW** - Load type mismatch warning and override dialog
  - `RollbackManager.js`: **NEW** - Comprehensive backup management and rollback interface
  - `ProgressDisplay.js`: Real-time ingestion progress tracking
  - `LogsDisplay.js`: System logs with auto-refresh
  - `ConfigurationPanel.js`: System configuration display
  - `ReferenceDataConfigDisplay.js`: Reference Data Configuration viewer

### Database Integration
- **Primary Database**: SQL Server
- **Schemas**: 
  - `ref`: Main data tables and validation procedures
  - `bkp`: **Enhanced** Backup tables with versioning and metadata columns
  - `dbo`: Reference_Data_Cfg table for system configuration
- **Connection**: pyodbc with connection pooling and retry logic
- **Security**: Parameterized queries prevent SQL injection
- **New Features**:
  - **Load type column** (`loadtype`) in all table types (main, stage, backup)
  - **Version control** with `version_id` in backup tables
  - **Static timestamp handling** for `ref_data_loadtime` consistency

### File Processing
- **Upload Location**: `C:\data\reference_data\temp`
- **Archive Location**: `C:\data\reference_data\archive`
- **Format Storage**: `C:\data\reference_data\format`
- **Supported Formats**: CSV (RFC 4180 compliant)
- **Size Limit**: 20MB (configurable)

## Load Type Management System

### Load Type Determination Rules
The system intelligently determines load types based on existing data and user preferences:

1. **Override Priority**: User-specified override takes precedence
2. **First Load**: Uses requested load mode ('F' for full, 'A' for append)
3. **Existing Data Analysis**:
   - Only 'F' exists → Use 'F'
   - Only 'A' exists → Use 'A'
   - Both 'F' and 'A' exist → Use 'F'
   - No existing loadtype data → Use requested mode

### Load Type Verification Workflow
1. **Pre-upload Verification**: `/verify-load-type` endpoint checks for mismatches
2. **Warning Dialog**: `LoadTypeWarningDialog` displays mismatch information
3. **User Override**: Option to override with 'Full' or 'Append' load type
4. **Override Application**: `override_load_type` parameter forces specific load type

### Database Schema Changes
All tables now include the `loadtype` column:
```sql
-- Main table (ref schema)
CREATE TABLE [ref].[tablename] (
    [column1] varchar(4000),
    [column2] varchar(4000),
    [loadtype] varchar(255),        -- NEW: Load type tracking
    [ref_data_loadtime] datetime DEFAULT GETDATE()
)

-- Backup table (bkp schema) 
CREATE TABLE [bkp].[tablename_backup] (
    [column1] varchar(4000),
    [column2] varchar(4000),
    [loadtype] varchar(255),        -- NEW: Load type tracking
    [ref_data_loadtime] datetime,   -- Static timestamp from main table
    [version_id] int NOT NULL       -- Version control
)
```

## Backup and Rollback System

### Backup Table Management
- **Automatic Versioning**: Each full load creates a new version in backup table
- **Version Tracking**: `version_id` column increments with each backup
- **Metadata Preservation**: All original columns plus version information
- **Schema Validation**: Backup tables validated for compatibility with main/stage tables

### Rollback Functionality
- **Version Selection**: Users can select any available version for rollback
- **Data Restoration**: Complete replacement of main table data with selected version
- **Progress Tracking**: Real-time feedback during rollback operations
- **Validation**: Ensures backup version exists and is compatible

### Export Capabilities
- **Main Table Export**: Current data export to CSV format
- **Backup Version Export**: Export specific backup versions to CSV
- **Format Handling**: Proper CSV escaping and filename generation
- **Streaming Download**: Efficient handling of large datasets

## Enhanced API Endpoints

### Load Type Management
- **`POST /verify-load-type`**: Verify load type compatibility and detect mismatches
  - Parameters: `filename`, `load_mode`
  - Returns: Verification result with mismatch information and existing load types

### Backup Management
- **`GET /backups`**: List all backup tables with validation status
- **`GET /backups/{base_name}/versions`**: Get all versions for a backup table
- **`GET /backups/{base_name}/versions/{version_id}`**: View specific backup version data
- **`POST /backups/{base_name}/rollback/{version_id}`**: Rollback to specific version
- **`GET /backups/{base_name}/export-main`**: Export current main table as CSV
- **`GET /backups/{base_name}/versions/{version_id}/export`**: Export backup version as CSV

### Enhanced Upload Endpoint
- **`POST /upload`**: Enhanced with `override_load_type` parameter
  - New Parameter: `override_load_type` - Optional override for load type determination

### Configuration and Status
- **`GET /reference-data-config`**: Retrieve Reference_Data_Cfg table contents
- **`GET /features`**: System feature flags and capabilities

## Frontend Enhancements

### Load Type Warning Dialog
- **Component**: `LoadTypeWarningDialog.js`
- **Functionality**:
  - Displays load type mismatch warnings
  - Shows current situation and system decision
  - Provides override options (Full/Append)
  - Explains impact of override selection
  - TD Bank corporate styling

### Rollback Manager
- **Component**: `RollbackManager.js`
- **Features**:
  - List all backup tables with status indicators
  - Expandable version history for each table
  - Version data preview with advanced filtering
  - Export functionality for main tables and backup versions
  - Rollback confirmation with safety checks
  - Real-time status updates

### Enhanced File Upload
- **Load Type Verification**: Pre-upload verification workflow
- **Override Selection**: Seamless integration with load type dialog
- **Improved Error Handling**: Better user experience with detailed feedback

## Security and Performance Enhancements

### Parameterized Queries
All database operations use parameterized queries to prevent SQL injection:
- Load type determination queries
- Backup table operations
- Export data retrieval
- Version management operations

### Load Type Validation
- Input sanitization for load type values
- Regex validation for table and column names
- Safe dynamic SQL construction where necessary

### Performance Optimizations
- Connection pooling for database operations
- Streaming responses for large exports
- Efficient backup version queries
- Chunked data processing for large datasets

## Development Environment

### Prerequisites
- Python 3.8+ with virtual environment
- Node.js 16+ for frontend development
- SQL Server with schema creation permissions
- pyodbc drivers installed

### Environment Setup
```bash
# Backend setup
cd backend
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
pip install -r requirements.txt

# Frontend setup
cd frontend
npm install

# Environment configuration
cp .env.example .env
# Edit .env with database credentials
```

### Key Environment Variables
```bash
# Database Configuration
db_host=localhost
db_name=test
db_user=your_username
db_password=your_password

# Schema Configuration
data_schema=ref
backup_schema=bkp
validation_sp_schema=ref

# File Locations
data_drop_location=C:\data\reference_data
temp_location=C:\data\reference_data\temp
archive_location=C:\data\reference_data\archive
format_location=C:\data\reference_data\format

# Upload Limits
max_upload_size=20971520  # 20MB
```

### Development Scripts
- `install.sh`: Automated environment setup
- `start_dev.sh`: Start both backend and frontend
- `start_backend.py`: Backend server startup
- `shutdown_server.sh`: Clean shutdown

## Data Processing Pipeline

### Enhanced Ingestion Flow

#### 1. File Upload Phase
- Validate file type and size limits
- **Load Type Verification**: Check for potential mismatches
- **User Override**: Allow load type override if needed
- Save to temporary location with timestamp
- Create format configuration file (.fmt)
- Initialize progress tracking

#### 2. Format Detection Phase
- Auto-detect delimiters and text qualifiers
- Identify header and trailer patterns
- Estimate column count and data types
- Generate format suggestions for user

#### 3. Schema Preparation Phase
- **Load Type Determination**: Apply rules or user override
- Create/validate table schemas (main, stage, backup)
- **Add loadtype column** to all table types
- Ensure backup table version compatibility

#### 4. Data Loading Phase
- Load data into stage table with progress tracking
- **Apply load type** to all inserted records
- Execute validation stored procedures
- Handle validation results

#### 5. Data Movement Phase
- **Create backup version** before full load replacement
- Move validated data to main table
- **Preserve static timestamps** in backup
- Update version tracking

#### 6. Cleanup and Archival
- Archive processed files with timestamps
- Clean up temporary files
- Update system logs
- **Update backup metadata**

## Table Management

### Enhanced Naming Conventions
- Input: `filename.csv` → Table: `filename`
- Input: `data.20250810.csv` → Table: `data`
- Input: `report.20250810143000.csv` → Table: `report`

### Enhanced Schema Structure
```sql
-- Main table (ref schema)
CREATE TABLE [ref].[tablename] (
    [column1] varchar(4000),
    [column2] varchar(4000),
    [loadtype] varchar(255),        -- Load type ('F'/'A')
    [ref_data_loadtime] datetime DEFAULT GETDATE()
)

-- Stage table (ref schema)
CREATE TABLE [ref].[tablename_stage] (
    -- Same structure as main table including loadtype
    [loadtype] varchar(255),
    [ref_data_loadtime] datetime
)

-- Backup table (bkp schema)
CREATE TABLE [bkp].[tablename_backup] (
    -- All main table columns plus:
    [loadtype] varchar(255),        -- Preserved load type
    [ref_data_loadtime] datetime,   -- Static timestamp 
    [version_id] int NOT NULL       -- Version control
)
```

### Load Modes
- **Full Load**: Complete replacement with automatic versioned backup
- **Append Load**: Incremental addition with load type tracking

## Testing Strategy

### Manual Testing
- Use provided test CSV files with various formats
- Test load type verification and override workflows
- Test rollback functionality with multiple versions
- Verify export functionality for main and backup tables
- Test error conditions (invalid files, connection failures)
- Validate security measures and input sanitization

### Automated Testing
- `test_parameterized_sql.py`: SQL injection prevention tests
- `test_trailer_and_type_adjust.py`: Trailer detection tests
- `test_type_inference.py`: Data type inference tests
- `test_backup_schema.py`: Backup table schema validation

## Production Considerations

### Performance Optimizations
- Connection pooling for database operations
- Streaming responses for large file processing and exports
- Chunked data loading for memory efficiency
- Background task processing for concurrent uploads
- Efficient backup version queries

### Security Best Practices
- All database queries use parameterized statements
- Load type input validation and sanitization
- File upload restrictions and validation
- Error message sanitization
- Secure credential management
- Safe dynamic SQL construction

### Monitoring and Logging
- Comprehensive database and file logging
- Real-time progress tracking
- Error tracking with full stack traces
- System health monitoring endpoints
- Load type verification logging
- Backup/rollback operation tracking

## Troubleshooting

### Common Issues
1. **Database Connection Failures**: Check credentials and network connectivity
2. **File Upload Errors**: Verify file size limits and format requirements
3. **Load Type Conflicts**: Use verification and override system
4. **Backup Table Issues**: Validate schema compatibility
5. **Permission Issues**: Ensure database user has schema creation permissions
6. **Memory Issues**: Monitor large file processing and adjust chunk sizes

### Debug Features
- Detailed error logging with stack traces
- Progress tracking for long-running operations
- Database connection pool monitoring
- Real-time log streaming in web interface
- Load type verification debugging
- Backup operation status tracking

## Architecture Notes

- **Async Design**: FastAPI with async/await for concurrent operations
- **Enhanced Error Handling**: Comprehensive exception handling with user-friendly messages
- **Security First**: All user inputs are validated and sanitized with parameterized queries
- **Scalability**: Connection pooling and background task processing
- **Maintainability**: Modular design with clear separation of concerns
- **Data Integrity**: Comprehensive backup system with version control
- **User Experience**: Intelligent load type management with user override capabilities

This system is production-ready and has been enhanced with advanced data management features while maintaining security hardening against common vulnerabilities and providing high performance and reliability for reference data management operations.