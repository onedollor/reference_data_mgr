# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Reference Data Auto Ingest System is a production-ready data management application designed to automate the ingestion of CSV reference data into SQL Server databases. The system features a FastAPI backend with React frontend, providing both web interface uploads and automated file processing capabilities.

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

### Frontend (React)
- **Location**: `/frontend/src/`
- **Port**: 3000 (development)
- **Framework**: React 18 with Material-UI components
- **Key Components**:
  - `FileUploadComponent.js`: Drag-and-drop file upload with validation
  - `ProgressDisplay.js`: Real-time ingestion progress tracking
  - `LogsDisplay.js`: System logs with auto-refresh
  - `ConfigurationPanel.js`: System configuration display

### Database Integration
- **Primary Database**: SQL Server
- **Schemas**: 
  - `ref`: Main data tables and validation procedures
  - `bkp`: Backup tables with versioning
- **Connection**: pyodbc with connection pooling and retry logic
- **Security**: Parameterized queries prevent SQL injection

### File Processing
- **Upload Location**: `C:\data\reference_data\temp`
- **Archive Location**: `C:\data\reference_data\archive`
- **Format Storage**: `C:\data\reference_data\format`
- **Supported Formats**: CSV (RFC 4180 compliant)
- **Size Limit**: 20MB (configurable)

## Recent Security Improvements

### SQL Injection Vulnerability Fixes
- **Files Updated**: `database.py`, `ingest.py`, `logger.py`
- **Changes Made**:
  - Implemented parameterized queries for all user inputs
  - Added safe dynamic SQL construction for schema operations
  - Secured file upload validation processes
  - Enhanced error handling with sanitized output

### Security Measures Implemented
- Input validation and sanitization
- File type and size restrictions
- SQL injection prevention through parameterized queries
- Safe column name sanitization with regex validation
- Error message sanitization to prevent information leakage

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

## API Architecture

### Core Endpoints
- `GET /`: Health check and system status
- `GET /config`: System configuration and delimiter options
- `POST /detect-format`: Auto-detect CSV format parameters
- `POST /upload`: File upload with format configuration
- `POST /ingest/{filename}`: Stream ingestion progress
- `GET /logs`: Retrieve system logs with no-cache headers
- `GET /schema/{table_name}`: Get table schema information
- `GET /progress/{key}`: Real-time progress tracking

### Advanced Features
- Server-Sent Events for real-time progress streaming
- Background task processing for large files
- Automatic CSV format detection and suggestion
- Database connection pooling and retry logic
- Comprehensive logging to both file and database

## Data Processing Pipeline

### 1. File Upload Phase
- Validate file type and size limits
- Save to temporary location with timestamp
- Create format configuration file (.fmt)
- Initialize progress tracking

### 2. Format Detection Phase
- Auto-detect delimiters and text qualifiers
- Identify header and trailer patterns
- Estimate column count and data types
- Generate format suggestions for user

### 3. Ingestion Phase
- Connect to database with retry logic
- Create/validate table schemas (main, stage, backup)
- Load data into stage table with progress tracking
- Execute validation stored procedures
- Move validated data to main table
- Archive processed files with timestamps

### 4. Validation and Cleanup
- Run custom validation procedures
- Generate JSON validation results
- Handle validation failures gracefully
- Clean up temporary files
- Update system logs

## Table Management

### Naming Conventions
- Input: `filename.csv` → Table: `filename`
- Input: `data.20250810.csv` → Table: `data`
- Input: `report.20250810143000.csv` → Table: `report`

### Schema Structure
```sql
-- Main table (ref schema)
CREATE TABLE [ref].[tablename] (
    [column1] varchar(4000),
    [column2] varchar(4000),
    [ref_data_loadtime] datetime DEFAULT GETDATE()
)

-- Stage table (ref schema)
CREATE TABLE [ref].[tablename_stage] (
    -- Same structure as main table
)

-- Backup table (bkp schema)
CREATE TABLE [bkp].[tablename_backup] (
    -- Main table columns plus:
    [version_id] int NOT NULL
)
```

### Load Modes
- **Full Load**: Complete replacement with automatic backup
- **Append Load**: Incremental addition with dataset_id tracking

## Testing Strategy

### Manual Testing
- Use provided test CSV files with various formats
- Test error conditions (invalid files, connection failures)
- Verify progress tracking and real-time updates
- Validate security measures and input sanitization

### Automated Testing
- `test_parameterized_sql.py`: SQL injection prevention tests
- `test_trailer_and_type_adjust.py`: Trailer detection tests
- `test_type_inference.py`: Data type inference tests

## Production Considerations

### Performance Optimizations
- Connection pooling for database operations
- Streaming responses for large file processing
- Chunked data loading for memory efficiency
- Background task processing for concurrent uploads

### Security Best Practices
- All database queries use parameterized statements
- Input validation and sanitization
- File upload restrictions and validation
- Error message sanitization
- Secure credential management

### Monitoring and Logging
- Comprehensive database and file logging
- Real-time progress tracking
- Error tracking with full stack traces
- System health monitoring endpoints

## Troubleshooting

### Common Issues
1. **Database Connection Failures**: Check credentials and network connectivity
2. **File Upload Errors**: Verify file size limits and format requirements
3. **Permission Issues**: Ensure database user has schema creation permissions
4. **Memory Issues**: Monitor large file processing and adjust chunk sizes

### Debug Features
- Detailed error logging with stack traces
- Progress tracking for long-running operations
- Database connection pool monitoring
- Real-time log streaming in web interface

## Architecture Notes

- **Async Design**: FastAPI with async/await for concurrent operations
- **Error Handling**: Comprehensive exception handling with user-friendly messages
- **Security First**: All user inputs are validated and sanitized
- **Scalability**: Connection pooling and background task processing
- **Maintainability**: Modular design with clear separation of concerns

This system is production-ready and has been hardened against common security vulnerabilities while maintaining high performance and reliability for reference data management operations.