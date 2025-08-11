# Reference Data Auto Ingest System

Enterprise-grade automated CSV data ingestion system for SQL Server with intelligent load type management, comprehensive backup/rollback capabilities, and professional TD Bank themed interface. Built with FastAPI backend and React frontend delivering advanced features including real-time progress tracking, automatic format detection, and versioned data recovery.

## Features

### Core Data Ingestion
- **Advanced CSV Processing**: Auto-detection of delimiters, text qualifiers, headers, and trailer patterns
- **Intelligent Load Type Management**: Automatic detection of Full ('F') vs Append ('A') loads with user override capabilities
- **Real-time Progress Streaming**: Live updates with row counts, phase tracking, and cancellation support
- **Comprehensive Error Handling**: Detailed logging with user-friendly messages and full stack traces

### Enterprise Backup & Rollback System
- **Automatic Backup Versioning**: Every full load creates timestamped backup with incremental version tracking
- **Point-in-time Recovery**: Restore main and stage tables to any previous backup version
- **CSV Export Capabilities**: Download main tables and specific backup versions as CSV files
- **Advanced Backup Browser**: Professional UI with data preview, filtering, and search capabilities

### Professional User Interface
- **TD Bank Corporate Theme**: Professional branding with corporate colors and styling
- **Material-UI Components**: Modern, responsive design optimized for desktop and mobile
- **Load Type Warning Dialogs**: Clear explanations and override options for data consistency conflicts
- **Rollback Management Interface**: Intuitive backup browsing and restoration workflow

### Database Integration
- **Three-Schema Architecture**: ref (main/stage), bkp (backup), dbo (configuration tables)
- **Reference_Data_Cfg Integration**: Automatic table registration with post-load procedure execution
- **Schema Synchronization**: Dynamic table creation and validation with migration support
- **Connection Pooling**: Optimized database connections with retry logic and health monitoring

## Quick Start

### Prerequisites
- Python 3.8+
- Node.js 16+
- SQL Server (with schema creation permissions)

### Installation & Setup

1. **Clone and Setup Environment**:
   ```bash
   # Use automated installer
   ./install.sh
   
   # Or start development environment directly
   ./start_dev.sh
   ```

2. **Manual Setup** (if preferred):
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
   cp dot_env.example .env
   # Edit .env with your database credentials
   ```

3. **Configure Database Connection** (edit .env file):
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
   temp_location=C:\data\reference_data\temp
   archive_location=C:\data\reference_data\archive
   format_location=C:\data\reference_data\format

   # Upload Limits
   max_upload_size=20971520  # 20MB
   ```

4. **Create Required Directories**:
   ```bash
   # Windows paths
   mkdir "C:\data\reference_data\temp"
   mkdir "C:\data\reference_data\archive"
   mkdir "C:\data\reference_data\format"
   
   # Linux/Mac paths (if different)
   mkdir -p ./data/reference_data/{temp,archive,format}
   ```

5. **Start the Application**:
   ```bash
   # Terminal 1: Backend server
   python start_backend.py
   
   # Terminal 2: Frontend development server
   cd frontend && npm start
   ```

6. **Access the Application**: Navigate to `http://localhost:3000`

## Usage Guide

### File Upload Workflow

1. **Select CSV File**: Choose a CSV file (max 20MB) using drag-and-drop interface
2. **Format Detection**: System automatically detects CSV format parameters
3. **Configure Settings**: Adjust delimiters, text qualifiers, and trailer patterns if needed
4. **Load Type Selection**: Choose "Full Load" (replace data) or "Append Load" (add data)
5. **Load Type Verification**: System checks for conflicts and shows warning dialog if needed
6. **Upload & Process**: Monitor real-time progress with detailed status updates

### Load Type Management

The system intelligently manages load types to ensure data consistency:

#### Automatic Load Type Determination
- **First Load**: Uses your selected mode (Full='F' or Append='A')
- **Subsequent Loads**: Analyzes existing data patterns
- **Conflict Detection**: Warns when user mode conflicts with data history
- **User Override**: Professional dialog allows forcing specific load type

#### Load Type Warning Dialog
When conflicts are detected, you'll see:
- Clear explanation of the situation
- Current data patterns and requested mode
- Override options with consequences explained
- Professional TD Bank themed interface

### Backup and Rollback System

#### Automatic Backups
- Every full load automatically creates a versioned backup
- Backup tables include version_id for tracking
- Schema compatibility checks ensure data integrity
- Metadata preservation maintains audit trail

#### Rollback Interface
1. **Access Rollback Manager**: Available in main interface
2. **Browse Backups**: View all backup tables with version history
3. **Preview Data**: Sample data display with filtering options
4. **Execute Rollback**: One-click restoration to selected version
5. **Verify Success**: Automatic validation with row count confirmation

#### CSV Export Options
- **Export Main Table**: Download current data as CSV
- **Export Backup Version**: Download specific historical version
- **Streaming Export**: Efficient handling of large datasets

### Table Management

#### Naming Conventions
Tables are created based on CSV filename:
- `filename.csv` → `filename` (main table)
- `filename_stage` (staging table for validation)
- `filename_backup` (backup table with version tracking)

Supported filename patterns:
- `tablename.csv`
- `tablename.20250810.csv`
- `tablename.20250810143000.csv`

#### Schema Structure
- **Main Tables**: All columns as varchar(4000) with ref_data_loadtime and loadtype
- **Stage Tables**: Identical structure for validation processing
- **Backup Tables**: Main structure plus version_id for tracking

### Configuration Options

#### CSV Format Settings
- **Delimiters**: `,` `;` `|` (custom supported)
- **Text Qualifiers**: `"` `'` `""` (custom supported)  
- **Row Terminators**: `\r` `\n` `\r\n` `|""\r\n` (custom supported)
- **Header Options**: Skip lines, trailer patterns
- **Auto-Detection**: Smart format recognition with confidence scoring

#### System Settings
```bash
# Performance Settings
INGEST_TYPE_INFERENCE=1           # Enable data type inference
INGEST_DATE_THRESHOLD=0.8         # Date detection threshold
INGEST_PROGRESS_INTERVAL=5        # Progress update frequency

# File Processing
max_upload_size=20971520          # 20MB default limit
COMMIT_BATCH_SIZE=1000           # Database batch size
```

## API Reference

### Core Endpoints
- `GET /` - System health check and status
- `GET /config` - Configuration options and delimiter choices
- `GET /features` - Feature flags and system capabilities

### File Processing
- `POST /detect-format` - Auto-detect CSV format parameters
- `POST /upload` - Upload file with format configuration
- `POST /ingest/{filename}` - Stream ingestion with load type options
- `GET /progress/{key}` - Real-time progress tracking
- `POST /progress/{key}/cancel` - Cancel ongoing processing

### Load Type Management
- `POST /verify-load-type` - Check load type compatibility
- `GET /reference-data-config` - View Reference_Data_Cfg records

### Backup & Rollback
- `GET /backups` - List all backup tables with metadata
- `GET /backups/{base_name}/versions` - Get version history
- `GET /backups/{base_name}/versions/{version_id}` - View version data
- `POST /backups/{base_name}/rollback/{version_id}` - Execute rollback
- `GET /backups/{base_name}/export-main` - Export main table CSV
- `GET /backups/{base_name}/versions/{version_id}/export` - Export version CSV

### System Monitoring
- `GET /logs` - System logs with auto-refresh
- `GET /db/pool-stats` - Database connection pool statistics
- `GET /schema/{table_name}` - Table schema information

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   ```bash
   # Check connection settings
   python -c "import pyodbc; print(pyodbc.drivers())"
   
   # Verify database connectivity
   python debug_sync.py
   ```

2. **File Upload Failures**:
   - Ensure file size under 20MB limit
   - Verify CSV file extension
   - Check temp directories exist and are writable
   - Review error logs for detailed messages

3. **Load Type Conflicts**:
   - Review warning dialog explanations
   - Check existing data patterns in table
   - Use override options if business logic requires it
   - Consult with data stakeholders on consistency

4. **Schema Validation Issues**:
   - Verify column names are valid SQL identifiers
   - Check for empty or duplicate headers
   - Review CSV format detection results
   - Use manual format configuration if needed

### Error Analysis

1. **File-based Logs**: `backend/logs/system.log`
2. **Database Logs**: `ref.system_log` table
3. **Web Interface**: Real-time error display with expandable details
4. **API Responses**: Detailed error messages with request IDs

### Performance Optimization

1. **Large Files**: System handles up to 20MB with streaming processing
2. **Concurrent Users**: Background task processing supports multiple uploads
3. **Database Performance**: Connection pooling optimizes resource usage
4. **Memory Usage**: Chunked processing prevents memory issues

## Development

### Backend Development
```bash
cd backend
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Development
```bash
cd frontend
npm start  # Development server with hot reload
npm run build  # Production build
```

### Testing
```bash
# Backend tests
cd backend
python -m pytest tests/

# Test security hardening
python test_parameterized_sql.py
```

### Adding Custom Features

1. **Custom Validation**: Edit generated stored procedures
   ```sql
   ALTER PROCEDURE [ref].[sp_ref_validate_tablename]
   AS BEGIN
       -- Add custom validation logic
       SELECT '{"validation_result": 0, "validation_issue_list": []}' AS ValidationResult
   END
   ```

2. **Post-load Processing**: Customize `usp_reference_data_postLoad` procedure
3. **Load Type Logic**: Modify determination rules in `database.py`
4. **UI Themes**: Adjust TD Bank branding in React components

## Production Deployment

### Backend Deployment
```bash
# Production ASGI server
pip install gunicorn
gunicorn -w 4 -k uvicorn.workers.UvicornWorker app.main:app --bind 0.0.0.0:8000

# Or use uvicorn directly
uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
```

### Frontend Deployment
```bash
# Build production assets
npm run build

# Serve with web server (nginx example)
server {
    listen 80;
    root /path/to/frontend/build;
    location / {
        try_files $uri $uri/ /index.html;
    }
    location /api/ {
        proxy_pass http://backend:8000/;
    }
}
```

### Environment Configuration
- Use environment-specific .env files
- Configure database connection pooling
- Set up monitoring and alerting
- Enable HTTPS and security headers
- Configure backup retention policies

### Security Considerations
- **Database Security**: Use dedicated service accounts with minimal permissions
- **Network Security**: Implement firewall rules and VPN access
- **Data Security**: Enable SQL Server encryption and audit logging
- **Application Security**: Regular security updates and vulnerability scanning
- **Access Control**: Implement user authentication (future enhancement)

## Architecture Details

### Database Schema Design
```sql
-- Main table example
CREATE TABLE [ref].[airports] (
    [id] varchar(4000),
    [name] varchar(4000),
    [city] varchar(4000),
    [ref_data_loadtime] datetime DEFAULT GETDATE(),
    [loadtype] varchar(255)
)

-- Backup table example
CREATE TABLE [bkp].[airports_backup] (
    [id] varchar(4000),
    [name] varchar(4000), 
    [city] varchar(4000),
    [ref_data_loadtime] datetime,
    [loadtype] varchar(255),
    [version_id] int NOT NULL
)

-- Configuration table
CREATE TABLE [dbo].[Reference_Data_Cfg] (
    [Id] int IDENTITY(1,1) PRIMARY KEY,
    [TableName] varchar(255) NOT NULL,
    [LastUpdated] datetime DEFAULT GETDATE(),
    [Status] varchar(50),
    [RecordCount] int
)
```

### Security Architecture
- **Parameterized Queries**: All SQL operations use parameter binding
- **Input Validation**: Comprehensive sanitization of user inputs
- **Error Sanitization**: Safe error messages without sensitive information
- **File Upload Security**: Type validation and secure path handling
- **Connection Security**: Encrypted connections and credential management

## System Requirements

### Minimum Requirements
- **Python**: 3.8+
- **Node.js**: 16+
- **SQL Server**: 2016+ with schema creation permissions
- **Memory**: 4GB RAM
- **Storage**: 100MB application + data storage requirements
- **Network**: HTTP/HTTPS access for web interface

### Recommended Production Requirements
- **CPU**: 4+ cores for concurrent processing
- **Memory**: 8GB+ RAM for large file processing
- **Storage**: SSD storage for temp files and database
- **Database**: Dedicated SQL Server instance with backup strategy
- **Network**: Load balancer for high availability

## License

This project is developed for internal use and follows company guidelines for reference data management.

## Support

For technical support and feature requests:
1. Review this documentation and troubleshooting guide
2. Check system logs for detailed error information
3. Verify environment configuration and database connectivity
4. Contact development team with specific error messages and use case details

**Status**: ✅ **PRODUCTION READY WITH ENTERPRISE FEATURES**