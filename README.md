# Reference Data Auto Ingest System

Automated CSV data ingestion system for SQL Server (FastAPI + React) delivering: upload, auto format detection (delimiter + trailer), schema inference & safety downgrades (numeric/datetime), staged validation, backup/versioning, and detailed logging with progress streaming.

## System Configuration (Quick View)

| Category | Key Settings | Default / Example |
|----------|--------------|-------------------|
| Database | host / name / user / password | localhost / test / tester / ****** |
| Schemas  | data_schema / backup_schema / validation_sp_schema | ref / bkp / ref |
| Paths    | temp / archive / format | C:\data\reference_data\temp etc. |
| Limits   | max_upload_size | 20971520 (20MB) |
| Inference| INGEST_TYPE_INFERENCE / INGEST_DATE_THRESHOLD | 0 / 0.8 |
| Progress | INGEST_PROGRESS_INTERVAL | 5 |
| Logging  | backend/logs/system.log + ref.system_log | n/a |

Environment variables (.env):
```
db_host=localhost
db_name=test
db_user=tester
db_password=121@abc!
data_schema=ref
backup_schema=bkp
validation_sp_schema=ref
temp_location=C:\data\reference_data\temp
archive_location=C:\data\reference_data\archive
format_location=C:\data\reference_data\format
max_upload_size=20971520
INGEST_TYPE_INFERENCE=1
INGEST_DATE_THRESHOLD=0.8
```

### Automated CSV Ingestion Flow
1. Upload CSV (drag/drop) -> format detection (delimiter, text qualifier, header, trailer pattern regex)
2. Format (.fmt) file persisted (stores csv_format + optional inferred_schema)
3. Full file read (strings) -> header sanitize/deduplicate
4. Type inference sample (configurable rows) -> full-dataset validation pass downgrades:
    - Numeric columns with any non-numeric sampled value -> widened varchar
    - Datetime columns with any invalid parse across full set -> varchar(size)
5. Tables (stage, main, backup) created / recreated (full) or validated (append) with dynamic widening
6. Data cleaned (None/nan -> '') + datetime normalization (unparsable -> NULL)
7. Batched multi-row INSERT (≤990 rows batch) into stage
8. Validation stored procedure execution (template returns success unless customized)
9. Move stage -> main (full replace or append)
10. Archive original file, persist inferred schema
11. Progress + logs exposed via endpoints; trailer row removed if pattern or heuristic matches

### Log Refresh Behavior
- Frontend auto-refreshes /logs every 10s. If you do not see new entries:
   1. Confirm backend returns fresh JSON (curl http://localhost:8000/logs)
   2. Check browser dev tools (network) for caching; response should not be cached.
   3. Ensure reverse proxy (if any) not adding cache headers.
   4. Verify logger writes (tail backend/logs/system.log).
   5. If long-poll progress running, tab visibility throttling can delay timers; click Refresh button.

Add a Cache-Control header on /logs if needed for strict no-cache (future enhancement).

## Features

- **Web Interface Upload**: Upload CSV files via a modern React web interface
- **Flexible CSV Format Support**: Configurable delimiters, text qualifiers, and row terminators
- **Automatic Table Management**: Creates main tables, stage tables, and backup tables automatically
- **Data Validation**: Built-in validation with custom stored procedures
- **Full and Append Load Modes**: Support for both complete data replacement and incremental loading
- **Real-time Progress Tracking**: Live progress updates during file processing
- **Comprehensive Logging**: Detailed logging with error tracebacks for troubleshooting
- **Schema Management**: Automatic schema creation and validation

## Architecture

### Backend (Python FastAPI)
- **FastAPI**: REST API framework for handling file uploads and data processing
- **pyodbc**: SQL Server database connectivity
- **pandas**: CSV file processing and data manipulation
- **Async Processing**: Background task processing with real-time progress streaming

### Frontend (React)
- **Material-UI**: Modern, responsive user interface components
- **File Upload**: Drag-and-drop file upload with validation
- **Configuration Panel**: Dynamic CSV format configuration
- **Progress Monitoring**: Real-time progress display with error highlighting
- **Logs Viewer**: System logs with expandable details and auto-refresh

### Database (SQL Server)
- **ref schema**: Main data tables and validation stored procedures
- **bkp schema**: Backup tables with version tracking
- **Automated DDL**: Dynamic table and stored procedure creation

## Installation

### Prerequisites
- Python 3.8+
- Node.js 16+
- SQL Server (with appropriate permissions for schema creation)

### Backend Setup

1. **Install Python dependencies**:
   ```bash
   cd backend
   pip install -r requirements.txt
   ```

2. **Configure environment variables**:
   Copy the `.env` file and update with your database settings:
   ```
   db_host=localhost
   db_name=test
   db_user=tester
   db_password=121@abc!
   ```

3. **Create required directories** (Windows paths as specified in PRD):
   ```
   C:\data\reference_data\temp
   C:\data\reference_data\archive
   C:\data\reference_data\format
   ```

4. **Start the backend server**:
   ```bash
   python ../start_backend.py
   ```
   Or directly:
   ```bash
   cd backend
   python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

### Frontend Setup

1. **Install Node.js dependencies**:
   ```bash
   cd frontend
   npm install
   ```

2. **Start the React development server**:
   ```bash
   npm start
   ```
   The application will be available at `http://localhost:3000`

## Usage

### File Upload Process

1. **Access the Web Interface**: Navigate to `http://localhost:3000`

2. **Select CSV File**: Choose a CSV file (max 20MB) for upload

3. **Configure Format**: Set CSV format parameters:
   - **Header Delimiter**: Separator for header row (default: `|`)
   - **Column Delimiter**: Separator between columns (default: `|`)
   - **Row Delimiter**: Line ending format (default: `|""\r\n`)
   - **Text Qualifier**: Quote character for text fields (default: `"`)
   - **Skip Lines**: Number of lines to skip after header
   - **Trailer Pattern**: Optional pattern for trailer validation

4. **Choose Load Mode**:
   - **Full Load**: Replaces all existing data (with backup)
   - **Append Load**: Adds new data to existing table

5. **Upload and Process**: Click "Upload and Process" to start ingestion

6. **Monitor Progress**: Watch real-time progress updates and logs

### Table Naming Convention

Tables are created based on the CSV filename:
- `filename.csv` → `filename` (main table)
- `filename_stage` (staging table for validation)
- `filename_backup` (backup table with version tracking)

Supported filename patterns:
- `tablename.csv`
- `tablename.20250801.csv`
- `tablename.20250801000000.csv`

### Data Validation

The system automatically creates validation stored procedures:
- **Procedure Name**: `sp_ref_validate_{tablename}`
- **Schema**: `ref`
- **Returns**: JSON with validation results and issue details

Example validation result:
```json
{
  "validation_result": 0,
  "validation_issue_list": []
}
```

## API Endpoints

### Backend REST API

- `GET /`: Health check and system status
- `GET /config`: System configuration and delimiter options
- `POST /upload`: Upload CSV file with format configuration
- `POST /ingest/{filename}`: Trigger data ingestion with progress streaming
- `GET /logs`: Retrieve system logs

## Database Schema

### Main Tables (ref schema)
- All columns: `varchar(4000)` by default
- `ref_data_loadtime`: `datetime` (auto-populated)

### Backup Tables (bkp schema)
- Same structure as main table
- `version_id`: `int` for version tracking

### System Logs (ref schema)
- `system_log`: Comprehensive logging table

## Configuration

### Environment Variables (.env)
```bash
# Database
db_host=localhost
db_name=test
db_user=tester
db_password=121@abc!

# Schemas
data_schema=ref
backup_schema=bkp
validation_sp_schema=ref

# File locations
temp_location=C:\data\reference_data\temp
archive_location=C:\data\reference_data\archive
format_location=C:\data\reference_data\format

# File size limit (20MB)
max_upload_size=20971520
```

### CSV Format Options
- **Delimiters**: `,`, `;`, `|`, custom
- **Text Qualifiers**: `"`, `'`, `""`, custom
- **Row Delimiters**: `\r`, `\n`, `\r\n`, `|""\r\n`, custom

## Troubleshooting

### Common Issues

1. **Database Connection Errors**:
   - Verify SQL Server is running and accessible
   - Check credentials in `.env` file
   - Ensure user has schema creation permissions

2. **File Upload Failures**:
   - Check file size (max 20MB)
   - Ensure file has `.csv` extension
   - Verify temp directories exist

3. **Schema Validation Errors**:
   - Review column headers for invalid characters
   - Check for empty or duplicate column names

### Error Logs
- **File Logs**: `backend/logs/system.log`
- **Database Logs**: `ref.system_log` table
- **Web Interface**: Real-time error display with stack traces

## Development

### Backend Development
```bash
cd backend
python -m uvicorn app.main:app --reload
```

### Frontend Development
```bash
cd frontend
npm start
```

### Adding Custom Validation
Edit the generated stored procedure in SQL Server:
```sql
ALTER PROCEDURE [ref].[sp_ref_validate_tablename]
AS
BEGIN
    -- Add custom validation logic here
    -- Return JSON with validation results
END
```

## Production Deployment

### Backend
- Use production ASGI server (e.g., Gunicorn + Uvicorn)
- Configure appropriate logging levels
- Set up database connection pooling
- Enable HTTPS

### Frontend
- Build production bundle: `npm run build`
- Serve static files with web server (e.g., Nginx)
- Configure production API endpoints

### Security Considerations
- Use strong database passwords
- Implement user authentication (not included in this version)
- Configure firewall rules
- Enable SQL Server encryption
- Validate file uploads thoroughly

## License

This project is developed for internal use and follows company guidelines for reference data management.