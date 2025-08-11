# API Documentation - Reference Data Auto Ingest System

## Overview

The Reference Data Auto Ingest System provides a comprehensive REST API for managing CSV data ingestion, backup/rollback operations, and load type management. All endpoints support JSON responses and follow RESTful conventions.

**Base URL**: `http://localhost:8000`

## Authentication

Currently, the API does not implement authentication. All endpoints are publicly accessible on the configured port.

## Core Endpoints

### System Health and Configuration

#### `GET /`
**Description**: Health check and system status  
**Response**:
```json
{
  "message": "Reference Data Auto Ingest System is running",
  "version": "1.0.0",
  "status": "healthy"
}
```

#### `GET /config`
**Description**: Get system configuration including delimiters and file size limits  
**Response**:
```json
{
  "max_upload_size": 20971520,
  "supported_formats": ["csv"],
  "default_delimiters": {
    "header_delimiter": "|",
    "column_delimiter": "|",
    "row_delimiter": "|\"\"\\r\\n",
    "text_qualifier": "\""
  },
  "delimiter_options": {
    "header_delimiter": [",", ";", "|"],
    "column_delimiter": [",", ";", "|"],
    "row_delimiter": ["\\r", "\\n", "\\r\\n", "|\"\"\\r\\n"],
    "text_qualifier": ["\"", "'", "\"\""]
  }
}
```

#### `GET /features`
**Description**: System feature flags and capabilities  
**Response**:
```json
{
  "type_inference": "0",
  "chunk_size": "0",
  "batch_size": "0",
  "append_mode_schema_sync": true,
  "pooling": true,
  "dry_run": "0",
  "stream_commit_rows": "50000"
}
```

## File Processing Endpoints

### CSV Format Detection

#### `POST /detect-format`
**Description**: Auto-detect CSV format parameters from uploaded file  
**Content-Type**: `multipart/form-data`  
**Parameters**:
- `file`: CSV file to analyze

**Response**:
```json
{
  "filename": "data.csv",
  "file_size": 1024,
  "detected_format": {
    "header_delimiter": ",",
    "column_delimiter": ",",
    "row_delimiter": "\\n",
    "text_qualifier": "\"",
    "has_header": true,
    "has_trailer": false,
    "skip_lines": 0,
    "trailer_line": null
  },
  "confidence": 0.95,
  "analysis": {
    "encoding": "utf-8",
    "estimated_columns": 5,
    "sample_rows": 10,
    "trailer_format": null,
    "sample_data": [
      ["col1", "col2", "col3"],
      ["val1", "val2", "val3"]
    ]
  },
  "error": null
}
```

### Load Type Management

#### `POST /verify-load-type`
**Description**: Verify load type compatibility between requested mode and existing table data  
**Content-Type**: `multipart/form-data`  
**Parameters**:
- `filename` (string): CSV filename to process
- `load_mode` (string): Requested load mode ("full" or "append")

**Response**:
```json
{
  "table_name": "airports",
  "requested_load_mode": "full", 
  "requested_load_type": "F",
  "determined_load_type": "A",
  "has_mismatch": true,
  "existing_load_types": ["A"],
  "table_exists": true,
  "explanation": "Based on existing data, load type will be 'A' but you requested 'F'"
}
```

### File Upload and Processing

#### `POST /upload`
**Description**: Upload CSV file with format configuration and optional load type override  
**Content-Type**: `multipart/form-data`  
**Parameters**:
- `file` (file): CSV file to upload
- `header_delimiter` (string, default: "|"): Header row delimiter
- `column_delimiter` (string, default: "|"): Column separator
- `row_delimiter` (string, default: "|\"\"\\r\\n"): Row terminator
- `text_qualifier` (string, default: "\""): Text quote character
- `skip_lines` (integer, default: 0): Lines to skip after header
- `trailer_line` (string, optional): Trailer pattern for validation
- `load_mode` (string, default: "full"): Load mode ("full" or "append")
- `override_load_type` (string, optional): Force load type ("Full" or "Append")

**Response**:
```json
{
  "message": "File uploaded successfully",
  "filename": "data.csv",
  "file_size": 2048,
  "status": "processing",
  "progress_key": "data_csv"
}
```

#### `POST /ingest/{filename}`
**Description**: Stream ingestion progress for a specific file  
**Parameters**:
- `filename` (path): File to ingest
- `load_mode` (query, optional): Load mode ("full" or "append")

**Response**: Server-Sent Events stream with progress updates
```
data: Starting ingestion for data.csv
data: Processing row 100 of 1000 (10%)
data: Validation completed successfully
data: Ingestion completed: 1000 rows processed
```

#### `GET /ingest/{filename}`
**Description**: Get ingestion prerequisites and instructions  
**Response**:
```json
{
  "message": "Use POST /ingest/{filename} to start streaming ingestion output.",
  "filename": "data.csv",
  "file_present": true,
  "format_present": true,
  "next_steps": [
    "Upload the file first via POST /upload (multipart form field 'file')",
    "Then call: curl -N -X POST http://host:8000/ingest/yourfile.csv",
    "Optionally include load_mode query param: ?load_mode=append"
  ],
  "example_curl_upload": "curl -F file=@test.csv -F load_mode=full http://localhost:8000/upload",
  "example_curl_ingest_stream": "curl -N -X POST http://localhost:8000/ingest/data.csv",
  "dry_run": "0"
}
```

## Backup and Rollback Management

### Backup Listing

#### `GET /backups`
**Description**: List all backup tables with validation status  
**Response**:
```json
{
  "backups": [
    {
      "backup_table": "airports_backup",
      "base_name": "airports",
      "version_count": 3,
      "latest_version": 3,
      "has_main": true,
      "has_stage": true
    },
    {
      "backup_table": "countries_backup", 
      "base_name": "countries",
      "version_count": 1,
      "latest_version": 1,
      "has_main": true,
      "has_stage": false
    }
  ]
}
```

### Version Management

#### `GET /backups/{base_name}/versions`
**Description**: Get all versions for a specific backup table  
**Parameters**:
- `base_name` (path): Base table name

**Response**:
```json
{
  "base_name": "airports",
  "versions": [
    {
      "version_id": 1,
      "row_count": 500,
      "created_date": "2025-01-08T10:30:00"
    },
    {
      "version_id": 2,
      "row_count": 750,
      "created_date": "2025-01-09T14:15:00"
    },
    {
      "version_id": 3,
      "row_count": 1000,
      "created_date": "2025-01-10T09:45:00"
    }
  ]
}
```

#### `GET /backups/{base_name}/versions/{version_id}`
**Description**: View specific backup version data with pagination  
**Parameters**:
- `base_name` (path): Base table name
- `version_id` (path): Version identifier
- `limit` (query, default: 50): Number of rows to return
- `offset` (query, default: 0): Starting row offset

**Response**:
```json
{
  "base_name": "airports",
  "version_id": 2,
  "total_rows": 750,
  "returned_rows": 50,
  "columns": ["code", "name", "city", "country", "loadtype", "ref_data_loadtime"],
  "rows": [
    ["LAX", "Los Angeles International", "Los Angeles", "USA", "F", "2025-01-09T14:15:00"],
    ["JFK", "John F Kennedy International", "New York", "USA", "F", "2025-01-09T14:15:00"]
  ]
}
```

### Rollback Operations

#### `POST /backups/{base_name}/rollback/{version_id}`
**Description**: Rollback main table to specific backup version  
**Parameters**:
- `base_name` (path): Base table name
- `version_id` (path): Version to rollback to

**Response**:
```json
{
  "status": "success",
  "message": "Rollback completed successfully",
  "main_rows": 750,
  "stage_rows": 750,
  "rollback_timestamp": "2025-01-11T16:30:00"
}
```

**Error Response**:
```json
{
  "status": "error",
  "error": "Version 5 does not exist for table airports",
  "available_versions": [1, 2, 3]
}
```

### Data Export

#### `GET /backups/{base_name}/export-main`
**Description**: Export current main table data as CSV  
**Parameters**:
- `base_name` (path): Base table name

**Response**: CSV file download
- Content-Type: `text/csv`
- Content-Disposition: `attachment; filename=airports.20250111.csv`

#### `GET /backups/{base_name}/versions/{version_id}/export`
**Description**: Export specific backup version as CSV  
**Parameters**:
- `base_name` (path): Base table name  
- `version_id` (path): Version to export

**Response**: CSV file download
- Content-Type: `text/csv`
- Content-Disposition: `attachment; filename=airports.20250111.csv`

## Schema and Progress Monitoring

### Schema Information

#### `GET /schema/{table_name}`
**Description**: Get table schema information  
**Parameters**:
- `table_name` (path): Table name to inspect

**Response**:
```json
{
  "table": "airports",
  "exists": true,
  "columns": [
    {
      "name": "code",
      "data_type": "varchar",
      "max_length": 4000,
      "is_nullable": true
    },
    {
      "name": "name", 
      "data_type": "varchar",
      "max_length": 4000,
      "is_nullable": true
    },
    {
      "name": "loadtype",
      "data_type": "varchar", 
      "max_length": 255,
      "is_nullable": true
    },
    {
      "name": "ref_data_loadtime",
      "data_type": "datetime",
      "max_length": -1,
      "is_nullable": true
    }
  ]
}
```

#### `GET /schema/inferred/{fmt_filename}`
**Description**: Get inferred schema from format file  
**Parameters**:
- `fmt_filename` (path): Format file name (.fmt extension optional)

**Response**:
```json
{
  "fmt_file": "20250111_143000_airports.fmt",
  "inferred_schema": {
    "columns": [
      {"name": "code", "inferred_type": "varchar(10)"},
      {"name": "name", "inferred_type": "varchar(200)"},
      {"name": "city", "inferred_type": "varchar(100)"},
      {"name": "country", "inferred_type": "varchar(50)"}
    ],
    "confidence": 0.85
  }
}
```

### Progress Tracking

#### `GET /progress/{key}`
**Description**: Get ingestion progress for a specific operation  
**Parameters**:
- `key` (path): Progress key (sanitized filename)

**Response**:
```json
{
  "key": "airports_csv",
  "status": "processing",
  "current_step": "loading_data",
  "progress_percent": 65,
  "rows_processed": 650,
  "total_rows": 1000,
  "start_time": "2025-01-11T16:00:00",
  "estimated_completion": "2025-01-11T16:05:30",
  "last_update": "2025-01-11T16:03:15"
}
```

#### `POST /progress/{key}/cancel`
**Description**: Request cancellation of in-flight ingestion  
**Parameters**:
- `key` (path): Progress key to cancel

**Response**:
```json
{
  "message": "cancel requested",
  "key": "airports_csv", 
  "timestamp": "16:03:45.123"
}
```

## Logging and Monitoring

### System Logs

#### `GET /logs`
**Description**: Retrieve recent system logs  
**Parameters**:
- `limit` (query, default: 100): Maximum number of log entries

**Response**:
```json
{
  "logs": [
    {
      "timestamp": "2025-01-11T16:03:45",
      "level": "INFO",
      "component": "upload_file",
      "message": "File uploaded successfully: airports.csv",
      "details": null
    },
    {
      "timestamp": "2025-01-11T16:03:50", 
      "level": "ERROR",
      "component": "database_connection",
      "message": "Connection timeout after 30 seconds",
      "details": "Full stack trace..."
    }
  ]
}
```

**Headers**: 
- Cache-Control: no-store, no-cache, must-revalidate, max-age=0
- Pragma: no-cache
- Expires: 0

### Database Pool Statistics

#### `GET /db/pool-stats`
**Description**: Get database connection pool statistics  
**Response**:
```json
{
  "pool_size": 5,
  "connections_in_use": 2,
  "available_connections": 3,
  "total_created": 15,
  "total_closed": 10,
  "connection_errors": 1
}
```

### Reference Data Configuration

#### `GET /reference-data-config`
**Description**: Retrieve Reference_Data_Cfg table contents  
**Response**:
```json
{
  "data": [
    {
      "sp_name": "sp_ref_validate_airports",
      "ref_name": "airports",
      "source_db": "RefDataDB",
      "source_schema": "ref",
      "source_table": "airports",
      "is_enabled": true
    }
  ],
  "count": 1
}
```

## Error Handling

### Standard Error Response
```json
{
  "detail": "Error description",
  "error_code": "VALIDATION_FAILED",
  "timestamp": "2025-01-11T16:00:00Z"
}
```

### HTTP Status Codes
- **200**: Success
- **400**: Bad Request (invalid parameters, validation errors)
- **404**: Not Found (file, table, or version doesn't exist)
- **413**: Payload Too Large (file size exceeds limit)
- **500**: Internal Server Error (database connection, processing errors)

### Common Error Scenarios

#### File Upload Errors
```json
{
  "detail": "File size (25.5 MB) exceeds maximum limit of 20.0 MB"
}
```

#### Load Type Conflicts  
```json
{
  "detail": "Load type mismatch detected. Use verify-load-type endpoint first."
}
```

#### Database Errors
```json
{
  "detail": "Database connection failed: Login timeout expired"
}
```

#### Validation Errors
```json
{
  "detail": "Invalid table name: contains illegal characters"
}
```

## Rate Limiting

Currently, no rate limiting is implemented. For production deployment, consider implementing appropriate rate limiting based on your requirements.

## WebSocket/SSE Support

The system supports Server-Sent Events (SSE) for real-time progress streaming via the `/ingest/{filename}` POST endpoint. Clients should use EventSource or equivalent SSE client libraries.

## API Usage Examples

### Complete Upload Workflow
```bash
# 1. Detect format
curl -F "file=@data.csv" http://localhost:8000/detect-format

# 2. Verify load type
curl -F "filename=data.csv" -F "load_mode=full" http://localhost:8000/verify-load-type

# 3. Upload with override (if needed)
curl -F "file=@data.csv" \
     -F "load_mode=full" \
     -F "override_load_type=Full" \
     -F "column_delimiter=," \
     http://localhost:8000/upload

# 4. Monitor progress
curl http://localhost:8000/progress/data_csv
```

### Backup and Rollback Workflow
```bash
# 1. List backups
curl http://localhost:8000/backups

# 2. View specific version
curl http://localhost:8000/backups/airports/versions/2

# 3. Export current data
curl http://localhost:8000/backups/airports/export-main > airports_current.csv

# 4. Rollback to previous version
curl -X POST http://localhost:8000/backups/airports/rollback/2
```

## Security Considerations

- All database queries use parameterized statements
- File uploads are validated for type and size
- Table and column names are sanitized with regex validation
- Error messages are sanitized to prevent information leakage
- Consider implementing authentication for production use