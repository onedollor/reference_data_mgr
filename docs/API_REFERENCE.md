# API Reference Documentation

## Overview

The Reference Data Auto Ingest System provides a comprehensive RESTful API built with FastAPI, offering endpoints for file upload, format detection, data ingestion, progress monitoring, and system administration.

**Base URL**: `http://localhost:8000`  
**API Version**: 1.0.0  
**Content-Type**: `application/json` (unless specified otherwise)

## Authentication

Currently, the API does not require authentication. All endpoints are publicly accessible when the service is running.

## Core System Endpoints

### GET /

**Description**: System health check and version information

**Response**:
```json
{
  "message": "Reference Data Auto Ingest System is running",
  "version": "1.0.0",
  "status": "healthy"
}
```

**Example**:
```bash
curl -X GET http://localhost:8000/
```

### GET /config

**Description**: Retrieve system configuration and CSV format options

**Response**:
```json
{
  "max_upload_size": 20971520,
  "supported_formats": ["csv"],
  "default_delimiters": {
    "header_delimiter": "|",
    "column_delimiter": "|",
    "row_delimiter": "|""\r\n",
    "text_qualifier": "\""
  },
  "delimiter_options": {
    "header_delimiter": [",", ";", "|"],
    "column_delimiter": [",", ";", "|"],
    "row_delimiter": ["\r", "\n", "\r\n", "|""\r\n"],
    "text_qualifier": ["\"", "'", "\"\""]
  }
}
```

**Example**:
```bash
curl -X GET http://localhost:8000/config
```

### GET /features

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
  "stream_commit_rows": "50000",
  "bulk_insert": "0",
  "bulk_min_rows": "50000"
}
```

## File Processing Endpoints

### POST /detect-format

**Description**: Auto-detect CSV format parameters from an uploaded file

**Request**: `multipart/form-data`
- `file`: CSV file (required)

**Response**:
```json
{
  "filename": "example.csv",
  "file_size": 1024,
  "detected_format": {
    "header_delimiter": ",",
    "column_delimiter": ",",
    "row_delimiter": "\n",
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
    "sample_rows": 3,
    "trailer_format": null,
    "sample_data": [
      ["Column1", "Column2", "Column3"],
      ["Value1", "Value2", "Value3"],
      ["Value4", "Value5", "Value6"]
    ]
  },
  "error": null
}
```

**Example**:
```bash
curl -X POST http://localhost:8000/detect-format \
  -F "file=@example.csv"
```

### POST /upload

**Description**: Upload CSV file with format configuration and start processing

**Request**: `multipart/form-data`
- `file`: CSV file (required)
- `header_delimiter`: Header row delimiter (default: "|")
- `column_delimiter`: Column separator (default: "|")
- `row_delimiter`: Row terminator (default: '|""\r\n')
- `text_qualifier`: Text qualifier character (default: '"')
- `skip_lines`: Lines to skip after header (default: 0)
- `trailer_line`: Optional trailer pattern
- `load_mode`: "full" or "append" (default: "full")

**Response**:
```json
{
  "message": "File uploaded successfully",
  "filename": "example.csv",
  "file_size": 1024,
  "status": "processing",
  "progress_key": "example_csv"
}
```

**Example**:
```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@example.csv" \
  -F "header_delimiter=," \
  -F "column_delimiter=," \
  -F "row_delimiter=\n" \
  -F "text_qualifier=\"" \
  -F "load_mode=full"
```

### POST /ingest/{filename}

**Description**: Stream real-time ingestion progress for a specific file

**Parameters**:
- `filename`: Name of the file to ingest
- `load_mode`: "full" or "append" (query parameter)

**Response**: Server-Sent Events (SSE) stream with progress updates

**Example**:
```bash
curl -N -X POST "http://localhost:8000/ingest/example.csv?load_mode=full"
```

**Stream Response Format**:
```
data: Starting data ingestion process...

data: Connecting to database...

data: Database connection established (took 0.12s)

data: Progress: {"inserted": 500, "total": 1000, "percent": 50.0}

data: Ingestion completed successfully
```

### GET /ingest/{filename}

**Description**: Get ingestion prerequisites and instructions for a file

**Response**:
```json
{
  "message": "Use POST /ingest/{filename} to start streaming ingestion output.",
  "filename": "example.csv",
  "file_present": true,
  "format_present": true,
  "next_steps": [
    "Upload the file first via POST /upload (multipart form field 'file')",
    "Then call: curl -N -X POST http://host:8000/ingest/yourfile.csv",
    "Optionally include load_mode query param: ?load_mode=append"
  ],
  "example_curl_upload": "curl -F file=@test.csv -F load_mode=full http://localhost:8000/upload",
  "example_curl_ingest_stream": "curl -N -X POST http://localhost:8000/ingest/example.csv",
  "dry_run": "0"
}
```

## Progress Tracking Endpoints

### GET /progress/{key}

**Description**: Get current progress status for an operation

**Parameters**:
- `key`: Progress tracking key (sanitized filename)

**Response**:
```json
{
  "found": true,
  "inserted": 750,
  "total": 1000,
  "percent": 75.0,
  "stage": "inserting_data",
  "done": false,
  "error": null,
  "canceled": false
}
```

**Example**:
```bash
curl -X GET http://localhost:8000/progress/example_csv
```

### POST /progress/{key}/cancel

**Description**: Request cancellation of an ongoing ingestion process

**Parameters**:
- `key`: Progress tracking key

**Response**:
```json
{
  "message": "cancel requested",
  "key": "example_csv"
}
```

**Example**:
```bash
curl -X POST http://localhost:8000/progress/example_csv/cancel
```

## Database & Schema Endpoints

### GET /schema/{table_name}

**Description**: Get table schema information

**Parameters**:
- `table_name`: Name of the table

**Response**:
```json
{
  "table": "example",
  "exists": true,
  "columns": [
    {
      "column_name": "column1",
      "data_type": "varchar",
      "max_length": 4000,
      "is_nullable": true
    },
    {
      "column_name": "ref_data_loadtime",
      "data_type": "datetime",
      "is_nullable": true
    }
  ]
}
```

**Example**:
```bash
curl -X GET http://localhost:8000/schema/example
```

### GET /schema/inferred/{fmt_filename}

**Description**: Get inferred schema from format file

**Parameters**:
- `fmt_filename`: Format filename (with or without .fmt extension)

**Response**:
```json
{
  "fmt_file": "20250810_120000_example.fmt",
  "inferred_schema": {
    "columns": [
      {
        "name": "column1",
        "type": "varchar",
        "length": 4000
      },
      {
        "name": "column2",
        "type": "varchar",
        "length": 4000
      }
    ]
  }
}
```

### GET /db/pool-stats

**Description**: Database connection pool statistics

**Response**:
```json
{
  "pool_size": 5,
  "active_connections": 2,
  "available_connections": 3,
  "total_created": 15,
  "total_closed": 10
}
```

## Monitoring Endpoints

### GET /logs

**Description**: Retrieve system logs with pagination

**Parameters** (Query):
- `limit`: Maximum number of log entries to return (default: 100)

**Response**:
```json
{
  "logs": [
    {
      "timestamp": "2025-08-10T10:30:00.000Z",
      "level": "INFO",
      "module": "upload_file",
      "message": "File uploaded successfully: example.csv",
      "details": null
    },
    {
      "timestamp": "2025-08-10T10:29:30.000Z",
      "level": "ERROR",
      "module": "database_connection",
      "message": "Connection timeout occurred",
      "details": "Traceback information..."
    }
  ]
}
```

**Headers**:
- `Cache-Control`: `no-store, no-cache, must-revalidate, max-age=0`
- `Pragma`: `no-cache`
- `Expires`: `0`

**Example**:
```bash
curl -X GET "http://localhost:8000/logs?limit=50"
```

## Error Responses

All endpoints return standardized error responses in the following format:

### HTTP 400 Bad Request
```json
{
  "detail": "Only CSV files are supported"
}
```

### HTTP 404 Not Found
```json
{
  "detail": "File not found in temp location"
}
```

### HTTP 413 Payload Too Large
```json
{
  "detail": "File size exceeds maximum limit of 20971520 bytes"
}
```

### HTTP 500 Internal Server Error
```json
{
  "detail": "Upload failed: Database connection error"
}
```

## Rate Limiting

Currently, no rate limiting is implemented. The system handles concurrent requests through connection pooling and background task processing.

## File Size Limits

- Maximum file size: 20MB (configurable via `max_upload_size` environment variable)
- Supported file types: CSV only
- File extensions: `.csv` (case-insensitive)

## Content-Type Requirements

- File uploads: `multipart/form-data`
- JSON responses: `application/json`
- SSE streams: `text/plain`

## CORS Configuration

The API is configured to accept requests from:
- `http://localhost:3000` (React development server)

Allowed methods:
- GET, POST, PUT, DELETE, OPTIONS

Allowed headers:
- Content-Type, Authorization, Accept, X-Requested-With

## WebSocket & Real-time Features

The system uses Server-Sent Events (SSE) for real-time progress streaming rather than WebSockets. This provides a simpler, more reliable connection for one-way progress updates.

## Environment Variables

Key environment variables that affect API behavior:

```bash
# File processing
max_upload_size=20971520
temp_location=C:\data\reference_data\temp
archive_location=C:\data\reference_data\archive
format_location=C:\data\reference_data\format

# Database
db_host=localhost
db_name=test
db_user=username
db_password=password

# Performance
INGEST_PROGRESS_INTERVAL=5
DB_POOL_SIZE=5
```

## OpenAPI Specification

The complete OpenAPI specification is available at:
- Interactive docs: `http://localhost:8000/docs`
- JSON spec: `http://localhost:8000/openapi.json`
- ReDoc: `http://localhost:8000/redoc`

## SDK Examples

### Python Example
```python
import requests
import json

# Upload file
with open('example.csv', 'rb') as f:
    response = requests.post(
        'http://localhost:8000/upload',
        files={'file': f},
        data={
            'header_delimiter': ',',
            'column_delimiter': ',',
            'load_mode': 'full'
        }
    )

print(response.json())

# Check progress
progress_key = response.json()['progress_key']
progress = requests.get(f'http://localhost:8000/progress/{progress_key}')
print(progress.json())
```

### JavaScript Example
```javascript
// Upload file
const formData = new FormData();
formData.append('file', fileInput.files[0]);
formData.append('load_mode', 'full');

const response = await fetch('http://localhost:8000/upload', {
  method: 'POST',
  body: formData
});

const result = await response.json();
console.log(result);

// Stream progress
const eventSource = new EventSource(`http://localhost:8000/ingest/${filename}`);
eventSource.onmessage = function(event) {
  console.log('Progress:', event.data);
};
```

## Support

For API issues or questions:
1. Check the interactive documentation at `/docs`
2. Review the system logs via `/logs` endpoint
3. Verify system health via `/` endpoint
4. Check configuration via `/config` endpoint