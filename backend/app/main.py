"""
Reference Data Auto Ingest System - FastAPI Backend
Main application entry point
"""

import os
import re
import sys

# Ensure backend root (containing 'utils') is on sys.path when running from repo root
_CURRENT_DIR = os.path.dirname(__file__)
_BACKEND_ROOT = os.path.abspath(os.path.join(_CURRENT_DIR, '..'))
if _BACKEND_ROOT not in sys.path:
    sys.path.insert(0, _BACKEND_ROOT)
import traceback
from typing import Optional, Dict, Any, List
from datetime import datetime
import json

from fastapi import FastAPI, File, UploadFile, HTTPException, Form, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
import traceback
from dotenv import load_dotenv

from utils.database import DatabaseManager
from utils.file_handler import FileHandler
from utils.logger import Logger, DatabaseLogger
from utils.csv_detector import CSVFormatDetector
from utils import progress as prog

# Load environment variables
load_dotenv()

app = FastAPI(
    title="Reference Data Auto Ingest System",
    description="Automated reference data ingestion from CSV files to SQL Server",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize database and ensure required tables and procedures exist on startup"""
    try:
        # Initialize database connection and check required objects
        connection = db_manager.get_connection()
        db_manager.ensure_reference_data_cfg_table(connection)
        db_manager.ensure_postload_stored_procedure(connection)
        connection.close()
        await logger.log_info("startup", "Application started successfully - Reference_Data_Cfg table and post-load procedure verified", source_ip="system")
    except Exception as e:
        error_msg = f"Startup failed: {str(e)}"
        print(f"ERROR: {error_msg}")
        await logger.log_error("startup", error_msg, traceback.format_exc(), source_ip="system")
        # Continue startup - don't fail the entire application
        pass

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],  # Specific methods only
    allow_headers=["Content-Type", "Authorization", "Accept", "X-Requested-With"],  # Specific headers only
)

# Initialize components
db_manager = DatabaseManager()
file_handler = FileHandler()
logger = DatabaseLogger(db_manager)

def get_client_ip(request: Request) -> str:
    """Extract client IP address from request"""
    # Check for X-Forwarded-For header (proxy/load balancer)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # Take the first IP in the chain
        return forwarded_for.split(",")[0].strip()
    
    # Check for X-Real-IP header (nginx)
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    
    # Fallback to direct client IP
    if hasattr(request, "client") and request.client:
        return request.client.host
    
    return "unknown"

# Ensure schemas on startup
@app.on_event("startup")
async def startup_init():
    try:
        conn = db_manager.get_connection()
        db_manager.ensure_schemas_exist(conn)
        conn.close()
    except Exception as e:
        # Log but don't crash app
        try:
            await logger.log_error("startup", f"Schema init failed: {e}", source_ip="system")
        except Exception:
            print(f"Startup schema init failed: {e}")

@app.get("/")
async def root():
    """Root endpoint for health check"""
    return {
        "message": "Reference Data Auto Ingest System is running",
        "version": "1.0.0",
        "status": "healthy"
    }

@app.get("/health/database")
async def database_health(request: Request):
    """Check database connection health"""
    try:
        # Test database connection
        connection = db_manager.get_connection()
        
        # Run a simple query to verify connectivity
        cursor = connection.cursor()
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        if result and result[0] == 1:
            return {"status": "healthy"}
        else:
            return {"status": "error", "message": "Database query failed"}
            
    except Exception as e:
        error_msg = f"Database connection failed: {str(e)}"
        try:
            await logger.log_error("database_health", error_msg, source_ip=get_client_ip(request))
        except Exception:
            pass  # Don't fail if logging fails
        return {"status": "error", "message": error_msg}

# ---------------- Rollback / Backup APIs ----------------
@app.get("/backups")
async def list_backups():
    """List backup tables with validation of related main/stage tables."""
    try:
        conn = db_manager.get_connection()
        data = db_manager.list_backup_tables(conn)
        conn.close()
        return {"backups": data}
    except Exception as e:
        await logger.log_error("list_backups", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/backups/{base_name}/versions")
async def get_backup_versions(base_name: str):
    try:
        conn = db_manager.get_connection()
        versions = db_manager.get_backup_versions(conn, base_name)
        conn.close()
        return {"base_name": base_name, "versions": versions}
    except Exception as e:
        await logger.log_error("get_backup_versions", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/backups/{base_name}/versions/{ref_data_version_id}")
async def view_backup_version(base_name: str, ref_data_version_id: int, limit: int = 50, offset: int = 0):
    try:
        conn = db_manager.get_connection()
        data = db_manager.get_backup_version_rows(conn, base_name, ref_data_version_id, limit, offset)
        conn.close()
        if data.get('error'):
            raise HTTPException(status_code=400, detail=data['error'])
        return data
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("view_backup_version", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/backups/{base_name}/rollback/{ref_data_version_id}")
async def rollback_backup_version(base_name: str, ref_data_version_id: int):
    try:
        conn = db_manager.get_connection()
        outcome = db_manager.rollback_to_version(conn, base_name, ref_data_version_id)
        conn.close()
        if outcome.get('status') == 'error':
            raise HTTPException(status_code=400, detail=outcome.get('error', 'Rollback failed'))
        await logger.log_info("rollback", f"Rollback executed for {base_name} to version {ref_data_version_id}: status={outcome.get('status')}")
        return outcome
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("rollback", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/backups/{base_name}/export-main")
async def export_main_table_csv(base_name: str):
    """Export current main table data for the given base_name as CSV."""
    try:
        # Basic validation
        if not re.fullmatch(r"[A-Za-z0-9_]+", base_name):
            raise HTTPException(status_code=400, detail="Invalid base name")
        conn = db_manager.get_connection()
        if not db_manager.table_exists(conn, base_name, db_manager.data_schema):
            conn.close()
            raise HTTPException(status_code=404, detail="Main table not found")
        # Fetch columns
        cols_meta = db_manager.get_table_columns(conn, base_name, db_manager.data_schema)
        # Exclude internal/meta columns from export
        exclude_cols = {"ref_data_loadtime", "ref_data_loadtype"}
        col_names = [c['name'] for c in cols_meta if c['name'].lower() not in exclude_cols]
        if not col_names:
            conn.close()
            raise HTTPException(status_code=400, detail="No columns found")
        cursor = conn.cursor()
        # Build select
        select_sql = "SELECT " + ", ".join(f"[{c}]" for c in col_names) + f" FROM [{db_manager.data_schema}].[{base_name}]"
        cursor.execute(select_sql)

        def row_to_csv(row):
            out_fields = []
            for v in row:
                if v is None:
                    out_fields.append('')
                else:
                    s = str(v)
                    # Escape quotes by doubling
                    if '"' in s:
                        s = s.replace('"', '""')
                    # Quote if contains delimiter, quote, or newline
                    if any(ch in s for ch in [',', '"', '\n', '\r']):
                        s = f'"{s}"'
                    out_fields.append(s)
            return ','.join(out_fields)

        async def stream_csv():
            # Header
            header = ','.join(col_names) + '\n'
            yield header
            batch_size = 500
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                lines = [row_to_csv(r) for r in rows]
                yield '\n'.join(lines) + '\n'
            try:
                conn.close()
            except Exception:
                pass

        # Filename pattern: table.yyyyMMdd.csv (UTC date)
        filename = f"{base_name}.{datetime.utcnow().strftime('%Y%m%d')}.csv"
        return StreamingResponse(stream_csv(), media_type="text/csv", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("export_main_table", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/backups/{base_name}/versions/{ref_data_version_id}/export")
async def export_backup_version_csv(base_name: str, ref_data_version_id: int):
    """Export a specific backup table version as CSV (excluding version/metadata columns)."""
    try:
        if not re.fullmatch(r"[A-Za-z0-9_]+", base_name):
            raise HTTPException(status_code=400, detail="Invalid base name")
        conn = db_manager.get_connection()
        backup_table = f"{base_name}_backup"
        if not db_manager.table_exists(conn, backup_table, db_manager.backup_schema):
            conn.close()
            raise HTTPException(status_code=404, detail="Backup table not found")
        # Validate version exists
        cursor = conn.cursor()
        cursor.execute(
            "SELECT TOP 1 1 FROM [" + db_manager.backup_schema + "].[" + backup_table + "] WHERE ref_data_version_id = ?",
            ref_data_version_id
        )
        if cursor.fetchone() is None:
            conn.close()
            raise HTTPException(status_code=404, detail="Version not found")
        # Columns excluding metadata
        cols_meta = db_manager.get_table_columns(conn, backup_table, db_manager.backup_schema)
        exclude_cols = {"ref_data_loadtime", "ref_data_loadtype", "ref_data_version_id"}
        col_names = [c['name'] for c in cols_meta if c['name'].lower() not in exclude_cols]
        if not col_names:
            conn.close()
            raise HTTPException(status_code=400, detail="No exportable columns")
        select_sql = (
            "SELECT " + ", ".join(f"[{c}]" for c in col_names) +
            " FROM [" + db_manager.backup_schema + "].[" + backup_table + "] WHERE ref_data_version_id = ?"
        )
        cursor.execute(select_sql, ref_data_version_id)

        def row_to_csv(row):
            out_fields = []
            for v in row:
                if v is None:
                    out_fields.append('')
                else:
                    s = str(v)
                    if '"' in s:
                        s = s.replace('"', '""')
                    if any(ch in s for ch in [',', '"', '\n', '\r']):
                        s = f'"{s}"'
                    out_fields.append(s)
            return ','.join(out_fields)

        async def stream_csv():
            header = ','.join(col_names) + '\n'
            yield header
            batch_size = 500
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield '\n'.join(row_to_csv(r) for r in rows) + '\n'
            try:
                conn.close()
            except Exception:
                pass

        # Filename excludes version id per request: table.yyyyMMdd.csv
        filename = f"{base_name}.{datetime.utcnow().strftime('%Y%m%d')}.csv"
        return StreamingResponse(stream_csv(), media_type="text/csv", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("export_backup_version", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/config")
async def get_config():
    """Get system configuration"""
    try:
        config = {
            "max_upload_size": int(os.getenv("max_upload_size", "20971520")),
            "supported_formats": ["csv"],
            "default_delimiters": {
                "header_delimiter": "|",
                "column_delimiter": "|", 
                "row_delimiter": '|""\r\n',
                "text_qualifier": '"'
            },
            "delimiter_options": {
                "header_delimiter": [",", ";", "|"],
                "column_delimiter": [",", ";", "|"],
                "row_delimiter": ["\r", "\n", "\r\n", '|""\r\n'],
                "text_qualifier": ['"', "'", '""']
            }
        }
        return config
    except Exception as e:
        await logger.log_error("get_config", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Configuration error: {str(e)}")

@app.post("/detect-format")
async def detect_csv_format(file: UploadFile = File(...)):
    """
    Auto-detect CSV format parameters from uploaded file
    Returns suggested format settings
    """
    try:
        # Initialize detector
        detector = CSVFormatDetector()
        
        # Save temporary file for analysis
        temp_location = os.getenv("temp_location", "temp")
        temp_path = os.path.join(temp_location, f"detect_{file.filename}")
        
        # Write uploaded file to temp location
        with open(temp_path, "wb") as temp_file:
            content = await file.read()
            temp_file.write(content)
        
        # Reset file position for potential future reads
        await file.seek(0)
        
        # Detect format
        detection_result = detector.detect_format(temp_path)
        
        # Clean up temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        # Log detection attempt
        await logger.log_info(
            "csv_format_detection",
            f"Format detection completed for {file.filename}. Confidence: {detection_result.get('detection_confidence', 0):.2f}"
        )
        
        # Prepare response
        response = {
            "filename": file.filename,
            "file_size": len(content),
            "detected_format": {
                "header_delimiter": detection_result.get("header_delimiter", ","),
                "column_delimiter": detection_result.get("column_delimiter", ","),
                "row_delimiter": detection_result.get("row_delimiter", "\n"),
                "text_qualifier": detection_result.get("text_qualifier", '"'),
                "has_header": detection_result.get("has_header", True),
                "has_trailer": detection_result.get("has_trailer", False),
                "skip_lines": detection_result.get("skip_lines", 0),
                "trailer_line": detection_result.get("trailer_line")
            },
            "confidence": detection_result.get("detection_confidence", 0.0),
            "analysis": {
                "encoding": detection_result.get("encoding", "utf-8"),
                "estimated_columns": detection_result.get("estimated_columns", 0),
                "sample_rows": detection_result.get("sample_rows", 0),
                "trailer_format": detection_result.get("trailer_format"),
                "sample_data": detection_result.get("sample_data", [])[:3],  # First 3 rows
                "columns": detection_result.get("sample_data", [[]])[0] if detection_result.get("has_header", True) and detection_result.get("sample_data") else []
            },
            "error": detection_result.get("error")
        }
        
        return response
        
    except Exception as e:
        error_msg = f"Format detection failed: {str(e)}"
        await logger.log_error("detect_csv_format", error_msg, traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)

@app.post("/upload")
async def upload_file(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    header_delimiter: str = Form("|"),
    column_delimiter: str = Form("|"),
    row_delimiter: str = Form('|""\r\n'),
    text_qualifier: str = Form('"'),
    skip_lines: int = Form(0),
    trailer_line: Optional[str] = Form(None),
    load_mode: str = Form("full"),  # full or append
    override_load_type: Optional[str] = Form(None),  # Optional override for load type
    config_reference_data: bool = Form(False)  # Configure for reference data
):
    """Upload and process CSV file"""
    try:
        # Validate file type
        if not file.filename.lower().endswith('.csv'):
            raise HTTPException(status_code=400, detail="Only CSV files are supported")
        
        # Validate file size
        max_size = int(os.getenv("max_upload_size", "20971520"))  # 20MB default
        file_content = await file.read()
        if len(file_content) > max_size:
            raise HTTPException(
                status_code=413, 
                detail=f"File size exceeds maximum limit of {max_size} bytes"
            )
        
        # Reset file pointer
        await file.seek(0)
        
        # Save file and create format file
        temp_file_path, fmt_file_path = await file_handler.save_uploaded_file(
            file, header_delimiter, column_delimiter, row_delimiter, 
            text_qualifier, skip_lines, trailer_line
        )
        
        # Start background ingestion process
        background_tasks.add_task(
            ingest_data_background,
            temp_file_path,
            fmt_file_path,
            load_mode,
            file.filename,
            override_load_type,
            config_reference_data
        )
        
        return {
            "message": "File uploaded successfully",
            "filename": file.filename,
            "file_size": len(file_content),
            "status": "processing",
            "progress_key": re.sub(r'[^a-zA-Z0-9_]', '_', file.filename)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("upload_file", str(e), traceback.format_exc(), source_ip=get_client_ip(request))
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@app.post("/ingest/{filename}")
async def trigger_ingestion(
    filename: str,
    load_mode: str = "full"
):
    """Trigger data ingestion for a specific file"""
    try:
        temp_file_path = os.path.join(os.getenv("temp_location"), filename)
        fmt_file_path = os.path.join(os.getenv("format_location"), filename.replace('.csv', '.fmt'))
        
        if not os.path.exists(temp_file_path):
            raise HTTPException(status_code=404, detail="File not found in temp location")
        
        if not os.path.exists(fmt_file_path):
            raise HTTPException(status_code=404, detail="Format file not found")
        
        # Process ingestion with progress streaming
        return StreamingResponse(
            ingest_data_stream(temp_file_path, fmt_file_path, load_mode, filename),
            media_type="text/plain"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("trigger_ingestion", str(e), traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

@app.get("/ingest/{filename}")
async def ingest_usage(filename: str):
    """Helper GET endpoint to avoid Method Not Allowed confusion.
    Returns instructions for triggering ingestion via POST or checking prerequisites.
    """
    temp_file_path = os.path.join(os.getenv("temp_location"), filename)
    fmt_file_path = os.path.join(os.getenv("format_location"), filename.replace('.csv', '.fmt'))
    return {
        "message": "Use POST /ingest/{filename} to start streaming ingestion output.",
        "filename": filename,
        "file_present": os.path.exists(temp_file_path),
        "format_present": os.path.exists(fmt_file_path),
        "next_steps": [
            "Upload the file first via POST /upload (multipart form field 'file')",
            "Then call: curl -N -X POST http://host:8000/ingest/yourfile.csv",
            "Optionally include load_mode query param: ?load_mode=append"
        ],
        "example_curl_upload": "curl -F file=@test.csv -F load_mode=full http://localhost:8000/upload",
        "example_curl_ingest_stream": f"curl -N -X POST http://localhost:8000/ingest/{filename}",
        "dry_run": os.getenv("INGEST_DRY_RUN", "0"),
    }

@app.get("/schema/{table_name}")
async def get_table_schema(table_name: str):
    try:
        conn = db_manager.get_connection()
        if not db_manager.table_exists(conn, table_name):
            return {"table": table_name, "exists": False}
        cols = db_manager.get_table_columns(conn, table_name)
        return {"table": table_name, "exists": True, "columns": cols}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            conn.close()
        except:
            pass

@app.get("/schema/inferred/{fmt_filename}")
async def get_inferred_schema(fmt_filename: str):
    # Expect a .fmt file name (or base without ext)
    try:
        if not fmt_filename.endswith('.fmt'):
            # Allow base name; resolve latest matching
            base = fmt_filename
        else:
            base = fmt_filename[:-4]
        fmt_dir = os.getenv("format_location", "C:\\data\\reference_data\\format")
        # Find latest matching format file for base
        candidates = [f for f in os.listdir(fmt_dir) if f.endswith('.fmt') and base in f]
        if not candidates:
            return {"inferred_schema": None, "message": "No matching format file"}
        # Pick newest by modified time
        candidates_full = [os.path.join(fmt_dir, f) for f in candidates]
        newest = max(candidates_full, key=lambda p: os.path.getmtime(p))
        import json
        with open(newest, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {"fmt_file": os.path.basename(newest), "inferred_schema": data.get('inferred_schema')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/db/pool-stats")
async def pool_stats():
    try:
        return db_manager.get_pool_stats()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/features")
async def feature_flags():
    return {
        "type_inference": os.getenv("INGEST_TYPE_INFERENCE", "0"),
        "chunk_size": os.getenv("INGEST_CHUNK_SIZE", "0"),
        "batch_size": os.getenv("INGEST_BATCH_SIZE", "0"),
        "append_mode_schema_sync": True,
        "pooling": True,
        "dry_run": os.getenv("INGEST_DRY_RUN", "0"),
        "stream_commit_rows": os.getenv("INGEST_STREAM_COMMIT_ROWS", "50000")
    }

async def ingest_data_background(
    file_path: str, 
    fmt_file_path: str, 
    load_mode: str, 
    filename: str,
    override_load_type: str = None,
    config_reference_data: bool = False
):
    """Background task for data ingestion"""
    try:
        from utils.ingest import DataIngester
        ingester = DataIngester(db_manager, logger)
        
        async for progress in ingester.ingest_data(file_path, fmt_file_path, load_mode, filename, override_load_type, config_reference_data):
            await logger.log_info("background_ingestion", f"Progress: {progress}")
            
    except Exception as e:
        # Check if this is a cancellation exception
        if "canceled by user" in str(e).lower():
            # Mark progress as canceled
            progress_key = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
            prog.mark_canceled(progress_key, str(e))
            await logger.log_info("ingest_data_background", f"Ingestion canceled: {str(e)}")
        else:
            await logger.log_error("ingest_data_background", str(e), traceback.format_exc())

async def ingest_data_stream(
    file_path: str, 
    fmt_file_path: str, 
    load_mode: str, 
    filename: str
):
    """Stream ingestion progress to client"""
    try:
        from utils.ingest import DataIngester
        ingester = DataIngester(db_manager, logger)
        
        async for progress in ingester.ingest_data(file_path, fmt_file_path, load_mode, filename):
            yield f"data: {progress}\n\n"
            
    except Exception as e:
        error_msg = f"ERROR! Ingestion failed: {str(e)}\n{traceback.format_exc()}"
        await logger.log_error("ingest_data_stream", str(e), traceback.format_exc())
        yield f"data: {error_msg}\n\n"

@app.get("/logs")
async def get_logs(request: Request, limit: int = 100):
    """Get recent log entries (no-cache)."""
    try:
        logs = await logger.get_logs(limit)
        return JSONResponse(
            content={"logs": logs},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
    except Exception as e:
        await logger.log_error("get_logs", str(e), traceback.format_exc(), source_ip=get_client_ip(request))
        raise HTTPException(status_code=500, detail=f"Failed to retrieve logs: {str(e)}")

@app.get("/logs/{log_type}")
async def get_logs_by_type(request: Request, log_type: str, limit: int = 100):
    """Get recent log entries by type (system, error, ingest) (no-cache)."""
    try:
        if log_type not in ["system", "error", "ingest"]:
            raise HTTPException(status_code=400, detail="Invalid log type. Must be one of: system, error, ingest")
        
        logs = await logger.get_logs_by_type(log_type, limit)
        return JSONResponse(
            content={"logs": logs, "log_type": log_type},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache", 
                "Expires": "0"
            }
        )
    except HTTPException:
        raise
    except Exception as e:
        await logger.log_error("get_logs_by_type", str(e), traceback.format_exc(), source_ip=get_client_ip(request))
        raise HTTPException(status_code=500, detail=f"Failed to retrieve {log_type} logs: {str(e)}")

@app.post("/logs/rotate")
async def rotate_logs(request: Request, max_size_mb: int = 10):
    """Rotate log files if they exceed max_size_mb."""
    try:
        logger.rotate_logs(max_size_mb)
        await logger.log_info("rotate_logs", f"Log rotation completed with max size: {max_size_mb}MB", source_ip=get_client_ip(request))
        return {"message": "Log rotation completed", "max_size_mb": max_size_mb}
    except Exception as e:
        await logger.log_error("rotate_logs", str(e), traceback.format_exc(), source_ip=get_client_ip(request))
        raise HTTPException(status_code=500, detail=f"Failed to rotate logs: {str(e)}")

@app.get("/progress/{key}")
async def get_progress(key: str):
    """Return ingestion progress for given key (sanitized filename)."""
    return prog.get_progress(key)

@app.post("/progress/{key}/cancel")
async def cancel_ingestion(key: str):
    """Request cancellation of an in-flight ingestion."""
    import time
    timestamp = time.strftime("%H:%M:%S.%f")[:-3]  # HH:MM:SS.mmm
    await logger.log_info("cancel_request", f"[{timestamp}] Cancellation requested for progress key: {key}")
    prog.request_cancel(key)
    current_progress = prog.get_progress(key)
    await logger.log_info("cancel_status", f"[{timestamp}] Progress after cancel request: {current_progress}")
    return {"message": "cancel requested", "key": key, "timestamp": timestamp}

@app.post("/verify-load-type")
async def verify_load_type(
    filename: str = Form(...),
    load_mode: str = Form(...)
):
    """Verify load type compatibility between current load mode and existing table data"""
    connection = None
    try:
        connection = db_manager.get_connection()
        
        # Extract table name from filename  
        from utils.file_handler import FileHandler
        file_handler = FileHandler()
        table_name = file_handler.extract_table_base_name(filename)
        
        # Get current load type that would be determined
        current_load_type = db_manager.determine_load_type(connection, table_name, load_mode)
        
        # Get requested load type
        requested_load_type = 'F' if load_mode == 'full' else 'A'
        
        # Check if there's a mismatch
        mismatch = current_load_type != requested_load_type
        
        # Get existing load types for context
        existing_load_types = []
        if db_manager.table_exists(connection, table_name):
            cursor = connection.cursor()
            try:
                query = f"SELECT DISTINCT [ref_data_loadtype] FROM [{db_manager.data_schema}].[{table_name}] WHERE [ref_data_loadtype] IS NOT NULL"
                cursor.execute(query)
                for row in cursor.fetchall():
                    if row[0]:
                        existing_load_types.append(row[0].strip())
            except Exception:
                pass  # Table might not have ref_data_loadtype column yet
        
        result = {
            "table_name": table_name,
            "requested_load_mode": load_mode,
            "requested_load_type": requested_load_type,
            "determined_load_type": current_load_type,
            "has_mismatch": mismatch,
            "existing_load_types": existing_load_types,
            "table_exists": db_manager.table_exists(connection, table_name),
            "explanation": f"Based on existing data, load type will be '{current_load_type}' but you requested '{requested_load_type}'"
        }
        
        await logger.log_info("load_type_verification", f"Verified load type for {table_name}: mismatch={mismatch}")
        
        return JSONResponse(content=result)
        
    except Exception as e:
        error_msg = f"Failed to verify load type: {str(e)}"
        await logger.log_error("load_type_verification", error_msg, traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

@app.get("/reference-data-config")
async def get_reference_data_config():
    """Get all Reference_Data_Cfg records"""
    connection = None
    try:
        connection = db_manager.get_connection()
        cursor = connection.cursor()
        
        # Query all records from Reference_Data_Cfg table
        query = f"""
            SELECT [sp_name], [ref_name], [source_db], [source_schema], [source_table], [is_enabled]
            FROM [{db_manager.staff_database}].[dbo].[Reference_Data_Cfg]
            ORDER BY [ref_name]
        """
        cursor.execute(query)
        
        # Convert results to list of dictionaries
        columns = [column[0] for column in cursor.description]
        results = []
        for row in cursor.fetchall():
            row_dict = {}
            for i, value in enumerate(row):
                row_dict[columns[i]] = value
            results.append(row_dict)
        
        await logger.log_info("reference_data_config_query", f"Retrieved {len(results)} Reference_Data_Cfg records")
        
        return JSONResponse(
            content={"data": results, "count": len(results)},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
        
    except Exception as e:
        error_msg = f"Failed to retrieve Reference_Data_Cfg records: {str(e)}"
        await logger.log_error("reference_data_config_query", error_msg, traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

@app.get("/tables/all")
async def get_all_tables():
    """Get list of ALL tables in the database for validation purposes"""
    connection = None
    try:
        connection = db_manager.get_connection()
        cursor = connection.cursor()
        
        # Query to get ALL table names (including system tables)
        query = f"""
            SELECT TABLE_NAME 
            FROM [{db_manager.database}].INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        cursor.execute(query)
        
        # Extract all table names from results
        tables = [row[0] for row in cursor.fetchall()]
        
        await logger.log_info("all_tables_query", f"Retrieved {len(tables)} total tables for validation")
        
        return JSONResponse(
            content={"tables": tables, "count": len(tables)},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
        
    except Exception as e:
        error_msg = f"Failed to retrieve all tables: {str(e)}"
        await logger.log_error("all_tables_query", error_msg, traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

@app.get("/tables")
async def get_available_tables():
    """Get list of available tables in the database"""
    connection = None
    try:
        connection = db_manager.get_connection()
        cursor = connection.cursor()
        
        # Query to get main tables that have complete sets (main, stage, backup)
        query = f"""
            WITH all_tables AS (
                SELECT TABLE_NAME 
                FROM [{db_manager.database}].INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            ),
            main_tables AS (
                SELECT TABLE_NAME as main_table
                FROM all_tables 
                WHERE TABLE_NAME NOT LIKE '%_stage' 
                AND TABLE_NAME NOT LIKE '%_backup'
                AND TABLE_NAME NOT IN ('Reference_Data_Cfg', 'system_log')
            )
            SELECT m.main_table
            FROM main_tables m
            WHERE EXISTS (
                SELECT 1 FROM all_tables WHERE TABLE_NAME = m.main_table + '_stage'
            )
            AND EXISTS (
                SELECT 1 FROM all_tables WHERE TABLE_NAME = m.main_table + '_backup'  
            )
            ORDER BY m.main_table
        """
        cursor.execute(query)
        
        # Extract main table names from results
        tables = [row[0] for row in cursor.fetchall()]
        
        await logger.log_info("tables_query", f"Retrieved {len(tables)} available tables")
        
        return JSONResponse(
            content={"tables": tables, "count": len(tables)},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )
        
    except Exception as e:
        error_msg = f"Failed to retrieve available tables: {str(e)}"
        await logger.log_error("tables_query", error_msg, traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

@app.post("/tables/schema-match")
async def get_schema_matched_tables(file_columns: Dict[str, List[str]]):
    """Get tables that match the schema of the uploaded file"""
    connection = None
    try:
        connection = db_manager.get_connection()
        
        # Extract columns from the request
        columns = file_columns.get("columns", [])
        if not columns:
            return JSONResponse(
                content={"tables": [], "count": 0, "message": "No columns provided"},
                headers={
                    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                    "Pragma": "no-cache",
                    "Expires": "0"
                }
            )
        
        # Get all available tables with complete sets
        cursor = connection.cursor()
        query = f"""
            WITH all_tables AS (
                SELECT TABLE_NAME 
                FROM [{db_manager.database}].INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
            ),
            main_tables AS (
                SELECT TABLE_NAME as main_table
                FROM all_tables 
                WHERE TABLE_NAME NOT LIKE '%_stage' 
                AND TABLE_NAME NOT LIKE '%_backup'
                AND TABLE_NAME NOT IN ('Reference_Data_Cfg', 'system_log')
            )
            SELECT m.main_table
            FROM main_tables m
            WHERE EXISTS (
                SELECT 1 FROM all_tables WHERE TABLE_NAME = m.main_table + '_stage'
            )
            AND EXISTS (
                SELECT 1 FROM all_tables WHERE TABLE_NAME = m.main_table + '_backup'  
            )
            ORDER BY m.main_table
        """
        cursor.execute(query)
        all_tables = [row[0] for row in cursor.fetchall()]
        
        # Filter tables by schema match
        matching_tables = []
        file_columns_set = set(col.lower().strip() for col in columns)
        
        for table_name in all_tables:
            try:
                # Get table columns (excluding metadata columns)
                table_columns = db_manager.get_table_columns(connection, table_name, db_manager.data_schema)
                exclude_meta = {'ref_data_loadtime', 'ref_data_loadtype'}
                table_columns_set = set(col['name'].lower() for col in table_columns 
                                      if col['name'].lower() not in exclude_meta)
                
                # Check if file columns match table columns exactly
                if file_columns_set == table_columns_set:
                    matching_tables.append(table_name)
                    
            except Exception as e:
                # Skip tables that can't be queried
                await logger.log_warning("schema_match", f"Could not check schema for table {table_name}: {str(e)}")
                continue
        
        await logger.log_info("schema_match", f"Found {len(matching_tables)} schema-matched tables out of {len(all_tables)} total tables")
        
        return JSONResponse(
            content={"tables": matching_tables, "count": len(matching_tables)},
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache", 
                "Expires": "0"
            }
        )
        
    except Exception as e:
        error_msg = f"Failed to retrieve schema-matched tables: {str(e)}"
        await logger.log_error("schema_match", error_msg, traceback.format_exc())
        raise HTTPException(status_code=500, detail=error_msg)
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)