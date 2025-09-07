"""
Unified logging system for the Reference Data Auto Ingest System
"""

import os
import json
import traceback
import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:  # Fallback (should not happen in 3.12)
    ZoneInfo = None
from enum import Enum


class LogLevel(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class Logger:
    """Unified logger for all system activities"""
    
    def __init__(self):
        self.log_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            "log"
        )
        self.log_file = os.path.join(self.log_dir, "system.log")
        self.error_log_file = os.path.join(self.log_dir, "error.log")
        self.ingest_log_file = os.path.join(self.log_dir, "ingest.log")
        self._ensure_log_directory()
        self.timezone_name = os.getenv("LOG_TIMEZONE", "America/Toronto")
        self._tz = None
        if ZoneInfo:
            try:
                self._tz = ZoneInfo(self.timezone_name)
            except Exception:
                self._tz = None
    
    def _ensure_log_directory(self):
        """Ensure log directory exists"""
        os.makedirs(self.log_dir, exist_ok=True)
    
    async def log_info(
        self, 
        action_step: str, 
        message: str, 
        metadata: Optional[Dict[str, Any]] = None,
        source_ip: Optional[str] = None
    ):
        """Log information message"""
        await self._write_log(LogLevel.INFO, action_step, message, None, metadata, source_ip)
    
    async def log_warning(
        self, 
        action_step: str, 
        message: str, 
        metadata: Optional[Dict[str, Any]] = None,
        source_ip: Optional[str] = None
    ):
        """Log warning message"""
        await self._write_log(LogLevel.WARNING, action_step, message, None, metadata, source_ip)
    
    async def log_error(
        self, 
        action_step: str, 
        message: str, 
        traceback_info: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source_ip: Optional[str] = None
    ):
        """Log error message with traceback"""
        await self._write_log(LogLevel.ERROR, action_step, message, traceback_info, metadata, source_ip)
    
    async def log_debug(
        self, 
        action_step: str, 
        message: str, 
        metadata: Optional[Dict[str, Any]] = None,
        source_ip: Optional[str] = None
    ):
        """Log debug message"""
        await self._write_log(LogLevel.DEBUG, action_step, message, None, metadata, source_ip)
    
    async def _write_log(
        self,
        level: LogLevel,
        action_step: str,
        message: str,
        traceback_info: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source_ip: Optional[str] = None
    ):
        """Write log entry to appropriate log files"""
        try:
            # Local (configured TZ) and UTC timestamps
            now_local = datetime.now(self._tz) if self._tz else datetime.now()
            now_utc = datetime.utcnow()
            # Primary timestamp kept for backward compatibility (UTC ISO)
            log_entry = {
                "timestamp": now_utc.isoformat() + "Z",
                "timestamp_local": now_local.isoformat(),
                "tz": self.timezone_name if self._tz else None,
                "level": level.value,
                "action_step": action_step,
                "message": message,
                "source_ip": source_ip,
                "traceback": traceback_info,
                "metadata": metadata or {}
            }
            
            log_line = json.dumps(log_entry) + '\n'
            
            # Write to main system log
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
            
            # Write to specific log files based on level or action type
            if level == LogLevel.ERROR:
                with open(self.error_log_file, 'a', encoding='utf-8') as f:
                    f.write(log_line)
            
            # Write to ingest log for data processing activities
            if any(keyword in action_step.lower() for keyword in ['ingest', 'upload', 'import', 'process_file', 'validate']):
                with open(self.ingest_log_file, 'a', encoding='utf-8') as f:
                    f.write(log_line)
            
            # Also print to console for development
            print(f"[{log_entry['timestamp_local']}] {level.value} - {action_step}: {message}")
            if traceback_info:
                print(f"Traceback: {traceback_info}")
                
        except Exception as e:
            # Fallback logging to console if file logging fails
            print(f"Logger error: {str(e)}")
            print(f"Original log: [{level.value}] {action_step}: {message}")
    
    async def get_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent log entries"""
        try:
            if not os.path.exists(self.log_file):
                return []
            
            logs = []
            with open(self.log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # Get the last 'limit' lines
                recent_lines = lines[-limit:] if len(lines) > limit else lines
                
                for line in recent_lines:
                    try:
                        log_entry = json.loads(line.strip())
                        logs.append(log_entry)
                    except json.JSONDecodeError:
                        continue
            
            return logs
            
        except Exception as e:
            await self.log_error("get_logs", f"Failed to read logs: {str(e)}", traceback.format_exc())
            return []
    
    async def get_logs_by_type(self, log_type: str = "system", limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent log entries from specific log file"""
        try:
            log_file_map = {
                "system": self.log_file,
                "error": self.error_log_file,
                "ingest": self.ingest_log_file
            }
            
            target_file = log_file_map.get(log_type, self.log_file)
            
            if not os.path.exists(target_file):
                return []
            
            logs = []
            with open(target_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                
                # Get the last 'limit' lines
                recent_lines = lines[-limit:] if len(lines) > limit else lines
                
                for line in recent_lines:
                    try:
                        log_entry = json.loads(line.strip())
                        logs.append(log_entry)
                    except json.JSONDecodeError:
                        continue
            
            return logs
            
        except Exception as e:
            await self.log_error("get_logs_by_type", f"Failed to read {log_type} logs: {str(e)}", traceback.format_exc())
            return []

    async def clear_logs(self):
        """Clear all log entries"""
        try:
            log_files = [self.log_file, self.error_log_file, self.ingest_log_file]
            for log_file in log_files:
                if os.path.exists(log_file):
                    os.remove(log_file)
            await self.log_info("clear_logs", "All log files cleared")
        except Exception as e:
            print(f"Failed to clear logs: {str(e)}")
    
    def rotate_logs(self, max_size_mb: int = 10):
        """Rotate log files when they exceed max_size_mb"""
        try:
            max_size_bytes = max_size_mb * 1024 * 1024
            
            log_files = [
                (self.log_file, "system"),
                (self.error_log_file, "error"),
                (self.ingest_log_file, "ingest")
            ]
            
            for log_file, log_type in log_files:
                if os.path.exists(log_file) and os.path.getsize(log_file) > max_size_bytes:
                    # Create backup with timestamp
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    backup_file = f"{log_file}.{timestamp}.bak"
                    os.rename(log_file, backup_file)
                    print(f"Rotated {log_type} log: {backup_file}")
        except Exception as e:
            print(f"Failed to rotate logs: {str(e)}")


# Database-based logging (optional enhancement for production)
class DatabaseLogger(Logger):
    """Enhanced logger that also writes to database"""
    
    def __init__(self, db_manager=None):
        super().__init__()
        self.db_manager = db_manager
        self._log_table_created = False
    
    def _ensure_log_table(self, connection):
        """Ensure log table exists in database"""
        if self._log_table_created:
            return
        
        try:
            # Ensure required schemas exist first if db_manager provides method
            try:
                if self.db_manager and hasattr(self.db_manager, 'ensure_schemas_exist'):
                    self.db_manager.ensure_schemas_exist(connection)
            except Exception as _e:
                print(f"Warning: ensure_schemas_exist during logging failed: {_e}")
            cursor = connection.cursor()
            
            # Create log table if it doesn't exist
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[system_log]') AND type in (N'U'))
                BEGIN
                    CREATE TABLE [ref].[system_log] (
                        [log_id] bigint IDENTITY(1,1) PRIMARY KEY,
                        [timestamp] datetime2 DEFAULT GETDATE(),
                        [level] varchar(20),
                        [action_step] varchar(255),
                        [message] nvarchar(max),
                        [source_ip] varchar(45),
                        [error_traceback] nvarchar(max),
                        [file_name] varchar(255),
                        [table_name] varchar(255),
                        [row_count] int,
                        [metadata] nvarchar(max)
                    )
                END
            """)
            
            # Add source_ip column if it doesn't exist (for existing tables)
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sys.columns 
                             WHERE object_id = OBJECT_ID(N'[ref].[system_log]') 
                             AND name = 'source_ip')
                BEGIN
                    ALTER TABLE [ref].[system_log] 
                    ADD [source_ip] varchar(45)
                END
            """)
            
            self._log_table_created = True
            
        except Exception as e:
            print(f"Failed to create log table: {str(e)}")
    
    async def _write_log(
        self,
        level: LogLevel,
        action_step: str,
        message: str,
        traceback_info: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source_ip: Optional[str] = None
    ):
        """Write log entry to both file and database"""
        # Always write to file first
        await super()._write_log(level, action_step, message, traceback_info, metadata, source_ip)
        
        # Also write to database if available
        if self.db_manager:
            try:
                connection = self.db_manager.get_connection()
                self._ensure_log_table(connection)
                
                cursor = connection.cursor()
                
                # Extract common metadata fields
                file_name = None
                table_name = None
                row_count = None
                
                if metadata:
                    file_name = metadata.get('filename')
                    table_name = metadata.get('table_name')
                    row_count = metadata.get('row_count')

                # Use same timezone-aware timestamp (convert to naive for SQL if tz present)
                now = datetime.now(self._tz) if self._tz else datetime.now()
                ts_for_sql = now.replace(tzinfo=None)
                
                cursor.execute("""
                    INSERT INTO [ref].[system_log] 
                    ([timestamp], [level], [action_step], [message], [source_ip], [error_traceback], 
                     [file_name], [table_name], [row_count], [metadata])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ts_for_sql,
                    level.value,
                    action_step,
                    message,
                    source_ip,
                    traceback_info,
                    file_name,
                    table_name,
                    row_count,
                    json.dumps(metadata) if metadata else None
                ))
                
                connection.close()
                
            except Exception as e:
                # Don't fail the main operation if database logging fails
                print(f"Database logging failed: {str(e)}")
    
    async def get_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent log entries from database if available, otherwise from file"""
        if not self.db_manager:
            return await super().get_logs(limit)
        
        try:
            connection = self.db_manager.get_connection()
            cursor = connection.cursor()
            
            # Check if source_ip column exists and build query accordingly
            cursor.execute("""
                SELECT COUNT(*) FROM sys.columns 
                WHERE object_id = OBJECT_ID(N'[ref].[system_log]') 
                AND name = 'source_ip'
            """)
            has_source_ip = cursor.fetchone()[0] > 0
            
            if has_source_ip:
                # Use query with source_ip column
                cursor.execute("""
                    SELECT TOP (?)
                        [timestamp], [level], [action_step], [message], [source_ip],
                        [error_traceback], [file_name], [table_name], 
                        [row_count], [metadata]
                    FROM [ref].[system_log]
                    ORDER BY [timestamp] DESC, [log_id] DESC
                """, limit)
            else:
                # Use query without source_ip column for backward compatibility
                cursor.execute("""
                    SELECT TOP (?)
                        [timestamp], [level], [action_step], [message], 
                        [error_traceback], [file_name], [table_name], 
                        [row_count], [metadata]
                    FROM [ref].[system_log]
                    ORDER BY [timestamp] DESC, [log_id] DESC
                """, limit)
            
            cursor_results = cursor.fetchall()
            
            logs = []
            tzname = self.timezone_name
            for row in cursor_results:
                ts = row[0]
                timestamp_local = None
                if ts:
                    try:
                        if self._tz:
                            # Treat DB timestamp as naive local server time; convert to Toronto if different
                            naive = ts if ts.tzinfo is None else ts.replace(tzinfo=None)
                            timestamp_local = naive.astimezone(self._tz).isoformat() if hasattr(naive, 'astimezone') else naive.isoformat()
                        else:
                            timestamp_local = ts.isoformat()
                    except Exception:
                        timestamp_local = ts.isoformat()
                if has_source_ip:
                    # Parse row with source_ip column
                    log_entry = {
                        "timestamp": ts.isoformat() + 'Z' if ts else None,  # keep original as UTC-style (approx)
                        "timestamp_local": timestamp_local,
                        "tz": tzname,
                        "level": row[1],
                        "action_step": row[2],
                        "message": row[3],
                        "source_ip": row[4],
                        "traceback": row[5],
                        "metadata": {
                            "filename": row[6],
                            "table_name": row[7],
                            "row_count": row[8],
                            **(json.loads(row[9]) if row[9] else {})
                        }
                    }
                else:
                    # Parse row without source_ip column (backward compatibility)
                    log_entry = {
                        "timestamp": ts.isoformat() + 'Z' if ts else None,  # keep original as UTC-style (approx)
                        "timestamp_local": timestamp_local,
                        "tz": tzname,
                        "level": row[1],
                        "action_step": row[2],
                        "message": row[3],
                        "source_ip": None,  # Default for missing column
                        "traceback": row[4],
                        "metadata": {
                            "filename": row[5],
                            "table_name": row[6],
                            "row_count": row[7],
                            **(json.loads(row[8]) if row[8] else {})
                        }
                    }
                logs.append(log_entry)
            
            connection.close()
            return logs
            
        except Exception as e:
            print(f"Database log retrieval failed: {str(e)}")
            # Fallback to file-based logging
            return await super().get_logs(limit)
    
