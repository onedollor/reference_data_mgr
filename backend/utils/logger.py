"""
Unified logging system for the Reference Data Auto Ingest System
"""

import os
import json
import traceback
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
        self.log_file = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            "logs", 
            "system.log"
        )
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
        log_dir = os.path.dirname(self.log_file)
        os.makedirs(log_dir, exist_ok=True)
    
    async def log_info(
        self, 
        action_step: str, 
        message: str, 
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log information message"""
        await self._write_log(LogLevel.INFO, action_step, message, None, metadata)
    
    async def log_warning(
        self, 
        action_step: str, 
        message: str, 
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log warning message"""
        await self._write_log(LogLevel.WARNING, action_step, message, None, metadata)
    
    async def log_error(
        self, 
        action_step: str, 
        message: str, 
        traceback_info: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log error message with traceback"""
        await self._write_log(LogLevel.ERROR, action_step, message, traceback_info, metadata)
    
    async def log_debug(
        self, 
        action_step: str, 
        message: str, 
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Log debug message"""
        await self._write_log(LogLevel.DEBUG, action_step, message, None, metadata)
    
    async def _write_log(
        self,
        level: LogLevel,
        action_step: str,
        message: str,
        traceback_info: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Write log entry to file"""
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
                "traceback": traceback_info,
                "metadata": metadata or {}
            }
            
            # Write to log file
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(log_entry) + '\n')
            
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
    
    async def clear_logs(self):
        """Clear all log entries"""
        try:
            if os.path.exists(self.log_file):
                os.remove(self.log_file)
            await self.log_info("clear_logs", "Log file cleared")
        except Exception as e:
            print(f"Failed to clear logs: {str(e)}")


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
                        [error_traceback] nvarchar(max),
                        [file_name] varchar(255),
                        [table_name] varchar(255),
                        [row_count] int,
                        [metadata] nvarchar(max)
                    )
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
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Write log entry to both file and database"""
        # Always write to file first
        await super()._write_log(level, action_step, message, traceback_info, metadata)
        
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
                    ([timestamp], [level], [action_step], [message], [error_traceback], 
                     [file_name], [table_name], [row_count], [metadata])
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    ts_for_sql,
                    level.value,
                    action_step,
                    message,
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
            
            cursor.execute(f"""
                SELECT TOP ({limit})
                    [timestamp], [level], [action_step], [message], 
                    [error_traceback], [file_name], [table_name], 
                    [row_count], [metadata]
                FROM [ref].[system_log]
                ORDER BY [timestamp] DESC, [log_id] DESC
            """)
            
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
                log_entry = {
                    "timestamp": ts.isoformat() + 'Z' if ts else None,  # keep original as UTC-style (approx)
                    "timestamp_local": timestamp_local,
                    "tz": tzname,
                    "level": row[1],
                    "action_step": row[2],
                    "message": row[3],
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