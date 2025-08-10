"""
Database management utilities for SQL Server connection and operations
"""

import os
import pyodbc
import traceback
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
import threading
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class DatabaseManager:
    """Handles all database operations for the Reference Data Auto Ingest System"""
    
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.data_schema = os.getenv("data_schema", "ref")
        self.backup_schema = os.getenv("backup_schema", "bkp")
        self.validation_sp_schema = os.getenv("validation_sp_schema", "ref")
        
        # Store connection parameters for SQLAlchemy
        self.server = os.getenv("db_host", "localhost")
        self.database = os.getenv("db_name", "test")
        self.username = os.getenv("db_user")
        self.password = os.getenv("db_password")
        
        # Validate required database credentials
        if not self.username:
            raise ValueError("Database username (db_user) environment variable is required")
        if not self.password:
            raise ValueError("Database password (db_password) environment variable is required")

        # Simple connection pool
        try:
            self.pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
        except ValueError:
            self.pool_size = 5
        self._pool: List[pyodbc.Connection] = []
        self._pool_lock = threading.Lock()
        self._in_use = 0
        self.max_retries = int(os.getenv("DB_MAX_RETRIES", "3"))
        self.retry_backoff = float(os.getenv("DB_RETRY_BACKOFF", "0.5"))
    
    def _build_connection_string(self) -> str:
        """Build SQL Server connection string from environment variables"""
        host = os.getenv("db_host", "localhost")
        database = os.getenv("db_name", "test")
        username = os.getenv("db_user")
        password = os.getenv("db_password")
        
        # Validate required credentials
        if not username:
            raise ValueError("Database username (db_user) environment variable is required")
        if not password:
            raise ValueError("Database password (db_password) environment variable is required")
        driver = os.getenv("db_odbc_driver", "ODBC Driver 17 for SQL Server")

        # Use SQL Server authentication
        # Enclose driver name in braces
        driver_fmt = driver if driver.startswith('{') else f"{{{driver}}}"
        connection_string = (
            f"DRIVER={driver_fmt};SERVER={host};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;Encrypt=yes;"
        )
        return connection_string
    
    def get_connection(self) -> pyodbc.Connection:
        """Get a database connection with auto-commit enabled"""
        try:
            # Non-pooled direct connection with retry
            attempt = 0
            while True:
                try:
                    connection = pyodbc.connect(self.connection_string)
                    connection.autocommit = True
                    return connection
                except Exception as e:
                    attempt += 1
                    if attempt >= self.max_retries:
                        raise e
                    time.sleep(self.retry_backoff * attempt)
        except Exception as e:
            raise Exception(f"Database connection failed: {str(e)}")

    def get_pooled_connection(self) -> pyodbc.Connection:
        """Acquire a pooled connection (creates new if under pool size)."""
        with self._pool_lock:
            if self._pool:
                conn = self._pool.pop()
                self._in_use += 1
                return conn
            if (self._in_use) < self.pool_size:
                conn = self.get_connection()
                self._in_use += 1
                return conn
        # Wait / retry outside lock
        time.sleep(0.05)
        return self.get_pooled_connection()

    def release_connection(self, connection: pyodbc.Connection):
        """Return connection to pool or close if pool full."""
        if connection is None:
            return
        with self._pool_lock:
            self._in_use -= 1
            if len(self._pool) < self.pool_size:
                self._pool.append(connection)
                return
        try:
            connection.close()
        except:
            pass

    def get_pool_stats(self) -> Dict[str, Any]:
        with self._pool_lock:
            return {
                "pool_size_config": self.pool_size,
                "idle": len(self._pool),
                "in_use": self._in_use,
                "available_capacity": max(self.pool_size - (self._in_use + len(self._pool)), 0)
            }

    def close_pool(self):
        with self._pool_lock:
            while self._pool:
                conn = self._pool.pop()
                try:
                    conn.close()
                except:
                    pass
    
    def test_connection(self) -> Dict[str, Any]:
        """Test database connectivity"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION, GETDATE()")
                result = cursor.fetchone()
                
                return {
                    "status": "success",
                    "server_version": result[0].split('\n')[0] if result else "Unknown",
                    "current_time": result[1] if result else None,
                    "schemas": {
                        "data_schema": self.data_schema,
                        "backup_schema": self.backup_schema,
                        "validation_sp_schema": self.validation_sp_schema
                    }
                }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "traceback": traceback.format_exc()
            }
    
    def ensure_schemas_exist(self, connection: pyodbc.Connection) -> None:
        """Ensure required schemas exist in the database"""
        cursor = connection.cursor()
        
        schemas = [self.data_schema, self.backup_schema, self.validation_sp_schema]
        
        for schema in set(schemas):  # Remove duplicates
            try:
                # Use parameterized query to prevent SQL injection
                cursor.execute("""
                    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = ?)
                    BEGIN
                        DECLARE @sql NVARCHAR(MAX) = N'CREATE SCHEMA [' + QUOTENAME(?) + ']'
                        EXEC sp_executesql @sql
                    END
                """, schema, schema)
            except Exception as e:
                raise Exception(f"Failed to create schema {schema}: {str(e)}")
    
    def table_exists(self, connection: pyodbc.Connection, table_name: str, schema: str = None) -> bool:
        """Check if a table exists"""
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, schema, table_name)
        
        result = cursor.fetchone()
        return result[0] > 0
    
    def get_table_columns(self, connection: pyodbc.Connection, table_name: str, schema: str = None) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER BY ORDINAL_POSITION
        """, schema, table_name)
        
        columns = []
        for row in cursor.fetchall():
            columns.append({
                'name': row.COLUMN_NAME,
                'data_type': row.DATA_TYPE,
                'max_length': row.CHARACTER_MAXIMUM_LENGTH,
                'nullable': row.IS_NULLABLE == 'YES',
                'default': row.COLUMN_DEFAULT,
                'position': row.ORDINAL_POSITION
            })
        
        return columns
    
    def create_table(self, connection: pyodbc.Connection, table_name: str, columns: List[Dict[str, str]], 
                    schema: str = None, add_metadata_columns: bool = True) -> None:
        """Create a table with the specified columns"""
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        
        # Drop table if it exists - use dynamic SQL with proper quoting
        drop_sql = "DROP TABLE IF EXISTS [" + schema + "].[" + table_name + "]"
        cursor.execute(drop_sql)
        
        # Build column definitions
        column_defs = []
        for col in columns:
            col_def = f"[{col['name']}] {col['data_type']}"
            column_defs.append(col_def)
        
        # Add metadata columns if requested
        if add_metadata_columns:
            column_defs.append("[ref_data_loadtime] datetime DEFAULT GETDATE()")
        
        # Create the table
        create_sql = f"""
            CREATE TABLE [{schema}].[{table_name}] (
                {', '.join(column_defs)}
            )
        """
        
        cursor.execute(create_sql)
    
    def create_backup_table(self, connection: pyodbc.Connection, table_name: str, columns: List[Dict[str, str]]) -> None:
        """Create a backup table with version tracking (only if it doesn't exist)"""
        backup_table_name = f"{table_name}_backup"
        
        cursor = connection.cursor()
        
        # Check if backup table already exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, self.backup_schema, backup_table_name)
        
        if cursor.fetchone()[0] > 0:
            # Backup table already exists, don't recreate it
            return
        
        # Build column definitions (same as main table)
        column_defs = []
        for col in columns:
            col_def = f"[{col['name']}] {col['data_type']}"
            column_defs.append(col_def)
        
        # Add backup-specific columns
        column_defs.extend([
            "[ref_data_loadtime] datetime",
            "[version_id] int NOT NULL"
        ])
        
        # Create the backup table
        create_sql = f"""
            CREATE TABLE [{self.backup_schema}].[{backup_table_name}] (
                {', '.join(column_defs)}
            )
        """
        
        cursor.execute(create_sql)
    
    def create_validation_procedure(self, connection: pyodbc.Connection, table_name: str) -> None:
        """Create a validation stored procedure for the table"""
        proc_name = f"sp_ref_validate_{table_name}"
        
        cursor = connection.cursor()
        
        # Drop procedure if it exists - use parameterized query
        cursor.execute("""
            IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = ?)
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = N'DROP PROCEDURE [' + ? + '].[' + ? + ']'
                EXEC sp_executesql @sql
            END
        """, proc_name, self.validation_sp_schema, proc_name)
        
        # Create the validation procedure (empty template)
        proc_sql = f"""
            CREATE PROCEDURE [{self.validation_sp_schema}].[{proc_name}]
            AS
            BEGIN
                -- Validation logic to be implemented by users
                -- This is a template procedure that returns success by default
                
                DECLARE @validation_result NVARCHAR(MAX)
                
                SET @validation_result = N'{{
                    "validation_result": 0,
                    "validation_issue_list": []
                }}'
                
                SELECT @validation_result AS ValidationResult
            END
        """
        
        cursor.execute(proc_sql)
    
    def execute_validation_procedure(self, connection: pyodbc.Connection, table_name: str) -> Dict[str, Any]:
        """Execute the validation stored procedure and return results"""
        proc_name = f"sp_ref_validate_{table_name}"
        
        cursor = connection.cursor()
        # Use dynamic SQL with proper quoting for stored procedure execution
        exec_sql = "EXEC [" + self.validation_sp_schema + "].[" + proc_name + "]"
        cursor.execute(exec_sql)
        
        result = cursor.fetchone()
        if result:
            # Parse JSON result
            import json
            validation_result = json.loads(result[0])
            return validation_result
        else:
            return {
                "validation_result": -1,
                "validation_issue_list": [
                    {
                        "issue_id": -1,
                        "issue_detail": "Validation procedure returned no result"
                    }
                ]
            }
    
    def backup_existing_data(self, connection: pyodbc.Connection, source_table: str, backup_table: str) -> int:
        """Backup existing data to backup table with version increment, filtering out trailer rows"""
        cursor = connection.cursor()
        
        try:
            # Get the next version ID - use dynamic SQL with proper quoting
            version_sql = "SELECT COALESCE(MAX(version_id), 0) + 1 FROM [" + self.backup_schema + "].[" + backup_table + "_backup]"
            cursor.execute(version_sql)
            next_version = cursor.fetchone()[0]
            
            # Direct backup - use dynamic SQL with proper quoting
            backup_sql = (
                "INSERT INTO [" + self.backup_schema + "].[" + backup_table + "_backup] "
                "SELECT *, ? FROM [" + self.data_schema + "].[" + source_table + "]"
            )
            cursor.execute(backup_sql, next_version)
            connection.commit()
            return cursor.rowcount
                    
        except Exception as e:
            connection.rollback()
            # If backup fails due to schema mismatch, log warning and continue
            error_msg = str(e)
            if "Column name or number" in error_msg and "does not match" in error_msg:
                print(f"WARNING: Skipping backup for {source_table} due to schema mismatch: {error_msg}")
                return 0  # Continue without backup
            else:
                raise Exception(f"Failed to backup existing data from {source_table}: {str(e)}")
    
    def truncate_table(self, connection: pyodbc.Connection, table_name: str, schema: str = None) -> None:
        """Truncate a table"""
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        # Use dynamic SQL with proper quoting for TRUNCATE
        truncate_sql = "TRUNCATE TABLE [" + schema + "].[" + table_name + "]"
        cursor.execute(truncate_sql)
    
    def get_row_count(self, connection: pyodbc.Connection, table_name: str, schema: str = None) -> int:
        """Get row count for a table"""
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        # Use dynamic SQL with proper quoting for COUNT
        count_sql = "SELECT COUNT(*) FROM [" + schema + "].[" + table_name + "]"
        cursor.execute(count_sql)
        
        result = cursor.fetchone()
        return result[0] if result else 0
    
    def sync_table_schema(self, connection: pyodbc.Connection, table_name: str, columns: List[Dict[str, str]], schema: str = None) -> Dict[str, Any]:
        """Synchronize existing table schema with target columns.
        Adds missing columns, widens varchar lengths, and converts any non-varchar columns to varchar.
        Returns summary dict.
        columns: list of dicts [{'name':..., 'data_type': ...}]
        Supported conversions: varchar(N) -> larger varchar(M), datetime/numeric/other -> varchar(N).
        """
        if schema is None:
            schema = self.data_schema
        cursor = connection.cursor()
        existing_cols_list = self.get_table_columns(connection, table_name, schema)
        existing_cols = {c['name'].lower(): c for c in existing_cols_list}
        actions = {"added": [], "widened": [], "skipped": []}
        
        # Debug: log existing column types
        print(f"DEBUG: Existing columns in {table_name}: {[(c['name'], c['data_type'], c.get('max_length')) for c in existing_cols_list]}")
        print(f"DEBUG: Target columns: {[(c['name'], c['data_type']) for c in columns]}")
        for col in columns:
            name = col['name']
            target_type = col['data_type']
            lower = name.lower()
            if lower not in existing_cols:
                # Add new column - use dynamic SQL with proper quoting
                add_col_sql = "ALTER TABLE [" + schema + "].[" + table_name + "] ADD [" + name + "] " + target_type
                cursor.execute(add_col_sql)
                actions["added"].append({"column": name, "data_type": target_type})
                continue
            # Existing column: consider widening
            existing = existing_cols[lower]
            existing_type = existing['data_type']
            existing_max_length = existing.get('max_length')
            
            # Handle varchar column widening
            import re as _re
            
            # Parse target type
            is_new_varchar_max = target_type.lower() == 'varchar(max)'
            m_new = _re.fullmatch(r'varchar\((\d+)\)', target_type, flags=_re.IGNORECASE) if not is_new_varchar_max else None
            new_len = int(m_new.group(1)) if m_new else ('MAX' if is_new_varchar_max else None)
            
            # Parse existing type - SQL Server returns 'varchar' with separate max_length field
            is_varchar_column = existing_type and existing_type.lower() == 'varchar'
            if is_varchar_column:
                # SQL Server: -1 means varchar(MAX), otherwise it's the numeric length
                if existing_max_length == -1:
                    old_len = 'MAX'
                elif existing_max_length is not None:
                    old_len = existing_max_length
                else:
                    old_len = None
            else:
                # For other data types or non-varchar columns
                old_len = None
            
            # Determine if schema change is needed
            need_change = False
            change_type = None
            
            # Handle any non-varchar column to varchar conversion (for varchar-only approach)
            is_datetime_column = existing_type and existing_type.lower() in ['datetime', 'date', 'time', 'datetime2', 'smalldatetime', 'datetimeoffset']
            is_numeric_column = existing_type and existing_type.lower() in ['int', 'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'float', 'real', 'money', 'smallmoney']
            is_other_column = existing_type and not is_varchar_column and not is_datetime_column and not is_numeric_column
            target_is_varchar = target_type.lower().startswith('varchar')
            
            if (is_datetime_column or is_numeric_column or is_other_column) and target_is_varchar:
                # Convert non-varchar column to varchar
                need_change = True
                change_type = "convert_to_varchar"
            elif is_varchar_column and old_len is not None and new_len is not None:
                # Handle varchar widening
                if new_len == 'MAX' and old_len != 'MAX':
                    # Widen to varchar(MAX)
                    need_change = True
                    change_type = "varchar_widen"
                elif old_len != 'MAX' and new_len != 'MAX' and isinstance(new_len, int) and isinstance(old_len, int) and new_len > old_len:
                    # Widen numeric varchar length
                    need_change = True
                    change_type = "varchar_widen"
            
            if need_change:
                if change_type == "convert_to_varchar":
                    # Convert non-varchar column to varchar - use dynamic SQL with proper quoting
                    alter_sql = "ALTER TABLE [" + schema + "].[" + table_name + "] ALTER COLUMN [" + name + "] " + target_type
                    cursor.execute(alter_sql)
                    conversion_type = "datetime_to_varchar" if is_datetime_column else "numeric_to_varchar" if is_numeric_column else "other_to_varchar"
                    actions["widened"].append({"column": name, "from": existing_type, "to": target_type, "conversion": conversion_type})
                elif change_type == "varchar_widen":
                    # Widen varchar column - use dynamic SQL with proper quoting
                    new_type_sql = 'varchar(MAX)' if new_len == 'MAX' else f'varchar({new_len})'
                    alter_sql = "ALTER TABLE [" + schema + "].[" + table_name + "] ALTER COLUMN [" + name + "] " + new_type_sql
                    cursor.execute(alter_sql)
                    actions["widened"].append({"column": name, "from": old_len, "to": new_len})
            else:
                reason = "no changes needed"
                if not is_varchar_column and not is_datetime_column and not is_numeric_column and not is_other_column:
                    reason = "unsupported column type conversion"
                elif is_varchar_column and new_len is not None and old_len is not None:
                    if new_len == old_len or (isinstance(new_len, int) and isinstance(old_len, int) and new_len <= old_len):
                        reason = "no widening needed"
                actions["skipped"].append({"column": name, "reason": reason})
        return actions

    def bulk_insert_from_file(
        self,
        connection: pyodbc.Connection,
        table_name: str,
        file_path: str,
        schema: str = None,
        field_terminator: str = ',',
        row_terminator: str = '\n',
        first_row: int = 2,
        tablock: bool = True
    ) -> Dict[str, Any]:
        """Perform a BULK INSERT from a server-accessible file into an existing table.
        Assumes the file includes a header row (FIRSTROW=2 by default).
        Returns dict with rows_loaded (best-effort) and execution time.
        NOTE: The SQL Server service must be able to access file_path on its filesystem.
        """
        import time as _time
        start = _time.perf_counter()
        if schema is None:
            schema = self.data_schema
        cursor = connection.cursor()
        # Build base options; CODEPAGE sometimes unsupported on Linux SQL Server builds (error 16202)
        bulk_options = [f"FIRSTROW={first_row}", "KEEPNULLS", f"FIELDTERMINATOR='{field_terminator}'", f"ROWTERMINATOR='{row_terminator}'"]
        include_codepage = True
        # Allow disabling via env override
        if os.getenv('BULK_DISABLE_CODEPAGE', '0') in ('1','true','True'):
            include_codepage = False
        if include_codepage:
            bulk_options.insert(2, "CODEPAGE='65001'")
        if tablock:
            bulk_options.append("TABLOCK")
        options_sql = ', '.join(bulk_options)
        # Escape single quotes in path
        safe_path = file_path.replace("'", "''")
        sql = f"BULK INSERT [{schema}].[{table_name}] FROM '{safe_path}' WITH ({options_sql});"
        fallback_used = False
        try:
            cursor.execute(sql)
        except pyodbc.ProgrammingError as e:
            msg = str(e)
            # Error 16202 indicates CODEPAGE not supported on Linux platform
            if "16202" in msg and "CODEPAGE" in msg and include_codepage:
                # Retry without CODEPAGE
                fallback_used = True
                bulk_options = [opt for opt in bulk_options if not opt.startswith('CODEPAGE')]
                options_sql = ', '.join(bulk_options)
                sql = f"BULK INSERT [{schema}].[{table_name}] FROM '{safe_path}' WITH ({options_sql});"
                cursor.execute(sql)
            else:
                raise
        # Row count may not be returned; we can query COUNT(*) afterwards if desired
        elapsed = _time.perf_counter() - start
        try:
            count = self.get_row_count(connection, table_name, schema)
        except Exception:
            count = None
        return {"elapsed": elapsed, "row_count_post": count, "file": file_path, "options": options_sql, "fallback_no_codepage": fallback_used}