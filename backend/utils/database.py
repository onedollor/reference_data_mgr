"""
Database management utilities for SQL Server connection and operations
"""

import os
import pyodbc
import traceback
import re
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
        self.staff_database = os.getenv("staff_database", "StaffDatabase")
        self.username = os.getenv("db_user")
        self.password = os.getenv("db_password")
        
        # Validate required database credentials
        if not self.username:
            raise ValueError("Database username (db_user) environment variable is required")
        if not self.password:
            raise ValueError("Database password (db_password) environment variable is required")
        
        # Dynamic stored procedure name based on database name
        self.postload_sp_name = f"usp_reference_data_{self.database}"

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
            column_defs.append("[loadtype] varchar(255)")
        
        # Create the table
        create_sql = f"""
            CREATE TABLE [{schema}].[{table_name}] (
                {', '.join(column_defs)}
            )
        """
        
        cursor.execute(create_sql)
    
    def create_backup_table(self, connection: pyodbc.Connection, table_name: str, columns: List[Dict[str, str]]) -> None:
        """Create a backup table with version tracking, validating schema compatibility"""
        backup_table_name = f"{table_name}_backup"
        cursor = connection.cursor()
        
        # Check if backup table already exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, self.backup_schema, backup_table_name)
        
        backup_exists = cursor.fetchone()[0] > 0
        
        if backup_exists:
            # Validate schema compatibility
            if self._backup_schema_matches(connection, backup_table_name, columns):
                # Schema matches, backup table is compatible
                return
            else:
                # Schema doesn't match, rename existing backup table and create new one
                timestamp_suffix = self._get_timestamp_suffix()
                old_backup_name = f"{backup_table_name}_{timestamp_suffix}"
                
                # Rename existing backup table
                rename_sql = (
                    "EXEC sp_rename '[" + self.backup_schema + "].[" + backup_table_name + "]', '" + old_backup_name + "'"
                )
                cursor.execute(rename_sql)
                print(f"INFO: Renamed incompatible backup table {backup_table_name} to {old_backup_name}")
        
        # Build column definitions (same as main table)
        column_defs = []
        for col in columns:
            col_def = f"[{col['name']}] {col['data_type']}"
            column_defs.append(col_def)
        
        # Add backup-specific columns
        column_defs.extend([
            "[ref_data_loadtime] datetime",
            "[loadtype] varchar(255)",
            "[version_id] int NOT NULL"
        ])
        
        # Create the backup table with proper schema quoting
        create_sql = (
            "CREATE TABLE [" + self.backup_schema + "].[" + backup_table_name + "] (" +
            ', '.join(column_defs) + ")"
        )
        cursor.execute(create_sql)
    
    def _backup_schema_matches(self, connection: pyodbc.Connection, backup_table_name: str, expected_columns: List[Dict[str, str]]) -> bool:
        """Check if existing backup table schema matches expected columns"""
        try:
            # Get existing backup table columns (excluding metadata columns)
            existing_columns = self.get_table_columns(connection, backup_table_name, self.backup_schema)
            
            # Filter out backup-specific metadata columns
            data_columns = [col for col in existing_columns 
                           if col['name'] not in ['ref_data_loadtime', 'loadtype', 'version_id']]
            
            # Create comparison sets (normalize data types)
            expected_set = set()
            for col in expected_columns:
                col_name = col['name'].lower()
                col_type = self._normalize_data_type(col['data_type'])
                expected_set.add((col_name, col_type))
            
            existing_set = set()
            for col in data_columns:
                col_name = col['name'].lower()
                col_type = self._normalize_data_type(col['data_type'], col.get('max_length'))
                existing_set.add((col_name, col_type))
            
            return expected_set == existing_set
            
        except Exception as e:
            # If we can't validate schema, assume it doesn't match to be safe
            print(f"WARNING: Could not validate backup table schema: {str(e)}")
            return False
    
    def _normalize_data_type(self, data_type: str, max_length: int = None) -> str:
        """Normalize data type for comparison (handle varchar length variations)"""
        data_type = data_type.lower().strip()
        
        # Handle varchar with lengths
        if data_type.startswith('varchar'):
            if max_length == -1:
                return 'varchar(max)'
            elif max_length is not None:
                return f'varchar({max_length})'
            else:
                # Extract length from data_type string if present
                import re
                match = re.search(r'varchar\((\d+|max)\)', data_type, re.IGNORECASE)
                if match:
                    return f'varchar({match.group(1).lower()})'
                else:
                    return 'varchar(4000)'  # Default fallback
        
        return data_type
    
    def _get_timestamp_suffix(self) -> str:
        """Generate timestamp suffix in format yyyyMMddHHmmss"""
        from datetime import datetime
        return datetime.now().strftime('%Y%m%d%H%M%S')
    
    def create_validation_procedure(self, connection: pyodbc.Connection, table_name: str) -> None:
        """Ensure a validation stored procedure exists for the table.
        Will NOT drop/recreate if it already exists (idempotent)."""
        proc_name = f"sp_ref_validate_{table_name}"
        cursor = connection.cursor()

        # Create only if missing
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = ?)
            BEGIN
                DECLARE @sql NVARCHAR(MAX) = N'
                CREATE PROCEDURE [' + ? + '].[' + ? + ']
                AS
                BEGIN
                    SET NOCOUNT ON;
                    DECLARE @validation_result NVARCHAR(MAX);
                    SET @validation_result = N''{
                        "validation_result": 0,
                        "validation_issue_list": []
                    }'';
                    SELECT @validation_result AS ValidationResult;
                END'
                EXEC sp_executesql @sql
            END
        """, proc_name, self.validation_sp_schema, proc_name)
    
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
    
    def ensure_backup_table_metadata_columns(self, connection: pyodbc.Connection, backup_table_name: str) -> Dict[str, Any]:
        """Ensure backup table has all required metadata columns"""
        cursor = connection.cursor()
        existing_cols_list = self.get_table_columns(connection, backup_table_name, self.backup_schema)
        existing_cols = {c['name'].lower(): c for c in existing_cols_list}
        actions = {"added": [], "skipped": []}
        
        # Define required backup metadata columns
        backup_metadata_columns = [
            {"name": "ref_data_loadtime", "data_type": "datetime", "default": ""},
            {"name": "loadtype", "data_type": "varchar(255)", "default": ""},
            {"name": "version_id", "data_type": "int NOT NULL", "default": ""}
        ]
        
        for meta_col in backup_metadata_columns:
            col_name_lower = meta_col["name"].lower()
            if col_name_lower not in existing_cols:
                # Add missing metadata column
                add_col_sql = f"ALTER TABLE [{self.backup_schema}].[{backup_table_name}] ADD [{meta_col['name']}] {meta_col['data_type']}"
                cursor.execute(add_col_sql)
                actions["added"].append({"column": meta_col["name"], "data_type": meta_col["data_type"]})
                print(f"Added metadata column {meta_col['name']} to backup table {backup_table_name}")
            else:
                actions["skipped"].append({"column": meta_col["name"], "reason": "already exists"})
        
        return actions

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
    
    def ensure_metadata_columns(self, connection: pyodbc.Connection, table_name: str, schema: str = None) -> Dict[str, Any]:
        """Ensure metadata columns (ref_data_loadtime, loadtype) exist in the table"""
        if schema is None:
            schema = self.data_schema
        
        cursor = connection.cursor()
        existing_cols_list = self.get_table_columns(connection, table_name, schema)
        existing_cols = {c['name'].lower(): c for c in existing_cols_list}
        actions = {"added": [], "skipped": []}
        
        # Define required metadata columns
        metadata_columns = [
            {"name": "ref_data_loadtime", "data_type": "datetime", "default": "DEFAULT GETDATE()"},
            {"name": "loadtype", "data_type": "varchar(255)", "default": ""}
        ]
        
        for meta_col in metadata_columns:
            col_name_lower = meta_col["name"].lower()
            if col_name_lower not in existing_cols:
                # Add missing metadata column
                default_clause = meta_col["default"] if meta_col["default"] else ""
                add_col_sql = f"ALTER TABLE [{schema}].[{table_name}] ADD [{meta_col['name']}] {meta_col['data_type']} {default_clause}"
                cursor.execute(add_col_sql)
                actions["added"].append({"column": meta_col["name"], "data_type": meta_col["data_type"]})
                print(f"Added metadata column {meta_col['name']} to {schema}.{table_name}")
            else:
                actions["skipped"].append({"column": meta_col["name"], "reason": "already exists"})
        
        return actions

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

    
    def ensure_reference_data_cfg_table(self, connection: pyodbc.Connection) -> None:
        """Ensure Reference_Data_Cfg table exists in staff database dbo schema (configurable via staff_database env var)"""
        cursor = connection.cursor()
        
        # Check if table exists in staff database dbo schema
        table_exists_sql = f"""
            SELECT COUNT(*) 
            FROM [{self.staff_database}].INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Reference_Data_Cfg'
        """
        cursor.execute(table_exists_sql)
        exists = cursor.fetchone()[0] > 0
        
        if not exists:
            # Create the Reference_Data_Cfg table
            create_table_sql = f"""
                CREATE TABLE [{self.staff_database}].[dbo].[Reference_Data_Cfg](
                    [sp_name] [varchar](255) NOT NULL,
                    [ref_name] [varchar](255) NOT NULL,
                    [source_db] [varchar](4000) NULL,
                    [source_schema] [varchar](255) NOT NULL,
                    [source_table] [varchar](255) NOT NULL,
                    [is_enabled] [int] NULL
                ) ON [PRIMARY]
            """
            cursor.execute(create_table_sql)
            
            # Add primary key constraint
            add_pk_sql = f"""
                ALTER TABLE [{self.staff_database}].[dbo].[Reference_Data_Cfg] 
                ADD CONSTRAINT [pk_ref_data_cfg] PRIMARY KEY CLUSTERED 
                ([ref_name] ASC)
                WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, 
                      IGNORE_DUP_KEY = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) 
                ON [PRIMARY]
            """
            cursor.execute(add_pk_sql)
            
            connection.commit()
            print("Created Reference_Data_Cfg table in dbo schema")
    
    def ensure_postload_stored_procedure(self, connection: pyodbc.Connection) -> None:
        """Ensure ref.usp_reference_data_{database_name} stored procedure exists"""
        cursor = connection.cursor()
        
        try:
            # Check if stored procedure exists in ref schema
            check_sp_sql = f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.ROUTINES 
                WHERE ROUTINE_SCHEMA = 'ref' AND ROUTINE_NAME = '{self.postload_sp_name}' AND ROUTINE_TYPE = 'PROCEDURE'
            """
            cursor.execute(check_sp_sql)
            exists = cursor.fetchone()[0] > 0
            
            if not exists:
                # Create empty stored procedure
                create_sp_sql = f"""
                    CREATE PROCEDURE [ref].[{self.postload_sp_name}]
                    AS
                    BEGIN
                        -- Empty post-load procedure - customize as needed
                        -- This procedure is called after each successful reference data ingestion
                        PRINT 'Post-load procedure executed for {self.database} - no custom logic defined'
                    END
                """
                cursor.execute(create_sp_sql)
                connection.commit()
                print(f"Created empty ref.{self.postload_sp_name} stored procedure")
            else:
                print(f"ref.{self.postload_sp_name} stored procedure already exists")
                
        except Exception as e:
            connection.rollback()
            error_msg = f"Failed to create ref.{self.postload_sp_name} stored procedure: {str(e)}"
            print(f"WARNING: {error_msg}")
            # Don't raise - this shouldn't fail the entire startup
    
    def determine_load_type(self, connection: pyodbc.Connection, table_name: str, current_load_mode: str, override_load_type: str = None) -> str:
        """
        Determine the loadtype value based on existing data and current load mode.
        Rules:
        - If override_load_type provided: Use override ('F' or 'A')
        - First time ingest: Use current load mode ('F' for full, 'A' for append)
        - Subsequent ingests: Check existing distinct loadtype values
          - If only 'F' exists: Use 'F'
          - If only 'A' exists: Use 'A' 
          - If both 'A' and 'F' exist: Use 'F'
        """
        try:
            # If user provided override, use it
            if override_load_type:
                override_upper = override_load_type.strip().upper()
                if override_upper in ['F', 'A', 'FULL', 'APPEND']:
                    return 'F' if override_upper in ['F', 'FULL'] else 'A'
            cursor = connection.cursor()
            
            # Check if table exists and has data
            if not self.table_exists(connection, table_name):
                # First time - use current load mode
                return 'F' if current_load_mode == 'full' else 'A'
            
            # Get row count
            row_count = self.get_row_count(connection, table_name)
            if row_count == 0:
                # Empty table - use current load mode
                return 'F' if current_load_mode == 'full' else 'A'
            
            # Get distinct loadtype values from existing data
            query = f"SELECT DISTINCT [loadtype] FROM [{self.data_schema}].[{table_name}] WHERE [loadtype] IS NOT NULL"
            cursor.execute(query)
            existing_types = set()
            for row in cursor.fetchall():
                if row[0]:  # Not NULL
                    existing_types.add(row[0].strip().upper())
            
            # Apply rules
            if not existing_types:
                # No existing loadtype data - use current load mode
                return 'F' if current_load_mode == 'full' else 'A'
            elif 'F' in existing_types and 'A' in existing_types:
                # Both F and A exist - return F
                return 'F'
            elif 'F' in existing_types:
                # Only F exists - return F
                return 'F'
            elif 'A' in existing_types:
                # Only A exists - return A
                return 'A'
            else:
                # Unknown values - default to current load mode
                return 'F' if current_load_mode == 'full' else 'A'
                
        except Exception as e:
            # On error, default to current load mode
            print(f"WARNING: Failed to determine load type for {table_name}: {str(e)}")
            return 'F' if current_load_mode == 'full' else 'A'

    # ---------------- Rollback / Backup Introspection Helpers -----------------
    def list_backup_tables(self, connection: pyodbc.Connection) -> List[Dict[str, Any]]:
        """Return metadata for all backup tables in backup schema with validation of related main & stage tables."""
        cursor = connection.cursor()
        cursor.execute("""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
              AND TABLE_NAME LIKE '%[_]backup'
        """, self.backup_schema)
        rows = cursor.fetchall()
        results = []
        for r in rows:
            backup_table = r[0]
            if not backup_table.endswith('_backup'):
                continue
            base_name = backup_table[:-7]  # remove _backup
            # Validate related main and stage
            has_main = self.table_exists(connection, base_name, self.data_schema)
            has_stage = self.table_exists(connection, f"{base_name}_stage", self.data_schema)
            # Count versions
            version_count = 0
            latest_version = None
            try:
                v_cursor = connection.cursor()
                v_cursor.execute(
                    "SELECT COUNT(DISTINCT version_id), MAX(version_id) FROM [" + self.backup_schema + "].[" + backup_table + "]"
                )
                vrow = v_cursor.fetchone()
                if vrow:
                    version_count = vrow[0] or 0
                    latest_version = vrow[1]
            except Exception:
                pass
            results.append({
                'backup_table': backup_table,
                'base_name': base_name,
                'has_main': has_main,
                'has_stage': has_stage,
                'version_count': version_count,
                'latest_version': latest_version
            })
        return results

    def get_backup_versions(self, connection: pyodbc.Connection, base_name: str) -> List[int]:
        """Return list of version_ids for a given backup base name (descending)."""
        if not base_name or not re.match(r'^[A-Za-z0-9_]+$', base_name):
            return []
        backup_table = f"{base_name}_backup"
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT DISTINCT version_id FROM [" + self.backup_schema + "].[" + backup_table + "] ORDER BY version_id DESC"
            )
            return [row[0] for row in cursor.fetchall() if row[0] is not None]
        except Exception:
            return []

    def get_backup_version_rows(self, connection: pyodbc.Connection, base_name: str, version_id: int, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
        """Return rows for a version with pagination (offset, limit) plus columns & total row count.
        Enforces 1 <= limit <= 1000. Offset >= 0.
        """
        result = {'rows': [], 'columns': [], 'total_rows': 0, 'offset': offset, 'limit': limit}
        if not base_name or not re.match(r'^[A-Za-z0-9_]+$', base_name):
            return result
        backup_table = f"{base_name}_backup"
        cursor = connection.cursor()
        try:
            # Enforce sane bounds (1..1000)
            try:
                limit = int(limit)
            except Exception:
                limit = 50
            if limit < 1:
                limit = 1
            if limit > 1000:
                limit = 1000
            try:
                offset = int(offset)
            except Exception:
                offset = 0
            if offset < 0:
                offset = 0
            result['limit'] = limit
            result['offset'] = offset
            # columns
            cols = self.get_table_columns(connection, backup_table, self.backup_schema)
            col_names = [c['name'] for c in cols]
            result['columns'] = col_names
            # total rows for version
            count_sql = "SELECT COUNT(*) FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE version_id = ?"
            cursor.execute(count_sql, version_id)
            result['total_rows'] = cursor.fetchone()[0]
            # paged rows (order by first column for deterministic paging)
            select_sql = (
                "SELECT * FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE version_id = ? ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            )
            cursor.execute(select_sql, version_id, offset, limit)
            for row in cursor.fetchall():
                # pyodbc row -> list
                result['rows'].append([row[i] for i in range(len(row))])
        except Exception as e:
            result['error'] = str(e)
        return result

    def rollback_to_version(self, connection: pyodbc.Connection, base_name: str, version_id: int) -> Dict[str, Any]:
        """Rollback main (and stage if exists) table to data from specified backup version.
        Returns dict with counts and actions."""
        outcome = {'base_name': base_name, 'version_id': version_id, 'main_rows': 0, 'stage_rows': 0, 'status': 'started'}
        if not base_name or not re.match(r'^[A-Za-z0-9_]+$', base_name):
            outcome['status'] = 'invalid_base_name'
            return outcome
        backup_table = f"{base_name}_backup"
        main_exists = self.table_exists(connection, base_name, self.data_schema)
        if not main_exists:
            outcome['status'] = 'main_missing'
            return outcome
        stage_name = f"{base_name}_stage"
        stage_exists = self.table_exists(connection, stage_name, self.data_schema)
        try:
            connection.autocommit = False
            cursor = connection.cursor()
            # Determine intersection columns between backup and main (exclude version_id)
            backup_cols = [c['name'] for c in self.get_table_columns(connection, backup_table, self.backup_schema) if c['name'].lower() != 'version_id']
            main_cols = [c['name'] for c in self.get_table_columns(connection, base_name, self.data_schema)]
            # Exclude metadata columns that may not exist in main
            exclude_meta = {'version_id'}
            common_cols = [c for c in backup_cols if c in main_cols and c.lower() not in exclude_meta]
            if not common_cols:
                raise Exception('No common columns between backup and main tables')
            col_list = ', '.join('[' + c + ']' for c in common_cols)
            # Truncate main
            cursor.execute("TRUNCATE TABLE [" + self.data_schema + "].[" + base_name + "]")
            # Insert from backup version
            insert_sql = (
                "INSERT INTO [" + self.data_schema + "].[" + base_name + "] (" + col_list + ") "
                "SELECT " + col_list + " FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE version_id = ?"
            )
            cursor.execute(insert_sql, version_id)
            outcome['main_rows'] = cursor.rowcount if cursor.rowcount is not None else 0
            # Stage table optional
            if stage_exists:
                # Use same columns intersection for stage
                cursor.execute("TRUNCATE TABLE [" + self.data_schema + "].[" + stage_name + "]")
                cursor.execute(
                    "INSERT INTO [" + self.data_schema + "].[" + stage_name + "] (" + col_list + ") SELECT " + col_list + " FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE version_id = ?",
                    version_id
                )
                outcome['stage_rows'] = cursor.rowcount if cursor.rowcount is not None else 0
            connection.commit()
            outcome['status'] = 'success'
        except Exception as e:
            try:
                connection.rollback()
            except Exception:
                pass
            outcome['status'] = 'error'
            outcome['error'] = str(e)
        finally:
            connection.autocommit = True
        return outcome
    
    def insert_reference_data_cfg_record(self, connection: pyodbc.Connection, table_name: str) -> None:
        """Insert a record into Reference_Data_Cfg table after successful ingestion"""
        cursor = connection.cursor()
        
        try:
            # Get the current database name
            cursor.execute("SELECT DB_NAME()")
            database_name = cursor.fetchone()[0]
            
            # Prepare the record data
            sp_name = f"usp_RefreshReferenceData_{database_name}"
            ref_name = f"ref_{table_name}"
            source_db = database_name
            source_schema = "ref"
            source_table = table_name
            is_enabled = 1
            
            # Check if record already exists and compare all columns
            check_sql = f"""
                SELECT [sp_name], [source_db], [source_schema], [source_table], [is_enabled]
                FROM [{self.staff_database}].[dbo].[Reference_Data_Cfg] 
                WHERE [ref_name] = ?
            """
            cursor.execute(check_sql, ref_name)
            existing_record = cursor.fetchone()
            
            if existing_record is None:
                # Insert new record if it doesn't exist
                insert_sql = f"""
                    INSERT INTO [{self.staff_database}].[dbo].[Reference_Data_Cfg] 
                    ([sp_name], [ref_name], [source_db], [source_schema], [source_table], [is_enabled])
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                cursor.execute(insert_sql, sp_name, ref_name, source_db, source_schema, source_table, is_enabled)
                print(f"Inserted new Reference_Data_Cfg record for {ref_name}")
            else:
                # Compare all columns to see if update is needed
                existing_sp_name = existing_record[0]
                existing_source_db = existing_record[1] 
                existing_source_schema = existing_record[2]
                existing_source_table = existing_record[3]
                existing_is_enabled = existing_record[4]
                
                # Check if any values are different
                needs_update = (
                    existing_sp_name != sp_name or
                    existing_source_db != source_db or
                    existing_source_schema != source_schema or
                    existing_source_table != source_table or
                    existing_is_enabled != is_enabled
                )
                
                if needs_update:
                    # Update existing record with new values
                    update_sql = f"""
                        UPDATE [{self.staff_database}].[dbo].[Reference_Data_Cfg] 
                        SET [sp_name] = ?, [source_db] = ?, [source_schema] = ?, 
                            [source_table] = ?, [is_enabled] = ?
                        WHERE [ref_name] = ?
                    """
                    cursor.execute(update_sql, sp_name, source_db, source_schema, source_table, is_enabled, ref_name)
                    print(f"Updated Reference_Data_Cfg record for {ref_name} (values changed)")
                else:
                    print(f"Reference_Data_Cfg record for {ref_name} unchanged - no update needed")
            
            connection.commit()
            
            # Call post-load stored procedure
            try:
                postload_cursor = connection.cursor()
                postload_cursor.execute(f"EXEC [ref].[{self.postload_sp_name}]")
                connection.commit()
                print(f"Called ref.{self.postload_sp_name} stored procedure")
            except Exception as sp_error:
                # Log warning but don't fail the entire process
                print(f"WARNING: Failed to call ref.{self.postload_sp_name}: {str(sp_error)}")
                # Don't rollback the Reference_Data_Cfg changes if SP fails
            
        except Exception as e:
            connection.rollback()
            error_msg = f"Failed to insert/update Reference_Data_Cfg record for {table_name}: {str(e)}"
            print(f"WARNING: {error_msg}")
            # Don't raise - this shouldn't fail the entire ingestion process
            # Just log the warning and continue