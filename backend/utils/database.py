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
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
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
                'numeric_precision': row.NUMERIC_PRECISION,
                'numeric_scale': row.NUMERIC_SCALE,
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
            column_defs.append("[ref_data_loadtype] varchar(255)")
        
        # Create the table
        create_sql = f"""
            CREATE TABLE [{schema}].[{table_name}] (
                {', '.join(column_defs)}
            )
        """
        
        cursor.execute(create_sql)
    
    def drop_table_if_exists(self, connection: pyodbc.Connection, table_name: str, schema: str = None) -> bool:
        """Drop table if it exists. Returns True if table was dropped, False if it didn't exist."""
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """, schema, table_name)
        
        table_exists = cursor.fetchone()[0] > 0
        
        if table_exists:
            # Drop the table
            drop_sql = f"DROP TABLE [{schema}].[{table_name}]"
            cursor.execute(drop_sql)
            print(f"INFO: Dropped table [{schema}].[{table_name}]")
            return True
        else:
            print(f"INFO: Table [{schema}].[{table_name}] does not exist, no drop needed")
            return False
    
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
            # Validate schema compatibility and sync if needed
            if self._backup_schema_matches(connection, backup_table_name, columns):
                # Schema matches, backup table is compatible
                print(f"INFO: Backup table schema is compatible")
                return
            else:
                # Schema doesn't match, attempt to sync backup table schema
                print(f"INFO: Backup table schema mismatch detected for {backup_table_name}")
                sync_result = self._sync_backup_table_schema(connection, backup_table_name, columns)
                
                if sync_result['success']:
                    print(f"INFO: Successfully synced backup table schema: {sync_result['summary']}")
                    return
                else:
                    # If sync fails, fallback to rename and recreate as last resort
                    print(f"WARNING: Could not sync backup table schema: {sync_result['error']}")
                    print(f"INFO: Falling back to rename and recreate strategy as last resort")
                    
                    timestamp_suffix = self._get_timestamp_suffix()
                    old_backup_name = f"{backup_table_name}_{timestamp_suffix}"
                    
                    # Rename existing backup table to preserve historical data
                    rename_sql = (
                        "EXEC sp_rename '[" + self.backup_schema + "].[" + backup_table_name + "]', '" + old_backup_name + "'"
                    )
                    cursor.execute(rename_sql)
                    print(f"INFO: Preserved historical backup data by renaming to {old_backup_name}")
        
        # Build column definitions (same as main table)
        column_defs = []
        for col in columns:
            # Use normalized data type to include proper length specifications
            normalized_type = self._normalize_data_type(col['data_type'], col.get('max_length'), col.get('numeric_precision'), col.get('numeric_scale'))
            col_def = f"[{col['name']}] {normalized_type}"
            column_defs.append(col_def)
            print(f"DEBUG: Backup table column: {col['name']} -> {normalized_type}")
        
        # Add backup-specific columns
        column_defs.extend([
            "[ref_data_loadtime] datetime",
            "[ref_data_loadtype] varchar(255)",
            "[ref_data_version_id] int NOT NULL"
        ])
        
        # Create the backup table with proper schema quoting
        create_sql = (
            "CREATE TABLE [" + self.backup_schema + "].[" + backup_table_name + "] (" +
            ', '.join(column_defs) + ")"
        )
        cursor.execute(create_sql)
    
    def _backup_schema_matches(self, connection: pyodbc.Connection, backup_table_name: str, expected_columns: List[Dict[str, str]]) -> bool:
        """Check if existing backup table schema matches expected columns including data types"""
        try:
            print(f"DEBUG: Validating backup table schema for {backup_table_name}")
            
            # Get existing backup table columns (excluding metadata columns)
            existing_columns = self.get_table_columns(connection, backup_table_name, self.backup_schema)
            
            # Filter out backup-specific metadata columns
            data_columns = [col for col in existing_columns 
                           if col['name'] not in ['ref_data_loadtime', 'ref_data_loadtype', 'ref_data_version_id']]
            
            print(f"DEBUG: Found {len(data_columns)} data columns in existing backup table")
            print(f"DEBUG: Expected {len(expected_columns)} columns from main table")
            
            # Create comparison sets (normalize data types)
            expected_set = set()
            for col in expected_columns:
                col_name = col['name'].lower()
                col_type = self._normalize_data_type(col['data_type'], col.get('max_length'), col.get('numeric_precision'), col.get('numeric_scale'))
                expected_set.add((col_name, col_type))
                print(f"DEBUG: Expected column: {col_name} -> {col_type}")
            
            existing_set = set()
            for col in data_columns:
                col_name = col['name'].lower()
                col_type = self._normalize_data_type(col['data_type'], col.get('max_length'), col.get('numeric_precision'), col.get('numeric_scale'))
                existing_set.add((col_name, col_type))
                print(f"DEBUG: Existing column: {col_name} -> {col_type}")
            
            # Check for differences
            missing_in_backup = expected_set - existing_set
            extra_in_backup = existing_set - expected_set
            
            if missing_in_backup:
                print(f"DEBUG: Missing columns in backup table: {missing_in_backup}")
            if extra_in_backup:
                print(f"DEBUG: Extra columns in backup table: {extra_in_backup}")
            
            schema_matches = expected_set == existing_set
            print(f"DEBUG: Backup table schema matches: {schema_matches}")
            
            return schema_matches
            
        except Exception as e:
            # If we can't validate schema, assume it doesn't match to be safe
            print(f"WARNING: Could not validate backup table schema: {str(e)}")
            return False
    
    def _sync_backup_table_schema(self, connection: pyodbc.Connection, backup_table_name: str, expected_columns: List[Dict[str, str]]) -> dict:
        """Attempt to sync backup table schema with expected columns by adding/modifying columns including data types"""
        print(f"DEBUG: Starting backup table schema sync for {backup_table_name}")
        try:
            cursor = connection.cursor()
            
            # Get existing backup table columns
            existing_columns = self.get_table_columns(connection, backup_table_name, self.backup_schema)
            
            # Filter out backup-specific metadata columns for comparison
            data_columns = {col['name'].lower(): col for col in existing_columns 
                           if col['name'] not in ['ref_data_loadtime', 'ref_data_loadtype', 'ref_data_version_id']}
            
            # Track changes made
            changes = {
                'added': [],
                'modified': [],
                'errors': []
            }
            
            # Process each expected column
            for expected_col in expected_columns:
                col_name = expected_col['name']
                col_name_lower = col_name.lower()
                expected_type = expected_col['data_type']
                
                if col_name_lower not in data_columns:
                    # Column doesn't exist, add it
                    try:
                        expected_type_norm = self._normalize_data_type(expected_col['data_type'], expected_col.get('max_length'), expected_col.get('numeric_precision'), expected_col.get('numeric_scale'))
                        alter_sql = f"ALTER TABLE [{self.backup_schema}].[{backup_table_name}] ADD [{col_name}] {expected_type_norm}"
                        cursor.execute(alter_sql)
                        changes['added'].append({'column': col_name, 'type': expected_type_norm})
                        print(f"INFO: Added column [{col_name}] {expected_type_norm} to backup table")
                    except Exception as e:
                        error_msg = f"Failed to add column {col_name}: {str(e)}"
                        changes['errors'].append(error_msg)
                        print(f"WARNING: {error_msg}")
                else:
                    # Column exists, check if type needs modification
                    existing_col = data_columns[col_name_lower]
                    existing_type = self._normalize_data_type(existing_col['data_type'], existing_col.get('max_length'), existing_col.get('numeric_precision'), existing_col.get('numeric_scale'))
                    expected_type_norm = self._normalize_data_type(expected_col['data_type'], expected_col.get('max_length'), expected_col.get('numeric_precision'), expected_col.get('numeric_scale'))
                    
                    if existing_type != expected_type_norm:
                        # Check if this is a safe modification (e.g., widening varchar)
                        if self._is_safe_column_modification(existing_col, expected_col):
                            try:
                                alter_sql = f"ALTER TABLE [{self.backup_schema}].[{backup_table_name}] ALTER COLUMN [{col_name}] {expected_type_norm}"
                                cursor.execute(alter_sql)
                                changes['modified'].append({
                                    'column': col_name, 
                                    'from': existing_type, 
                                    'to': expected_type_norm
                                })
                                print(f"INFO: Modified column [{col_name}] from {existing_type} to {expected_type_norm}")
                            except Exception as e:
                                error_msg = f"Failed to modify column {col_name}: {str(e)}"
                                changes['errors'].append(error_msg)
                                print(f"WARNING: {error_msg}")
                        else:
                            # Unsafe modifications should cause sync to fail, triggering recreation
                            error_msg = f"Incompatible type change for column {col_name}: {existing_type} -> {expected_type_norm}"
                            changes['errors'].append(error_msg)
                            print(f"ERROR: {error_msg} - this will trigger backup table recreation")
            
            # Check for extra columns in backup table that aren't in expected columns
            expected_col_names = {col['name'].lower() for col in expected_columns}
            extra_columns = [col_name for col_name in data_columns.keys() 
                            if col_name not in expected_col_names]
            
            if extra_columns:
                print(f"INFO: Backup table has extra columns that will be preserved: {extra_columns}")
            
            # Commit changes if no errors
            if changes['errors']:
                connection.rollback()
                return {
                    'success': False,
                    'error': f"Schema sync failed with {len(changes['errors'])} errors: {changes['errors'][:3]}",
                    'changes': changes
                }
            else:
                connection.commit()
                summary_parts = []
                if changes['added']:
                    summary_parts.append(f"added {len(changes['added'])} columns")
                if changes['modified']:
                    summary_parts.append(f"modified {len(changes['modified'])} columns")
                
                summary = ", ".join(summary_parts) if summary_parts else "no changes needed"
                
                return {
                    'success': True,
                    'summary': summary,
                    'changes': changes
                }
                
        except Exception as e:
            connection.rollback()
            return {
                'success': False,
                'error': f"Unexpected error during schema sync: {str(e)}",
                'changes': {}
            }
    
    def _is_safe_column_modification(self, existing_col: dict, expected_col: dict) -> bool:
        """Check if a column modification is safe (e.g., widening varchar, compatible type changes)"""
        # Use normalized types for comparison
        existing_type = self._normalize_data_type(existing_col['data_type'], existing_col.get('max_length'), existing_col.get('numeric_precision'), existing_col.get('numeric_scale')).lower()
        expected_type = self._normalize_data_type(expected_col['data_type'], expected_col.get('max_length'), expected_col.get('numeric_precision'), expected_col.get('numeric_scale')).lower()
        
        print(f"DEBUG: Checking safety of column modification: {existing_type} -> {expected_type}")
        
        # Extract base types
        if 'varchar' in existing_type and 'varchar' in expected_type:
            # Check if we're widening varchar
            import re
            existing_match = re.search(r'varchar\((\d+)\)', existing_type)
            expected_match = re.search(r'varchar\((\d+)\)', expected_type)
            
            if existing_match and expected_match:
                existing_length = int(existing_match.group(1))
                expected_length = int(expected_match.group(1))
                return expected_length >= existing_length
            
            # If one is varchar(max), that's generally safe
            if 'max' in expected_type:
                return True
        
        # Extract base types
        existing_base = existing_type.split('(')[0]
        expected_base = expected_type.split('(')[0]
        
        # Explicitly reject incompatible type changes
        incompatible_changes = {
            ('varchar', 'decimal'), ('varchar', 'numeric'), ('varchar', 'int'), ('varchar', 'bigint'),
            ('decimal', 'varchar'), ('numeric', 'varchar'), ('int', 'varchar'), ('bigint', 'varchar'),
            ('decimal', 'int'), ('int', 'decimal'), ('numeric', 'int'), ('int', 'numeric')
        }
        
        # Check for explicitly incompatible changes
        if (existing_base, expected_base) in incompatible_changes:
            print(f"DEBUG: Rejecting incompatible type change: {existing_base} -> {expected_base}")
            return False
        
        # Safe type conversions (same base type or compatible widening)
        safe_conversions = {
            'int': ['bigint'],
            'smallint': ['int', 'bigint'],
            'tinyint': ['smallint', 'int', 'bigint'],
            'float': ['real'],
            'datetime': ['datetime2'],
            'char': ['varchar', 'nvarchar'],
            'varchar': ['nvarchar']
        }
        
        if existing_base == expected_base:
            return True
        
        if existing_base in safe_conversions:
            return expected_base in safe_conversions[existing_base]
        
        print(f"DEBUG: Rejecting unsafe type change: {existing_base} -> {expected_base}")
        return False
    
    def _normalize_data_type(self, data_type: str, max_length: int = None, numeric_precision: int = None, numeric_scale: int = None) -> str:
        """Normalize data type for comparison (handle varchar, decimal, and other type variations)"""
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
        
        # Handle nvarchar with lengths
        elif data_type.startswith('nvarchar'):
            if max_length == -1:
                return 'nvarchar(max)'
            elif max_length is not None:
                return f'nvarchar({max_length})'
            else:
                # Extract length from data_type string if present
                import re
                match = re.search(r'nvarchar\((\d+|max)\)', data_type, re.IGNORECASE)
                if match:
                    return f'nvarchar({match.group(1).lower()})'
                else:
                    return 'nvarchar(4000)'  # Default fallback
        
        # Handle decimal/numeric with precision and scale
        elif data_type.startswith(('decimal', 'numeric')):
            # Use provided precision and scale from INFORMATION_SCHEMA
            if numeric_precision is not None and numeric_scale is not None:
                return f'decimal({numeric_precision},{numeric_scale})'
            else:
                # Extract precision and scale from string if present
                import re
                match = re.search(r'(decimal|numeric)\((\d+),(\d+)\)', data_type, re.IGNORECASE)
                if match:
                    type_name = match.group(1).lower()
                    precision = match.group(2)
                    scale = match.group(3)
                    return f'{type_name}({precision},{scale})'
                else:
                    # If no precision/scale specified, use default
                    return 'decimal(18,0)'
        
        # Handle char with lengths  
        elif data_type.startswith('char') and not data_type.startswith('varchar'):
            if max_length is not None:
                return f'char({max_length})'
            else:
                import re
                match = re.search(r'char\((\d+)\)', data_type, re.IGNORECASE)
                if match:
                    return f'char({match.group(1)})'
                else:
                    return 'char(1)'  # Default fallback
        
        # For other types, return as-is (int, bigint, datetime, etc.)
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
            {"name": "ref_data_loadtype", "data_type": "varchar(255)", "default": ""},
            {"name": "ref_data_version_id", "data_type": "int NOT NULL", "default": ""}
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
        
        print(f"DEBUG: Starting backup - source_table: {source_table}, backup_table: {backup_table}")
        try:
            # Get the next version ID - use dynamic SQL with proper quoting
            version_sql = "SELECT COALESCE(MAX(ref_data_version_id), 0) + 1 FROM [" + self.backup_schema + "].[" + backup_table + "_backup]"
            cursor.execute(version_sql)
            next_version = cursor.fetchone()[0]
            
            # Get column lists for explicit backup (avoid SELECT *)
            source_columns = self.get_table_columns(connection, source_table, self.data_schema)
            backup_columns = self.get_table_columns(connection, backup_table + "_backup", self.backup_schema)
            
            # Filter source columns to exclude metadata columns (backup table has different metadata structure)
            source_data_columns = [col for col in source_columns if col['name'].lower() not in ['ref_data_loadtime', 'ref_data_loadtype']]
            
            # Build column lists for INSERT and SELECT
            insert_columns = []
            select_columns = []
            
            # Add matching data columns
            for backup_col in backup_columns:
                backup_col_name = backup_col['name']
                backup_col_lower = backup_col_name.lower()
                
                # Skip backup-specific metadata columns for now
                if backup_col_lower in ['ref_data_loadtime', 'ref_data_loadtype', 'ref_data_version_id']:
                    continue
                
                # Find matching source column
                matching_source_col = next((col for col in source_data_columns if col['name'].lower() == backup_col_lower), None)
                if matching_source_col:
                    insert_columns.append(f"[{backup_col_name}]")
                    select_columns.append(f"[{matching_source_col['name']}]")
            
            # Add backup metadata columns
            insert_columns.extend(["[ref_data_loadtime]", "[ref_data_loadtype]", "[ref_data_version_id]"])
            select_columns.extend(["GETDATE()", "'backup'", "?"])
            
            # Build explicit column backup SQL
            insert_column_list = ", ".join(insert_columns)
            select_column_list = ", ".join(select_columns)
            
            backup_sql = (
                f"INSERT INTO [{self.backup_schema}].[{backup_table}_backup] ({insert_column_list}) "
                f"SELECT {select_column_list} FROM [{self.data_schema}].[{source_table}]"
            )
            cursor.execute(backup_sql, next_version)
            connection.commit()
            backup_count = cursor.rowcount
            print(f"DEBUG: Backup completed successfully - {backup_count} rows backed up to version {next_version}")
            return backup_count
                    
        except Exception as e:
            connection.rollback()
            # If backup fails due to schema issues, log warning and continue
            error_msg = str(e)
            if ("Column name or number" in error_msg and "does not match" in error_msg) or \
               ("String or binary data would be truncated" in error_msg) or \
               ("42000" in error_msg and "truncated" in error_msg.lower()):
                print(f"DEBUG: Backup skipped due to schema issue - source_table: {source_table}")
                print(f"WARNING: Skipping backup for {source_table} due to schema compatibility issue: {error_msg}")
                print(f"INFO: This typically occurs when backup table schema needs updating")
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
        """Ensure metadata columns (ref_data_loadtime, ref_data_loadtype) exist in the table"""
        if schema is None:
            schema = self.data_schema
        
        cursor = connection.cursor()
        existing_cols_list = self.get_table_columns(connection, table_name, schema)
        existing_cols = {c['name'].lower(): c for c in existing_cols_list}
        actions = {"added": [], "skipped": []}
        
        # Define required metadata columns
        metadata_columns = [
            {"name": "ref_data_loadtime", "data_type": "datetime", "default": "DEFAULT GETDATE()"},
            {"name": "ref_data_loadtype", "data_type": "varchar(255)", "default": ""}
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
    
    def sync_main_table_columns(self, connection: pyodbc.Connection, table_name: str, file_columns: List[Dict[str, str]], schema: str = None) -> Dict[str, Any]:
        """Safely synchronize main table columns with input file columns.
        ONLY ADDS missing columns - NEVER modifies existing column data types.
        This preserves data integrity and existing table structure.
        
        Args:
            connection: Database connection
            table_name: Name of the main table
            file_columns: List of columns from input file [{'name': ..., 'data_type': ...}]
            schema: Table schema (defaults to data_schema)
            
        Returns:
            Dict with 'added', 'skipped', 'mismatched' lists
        """
        if schema is None:
            schema = self.data_schema
            
        cursor = connection.cursor()
        
        # Get existing table columns (excluding metadata columns for comparison)
        existing_cols_list = self.get_table_columns(connection, table_name, schema)
        existing_cols = {c['name'].lower(): c for c in existing_cols_list 
                        if c['name'].lower() not in ['ref_data_loadtime', 'ref_data_loadtype']}
        
        actions = {
            "added": [],           # Columns successfully added
            "skipped": [],         # Columns that already exist (no change)
            "mismatched": []       # Columns that exist but with different data types (no change)
        }
        
        print(f"INFO: Synchronizing main table [{schema}].[{table_name}] columns with input file")
        existing_cols_display = [f"{c['name']}({c['data_type']})" for c in existing_cols_list if c['name'].lower() not in ['ref_data_loadtime', 'ref_data_loadtype']]
        file_cols_display = [f"{c['name']}({c['data_type']})" for c in file_columns]
        print(f"INFO: Existing table columns: {existing_cols_display}")
        print(f"INFO: Input file columns: {file_cols_display}")
        
        for file_col in file_columns:
            col_name = file_col['name']
            col_name_lower = col_name.lower()
            file_data_type = file_col['data_type']
            
            if col_name_lower not in existing_cols:
                # Column doesn't exist in table - ADD it
                try:
                    add_col_sql = f"ALTER TABLE [{schema}].[{table_name}] ADD [{col_name}] {file_data_type}"
                    cursor.execute(add_col_sql)
                    actions["added"].append({
                        "column": col_name, 
                        "data_type": file_data_type
                    })
                    print(f"INFO: Added column [{col_name}] {file_data_type} to main table")
                except Exception as e:
                    print(f"WARNING: Failed to add column [{col_name}]: {str(e)}")
                    # Don't fail the entire process for one column addition failure
                    continue
            else:
                # Column exists - check if types match
                existing_col = existing_cols[col_name_lower]
                existing_type = existing_col['data_type']
                existing_max_length = existing_col.get('max_length')
                
                # Normalize types for comparison
                existing_type_normalized = self._normalize_data_type(existing_type, existing_max_length)
                file_type_normalized = self._normalize_data_type(file_data_type)
                
                if existing_type_normalized == file_type_normalized:
                    # Types match - no action needed
                    actions["skipped"].append({
                        "column": col_name,
                        "reason": "column exists with matching type",
                        "existing_type": existing_type_normalized
                    })
                else:
                    # Types don't match - PRESERVE existing type, don't modify
                    actions["mismatched"].append({
                        "column": col_name,
                        "existing_type": existing_type_normalized,
                        "file_type": file_type_normalized,
                        "action": "preserved existing type (no modification)"
                    })
                    print(f"WARNING: Column [{col_name}] type mismatch - table has {existing_type_normalized}, file has {file_type_normalized}. Preserving table type.")
        
        # Check for extra columns in table that aren't in file
        file_col_names = {col['name'].lower() for col in file_columns}
        extra_table_cols = [col_name for col_name in existing_cols.keys() 
                           if col_name not in file_col_names]
        
        if extra_table_cols:
            print(f"INFO: Table has {len(extra_table_cols)} extra columns not in input file (preserved): {list(extra_table_cols)}")
        
        # Commit changes
        connection.commit()
        
        # Summary reporting
        summary_parts = []
        if actions['added']:
            summary_parts.append(f"added {len(actions['added'])} columns")
        if actions['mismatched']:
            summary_parts.append(f"preserved {len(actions['mismatched'])} mismatched columns")
        if actions['skipped']:
            summary_parts.append(f"skipped {len(actions['skipped'])} existing columns")
        
        summary = ", ".join(summary_parts) if summary_parts else "no changes needed"
        print(f"INFO: Main table column sync completed: {summary}")
        
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
        Determine the ref_data_loadtype value based on existing data and current load mode.
        Rules:
        - If override_load_type provided: Use override ('F' or 'A')
        - First time ingest: Use current load mode ('F' for full, 'A' for append)
        - Subsequent ingests: Check existing distinct ref_data_loadtype values
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
            
            # Get distinct ref_data_loadtype values from existing data
            query = f"SELECT DISTINCT [ref_data_loadtype] FROM [{self.data_schema}].[{table_name}] WHERE [ref_data_loadtype] IS NOT NULL"
            cursor.execute(query)
            existing_types = set()
            for row in cursor.fetchall():
                if row[0]:  # Not NULL
                    existing_types.add(row[0].strip().upper())
            
            # Apply rules
            if not existing_types:
                # No existing ref_data_loadtype data - use current load mode
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
                    "SELECT COUNT(DISTINCT ref_data_version_id), MAX(ref_data_version_id) FROM [" + self.backup_schema + "].[" + backup_table + "]"
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
        """Return list of ref_data_version_ids for a given backup base name (descending)."""
        if not base_name or not re.match(r'^[A-Za-z0-9_]+$', base_name):
            return []
        backup_table = f"{base_name}_backup"
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT DISTINCT ref_data_version_id FROM [" + self.backup_schema + "].[" + backup_table + "] ORDER BY ref_data_version_id DESC"
            )
            return [row[0] for row in cursor.fetchall() if row[0] is not None]
        except Exception:
            return []

    def get_backup_version_rows(self, connection: pyodbc.Connection, base_name: str, ref_data_version_id: int, limit: int = 50, offset: int = 0) -> Dict[str, Any]:
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
            count_sql = "SELECT COUNT(*) FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE ref_data_version_id = ?"
            cursor.execute(count_sql, ref_data_version_id)
            result['total_rows'] = cursor.fetchone()[0]
            # paged rows (order by first column for deterministic paging)
            select_sql = (
                "SELECT * FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE ref_data_version_id = ? ORDER BY 1 OFFSET ? ROWS FETCH NEXT ? ROWS ONLY"
            )
            cursor.execute(select_sql, ref_data_version_id, offset, limit)
            for row in cursor.fetchall():
                # pyodbc row -> list
                result['rows'].append([row[i] for i in range(len(row))])
        except Exception as e:
            result['error'] = str(e)
        return result

    def rollback_to_version(self, connection: pyodbc.Connection, base_name: str, ref_data_version_id: int) -> Dict[str, Any]:
        """Rollback main (and stage if exists) table to data from specified backup version.
        Returns dict with counts and actions."""
        outcome = {'base_name': base_name, 'ref_data_version_id': ref_data_version_id, 'main_rows': 0, 'stage_rows': 0, 'status': 'started'}
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
            # Determine intersection columns between backup and main (exclude ref_data_version_id)
            backup_cols = [c['name'] for c in self.get_table_columns(connection, backup_table, self.backup_schema) if c['name'].lower() != 'ref_data_version_id']
            main_cols = [c['name'] for c in self.get_table_columns(connection, base_name, self.data_schema)]
            # Exclude metadata columns that may not exist in main
            exclude_meta = {'ref_data_version_id'}
            common_cols = [c for c in backup_cols if c in main_cols and c.lower() not in exclude_meta]
            if not common_cols:
                raise Exception('No common columns between backup and main tables')
            col_list = ', '.join('[' + c + ']' for c in common_cols)
            # Truncate main
            cursor.execute("TRUNCATE TABLE [" + self.data_schema + "].[" + base_name + "]")
            # Insert from backup version
            insert_sql = (
                "INSERT INTO [" + self.data_schema + "].[" + base_name + "] (" + col_list + ") "
                "SELECT " + col_list + " FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE ref_data_version_id = ?"
            )
            cursor.execute(insert_sql, ref_data_version_id)
            outcome['main_rows'] = cursor.rowcount if cursor.rowcount is not None else 0
            # Stage table optional
            if stage_exists:
                # Use same columns intersection for stage
                cursor.execute("TRUNCATE TABLE [" + self.data_schema + "].[" + stage_name + "]")
                cursor.execute(
                    "INSERT INTO [" + self.data_schema + "].[" + stage_name + "] (" + col_list + ") SELECT " + col_list + " FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE ref_data_version_id = ?",
                    ref_data_version_id
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