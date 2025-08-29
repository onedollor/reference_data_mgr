"""
Comprehensive tests for utils.database.DatabaseManager to increase coverage from 12% to 60%+
Targeting all major methods and error paths in database.py
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock, call
from datetime import datetime


class TestDatabaseManagerInit:
    """Test DatabaseManager initialization and connection setup"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'test_host',
        'db_name': 'test_db',
        'data_schema': 'test_ref',
        'backup_schema': 'test_bkp',
        'validation_sp_schema': 'test_val',
        'DB_POOL_SIZE': '10',
        'DB_MAX_RETRIES': '5',
        'DB_RETRY_BACKOFF': '1.0'
    })
    @patch('utils.database.pyodbc')
    def test_complete_initialization(self, mock_pyodbc):
        """Test complete DatabaseManager initialization with all env vars"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Verify all properties are set correctly
        assert db_manager.server == 'test_host'
        assert db_manager.database == 'test_db'
        assert db_manager.username == 'test_user'
        assert db_manager.password == 'test_pass'
        assert db_manager.data_schema == 'test_ref'
        assert db_manager.backup_schema == 'test_bkp'
        assert db_manager.validation_sp_schema == 'test_val'
        assert db_manager.pool_size == 10
        assert db_manager.max_retries == 5
        assert db_manager.retry_backoff == 1.0
        assert db_manager.postload_sp_name == 'usp_reference_data_test_db'

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_build_connection_string_variations(self, mock_pyodbc):
        """Test connection string building with various configurations"""
        from utils.database import DatabaseManager
        
        # Test default values
        db_manager = DatabaseManager()
        conn_string = db_manager._build_connection_string()
        
        assert 'DRIVER={ODBC Driver 17 for SQL Server}' in conn_string
        assert 'SERVER=localhost' in conn_string
        assert 'DATABASE=test' in conn_string
        assert 'UID=test_user' in conn_string
        assert 'PWD=test_pass' in conn_string
        assert 'TrustServerCertificate=yes' in conn_string
        assert 'Encrypt=yes' in conn_string

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_odbc_driver': '{Custom ODBC Driver}'
    })
    @patch('utils.database.pyodbc')
    def test_build_connection_string_custom_driver(self, mock_pyodbc):
        """Test connection string with custom ODBC driver"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        conn_string = db_manager._build_connection_string()
        
        assert 'DRIVER={Custom ODBC Driver}' in conn_string

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_odbc_driver': 'Custom Driver Without Braces'
    })
    @patch('utils.database.pyodbc')
    def test_build_connection_string_driver_formatting(self, mock_pyodbc):
        """Test connection string driver formatting"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        conn_string = db_manager._build_connection_string()
        
        assert 'DRIVER={Custom Driver Without Braces}' in conn_string


class TestDatabaseConnectionPooling:
    """Test database connection pooling functionality"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '3'
    })
    @patch('utils.database.pyodbc')
    def test_pooled_connection_management(self, mock_pyodbc):
        """Test pooled connection creation and management"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        
        # Test getting connection when pool is empty
        conn1 = db_manager.get_pooled_connection()
        assert conn1 == mock_conn1
        assert db_manager._in_use == 1
        
        # Test pool stats
        stats = db_manager.get_pool_stats()
        assert stats['pool_size_config'] == 3
        assert stats['idle'] == 0
        assert stats['in_use'] == 1
        assert stats['available_capacity'] == 2
        
        # Test releasing connection back to pool
        db_manager.release_connection(conn1)
        assert db_manager._in_use == 0
        assert len(db_manager._pool) == 1
        
        # Test getting connection from pool
        conn_from_pool = db_manager.get_pooled_connection()
        assert conn_from_pool == mock_conn1
        assert db_manager._in_use == 1

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '1'
    })
    @patch('utils.database.pyodbc')
    def test_release_connection_pool_full(self, mock_pyodbc):
        """Test releasing connection when pool is full"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        
        # Fill pool
        conn1 = db_manager.get_pooled_connection()
        db_manager.release_connection(conn1)
        
        # Get another connection
        conn2 = db_manager.get_pooled_connection()
        
        # Try to release when pool is full - should close connection
        db_manager.release_connection(conn2)
        mock_conn2.close.assert_called_once()

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_release_none_connection(self, mock_pyodbc):
        """Test releasing None connection"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Should handle None gracefully
        db_manager.release_connection(None)
        # No assertions needed - just shouldn't crash

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_close_pool(self, mock_pyodbc):
        """Test closing all connections in pool"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        
        # Add connections to pool
        conn1 = db_manager.get_pooled_connection()
        conn2 = db_manager.get_pooled_connection()
        db_manager.release_connection(conn1)
        db_manager.release_connection(conn2)
        
        # Close pool
        db_manager.close_pool()
        
        # All connections should be closed
        mock_conn1.close.assert_called()
        mock_conn2.close.assert_called()
        assert len(db_manager._pool) == 0

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_close_pool_with_exceptions(self, mock_pyodbc):
        """Test closing pool when connection.close() raises exception"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close failed")
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Add connection to pool
        conn = db_manager.get_pooled_connection()
        db_manager.release_connection(conn)
        
        # Close pool - should handle exception gracefully
        db_manager.close_pool()
        assert len(db_manager._pool) == 0


class TestDatabaseOperations:
    """Test database table operations and schema management"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_test_connection_success(self, mock_pyodbc):
        """Test successful database connection test"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = ["SQL Server 2019", datetime(2024, 12, 25, 10, 30, 0)]
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert result['status'] == 'success'
        assert 'SQL Server 2019' in result['server_version']
        assert result['current_time'] == datetime(2024, 12, 25, 10, 30, 0)
        assert 'schemas' in result
        mock_cursor.execute.assert_called_with("SELECT @@VERSION, GETDATE()")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_test_connection_failure(self, mock_pyodbc):
        """Test database connection test failure"""
        from utils.database import DatabaseManager
        
        mock_pyodbc.connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert result['status'] == 'error'
        assert 'Connection failed' in result['error']
        assert 'traceback' in result

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_ensure_schemas_exist(self, mock_pyodbc):
        """Test ensuring database schemas exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        db_manager.ensure_schemas_exist(mock_conn)
        
        # Should execute schema creation SQL for each unique schema
        expected_calls = 3  # ref, bkp, and validation schemas
        assert mock_cursor.execute.call_count == expected_calls
        
        # Check that schema creation SQL was called
        calls = mock_cursor.execute.call_args_list
        for call_args, call_kwargs in calls:
            sql = call_args[0]
            assert 'CREATE SCHEMA' in sql
            assert 'QUOTENAME' in sql

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_ensure_schemas_exist_failure(self, mock_pyodbc):
        """Test schema creation failure"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Schema creation failed")
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with pytest.raises(Exception, match="Failed to create schema"):
            db_manager.ensure_schemas_exist(mock_conn)

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_table_exists_true(self, mock_pyodbc):
        """Test table existence check - table exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Table exists
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.table_exists(mock_conn, "users", "ref")
        
        assert result == True
        mock_cursor.execute.assert_called_with(
            "\n            SELECT COUNT(*) \n            FROM INFORMATION_SCHEMA.TABLES \n            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?\n        ",
            "ref", "users"
        )

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_table_exists_false(self, mock_pyodbc):
        """Test table existence check - table doesn't exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]  # Table doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.table_exists(mock_conn, "nonexistent", "ref")
        
        assert result == False

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_table_exists_default_schema(self, mock_pyodbc):
        """Test table existence check with default schema"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.table_exists(mock_conn, "users")  # No schema specified
        
        assert result == True
        # Should use default data_schema
        mock_cursor.execute.assert_called_with(
            "\n            SELECT COUNT(*) \n            FROM INFORMATION_SCHEMA.TABLES \n            WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?\n        ",
            "ref", "users"
        )

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_table_columns(self, mock_pyodbc):
        """Test getting table column information"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock fetchall to return column information
        mock_row = MagicMock()
        mock_row.COLUMN_NAME = "id"
        mock_row.DATA_TYPE = "int"
        mock_row.CHARACTER_MAXIMUM_LENGTH = None
        mock_row.NUMERIC_PRECISION = 10
        mock_row.NUMERIC_SCALE = 0
        mock_row.IS_NULLABLE = "NO"
        mock_row.COLUMN_DEFAULT = None
        mock_row.ORDINAL_POSITION = 1
        
        mock_cursor.fetchall.return_value = [mock_row]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        columns = db_manager.get_table_columns(mock_conn, "users", "ref")
        
        assert len(columns) == 1
        col = columns[0]
        assert col['name'] == 'id'
        assert col['data_type'] == 'int'
        assert col['max_length'] is None
        assert col['numeric_precision'] == 10
        assert col['numeric_scale'] == 0
        assert col['nullable'] == False
        assert col['default'] is None
        assert col['position'] == 1

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_create_table_with_metadata(self, mock_pyodbc):
        """Test creating table with metadata columns"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar(255)'}
        ]
        
        db_manager.create_table(mock_conn, "test_table", columns, "ref", True)
        
        # Should execute DROP and CREATE statements
        assert mock_cursor.execute.call_count == 2
        
        calls = mock_cursor.execute.call_args_list
        drop_sql = calls[0][0][0]
        create_sql = calls[1][0][0]
        
        assert "DROP TABLE IF EXISTS [ref].[test_table]" in drop_sql
        assert "CREATE TABLE [ref].[test_table]" in create_sql
        assert "[id] int" in create_sql
        assert "[name] varchar(255)" in create_sql
        assert "[ref_data_loadtime] datetime DEFAULT GETDATE()" in create_sql
        assert "[ref_data_loadtype] varchar(255)" in create_sql

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_create_table_without_metadata(self, mock_pyodbc):
        """Test creating table without metadata columns"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar(255)'}
        ]
        
        db_manager.create_table(mock_conn, "test_table", columns, "ref", False)
        
        calls = mock_cursor.execute.call_args_list
        create_sql = calls[1][0][0]
        
        assert "CREATE TABLE [ref].[test_table]" in create_sql
        assert "[id] int" in create_sql
        assert "[name] varchar(255)" in create_sql
        assert "ref_data_loadtime" not in create_sql
        assert "ref_data_loadtype" not in create_sql

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_drop_table_if_exists_table_exists(self, mock_pyodbc):
        """Test dropping table that exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Table exists
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.drop_table_if_exists(mock_conn, "test_table", "ref")
        
        assert result == True
        assert mock_cursor.execute.call_count == 2
        
        calls = mock_cursor.execute.call_args_list
        # First call checks existence, second call drops table
        drop_sql = calls[1][0][0]
        assert "DROP TABLE [ref].[test_table]" in drop_sql
        mock_print.assert_called_with("INFO: Dropped table [ref].[test_table]")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_drop_table_if_exists_table_not_exists(self, mock_pyodbc):
        """Test dropping table that doesn't exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]  # Table doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.drop_table_if_exists(mock_conn, "test_table", "ref")
        
        assert result == False
        # Should only execute existence check, not drop
        assert mock_cursor.execute.call_count == 1
        mock_print.assert_called_with("INFO: Table [ref].[test_table] does not exist, no drop needed")


class TestDatabaseBackupOperations:
    """Test database backup table operations"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_create_backup_table_new(self, mock_pyodbc):
        """Test creating new backup table"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]  # Backup table doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255}
        ]
        
        with patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml})" if ml else dt):
            with patch('builtins.print'):
                db_manager.create_backup_table(mock_conn, "test_table", columns)
        
        # Should execute existence check and CREATE TABLE
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        create_sql = calls[1][0][0]
        
        assert "CREATE TABLE [bkp].[test_table_backup]" in create_sql
        assert "[id] int" in create_sql
        assert "[name] varchar(255)" in create_sql
        assert "[ref_data_loadtime] datetime" in create_sql
        assert "[ref_data_loadtype] varchar(255)" in create_sql
        assert "[ref_data_version_id] int NOT NULL" in create_sql

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_create_backup_table_exists_compatible(self, mock_pyodbc):
        """Test creating backup table when compatible one exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Backup table exists
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255}
        ]
        
        with patch.object(db_manager, '_backup_schema_matches', return_value=True):
            with patch('builtins.print') as mock_print:
                db_manager.create_backup_table(mock_conn, "test_table", columns)
        
        # Should only check existence and schema compatibility
        assert mock_cursor.execute.call_count == 1
        mock_print.assert_called_with("INFO: Backup table schema is compatible")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_backup_schema_matches_true(self, mock_pyodbc):
        """Test backup schema matching returns True"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        db_manager = DatabaseManager()
        
        # Mock get_table_columns to return matching columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None, 'numeric_precision': None, 'numeric_scale': None},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255, 'numeric_precision': None, 'numeric_scale': None},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'},  # Should be filtered out
        ]
        
        expected_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml})" if ml else dt):
                with patch('builtins.print'):
                    result = db_manager._backup_schema_matches(mock_conn, "test_table_backup", expected_columns)
        
        assert result == True

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_backup_schema_matches_false(self, mock_pyodbc):
        """Test backup schema matching returns False"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        db_manager = DatabaseManager()
        
        # Mock get_table_columns to return different columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'email', 'data_type': 'varchar', 'max_length': 255}  # Different column
        ]
        
        expected_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255}  # Different column
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml})" if ml else dt):
                with patch('builtins.print'):
                    result = db_manager._backup_schema_matches(mock_conn, "test_table_backup", expected_columns)
        
        assert result == False


class TestDatabaseUtilityMethods:
    """Test utility methods in DatabaseManager"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_normalize_data_type_varchar(self, mock_pyodbc):
        """Test data type normalization for varchar"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test varchar with max length
        result = db_manager._normalize_data_type('varchar', 255, None, None)
        assert result == 'varchar(255)'
        
        # Test varchar without max length
        result = db_manager._normalize_data_type('varchar', None, None, None)
        assert result == 'varchar'

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_normalize_data_type_decimal(self, mock_pyodbc):
        """Test data type normalization for decimal"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test decimal with precision and scale
        result = db_manager._normalize_data_type('decimal', None, 10, 2)
        assert result == 'decimal(10,2)'
        
        # Test decimal with only precision
        result = db_manager._normalize_data_type('decimal', None, 10, None)
        assert result == 'decimal(10)'
        
        # Test decimal without precision/scale
        result = db_manager._normalize_data_type('decimal', None, None, None)
        assert result == 'decimal'

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_normalize_data_type_int(self, mock_pyodbc):
        """Test data type normalization for int"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        result = db_manager._normalize_data_type('int', None, None, None)
        assert result == 'int'

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_timestamp_suffix(self, mock_pyodbc):
        """Test timestamp suffix generation"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        with patch('utils.database.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2024, 12, 25, 10, 30, 45)
            
            result = db_manager._get_timestamp_suffix()
            assert result == '20241225_103045'


class TestDatabaseErrorHandling:
    """Test error handling in database operations"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_backup_schema_matches_exception(self, mock_pyodbc):
        """Test backup schema matching with exception"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_table_columns', side_effect=Exception("Database error")):
            with patch('builtins.print'):
                result = db_manager._backup_schema_matches(mock_conn, "test_table_backup", [])
        
        # Should return False on exception
        assert result == False

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    @patch('utils.database.time.sleep')
    def test_pooled_connection_recursive_call(self, mock_sleep, mock_pyodbc):
        """Test pooled connection recursive waiting"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        db_manager.pool_size = 1
        
        # Simulate pool being full by setting _in_use to pool_size
        db_manager._in_use = 1
        
        # Mock get_connection to return a connection after one sleep cycle
        original_get_connection = db_manager.get_connection
        call_count = 0
        
        def mock_get_connection():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call - simulate pool being full, trigger recursive call
                db_manager._in_use = 0  # Reset for second call
                return original_get_connection()
            else:
                return mock_conn
        
        with patch.object(db_manager, 'get_connection', side_effect=mock_get_connection):
            result = db_manager.get_pooled_connection()
        
        assert result == mock_conn
        mock_sleep.assert_called_with(0.05)