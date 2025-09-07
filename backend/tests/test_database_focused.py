"""
Focused test coverage for utils/database.py
Comprehensive tests to significantly improve coverage from 12% to much higher
"""

import pytest
import os
import pyodbc
import threading
import time
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime


class TestDatabaseManagerInit:
    """Test DatabaseManager initialization and configuration"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'testserver',
        'db_name': 'testdb',
        'data_schema': 'ref',
        'backup_schema': 'bkp',
        'validation_sp_schema': 'ref'
    })
    def test_database_manager_init_success(self):
        """Test successful DatabaseManager initialization"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        assert db_manager.server == 'testserver'
        assert db_manager.database == 'testdb'
        assert db_manager.username == 'test_user'
        assert db_manager.password == 'test_pass'
        assert db_manager.data_schema == 'ref'
        assert db_manager.backup_schema == 'bkp'
        assert db_manager.postload_sp_name == 'usp_reference_data_testdb'
        assert db_manager.pool_size == 5
        assert db_manager.max_retries == 3
        assert db_manager.retry_backoff == 0.5

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '10',
        'DB_MAX_RETRIES': '5',
        'DB_RETRY_BACKOFF': '1.0'
    })
    def test_database_manager_init_custom_settings(self):
        """Test DatabaseManager initialization with custom settings"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        assert db_manager.pool_size == 10
        assert db_manager.max_retries == 5
        assert db_manager.retry_backoff == 1.0

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': 'invalid'
    })
    def test_database_manager_init_invalid_pool_size(self):
        """Test DatabaseManager initialization with invalid pool size"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Should default to 5 when invalid
        assert db_manager.pool_size == 5

    @patch.dict('os.environ', {
        'db_password': 'test_pass'
    }, clear=True)
    def test_database_manager_init_missing_username(self):
        """Test DatabaseManager initialization with missing username"""
        from utils.database import DatabaseManager
        
        with pytest.raises(ValueError, match="Database username.*required"):
            DatabaseManager()

    @patch.dict('os.environ', {
        'db_user': 'test_user'
    }, clear=True)
    def test_database_manager_init_missing_password(self):
        """Test DatabaseManager initialization with missing password"""
        from utils.database import DatabaseManager
        
        with pytest.raises(ValueError, match="Database password.*required"):
            DatabaseManager()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_build_connection_string(self):
        """Test connection string building"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        conn_str = db_manager._build_connection_string()
        
        assert 'test_user' in conn_str
        assert 'test_pass' in conn_str
        assert 'localhost' in conn_str
        assert 'test' in conn_str
        assert 'ODBC Driver 17 for SQL Server' in conn_str
        assert 'TrustServerCertificate=yes' in conn_str

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_odbc_driver': 'Custom Driver'
    })
    def test_build_connection_string_custom_driver(self):
        """Test connection string building with custom driver"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        conn_str = db_manager._build_connection_string()
        
        assert '{Custom Driver}' in conn_str

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_odbc_driver': '{Pre-braced Driver}'
    })
    def test_build_connection_string_pre_braced_driver(self):
        """Test connection string building with pre-braced driver"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        conn_str = db_manager._build_connection_string()
        
        assert '{Pre-braced Driver}' in conn_str
        # Ensure we don't double-brace
        assert '{{Pre-braced Driver}}' not in conn_str


class TestDatabaseConnections:
    """Test database connection management"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_connection_success(self, mock_pyodbc):
        """Test successful database connection"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        connection = db_manager.get_connection()
        
        assert connection == mock_conn
        assert connection.autocommit is True
        mock_pyodbc.connect.assert_called_once()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_connection_retry_success(self, mock_pyodbc):
        """Test database connection with retry success"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        # Fail first attempt, succeed on second
        mock_pyodbc.connect.side_effect = [Exception("Connection failed"), mock_conn]
        
        db_manager = DatabaseManager()
        
        with patch('utils.database.time.sleep') as mock_sleep:
            connection = db_manager.get_connection()
        
        assert connection == mock_conn
        assert mock_pyodbc.connect.call_count == 2
        mock_sleep.assert_called_once()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_MAX_RETRIES': '2'
    })
    @patch('utils.database.pyodbc')
    def test_get_connection_max_retries_exceeded(self, mock_pyodbc):
        """Test database connection when max retries exceeded"""
        from utils.database import DatabaseManager
        
        mock_pyodbc.connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager()
        
        with pytest.raises(Exception, match="Database connection failed"):
            db_manager.get_connection()
        
        assert mock_pyodbc.connect.call_count == 2  # max_retries

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '2'
    })
    @patch('utils.database.pyodbc')
    def test_get_pooled_connection_new(self, mock_pyodbc):
        """Test getting pooled connection when pool is empty"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        connection = db_manager.get_pooled_connection()
        
        assert connection == mock_conn
        assert db_manager._in_use == 1
        mock_pyodbc.connect.assert_called_once()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '1'
    })
    @patch('utils.database.pyodbc')
    def test_get_pooled_connection_from_pool(self, mock_pyodbc):
        """Test getting pooled connection from existing pool"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        
        # First connection
        conn1 = db_manager.get_pooled_connection()
        db_manager.release_connection(conn1)
        
        # Second connection should come from pool
        conn2 = db_manager.get_pooled_connection()
        
        assert conn2 == mock_conn1  # Reused from pool
        assert db_manager._in_use == 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '1'
    })
    @patch('utils.database.pyodbc')
    def test_release_connection_to_pool(self, mock_pyodbc):
        """Test releasing connection back to pool"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        connection = db_manager.get_pooled_connection()
        assert db_manager._in_use == 1
        assert len(db_manager._pool) == 0
        
        db_manager.release_connection(connection)
        
        assert db_manager._in_use == 0
        assert len(db_manager._pool) == 1
        assert db_manager._pool[0] == connection

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '1'
    })
    @patch('utils.database.pyodbc')
    def test_release_connection_pool_full_closes(self, mock_pyodbc):
        """Test releasing connection when pool is full closes connection"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        
        # Fill the pool
        conn1 = db_manager.get_pooled_connection()
        db_manager.release_connection(conn1)
        
        # Get another connection
        conn2 = db_manager.get_pooled_connection()
        
        # Pool is full, releasing should close the connection
        db_manager.release_connection(conn2)
        
        mock_conn2.close.assert_called_once()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_pool_stats(self, mock_pyodbc):
        """Test getting pool statistics"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Get a pooled connection
        connection = db_manager.get_pooled_connection()
        
        stats = db_manager.get_pool_stats()
        
        assert stats['pool_size'] == 5
        assert stats['available'] == 0
        assert stats['in_use'] == 1
        assert stats['total_created'] == 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_close_pool(self, mock_pyodbc):
        """Test closing connection pool"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        
        # Create and release connections to populate pool
        conn1 = db_manager.get_pooled_connection()
        db_manager.release_connection(conn1)
        
        conn2 = db_manager.get_pooled_connection()
        db_manager.release_connection(conn2)
        
        # Close pool
        db_manager.close_pool()
        
        # All connections should be closed
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        assert len(db_manager._pool) == 0
        assert db_manager._in_use == 0

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_test_connection_success(self, mock_pyodbc):
        """Test test_connection method with successful connection"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ['Test DB', '1']
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert result['success'] is True
        assert result['database_name'] == 'Test DB'
        assert result['database_version'] == '1'
        mock_cursor.execute.assert_called()
        mock_conn.close.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_test_connection_failure(self, mock_pyodbc):
        """Test test_connection method with connection failure"""
        from utils.database import DatabaseManager
        
        mock_pyodbc.connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert result['success'] is False
        assert 'error' in result
        assert 'Connection failed' in result['error']


class TestDatabaseSchemaOperations:
    """Test database schema and table operations"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_ensure_schemas_exist(self):
        """Test ensuring database schemas exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        db_manager.ensure_schemas_exist(mock_conn)
        
        # Should create data and backup schemas (2 unique schemas)
        assert mock_cursor.execute.call_count == 2
        
        # Verify schema creation SQL
        calls = mock_cursor.execute.call_args_list
        schema_calls = [call for call in calls if 'CREATE SCHEMA' in str(call)]
        assert len(schema_calls) == 2

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_table_exists_true(self):
        """Test table_exists when table exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.table_exists(mock_conn, 'test_table', 'ref')
        
        assert result is True
        mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_table_exists_false(self):
        """Test table_exists when table doesn't exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.table_exists(mock_conn, 'test_table', 'ref')
        
        assert result is False

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_table_exists_default_schema(self):
        """Test table_exists with default schema"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.table_exists(mock_conn, 'test_table')
        
        assert result is True
        # Should use default data_schema
        execute_call = mock_cursor.execute.call_args[0][0]
        assert 'ref' in execute_call  # default data_schema

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_get_table_columns(self):
        """Test getting table column information"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock column data
        mock_row = MagicMock()
        mock_row.COLUMN_NAME = 'test_col'
        mock_row.DATA_TYPE = 'varchar'
        mock_row.CHARACTER_MAXIMUM_LENGTH = 50
        mock_row.NUMERIC_PRECISION = None
        mock_row.NUMERIC_SCALE = None
        mock_row.IS_NULLABLE = 'YES'
        mock_row.COLUMN_DEFAULT = None
        mock_row.ORDINAL_POSITION = 1
        
        mock_cursor.fetchall.return_value = [mock_row]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        columns = db_manager.get_table_columns(mock_conn, 'test_table', 'ref')
        
        assert len(columns) == 1
        assert columns[0]['name'] == 'test_col'
        assert columns[0]['type'] == 'varchar'
        assert columns[0]['max_length'] == 50
        assert columns[0]['nullable'] is True

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_create_table(self):
        """Test creating a new table"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        columns = [
            {'name': 'id', 'type': 'int', 'nullable': False},
            {'name': 'name', 'type': 'varchar', 'max_length': 100, 'nullable': True}
        ]
        
        db_manager.create_table(mock_conn, 'test_table', columns, 'ref')
        
        mock_cursor.execute.assert_called()
        execute_call = mock_cursor.execute.call_args[0][0]
        assert 'CREATE TABLE' in execute_call
        assert '[ref].[test_table]' in execute_call
        assert 'id' in execute_call
        assert 'name' in execute_call

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_drop_table_if_exists(self):
        """Test dropping table if it exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=True):
            result = db_manager.drop_table_if_exists(mock_conn, 'test_table', 'ref')
        
        assert result is True
        mock_cursor.execute.assert_called()
        execute_call = mock_cursor.execute.call_args[0][0]
        assert 'DROP TABLE' in execute_call

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_drop_table_if_exists_not_exists(self):
        """Test dropping table when it doesn't exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=False):
            result = db_manager.drop_table_if_exists(mock_conn, 'test_table', 'ref')
        
        assert result is False
        # Should not execute DROP TABLE
        mock_cursor.execute.assert_not_called()


class TestDatabaseUtilities:
    """Test database utility methods"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_normalize_data_type_varchar(self):
        """Test data type normalization for varchar"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        result = db_manager._normalize_data_type('varchar', max_length=100)
        assert result == 'VARCHAR(100)'
        
        result = db_manager._normalize_data_type('varchar')
        assert result == 'VARCHAR(255)'  # default

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_normalize_data_type_decimal(self):
        """Test data type normalization for decimal"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        result = db_manager._normalize_data_type('decimal', numeric_precision=10, numeric_scale=2)
        assert result == 'DECIMAL(10,2)'
        
        result = db_manager._normalize_data_type('decimal')
        assert result == 'DECIMAL(18,0)'  # default

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_normalize_data_type_int(self):
        """Test data type normalization for int"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        result = db_manager._normalize_data_type('int')
        assert result == 'INT'

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_get_timestamp_suffix(self):
        """Test getting timestamp suffix"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        timestamp = db_manager._get_timestamp_suffix()
        
        # Should be in format YYYYMMDD_HHMMSS
        assert len(timestamp) == 15  # YYYYMMDD_HHMMSS
        assert timestamp[8] == '_'
        
        # Should be parseable as datetime components
        date_part = timestamp[:8]
        time_part = timestamp[9:]
        assert date_part.isdigit()
        assert time_part.isdigit()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_truncate_table(self):
        """Test truncating a table"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        db_manager.truncate_table(mock_conn, 'test_table', 'ref')
        
        mock_cursor.execute.assert_called()
        execute_call = mock_cursor.execute.call_args[0][0]
        assert 'TRUNCATE TABLE' in execute_call
        assert '[ref].[test_table]' in execute_call

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_get_row_count(self):
        """Test getting table row count"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [100]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        count = db_manager.get_row_count(mock_conn, 'test_table', 'ref')
        
        assert count == 100
        mock_cursor.execute.assert_called()
        execute_call = mock_cursor.execute.call_args[0][0]
        assert 'SELECT COUNT(*)' in execute_call


class TestDatabaseErrorHandling:
    """Test database error handling scenarios"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_connection_pool_error_handling(self, mock_pyodbc):
        """Test connection pool error handling"""
        from utils.database import DatabaseManager
        
        # First connection succeeds, second fails
        mock_conn = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn, Exception("Connection failed")]
        
        db_manager = DatabaseManager()
        
        # First connection should succeed
        conn1 = db_manager.get_pooled_connection()
        assert conn1 == mock_conn
        
        # Second connection should raise exception
        with pytest.raises(Exception, match="Connection failed"):
            db_manager.get_pooled_connection()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_table_exists_sql_error(self):
        """Test table_exists with SQL error"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("SQL Error")
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with pytest.raises(Exception, match="SQL Error"):
            db_manager.table_exists(mock_conn, 'test_table')

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_create_table_sql_error(self):
        """Test create_table with SQL error"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("CREATE TABLE failed")
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        columns = [{'name': 'id', 'type': 'int', 'nullable': False}]
        
        with pytest.raises(Exception, match="CREATE TABLE failed"):
            db_manager.create_table(mock_conn, 'test_table', columns)


class TestDatabaseThreadSafety:
    """Test database thread safety and connection pooling"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': '2'
    })
    @patch('utils.database.pyodbc')
    def test_concurrent_connection_access(self, mock_pyodbc):
        """Test concurrent access to connection pool"""
        from utils.database import DatabaseManager
        
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_pyodbc.connect.side_effect = [mock_conn1, mock_conn2]
        
        db_manager = DatabaseManager()
        connections = []
        exceptions = []
        
        def get_connection():
            try:
                conn = db_manager.get_pooled_connection()
                connections.append(conn)
                time.sleep(0.01)  # Hold connection briefly
                db_manager.release_connection(conn)
            except Exception as e:
                exceptions.append(e)
        
        # Create multiple threads
        threads = []
        for _ in range(3):
            thread = threading.Thread(target=get_connection)
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Should have successfully handled concurrent access
        assert len(exceptions) == 0
        assert len(connections) == 3