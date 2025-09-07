"""
Comprehensive tests to push utils/database.py from 30% to >90% coverage
This is a foundational test file targeting the most critical database functionality
"""

import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock, call
from datetime import datetime

# Mock pyodbc before importing database module
import sys
mock_pyodbc = MagicMock()
sys.modules['pyodbc'] = mock_pyodbc

from utils.database import DatabaseManager


class MockConnection:
    """Mock database connection"""
    
    def __init__(self, simulate_error=False, fail_on_execute=False):
        self.simulate_error = simulate_error
        self.fail_on_execute = fail_on_execute
        self.autocommit = True
        self.closed = False
        self.cursor_calls = []
        
    def cursor(self):
        if self.simulate_error:
            raise Exception("Cursor creation failed")
        return MockCursor(fail_on_execute=self.fail_on_execute)
    
    def close(self):
        self.closed = True
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        

class MockCursor:
    """Mock database cursor"""
    
    def __init__(self, fail_on_execute=False, return_data=None):
        self.fail_on_execute = fail_on_execute
        self.return_data = return_data or []
        self.executed_queries = []
        self.current_data_index = 0
        
    def execute(self, query, *params):
        self.executed_queries.append((query, params))
        if self.fail_on_execute:
            raise Exception("SQL execution failed")
    
    def fetchone(self):
        if self.return_data and self.current_data_index < len(self.return_data):
            result = self.return_data[self.current_data_index]
            self.current_data_index += 1
            return result
        return None
    
    def fetchall(self):
        return self.return_data


class TestDatabaseManager90Percent:
    """Comprehensive tests for DatabaseManager class"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Mock environment variables
        self.env_vars = {
            'db_host': 'test-server',
            'db_name': 'test_db', 
            'db_user': 'test_user',
            'db_password': 'test_password',
            'data_schema': 'ref',
            'backup_schema': 'bkp',
            'validation_sp_schema': 'ref',
            'DB_POOL_SIZE': '5',
            'DB_MAX_RETRIES': '3',
            'DB_RETRY_BACKOFF': '0.5'
        }
        
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_init_success(self):
        """Test successful DatabaseManager initialization"""
        db_manager = DatabaseManager()
        
        assert db_manager.username == 'test_user'
        assert db_manager.password == 'test_pass'
        assert db_manager.data_schema == 'ref'
        assert db_manager.backup_schema == 'bkp'
        assert db_manager.pool_size == 5
        assert db_manager.max_retries == 3
    
    @patch.dict('os.environ', {'db_password': 'test_pass'}, clear=True)
    def test_init_missing_username(self):
        """Test initialization with missing username"""
        with pytest.raises(ValueError, match="Database username.*required"):
            DatabaseManager()
    
    @patch.dict('os.environ', {'db_user': 'test_user'}, clear=True)
    def test_init_missing_password(self):
        """Test initialization with missing password"""
        with pytest.raises(ValueError, match="Database password.*required"):
            DatabaseManager()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass', 'DB_POOL_SIZE': 'invalid'})
    def test_init_invalid_pool_size(self):
        """Test initialization with invalid pool size"""
        db_manager = DatabaseManager()
        # Should default to 5 when invalid
        assert db_manager.pool_size == 5
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_build_connection_string(self):
        """Test connection string building"""
        db_manager = DatabaseManager()
        conn_string = db_manager._build_connection_string()
        
        assert 'DRIVER=' in conn_string
        assert 'SERVER=localhost' in conn_string
        assert 'DATABASE=test' in conn_string
        assert 'UID=test_user' in conn_string
        assert 'PWD=test_pass' in conn_string
        assert 'TrustServerCertificate=yes' in conn_string
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass', 'db_odbc_driver': 'Custom Driver'})
    def test_build_connection_string_custom_driver(self):
        """Test connection string with custom driver"""
        db_manager = DatabaseManager()
        conn_string = db_manager._build_connection_string()
        
        assert 'DRIVER={Custom Driver}' in conn_string
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_get_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_connection = MockConnection()
        mock_connect.return_value = mock_connection
        
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        
        assert conn is not None
        assert conn.autocommit is True
        mock_connect.assert_called_once()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_get_connection_retry_success(self, mock_connect):
        """Test connection with retry logic"""
        # First call fails, second succeeds
        mock_connection = MockConnection()
        mock_connect.side_effect = [Exception("Connection failed"), mock_connection]
        
        db_manager = DatabaseManager()
        
        with patch('time.sleep') as mock_sleep:
            conn = db_manager.get_connection()
            
            assert conn is not None
            assert mock_connect.call_count == 2
            mock_sleep.assert_called()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_get_connection_max_retries_exceeded(self, mock_connect):
        """Test connection failure after max retries"""
        mock_connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager()
        
        with patch('time.sleep'):
            with pytest.raises(Exception, match="Database connection failed"):
                db_manager.get_connection()
                
            # Should try max_retries times (3)
            assert mock_connect.call_count == 3
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_pooled_connection_operations(self, mock_connect):
        """Test pooled connection get/release operations"""
        mock_connection1 = MockConnection()
        mock_connection2 = MockConnection()
        mock_connect.side_effect = [mock_connection1, mock_connection2]
        
        db_manager = DatabaseManager()
        
        # Get first connection
        conn1 = db_manager.get_pooled_connection()
        assert db_manager._in_use == 1
        
        # Get second connection 
        conn2 = db_manager.get_pooled_connection()
        assert db_manager._in_use == 2
        
        # Release first connection
        db_manager.release_connection(conn1)
        assert db_manager._in_use == 1
        assert len(db_manager._pool) == 1
        
        # Release second connection
        db_manager.release_connection(conn2)
        assert db_manager._in_use == 0
        assert len(db_manager._pool) == 2
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_release_connection_none(self):
        """Test releasing None connection"""
        db_manager = DatabaseManager()
        # Should handle None gracefully
        db_manager.release_connection(None)
        assert db_manager._in_use == 0
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_pool_stats(self):
        """Test pool statistics"""
        db_manager = DatabaseManager()
        
        stats = db_manager.get_pool_stats()
        
        assert 'pool_size_config' in stats
        assert 'idle' in stats
        assert 'in_use' in stats
        assert 'available_capacity' in stats
        assert stats['pool_size_config'] == 5
        assert stats['idle'] == 0
        assert stats['in_use'] == 0
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_close_pool(self, mock_connect):
        """Test closing connection pool"""
        mock_connection = MockConnection()
        mock_connect.return_value = mock_connection
        
        db_manager = DatabaseManager()
        
        # Add connection to pool
        conn = db_manager.get_pooled_connection()
        db_manager.release_connection(conn)
        assert len(db_manager._pool) == 1
        
        # Close pool
        db_manager.close_pool()
        assert len(db_manager._pool) == 0
        assert mock_connection.closed is True
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_test_connection_success(self, mock_connect):
        """Test connection testing success"""
        # Mock cursor with version and date data
        mock_cursor = MockCursor(return_data=[("SQL Server 2019", datetime.now())])
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        mock_connect.return_value = mock_connection
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert result['status'] == 'success'
        assert 'server_version' in result
        assert 'current_time' in result
        assert 'schemas' in result
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_test_connection_failure(self, mock_connect):
        """Test connection testing failure"""
        mock_connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert result['status'] == 'error'
        assert 'error' in result
        assert 'traceback' in result
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_ensure_schemas_exist_success(self):
        """Test successful schema creation"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        db_manager.ensure_schemas_exist(mock_connection)
        
        # Should execute schema creation queries
        assert len(mock_cursor.executed_queries) >= 2  # At least ref and bkp schemas
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_ensure_schemas_exist_failure(self):
        """Test schema creation failure"""
        mock_cursor = MockCursor(fail_on_execute=True)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with pytest.raises(Exception, match="Failed to create schema"):
            db_manager.ensure_schemas_exist(mock_connection)
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_table_exists_true(self):
        """Test table exists check - table exists"""
        mock_cursor = MockCursor(return_data=[(1,)])  # Table exists
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        exists = db_manager.table_exists(mock_connection, 'test_table')
        
        assert exists is True
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_table_exists_false(self):
        """Test table exists check - table does not exist"""
        mock_cursor = MockCursor(return_data=[(0,)])  # Table doesn't exist
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        exists = db_manager.table_exists(mock_connection, 'nonexistent_table')
        
        assert exists is False
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_table_columns(self):
        """Test getting table column information"""
        # Mock column data
        class MockRow:
            def __init__(self, col_name, data_type, max_len, precision, scale, nullable, default, position):
                self.COLUMN_NAME = col_name
                self.DATA_TYPE = data_type
                self.CHARACTER_MAXIMUM_LENGTH = max_len
                self.NUMERIC_PRECISION = precision
                self.NUMERIC_SCALE = scale
                self.IS_NULLABLE = nullable
                self.COLUMN_DEFAULT = default
                self.ORDINAL_POSITION = position
        
        mock_rows = [
            MockRow('id', 'int', None, 10, 0, 'NO', None, 1),
            MockRow('name', 'varchar', 255, None, None, 'YES', None, 2)
        ]
        
        mock_cursor = MockCursor()
        mock_cursor.fetchall = MagicMock(return_value=mock_rows)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        columns = db_manager.get_table_columns(mock_connection, 'test_table')
        
        assert len(columns) == 2
        assert columns[0]['name'] == 'id'
        assert columns[0]['data_type'] == 'int'
        assert columns[0]['nullable'] is False
        assert columns[1]['name'] == 'name'
        assert columns[1]['data_type'] == 'varchar'
        assert columns[1]['nullable'] is True
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_table_success(self):
        """Test successful table creation"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        columns = [
            {'name': 'id', 'data_type': 'INT'},
            {'name': 'name', 'data_type': 'VARCHAR(255)'}
        ]
        
        db_manager = DatabaseManager()
        db_manager.create_table(mock_connection, 'test_table', columns)
        
        # Should execute DROP and CREATE statements
        assert len(mock_cursor.executed_queries) == 2
        
        # Check DROP statement
        drop_query = mock_cursor.executed_queries[0][0]
        assert 'DROP TABLE IF EXISTS' in drop_query
        
        # Check CREATE statement  
        create_query = mock_cursor.executed_queries[1][0]
        assert 'CREATE TABLE' in create_query
        assert 'ref_data_loadtime' in create_query  # Metadata columns added by default
        assert 'ref_data_loadtype' in create_query
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_table_no_metadata(self):
        """Test table creation without metadata columns"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        columns = [{'name': 'id', 'data_type': 'INT'}]
        
        db_manager = DatabaseManager()
        db_manager.create_table(mock_connection, 'test_table', columns, add_metadata_columns=False)
        
        create_query = mock_cursor.executed_queries[1][0]
        assert 'ref_data_loadtime' not in create_query
        assert 'ref_data_loadtype' not in create_query
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_drop_table_if_exists_success(self):
        """Test successful table drop"""
        mock_cursor = MockCursor(return_data=[(1,)])  # Table exists
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.drop_table_if_exists(mock_connection, 'test_table')
            
            assert result is True
            mock_print.assert_called_with("INFO: Dropped table [ref].[test_table]")
            
            # Should execute check and drop statements
            assert len(mock_cursor.executed_queries) == 2
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_drop_table_if_exists_not_exists(self):
        """Test table drop when table doesn't exist"""
        mock_cursor = MockCursor(return_data=[(0,)])  # Table doesn't exist
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.drop_table_if_exists(mock_connection, 'nonexistent_table')
            
            assert result is False
            mock_print.assert_called_with("INFO: Table [ref].[nonexistent_table] does not exist, no drop needed")
            
            # Should only execute check statement
            assert len(mock_cursor.executed_queries) == 1