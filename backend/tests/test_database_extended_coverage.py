"""
Extended tests to push utils/database.py coverage to the next level
Targeting backup operations, schema validation, and advanced database functionality
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
    """Enhanced mock database connection"""
    
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
    
    def commit(self):
        """Mock commit method"""
        pass
    
    def rollback(self):
        """Mock rollback method"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MockCursor:
    """Enhanced mock database cursor"""
    
    def __init__(self, fail_on_execute=False, return_data=None, custom_responses=None):
        self.fail_on_execute = fail_on_execute
        self.return_data = return_data or []
        self.custom_responses = custom_responses or {}
        self.executed_queries = []
        self.current_data_index = 0
        
    def execute(self, query, *params):
        self.executed_queries.append((query, params))
        if self.fail_on_execute:
            if isinstance(self.fail_on_execute, str) and self.fail_on_execute in query:
                raise Exception(f"SQL execution failed for: {self.fail_on_execute}")
            elif self.fail_on_execute is True:
                raise Exception("SQL execution failed")
    
    def fetchone(self):
        query_key = None
        if self.executed_queries:
            last_query = self.executed_queries[-1][0]
            # Determine query type for custom responses
            if 'COUNT(*)' in last_query and 'INFORMATION_SCHEMA.TABLES' in last_query:
                query_key = 'table_exists'
            elif 'INFORMATION_SCHEMA.COLUMNS' in last_query:
                query_key = 'column_info'
            elif 'SELECT @@VERSION' in last_query:
                query_key = 'version_info'
        
        if query_key and query_key in self.custom_responses:
            return self.custom_responses[query_key]
        
        if self.return_data and self.current_data_index < len(self.return_data):
            result = self.return_data[self.current_data_index]
            self.current_data_index += 1
            return result
        return None
    
    def fetchall(self):
        if self.return_data:
            return self.return_data
        return []


class TestDatabaseManagerExtendedCoverage:
    """Extended tests for DatabaseManager class to achieve higher coverage"""
    
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
            'validation_sp_schema': 'ref'
        }
        
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    @patch('utils.database.pyodbc.connect')
    def test_pooled_connection_pool_full_scenario(self, mock_connect):
        """Test pooled connection when pool is full"""
        mock_connections = [MockConnection() for _ in range(6)]
        mock_connect.side_effect = mock_connections
        
        db_manager = DatabaseManager()
        db_manager.pool_size = 2  # Small pool size
        
        # Fill the pool
        conn1 = db_manager.get_pooled_connection()
        conn2 = db_manager.get_pooled_connection()
        
        # Pool should be full, next connection should wait
        with patch('time.sleep') as mock_sleep:
            # Release one connection to make space
            db_manager.release_connection(conn1)
            
            # This should now work without creating a new connection
            conn3 = db_manager.get_pooled_connection()
            
            assert conn3 is not None
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_release_connection_pool_full(self):
        """Test releasing connection when pool is full - should close connection"""
        db_manager = DatabaseManager()
        db_manager.pool_size = 1  # Very small pool
        
        # Create mock connections
        conn1 = MockConnection()
        conn2 = MockConnection()
        
        # Fill the pool
        db_manager._pool.append(conn1)
        
        # Release another connection - should close it since pool is full
        db_manager.release_connection(conn2)
        
        assert conn2.closed is True
        assert len(db_manager._pool) == 1  # Pool still has original connection
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_release_connection_exception_handling(self):
        """Test release_connection with exception during close"""
        db_manager = DatabaseManager()
        
        # Mock connection that raises exception on close
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close failed")
        
        # Should handle exception gracefully
        db_manager.release_connection(mock_conn)
        
        # Should not raise exception
        assert True
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_close_pool_with_exception(self):
        """Test close_pool with exception during connection close"""
        db_manager = DatabaseManager()
        
        # Add mock connection that raises exception on close
        mock_conn = MagicMock()
        mock_conn.close.side_effect = Exception("Close failed")
        db_manager._pool.append(mock_conn)
        
        # Should handle exception gracefully
        db_manager.close_pool()
        
        assert len(db_manager._pool) == 0
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_backup_table_new_table(self):
        """Test creating a new backup table - covers lines 297-359"""
        # Mock cursor responses for backup table creation
        mock_cursor = MockCursor(custom_responses={
            'table_exists': (0,)  # Backup table doesn't exist
        })
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None, 'numeric_precision': 10, 'numeric_scale': 0},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255, 'numeric_precision': None, 'numeric_scale': None}
        ]
        
        with patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml or np or 'MAX'})"):
            with patch('builtins.print'):  # Mock print statements
                db_manager.create_backup_table(mock_connection, 'test_table', columns)
        
        # Verify table creation query was executed
        create_queries = [q for q in mock_cursor.executed_queries if 'CREATE TABLE' in q[0]]
        assert len(create_queries) == 1
        
        create_query = create_queries[0][0]
        assert 'bkp' in create_query  # Should use backup schema
        assert 'test_table_backup' in create_query
        assert 'ref_data_version_id' in create_query  # Should have backup-specific columns
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_backup_table_exists_schema_matches(self):
        """Test backup table creation when table exists and schema matches"""
        mock_cursor = MockCursor(custom_responses={
            'table_exists': (1,)  # Backup table exists
        })
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        columns = [{'name': 'id', 'data_type': 'int'}]
        
        with patch.object(db_manager, '_backup_schema_matches', return_value=True):
            with patch('builtins.print') as mock_print:
                db_manager.create_backup_table(mock_connection, 'test_table', columns)
                
                # Should print schema compatibility message
                mock_print.assert_called()
        
        # Should not execute CREATE TABLE since schema matches
        create_queries = [q for q in mock_cursor.executed_queries if 'CREATE TABLE' in q[0]]
        assert len(create_queries) == 0
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_backup_table_exists_schema_mismatch_sync_success(self):
        """Test backup table with schema mismatch but successful sync"""
        mock_cursor = MockCursor(custom_responses={'table_exists': (1,)})
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        columns = [{'name': 'id', 'data_type': 'int'}]
        
        with patch.object(db_manager, '_backup_schema_matches', return_value=False), \
             patch.object(db_manager, '_sync_backup_table_schema', return_value={'success': True, 'summary': 'Added 1 column'}), \
             patch('builtins.print') as mock_print:
            
            db_manager.create_backup_table(mock_connection, 'test_table', columns)
            
            # Should print sync success message
            sync_calls = [call for call in mock_print.call_args_list if 'Successfully synced' in str(call)]
            assert len(sync_calls) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_backup_table_exists_schema_mismatch_sync_fails(self):
        """Test backup table with schema mismatch and sync failure - covers lines 324-336"""
        mock_cursor = MockCursor(custom_responses={'table_exists': (1,)})
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        columns = [{'name': 'id', 'data_type': 'int'}]
        
        with patch.object(db_manager, '_backup_schema_matches', return_value=False), \
             patch.object(db_manager, '_sync_backup_table_schema', return_value={'success': False, 'error': 'Sync failed'}), \
             patch.object(db_manager, '_get_timestamp_suffix', return_value='20240101_120000'), \
             patch('builtins.print') as mock_print:
            
            db_manager.create_backup_table(mock_connection, 'test_table', columns)
            
            # Should print fallback messages
            fallback_calls = [call for call in mock_print.call_args_list if 'rename and recreate' in str(call)]
            assert len(fallback_calls) >= 1
            
            # Should execute rename operation
            rename_queries = [q for q in mock_cursor.executed_queries if 'sp_rename' in q[0]]
            assert len(rename_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_backup_schema_matches_success(self):
        """Test _backup_schema_matches when schemas match - covers lines 363-408"""
        # Mock column data for existing backup table
        class MockRow:
            def __init__(self, name, data_type, max_len=None, precision=None, scale=None):
                self.COLUMN_NAME = name
                self.DATA_TYPE = data_type
                self.CHARACTER_MAXIMUM_LENGTH = max_len
                self.NUMERIC_PRECISION = precision
                self.NUMERIC_SCALE = scale
                self.IS_NULLABLE = 'YES'
                self.COLUMN_DEFAULT = None
                self.ORDINAL_POSITION = 1
        
        existing_columns_data = [
            MockRow('id', 'int', precision=10, scale=0),
            MockRow('name', 'varchar', max_len=255),
            MockRow('ref_data_loadtime', 'datetime'),  # Metadata column - should be filtered
            MockRow('ref_data_version_id', 'int')  # Metadata column - should be filtered
        ]
        
        mock_connection = MockConnection()
        db_manager = DatabaseManager()
        
        expected_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None, 'numeric_precision': 10, 'numeric_scale': 0},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255, 'numeric_precision': None, 'numeric_scale': None}
        ]
        
        with patch.object(db_manager, 'get_table_columns') as mock_get_columns, \
             patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml or np or 'MAX'})"), \
             patch('builtins.print'):
            
            # Mock get_table_columns to return the existing columns
            mock_columns = []
            for row in existing_columns_data:
                mock_columns.append({
                    'name': row.COLUMN_NAME,
                    'data_type': row.DATA_TYPE,
                    'max_length': row.CHARACTER_MAXIMUM_LENGTH,
                    'numeric_precision': row.NUMERIC_PRECISION,
                    'numeric_scale': row.NUMERIC_SCALE
                })
            mock_get_columns.return_value = mock_columns
            
            result = db_manager._backup_schema_matches(mock_connection, 'test_table_backup', expected_columns)
            
            assert result is True
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_backup_schema_matches_failure(self):
        """Test _backup_schema_matches when schemas don't match"""
        mock_connection = MockConnection()
        db_manager = DatabaseManager()
        
        # Mock existing columns (different from expected)
        existing_columns = [
            {'name': 'id', 'data_type': 'bigint', 'max_length': None, 'numeric_precision': 19, 'numeric_scale': 0},
            {'name': 'different_name', 'data_type': 'varchar', 'max_length': 100}
        ]
        
        expected_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None, 'numeric_precision': 10, 'numeric_scale': 0},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns), \
             patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml or np or 'MAX'})"), \
             patch('builtins.print'):
            
            result = db_manager._backup_schema_matches(mock_connection, 'test_table_backup', expected_columns)
            
            assert result is False
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_backup_schema_matches_exception(self):
        """Test _backup_schema_matches exception handling - covers lines 405-408"""
        mock_connection = MockConnection()
        db_manager = DatabaseManager()
        
        expected_columns = [{'name': 'id', 'data_type': 'int'}]
        
        with patch.object(db_manager, 'get_table_columns', side_effect=Exception("Column fetch failed")), \
             patch('builtins.print') as mock_print:
            
            result = db_manager._backup_schema_matches(mock_connection, 'test_table_backup', expected_columns)
            
            # Should return False when exception occurs
            assert result is False
            
            # Should print warning
            warning_calls = [call for call in mock_print.call_args_list if 'Could not validate' in str(call)]
            assert len(warning_calls) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_sync_backup_table_schema_add_columns(self):
        """Test _sync_backup_table_schema adding missing columns - covers lines 410-444"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        # Existing columns (missing 'name' column)
        existing_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'}  # Metadata column
        ]
        
        # Expected columns (includes new 'name' column)
        expected_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 255}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns), \
             patch.object(db_manager, '_normalize_data_type', side_effect=lambda dt, ml, np, ns: f"{dt}({ml or 'MAX'})"), \
             patch('builtins.print') as mock_print:
            
            result = db_manager._sync_backup_table_schema(mock_connection, 'test_table_backup', expected_columns)
            
            assert result['success'] is True
            assert len(result['changes']['added']) == 1
            assert result['changes']['added'][0]['column'] == 'name'
            
            # Should execute ALTER TABLE ADD COLUMN
            alter_queries = [q for q in mock_cursor.executed_queries if 'ALTER TABLE' in q[0] and 'ADD' in q[0]]
            assert len(alter_queries) == 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_available_capacity_calculation(self):
        """Test get_pool_stats available capacity calculation - covers line 130"""
        db_manager = DatabaseManager()
        db_manager.pool_size = 5
        
        # Test different scenarios
        test_cases = [
            (0, 0, 5),  # empty pool, no connections in use
            (2, 1, 2),  # 2 idle, 1 in use, capacity = 5 - (1 + 2) = 2
            (0, 5, 0),  # no idle, 5 in use, capacity = 0
            (5, 2, 0),  # 5 idle, 2 in use would be 7 total, but max is 5, so capacity = 0
        ]
        
        for idle_count, in_use_count, expected_capacity in test_cases:
            with db_manager._pool_lock:
                # Set up mock pool state
                db_manager._pool = [MagicMock() for _ in range(idle_count)]
                db_manager._in_use = in_use_count
            
            stats = db_manager.get_pool_stats()
            
            assert stats['available_capacity'] == expected_capacity
            assert stats['idle'] == idle_count
            assert stats['in_use'] == in_use_count
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass', 'db_odbc_driver': '{Custom ODBC Driver}'})
    def test_build_connection_string_driver_already_quoted(self):
        """Test connection string with driver already in braces - covers line 71"""
        db_manager = DatabaseManager()
        conn_string = db_manager._build_connection_string()
        
        # Should not double-quote driver that's already in braces
        assert 'DRIVER={Custom ODBC Driver}' in conn_string
        assert 'DRIVER={{Custom ODBC Driver}}' not in conn_string  # Should not double-brace
    
    @patch.dict('os.environ', {'db_user': '', 'db_password': 'test_pass'})
    def test_build_connection_string_empty_username(self):
        """Test connection string building with empty username - covers lines 63-64"""
        with pytest.raises(ValueError, match="Database username.*required"):
            DatabaseManager()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': ''})
    def test_build_connection_string_empty_password(self):
        """Test connection string building with empty password - covers lines 65-66"""
        with pytest.raises(ValueError, match="Database password.*required"):
            DatabaseManager()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_table_exists_with_custom_schema(self):
        """Test table_exists with custom schema parameter"""
        mock_cursor = MockCursor(return_data=[(1,)])  # Table exists
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        exists = db_manager.table_exists(mock_connection, 'test_table', schema='custom_schema')
        
        assert exists is True
        
        # Verify the query used the custom schema
        query_params = mock_cursor.executed_queries[0][1]
        assert 'custom_schema' in query_params
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_table_columns_with_custom_schema(self):
        """Test get_table_columns with custom schema parameter"""
        # Mock column data
        class MockRow:
            def __init__(self):
                self.COLUMN_NAME = 'test_col'
                self.DATA_TYPE = 'varchar'
                self.CHARACTER_MAXIMUM_LENGTH = 100
                self.NUMERIC_PRECISION = None
                self.NUMERIC_SCALE = None
                self.IS_NULLABLE = 'NO'
                self.COLUMN_DEFAULT = None
                self.ORDINAL_POSITION = 1
        
        mock_cursor = MockCursor()
        mock_cursor.fetchall = MagicMock(return_value=[MockRow()])
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        columns = db_manager.get_table_columns(mock_connection, 'test_table', schema='custom_schema')
        
        assert len(columns) == 1
        assert columns[0]['name'] == 'test_col'
        assert columns[0]['nullable'] is False  # 'NO' -> False
        
        # Verify the query used the custom schema
        query_params = mock_cursor.executed_queries[0][1]
        assert 'custom_schema' in query_params
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_table_with_custom_schema(self):
        """Test create_table with custom schema parameter"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        columns = [{'name': 'id', 'data_type': 'INT'}]
        
        db_manager = DatabaseManager()
        db_manager.create_table(mock_connection, 'test_table', columns, schema='custom_schema')
        
        # Check that custom schema was used in both DROP and CREATE
        drop_query = mock_cursor.executed_queries[0][0]
        create_query = mock_cursor.executed_queries[1][0]
        
        assert '[custom_schema].[test_table]' in drop_query
        assert '[custom_schema].[test_table]' in create_query
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_drop_table_if_exists_with_custom_schema(self):
        """Test drop_table_if_exists with custom schema parameter"""
        mock_cursor = MockCursor(return_data=[(1,)])  # Table exists
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print'):
            result = db_manager.drop_table_if_exists(mock_connection, 'test_table', schema='custom_schema')
        
        assert result is True
        
        # Verify custom schema was used in both check and drop
        check_params = mock_cursor.executed_queries[0][1]
        drop_query = mock_cursor.executed_queries[1][0]
        
        assert 'custom_schema' in check_params
        assert '[custom_schema].[test_table]' in drop_query