"""
Tests to push utils/database.py coverage to the next level (targeting 50%+)
Focusing on data insertion, bulk operations, and advanced database functionality
"""

import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock, call
from datetime import datetime
import pandas as pd

# Mock pyodbc before importing database module
import sys
mock_pyodbc = MagicMock()
sys.modules['pyodbc'] = mock_pyodbc

from utils.database import DatabaseManager


class MockConnection:
    """Enhanced mock database connection with transaction support"""
    
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
        pass
    
    def rollback(self):
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class MockCursor:
    """Enhanced mock database cursor with execute tracking"""
    
    def __init__(self, fail_on_execute=False, return_data=None, custom_responses=None):
        self.fail_on_execute = fail_on_execute
        self.return_data = return_data or []
        self.custom_responses = custom_responses or {}
        self.executed_queries = []
        self.fetchall_data = []
        self.fetchone_data = None
        
    def execute(self, query, params=None):
        if self.fail_on_execute:
            raise Exception("Execute failed")
        self.executed_queries.append((query, params))
        
    def fetchall(self):
        return self.return_data
    
    def fetchone(self):
        # Handle custom responses for specific queries
        for key, response in self.custom_responses.items():
            if key in str(self.executed_queries[-1][0]) if self.executed_queries else "":
                return response
        return self.fetchone_data
    
    def executemany(self, query, param_list):
        if self.fail_on_execute:
            raise Exception("ExecuteMany failed")
        self.executed_queries.append((query, param_list))
    
    def close(self):
        pass


class TestDatabaseManagerNextLevel:
    """Tests targeting the next level of coverage for DatabaseManager"""
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_init_with_custom_pool_size(self):
        """Test DatabaseManager init with custom pool size - covers lines 37, 39"""
        with patch.dict('os.environ', {'max_pool_size': '10'}):
            db_manager = DatabaseManager()
            assert db_manager.pool_size == 10
            assert db_manager.username == 'test_user'
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_connection_pool_size_limit(self):
        """Test connection pooling when pool is at capacity - covers lines 107-108, 121-122"""
        db_manager = DatabaseManager()
        db_manager.pool_size = 1
        
        # Fill the pool
        mock_conn = MockConnection()
        db_manager.connection_pool = [mock_conn]
        db_manager.pool_lock = MagicMock()
        
        with patch.object(db_manager, '_create_connection', return_value=MockConnection()) as mock_create, \
             patch('threading.Lock') as mock_lock_class:
            
            # Pool is full, should create new connection
            result = db_manager.get_connection()
            assert result is not None
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_insert_data_single_row(self):
        """Test insert_data with single row - covers lines 519-574"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        # Single row data
        data = [{'id': 1, 'name': 'John', 'age': 30}]
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.insert_data('test_table', data, 'test_schema')
            
            assert result['success'] is True
            assert result['rows_inserted'] == 1
            
            # Should use executemany for single row
            assert len(mock_cursor.executed_queries) >= 1
            insert_query = [q for q in mock_cursor.executed_queries if 'INSERT' in q[0]]
            assert len(insert_query) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_insert_data_batch_processing(self):
        """Test insert_data with batch processing - covers lines 519-574"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        # Large dataset to trigger batching
        data = [{'id': i, 'name': f'User{i}', 'age': 20 + i} for i in range(150)]
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.insert_data('test_table', data, 'test_schema', batch_size=50)
            
            assert result['success'] is True
            assert result['rows_inserted'] == 150
            
            # Should have multiple batch inserts (3 batches of 50 each)
            insert_queries = [q for q in mock_cursor.executed_queries if 'INSERT' in q[0]]
            assert len(insert_queries) >= 3  # At least 3 batches
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_insert_data_exception_handling(self):
        """Test insert_data exception handling - covers lines 519-574"""
        mock_cursor = MockCursor(fail_on_execute=True)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        data = [{'id': 1, 'name': 'John'}]
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.insert_data('test_table', data, 'test_schema')
            
            assert result['success'] is False
            assert 'error' in result
            assert result['rows_inserted'] == 0
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_bulk_insert_dataframe(self):
        """Test bulk_insert with DataFrame - covers lines 582-593"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35]
        })
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.bulk_insert('test_table', df, 'test_schema')
            
            assert result['success'] is True
            assert result['rows_inserted'] == 3
            
            # Should convert DataFrame to records and use executemany
            insert_queries = [q for q in mock_cursor.executed_queries if 'INSERT' in q[0]]
            assert len(insert_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_bulk_insert_exception(self):
        """Test bulk_insert exception handling - covers lines 582-593"""
        mock_cursor = MockCursor(fail_on_execute=True)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        df = pd.DataFrame({'id': [1], 'name': ['Test']})
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.bulk_insert('test_table', df, 'test_schema')
            
            assert result['success'] is False
            assert 'error' in result
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_update_data_single_condition(self):
        """Test update_data with single condition - covers lines 597-608"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        update_data = {'name': 'Updated Name', 'age': 35}
        conditions = {'id': 1}
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.update_data('test_table', update_data, conditions, 'test_schema')
            
            assert result['success'] is True
            
            # Should execute UPDATE statement
            update_queries = [q for q in mock_cursor.executed_queries if 'UPDATE' in q[0]]
            assert len(update_queries) >= 1
            
            # Check that the query contains SET and WHERE clauses
            query = update_queries[0][0]
            assert 'SET' in query
            assert 'WHERE' in query
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_update_data_multiple_conditions(self):
        """Test update_data with multiple conditions - covers lines 597-608"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        update_data = {'status': 'active'}
        conditions = {'id': 1, 'category': 'A'}
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.update_data('test_table', update_data, conditions, 'test_schema')
            
            assert result['success'] is True
            
            # Should execute UPDATE with multiple WHERE conditions
            update_queries = [q for q in mock_cursor.executed_queries if 'UPDATE' in q[0]]
            assert len(update_queries) >= 1
            
            query = update_queries[0][0]
            assert query.count('AND') >= 1  # Multiple conditions joined by AND
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_delete_data_with_conditions(self):
        """Test delete_data with conditions - covers lines 613-626"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        conditions = {'status': 'inactive', 'created_date': '2023-01-01'}
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.delete_data('test_table', conditions, 'test_schema')
            
            assert result['success'] is True
            
            # Should execute DELETE statement
            delete_queries = [q for q in mock_cursor.executed_queries if 'DELETE' in q[0]]
            assert len(delete_queries) >= 1
            
            query = delete_queries[0][0]
            assert 'DELETE FROM' in query
            assert 'WHERE' in query
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_truncate_table_success(self):
        """Test truncate_table success - covers lines 630-638"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            # Method returns None on success
            db_manager.truncate_table(mock_connection, 'test_table', 'test_schema')
            
            # Should execute TRUNCATE statement
            truncate_queries = [q for q in mock_cursor.executed_queries if 'TRUNCATE' in q[0]]
            assert len(truncate_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_truncate_table_exception(self):
        """Test truncate_table exception handling - covers lines 630-638"""
        mock_cursor = MockCursor(fail_on_execute=True)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            # Should raise an exception
            with pytest.raises(Exception):
                db_manager.truncate_table(mock_connection, 'test_table', 'test_schema')
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_row_count_success(self):
        """Test get_row_count success - covers lines 806-817"""
        mock_cursor = MockCursor()
        mock_cursor.fetchone_data = (1000,)  # Row count result
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        result = db_manager.get_row_count(mock_connection, 'test_table', 'test_schema')
        
        assert result == 1000
        
        # Should execute COUNT query
        count_queries = [q for q in mock_cursor.executed_queries if 'COUNT' in q[0]]
        assert len(count_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_row_count_exception(self):
        """Test get_row_count exception handling - covers lines 806-817"""
        mock_cursor = MockCursor(fail_on_execute=True)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        # Should raise an exception since there's no try/catch in get_row_count
        with pytest.raises(Exception):
            db_manager.get_row_count(mock_connection, 'test_table', 'test_schema')
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_sync_backup_table_schema_edge_cases(self):
        """Test schema sync edge cases - covers lines 456-474, 482, 486-487, 498"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        # Test case where there are errors in sync
        existing_columns = [{'name': 'id', 'data_type': 'int'}]
        expected_columns = [{'name': 'id', 'data_type': 'varchar', 'max_length': 50}]  # Incompatible change
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns), \
             patch.object(db_manager, '_is_safe_column_modification', return_value=False), \
             patch('builtins.print') as mock_print:
            
            result = db_manager._sync_backup_table_schema(mock_connection, 'test_table_backup', expected_columns)
            
            # Should fail due to unsafe modification
            assert result['success'] is False
            assert 'error' in result
            assert len(result['changes']['errors']) > 0