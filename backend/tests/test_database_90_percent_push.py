"""
Comprehensive database tests targeting 90% coverage
Focuses on missing lines identified in coverage analysis
"""

import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock
from datetime import datetime


class TestDatabaseManager90PercentPush:
    """Comprehensive tests to push database coverage to 90%"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'testserver',
        'db_name': 'testdb'
    })
    def test_is_safe_column_modification_varchar_widening(self):
        """Test _is_safe_column_modification with varchar widening - covers lines 520-538"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test varchar widening (safe)
        existing_col = {'data_type': 'varchar(50)', 'max_length': 50}
        expected_col = {'data_type': 'varchar', 'max_length': 100}
        
        result = db_manager._is_safe_column_modification(existing_col, expected_col)
        assert result is True
        
        # Test varchar narrowing (unsafe)
        existing_col = {'data_type': 'varchar(100)', 'max_length': 100}
        expected_col = {'data_type': 'varchar', 'max_length': 50}
        
        result = db_manager._is_safe_column_modification(existing_col, expected_col)
        assert result is False
        
        # Test varchar to varchar(max) (safe)
        existing_col = {'data_type': 'varchar(50)', 'max_length': 50}
        expected_col = {'data_type': 'varchar', 'max_length': -1}
        
        result = db_manager._is_safe_column_modification(existing_col, expected_col)
        assert result is True

    @patch.dict('os.environ', {
        'db_user': 'test_user', 
        'db_password': 'test_pass'
    })
    def test_normalize_data_type_varchar_with_regex(self):
        """Test _normalize_data_type varchar handling with regex - covers lines 588-593"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test varchar with length extraction via regex
        result = db_manager._normalize_data_type('varchar(50)', max_length=None)
        assert '50' in result.lower()
        
        # Test varchar with max
        result = db_manager._normalize_data_type('varchar(max)', max_length=None)
        assert 'max' in result.lower()
        
        # Test varchar without length (fallback)
        result = db_manager._normalize_data_type('varchar', max_length=None)
        assert '4000' in result

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_normalize_data_type_nvarchar_scenarios(self):
        """Test _normalize_data_type nvarchar handling - covers lines 597-608"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test nvarchar with max_length = -1
        result = db_manager._normalize_data_type('nvarchar', max_length=-1)
        assert 'nvarchar(max)' in result.lower()
        
        # Test nvarchar with specific length
        result = db_manager._normalize_data_type('nvarchar', max_length=100)
        assert 'nvarchar(100)' in result.lower()
        
        # Test nvarchar with regex extraction
        result = db_manager._normalize_data_type('nvarchar(200)', max_length=None)
        assert '200' in result.lower()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_insert_data_comprehensive(self, mock_pyodbc):
        """Test insert_data method comprehensively - covers lines 520-574"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test single row insertion
        data = [{'id': 1, 'name': 'John', 'age': 30}]
        
        result = db_manager.insert_data('test_table', data, 'test_schema')
        
        assert result['success'] is True
        assert result['rows_inserted'] == 1
        mock_cursor.executemany.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_insert_data_batch_processing(self, mock_pyodbc):
        """Test insert_data with large batches - covers batch processing lines"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Large dataset requiring batching
        data = [{'id': i, 'name': f'User{i}', 'value': f'data{i}'} for i in range(300)]
        
        result = db_manager.insert_data('test_table', data, 'test_schema', batch_size=100)
        
        assert result['success'] is True
        assert result['rows_inserted'] == 300
        # Should call executemany multiple times for batching
        assert mock_cursor.executemany.call_count >= 3

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_bulk_insert_dataframe(self, mock_pyodbc):
        """Test bulk_insert with DataFrame - covers lines 588-593"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
            'age': [25, 30, 35, 28, 32],
            'salary': [50000.0, 60000.0, 70000.0, 55000.0, 65000.0]
        })
        
        result = db_manager.bulk_insert('test_table', df, 'test_schema')
        
        assert result['success'] is True
        assert result['rows_inserted'] == 5
        mock_cursor.executemany.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_update_data_comprehensive(self, mock_pyodbc):
        """Test update_data method - covers lines 597-608"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        update_data = {
            'name': 'Updated Name',
            'age': 35,
            'salary': 75000.0,
            'status': 'active'
        }
        conditions = {
            'id': 1,
            'department': 'IT'
        }
        
        result = db_manager.update_data('test_table', update_data, conditions, 'test_schema')
        
        assert result['success'] is True
        mock_cursor.execute.assert_called()
        
        # Verify the SQL contains proper SET and WHERE clauses
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'UPDATE' in call_args
        assert 'SET' in call_args
        assert 'WHERE' in call_args

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_delete_data_comprehensive(self, mock_pyodbc):
        """Test delete_data method - covers lines 617-626"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        conditions = {
            'status': 'inactive',
            'last_login': '2023-01-01',
            'department': 'OLD_DEPT'
        }
        
        result = db_manager.delete_data('test_table', conditions, 'test_schema')
        
        assert result['success'] is True
        mock_cursor.execute.assert_called()
        
        # Verify proper DELETE SQL construction
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'DELETE FROM' in call_args
        assert 'WHERE' in call_args
        # Should have multiple AND conditions
        assert call_args.count('AND') >= 2

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_columns_comprehensive(self, mock_pyodbc):
        """Test sync_table_columns method - covers lines 863-954"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Mock existing columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'nullable': False},
            {'name': 'name', 'data_type': 'varchar(50)', 'nullable': True},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime', 'nullable': True}
        ]
        
        # New file columns to sync
        file_columns = [
            {'name': 'id', 'data_type': 'int', 'nullable': False},
            {'name': 'name', 'data_type': 'varchar(100)', 'nullable': True},
            {'name': 'email', 'data_type': 'varchar(255)', 'nullable': True},
            {'name': 'age', 'data_type': 'int', 'nullable': True}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_is_safe_column_modification', return_value=True):
                result = db_manager.sync_table_columns(mock_conn, 'test_table', file_columns)
                
                assert result['success'] is True
                assert len(result['actions']['added']) >= 1  # Should add new columns
                mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_advanced_data_type_normalization(self, mock_pyodbc):
        """Test advanced data type normalization scenarios"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test decimal with precision and scale
        result = db_manager._normalize_data_type('decimal', max_length=None, numeric_precision=10, numeric_scale=2)
        assert 'decimal(10,2)' in result.lower()
        
        # Test numeric with precision
        result = db_manager._normalize_data_type('numeric', max_length=None, numeric_precision=18, numeric_scale=4)
        assert 'numeric(18,4)' in result.lower()
        
        # Test float/real types
        result = db_manager._normalize_data_type('float', max_length=None)
        assert 'float' in result.lower()
        
        # Test datetime types
        result = db_manager._normalize_data_type('datetime', max_length=None)
        assert 'datetime' in result.lower()
        
        # Test bit type
        result = db_manager._normalize_data_type('bit', max_length=None)
        assert 'bit' in result.lower()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_connection_error_handling_comprehensive(self, mock_pyodbc):
        """Test comprehensive error handling in database operations"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Simulate database error during insert
        mock_cursor.executemany.side_effect = Exception("Database insert failed")
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        data = [{'id': 1, 'name': 'Test'}]
        result = db_manager.insert_data('test_table', data, 'test_schema')
        
        assert result['success'] is False
        assert 'error' in result
        assert result['rows_inserted'] == 0

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_edge_case_data_handling(self, mock_pyodbc):
        """Test edge cases in data handling"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test with None values and special characters
        data = [
            {'id': 1, 'name': None, 'description': 'Test with special chars: \'quotes\' "double" & symbols'},
            {'id': 2, 'name': '', 'description': 'Empty string test'},
            {'id': 3, 'name': 'Normal', 'description': None}
        ]
        
        result = db_manager.insert_data('test_table', data, 'test_schema')
        assert result['success'] is True
        assert result['rows_inserted'] == 3

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_column_modification_safety_edge_cases(self):
        """Test edge cases in column modification safety"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test non-varchar types (should return False for safety)
        existing_col = {'data_type': 'int', 'max_length': None}
        expected_col = {'data_type': 'bigint', 'max_length': None}
        
        result = db_manager._is_safe_column_modification(existing_col, expected_col)
        assert result is False
        
        # Test varchar without clear length patterns
        existing_col = {'data_type': 'varchar', 'max_length': 50}
        expected_col = {'data_type': 'text', 'max_length': None}
        
        result = db_manager._is_safe_column_modification(existing_col, expected_col)
        assert result is False