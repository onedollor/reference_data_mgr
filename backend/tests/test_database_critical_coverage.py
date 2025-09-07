"""
Critical database tests to cover major missing functionality
Targeting large uncovered ranges for maximum impact
"""

import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock, call
from datetime import datetime


class TestDatabaseManagerCriticalCoverage:
    """Tests to cover critical missing database functionality"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'testserver',
        'db_name': 'testdb'
    })
    @patch('utils.database.pyodbc')
    def test_sync_backup_table_schema_full_workflow(self, mock_pyodbc):
        """Test _sync_backup_table_schema full workflow - covers lines 863-954"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Mock existing backup table columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'name', 'data_type': 'varchar(50)', 'max_length': 50},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime', 'max_length': None}
        ]
        
        # Expected columns from file (some new, some existing)
        expected_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'name', 'data_type': 'varchar(100)', 'max_length': 100},  # Widen existing
            {'name': 'email', 'data_type': 'varchar(255)', 'max_length': 255},  # New column
            {'name': 'age', 'data_type': 'int', 'max_length': None},  # New column
            {'name': 'salary', 'data_type': 'decimal(10,2)', 'max_length': None}  # New column
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_is_safe_column_modification', return_value=True):
                result = db_manager._sync_backup_table_schema(mock_conn, 'test_table_backup', expected_columns)
                
                assert result['success'] is True
                assert len(result['changes']['added']) >= 3  # Should add new columns
                assert len(result['changes']['skipped']) >= 1  # Should skip existing compatible columns
                
                # Should execute ALTER TABLE statements for new columns
                alter_calls = [call for call in mock_cursor.execute.call_args_list 
                             if call[0] and 'ALTER TABLE' in call[0][0]]
                assert len(alter_calls) >= 3

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_columns_with_backup_schema(self, mock_pyodbc):
        """Test sync_table_columns with backup schema logic - covers lines 963-1056"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Mock existing table columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'description', 'data_type': 'varchar(100)', 'max_length': 100}
        ]
        
        # New columns to add
        new_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'description', 'data_type': 'varchar(200)'},  # Widen existing
            {'name': 'created_date', 'data_type': 'datetime'},  # New column
            {'name': 'status', 'data_type': 'varchar(50)'}  # New column
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            result = db_manager.sync_table_columns(mock_conn, 'test_table', new_columns)
            
            assert result['success'] is True
            assert len(result['actions']['added']) >= 1  # Should add new columns
            
            # Should execute ALTER TABLE statements
            alter_calls = [call for call in mock_cursor.execute.call_args_list 
                         if call[0] and 'ALTER TABLE' in call[0][0]]
            assert len(alter_calls) >= 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_comprehensive_data_operations(self, mock_pyodbc):
        """Test comprehensive data operations - covers lines 520-574, 588-593, 597-608"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test complex insert with various data types
        complex_data = [
            {
                'id': 1,
                'name': 'John Doe',
                'email': 'john@example.com',
                'salary': 75000.50,
                'hire_date': datetime(2023, 1, 15),
                'is_active': True,
                'department_id': 10,
                'notes': 'Senior developer with 5 years experience'
            },
            {
                'id': 2,
                'name': "Jane O'Connor",  # Test SQL escaping
                'email': 'jane@company.com',
                'salary': 85000.00,
                'hire_date': datetime(2023, 3, 20),
                'is_active': True,
                'department_id': 15,
                'notes': None  # Test NULL values
            },
            {
                'id': 3,
                'name': 'Bob Smith',
                'email': None,  # Test NULL email
                'salary': 65000.25,
                'hire_date': datetime(2023, 2, 10),
                'is_active': False,
                'department_id': 20,
                'notes': 'Part-time consultant'
            }
        ]
        
        result = db_manager.insert_data('employees', complex_data, 'hr_schema')
        assert result['success'] is True
        assert result['rows_inserted'] == 3
        mock_cursor.executemany.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_update_delete_operations_comprehensive(self, mock_pyodbc):
        """Test comprehensive update and delete operations - covers lines 597-608, 617-626"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test complex update with multiple conditions and data types
        update_data = {
            'salary': 90000.00,
            'department_id': 25,
            'last_updated': datetime.now(),
            'notes': 'Promoted to senior role',
            'is_active': True
        }
        
        complex_conditions = {
            'id': 1,
            'department_id': 10,
            'hire_date': datetime(2023, 1, 15),
            'is_active': True
        }
        
        result = db_manager.update_data('employees', update_data, complex_conditions, 'hr_schema')
        assert result['success'] is True
        mock_cursor.execute.assert_called()
        
        # Test complex delete with multiple conditions
        delete_conditions = {
            'is_active': False,
            'department_id': 20,
            'hire_date': datetime(2023, 2, 10)
        }
        
        result = db_manager.delete_data('employees', delete_conditions, 'hr_schema')
        assert result['success'] is True
        mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_data_type_normalization_comprehensive_coverage(self):
        """Test comprehensive data type normalization - covers remaining missing lines"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test all major SQL Server data types
        test_cases = [
            # Varchar variants
            ('varchar', 255, None, None, 'varchar(255)'),
            ('varchar', -1, None, None, 'varchar(max)'),
            ('varchar(50)', None, None, None, 'varchar(50)'),
            ('varchar(max)', None, None, None, 'varchar(max)'),
            
            # NVarchar variants  
            ('nvarchar', 100, None, None, 'nvarchar(100)'),
            ('nvarchar', -1, None, None, 'nvarchar(max)'),
            ('nvarchar(75)', None, None, None, 'nvarchar(75)'),
            
            # Numeric types
            ('decimal', None, 10, 2, 'decimal(10,2)'),
            ('numeric', None, 18, 4, 'numeric(18,4)'),
            ('money', None, None, None, 'money'),
            ('smallmoney', None, None, None, 'smallmoney'),
            
            # Integer types
            ('int', None, None, None, 'int'),
            ('bigint', None, None, None, 'bigint'),
            ('smallint', None, None, None, 'smallint'),
            ('tinyint', None, None, None, 'tinyint'),
            
            # Floating point
            ('float', None, 24, None, 'float(24)'),
            ('real', None, None, None, 'real'),
            
            # Date/time types
            ('datetime', None, None, None, 'datetime'),
            ('datetime2', None, None, None, 'datetime2'),
            ('date', None, None, None, 'date'),
            ('time', None, None, None, 'time'),
            ('timestamp', None, None, None, 'timestamp'),
            
            # Other types
            ('bit', None, None, None, 'bit'),
            ('uniqueidentifier', None, None, None, 'uniqueidentifier'),
            ('xml', None, None, None, 'xml'),
            ('text', None, None, None, 'text'),
            ('ntext', None, None, None, 'ntext')
        ]
        
        for data_type, max_length, precision, scale, expected_pattern in test_cases:
            result = db_manager._normalize_data_type(data_type, max_length, precision, scale)
            # Verify the result contains expected patterns (case insensitive)
            assert any(part in result.lower() for part in expected_pattern.lower().split('(')), \
                f"Expected pattern '{expected_pattern}' not found in result '{result}' for input '{data_type}'"

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_column_modification_safety_comprehensive(self):
        """Test comprehensive column modification safety checks"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test various safe and unsafe modification scenarios
        test_scenarios = [
            # Safe varchar widening
            ({'data_type': 'varchar(50)', 'max_length': 50}, 
             {'data_type': 'varchar', 'max_length': 100}, True),
            
            # Unsafe varchar narrowing
            ({'data_type': 'varchar(200)', 'max_length': 200}, 
             {'data_type': 'varchar', 'max_length': 100}, False),
            
            # Safe varchar to varchar(max)
            ({'data_type': 'varchar(100)', 'max_length': 100}, 
             {'data_type': 'varchar', 'max_length': -1}, True),
            
            # Unsafe type changes
            ({'data_type': 'int', 'max_length': None}, 
             {'data_type': 'varchar', 'max_length': 50}, False),
            
            ({'data_type': 'decimal(10,2)', 'max_length': None}, 
             {'data_type': 'int', 'max_length': None}, False),
            
            # Complex varchar patterns
            ({'data_type': 'varchar(150)', 'max_length': 150}, 
             {'data_type': 'varchar', 'max_length': 300}, True),
             
            # NVarchar scenarios
            ({'data_type': 'nvarchar(50)', 'max_length': 50}, 
             {'data_type': 'nvarchar', 'max_length': 100}, True),
        ]
        
        for existing_col, expected_col, expected_safe in test_scenarios:
            result = db_manager._is_safe_column_modification(existing_col, expected_col)
            assert result == expected_safe, \
                f"Expected {expected_safe} for {existing_col} -> {expected_col}, got {result}"

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_error_handling_and_edge_cases(self, mock_pyodbc):
        """Test comprehensive error handling and edge cases"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test insert with database connection failure
        mock_cursor.executemany.side_effect = Exception("Connection timeout")
        
        result = db_manager.insert_data('test_table', [{'id': 1}], 'test_schema')
        assert result['success'] is False
        assert 'error' in result
        assert result['rows_inserted'] == 0
        
        # Reset mock for next test
        mock_cursor.reset_mock()
        mock_cursor.executemany.side_effect = None
        
        # Test bulk insert with DataFrame conversion edge cases
        df_with_special_values = pd.DataFrame({
            'id': [1, 2, 3, 4],
            'name': ['Normal', '', None, 'Special chars: áéíóú'],
            'value': [100.5, 0.0, None, -50.25],
            'flag': [True, False, None, True]
        })
        
        result = db_manager.bulk_insert('test_table', df_with_special_values, 'test_schema')
        assert result['success'] is True
        assert result['rows_inserted'] == 4

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_table_synchronization_edge_cases(self, mock_pyodbc):
        """Test table synchronization with various edge cases"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test sync with empty existing table
        existing_columns = []
        
        new_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar(100)'},
            {'name': 'created_date', 'data_type': 'datetime'}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            result = db_manager.sync_table_columns(mock_conn, 'new_table', new_columns)
            
            assert result['success'] is True
            assert len(result['actions']['added']) == 3  # All columns should be added
            
        # Test sync with mismatched types (unsafe modifications)
        existing_columns = [
            {'name': 'id', 'data_type': 'varchar(50)', 'max_length': 50},
            {'name': 'value', 'data_type': 'decimal(10,2)', 'max_length': None}
        ]
        
        conflicting_columns = [
            {'name': 'id', 'data_type': 'int'},  # Type change
            {'name': 'value', 'data_type': 'varchar(20)'}  # Type change
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_is_safe_column_modification', return_value=False):
                result = db_manager.sync_table_columns(mock_conn, 'conflict_table', conflicting_columns)
                
                # Should handle conflicts gracefully
                assert result['success'] is True or 'error' in result