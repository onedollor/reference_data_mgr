"""
Final comprehensive database tests to push toward 90% coverage
Targeting the largest remaining uncovered ranges with sophisticated mocking
"""

import pytest
import os
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock, call
from datetime import datetime
import re


class TestDatabaseManagerFinal90Push:
    """Final comprehensive tests targeting 90% database coverage"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'testserver',
        'db_name': 'testdb'
    })
    @patch('utils.database.pyodbc')
    def test_sync_main_table_columns_comprehensive_workflow(self, mock_pyodbc):
        """Test sync_main_table_columns comprehensive workflow - covers lines 863-954"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        db_manager.data_schema = 'test_schema'
        
        # Mock existing table columns with various scenarios
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'name', 'data_type': 'varchar(50)', 'max_length': 50},
            {'name': 'email', 'data_type': 'varchar(100)', 'max_length': 100},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime', 'max_length': None},  # Should be filtered out
            {'name': 'ref_data_loadtype', 'data_type': 'varchar(10)', 'max_length': 10},  # Should be filtered out
            {'name': 'status', 'data_type': 'varchar(20)', 'max_length': 20}
        ]
        
        # File columns to sync - mix of new, matching, and mismatched
        file_columns = [
            {'name': 'id', 'data_type': 'int'},  # Exact match
            {'name': 'name', 'data_type': 'varchar(100)'},  # Should be widened (safe)
            {'name': 'email', 'data_type': 'varchar(100)'},  # Exact match  
            {'name': 'description', 'data_type': 'varchar(255)'},  # New column
            {'name': 'age', 'data_type': 'int'},  # New column
            {'name': 'status', 'data_type': 'int'}  # Type mismatch - should be preserved
        ]
        
        # Mock get_table_columns and normalize methods
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_normalize_data_type') as mock_normalize:
                # Configure normalize to return predictable values
                mock_normalize.side_effect = lambda dt, ml=None: dt
                
                result = db_manager.sync_main_table_columns(mock_conn, 'test_table', file_columns)
                
                # Should successfully process all columns
                assert result is not None
                assert isinstance(result, dict)
                
                # Should have added new columns
                assert len(result['added']) >= 2  # description, age
                
                # Should have skipped matching columns
                assert len(result['skipped']) >= 1
                
                # Should have detected mismatched columns
                assert len(result['mismatched']) >= 1
                
                # Should execute ALTER TABLE statements for new columns
                alter_calls = [call for call in mock_cursor.execute.call_args_list 
                             if call[0] and 'ALTER TABLE' in call[0][0] and 'ADD' in call[0][0]]
                assert len(alter_calls) >= 2
                
                # Should commit changes
                mock_conn.commit.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'testserver',
        'db_name': 'testdb'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_schema_comprehensive_workflow(self, mock_pyodbc):
        """Test sync_table_schema comprehensive workflow - covers lines 963-1056"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        db_manager.data_schema = 'test_schema'
        
        # Complex existing columns with various data types
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 50},
            {'name': 'description', 'data_type': 'varchar', 'max_length': -1},  # varchar(max)
            {'name': 'price', 'data_type': 'decimal', 'max_length': None},
            {'name': 'created_date', 'data_type': 'datetime', 'max_length': None},
            {'name': 'flag', 'data_type': 'bit', 'max_length': None}
        ]
        
        # Target columns for various conversion scenarios
        target_columns = [
            {'name': 'id', 'data_type': 'varchar(20)'},  # Convert int to varchar
            {'name': 'name', 'data_type': 'varchar(100)'},  # Widen varchar
            {'name': 'description', 'data_type': 'varchar(500)'},  # varchar(max) to smaller
            {'name': 'price', 'data_type': 'varchar(50)'},  # Convert decimal to varchar
            {'name': 'created_date', 'data_type': 'varchar(50)'},  # Convert datetime to varchar
            {'name': 'flag', 'data_type': 'varchar(10)'},  # Convert bit to varchar
            {'name': 'new_field', 'data_type': 'varchar(200)'}  # New column
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            result = db_manager.sync_table_schema(mock_conn, 'test_table', target_columns)
            
            # Should process all scenarios
            assert result is not None
            assert isinstance(result, dict)
            
            # Should have various action types
            assert 'added' in result
            assert 'widened' in result  
            assert 'skipped' in result
            
            # Should execute various ALTER TABLE statements
            execute_calls = mock_cursor.execute.call_args_list
            
            # Should have ADD calls for new columns
            add_calls = [call for call in execute_calls 
                        if call[0] and 'ALTER TABLE' in call[0][0] and 'ADD' in call[0][0]]
            assert len(add_calls) >= 1
            
            # Should have ALTER COLUMN calls for conversions and widening
            alter_calls = [call for call in execute_calls 
                          if call[0] and 'ALTER TABLE' in call[0][0] and 'ALTER COLUMN' in call[0][0]]
            assert len(alter_calls) >= 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_schema_varchar_widening_scenarios(self, mock_pyodbc):
        """Test specific varchar widening scenarios in sync_table_schema - covers lines 988-1055"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test varchar(50) -> varchar(100) widening
        existing_columns = [
            {'name': 'test_col', 'data_type': 'varchar', 'max_length': 50}
        ]
        target_columns = [
            {'name': 'test_col', 'data_type': 'varchar(100)'}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            result = db_manager.sync_table_schema(mock_conn, 'test_table', target_columns, 'test_schema')
            
            # Should widen the varchar column
            assert len(result['widened']) >= 1
            widened_col = result['widened'][0]
            assert widened_col['column'] == 'test_col'
            assert widened_col['from'] == 50
            assert widened_col['to'] == 100
            
            # Should execute ALTER COLUMN statement
            alter_calls = [call for call in mock_cursor.execute.call_args_list 
                          if call[0] and 'ALTER COLUMN' in call[0][0]]
            assert len(alter_calls) >= 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_schema_varchar_max_scenarios(self, mock_pyodbc):
        """Test varchar(max) scenarios in sync_table_schema"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test varchar(100) -> varchar(max) widening
        existing_columns = [
            {'name': 'test_col', 'data_type': 'varchar', 'max_length': 100}
        ]
        target_columns = [
            {'name': 'test_col', 'data_type': 'varchar(max)'}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            result = db_manager.sync_table_schema(mock_conn, 'test_table', target_columns, 'test_schema')
            
            # Should widen to varchar(max)
            assert len(result['widened']) >= 1
            widened_col = result['widened'][0]
            assert widened_col['column'] == 'test_col'
            assert widened_col['from'] == 100
            assert widened_col['to'] == 'MAX'

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_schema_type_conversions(self, mock_pyodbc):
        """Test data type conversions in sync_table_schema - covers lines 1015-1041"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test various type conversions to varchar
        conversion_scenarios = [
            # Datetime conversions
            ({'name': 'date_col', 'data_type': 'datetime', 'max_length': None}, 
             {'name': 'date_col', 'data_type': 'varchar(50)'}),
            ({'name': 'date2_col', 'data_type': 'datetime2', 'max_length': None}, 
             {'name': 'date2_col', 'data_type': 'varchar(50)'}),
            
            # Numeric conversions
            ({'name': 'int_col', 'data_type': 'int', 'max_length': None}, 
             {'name': 'int_col', 'data_type': 'varchar(20)'}),
            ({'name': 'decimal_col', 'data_type': 'decimal', 'max_length': None}, 
             {'name': 'decimal_col', 'data_type': 'varchar(30)'}),
        ]
        
        for existing_col, target_col in conversion_scenarios:
            # Reset mock for each scenario
            mock_cursor.reset_mock()
            
            with patch.object(db_manager, 'get_table_columns', return_value=[existing_col]):
                result = db_manager.sync_table_schema(mock_conn, 'test_table', [target_col], 'test_schema')
                
                # Should convert the column type
                assert len(result['widened']) >= 1
                conversion = result['widened'][0]
                assert conversion['column'] == existing_col['name']
                assert 'conversion' in conversion
                
                # Should execute ALTER COLUMN statement
                alter_calls = [call for call in mock_cursor.execute.call_args_list 
                              if call[0] and 'ALTER COLUMN' in call[0][0]]
                assert len(alter_calls) >= 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_table_schema_skip_scenarios(self, mock_pyodbc):
        """Test skip scenarios in sync_table_schema - covers lines 1048-1055"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test scenarios that should be skipped
        skip_scenarios = [
            # Same varchar length - no change needed
            ({'name': 'col1', 'data_type': 'varchar', 'max_length': 100}, 
             {'name': 'col1', 'data_type': 'varchar(100)'}),
            
            # Smaller varchar length - no narrowing
            ({'name': 'col2', 'data_type': 'varchar', 'max_length': 200}, 
             {'name': 'col2', 'data_type': 'varchar(100)'}),
            
            # varchar(max) to varchar(max) - no change
            ({'name': 'col3', 'data_type': 'varchar', 'max_length': -1}, 
             {'name': 'col3', 'data_type': 'varchar(max)'}),
        ]
        
        for existing_col, target_col in skip_scenarios:
            # Reset mock for each scenario
            mock_cursor.reset_mock()
            
            with patch.object(db_manager, 'get_table_columns', return_value=[existing_col]):
                result = db_manager.sync_table_schema(mock_conn, 'test_table', [target_col], 'test_schema')
                
                # Should skip the column
                assert len(result['skipped']) >= 1
                skipped = result['skipped'][0]
                assert skipped['column'] == existing_col['name']
                assert 'reason' in skipped
                
                # Should not execute ALTER COLUMN for skipped items
                alter_calls = [call for call in mock_cursor.execute.call_args_list 
                              if call[0] and 'ALTER COLUMN' in call[0][0]]
                assert len(alter_calls) == 0

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_sync_operations_with_regex_patterns(self, mock_pyodbc):
        """Test sync operations that use regex patterns - covers line 989 and related"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test regex pattern matching for varchar types
        existing_columns = [
            {'name': 'test_col', 'data_type': 'varchar', 'max_length': 50}
        ]
        
        # Various target formats that should trigger regex parsing
        target_formats = [
            'varchar(100)',  # Should match regex pattern
            'VARCHAR(200)',  # Case insensitive
            'varchar(max)',  # Special max case
            'VARCHAR(MAX)',  # Case insensitive max
        ]
        
        for target_format in target_formats:
            mock_cursor.reset_mock()
            
            target_columns = [{'name': 'test_col', 'data_type': target_format}]
            
            with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
                result = db_manager.sync_table_schema(mock_conn, 'test_table', target_columns, 'test_schema')
                
                # Should process the regex pattern correctly
                assert result is not None
                # Should either widen or skip depending on the target length
                assert len(result['widened']) + len(result['skipped']) >= 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_complex_error_scenarios_in_sync_operations(self, mock_pyodbc):
        """Test error handling in sync operations"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Test error during column addition
        existing_columns = [
            {'name': 'existing_col', 'data_type': 'int', 'max_length': None}
        ]
        
        file_columns = [
            {'name': 'existing_col', 'data_type': 'int'},
            {'name': 'new_col', 'data_type': 'varchar(100)'}  # This will cause error
        ]
        
        # Mock cursor to raise exception on ADD column
        def mock_execute(sql):
            if 'ADD' in sql and 'new_col' in sql:
                raise Exception("SQL execution failed for ADD")
            return None
            
        mock_cursor.execute.side_effect = mock_execute
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            # Should handle the error gracefully and continue
            result = db_manager.sync_main_table_columns(mock_conn, 'test_table', file_columns)
            
            # Should still return a result structure
            assert result is not None
            assert isinstance(result, dict)
            
            # Should have attempted to add the column (which failed)
            assert mock_cursor.execute.called

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_debug_logging_in_sync_operations(self, mock_pyodbc):
        """Test debug logging paths in sync operations - covers lines 970-972"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'name', 'data_type': 'varchar', 'max_length': 100}
        ]
        
        target_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar(200)'}
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch('builtins.print') as mock_print:
                result = db_manager.sync_table_schema(mock_conn, 'test_table', target_columns, 'test_schema')
                
                # Should have called print for debug logging
                debug_calls = [call for call in mock_print.call_args_list 
                              if call[0] and 'DEBUG:' in str(call[0][0])]
                assert len(debug_calls) >= 2  # Should log existing and target columns

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_summary_reporting_in_sync_main_columns(self, mock_pyodbc):
        """Test summary reporting in sync_main_table_columns - covers lines 943-954"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        existing_columns = [
            {'name': 'id', 'data_type': 'int', 'max_length': None},
            {'name': 'name', 'data_type': 'varchar(50)', 'max_length': 50},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime', 'max_length': None}
        ]
        
        file_columns = [
            {'name': 'id', 'data_type': 'int'},  # Match - should skip
            {'name': 'name', 'data_type': 'varchar(100)'},  # Different - should mismatch
            {'name': 'email', 'data_type': 'varchar(200)'},  # New - should add
        ]
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch.object(db_manager, '_normalize_data_type') as mock_normalize:
                # Configure normalize to create mismatch for name column
                def normalize_side_effect(data_type, max_length=None):
                    if 'varchar(50)' in str(data_type):
                        return 'varchar(50)'
                    elif 'varchar(100)' in str(data_type):
                        return 'varchar(100)'
                    return str(data_type)
                
                mock_normalize.side_effect = normalize_side_effect
                
                with patch('builtins.print') as mock_print:
                    result = db_manager.sync_main_table_columns(mock_conn, 'test_table', file_columns)
                    
                    # Should have various action types
                    assert len(result['added']) >= 1  # email
                    assert len(result['mismatched']) >= 1  # name 
                    assert len(result['skipped']) >= 1  # id
                    
                    # Should print summary with all action types
                    summary_calls = [call for call in mock_print.call_args_list 
                                   if call[0] and 'Main table column sync completed:' in str(call[0][0])]
                    assert len(summary_calls) >= 1
                    
                    # Summary should mention all action types
                    summary_text = str(summary_calls[0][0][0])
                    assert 'added' in summary_text
                    assert 'preserved' in summary_text
                    assert 'skipped' in summary_text