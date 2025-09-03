"""
Final push tests to get utils/database.py to 50%+ coverage
Targeting remaining high-impact missing lines for comprehensive database testing
"""

import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock, PropertyMock, call, mock_open
from datetime import datetime
import pandas as pd

# Mock pyodbc before importing database module
import sys
mock_pyodbc = MagicMock()
sys.modules['pyodbc'] = mock_pyodbc

from utils.database import DatabaseManager


class MockConnection:
    """Enhanced mock database connection with full transaction support"""
    
    def __init__(self, simulate_error=False, fail_on_execute=False, autocommit=True):
        self.simulate_error = simulate_error
        self.fail_on_execute = fail_on_execute
        self.autocommit = autocommit
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
    """Enhanced mock database cursor with comprehensive execute tracking"""
    
    def __init__(self, fail_on_execute=False, return_data=None, custom_responses=None):
        self.fail_on_execute = fail_on_execute
        self.return_data = return_data or []
        self.custom_responses = custom_responses or {}
        self.executed_queries = []
        self.fetchall_data = []
        self.fetchone_data = None
        
    def execute(self, query, *params):
        if self.fail_on_execute:
            raise Exception("Execute failed")
        self.executed_queries.append((query, params))
        
    def fetchall(self):
        # Handle custom responses for specific queries
        for key, response in self.custom_responses.items():
            if key in str(self.executed_queries[-1][0]) if self.executed_queries else "":
                return response
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


class TestDatabaseManagerFinalPush:
    """Final comprehensive tests to push DatabaseManager coverage to 50%+"""
    
    def test_init_with_missing_env_vars(self):
        """Test initialization with missing environment variables - covers lines 37, 39"""
        # Test with only max_pool_size set, missing credentials will cause ValueError
        with patch.dict('os.environ', {'max_pool_size': '10'}, clear=True):
            with patch('builtins.print') as mock_print:
                # Should raise error for missing credentials
                with pytest.raises(ValueError, match="Database username.*environment variable is required"):
                    db_manager = DatabaseManager()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_pooled_connection_edge_case(self):
        """Test pooled connection when no connections available - covers lines 107-108, 121-122"""
        db_manager = DatabaseManager()
        db_manager.connection_pool = []  # Empty pool
        db_manager.pool_size = 2
        
        with patch.object(db_manager, '_create_connection', return_value=MockConnection()) as mock_create, \
             patch('threading.Lock') as mock_lock:
            
            result = db_manager.get_pooled_connection()
            assert result is not None
            mock_create.assert_called_once()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_ensure_metadata_columns_success(self):
        """Test ensure_metadata_columns success - covers lines 819-847"""
        mock_cursor = MockCursor(custom_responses={
            'COLUMN_NAME': [],  # No existing metadata columns
        })
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.ensure_metadata_columns(mock_connection, 'test_table', 'test_schema')
            
            assert result['success'] is True
            
            # Should check for existing columns and add missing ones
            info_queries = [q for q in mock_cursor.executed_queries if 'INFORMATION_SCHEMA' in q[0]]
            assert len(info_queries) >= 1
            
            # Should add metadata columns
            alter_queries = [q for q in mock_cursor.executed_queries if 'ALTER TABLE' in q[0] and 'ADD' in q[0]]
            assert len(alter_queries) >= 3  # Should add 3 metadata columns
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_ensure_metadata_columns_already_exist(self):
        """Test ensure_metadata_columns when columns already exist - covers lines 819-847"""
        existing_columns = [
            ('ref_data_loadtime',), 
            ('ref_data_loadtype',),
            ('ref_data_version_id',)
        ]
        mock_cursor = MockCursor(return_data=existing_columns)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.ensure_metadata_columns(mock_connection, 'test_table', 'test_schema')
            
            assert result['success'] is True
            
            # Should check for existing columns but not add any
            info_queries = [q for q in mock_cursor.executed_queries if 'INFORMATION_SCHEMA' in q[0]]
            assert len(info_queries) >= 1
            
            # Should not add columns since they exist
            alter_queries = [q for q in mock_cursor.executed_queries if 'ALTER TABLE' in q[0] and 'ADD' in q[0]]
            assert len(alter_queries) == 0
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_create_validation_procedure(self):
        """Test create_validation_procedure - covers lines 863-954"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            db_manager.create_validation_procedure(mock_connection, 'test_table')
            
            # Should create stored procedure
            proc_queries = [q for q in mock_cursor.executed_queries if 'CREATE PROCEDURE' in q[0]]
            assert len(proc_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_validate_data_integrity_success(self):
        """Test validate_data_integrity success - covers lines 963-1056"""
        # Mock validation results
        validation_results = [(0, 'No duplicates found', None)]
        mock_cursor = MockCursor(return_data=validation_results)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.validate_data_integrity('test_table', 'test_schema')
            
            assert result['success'] is True
            assert result['issues_found'] == 0
            
            # Should execute validation procedure
            proc_exec = [q for q in mock_cursor.executed_queries if 'EXEC' in q[0] or 'EXECUTE' in q[0]]
            assert len(proc_exec) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_validate_data_integrity_with_issues(self):
        """Test validate_data_integrity with data issues - covers lines 963-1056"""
        # Mock validation results with issues
        validation_results = [
            (1, 'Duplicate key found', 'id=123'),
            (2, 'Invalid date format', 'date_field=invalid')
        ]
        mock_cursor = MockCursor(return_data=validation_results)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.validate_data_integrity('test_table', 'test_schema')
            
            assert result['success'] is True  # Success even with issues found
            assert result['issues_found'] == 2
            assert len(result['issues']) == 2
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_backup_data_to_file_success(self):
        """Test backup_data_to_file success - covers lines 1061-1098"""
        # Mock data to backup
        backup_data = [
            ('John', 30, '2023-01-01'),
            ('Jane', 25, '2023-01-02')
        ]
        mock_cursor = MockCursor(return_data=backup_data)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.open', mock_open()) as mock_file, \
             patch('os.makedirs') as mock_makedirs, \
             patch('os.path.exists', return_value=False), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.backup_data_to_file('test_table', '/tmp/backup.csv', 'test_schema')
            
            assert result['success'] is True
            assert result['rows_backed_up'] == 2
            
            # Should execute SELECT query
            select_queries = [q for q in mock_cursor.executed_queries if 'SELECT' in q[0]]
            assert len(select_queries) >= 1
            
            # Should create backup directory
            mock_makedirs.assert_called_once()
            
            # Should write to file
            mock_file.assert_called_once()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_backup_data_to_file_exception(self):
        """Test backup_data_to_file exception handling - covers lines 1061-1098"""
        mock_cursor = MockCursor(fail_on_execute=True)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.backup_data_to_file('test_table', '/tmp/backup.csv', 'test_schema')
            
            assert result['success'] is False
            assert 'error' in result
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'}) 
    def test_restore_data_from_file_success(self):
        """Test restore_data_from_file success - covers lines 1102-1134"""
        # Mock CSV data
        csv_data = "name,age,date\nJohn,30,2023-01-01\nJane,25,2023-01-02"
        
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.open', mock_open(read_data=csv_data)), \
             patch('pandas.read_csv') as mock_read_csv, \
             patch.object(db_manager, 'bulk_insert', return_value={'success': True, 'rows_inserted': 2}), \
             patch('builtins.print') as mock_print:
            
            # Mock pandas DataFrame
            mock_df = MagicMock()
            mock_df.__len__ = MagicMock(return_value=2)
            mock_read_csv.return_value = mock_df
            
            result = db_manager.restore_data_from_file('/tmp/backup.csv', 'test_table', 'test_schema')
            
            assert result['success'] is True
            assert result['rows_restored'] == 2
            
            # Should read CSV file
            mock_read_csv.assert_called_once()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_get_database_stats_success(self):
        """Test get_database_stats success - covers lines 1148-1195"""
        # Mock database statistics
        stats_data = [
            ('test_table', 1000, 5242880),  # table_name, row_count, size_in_bytes
            ('another_table', 500, 2621440)
        ]
        mock_cursor = MockCursor(return_data=stats_data)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.get_database_stats('test_schema')
            
            assert result['success'] is True
            assert 'tables' in result
            assert len(result['tables']) == 2
            assert result['total_tables'] == 2
            assert result['total_rows'] == 1500
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_analyze_table_structure_success(self):
        """Test analyze_table_structure success - covers lines 1200-1239"""
        # Mock table structure data
        structure_data = [
            ('id', 'int', 'NO', 'PRI', None, ''),
            ('name', 'varchar', 'YES', '', None, ''),
            ('created_date', 'datetime', 'YES', '', None, '')
        ]
        mock_cursor = MockCursor(return_data=structure_data)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.analyze_table_structure('test_table', 'test_schema')
            
            assert result['success'] is True
            assert 'structure' in result
            assert len(result['structure']) == 3
            assert result['column_count'] == 3
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_optimize_table_indexes_success(self):
        """Test optimize_table_indexes success - covers lines 1243-1253"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.optimize_table_indexes('test_table', 'test_schema')
            
            assert result['success'] is True
            
            # Should execute index optimization commands
            reindex_queries = [q for q in mock_cursor.executed_queries if 'REINDEX' in q[0] or 'REBUILD' in q[0]]
            assert len(reindex_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_cleanup_old_data_success(self):
        """Test cleanup_old_data success - covers lines 1259-1300"""
        mock_cursor = MockCursor()
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.cleanup_old_data('test_table', 'created_date', 30, 'test_schema')
            
            assert result['success'] is True
            
            # Should execute DELETE with date condition
            delete_queries = [q for q in mock_cursor.executed_queries if 'DELETE' in q[0] and 'DATEADD' in q[0]]
            assert len(delete_queries) >= 1
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_export_table_structure_success(self):
        """Test export_table_structure success - covers lines 1305-1357"""
        # Mock table structure for export
        table_structure = [
            ('id', 'int', 'NO', 'PRI', None, ''),
            ('name', 'varchar(255)', 'YES', '', None, ''),
        ]
        mock_cursor = MockCursor(return_data=table_structure)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.open', mock_open()) as mock_file, \
             patch('json.dump') as mock_json_dump, \
             patch('builtins.print') as mock_print:
            
            result = db_manager.export_table_structure('test_table', '/tmp/structure.json', 'test_schema')
            
            assert result['success'] is True
            
            # Should query table structure
            info_queries = [q for q in mock_cursor.executed_queries if 'INFORMATION_SCHEMA' in q[0]]
            assert len(info_queries) >= 1
            
            # Should write to file
            mock_file.assert_called_once()
            mock_json_dump.assert_called_once()
    
    @patch.dict('os.environ', {'db_user': 'test_user', 'db_password': 'test_pass'})
    def test_monitor_table_changes_success(self):
        """Test monitor_table_changes success - covers lines 1361-1440"""
        # Mock table change monitoring data
        changes_data = [
            ('INSERT', 'John Doe', '2023-01-01 10:00:00'),
            ('UPDATE', 'Jane Smith', '2023-01-01 11:00:00'),
            ('DELETE', 'Bob Johnson', '2023-01-01 12:00:00')
        ]
        mock_cursor = MockCursor(return_data=changes_data)
        mock_connection = MockConnection()
        mock_connection.cursor = MagicMock(return_value=mock_cursor)
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_connection', return_value=mock_connection), \
             patch.object(db_manager, 'release_connection'), \
             patch('builtins.print') as mock_print:
            
            result = db_manager.monitor_table_changes('test_table', '2023-01-01', 'test_schema')
            
            assert result['success'] is True
            assert 'changes' in result
            assert len(result['changes']) == 3
            assert result['total_changes'] == 3
            
            # Should query change tracking table or logs
            change_queries = [q for q in mock_cursor.executed_queries if 'SELECT' in q[0]]
            assert len(change_queries) >= 1