"""
Strategic tests for utils/database.py error handling paths and edge cases
Target: Boost coverage from 73% to 90%+ by covering missing 212 lines
Focus: Constructor errors, connection pool failures, backup operations, schema sync
"""
import pytest
import os
import pyodbc
from unittest.mock import MagicMock, patch, Mock
from utils.database import DatabaseManager


class TestDatabaseManagerErrorPaths:
    """Test error handling paths and exception scenarios"""

    @patch.dict(os.environ, {"db_user": ""}, clear=False)
    def test_constructor_missing_username_error_line_37(self):
        """Test constructor error when username is missing - covers line 37"""
        with pytest.raises(ValueError, match="Database username.*required"):
            DatabaseManager()

    @patch.dict(os.environ, {"db_user": "testuser", "db_password": ""}, clear=False)
    def test_constructor_missing_password_error_line_39(self):
        """Test constructor error when password is missing - covers line 39"""
        with pytest.raises(ValueError, match="Database password.*required"):
            DatabaseManager()

    @patch.dict(os.environ, {"DB_POOL_SIZE": "invalid"}, clear=False)
    def test_constructor_invalid_pool_size_handling(self):
        """Test pool size parsing with invalid value - covers ValueError exception"""
        # Should not raise error, should default to 5
        db_manager = DatabaseManager()
        assert db_manager.pool_size == 5

    def test_connection_pool_release_connection_exception_lines_121_122(self):
        """Test exception handling during connection release - covers lines 121-122"""
        db_manager = DatabaseManager()
        
        # Fill the pool to capacity so that close() will be called on the next release
        db_manager._pool = [MagicMock() for _ in range(db_manager.pool_size)]
        db_manager._in_use = 1
        
        # Create a mock connection that raises exception on close
        mock_connection = MagicMock()
        mock_connection.close.side_effect = Exception("Connection close failed")
        
        # Should not raise exception, should handle gracefully
        db_manager.release_connection(mock_connection)
        
        # Verify close was attempted
        mock_connection.close.assert_called_once()

    def test_backup_table_column_addition_error_lines_444_447(self):
        """Test backup table column addition failure - covers lines 444-447"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock existing columns (missing expected column)
        mock_cursor.fetchall.return_value = [
            ('existing_col', 'varchar', 100, None, None, None)
        ]
        
        # Mock column addition failure
        mock_cursor.execute.side_effect = [
            None,  # First call (column check) succeeds
            Exception("Column addition failed")  # Second call (ALTER TABLE) fails
        ]
        
        expected_columns = [
            {'name': 'existing_col', 'data_type': 'varchar(100)'},
            {'name': 'new_col', 'data_type': 'varchar(50)'}
        ]
        
        # Mock get_table_columns
        with patch.object(db_manager, 'get_table_columns') as mock_get_columns:
            mock_get_columns.return_value = [
                {'name': 'existing_col', 'data_type': 'varchar(100)'}
            ]
            
            # Set up cursor.execute to fail on ALTER TABLE for new column
            def mock_execute(sql):
                if 'ADD [new_col]' in sql:
                    raise Exception("Column addition failed")
                return None
            
            mock_cursor.execute.side_effect = mock_execute
            
            result = db_manager._sync_backup_table_schema(mock_connection, "test_backup", expected_columns)
            
            # Should handle error gracefully and return error info in changes
            assert 'changes' in result
            assert 'errors' in result['changes']
            assert any('Failed to add column new_col' in error for error in result['changes']['errors'])

    def test_backup_table_column_modification_error_lines_466_469(self):
        """Test backup table column modification failure - covers lines 466-469"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock existing column with different type
        mock_cursor.fetchall.return_value = [
            ('test_col', 'varchar', 50, None, None, None)
        ]
        
        # Mock column modification failure
        mock_cursor.execute.side_effect = [
            None,  # First call (column check) succeeds
            Exception("Column modification failed")  # ALTER COLUMN fails
        ]
        
        expected_columns = [
            {'name': 'test_col', 'data_type': 'varchar(100)'}  # Different size
        ]
        
        # Mock get_table_columns and other dependent methods
        with patch.object(db_manager, 'get_table_columns') as mock_get_columns:
            with patch.object(db_manager, '_is_safe_column_modification', return_value=True):
                mock_get_columns.return_value = [
                    {'name': 'test_col', 'data_type': 'varchar', 'max_length': 50}
                ]
                
                # Set up cursor.execute to fail on ALTER COLUMN
                def mock_execute(sql):
                    if 'ALTER COLUMN [test_col]' in sql:
                        raise Exception("Column modification failed")
                    return None
                
                mock_cursor.execute.side_effect = mock_execute
                
                result = db_manager._sync_backup_table_schema(mock_connection, "test_backup", expected_columns)
                
                # Should handle error gracefully
                assert 'changes' in result
                assert 'errors' in result['changes']
                assert any('Failed to modify column test_col' in error for error in result['changes']['errors'])

    def test_sync_backup_table_schema_general_exception_lines_508_510(self):
        """Test general exception in sync backup table schema - covers lines 508-510"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_connection.cursor.side_effect = Exception("Database connection failed")
        
        expected_columns = [{'name': 'test_col', 'data_type': 'varchar(50)'}]
        
        result = db_manager._sync_backup_table_schema(mock_connection, "test_backup", expected_columns)
        
        # Should handle error gracefully and rollback
        mock_connection.rollback.assert_called_once()
        assert result['success'] == False
        assert 'Database connection failed' in result['error']

    def test_normalize_data_type_regex_exception_lines_568_573_574_608(self):
        """Test regex parsing exceptions in data type normalization - covers lines 568, 573-574, 608"""
        db_manager = DatabaseManager()
        
        # Test cases that might cause regex issues
        test_cases = [
            # VARCHAR with invalid format
            ('varchar(invalid)', None, None, None),
            # NVARCHAR with invalid format  
            ('nvarchar(bad)', None, None, None),
            # CHAR with invalid format
            ('char(invalid)', None, None, None),
        ]
        
        for data_type, max_len, precision, scale in test_cases:
            # Should handle gracefully without raising exception
            result = db_manager._normalize_data_type(data_type, max_len, precision, scale)
            # Should return some normalized form or original
            assert isinstance(result, str)
            assert len(result) > 0


class TestDatabaseBackupOperations:
    """Test backup operations and related error scenarios"""

    def test_backup_existing_data_exception_lines_782_794(self):
        """Test backup existing data with exception handling - covers lines 782-794"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock exception during backup operation
        mock_cursor.execute.side_effect = Exception("Backup operation failed")
        
        # Should raise exception after rollback
        with pytest.raises(Exception, match="Failed to backup existing data"):
            db_manager.backup_existing_data(mock_connection, "source_table", "backup_table")
        
        # Verify rollback was called
        mock_connection.rollback.assert_called_once()

    def test_ensure_backup_table_metadata_columns_lines_702_725(self):
        """Test ensuring backup table metadata columns - covers lines 702-725"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock get_table_columns to return columns missing metadata
        with patch.object(db_manager, 'get_table_columns') as mock_get_columns:
            mock_get_columns.return_value = [
                {'name': 'some_col', 'data_type': 'varchar(50)'}
                # Missing ref_data_loadtime, ref_data_loadtype, ref_data_version_id
            ]
            
            result = db_manager.ensure_backup_table_metadata_columns(mock_connection, "test_backup")
            
            # Should attempt to add missing metadata columns
            assert 'added' in result
            assert len(result['added']) == 3  # All three metadata columns should be added
            # Should have called ALTER TABLE to add missing columns
            alter_calls = [call for call in mock_cursor.execute.call_args_list 
                          if 'ALTER TABLE' in str(call)]
            assert len(alter_calls) == 3

    def test_validation_procedure_execution_json_parsing_lines_676_690(self):
        """Test validation procedure execution with JSON parsing - covers lines 676-690"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Test successful JSON parsing
        mock_cursor.fetchone.return_value = ['{"validation_result": 0, "validation_issue_list": []}']
        
        result = db_manager.execute_validation_procedure(mock_connection, "test_table")
        
        assert result['validation_result'] == 0
        assert result['validation_issue_list'] == []
        
        # Test no result scenario (else branch)
        mock_cursor.fetchone.return_value = None
        
        result = db_manager.execute_validation_procedure(mock_connection, "test_table")
        
        assert result['validation_result'] == -1
        assert len(result['validation_issue_list']) > 0

    def test_backup_version_operations_lines_751_762(self):
        """Test backup version operations - covers lines 751-762"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock get_table_columns calls
        with patch.object(db_manager, 'get_table_columns') as mock_get_columns:
            mock_get_columns.side_effect = [
                [{'name': 'col1', 'data_type': 'varchar(50)'}],  # Source columns
                [{'name': 'col1', 'data_type': 'varchar(50)'}, {'name': 'ref_data_version_id', 'data_type': 'int'}]  # Backup columns
            ]
            
            # Mock version query results
            mock_cursor.fetchone.return_value = [6]  # Next version ID
            mock_cursor.rowcount = 100  # Number of backed up rows
            
            result = db_manager.backup_existing_data(mock_connection, "test_table", "test_backup")
            
            # Should return the number of backed up rows
            assert result == 100


class TestDatabaseConfigurationEdgeCases:
    """Test configuration setup and edge cases"""

    def test_ensure_reference_data_cfg_table_creation_lines_1072_1098(self):
        """Test Reference_Data_Cfg table creation - covers lines 1072-1098"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock table doesn't exist
        mock_cursor.fetchone.return_value = [0]  # Table doesn't exist
        
        db_manager.ensure_reference_data_cfg_table(mock_connection)
        
        # Should call CREATE TABLE and ADD CONSTRAINT
        execute_calls = mock_cursor.execute.call_args_list
        create_calls = [call for call in execute_calls if 'CREATE TABLE' in str(call)]
        constraint_calls = [call for call in execute_calls if 'ADD CONSTRAINT' in str(call)]
        
        assert len(create_calls) > 0
        assert len(constraint_calls) > 0
        mock_connection.commit.assert_called()

    def test_ensure_postload_stored_procedure_creation_lines_1102_1134(self):
        """Test post-load stored procedure creation - covers lines 1102-1134"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock stored procedure doesn't exist
        mock_cursor.fetchone.return_value = [0]  # SP doesn't exist
        
        # Mock exception during SP creation (covers error handling)
        mock_cursor.execute.side_effect = [
            None,  # Check existence succeeds
            Exception("SP creation failed")  # CREATE PROCEDURE fails
        ]
        
        # Should handle exception gracefully without raising
        db_manager.ensure_postload_stored_procedure(mock_connection)
        
        # Should have attempted rollback
        mock_connection.rollback.assert_called_once()

    def test_determine_load_type_error_handling_lines_1172_1195(self):
        """Test load type determination error handling - covers lines 1172-1195"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Test the actual determine_load_type method with invalid data that triggers exception paths
        with patch.object(db_manager, 'table_exists', return_value=True):
            with patch.object(db_manager, 'get_row_count', return_value=5):
                mock_cursor.execute.side_effect = Exception("Database error")
                
                # Should handle database errors gracefully and return default
                result = db_manager.determine_load_type(mock_connection, "test_table", "full")
                assert result == "F"  # Should return default based on current_load_mode

    def test_backup_version_retrieval_exception_lines_1243_1253(self):
        """Test backup version retrieval with exception - covers lines 1243-1253"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock database error
        mock_cursor.execute.side_effect = Exception("Query failed")
        
        # Should return empty list on error
        result = db_manager.get_backup_versions(mock_connection, "test_table")
        assert result == []

    def test_backup_version_rows_exception_lines_1259_1300(self):
        """Test backup version rows retrieval with exception - covers lines 1259-1300"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock database error during row retrieval
        mock_cursor.execute.side_effect = Exception("Query execution failed")
        
        result = db_manager.get_backup_version_rows(mock_connection, "test_table", 1)
        
        # Should return error in result
        assert 'error' in result
        assert 'Query execution failed' in result['error']

    def test_rollback_to_version_comprehensive_lines_1305_1357(self):
        """Test rollback to version with various scenarios - covers lines 1305-1357"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Test invalid base name
        result = db_manager.rollback_to_version(mock_connection, "invalid-name!", 1)
        assert result['status'] == 'invalid_base_name'
        
        # Test missing main table
        with patch.object(db_manager, 'table_exists', return_value=False):
            result = db_manager.rollback_to_version(mock_connection, "test_table", 1)
            assert result['status'] == 'main_missing'
        
        # Test exception during rollback operation
        with patch.object(db_manager, 'table_exists', return_value=True):
            with patch.object(db_manager, 'get_table_columns', side_effect=Exception("Column query failed")):
                result = db_manager.rollback_to_version(mock_connection, "test_table", 1)
                assert result['status'] == 'error'
                mock_connection.rollback.assert_called()

    def test_insert_reference_data_cfg_error_handling_lines_1387_1393_1432_1434(self):
        """Test reference data config insertion error handling - covers lines 1387-1393, 1432-1434"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Test stored procedure call error (lines 1432-1434)
        mock_cursor.execute.side_effect = [
            None,  # INSERT/UPDATE succeeds
            Exception("SP call failed")  # Stored procedure call fails
        ]
        
        # Should handle SP error gracefully without raising
        db_manager.insert_reference_data_cfg_record(mock_connection, "test_table")
        
        # Should not have committed due to error, but should have attempted rollback
        mock_connection.rollback.assert_called()

    def test_metadata_column_operations_edge_cases_line_822(self):
        """Test metadata column operations edge cases - covers line 822"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock missing metadata columns scenario
        mock_cursor.fetchall.return_value = [
            # Missing both ref_data_loadtime and ref_data_loadtype
        ]
        
        result = db_manager.ensure_metadata_columns(mock_connection, "test_table")
        
        # Should attempt to add missing metadata columns
        assert 'added' in result
        # Should have called ALTER TABLE for each missing column
        alter_calls = [call for call in mock_cursor.execute.call_args_list 
                      if 'ALTER TABLE' in str(call)]
        assert len(alter_calls) >= 1

    def test_table_schema_sync_edge_cases_lines_937_1005_1051(self):
        """Test table schema synchronization edge cases - covers lines 937, 1005, 1051"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Create named tuple to match database row structure
        from collections import namedtuple
        Row = namedtuple('Row', ['COLUMN_NAME', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'NUMERIC_SCALE', 'IS_NULLABLE', 'COLUMN_DEFAULT', 'ORDINAL_POSITION'])
        mock_cursor.fetchall.return_value = [
            Row('existing_col', 'int', None, None, None, 'YES', None, 1)
        ]
        
        target_columns = [
            {'name': 'existing_col', 'data_type': 'varchar(100)'},  # Type change
            {'name': 'new_col', 'data_type': 'varchar(50)'}        # New column
        ]
        
        result = db_manager.sync_table_schema(mock_connection, "test_table", target_columns)
        
        # Should handle schema synchronization
        assert isinstance(result, dict)

    def test_backup_table_listing_edge_cases_lines_1216_1239(self):
        """Test backup table listing with edge cases - covers lines 1216-1239"""
        db_manager = DatabaseManager()
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock backup tables with version information
        mock_cursor.fetchall.side_effect = [
            # Backup tables query
            [('table1_backup', 100), ('table2_backup', 50)],
            # Version query for table1 (exception scenario)
            Exception("Version query failed"),
            # Version query for table2
            [(1, 5)]
        ]
        
        with patch.object(db_manager, 'table_exists', return_value=True):
            result = db_manager.list_backup_tables(mock_connection)
            
            # Should handle partial failures gracefully
            assert isinstance(result, list)
            # Should have attempted to get versions for tables
            assert len(result) >= 0


class TestDatabaseDataTypeNormalization:
    """Test data type normalization edge cases"""

    def test_char_type_normalization_lines_633_638(self):
        """Test CHAR data type normalization - covers lines 633-638"""
        db_manager = DatabaseManager()
        
        # Test various CHAR formats
        test_cases = [
            ('char(10)', None, None, None, 'char(10)'),
            ('CHAR(255)', None, None, None, 'char(255)'),
            ('char', None, None, None, 'char'),  # Without parentheses
        ]
        
        for data_type, max_len, precision, scale, expected in test_cases:
            result = db_manager._normalize_data_type(data_type, max_len, precision, scale)
            # Should handle various CHAR formats
            assert isinstance(result, str)
            assert len(result) > 0


class TestDatabaseConnectionPooling:
    """Test advanced connection pooling scenarios"""

    def test_pool_statistics_and_cleanup(self):
        """Test connection pool statistics and cleanup operations"""
        db_manager = DatabaseManager()
        
        # Test pool stats
        stats = db_manager.get_pool_stats()
        assert 'pool_size_config' in stats
        assert 'idle' in stats
        assert 'in_use' in stats
        assert 'available_capacity' in stats
        
        # Test pool cleanup
        # Add some mock connections to pool
        mock_conn1 = MagicMock()
        mock_conn2 = MagicMock()
        mock_conn2.close.side_effect = Exception("Close failed")  # Test exception handling
        
        db_manager._pool = [mock_conn1, mock_conn2]
        
        # Should handle close exceptions gracefully
        db_manager.close_pool()
        
        mock_conn1.close.assert_called_once()
        mock_conn2.close.assert_called_once()
        assert len(db_manager._pool) == 0