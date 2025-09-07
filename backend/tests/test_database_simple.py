"""
Simple focused tests for utils/database.py to improve coverage
Focuses on working functionality and proper method signatures
"""

import pytest
import os
from unittest.mock import patch, MagicMock, PropertyMock


class TestDatabaseBasics:
    """Basic database functionality tests"""

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'testserver',
        'db_name': 'testdb'
    })
    def test_database_manager_initialization(self):
        """Test DatabaseManager basic initialization"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test basic properties
        assert db_manager.username == 'test_user'
        assert db_manager.password == 'test_pass'
        assert db_manager.server == 'testserver' 
        assert db_manager.database == 'testdb'
        assert db_manager.data_schema == 'ref'
        assert db_manager.backup_schema == 'bkp'

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_connection_string_building(self):
        """Test connection string construction"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        conn_str = db_manager._build_connection_string()
        
        assert 'test_user' in conn_str
        assert 'test_pass' in conn_str
        assert 'DRIVER=' in conn_str
        assert 'SERVER=' in conn_str
        assert 'DATABASE=' in conn_str

    @patch.dict('os.environ', {})
    def test_missing_credentials_validation(self):
        """Test validation of required credentials"""
        from utils.database import DatabaseManager
        
        with pytest.raises(ValueError, match="Database username.*required"):
            DatabaseManager()

    @patch.dict('os.environ', {'db_user': 'test_user'})
    def test_missing_password_validation(self):
        """Test validation of required password"""
        from utils.database import DatabaseManager
        
        with pytest.raises(ValueError, match="Database password.*required"):
            DatabaseManager()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_connection_basic(self, mock_pyodbc):
        """Test basic database connection"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        connection = db_manager.get_connection()
        
        assert connection == mock_conn
        mock_conn.__setattr__.assert_called_with('autocommit', True)

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_connection_retry_logic(self, mock_pyodbc):
        """Test connection retry mechanism"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        # Fail first attempt, succeed on second
        mock_pyodbc.connect.side_effect = [Exception("Connection failed"), mock_conn]
        
        db_manager = DatabaseManager()
        
        with patch('utils.database.time.sleep'):
            connection = db_manager.get_connection()
        
        assert connection == mock_conn
        assert mock_pyodbc.connect.call_count == 2

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_table_exists_method(self):
        """Test table_exists method signature and basic functionality"""
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
    def test_get_table_columns_method(self):
        """Test get_table_columns method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock column row
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

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_create_table_method(self):
        """Test create_table method"""
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

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_ensure_schemas_exist_method(self):
        """Test ensure_schemas_exist method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        db_manager.ensure_schemas_exist(mock_conn)
        
        # Should execute schema creation statements
        assert mock_cursor.execute.call_count >= 1

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_truncate_table_method(self):
        """Test truncate_table method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        db_manager.truncate_table(mock_conn, 'test_table', 'ref')
        
        mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_get_row_count_method(self):
        """Test get_row_count method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [100]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        count = db_manager.get_row_count(mock_conn, 'test_table', 'ref')
        
        assert count == 100

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_normalize_data_type_method(self):
        """Test _normalize_data_type method"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test varchar
        result = db_manager._normalize_data_type('varchar', max_length=100)
        assert 'VARCHAR' in result.upper()
        
        # Test int
        result = db_manager._normalize_data_type('int')
        assert result.upper() == 'INT'

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_get_timestamp_suffix_method(self):
        """Test _get_timestamp_suffix method"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        timestamp = db_manager._get_timestamp_suffix()
        
        # Should return a timestamp string
        assert isinstance(timestamp, str)
        assert len(timestamp) > 0

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_pooled_connection_basic(self, mock_pyodbc):
        """Test basic pooled connection functionality"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        
        # Get pooled connection
        connection = db_manager.get_pooled_connection()
        assert connection == mock_conn
        
        # Release connection
        db_manager.release_connection(connection)
        
        # Pool stats
        stats = db_manager.get_pool_stats()
        assert 'pool_size_config' in stats
        assert 'idle' in stats
        assert 'in_use' in stats

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_drop_table_if_exists_method(self):
        """Test drop_table_if_exists method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        # Test when table exists
        with patch.object(db_manager, 'table_exists', return_value=True):
            result = db_manager.drop_table_if_exists(mock_conn, 'test_table', 'ref')
            assert result is True
            mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_is_safe_column_modification_method(self):
        """Test _is_safe_column_modification method"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        existing_col = {'type': 'varchar', 'max_length': 50}
        expected_col = {'type': 'varchar', 'max_length': 100}
        
        # This should be safe (increasing length)
        result = db_manager._is_safe_column_modification(existing_col, expected_col)
        assert isinstance(result, bool)

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_test_connection_method(self, mock_pyodbc):
        """Test test_connection method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ['SQL Server Version', '2023-01-01']
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        result = db_manager.test_connection()
        
        assert 'status' in result
        mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_backup_existing_data_method(self):
        """Test backup_existing_data method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        count = db_manager.backup_existing_data(mock_conn, 'source_table', 'backup_table')
        
        assert count == 5
        mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_ensure_metadata_columns_method(self):
        """Test ensure_metadata_columns method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.ensure_metadata_columns(mock_conn, 'test_table', 'ref')
        
        assert isinstance(result, dict)
        mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_determine_load_type_method(self):
        """Test determine_load_type method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        db_manager = DatabaseManager()
        
        # Test with table that doesn't exist - should return fullload
        with patch.object(db_manager, 'table_exists', return_value=False):
            load_type = db_manager.determine_load_type(mock_conn, 'test_table', 'fullload')
            assert load_type == 'fullload'

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_ensure_reference_data_cfg_table_method(self):
        """Test ensure_reference_data_cfg_table method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        # Test when table doesn't exist
        with patch.object(db_manager, 'table_exists', return_value=False):
            db_manager.ensure_reference_data_cfg_table(mock_conn)
            mock_cursor.execute.assert_called()

    @patch.dict('os.environ', {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_insert_reference_data_cfg_record_method(self):
        """Test insert_reference_data_cfg_record method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        db_manager.insert_reference_data_cfg_record(mock_conn, 'test_table')
        
        mock_cursor.execute.assert_called()