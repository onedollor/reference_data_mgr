"""
Additional focused tests for utils.database to further increase coverage
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock, call


class TestDatabaseAdditionalMethods:
    """Test additional database methods not covered in comprehensive tests"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_is_safe_column_modification(self, mock_pyodbc):
        """Test _is_safe_column_modification method"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Test safe modifications (only widening varchar)
        assert db_manager._is_safe_column_modification('varchar(100)', 'varchar(200)') == True
        assert db_manager._is_safe_column_modification('varchar(100)', 'varchar(150)') == True
        
        # Test unsafe modifications
        assert db_manager._is_safe_column_modification('varchar(200)', 'varchar(100)') == False
        assert db_manager._is_safe_column_modification('int', 'varchar(100)') == False
        assert db_manager._is_safe_column_modification('varchar(100)', 'int') == False
        
        # Test same type/size
        assert db_manager._is_safe_column_modification('varchar(100)', 'varchar(100)') == True
        assert db_manager._is_safe_column_modification('int', 'int') == True

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_row_count(self, mock_pyodbc):
        """Test get_row_count method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [100]  # 100 rows
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.get_row_count(mock_conn, "test_table", "ref")
        
        assert result == 100
        mock_cursor.execute.assert_called_with("SELECT COUNT(*) FROM [ref].[test_table]")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_row_count_default_schema(self, mock_pyodbc):
        """Test get_row_count with default schema"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [50]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.get_row_count(mock_conn, "test_table")  # No schema specified
        
        assert result == 50
        mock_cursor.execute.assert_called_with("SELECT COUNT(*) FROM [ref].[test_table]")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_truncate_table(self, mock_pyodbc):
        """Test truncate_table method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            db_manager.truncate_table(mock_conn, "test_table", "ref")
        
        mock_cursor.execute.assert_called_with("TRUNCATE TABLE [ref].[test_table]")
        mock_print.assert_called_with("INFO: Truncated table [ref].[test_table]")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_truncate_table_default_schema(self, mock_pyodbc):
        """Test truncate_table with default schema"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print'):
            db_manager.truncate_table(mock_conn, "test_table")  # No schema
        
        mock_cursor.execute.assert_called_with("TRUNCATE TABLE [ref].[test_table]")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_ensure_metadata_columns_missing_columns(self, mock_pyodbc):
        """Test ensure_metadata_columns when columns are missing"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock get_table_columns to return table without metadata columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar'}
        ]
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch('builtins.print') as mock_print:
                db_manager.ensure_metadata_columns(mock_conn, "test_table", "ref")
        
        # Should execute ALTER TABLE commands to add missing columns
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        
        alter1 = calls[0][0][0]
        alter2 = calls[1][0][0]
        
        assert "ALTER TABLE [ref].[test_table] ADD [ref_data_loadtime] datetime DEFAULT GETDATE()" in alter1
        assert "ALTER TABLE [ref].[test_table] ADD [ref_data_loadtype] varchar(255)" in alter2

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_ensure_metadata_columns_existing_columns(self, mock_pyodbc):
        """Test ensure_metadata_columns when columns already exist"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        # Mock get_table_columns to return table WITH metadata columns
        existing_columns = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'name', 'data_type': 'varchar'},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'},
            {'name': 'ref_data_loadtype', 'data_type': 'varchar'}
        ]
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'get_table_columns', return_value=existing_columns):
            with patch('builtins.print') as mock_print:
                db_manager.ensure_metadata_columns(mock_conn, "test_table", "ref")
        
        # Should not execute any ALTER TABLE commands
        mock_print.assert_called_with("INFO: All required metadata columns already exist in [ref].[test_table]")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_determine_load_type_fullload(self, mock_pyodbc):
        """Test determine_load_type returning fullload"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]  # Empty table
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=True):
            result = db_manager.determine_load_type(mock_conn, "test_table", "ref")
        
        assert result == "fullload"

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_determine_load_type_append(self, mock_pyodbc):
        """Test determine_load_type returning append"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [100]  # Table has data
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=True):
            result = db_manager.determine_load_type(mock_conn, "test_table", "ref")
        
        assert result == "append"

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_determine_load_type_new_table(self, mock_pyodbc):
        """Test determine_load_type for new table"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=False):
            result = db_manager.determine_load_type(mock_conn, "new_table", "ref")
        
        assert result == "fullload"

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_backup_existing_data(self, mock_pyodbc):
        """Test backup_existing_data method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Next version ID
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            result = db_manager.backup_existing_data(mock_conn, "test_table", "ref", "fullload")
        
        assert result == 1  # Should return version ID
        
        # Should execute INSERT query
        assert mock_cursor.execute.call_count >= 2
        calls = mock_cursor.execute.call_args_list
        
        # Check that INSERT statement was executed
        insert_call = None
        for call_args, _ in calls:
            if 'INSERT INTO' in call_args[0]:
                insert_call = call_args[0]
                break
        
        assert insert_call is not None
        assert 'INSERT INTO [bkp].[test_table_backup]' in insert_call
        assert 'SELECT *, GETDATE(), ?, ?' in insert_call

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_ensure_reference_data_cfg_table_exists(self, mock_pyodbc):
        """Test ensure_reference_data_cfg_table when table exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=True):
            with patch('builtins.print') as mock_print:
                db_manager.ensure_reference_data_cfg_table(mock_conn)
        
        mock_print.assert_called_with("INFO: Reference_Data_Cfg table already exists")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_ensure_reference_data_cfg_table_create(self, mock_pyodbc):
        """Test ensure_reference_data_cfg_table when table needs creation"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch.object(db_manager, 'table_exists', return_value=False):
            with patch('builtins.print') as mock_print:
                db_manager.ensure_reference_data_cfg_table(mock_conn)
        
        # Should execute CREATE TABLE
        mock_cursor.execute.assert_called_once()
        create_sql = mock_cursor.execute.call_args[0][0]
        
        assert 'CREATE TABLE [ref].[Reference_Data_Cfg]' in create_sql
        assert '[table_name] varchar(255)' in create_sql
        assert '[created_date] datetime' in create_sql

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_insert_reference_data_cfg_record(self, mock_pyodbc):
        """Test insert_reference_data_cfg_record method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]  # Record doesn't exist
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        db_manager.insert_reference_data_cfg_record(mock_conn, "test_table")
        
        # Should check existence and insert
        assert mock_cursor.execute.call_count == 2
        calls = mock_cursor.execute.call_args_list
        
        check_sql = calls[0][0][0]
        insert_sql = calls[1][0][0]
        
        assert 'SELECT COUNT(*)' in check_sql
        assert 'INSERT INTO [ref].[Reference_Data_Cfg]' in insert_sql

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')  
    def test_insert_reference_data_cfg_record_exists(self, mock_pyodbc):
        """Test insert_reference_data_cfg_record when record exists"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]  # Record exists
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        
        with patch('builtins.print') as mock_print:
            db_manager.insert_reference_data_cfg_record(mock_conn, "test_table")
        
        # Should only check existence, not insert
        assert mock_cursor.execute.call_count == 1
        mock_print.assert_called_with("INFO: Reference data config record already exists for test_table")

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_list_backup_tables(self, mock_pyodbc):
        """Test list_backup_tables method"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        # Mock fetchall to return backup tables
        mock_cursor.fetchall.return_value = [
            MagicMock(TABLE_NAME='users_backup'),
            MagicMock(TABLE_NAME='products_backup'),
        ]
        mock_conn.cursor.return_value = mock_cursor
        
        db_manager = DatabaseManager()
        result = db_manager.list_backup_tables(mock_conn)
        
        assert len(result) == 2
        assert result[0]['table_name'] == 'users_backup'
        assert result[1]['table_name'] == 'products_backup'
        
        # Should execute query for backup schema
        mock_cursor.execute.assert_called_with(
            "\n            SELECT TABLE_NAME \n            FROM INFORMATION_SCHEMA.TABLES \n            WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE '%_backup'\n            ORDER BY TABLE_NAME\n        ",
            "bkp"
        )


class TestDatabaseConnectionMethods:
    """Test connection-related methods that need coverage"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user', 
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_table_exists_method_signature(self, mock_pyodbc):
        """Test table_exists method can be called without connection parameter"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Mock get_connection to return a mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        
        with patch.object(db_manager, 'get_connection', return_value=mock_conn):
            result = db_manager.table_exists("test_table", "ref")
        
        assert result == True

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_table_columns_method_signature(self, mock_pyodbc):
        """Test get_table_columns method can be called without connection parameter"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Mock get_connection and table data
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        
        mock_row = MagicMock()
        mock_row.COLUMN_NAME = "id"
        mock_row.DATA_TYPE = "int" 
        mock_row.CHARACTER_MAXIMUM_LENGTH = None
        mock_row.NUMERIC_PRECISION = None
        mock_row.NUMERIC_SCALE = None
        mock_row.IS_NULLABLE = "NO"
        mock_row.COLUMN_DEFAULT = None
        mock_row.ORDINAL_POSITION = 1
        
        mock_cursor.fetchall.return_value = [mock_row]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        
        with patch.object(db_manager, 'get_connection', return_value=mock_conn):
            columns = db_manager.get_table_columns("test_table", "ref")
        
        assert len(columns) == 1
        assert columns[0]['name'] == 'id'

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_table_row_count_method_signature(self, mock_pyodbc):
        """Test get_table_row_count method can be called without connection parameter"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        # Mock get_connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [100]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.__exit__.return_value = None
        
        with patch.object(db_manager, 'get_connection', return_value=mock_conn):
            count = db_manager.get_table_row_count("test_table", "ref")
        
        assert count == 100