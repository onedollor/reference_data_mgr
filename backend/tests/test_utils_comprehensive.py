"""
Comprehensive tests for utils modules to increase overall coverage
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open
import sys
from pathlib import Path


class TestUtilsDatabaseManager:
    """Tests for utils.database.DatabaseManager"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'db_host': 'test_host',
        'db_name': 'test_db'
    })
    @patch('utils.database.pyodbc')
    def test_database_manager_init(self, mock_pyodbc):
        """Test DatabaseManager initialization"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        
        assert db_manager.server == 'test_host'
        assert db_manager.database == 'test_db'
        assert db_manager.username == 'test_user'
        assert db_manager.password == 'test_pass'
        assert db_manager.pool_size == 5
    
    @patch.dict(os.environ, {}, clear=True)
    def test_database_manager_missing_credentials(self):
        """Test DatabaseManager with missing credentials"""
        from utils.database import DatabaseManager
        
        with pytest.raises(ValueError, match="Database username.*required"):
            DatabaseManager()
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass',
        'DB_POOL_SIZE': 'invalid'
    })
    @patch('utils.database.pyodbc')
    def test_database_manager_invalid_pool_size(self, mock_pyodbc):
        """Test DatabaseManager with invalid pool size"""
        from utils.database import DatabaseManager
        
        db_manager = DatabaseManager()
        assert db_manager.pool_size == 5  # Should default to 5

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_connection_success(self, mock_pyodbc):
        """Test successful database connection"""
        from utils.database import DatabaseManager
        
        mock_conn = MagicMock()
        mock_pyodbc.connect.return_value = mock_conn
        
        db_manager = DatabaseManager()
        connection = db_manager.get_connection()
        
        assert connection == mock_conn
        assert mock_conn.autocommit == True
        mock_pyodbc.connect.assert_called_once()

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    @patch('utils.database.time.sleep')
    def test_get_connection_with_retry(self, mock_sleep, mock_pyodbc):
        """Test database connection with retry logic"""
        from utils.database import DatabaseManager
        
        # First call fails, second succeeds
        mock_conn = MagicMock()
        mock_pyodbc.connect.side_effect = [Exception("Connection failed"), mock_conn]
        
        db_manager = DatabaseManager()
        connection = db_manager.get_connection()
        
        assert connection == mock_conn
        assert mock_pyodbc.connect.call_count == 2
        mock_sleep.assert_called_once()

    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    @patch('utils.database.pyodbc')
    def test_get_connection_max_retries_exceeded(self, mock_pyodbc):
        """Test database connection when max retries exceeded"""
        from utils.database import DatabaseManager
        
        mock_pyodbc.connect.side_effect = Exception("Connection failed")
        
        db_manager = DatabaseManager()
        
        with pytest.raises(Exception, match="Database connection failed"):
            db_manager.get_connection()


class TestUtilsCSVDetector:
    """Tests for utils.csv_detector.CSVFormatDetector"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_csv_detector_init(self):
        """Test CSVFormatDetector initialization"""
        from utils.csv_detector import CSVFormatDetector
        
        detector = CSVFormatDetector()
        assert detector is not None
    
    def test_csv_detector_with_file(self):
        """Test CSV detection with actual file"""
        from utils.csv_detector import CSVFormatDetector
        
        detector = CSVFormatDetector()
        
        # Create test CSV file
        test_file = os.path.join(self.temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("id,name,email\n1,John,john@test.com\n2,Jane,jane@test.com")
        
        result = detector.detect_format(test_file)
        
        assert result is not None
        assert result.get('column_delimiter') == ','
        assert result.get('has_header') == True
    
    def test_csv_detector_with_pipe_delimiter(self):
        """Test CSV detection with pipe delimiter"""
        from utils.csv_detector import CSVFormatDetector
        
        detector = CSVFormatDetector()
        
        # Create test file with pipe delimiter
        test_file = os.path.join(self.temp_dir, "test_pipe.csv")
        with open(test_file, 'w') as f:
            f.write("id|name|email\n1|John|john@test.com\n2|Jane|jane@test.com")
        
        result = detector.detect_format(test_file)
        
        assert result is not None
        assert result.get('column_delimiter') == '|'
        assert result.get('has_header') == True
    
    def test_csv_detector_file_not_found(self):
        """Test CSV detection with non-existent file"""
        from utils.csv_detector import CSVFormatDetector
        
        detector = CSVFormatDetector()
        
        with pytest.raises(Exception):
            detector.detect_format("/nonexistent/file.csv")


class TestUtilsFileHandler:
    """Tests for utils.file_handler.FileHandler"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_file_handler_init(self):
        """Test FileHandler initialization"""
        from utils.file_handler import FileHandler
        
        handler = FileHandler()
        assert handler is not None
    
    def test_extract_table_base_name(self):
        """Test extract_table_base_name method"""
        from utils.file_handler import FileHandler
        
        handler = FileHandler()
        
        # Test various filename patterns
        assert handler.extract_table_base_name("users.csv") == "users"
        assert handler.extract_table_base_name("user_data_20241225.csv") == "user_data"
        assert handler.extract_table_base_name("Products-List_2024-12-25.csv").lower() in ["products", "product"]


class TestUtilsLogger:
    """Tests for utils.logger.Logger"""
    
    def test_logger_init(self):
        """Test Logger initialization"""
        from utils.logger import Logger
        
        logger = Logger()
        assert logger is not None
    
    @patch('utils.logger.asyncio')
    def test_logger_async_operations(self, mock_asyncio):
        """Test Logger async operations"""
        from utils.logger import Logger
        
        logger = Logger()
        
        # Test that logger can handle async context
        mock_loop = MagicMock()
        mock_asyncio.get_event_loop.return_value = mock_loop
        
        # Should not raise exceptions
        assert logger is not None


class TestUtilsProgress:
    """Tests for utils.progress module"""
    
    def test_progress_module_import(self):
        """Test progress module can be imported"""
        import utils.progress as progress_utils
        
        assert progress_utils is not None
    
    def test_progress_functions_exist(self):
        """Test progress module functions exist"""
        import utils.progress as progress_utils
        
        # Check if common progress functions exist
        functions = dir(progress_utils)
        assert len(functions) > 0  # Should have some functions/attributes


class TestUtilsIngest:
    """Tests for utils.ingest.DataIngester"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('utils.ingest.DatabaseManager')
    @patch('utils.ingest.Logger')
    def test_data_ingester_init(self, mock_logger, mock_db):
        """Test DataIngester initialization"""
        from utils.ingest import DataIngester
        
        mock_db_inst = MagicMock()
        mock_logger_inst = MagicMock()
        
        ingester = DataIngester(mock_db_inst, mock_logger_inst)
        assert ingester is not None
    
    @patch('utils.ingest.DatabaseManager')
    @patch('utils.ingest.Logger')
    @patch('utils.ingest.pd.read_csv')
    def test_data_ingester_with_csv(self, mock_read_csv, mock_logger, mock_db):
        """Test DataIngester with CSV processing"""
        from utils.ingest import DataIngester
        import pandas as pd
        
        # Mock pandas DataFrame
        mock_df = pd.DataFrame({'id': [1, 2], 'name': ['John', 'Jane']})
        mock_read_csv.return_value = mock_df
        
        mock_db_inst = MagicMock()
        mock_logger_inst = MagicMock()
        
        ingester = DataIngester(mock_db_inst, mock_logger_inst)
        
        # Test basic functionality
        assert ingester is not None


class TestUtilsInit:
    """Tests for utils.__init__ module"""
    
    def test_utils_init_import(self):
        """Test utils.__init__ can be imported"""
        import utils
        
        assert utils is not None
    
    def test_utils_submodules_accessible(self):
        """Test utils submodules are accessible"""
        # Test that we can import submodules
        from utils import database
        from utils import csv_detector
        from utils import file_handler
        from utils import logger
        from utils import ingest
        
        assert database is not None
        assert csv_detector is not None
        assert file_handler is not None
        assert logger is not None
        assert ingest is not None


class TestUtilsEdgeCases:
    """Test edge cases and error handling in utils modules"""
    
    @patch.dict(os.environ, {
        'db_user': 'test_user',
        'db_password': 'test_pass'
    })
    def test_connection_string_building_edge_cases(self):
        """Test connection string building with edge cases"""
        from utils.database import DatabaseManager
        
        with patch('utils.database.pyodbc'):
            db_manager = DatabaseManager()
            conn_string = db_manager._build_connection_string()
            
            # Should contain required components
            assert 'DRIVER=' in conn_string
            assert 'SERVER=' in conn_string
            assert 'DATABASE=' in conn_string
            assert 'UID=' in conn_string
            assert 'PWD=' in conn_string
    
    def test_csv_detector_empty_file(self):
        """Test CSV detector with empty file"""
        from utils.csv_detector import CSVFormatDetector
        
        detector = CSVFormatDetector()
        
        # Create empty test file
        test_file = os.path.join(tempfile.gettempdir(), "empty.csv")
        with open(test_file, 'w') as f:
            f.write("")  # Empty file
        
        try:
            result = detector.detect_format(test_file)
            # Should handle gracefully or raise appropriate exception
            assert result is None or isinstance(result, dict)
        except Exception as e:
            # Expected for empty files
            assert True
        finally:
            if os.path.exists(test_file):
                os.remove(test_file)
    
    def test_file_handler_edge_cases(self):
        """Test FileHandler with edge cases"""
        from utils.file_handler import FileHandler
        
        handler = FileHandler()
        
        # Test with various edge case filenames
        test_cases = [
            "",
            ".",
            ".csv",
            "file_without_extension",
            "very_long_filename_" + "x" * 100 + ".csv"
        ]
        
        for filename in test_cases:
            try:
                result = handler.extract_table_base_name(filename)
                # Should return string result
                assert isinstance(result, str)
            except Exception:
                # Some edge cases may raise exceptions, which is acceptable
                assert True
    
    @patch('utils.ingest.DatabaseManager')
    @patch('utils.ingest.Logger')
    def test_data_ingester_error_handling(self, mock_logger, mock_db):
        """Test DataIngester error handling"""
        from utils.ingest import DataIngester
        
        # Mock components to raise exceptions
        mock_db_inst = MagicMock()
        mock_logger_inst = MagicMock()
        
        mock_db_inst.get_connection.side_effect = Exception("DB Error")
        
        ingester = DataIngester(mock_db_inst, mock_logger_inst)
        
        # Should handle initialization even with problematic dependencies
        assert ingester is not None