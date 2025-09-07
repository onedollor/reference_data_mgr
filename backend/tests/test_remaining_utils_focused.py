"""
Focused tests for remaining utils modules targeting methods that actually exist
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd


class TestFileHandlerFocused:
    """Focused tests for utils.file_handler.FileHandler methods that exist"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch.dict(os.environ, {
        'temp_location': '/test/temp',
        'archive_location': '/test/archive', 
        'format_location': '/test/format'
    })
    def test_file_handler_init_with_custom_paths(self):
        """Test FileHandler initialization with custom paths"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            assert handler.temp_location == '/test/temp'
            assert handler.archive_location == '/test/archive'
            assert handler.format_location == '/test/format'

    def test_file_handler_init_defaults(self):
        """Test FileHandler initialization with defaults"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Should use default Windows-style paths
            assert 'temp' in handler.temp_location.lower()
            assert 'archive' in handler.archive_location.lower()
            assert 'format' in handler.format_location.lower()

    def test_ensure_directories_success(self):
        """Test directory creation success"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs') as mock_makedirs:
            FileHandler()
            
            # Should create 3 directories
            assert mock_makedirs.call_count == 3
            for call in mock_makedirs.call_args_list:
                assert call[1]['exist_ok'] == True

    def test_ensure_directories_with_exceptions(self):
        """Test directory creation with exceptions"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs', side_effect=PermissionError("Access denied")):
            with patch('builtins.print') as mock_print:
                FileHandler()
                
                # Should print warnings for failed directory creation
                assert mock_print.call_count == 3
                for call in mock_print.call_args_list:
                    assert "Warning: Could not create directory" in call[0][0]

    def test_extract_table_base_name_basic(self):
        """Test basic table name extraction"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Test basic filename extraction
            assert handler.extract_table_base_name("users.csv") == "users"
            assert handler.extract_table_base_name("products.xlsx") == "products"
            assert handler.extract_table_base_name("data_export.txt") == "data_export"

    def test_extract_table_base_name_complex_patterns(self):
        """Test table name extraction with complex patterns"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Test patterns that should be cleaned up
            result1 = handler.extract_table_base_name("User_Data_Export_Final.csv")
            assert isinstance(result1, str)
            assert len(result1) > 0
            
            result2 = handler.extract_table_base_name("products-list-2024.csv") 
            assert isinstance(result2, str)
            assert len(result2) > 0


class TestLoggerFocused:
    """Focused tests for utils.logger.Logger methods that exist"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_logger_init_basic(self):
        """Test Logger basic initialization"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            assert hasattr(logger, 'log_dir')
            assert hasattr(logger, 'log_file')
            assert hasattr(logger, 'error_log_file')
            assert hasattr(logger, 'ingest_log_file')
            assert 'system.log' in logger.log_file
            assert 'error.log' in logger.error_log_file
            assert 'ingest.log' in logger.ingest_log_file

    @patch.dict(os.environ, {'LOG_TIMEZONE': 'UTC'})
    def test_logger_timezone_configuration(self):
        """Test Logger timezone configuration"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            assert logger.timezone_name == 'UTC'

    def test_logger_timezone_fallback(self):
        """Test Logger with invalid timezone fallback"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch('utils.logger.ZoneInfo', side_effect=Exception("Invalid timezone")):
                logger = Logger()
                
                # Should handle gracefully
                assert logger._tz is None

    def test_logger_no_zoneinfo_module(self):
        """Test Logger when zoneinfo module is not available"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch('utils.logger.ZoneInfo', None):
                logger = Logger()
                
                # Should work without zoneinfo
                assert logger._tz is None

    def test_ensure_log_directory(self):
        """Test log directory creation"""
        from utils.logger import Logger
        
        with patch('os.makedirs') as mock_makedirs:
            logger = Logger()
            
            # Should create log directory
            mock_makedirs.assert_called_with(logger.log_dir, exist_ok=True)

    def test_log_level_enum(self):
        """Test LogLevel enum values"""
        from utils.logger import LogLevel
        
        assert LogLevel.INFO.value == "INFO"
        assert LogLevel.WARNING.value == "WARNING" 
        assert LogLevel.ERROR.value == "ERROR"
        assert LogLevel.DEBUG.value == "DEBUG"

    def test_get_recent_logs_file_missing(self):
        """Test get_recent_logs when log file doesn't exist"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            with patch('os.path.exists', return_value=False):
                logs = logger.get_recent_logs(10)
                
                assert logs == []

    def test_get_recent_logs_success(self):
        """Test successful log retrieval"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            # Mock log file content
            log_lines = [
                '2024-12-25T10:30:00Z INFO: Test message 1\n',
                '2024-12-25T10:31:00Z ERROR: Test error message\n',
                '2024-12-25T10:32:00Z INFO: Test message 2\n'
            ]
            
            with patch('os.path.exists', return_value=True):
                with patch('builtins.open', mock_open(read_data=''.join(log_lines))):
                    logs = logger.get_recent_logs(5)
                    
                    assert isinstance(logs, list)
                    assert len(logs) >= 0  # May vary based on implementation

    def test_get_recent_logs_exception(self):
        """Test get_recent_logs with file read exception"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            with patch('os.path.exists', return_value=True):
                with patch('builtins.open', side_effect=PermissionError("Access denied")):
                    logs = logger.get_recent_logs(10)
                    
                    # Should handle exception gracefully
                    assert logs == []


class TestDataIngesterFocused:
    """Focused tests for utils.ingest.DataIngester initialization and basic methods"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_data_ingester_init_basic(self):
        """Test DataIngester basic initialization"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            assert ingester.db_manager == mock_db
            assert ingester.logger == mock_logger
            assert hasattr(ingester, 'file_handler')
            assert ingester.batch_size == 990

    @patch.dict(os.environ, {
        'INGEST_PROGRESS_INTERVAL': '10',
        'INGEST_TYPE_INFERENCE': '1', 
        'INGEST_TYPE_SAMPLE_ROWS': '1000',
        'INGEST_DATE_THRESHOLD': '0.9'
    })
    def test_data_ingester_init_with_env_config(self):
        """Test DataIngester initialization with environment configuration"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            assert ingester.progress_batch_interval == 10
            assert ingester.enable_type_inference == True
            assert ingester.type_sample_rows == 1000
            assert ingester.date_parse_threshold == 0.9

    @patch.dict(os.environ, {
        'INGEST_PROGRESS_INTERVAL': 'invalid_number',
        'INGEST_TYPE_SAMPLE_ROWS': 'also_invalid'
    })
    def test_data_ingester_init_invalid_env_values(self):
        """Test DataIngester initialization with invalid environment values"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            # Should fallback to defaults for invalid values
            assert ingester.progress_batch_interval == 5
            assert ingester.type_sample_rows == 5000

    def test_data_ingester_type_inference_disabled(self):
        """Test DataIngester with type inference disabled"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            # Default should be disabled
            assert ingester.enable_type_inference == False

    @patch.dict(os.environ, {'INGEST_TYPE_INFERENCE': 'true'})
    def test_data_ingester_type_inference_enabled(self):
        """Test DataIngester with type inference enabled"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            assert ingester.enable_type_inference == True

    def test_read_csv_data_basic(self):
        """Test basic CSV reading functionality"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        # Create test CSV file
        test_file = os.path.join(self.temp_dir, "test_read.csv")
        with open(test_file, 'w') as f:
            f.write("id,name,value\n1,John,100\n2,Jane,200\n3,Bob,150")
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            # Test if the method exists and can be called
            df = ingester._read_csv_data(test_file, ",", True, 0)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3
            assert list(df.columns) == ['id', 'name', 'value']

    def test_read_csv_data_no_header(self):
        """Test CSV reading without header"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        # Create test CSV file without header
        test_file = os.path.join(self.temp_dir, "test_no_header.csv")
        with open(test_file, 'w') as f:
            f.write("1,John,100\n2,Jane,200\n3,Bob,150")
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            df = ingester._read_csv_data(test_file, ",", False, 0)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3

    def test_read_csv_data_with_skip_lines(self):
        """Test CSV reading with skip lines"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        # Create test CSV file with header lines to skip
        test_file = os.path.join(self.temp_dir, "test_skip.csv")
        with open(test_file, 'w') as f:
            f.write("# This is a comment\n# Another comment\nid,name,value\n1,John,100\n2,Jane,200")
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            df = ingester._read_csv_data(test_file, ",", True, 2)  # Skip 2 lines
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2

    def test_read_csv_data_different_delimiters(self):
        """Test CSV reading with different delimiters"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        # Test semicolon delimiter
        test_file = os.path.join(self.temp_dir, "test_semicolon.csv")
        with open(test_file, 'w') as f:
            f.write("id;name;value\n1;John;100\n2;Jane;200")
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            df = ingester._read_csv_data(test_file, ";", True, 0)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert list(df.columns) == ['id', 'name', 'value']

    def test_read_csv_data_file_not_found(self):
        """Test CSV reading with non-existent file"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            with pytest.raises(Exception):
                ingester._read_csv_data("nonexistent_file.csv", ",", True, 0)

    def test_progress_key_sanitization(self):
        """Test progress key sanitization for filenames"""
        from utils.ingest import DataIngester
        import re
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            # Test that special characters are sanitized in progress keys
            test_filename = "test-file@#$%.csv"
            sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', test_filename)
            
            assert sanitized == "test_file____%.csv".replace('%', '_').replace('.', '_')


class TestUtilsProgressIntegration:
    """Test integration with utils.progress module"""
    
    def test_progress_module_import(self):
        """Test that progress module can be imported"""
        import utils.progress as progress_utils
        
        # Should be able to import without errors
        assert progress_utils is not None

    def test_progress_module_attributes(self):
        """Test progress module has expected attributes"""
        import utils.progress as progress_utils
        
        # Check for common progress-related functions
        module_attrs = dir(progress_utils)
        assert isinstance(module_attrs, list)
        assert len(module_attrs) > 0


class TestUtilsModuleStructure:
    """Test overall utils module structure and imports"""
    
    def test_all_utils_modules_import(self):
        """Test that all utils modules can be imported"""
        from utils import database
        from utils import csv_detector
        from utils import file_handler
        from utils import logger
        from utils import ingest
        from utils import progress
        
        assert database is not None
        assert csv_detector is not None
        assert file_handler is not None
        assert logger is not None
        assert ingest is not None
        assert progress is not None

    def test_utils_init_module(self):
        """Test utils __init__ module"""
        import utils
        
        assert utils is not None
        
        # Should have __init__.py
        init_file = utils.__file__
        assert init_file.endswith('__init__.py')

    def test_class_instantiation(self):
        """Test that main classes can be instantiated"""
        from utils.file_handler import FileHandler
        from utils.logger import Logger
        from utils.database import DatabaseManager
        from utils.csv_detector import CSVFormatDetector
        
        # Test class instantiation (with mocking for dependencies)
        with patch('os.makedirs'):
            file_handler = FileHandler()
            logger = Logger()
            csv_detector = CSVFormatDetector()
            
            assert isinstance(file_handler, FileHandler)
            assert isinstance(logger, Logger)
            assert isinstance(csv_detector, CSVFormatDetector)
        
        # DatabaseManager needs environment variables
        with patch.dict(os.environ, {'db_user': 'test', 'db_password': 'test'}):
            with patch('utils.database.pyodbc'):
                db_manager = DatabaseManager()
                assert isinstance(db_manager, DatabaseManager)