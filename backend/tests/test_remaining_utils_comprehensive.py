"""
Comprehensive tests for remaining utils modules: file_handler.py, logger.py, and ingest.py
Target coverage improvements from 38%, 22%, 5% to 60%+ each
"""
import pytest
import os
import tempfile
import shutil
import asyncio
import json
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from datetime import datetime
from pathlib import Path


class TestFileHandler:
    """Comprehensive tests for utils.file_handler.FileHandler"""
    
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
    def test_file_handler_init_with_env_vars(self):
        """Test FileHandler initialization with environment variables"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            assert handler.temp_location == '/test/temp'
            assert handler.archive_location == '/test/archive'
            assert handler.format_location == '/test/format'

    def test_file_handler_init_defaults(self):
        """Test FileHandler initialization with default values"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            assert 'temp' in handler.temp_location.lower()
            assert 'archive' in handler.archive_location.lower()
            assert 'format' in handler.format_location.lower()

    def test_ensure_directories_success(self):
        """Test successful directory creation"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs') as mock_makedirs:
            handler = FileHandler()
            
            # Should call makedirs for each directory
            assert mock_makedirs.call_count == 3
            for call in mock_makedirs.call_args_list:
                assert call[1]['exist_ok'] == True

    def test_ensure_directories_failure(self):
        """Test directory creation with exception"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs', side_effect=Exception("Permission denied")):
            with patch('builtins.print') as mock_print:
                handler = FileHandler()
                
                # Should print warning messages for each failed directory
                assert mock_print.call_count == 3
                for call in mock_print.call_args_list:
                    args = call[0][0]
                    assert "Warning: Could not create directory" in args

    def test_sanitize_filename(self):
        """Test filename sanitization"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Test various filename scenarios
            assert handler._sanitize_filename("test.csv") == "test.csv"
            assert handler._sanitize_filename("test file.csv") == "test_file.csv"
            assert handler._sanitize_filename("test/\\file.csv") == "test__file.csv"
            assert handler._sanitize_filename("test<>file.csv") == "test__file.csv"
            assert handler._sanitize_filename("") == "unknown_file"
            assert handler._sanitize_filename(None) == "unknown_file"

    @pytest.mark.asyncio
    async def test_save_uploaded_file_success(self):
        """Test successful file upload and format creation"""
        from utils.file_handler import FileHandler
        
        # Create mock upload file
        mock_file = MagicMock()
        mock_file.filename = "test.csv"
        mock_file.read = AsyncMock(return_value=b"id,name\n1,John\n2,Jane")
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', mock_open()) as mock_aiofile:
                with patch.object(FileHandler, '_create_format_file', return_value="test.fmt") as mock_create_fmt:
                    handler = FileHandler()
                    
                    temp_path, fmt_path = await handler.save_uploaded_file(
                        mock_file, ",", ",", "\\n", "\"", 0, None
                    )
                    
                    assert temp_path.endswith("test.csv")
                    assert fmt_path == "test.fmt"
                    mock_create_fmt.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_uploaded_file_exception(self):
        """Test file upload with exception"""
        from utils.file_handler import FileHandler
        
        mock_file = MagicMock()
        mock_file.filename = "test.csv"
        mock_file.read = AsyncMock(side_effect=Exception("File read error"))
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            with pytest.raises(Exception, match="Failed to save uploaded file"):
                await handler.save_uploaded_file(
                    mock_file, ",", ",", "\\n", "\"", 0, None
                )

    @pytest.mark.asyncio
    async def test_create_format_file(self):
        """Test format file creation"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', mock_open()) as mock_aiofile:
                handler = FileHandler()
                
                fmt_path = await handler._create_format_file(
                    "test.csv", ",", ",", "\\n", "\"", 0, None, "20241225_103000"
                )
                
                assert fmt_path.endswith(".fmt")
                mock_aiofile.assert_called_once()

    def test_extract_table_base_name_simple(self):
        """Test table name extraction from simple filenames"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            assert handler.extract_table_base_name("users.csv") == "users"
            assert handler.extract_table_base_name("products.txt") == "products"
            assert handler.extract_table_base_name("data_export.csv") == "data_export"

    def test_extract_table_base_name_with_dates(self):
        """Test table name extraction with date patterns"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Should remove date patterns
            assert handler.extract_table_base_name("users_20241225.csv") == "users"
            assert handler.extract_table_base_name("products_2024-12-25.csv") == "products"
            assert handler.extract_table_base_name("data_20241225_103000.csv") == "data"

    def test_extract_table_base_name_complex(self):
        """Test table name extraction from complex filenames"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Should handle complex patterns
            result = handler.extract_table_base_name("User-Data_Export_2024-12-25_final.csv")
            assert "user" in result.lower()
            assert "data" in result.lower()

    def test_extract_table_base_name_edge_cases(self):
        """Test table name extraction edge cases"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            handler = FileHandler()
            
            # Edge cases
            assert handler.extract_table_base_name("") == "unknown_table"
            assert handler.extract_table_base_name(".csv") == "unknown_table"
            assert handler.extract_table_base_name("file_without_extension") == "file_without_extension"

    @pytest.mark.asyncio
    async def test_archive_file_success(self):
        """Test successful file archiving"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            with patch('shutil.move') as mock_move:
                handler = FileHandler()
                
                archived_path = await handler.archive_file("/temp/test.csv", "processed")
                
                assert "archive" in archived_path
                assert "processed" in archived_path
                mock_move.assert_called_once()

    @pytest.mark.asyncio
    async def test_archive_file_with_error(self):
        """Test file archiving with error handling"""
        from utils.file_handler import FileHandler
        
        with patch('os.makedirs'):
            with patch('shutil.move', side_effect=Exception("Move failed")):
                with patch('builtins.print') as mock_print:
                    handler = FileHandler()
                    
                    result = await handler.archive_file("/temp/test.csv", "error")
                    
                    # Should still return path even if move fails
                    assert result is not None
                    mock_print.assert_called()


class TestLogger:
    """Comprehensive tests for utils.logger.Logger"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch.dict(os.environ, {'LOG_TIMEZONE': 'UTC'})
    def test_logger_init_with_timezone(self):
        """Test Logger initialization with timezone"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            assert logger.timezone_name == 'UTC'
            assert "system.log" in logger.log_file
            assert "error.log" in logger.error_log_file
            assert "ingest.log" in logger.ingest_log_file

    def test_logger_init_default_timezone(self):
        """Test Logger initialization with default timezone"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            assert logger.timezone_name == "America/Toronto"

    def test_logger_init_invalid_timezone(self):
        """Test Logger initialization with invalid timezone"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch('utils.logger.ZoneInfo', side_effect=Exception("Invalid timezone")):
                logger = Logger()
                
                # Should handle invalid timezone gracefully
                assert logger._tz is None

    def test_logger_init_no_zoneinfo(self):
        """Test Logger initialization without zoneinfo module"""
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
            
            mock_makedirs.assert_called_with(logger.log_dir, exist_ok=True)

    @pytest.mark.asyncio
    async def test_log_info(self):
        """Test info logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch.object(Logger, '_write_log') as mock_write:
                logger = Logger()
                
                await logger.log_info("test_action", "test message", {"key": "value"}, "192.168.1.1")
                
                mock_write.assert_called_once()
                args = mock_write.call_args[0]
                assert args[0].value == "INFO"
                assert args[1] == "test_action"
                assert args[2] == "test message"

    @pytest.mark.asyncio
    async def test_log_warning(self):
        """Test warning logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch.object(Logger, '_write_log') as mock_write:
                logger = Logger()
                
                await logger.log_warning("test_action", "warning message")
                
                mock_write.assert_called_once()
                args = mock_write.call_args[0]
                assert args[0].value == "WARNING"

    @pytest.mark.asyncio
    async def test_log_error(self):
        """Test error logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch.object(Logger, '_write_log') as mock_write:
                logger = Logger()
                
                await logger.log_error("test_action", "error message", "traceback info")
                
                mock_write.assert_called_once()
                args = mock_write.call_args[0]
                assert args[0].value == "ERROR"
                assert args[3] == "traceback info"

    @pytest.mark.asyncio
    async def test_log_debug(self):
        """Test debug logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch.object(Logger, '_write_log') as mock_write:
                logger = Logger()
                
                await logger.log_debug("test_action", "debug message")
                
                mock_write.assert_called_once()
                args = mock_write.call_args[0]
                assert args[0].value == "DEBUG"

    @pytest.mark.asyncio
    async def test_write_log_with_timezone(self):
        """Test log writing with timezone"""
        from utils.logger import Logger, LogLevel
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', mock_open()) as mock_aiofile:
                logger = Logger()
                
                await logger._write_log(
                    LogLevel.INFO, "test_action", "test message", 
                    None, {"key": "value"}, "192.168.1.1"
                )
                
                mock_aiofile.assert_called()

    @pytest.mark.asyncio
    async def test_write_log_exception_handling(self):
        """Test log writing with exception"""
        from utils.logger import Logger, LogLevel
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', side_effect=Exception("Write failed")):
                with patch('builtins.print') as mock_print:
                    logger = Logger()
                    
                    await logger._write_log(LogLevel.ERROR, "test", "message")
                    
                    # Should handle exception and print error
                    mock_print.assert_called()

    @pytest.mark.asyncio
    async def test_log_ingestion_start(self):
        """Test ingestion start logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', mock_open()) as mock_aiofile:
                logger = Logger()
                
                await logger.log_ingestion_start("test.csv", "fullload", {"rows": 100})
                
                mock_aiofile.assert_called()

    @pytest.mark.asyncio
    async def test_log_ingestion_progress(self):
        """Test ingestion progress logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', mock_open()) as mock_aiofile:
                logger = Logger()
                
                await logger.log_ingestion_progress("test.csv", 50, 100)
                
                mock_aiofile.assert_called()

    @pytest.mark.asyncio
    async def test_log_ingestion_complete(self):
        """Test ingestion completion logging"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            with patch('aiofiles.open', mock_open()) as mock_aiofile:
                logger = Logger()
                
                await logger.log_ingestion_complete("test.csv", "success", 100, 1.5)
                
                mock_aiofile.assert_called()

    def test_get_recent_logs_file_not_found(self):
        """Test get_recent_logs when file doesn't exist"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            with patch('os.path.exists', return_value=False):
                logs = logger.get_recent_logs(10)
                
                assert logs == []

    def test_get_recent_logs_success(self):
        """Test successful recent logs retrieval"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            mock_logs = [
                '2024-12-25T10:30:00 INFO test_action: test message\n',
                '2024-12-25T10:31:00 ERROR error_action: error message\n'
            ]
            
            with patch('builtins.open', mock_open(read_data=''.join(mock_logs))):
                logs = logger.get_recent_logs(10)
                
                assert len(logs) == 2

    def test_get_recent_logs_exception(self):
        """Test get_recent_logs with exception"""
        from utils.logger import Logger
        
        with patch('os.makedirs'):
            logger = Logger()
            
            with patch('builtins.open', side_effect=Exception("Read failed")):
                logs = logger.get_recent_logs(10)
                
                assert logs == []


class TestDataIngester:
    """Comprehensive tests for utils.ingest.DataIngester"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch.dict(os.environ, {
        'INGEST_PROGRESS_INTERVAL': '10',
        'INGEST_TYPE_INFERENCE': '1',
        'INGEST_TYPE_SAMPLE_ROWS': '1000',
        'INGEST_DATE_THRESHOLD': '0.9'
    })
    def test_data_ingester_init_with_env_vars(self):
        """Test DataIngester initialization with environment variables"""
        from utils.ingest import DataIngester
        from utils.database import DatabaseManager
        from utils.logger import Logger
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            assert ingester.progress_batch_interval == 10
            assert ingester.enable_type_inference == True
            assert ingester.type_sample_rows == 1000
            assert ingester.date_parse_threshold == 0.9
            assert ingester.batch_size == 990

    def test_data_ingester_init_defaults(self):
        """Test DataIngester initialization with default values"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            assert ingester.progress_batch_interval == 5
            assert ingester.enable_type_inference == False
            assert ingester.type_sample_rows == 5000
            assert ingester.date_parse_threshold == 0.8

    def test_data_ingester_init_invalid_env_vars(self):
        """Test DataIngester initialization with invalid environment variables"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch.dict(os.environ, {
            'INGEST_PROGRESS_INTERVAL': 'invalid',
            'INGEST_TYPE_SAMPLE_ROWS': 'invalid'
        }):
            with patch('utils.ingest.FileHandler'):
                ingester = DataIngester(mock_db, mock_logger)
                
                # Should use defaults for invalid values
                assert ingester.progress_batch_interval == 5
                assert ingester.type_sample_rows == 5000

    @pytest.mark.asyncio
    async def test_ingest_data_basic_flow(self):
        """Test basic data ingestion flow"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        mock_connection = MagicMock()
        mock_db.get_connection.return_value = mock_connection
        
        # Create test CSV file
        test_file = os.path.join(self.temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("id,name\n1,John\n2,Jane")
        
        # Create test format file
        fmt_file = os.path.join(self.temp_dir, "test.fmt")
        fmt_data = {
            "csv_format": {
                "column_delimiter": ",",
                "has_header": True,
                "skip_lines": 0
            }
        }
        with open(fmt_file, 'w') as f:
            json.dump(fmt_data, f)
        
        with patch('utils.ingest.FileHandler'):
            with patch('utils.ingest.prog') as mock_progress:
                with patch('pandas.read_csv') as mock_read_csv:
                    with patch.object(DataIngester, '_process_data_batch') as mock_process:
                        mock_read_csv.return_value = MagicMock()
                        mock_progress.is_canceled.return_value = False
                        mock_process.return_value = None
                        
                        ingester = DataIngester(mock_db, mock_logger)
                        
                        messages = []
                        async for message in ingester.ingest_data(
                            test_file, fmt_file, "fullload", "test.csv"
                        ):
                            messages.append(message)
                        
                        assert len(messages) > 0
                        assert "Starting data ingestion process" in messages[0]

    @pytest.mark.asyncio
    async def test_ingest_data_cancellation(self):
        """Test data ingestion cancellation"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            with patch('utils.ingest.prog') as mock_progress:
                mock_progress.is_canceled.return_value = True
                
                ingester = DataIngester(mock_db, mock_logger)
                
                with pytest.raises(Exception, match="Ingestion canceled by user"):
                    async for message in ingester.ingest_data(
                        "test.csv", "test.fmt", "fullload", "test.csv"
                    ):
                        pass

    @pytest.mark.asyncio
    async def test_ingest_data_exception_handling(self):
        """Test data ingestion exception handling"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        mock_db.get_connection.side_effect = Exception("Connection failed")
        
        with patch('utils.ingest.FileHandler'):
            with patch('utils.ingest.prog') as mock_progress:
                mock_progress.is_canceled.return_value = False
                
                ingester = DataIngester(mock_db, mock_logger)
                
                messages = []
                async for message in ingester.ingest_data(
                    "test.csv", "test.fmt", "fullload", "test.csv"
                ):
                    messages.append(message)
                
                # Should handle exception and yield error message
                error_messages = [msg for msg in messages if "error" in msg.lower()]
                assert len(error_messages) > 0

    def test_read_csv_data_success(self):
        """Test successful CSV data reading"""
        from utils.ingest import DataIngester
        import pandas as pd
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        # Create test CSV file
        test_file = os.path.join(self.temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("id,name\n1,John\n2,Jane")
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            df = ingester._read_csv_data(test_file, ",", True, 0)
            
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 2
            assert list(df.columns) == ['id', 'name']

    def test_read_csv_data_exception(self):
        """Test CSV data reading with exception"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            with pytest.raises(Exception):
                ingester._read_csv_data("nonexistent.csv", ",", True, 0)

    def test_infer_column_types_basic(self):
        """Test basic column type inference"""
        from utils.ingest import DataIngester
        import pandas as pd
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        # Create test DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'score': [95.5, 87.2, 92.1],
            'active': [True, False, True]
        })
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            types = ingester._infer_column_types(df)
            
            assert 'id' in types
            assert 'name' in types
            assert 'score' in types
            assert 'active' in types

    def test_generate_create_table_sql(self):
        """Test CREATE TABLE SQL generation"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        column_types = {
            'id': 'int',
            'name': 'varchar(255)',
            'score': 'decimal(10,2)'
        }
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            sql = ingester._generate_create_table_sql("test_table", "ref", column_types, True)
            
            assert "CREATE TABLE [ref].[test_table]" in sql
            assert "[id] int" in sql
            assert "[name] varchar(255)" in sql
            assert "[score] decimal(10,2)" in sql
            assert "ref_data_loadtime" in sql
            assert "ref_data_loadtype" in sql

    def test_generate_create_table_sql_without_metadata(self):
        """Test CREATE TABLE SQL generation without metadata columns"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        column_types = {'id': 'int', 'name': 'varchar(255)'}
        
        with patch('utils.ingest.FileHandler'):
            ingester = DataIngester(mock_db, mock_logger)
            
            sql = ingester._generate_create_table_sql("test_table", "ref", column_types, False)
            
            assert "CREATE TABLE [ref].[test_table]" in sql
            assert "ref_data_loadtime" not in sql
            assert "ref_data_loadtype" not in sql