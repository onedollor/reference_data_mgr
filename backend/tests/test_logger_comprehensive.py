"""
Comprehensive test coverage for utils/logger.py
Focus on improving coverage from 22% to much higher level
"""

import pytest
import os
import tempfile
import shutil
import json
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open, call
from datetime import datetime
from typing import Dict, Any, List

from utils.logger import Logger, LogLevel, DatabaseLogger


class TestLoggerInit:
    """Test Logger initialization and configuration"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_logger_init_default(self):
        """Test Logger initialization with default settings"""
        with patch('os.makedirs'):
            logger = Logger()
            
            assert logger.log_dir is not None
            assert logger.log_file is not None
            assert logger.error_log_file is not None
            assert logger.ingest_log_file is not None
            assert logger.timezone_name == "America/Toronto"  # Default
    
    def test_logger_init_with_timezone_env(self):
        """Test Logger initialization with timezone from environment"""
        with patch.dict(os.environ, {'LOG_TIMEZONE': 'UTC'}):
            with patch('os.makedirs'):
                logger = Logger()
                
                assert logger.timezone_name == "UTC"
    
    def test_logger_ensure_log_directory(self):
        """Test log directory creation"""
        with patch('os.makedirs') as mock_makedirs:
            logger = Logger()
            
            mock_makedirs.assert_called_once_with(logger.log_dir, exist_ok=True)
    
    def test_logger_zoneinfo_import_error(self):
        """Test logger initialization when ZoneInfo is not available"""
        with patch('utils.logger.ZoneInfo', None):
            with patch('os.makedirs'):
                logger = Logger()
                
                assert logger._tz is None
    
    def test_logger_zoneinfo_exception(self):
        """Test logger initialization when ZoneInfo raises exception"""
        mock_zoneinfo = MagicMock()
        mock_zoneinfo.side_effect = Exception("Invalid timezone")
        
        with patch('utils.logger.ZoneInfo', mock_zoneinfo):
            with patch('os.makedirs'):
                logger = Logger()
                
                assert logger._tz is None


class TestLoggerBasicLogging:
    """Test basic logging functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_log_info(self):
        """Test info logging"""
        with patch('os.makedirs'):
            logger = Logger()
        
        with patch.object(logger, '_write_log') as mock_write:
            await logger.log_info("test_action", "Test info message")
            
            mock_write.assert_called_once_with(
                LogLevel.INFO,
                "test_action",
                "Test info message",
                None,
                None,
                None
            )
    
    @pytest.mark.asyncio
    async def test_log_warning(self):
        """Test warning logging"""
        with patch('os.makedirs'):
            logger = Logger()
        
        with patch.object(logger, '_write_log') as mock_write:
            await logger.log_warning("test_action", "Test warning message")
            
            mock_write.assert_called_once_with(
                LogLevel.WARNING,
                "test_action",
                "Test warning message",
                None,
                None,
                None
            )
    
    @pytest.mark.asyncio
    async def test_log_error(self):
        """Test error logging"""
        with patch('os.makedirs'):
            logger = Logger()
        
        try:
            raise ValueError("Test error")
        except ValueError as e:
            with patch.object(logger, '_write_log') as mock_write:
                await logger.log_error("test_action", "Test error message", e)
                
                # Should be called with error details
                mock_write.assert_called_once()
                call_args = mock_write.call_args[0]
                assert call_args[0] == LogLevel.ERROR
                assert call_args[1] == "test_action"
                assert call_args[2] == "Test error message"
    
    @pytest.mark.asyncio
    async def test_log_debug(self):
        """Test debug logging"""
        with patch('os.makedirs'):
            logger = Logger()
        
        with patch.object(logger, '_write_log') as mock_write:
            await logger.log_debug("test_action", "Debug message")
            
            mock_write.assert_called_once_with(
                LogLevel.DEBUG,
                "test_action",
                "Debug message",
                None,
                None,
                None
            )
    
    @pytest.mark.asyncio
    async def test_log_with_metadata(self):
        """Test logging with metadata"""
        with patch('os.makedirs'):
            logger = Logger()
        
        metadata = {"user": "test", "action": "upload"}
        
        with patch.object(logger, '_write_log') as mock_write:
            await logger.log_info("test_action", "Test message", metadata=metadata)
            
            mock_write.assert_called_once_with(
                LogLevel.INFO,
                "test_action",
                "Test message",
                None,
                metadata,
                None
            )


class TestLoggerWriteLog:
    """Test internal log writing functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_write_log_basic(self):
        """Test basic log writing"""
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = os.path.join(self.temp_dir, "system.log")
            logger.error_log_file = os.path.join(self.temp_dir, "error.log")
        
        mock_file_data = []
        
        async def mock_write(data):
            mock_file_data.append(data)
        
        mock_file = MagicMock()
        mock_file.write = AsyncMock(side_effect=mock_write)
        
        with patch('aiofiles.open', return_value=mock_file.__aenter__()):
            await logger._write_log(LogLevel.INFO, "Test message", None, {})
            
            # Verify file was opened for writing
            assert mock_file.write.call_count > 0
    
    @pytest.mark.asyncio  
    async def test_write_log_error_level(self):
        """Test error level log writing to separate file"""
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = os.path.join(self.temp_dir, "system.log")
            logger.error_log_file = os.path.join(self.temp_dir, "error.log")
        
        mock_file = MagicMock()
        mock_file.write = AsyncMock()
        
        with patch('aiofiles.open', return_value=mock_file.__aenter__()):
            await logger._write_log(LogLevel.ERROR, "Error message", "Exception details", {})
            
            # Should write to both system log and error log
            assert mock_file.write.call_count > 0
    
    @pytest.mark.asyncio
    async def test_write_log_with_timezone(self):
        """Test log writing with timezone"""
        mock_tz = MagicMock()
        
        with patch('os.makedirs'):
            logger = Logger()
            logger._tz = mock_tz
        
        mock_file = MagicMock()
        mock_file.write = AsyncMock()
        
        with patch('aiofiles.open', return_value=mock_file.__aenter__()):
            with patch('utils.logger.datetime') as mock_datetime:
                mock_now = MagicMock()
                mock_datetime.now.return_value = mock_now
                mock_now.replace.return_value = mock_now
                mock_now.isoformat.return_value = "2024-01-01T12:00:00"
                
                await logger._write_log(LogLevel.INFO, "Test message", None, {})
                
                # Should call datetime.now with timezone if available
                mock_datetime.now.assert_called_with(mock_tz)


class TestLoggerReading:
    """Test log reading functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_get_logs_basic(self):
        """Test getting logs from file"""
        log_file = os.path.join(self.temp_dir, "system.log")
        log_entries = [
            '{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "Test 1"}',
            '{"timestamp": "2024-01-01T12:01:00", "level": "ERROR", "message": "Test 2"}'
        ]
        
        with open(log_file, 'w') as f:
            for entry in log_entries:
                f.write(entry + "\n")
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = log_file
        
        result = await logger.get_logs(limit=10)
        
        assert len(result) == 2
        assert result[0]["level"] == "ERROR"  # Most recent first
        assert result[1]["level"] == "INFO"
    
    @pytest.mark.asyncio
    async def test_get_logs_nonexistent_file(self):
        """Test getting logs when file doesn't exist"""
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = "/nonexistent/file.log"
        
        result = await logger.get_logs()
        
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_system(self):
        """Test getting system logs by type"""
        log_file = os.path.join(self.temp_dir, "system.log")
        log_entries = [
            '{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "System log"}'
        ]
        
        with open(log_file, 'w') as f:
            for entry in log_entries:
                f.write(entry + "\n")
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = log_file
        
        result = await logger.get_logs_by_type("system", limit=10)
        
        assert len(result) == 1
        assert result[0]["message"] == "System log"
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_error(self):
        """Test getting error logs by type"""
        error_log_file = os.path.join(self.temp_dir, "error.log")
        log_entries = [
            '{"timestamp": "2024-01-01T12:00:00", "level": "ERROR", "message": "Error log"}'
        ]
        
        with open(error_log_file, 'w') as f:
            for entry in log_entries:
                f.write(entry + "\n")
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.error_log_file = error_log_file
        
        result = await logger.get_logs_by_type("error", limit=10)
        
        assert len(result) == 1
        assert result[0]["message"] == "Error log"
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_ingest(self):
        """Test getting ingest logs by type"""
        ingest_log_file = os.path.join(self.temp_dir, "ingest.log")
        log_entries = [
            '{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "Ingest log"}'
        ]
        
        with open(ingest_log_file, 'w') as f:
            for entry in log_entries:
                f.write(entry + "\n")
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.ingest_log_file = ingest_log_file
        
        result = await logger.get_logs_by_type("ingest", limit=10)
        
        assert len(result) == 1
        assert result[0]["message"] == "Ingest log"


class TestLoggerMaintenance:
    """Test log maintenance functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_clear_logs(self):
        """Test clearing all log files"""
        # Create test log files
        log_files = ["system.log", "error.log", "ingest.log"]
        for filename in log_files:
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, 'w') as f:
                f.write("test log content\n")
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = os.path.join(self.temp_dir, "system.log")
            logger.error_log_file = os.path.join(self.temp_dir, "error.log")
            logger.ingest_log_file = os.path.join(self.temp_dir, "ingest.log")
        
        await logger.clear_logs()
        
        # All files should be empty
        for filename in log_files:
            filepath = os.path.join(self.temp_dir, filename)
            with open(filepath, 'r') as f:
                assert f.read() == ""
    
    def test_rotate_logs_needed(self):
        """Test log rotation when file exceeds size limit"""
        log_file = os.path.join(self.temp_dir, "system.log")
        
        # Create a large log file
        with open(log_file, 'w') as f:
            # Write more than 1MB of data
            f.write("x" * (2 * 1024 * 1024))
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = log_file
        
        with patch('shutil.move') as mock_move:
            logger.rotate_logs(max_size_mb=1)  # 1MB limit
            
            # Should attempt to move the file
            mock_move.assert_called()
    
    def test_rotate_logs_not_needed(self):
        """Test log rotation when file is under size limit"""
        log_file = os.path.join(self.temp_dir, "system.log")
        
        # Create a small log file
        with open(log_file, 'w') as f:
            f.write("small content")
        
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = log_file
        
        with patch('shutil.move') as mock_move:
            logger.rotate_logs(max_size_mb=10)  # 10MB limit
            
            # Should not attempt to move the file
            mock_move.assert_not_called()
    
    def test_rotate_logs_file_not_exists(self):
        """Test log rotation when file doesn't exist"""
        with patch('os.makedirs'):
            logger = Logger()
            logger.log_file = "/nonexistent/system.log"
        
        # Should not raise exception
        logger.rotate_logs()


class TestDatabaseLogger:
    """Test DatabaseLogger functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.mock_db = MagicMock()
        
    def test_database_logger_init_with_db(self):
        """Test DatabaseLogger initialization with database manager"""
        db_logger = DatabaseLogger(self.mock_db)
        
        assert db_logger.db_manager is self.mock_db
    
    def test_database_logger_init_without_db(self):
        """Test DatabaseLogger initialization without database manager"""
        with patch('utils.logger.DatabaseManager') as mock_db_class:
            mock_db_instance = MagicMock()
            mock_db_class.return_value = mock_db_instance
            
            db_logger = DatabaseLogger()
            
            assert db_logger.db_manager is mock_db_instance
    
    def test_ensure_log_table(self):
        """Test log table creation"""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        db_logger = DatabaseLogger(self.mock_db)
        db_logger._ensure_log_table(mock_connection)
        
        # Should execute CREATE TABLE statement
        mock_cursor.execute.assert_called()
        create_table_call = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE" in create_table_call
        assert "system_logs" in create_table_call
    
    @pytest.mark.asyncio
    async def test_database_write_log(self):
        """Test writing log to database"""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        
        db_logger = DatabaseLogger(self.mock_db)
        
        with patch.object(db_logger, '_ensure_log_table'):
            await db_logger._write_log(
                LogLevel.INFO,
                "Test message",
                None,
                {"key": "value"}
            )
            
            # Should insert into database
            mock_cursor.execute.assert_called()
            insert_call = mock_cursor.execute.call_args[0][0]
            assert "INSERT INTO" in insert_call
            assert "system_logs" in insert_call
    
    @pytest.mark.asyncio
    async def test_database_get_logs(self):
        """Test getting logs from database"""
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock database results
        mock_cursor.fetchall.return_value = [
            (1, "2024-01-01T12:00:00", "INFO", "Test message", None, '{"key": "value"}')
        ]
        mock_cursor.description = [
            ("id",), ("timestamp",), ("level",), ("message",), ("exception_details",), ("metadata",)
        ]
        
        self.mock_db.get_connection.return_value = mock_connection
        
        db_logger = DatabaseLogger(self.mock_db)
        result = await db_logger.get_logs(limit=10)
        
        assert len(result) == 1
        assert result[0]["level"] == "INFO"
        assert result[0]["message"] == "Test message"