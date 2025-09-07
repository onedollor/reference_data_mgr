"""
Comprehensive logger tests targeting 100% coverage
Focus on the 11 missing lines: 14-15, 166-168, 200-202, 233-234, 407
"""

import pytest
import os
import tempfile
import shutil
import json
import sys
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from datetime import datetime

# Import test classes to ensure proper discovery
@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


class TestLoggerZoneInfoImport:
    """Test ZoneInfo import fallback - covers lines 14-15 and 43-44"""
    
    def test_zoneinfo_import_error_fallback(self):
        """Test ZoneInfo ImportError fallback - covers lines 14-15"""
        # Test by mocking ZoneInfo directly in utils.logger
        with patch('utils.logger.ZoneInfo', None):
            from utils.logger import Logger
            logger = Logger()
            # When ZoneInfo is None, _tz should remain None
            assert logger._tz is None
    
    def test_zoneinfo_exception_handling(self):
        """Test ZoneInfo exception handling - covers lines 43-44"""
        # Create a mock ZoneInfo that raises exception
        mock_zoneinfo = MagicMock(side_effect=Exception("Invalid timezone"))
        
        with patch('utils.logger.ZoneInfo', mock_zoneinfo):
            from utils.logger import Logger
            logger = Logger()
            # When ZoneInfo raises exception, _tz should be None
            assert logger._tz is None


class TestLoggerExceptionHandling:
    """Test exception handling in logger methods"""
    
    @pytest.fixture
    def logger_with_temp_dir(self, temp_dir):
        """Logger with temporary directory"""
        from utils.logger import Logger
        logger = Logger()
        logger.log_file = os.path.join(temp_dir, "system.log")
        logger.error_log_file = os.path.join(temp_dir, "error.log")
        logger.ingest_log_file = os.path.join(temp_dir, "ingest.log")
        return logger
    
    @pytest.mark.asyncio
    async def test_get_logs_exception_handling(self, logger_with_temp_dir):
        """Test get_logs exception handling - covers lines 166-168"""
        logger = logger_with_temp_dir
        
        # Create a log file that will cause a read exception
        with open(logger.log_file, 'w') as f:
            f.write("valid json line\n")
        
        # Mock aiofiles.open to raise an exception during file reading
        with patch('aiofiles.open', side_effect=OSError("File read error")):
            result = await logger.get_logs()
            
            # Should return empty list due to exception
            assert result == []
    
    @pytest.mark.asyncio 
    async def test_get_logs_by_type_exception_handling(self, logger_with_temp_dir):
        """Test get_logs_by_type exception handling - covers lines 200-202"""
        logger = logger_with_temp_dir
        
        # Create system log file
        with open(logger.log_file, 'w') as f:
            f.write('{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "test"}\n')
        
        # Mock open() to raise an exception 
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            result = await logger.get_logs_by_type("system")
            
            # Should return empty list due to exception
            assert result == []
    
    def test_rotate_logs_exception_handling(self, logger_with_temp_dir):
        """Test rotate_logs exception handling - covers lines 233-234"""  
        logger = logger_with_temp_dir
        
        # Create a large log file to trigger rotation
        large_content = "x" * 2000  # Larger than 1KB default
        with open(logger.log_file, 'w') as f:
            f.write(large_content)
        
        # Mock os.rename to raise an exception
        with patch('os.rename', side_effect=OSError("Rename failed")):
            with patch('builtins.print') as mock_print:
                logger.rotate_logs(max_size_mb=0.001)  # Very small size to force rotation
                
                # Should print error message due to exception
                error_calls = [call for call in mock_print.call_args_list 
                             if 'Failed to rotate logs' in str(call)]
                assert len(error_calls) >= 1


class TestDatabaseLoggerExceptionHandling:
    """Test DatabaseLogger exception handling"""
    
    @pytest.fixture
    def mock_db(self):
        """Mock database manager"""
        mock_db = MagicMock()
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_connection
        return mock_db
    
    @pytest.mark.asyncio
    async def test_database_logger_timestamp_exception_handling(self, mock_db):
        """Test timestamp conversion exception handling - covers line 407"""
        from utils.logger import DatabaseLogger, LogLevel
        
        # Mock database connection
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_connection
        
        # Create a problematic timestamp that will cause astimezone() to fail
        problematic_timestamp = MagicMock()
        problematic_timestamp.tzinfo = None
        problematic_timestamp.astimezone = MagicMock(side_effect=Exception("astimezone failed"))
        problematic_timestamp.isoformat.return_value = "2024-01-01T12:00:00"
        
        # Mock fetchall to return data with problematic timestamp
        mock_cursor.fetchall.return_value = [
            (1, problematic_timestamp, "INFO", "test_action", "Test message", None, "{}")
        ]
        mock_cursor.description = [
            ("id",), ("timestamp",), ("level",), ("action_step",), 
            ("message",), ("exception_details",), ("metadata",)
        ]
        
        db_logger = DatabaseLogger(mock_db)
        # Mock the _tz to be set (to trigger the timezone conversion path)
        db_logger._tz = MagicMock()
        
        result = await db_logger.get_logs(limit=1)
        
        # Should handle the exception and fall back to basic isoformat()
        assert len(result) >= 1
        # The timestamp_local should use the fallback isoformat()
        if result:
            assert "timestamp_local" in result[0]


class TestLoggerFileIOExceptions:
    """Test various file I/O exception scenarios"""
    
    @pytest.fixture
    def logger_with_temp_dir(self, temp_dir):
        """Logger with temporary directory"""
        from utils.logger import Logger
        logger = Logger()
        logger.log_file = os.path.join(temp_dir, "system.log")
        logger.error_log_file = os.path.join(temp_dir, "error.log") 
        logger.ingest_log_file = os.path.join(temp_dir, "ingest.log")
        return logger
    
    @pytest.mark.asyncio
    async def test_get_logs_file_does_not_exist(self, logger_with_temp_dir):
        """Test get_logs when file doesn't exist"""
        logger = logger_with_temp_dir
        
        # Don't create the log file - should return empty list
        result = await logger.get_logs()
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_file_does_not_exist(self, logger_with_temp_dir):
        """Test get_logs_by_type when file doesn't exist"""
        logger = logger_with_temp_dir
        
        # Don't create the log file - should return empty list
        result = await logger.get_logs_by_type("system")
        assert result == []
    
    @pytest.mark.asyncio
    async def test_get_logs_invalid_json_handling(self, logger_with_temp_dir):
        """Test handling of invalid JSON lines in log files"""
        logger = logger_with_temp_dir
        
        # Create log file with mix of valid and invalid JSON
        with open(logger.log_file, 'w') as f:
            f.write('{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "valid"}\n')
            f.write('invalid json line\n')  # This should be skipped
            f.write('{"timestamp": "2024-01-01T12:01:00", "level": "ERROR", "message": "valid2"}\n')
        
        result = await logger.get_logs()
        
        # Should return only valid JSON entries
        assert len(result) == 2
        assert result[0]["message"] == "valid"
        assert result[1]["message"] == "valid2"
    
    def test_rotate_logs_file_operations(self, logger_with_temp_dir):
        """Test rotate_logs file operations and edge cases"""
        logger = logger_with_temp_dir
        
        # Create files with different sizes
        with open(logger.log_file, 'w') as f:
            f.write("x" * 2000)  # Larger than 1KB
            
        with open(logger.error_log_file, 'w') as f:
            f.write("y" * 500)  # Smaller than 1KB
        
        # Create ingest log that doesn't exist initially
        # (ingest_log_file is not created)
        
        with patch('builtins.print') as mock_print:
            logger.rotate_logs(max_size_mb=0.001)  # Very small size
            
            # Should rotate the large file and print rotation message
            rotation_calls = [call for call in mock_print.call_args_list 
                            if 'Rotated' in str(call) and 'system' in str(call)]
            assert len(rotation_calls) >= 1


class TestLoggerEdgeCases:
    """Test edge cases and boundary conditions"""
    
    @pytest.fixture
    def logger_with_temp_dir(self, temp_dir):
        """Logger with temporary directory"""
        from utils.logger import Logger
        logger = Logger()
        logger.log_file = os.path.join(temp_dir, "system.log")
        logger.error_log_file = os.path.join(temp_dir, "error.log")
        logger.ingest_log_file = os.path.join(temp_dir, "ingest.log")
        return logger
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_unknown_type(self, logger_with_temp_dir):
        """Test get_logs_by_type with unknown log type"""
        logger = logger_with_temp_dir
        
        # Should return empty list for unknown log type
        result = await logger.get_logs_by_type("unknown")
        assert result == []
    
    @pytest.mark.asyncio 
    async def test_multiple_exception_scenarios(self, logger_with_temp_dir):
        """Test multiple exception scenarios in sequence"""
        logger = logger_with_temp_dir
        
        # Test file permission errors
        with patch('aiofiles.open', side_effect=[PermissionError("No access"), OSError("IO Error")]):
            result1 = await logger.get_logs()
            result2 = await logger.get_logs_by_type("system")
            
            assert result1 == []
            assert result2 == []
        
        # Test with actual files that can be read
        with open(logger.log_file, 'w') as f:
            f.write('{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "test"}\n')
        
        result3 = await logger.get_logs()
        assert len(result3) == 1
        assert result3[0]["message"] == "test"


class TestDatabaseLoggerAdvanced:
    """Test advanced DatabaseLogger scenarios"""
    
    @pytest.fixture
    def mock_db(self):
        """Mock database manager"""
        mock_db = MagicMock()
        return mock_db
    
    @pytest.mark.asyncio
    async def test_database_logger_timezone_edge_cases(self, mock_db):
        """Test timezone handling edge cases in DatabaseLogger"""
        from utils.logger import DatabaseLogger
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_connection
        
        # Test with timestamp that has tzinfo but astimezone fails
        problematic_ts = MagicMock()
        problematic_ts.tzinfo = MagicMock()  # Has tzinfo
        problematic_ts.replace.return_value = MagicMock()
        problematic_ts.replace.return_value.astimezone = MagicMock(side_effect=Exception("astimezone error"))
        problematic_ts.isoformat.return_value = "2024-01-01T12:00:00+00:00"
        
        mock_cursor.fetchall.return_value = [
            (1, problematic_ts, "INFO", "test_action", "Test message", None, "{}")
        ]
        mock_cursor.description = [
            ("id",), ("timestamp",), ("level",), ("action_step",), 
            ("message",), ("exception_details",), ("metadata",)
        ]
        
        db_logger = DatabaseLogger(mock_db)
        db_logger._tz = MagicMock()  # Set timezone
        
        result = await db_logger.get_logs(limit=1)
        
        # Should handle the exception gracefully
        assert len(result) >= 1
        if result:
            assert "timestamp_local" in result[0]
    
    @pytest.mark.asyncio
    async def test_database_logger_no_timezone_set(self, mock_db):
        """Test DatabaseLogger when no timezone is set"""
        from utils.logger import DatabaseLogger
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_connection
        
        test_timestamp = datetime(2024, 1, 1, 12, 0, 0)
        
        mock_cursor.fetchall.return_value = [
            (1, test_timestamp, "INFO", "test_action", "Test message", None, "{}")
        ]
        mock_cursor.description = [
            ("id",), ("timestamp",), ("level",), ("action_step",), 
            ("message",), ("exception_details",), ("metadata",)
        ]
        
        db_logger = DatabaseLogger(mock_db)
        db_logger._tz = None  # No timezone set - should trigger line 407
        
        result = await db_logger.get_logs(limit=1)
        
        # Should handle the no-timezone case
        assert len(result) >= 1
        if result:
            assert result[0]["timestamp_local"] == test_timestamp.isoformat()