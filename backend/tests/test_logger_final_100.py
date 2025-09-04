"""
Final push for 100% logger coverage - targeting the last 6 missing lines: 14-15, 166-168, 407
"""

import pytest
import os
import tempfile
import shutil
import sys
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


class TestZoneInfoImportFallback:
    """Target lines 14-15: ZoneInfo import fallback"""
    
    def test_zoneinfo_import_error_coverage(self):
        """Force ZoneInfo ImportError at import time - covers lines 14-15"""
        # Save original import function
        original_import = __builtins__['__import__']
        
        def mock_import(name, *args, **kwargs):
            if name == 'zoneinfo':
                raise ImportError("No module named 'zoneinfo'")
            return original_import(name, *args, **kwargs)
        
        # Temporarily replace import function
        __builtins__['__import__'] = mock_import
        
        try:
            # Force reload of utils.logger to trigger import error path
            if 'utils.logger' in sys.modules:
                del sys.modules['utils.logger']
            
            # Import should work with fallback
            import utils.logger
            
            # ZoneInfo should be None due to import error
            assert utils.logger.ZoneInfo is None
            
        finally:
            # Restore original import
            __builtins__['__import__'] = original_import


class TestGetLogsExceptionCoverage:
    """Target lines 166-168: get_logs exception handling"""
    
    @pytest.fixture
    def logger_with_temp_dir(self, temp_dir):
        """Logger with temporary directory"""
        from utils.logger import Logger
        logger = Logger()
        logger.log_file = os.path.join(temp_dir, "system.log")
        return logger
    
    @pytest.mark.asyncio
    async def test_get_logs_exception_path(self, logger_with_temp_dir):
        """Force exception in get_logs method - covers lines 166-168"""
        logger = logger_with_temp_dir
        
        # Create a valid log file
        with open(logger.log_file, 'w') as f:
            f.write('{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "test"}\n')
        
        # Mock aiofiles.open to raise an exception AFTER os.path.exists passes
        mock_file = MagicMock()
        
        async def mock_context_manager():
            raise OSError("Simulated file read error")
        
        mock_file.__aenter__ = mock_context_manager
        mock_file.__aexit__ = MagicMock()
        
        with patch('aiofiles.open', return_value=mock_file):
            result = await logger.get_logs()
            
            # Should catch exception and return empty list
            assert result == []
    
    @pytest.mark.asyncio
    async def test_get_logs_readline_exception(self, logger_with_temp_dir):
        """Force exception during file reading in get_logs - covers lines 166-168"""
        logger = logger_with_temp_dir
        
        # Create log file
        with open(logger.log_file, 'w') as f:
            f.write('{"timestamp": "2024-01-01T12:00:00", "level": "INFO", "message": "test"}\n')
        
        # Mock the async file context manager to raise exception on readlines
        mock_file = MagicMock()
        mock_file.readlines = MagicMock(side_effect=OSError("Read error"))
        
        async def mock_aenter():
            return mock_file
        
        async def mock_aexit(*args):
            pass
        
        mock_context = MagicMock()
        mock_context.__aenter__ = mock_aenter
        mock_context.__aexit__ = mock_aexit
        
        with patch('aiofiles.open', return_value=mock_context):
            result = await logger.get_logs()
            
            # Should catch exception and return empty list
            assert result == []


class TestDatabaseTimestampExceptionCoverage:
    """Target line 407: Database timestamp exception handling"""
    
    @pytest.fixture
    def mock_db(self):
        """Mock database manager"""
        mock_db = MagicMock()
        return mock_db
    
    @pytest.mark.asyncio
    async def test_timestamp_isoformat_exception_coverage(self, mock_db):
        """Force exception in timestamp conversion - covers line 407"""
        from utils.logger import DatabaseLogger
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_db.get_connection.return_value = mock_connection
        
        # Create a timestamp that will cause astimezone to fail
        problematic_timestamp = MagicMock()
        problematic_timestamp.tzinfo = None
        
        # Mock hasattr to return True so astimezone is attempted
        def mock_hasattr(obj, attr):
            if attr == 'astimezone':
                return True
            return hasattr(datetime.now(), attr)
        
        # Mock astimezone to raise exception, then fallback to isoformat which also fails
        problematic_timestamp.astimezone = MagicMock(side_effect=Exception("astimezone failed"))
        problematic_timestamp.isoformat = MagicMock(side_effect=Exception("isoformat failed"))
        
        mock_cursor.fetchall.return_value = [
            (1, problematic_timestamp, "INFO", "test_action", "Test message", None, "{}")
        ]
        mock_cursor.description = [
            ("id",), ("timestamp",), ("level",), ("action_step",), 
            ("message",), ("exception_details",), ("metadata",)
        ]
        
        db_logger = DatabaseLogger(mock_db)
        db_logger._tz = MagicMock()  # Set timezone to trigger astimezone path
        
        with patch('builtins.hasattr', side_effect=mock_hasattr):
            result = await db_logger.get_logs(limit=1)
            
            # Should handle both exceptions gracefully
            assert len(result) >= 1


class TestComplexExceptionScenarios:
    """Test complex scenarios to force edge case coverage"""
    
    @pytest.fixture
    def logger_with_temp_dir(self, temp_dir):
        """Logger with temporary directory"""
        from utils.logger import Logger
        logger = Logger()
        logger.log_file = os.path.join(temp_dir, "system.log")
        return logger
    
    @pytest.mark.asyncio
    async def test_async_context_manager_exception(self, logger_with_temp_dir):
        """Test exception in async context manager"""
        logger = logger_with_temp_dir
        
        # Create file
        with open(logger.log_file, 'w') as f:
            f.write('{"test": "data"}\n')
        
        # Mock aiofiles.open to return a context manager that raises on entry
        async def failing_context_manager():
            raise PermissionError("Cannot access file")
        
        mock_context = MagicMock()
        mock_context.__aenter__ = failing_context_manager
        mock_context.__aexit__ = MagicMock()
        
        with patch('aiofiles.open', return_value=mock_context):
            result = await logger.get_logs()
            assert result == []
    
    @pytest.mark.asyncio
    async def test_file_operations_exception_chain(self, logger_with_temp_dir):
        """Test exception handling in file operations"""
        logger = logger_with_temp_dir
        
        # Create file
        with open(logger.log_file, 'w') as f:
            f.write('valid json\n')
        
        # Create a mock that raises different exceptions
        class FailingAsyncFile:
            async def __aenter__(self):
                return self
            
            async def __aexit__(self, *args):
                pass
            
            async def readlines(self):
                raise IOError("File read failed")
        
        with patch('aiofiles.open', return_value=FailingAsyncFile()):
            result = await logger.get_logs()
            assert result == []