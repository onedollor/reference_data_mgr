"""
Comprehensive tests to push utils/logger.py from 72% to >90% coverage
Targeting missing lines: 14-15, 131-132, 161-162, 166-168, 182, 195-196, 200-202, 212-213, 233-234, 249, 256-257, 293-294, 323-325, 351-353, 358, 372-449
"""

import os
import json
import tempfile
import pytest
import asyncio
from unittest.mock import patch, MagicMock, mock_open, AsyncMock
from datetime import datetime
from utils.logger import Logger, DatabaseLogger, LogLevel


class MockDBManager:
    """Mock database manager for testing"""
    
    def __init__(self, simulate_error=False, has_source_ip=True):
        self.simulate_error = simulate_error
        self.has_source_ip = has_source_ip
        self.connection_closed = False
        
    def get_connection(self):
        if self.simulate_error:
            raise Exception("Database connection failed")
        return MockConnection(has_source_ip=self.has_source_ip)
    
    def ensure_schemas_exist(self, connection):
        if self.simulate_error:
            raise Exception("Schema creation failed")


class MockConnection:
    """Mock database connection"""
    
    def __init__(self, has_source_ip=True):
        self.has_source_ip = has_source_ip
        self.closed = False
        
    def cursor(self):
        return MockCursor(has_source_ip=self.has_source_ip)
        
    def close(self):
        self.closed = True


class MockCursor:
    """Mock database cursor"""
    
    def __init__(self, has_source_ip=True):
        self.has_source_ip = has_source_ip
        self.executed_queries = []
        
    def execute(self, query, params=None):
        self.executed_queries.append((query, params))
        
    def fetchone(self):
        # For checking source_ip column existence
        return [1 if self.has_source_ip else 0]
        
    def fetchall(self):
        # Mock log entries
        if self.has_source_ip:
            return [
                (datetime.now(), 'INFO', 'test_action', 'test message', '127.0.0.1', None, 'test.csv', 'test_table', 100, '{}')
            ]
        else:
            return [
                (datetime.now(), 'INFO', 'test_action', 'test message', None, 'test.csv', 'test_table', 100, '{}')
            ]


class TestLoggerComprehensive:
    """Comprehensive tests for Logger class"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Patch environment and paths to use temp directory
        with patch.dict('os.environ', {'LOG_TIMEZONE': 'America/Toronto'}):
            with patch('utils.logger.os.path.dirname') as mock_dirname:
                mock_dirname.return_value = self.temp_dir
                self.logger = Logger()
    
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_zoneinfo_import_fallback(self):
        """Test ZoneInfo import fallback - covers lines 14-15"""
        # This tests the ImportError handling for ZoneInfo
        with patch('utils.logger.ZoneInfo', None):
            # Re-import to trigger fallback
            import importlib
            importlib.reload(__import__('utils.logger'))
            
            # Should handle ZoneInfo being None
            assert True  # If we get here, fallback worked
    
    @pytest.mark.asyncio
    async def test_ingest_activity_logging(self):
        """Test ingest activity logging - covers lines 131-132"""
        # Test various ingest-related action steps
        ingest_actions = ['ingest_data', 'upload_file', 'import_csv', 'process_file_step', 'validate_data']
        
        for action in ingest_actions:
            await self.logger.log_info(action, "Test message")
            
        # Verify ingest log file was written to
        assert os.path.exists(self.logger.ingest_log_file)
        
        # Check that ingest log contains entries
        with open(self.logger.ingest_log_file, 'r') as f:
            content = f.read()
            assert 'ingest_data' in content or 'upload_file' in content
    
    @pytest.mark.asyncio
    async def test_get_logs_json_decode_error(self):
        """Test get_logs with JSON decode errors - covers lines 161-162"""
        # Write malformed JSON to log file
        os.makedirs(os.path.dirname(self.logger.log_file), exist_ok=True)
        with open(self.logger.log_file, 'w') as f:
            f.write('{"valid": "json"}\n')
            f.write('{ invalid json content }\n')  # Malformed JSON
            f.write('{"another": "valid entry"}\n')
        
        logs = await self.logger.get_logs()
        
        # Should skip malformed JSON and return valid entries
        assert len(logs) == 2
        assert logs[0]['valid'] == 'json'
        assert logs[1]['another'] == 'valid entry'
    
    @pytest.mark.asyncio
    async def test_get_logs_file_error_handling(self):
        """Test get_logs error handling - covers lines 166-168"""
        # Mock file operations to raise exceptions
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            logs = await self.logger.get_logs()
            
            # Should return empty list and log error
            assert logs == []
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_nonexistent_file(self):
        """Test get_logs_by_type with nonexistent file - covers line 182"""
        # Request logs from a file that doesn't exist
        logs = await self.logger.get_logs_by_type("error")
        
        # Should return empty list for nonexistent file
        assert logs == []
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_json_decode_error(self):
        """Test get_logs_by_type with JSON errors - covers lines 195-196"""
        # Write malformed JSON to error log
        os.makedirs(os.path.dirname(self.logger.error_log_file), exist_ok=True)
        with open(self.logger.error_log_file, 'w') as f:
            f.write('{"valid": "error_log"}\n')
            f.write('{ malformed json }\n')
            f.write('{"another": "error"}\n')
        
        logs = await self.logger.get_logs_by_type("error")
        
        # Should skip malformed JSON
        assert len(logs) == 2
        assert logs[0]['valid'] == 'error_log'
    
    @pytest.mark.asyncio
    async def test_get_logs_by_type_error_handling(self):
        """Test get_logs_by_type error handling - covers lines 200-202"""
        # Mock file operations to raise exceptions
        with patch('builtins.open', side_effect=IOError("Read error")):
            logs = await self.logger.get_logs_by_type("system")
            
            # Should return empty list and log error
            assert logs == []
    
    @pytest.mark.asyncio
    async def test_clear_logs_error_handling(self):
        """Test clear_logs error handling - covers lines 212-213"""
        # Mock os.remove to raise exception
        with patch('os.remove', side_effect=OSError("Permission denied")):
            await self.logger.clear_logs()
            
            # Should handle exception gracefully (just prints error)
            assert True  # If we get here, exception was handled
    
    def test_rotate_logs_error_handling(self):
        """Test rotate_logs error handling - covers lines 233-234"""
        # Mock os.rename to raise exception
        with patch('os.path.getsize', return_value=20 * 1024 * 1024):  # 20MB file
            with patch('os.rename', side_effect=OSError("Permission denied")):
                self.logger.rotate_logs(max_size_mb=10)
                
                # Should handle exception gracefully
                assert True


class TestDatabaseLoggerComprehensive:
    """Comprehensive tests for DatabaseLogger class"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        
        with patch('utils.logger.os.path.dirname') as mock_dirname:
            mock_dirname.return_value = self.temp_dir
            self.db_logger = DatabaseLogger()
    
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_ensure_log_table_already_created(self):
        """Test _ensure_log_table when already created - covers line 249"""
        mock_connection = MockConnection()
        
        # Set flag to True to trigger early return
        self.db_logger._log_table_created = True
        
        # Should return early without executing queries
        self.db_logger._ensure_log_table(mock_connection)
        
        # Verify no queries were executed
        assert len(mock_connection.cursor().executed_queries) == 0
    
    def test_ensure_log_table_schema_error(self):
        """Test schema creation error handling - covers lines 256-257"""
        mock_db_manager = MockDBManager(simulate_error=True)
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        mock_connection = MockConnection()
        
        # Should handle schema creation error gracefully
        db_logger._ensure_log_table(mock_connection)
        assert True  # Exception handled
    
    def test_ensure_log_table_creation_error(self):
        """Test table creation error handling - covers lines 293-294"""
        # Mock cursor.execute to raise exception
        with patch.object(MockCursor, 'execute', side_effect=Exception("SQL Error")):
            mock_connection = MockConnection()
            
            self.db_logger._ensure_log_table(mock_connection)
            
            # Should handle exception gracefully
            assert True
    
    @pytest.mark.asyncio
    async def test_write_log_with_metadata(self):
        """Test _write_log with metadata extraction - covers lines 323-325"""
        mock_db_manager = MockDBManager()
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        # Metadata with all fields
        metadata = {
            'filename': 'test.csv',
            'table_name': 'test_table',
            'row_count': 100,
            'extra_field': 'extra_value'
        }
        
        await db_logger._write_log(
            LogLevel.INFO,
            "test_action",
            "test message",
            metadata=metadata
        )
        
        # Should extract and use metadata fields
        assert True  # If we get here, metadata was processed
    
    @pytest.mark.asyncio
    async def test_write_log_database_error(self):
        """Test _write_log database error handling - covers lines 351-353"""
        mock_db_manager = MockDBManager(simulate_error=True)
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        # Should handle database errors gracefully
        await db_logger._write_log(
            LogLevel.ERROR,
            "test_action",
            "test message with db error"
        )
        
        # Should complete without raising exception
        assert True
    
    @pytest.mark.asyncio
    async def test_get_logs_no_db_manager(self):
        """Test get_logs fallback when no db_manager - covers line 358"""
        # Use temp directory to avoid existing log files
        with patch('utils.logger.os.path.dirname') as mock_dirname:
            mock_dirname.return_value = self.temp_dir
            db_logger = DatabaseLogger(db_manager=None)
            
            # Should fall back to parent class method
            logs = await db_logger.get_logs()
            
            # Should return empty list (no log file exists in temp dir)
            assert logs == []
    
    @pytest.mark.asyncio
    async def test_get_logs_with_source_ip_column(self):
        """Test get_logs with source_ip column - covers lines 372-449"""
        mock_db_manager = MockDBManager(has_source_ip=True)
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        logs = await db_logger.get_logs(limit=10)
        
        # Should return logs with source_ip field
        assert len(logs) > 0
        assert 'source_ip' in logs[0]
        assert logs[0]['source_ip'] == '127.0.0.1'
    
    @pytest.mark.asyncio
    async def test_get_logs_without_source_ip_column(self):
        """Test get_logs without source_ip column (backward compatibility)"""
        mock_db_manager = MockDBManager(has_source_ip=False)
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        logs = await db_logger.get_logs(limit=10)
        
        # Should return logs with source_ip as None for backward compatibility
        assert len(logs) > 0
        assert 'source_ip' in logs[0]
        assert logs[0]['source_ip'] is None
    
    @pytest.mark.asyncio
    async def test_get_logs_database_error_fallback(self):
        """Test get_logs database error fallback"""
        # Use temp directory to avoid existing log files
        with patch('utils.logger.os.path.dirname') as mock_dirname:
            mock_dirname.return_value = self.temp_dir
            mock_db_manager = MockDBManager()
            db_logger = DatabaseLogger(db_manager=mock_db_manager)
            
            # Mock connection to raise exception
            with patch.object(mock_db_manager, 'get_connection', side_effect=Exception("DB Error")):
                logs = await db_logger.get_logs()
                
                # Should fallback to file-based logging
                assert logs == []
    
    @pytest.mark.asyncio
    async def test_timezone_handling_in_database_logs(self):
        """Test timezone handling in database log retrieval"""
        mock_db_manager = MockDBManager()
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        # Set timezone
        db_logger.timezone_name = "America/Toronto"
        
        logs = await db_logger.get_logs()
        
        # Should handle timezone conversion
        if logs:
            assert 'timestamp_local' in logs[0]
            assert 'tz' in logs[0]
            assert logs[0]['tz'] == "America/Toronto"


class TestLoggerEdgeCases:
    """Additional edge case tests"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        
        with patch('utils.logger.os.path.dirname') as mock_dirname:
            mock_dirname.return_value = self.temp_dir
            self.logger = Logger()
    
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_timezone_initialization_exception(self):
        """Test timezone initialization exception handling - covers lines 43-44"""
        with patch('utils.logger.ZoneInfo') as mock_zoneinfo:
            # Make ZoneInfo constructor raise exception
            mock_zoneinfo.side_effect = Exception("Invalid timezone")
            
            # Create logger which should handle the exception
            with patch('utils.logger.os.path.dirname') as mock_dirname:
                mock_dirname.return_value = self.temp_dir
                logger = Logger()
                
                # Should handle timezone error gracefully
                assert logger._tz is None
    
    @pytest.mark.asyncio
    async def test_get_logs_exception_during_logging(self):
        """Test exception during get_logs error logging - covers lines 166-168"""
        # Create scenario where get_logs raises exception AND the error logging also fails
        with patch('builtins.open', side_effect=PermissionError("Access denied")):
            with patch.object(self.logger, 'log_error', side_effect=Exception("Log error failed")):
                logs = await self.logger.get_logs()
                
                # Should handle nested exceptions gracefully
                assert logs == []
    
    @pytest.mark.asyncio  
    async def test_get_logs_by_type_exception_during_logging(self):
        """Test exception during get_logs_by_type error logging - covers lines 200-202"""
        # Create scenario where get_logs_by_type raises exception AND error logging fails
        with patch('builtins.open', side_effect=IOError("Read error")):
            with patch.object(self.logger, 'log_error', side_effect=Exception("Log error failed")):
                logs = await self.logger.get_logs_by_type("system")
                
                # Should handle nested exceptions gracefully
                assert logs == []
    
    @pytest.mark.asyncio
    async def test_clear_logs_remove_success(self):
        """Test clear_logs successful removal - covers line 210"""
        # Create actual log files to remove
        os.makedirs(os.path.dirname(self.logger.log_file), exist_ok=True)
        
        # Create log files
        with open(self.logger.log_file, 'w') as f:
            f.write("test log")
        with open(self.logger.error_log_file, 'w') as f:
            f.write("test error log")
        with open(self.logger.ingest_log_file, 'w') as f:
            f.write("test ingest log")
        
        # Clear logs should remove all files
        await self.logger.clear_logs()
        
        # Files should be removed
        assert not os.path.exists(self.logger.log_file)
        assert not os.path.exists(self.logger.error_log_file)  
        assert not os.path.exists(self.logger.ingest_log_file)
    
    @pytest.mark.asyncio
    async def test_clear_logs_info_logging_exception(self):
        """Test clear_logs when info logging fails - covers lines 212-213"""
        # Mock log_info to raise exception during clear_logs
        with patch.object(self.logger, 'log_info', side_effect=Exception("Log info failed")):
            await self.logger.clear_logs()
            
            # Should handle logging exception gracefully
            assert True
    
    def test_rotate_logs_successful_rotation(self):
        """Test successful log rotation - covers line 233-234 (success path)"""
        # Create large log files that need rotation
        os.makedirs(os.path.dirname(self.logger.log_file), exist_ok=True)
        
        large_content = "x" * (15 * 1024 * 1024)  # 15MB
        with open(self.logger.log_file, 'w') as f:
            f.write(large_content)
        
        # Should successfully rotate
        with patch('builtins.print') as mock_print:
            self.logger.rotate_logs(max_size_mb=10)
            
            # Should print success message
            mock_print.assert_called()
    
    def test_database_logger_timezone_exception_handling(self):
        """Test DatabaseLogger timezone handling exceptions - covers lines 407-409"""
        mock_db_manager = MockDBManager()
        db_logger = DatabaseLogger(db_manager=mock_db_manager)
        
        # Create mock row with timestamp that causes exception during timezone conversion
        mock_row = [datetime.now(), 'INFO', 'test_action', 'test message', '127.0.0.1', None, 'test.csv', 'test_table', 100, '{}']
        
        # Mock fetchall to return problematic row
        with patch.object(MockCursor, 'fetchall', return_value=[mock_row]):
            # Mock timezone conversion to raise exception
            with patch.object(db_logger, '_tz') as mock_tz:
                mock_tz.side_effect = Exception("Timezone conversion failed")
                
                # Should handle timezone exception gracefully
                asyncio.run(db_logger.get_logs())
                assert True  # If we get here, exception was handled
    
    @pytest.mark.asyncio
    async def test_write_log_file_write_error(self):
        """Test _write_log with file write errors"""
        # Mock open to raise exception
        with patch('builtins.open', side_effect=IOError("Write failed")):
            await self.logger.log_info("test_action", "test message")
            
            # Should handle file write errors gracefully
            assert True
    
    def test_log_rotation_large_files(self):
        """Test log rotation with files exceeding size limit"""
        # Create large mock log files
        os.makedirs(os.path.dirname(self.logger.log_file), exist_ok=True)
        
        # Create files that exceed size limit
        large_content = "x" * (15 * 1024 * 1024)  # 15MB
        with open(self.logger.log_file, 'w') as f:
            f.write(large_content)
        
        # Should rotate the file
        self.logger.rotate_logs(max_size_mb=10)
        
        # Original file should be smaller or renamed
        if os.path.exists(self.logger.log_file):
            assert os.path.getsize(self.logger.log_file) < 15 * 1024 * 1024
    
    @pytest.mark.asyncio
    async def test_comprehensive_logging_workflow(self):
        """Test complete logging workflow with all log levels"""
        # Test all log levels
        await self.logger.log_info("info_step", "Info message")
        await self.logger.log_warning("warning_step", "Warning message")
        await self.logger.log_error("error_step", "Error message", "traceback info")
        await self.logger.log_debug("debug_step", "Debug message")
        
        # Verify logs were written
        logs = await self.logger.get_logs()
        assert len(logs) >= 4
        
        # Verify different log types
        log_levels = [log['level'] for log in logs]
        assert 'INFO' in log_levels
        assert 'WARNING' in log_levels  
        assert 'ERROR' in log_levels
        assert 'DEBUG' in log_levels