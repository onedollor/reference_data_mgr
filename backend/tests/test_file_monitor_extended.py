"""
Extended test coverage for file_monitor.py
Focuses on covering missing lines and methods to increase coverage from 44% to higher level
"""

import pytest
import os
import hashlib
import tempfile
from unittest.mock import patch, MagicMock, mock_open


class TestFileMonitorExtended:
    """Extended tests for FileMonitor to improve coverage"""

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_check_file_stability_new_file(self, mock_get_logger, mock_basic_config, 
                                         mock_makedirs, mock_api, mock_db):
        """Test check_file_stability for a new file"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock os.stat to return file stats
        mock_stat = MagicMock()
        mock_stat.st_size = 1000
        mock_stat.st_mtime = 1234567890.0
        with patch('file_monitor.os.stat', return_value=mock_stat):
            
            result = monitor.check_file_stability("/test/new_file.csv")
            
            # New file should return False and be added to tracking
            assert result is False
            assert "/test/new_file.csv" in monitor.file_tracking
            assert monitor.file_tracking["/test/new_file.csv"]['size'] == 1000
            assert monitor.file_tracking["/test/new_file.csv"]['stable_count'] == 0

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_check_file_stability_changed_file(self, mock_get_logger, mock_basic_config, 
                                             mock_makedirs, mock_api, mock_db):
        """Test check_file_stability when file changes"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Pre-populate tracking with old file info
        monitor.file_tracking["/test/changed_file.csv"] = {
            'size': 500,
            'mtime': 1234567880.0,
            'stable_count': 3
        }
        
        # Mock new file size/mtime (file changed)
        mock_stat = MagicMock()
        mock_stat.st_size = 1000
        mock_stat.st_mtime = 1234567890.0
        with patch('file_monitor.os.stat', return_value=mock_stat):
            
            result = monitor.check_file_stability("/test/changed_file.csv")
            
            # Changed file should return False and reset counter
            assert result is False
            assert monitor.file_tracking["/test/changed_file.csv"]['size'] == 1000
            assert monitor.file_tracking["/test/changed_file.csv"]['mtime'] == 1234567890.0
            assert monitor.file_tracking["/test/changed_file.csv"]['stable_count'] == 0

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_check_file_stability_stable_file(self, mock_get_logger, mock_basic_config, 
                                            mock_makedirs, mock_api, mock_db):
        """Test check_file_stability when file becomes stable"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Pre-populate tracking with file close to stable
        monitor.file_tracking["/test/stable_file.csv"] = {
            'size': 1000,
            'mtime': 1234567890.0,
            'stable_count': file_monitor.STABILITY_CHECKS - 1
        }
        
        # Mock same file size/mtime (no change)
        mock_stat = MagicMock()
        mock_stat.st_size = 1000
        mock_stat.st_mtime = 1234567890.0
        with patch('file_monitor.os.stat', return_value=mock_stat):
            
            result = monitor.check_file_stability("/test/stable_file.csv")
            
            # Should now be stable
            assert result is True
            assert monitor.file_tracking["/test/stable_file.csv"]['stable_count'] == file_monitor.STABILITY_CHECKS

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_check_file_stability_os_error(self, mock_get_logger, mock_basic_config, 
                                         mock_makedirs, mock_api, mock_db):
        """Test check_file_stability with OS error"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock os.stat to raise OSError
        with patch('file_monitor.os.stat', side_effect=OSError("File not accessible")):
            
            result = monitor.check_file_stability("/test/error_file.csv")
            
            # Should return False on error
            assert result is False
            mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_detect_csv_format_success(self, mock_get_logger, mock_basic_config, 
                                     mock_makedirs, mock_api, mock_db):
        """Test detect_csv_format with successful backend response"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock successful API response
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": True,
            "detected_format": {
                "column_delimiter": ";",
                "sample_data": [["Name", "Age", "City"], ["John", "30", "NYC"]]
            }
        }
        
        monitor = file_monitor.FileMonitor()
        
        delimiter, headers, sample_rows = monitor.detect_csv_format("/test/file.csv")
        
        assert delimiter == ";"
        assert headers == ["Name", "Age", "City"]
        assert len(sample_rows) == 2

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_detect_csv_format_failure_fallback(self, mock_get_logger, mock_basic_config, 
                                               mock_makedirs, mock_api, mock_db):
        """Test detect_csv_format with backend failure, triggering fallback"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock failed API response
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": False,
            "error": "Backend detection failed"
        }
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file content for fallback detection
        mock_file_content = "Name,Age,City\nJohn,30,NYC\n"
        with patch('builtins.open', mock_open(read_data=mock_file_content)):
            delimiter, headers, sample_rows = monitor.detect_csv_format("/test/file.csv")
        
        # Should use fallback detection
        assert delimiter == ","
        assert headers == ["Name", "Age", "City"]
        mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_fallback_csv_detection_various_delimiters(self, mock_get_logger, mock_basic_config, 
                                                      mock_makedirs, mock_api, mock_db):
        """Test _fallback_csv_detection with different delimiters"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Test semicolon delimiter
        mock_content = "Name;Age;City;Country\nJohn;30;NYC;USA\n"
        with patch('builtins.open', mock_open(read_data=mock_content)):
            delimiter, headers, sample_rows = monitor._fallback_csv_detection("/test/file.csv")
        
        assert delimiter == ";"
        assert len(headers) == 4
        
        # Test pipe delimiter
        mock_content = "Name|Age|City|Country|State\nJohn|30|NYC|USA|NY\n"
        with patch('builtins.open', mock_open(read_data=mock_content)):
            delimiter, headers, sample_rows = monitor._fallback_csv_detection("/test/file.csv")
        
        assert delimiter == "|"
        assert len(headers) == 5

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_fallback_csv_detection_empty_file(self, mock_get_logger, mock_basic_config, 
                                             mock_makedirs, mock_api, mock_db):
        """Test _fallback_csv_detection with empty file"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Test empty file
        with patch('builtins.open', mock_open(read_data="")):
            delimiter, headers, sample_rows = monitor._fallback_csv_detection("/test/empty.csv")
        
        assert delimiter == ","
        assert headers == []
        assert sample_rows == []

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_extract_table_name_fallback(self, mock_get_logger, mock_basic_config, 
                                        mock_makedirs, mock_api, mock_db):
        """Test extract_table_name fallback logic"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock API to raise exception, triggering fallback
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.extract_table_name_from_file.side_effect = Exception("API failed")
        
        monitor = file_monitor.FileMonitor()
        
        # Test various filename patterns in fallback
        test_cases = [
            ("/path/to/users_20241225.csv", "users"),
            ("/path/to/Products-List_2024-12-25.csv", "products_list"),
            ("/path/to/data.with.dots.csv", "data_with_dots"),
            ("/path/to/mixed123_data.csv", "mixed123_data"),
        ]
        
        for file_path, expected_base in test_cases:
            result = monitor.extract_table_name(file_path)
            assert expected_base in result.lower()
            mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_calculate_file_hash_success(self, mock_get_logger, mock_basic_config, 
                                       mock_makedirs, mock_api, mock_db):
        """Test calculate_file_hash successful operation"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file content
        test_content = b"Name,Age,City\nJohn,30,NYC\n"
        expected_hash = hashlib.md5(test_content).hexdigest()
        
        with patch('builtins.open', mock_open(read_data=test_content)) as mock_file:
            mock_file.return_value.__iter__ = lambda self: iter([test_content])
            result = monitor.calculate_file_hash("/test/file.csv")
        
        assert result == expected_hash

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_calculate_file_hash_error(self, mock_get_logger, mock_basic_config, 
                                     mock_makedirs, mock_api, mock_db):
        """Test calculate_file_hash with file error"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file open to raise exception
        with patch('builtins.open', side_effect=IOError("File not readable")):
            result = monitor.calculate_file_hash("/test/bad_file.csv")
        
        assert result is None
        mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_record_processing_success(self, mock_get_logger, mock_basic_config, 
                                     mock_makedirs, mock_api, mock_db):
        """Test record_processing successful database operation"""
        import file_monitor
        
        mock_get_logger.return_value = MagicMock()
        
        # Mock database components
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = mock_connection
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file operations
        with patch('file_monitor.os.path.basename', return_value="test.csv"), \
             patch('file_monitor.os.path.getsize', return_value=1000), \
             patch('file_monitor.os.path.exists', return_value=True), \
             patch.object(monitor, 'calculate_file_hash', return_value="abc123"):
            
            monitor.record_processing(
                "/test/file.csv", 
                "fullload", 
                "test_table", 
                ",", 
                ["col1", "col2"], 
                "processed", 
                is_reference_data=True,
                reference_config_inserted=True,
                error_msg=None
            )
        
        # Verify database operations
        mock_cursor.execute.assert_called()
        mock_connection.commit.assert_called()
        mock_connection.close.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_record_processing_database_error(self, mock_get_logger, mock_basic_config, 
                                            mock_makedirs, mock_api, mock_db):
        """Test record_processing with database error"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        # Create monitor first with working DB, then break DB for method call
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Now break the database connection for the method call
        mock_db_inst.get_connection.side_effect = Exception("Database connection failed")
        
        # Should not raise exception, just log error
        monitor.record_processing(
            "/test/file.csv", 
            "fullload", 
            "test_table", 
            ",", 
            ["col1", "col2"], 
            "error"
        )
        
        mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_api_none_scenarios(self, mock_get_logger, mock_basic_config, 
                              mock_makedirs, mock_api, mock_db):
        """Test scenarios when API is None"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        # Manually set API to None to test error paths
        monitor.api = None
        
        # Test detect_csv_format with None API
        with patch('builtins.open', mock_open(read_data="Name,Age\nJohn,30\n")):
            delimiter, headers, sample_rows = monitor.detect_csv_format("/test/file.csv")
        
        # Should fall back to simple detection
        assert delimiter == ","
        assert headers == ["Name", "Age"]
        mock_logger.error.assert_called()
        
        # Test extract_table_name with None API
        result = monitor.extract_table_name("/path/to/test_file.csv")
        assert "test_file" in result
        mock_logger.error.assert_called()