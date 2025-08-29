"""
Advanced test coverage for file_monitor.py to achieve >90% coverage
Covers remaining uncovered lines: main processing workflow, monitoring loop, and exception paths
"""

import pytest
import os
import time
from unittest.mock import patch, MagicMock, mock_open, call


class TestFileMonitor90Percent:
    """Advanced tests to push file_monitor.py coverage above 90%"""

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_success_reference_data(self, mock_get_logger, mock_basic_config,
                                                mock_makedirs, mock_api, mock_db):
        """Test successful file processing for reference data with config insertion"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock successful API responses
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": True,
            "detected_format": {
                "column_delimiter": ",",
                "sample_data": [["Name", "Age"], ["John", "30"]]
            }
        }
        mock_api_inst.extract_table_name_from_file.return_value = "test_table"
        mock_api_inst.process_file_sync.return_value = {"success": True}
        mock_api_inst.insert_reference_data_cfg_record.return_value = {"success": True}
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file operations and path methods
        with patch('file_monitor.os.path.basename', return_value="test.csv"), \
             patch('file_monitor.os.rename') as mock_rename, \
             patch.object(monitor, 'get_processed_path', return_value="/processed"), \
             patch.object(monitor, 'record_processing') as mock_record:
            
            # Add file to tracking to test removal
            monitor.file_tracking["/test/file.csv"] = {"size": 1000, "mtime": 123456.0, "stable_count": 6}
            
            monitor.process_file("/test/file.csv", "fullload", True)
            
            # Verify successful processing path
            mock_api_inst.process_file_sync.assert_called_once()
            mock_api_inst.insert_reference_data_cfg_record.assert_called_with("test_table")
            mock_rename.assert_called_once()
            mock_record.assert_called_once()
            
            # Verify file removed from tracking
            assert "/test/file.csv" not in monitor.file_tracking
            
            # Verify logging
            mock_logger.info.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_success_non_reference_data(self, mock_get_logger, mock_basic_config,
                                                    mock_makedirs, mock_api, mock_db):
        """Test successful file processing for non-reference data"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock successful API responses
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": True,
            "detected_format": {
                "column_delimiter": ";",
                "sample_data": [["Col1", "Col2"], ["Val1", "Val2"]]
            }
        }
        mock_api_inst.extract_table_name_from_file.return_value = "non_ref_table"
        mock_api_inst.process_file_sync.return_value = {"success": True}
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file operations
        with patch('file_monitor.os.path.basename', return_value="data.csv"), \
             patch('file_monitor.os.rename') as mock_rename, \
             patch.object(monitor, 'get_processed_path', return_value="/processed"), \
             patch.object(monitor, 'record_processing') as mock_record:
            
            monitor.process_file("/test/data.csv", "append", False)  # Non-reference data
            
            # Should not try to insert reference data config
            mock_api_inst.insert_reference_data_cfg_record.assert_not_called()
            
            # Verify processing still successful
            mock_rename.assert_called_once()
            mock_record.assert_called_with(
                "/processed/data.csv", "append", "non_ref_table", ";", ["Col1", "Col2"],
                "completed", False, False  # reference_config_inserted=False
            )

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_reference_config_failure(self, mock_get_logger, mock_basic_config,
                                                  mock_makedirs, mock_api, mock_db):
        """Test file processing when reference config insertion fails"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock API responses
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": True,
            "detected_format": {"column_delimiter": ",", "sample_data": [["ID"], ["1"]]}
        }
        mock_api_inst.extract_table_name_from_file.return_value = "ref_table"
        mock_api_inst.process_file_sync.return_value = {"success": True}
        mock_api_inst.insert_reference_data_cfg_record.return_value = {
            "success": False,
            "error": "Config insertion failed"
        }
        
        monitor = file_monitor.FileMonitor()
        
        with patch('file_monitor.os.path.basename', return_value="ref.csv"), \
             patch('file_monitor.os.rename'), \
             patch.object(monitor, 'get_processed_path', return_value="/processed"), \
             patch.object(monitor, 'record_processing') as mock_record:
            
            monitor.process_file("/test/ref.csv", "fullload", True)
            
            # Should still complete processing but log warning
            mock_logger.warning.assert_called()
            mock_record.assert_called_with(
                "/processed/ref.csv", "fullload", "ref_table", ",", ["ID"],
                "completed", True, False  # reference_config_inserted=False due to failure
            )

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_reference_config_exception(self, mock_get_logger, mock_basic_config,
                                                    mock_makedirs, mock_api, mock_db):
        """Test file processing when reference config insertion raises exception"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock API responses
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": True,
            "detected_format": {"column_delimiter": ",", "sample_data": [["ID"], ["1"]]}
        }
        mock_api_inst.extract_table_name_from_file.return_value = "ref_table"
        mock_api_inst.process_file_sync.return_value = {"success": True}
        mock_api_inst.insert_reference_data_cfg_record.side_effect = Exception("Config API error")
        
        monitor = file_monitor.FileMonitor()
        
        with patch('file_monitor.os.path.basename', return_value="ref.csv"), \
             patch('file_monitor.os.rename'), \
             patch.object(monitor, 'get_processed_path', return_value="/processed"), \
             patch.object(monitor, 'record_processing') as mock_record:
            
            monitor.process_file("/test/ref.csv", "fullload", True)
            
            # Should still complete processing but log error
            mock_logger.error.assert_called()
            mock_record.assert_called_with(
                "/processed/ref.csv", "fullload", "ref_table", ",", ["ID"],
                "completed", True, False  # reference_config_inserted=False due to exception
            )

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_backend_failure(self, mock_get_logger, mock_basic_config,
                                         mock_makedirs, mock_api, mock_db):
        """Test file processing when backend processing fails"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock failed backend processing
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.return_value = {
            "success": True,
            "detected_format": {"column_delimiter": ",", "sample_data": [["ID"], ["1"]]}
        }
        mock_api_inst.extract_table_name_from_file.return_value = "failed_table"
        mock_api_inst.process_file_sync.return_value = {
            "success": False,
            "error": "Backend processing failed"
        }
        
        monitor = file_monitor.FileMonitor()
        
        with patch('file_monitor.os.path.basename', return_value="failed.csv"), \
             patch('file_monitor.os.rename') as mock_rename, \
             patch.object(monitor, 'get_error_path', return_value="/error"), \
             patch.object(monitor, 'record_processing') as mock_record:
            
            # Add file to tracking to test removal
            monitor.file_tracking["/test/failed.csv"] = {"size": 1000, "mtime": 123456.0, "stable_count": 6}
            
            monitor.process_file("/test/failed.csv", "fullload", True)
            
            # Should move to error folder
            mock_rename.assert_called_with("/test/failed.csv", "/error/failed.csv")
            
            # Should record as error
            mock_record.assert_called()
            args = mock_record.call_args[0]
            assert args[5] == "error"  # status
            
            # Verify file removed from tracking
            assert "/test/failed.csv" not in monitor.file_tracking
            
            # Verify error logging
            mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_move_error(self, mock_get_logger, mock_basic_config,
                                   mock_makedirs, mock_api, mock_db):
        """Test file processing when file move to error folder fails"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock processing exception
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.detect_format.side_effect = Exception("Detection failed")
        
        monitor = file_monitor.FileMonitor()
        
        with patch('file_monitor.os.path.basename', return_value="bad.csv"), \
             patch('file_monitor.os.rename', side_effect=Exception("Cannot move file")) as mock_rename, \
             patch.object(monitor, 'get_error_path', return_value="/error"):
            
            monitor.process_file("/test/bad.csv", "fullload", True)
            
            # Should try to move to error folder but fail
            mock_rename.assert_called_with("/test/bad.csv", "/error/bad.csv")
            
            # Should log both the processing error and the move error
            assert mock_logger.error.call_count >= 2

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_detect_format_fallback_in_error(self, mock_get_logger, mock_basic_config,
                                                         mock_makedirs, mock_api, mock_db):
        """Test fallback format detection in error handling path"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # Mock initial processing to fail
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.process_file_sync.return_value = {"success": False, "error": "Backend failed"}
        
        # Mock detect_format to succeed initially but fail in error path
        format_calls = 0
        def mock_detect_format(file_path):
            nonlocal format_calls
            format_calls += 1
            if format_calls == 1:
                return {
                    "success": True,
                    "detected_format": {"column_delimiter": ",", "sample_data": [["ID"], ["1"]]}
                }
            else:
                # Second call (in error handling) should fail
                raise Exception("Format detection in error path failed")
        
        mock_api_inst.detect_format.side_effect = mock_detect_format
        mock_api_inst.extract_table_name_from_file.return_value = "test_table"
        
        monitor = file_monitor.FileMonitor()
        
        with patch('file_monitor.os.path.basename', return_value="test.csv"), \
             patch('file_monitor.os.rename') as mock_rename, \
             patch.object(monitor, 'get_error_path', return_value="/error"), \
             patch.object(monitor, 'record_processing') as mock_record:
            
            monitor.process_file("/test/test.csv", "fullload", True)
            
            # Should move to error folder
            mock_rename.assert_called_with("/test/test.csv", "/error/test.csv")
            
            # Should record with fallback values when detection fails in error path
            mock_record.assert_called()
            args = mock_record.call_args[0]
            assert args[3] == ','  # fallback delimiter
            assert args[4] == []   # fallback headers
            assert args[6] == 'unknown'  # fallback table name

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_cleanup_tracking(self, mock_get_logger, mock_basic_config,
                             mock_makedirs, mock_api, mock_db):
        """Test cleanup_tracking method"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Populate tracking with mix of existing and missing files
        monitor.file_tracking = {
            "/existing/file1.csv": {"size": 100, "mtime": 123.0, "stable_count": 2},
            "/missing/file2.csv": {"size": 200, "mtime": 456.0, "stable_count": 1},
            "/existing/file3.csv": {"size": 300, "mtime": 789.0, "stable_count": 3},
        }
        
        # Mock os.path.exists to return True for existing files
        def mock_exists(path):
            return "existing" in path
        
        with patch('file_monitor.os.path.exists', side_effect=mock_exists):
            monitor.cleanup_tracking()
        
        # Should remove missing files
        assert "/missing/file2.csv" not in monitor.file_tracking
        assert "/existing/file1.csv" in monitor.file_tracking
        assert "/existing/file3.csv" in monitor.file_tracking
        
        # Should log removal
        mock_logger.info.assert_called_with("Removed tracking for missing file: /missing/file2.csv")

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_run_method_monitoring_loop(self, mock_get_logger, mock_basic_config,
                                       mock_makedirs, mock_api, mock_db):
        """Test run method monitoring loop with KeyboardInterrupt"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock scan_folders to return a stable file
        mock_csv_files = [("/test/stable.csv", "fullload", True)]
        
        loop_count = 0
        def mock_sleep(seconds):
            nonlocal loop_count
            loop_count += 1
            if loop_count >= 2:  # Stop after 2 iterations
                raise KeyboardInterrupt()
        
        with patch.object(monitor, 'scan_folders', return_value=mock_csv_files), \
             patch.object(monitor, 'check_file_stability', return_value=True), \
             patch.object(monitor, 'process_file') as mock_process, \
             patch.object(monitor, 'cleanup_tracking'), \
             patch('file_monitor.time.sleep', side_effect=mock_sleep):
            
            monitor.run()
        
        # Should process stable files in each iteration
        assert mock_process.call_count == 2
        mock_process.assert_called_with("/test/stable.csv", "fullload", True)
        
        # Should log startup and shutdown messages
        startup_calls = [call for call in mock_logger.info.call_args_list if "File monitor started" in str(call)]
        shutdown_calls = [call for call in mock_logger.info.call_args_list if "stopped by user" in str(call)]
        assert len(startup_calls) >= 1
        assert len(shutdown_calls) >= 1

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_run_method_exception(self, mock_get_logger, mock_basic_config,
                                 mock_makedirs, mock_api, mock_db):
        """Test run method with unexpected exception"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock scan_folders to raise exception
        with patch.object(monitor, 'scan_folders', side_effect=Exception("Scan failed")):
            with pytest.raises(Exception, match="Scan failed"):
                monitor.run()
        
        # Should log the error
        mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_file_stability_debug_logging(self, mock_get_logger, mock_basic_config,
                                         mock_makedirs, mock_api, mock_db):
        """Test debug logging in file stability check"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Pre-populate tracking with file not quite stable
        monitor.file_tracking["/test/debug_file.csv"] = {
            'size': 1000,
            'mtime': 1234567890.0,
            'stable_count': 2  # Less than STABILITY_CHECKS
        }
        
        # Mock same file size/mtime (no change)
        mock_stat = MagicMock()
        mock_stat.st_size = 1000
        mock_stat.st_mtime = 1234567890.0
        with patch('file_monitor.os.stat', return_value=mock_stat):
            
            result = monitor.check_file_stability("/test/debug_file.csv")
            
            # Should not be stable yet, should increment counter
            assert result is False
            assert monitor.file_tracking["/test/debug_file.csv"]['stable_count'] == 3
            
            # Should log debug message
            mock_logger.debug.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_fallback_csv_detection_exception(self, mock_get_logger, mock_basic_config,
                                            mock_makedirs, mock_api, mock_db):
        """Test exception handling in _fallback_csv_detection"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock file open to raise exception
        with patch('builtins.open', side_effect=IOError("Cannot open file")):
            delimiter, headers, sample_rows = monitor._fallback_csv_detection("/test/bad_file.csv")
        
        # Should return default values
        assert delimiter == ","
        assert headers == []
        assert sample_rows == []
        
        # Should log the error
        mock_logger.error.assert_called()

    def test_main_function(self):
        """Test main function"""
        import file_monitor
        
        with patch.object(file_monitor, 'FileMonitor') as mock_monitor_class:
            mock_monitor_instance = MagicMock()
            mock_monitor_class.return_value = mock_monitor_instance
            
            file_monitor.main()
            
            # Should create monitor and call run
            mock_monitor_class.assert_called_once()
            mock_monitor_instance.run.assert_called_once()

    def test_name_main_execution(self):
        """Test __name__ == '__main__' execution path"""
        import file_monitor
        
        # Mock the main function
        with patch.object(file_monitor, 'main') as mock_main:
            # Simulate script execution
            exec(compile(open('/home/lin/repo/reference_data_mgr/backend/file_monitor.py').read(), 
                        'file_monitor.py', 'exec'), {'__name__': '__main__'})
            
            # Should call main function
            mock_main.assert_called_once()