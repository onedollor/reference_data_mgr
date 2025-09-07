"""
Final push to 100% coverage for file_monitor.py
Targeting lines: 181-193, 328-329, 366-368, 385-386, 406-407, 416, 422-423, 426-427, 464
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import patch, MagicMock, mock_open


class TestFileMonitorFinalPush:
    """Final push to 100% coverage"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_file_stability_incremental_checks(self, mock_get_logger, mock_basic_config,
                                              mock_makedirs, mock_api, mock_db):
        """Test lines 181-193: file stability incremental checks with debug and OSError"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Create a test file
        test_file = os.path.join(self.temp_dir, 'stability_test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Mock os.stat to simulate stable file (same size multiple times)
        with patch('os.stat') as mock_stat:
            mock_stat.return_value.st_size = 100
            
            # First call initializes tracking
            result1 = monitor.check_file_stability(test_file)
            assert result1 == False  # Not stable yet
            
            # Second call - should increment stable_count (line 181-182)
            result2 = monitor.check_file_stability(test_file)
            assert result2 == False  # Still not stable (hits line 189)
            
            # Ensure debug was called for line 188
            mock_logger.debug.assert_called()
            
            # Test OSError path (lines 191-193)
            mock_stat.side_effect = OSError("File access error")
            result3 = monitor.check_file_stability(test_file)
            assert result3 == False
            mock_logger.error.assert_called_with(f"Error checking file {test_file}: File access error")

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_extract_table_name_api_disabled(self, mock_get_logger, mock_basic_config,
                                            mock_makedirs, mock_api, mock_db):
        """Test lines 328-329: extract table name when API is None"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        # Disable API to hit line 329
        monitor.api = None
        
        test_file = os.path.join(self.temp_dir, 'test_table.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Should return filename without extension when API is None
        result = monitor.extract_table_name(test_file)
        assert result == 'test_table'

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_reference_config_warning_path(self, mock_get_logger, mock_basic_config,
                                         mock_makedirs, mock_api, mock_db):
        """Test lines 366-368: reference config warning and exception paths"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = mock_connection
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        test_file = os.path.join(self.temp_dir, 'ref_config_test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Mock detect_csv_format and extract_table_name
        monitor.detect_csv_format = MagicMock(return_value=(',', ['id', 'name'], [['1', 'John']]))
        monitor.extract_table_name = MagicMock(return_value='test_table')
        
        # Mock API process_file to succeed
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        # Test warning path - config insertion returns error (line 366)
        mock_api_inst.insert_reference_data_config.return_value = {
            'status': 'error', 
            'error': 'Config insertion failed'
        }
        
        with patch('os.rename'), patch('os.path.basename', return_value='ref_config_test.csv'):
            result = monitor.process_file(test_file, 'fullload', True)
            # Should log warning at line 366
            mock_logger.warning.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_backend_processing_failure(self, mock_get_logger, mock_basic_config,
                                       mock_makedirs, mock_api, mock_db):
        """Test lines 385-386: backend processing failure"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        test_file = os.path.join(self.temp_dir, 'backend_fail.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Mock detect_csv_format to return valid format
        monitor.detect_csv_format = MagicMock(return_value=(',', ['id', 'name'], [['1', 'John']]))
        
        # Mock API process_file to return failure status (lines 385-386)
        mock_api_inst.process_file.return_value = {
            'status': 'failed', 
            'error': 'Backend service unavailable'
        }
        
        with patch('os.rename'):
            result = monitor.process_file(test_file, 'fullload', False)
            # Should hit exception at line 386
            mock_logger.error.assert_called()

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_error_handling_with_fallback_detection_failure(self, mock_get_logger, mock_basic_config,
                                                           mock_makedirs, mock_api, mock_db):
        """Test lines 406-407: fallback detection failure in error handler"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = mock_connection
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        test_file = os.path.join(self.temp_dir, 'fallback_fail.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Setup tracking for line 416
        monitor.file_tracking[test_file] = {'size': 100, 'count': 1}
        
        # Mock initial detect_csv_format to return valid format
        monitor.detect_csv_format = MagicMock(return_value=(',', ['id', 'name'], [['1', 'John']]))
        
        # Mock API to fail, triggering error path
        mock_api_inst.process_file.side_effect = Exception("Processing failed")
        
        # Mock detect_csv_format and extract_table_name to fail in error handler (lines 406-407)
        def detect_side_effect(file_path):
            if 'error' in file_path:
                raise Exception("Detection failed")
            return (',', ['id', 'name'], [['1', 'John']])
        
        def extract_side_effect(file_path):
            if 'error' in file_path:
                raise Exception("Extraction failed")
            return 'test_table'
        
        monitor.detect_csv_format = MagicMock(side_effect=detect_side_effect)
        monitor.extract_table_name = MagicMock(side_effect=extract_side_effect)
        
        with patch('os.rename'):
            result = monitor.process_file(test_file, 'fullload', False)
            
            # Should hit error handling and lines 406-407 (fallback to defaults)
            mock_logger.error.assert_called()
            
            # Should have removed from tracking (line 416)
            assert test_file not in monitor.file_tracking

    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_cleanup_tracking_with_exceptions(self, mock_get_logger, mock_basic_config,
                                            mock_makedirs, mock_api, mock_db):
        """Test lines 422-423, 426-427: cleanup tracking with exceptions"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Add some test files to tracking
        monitor.file_tracking = {
            'existing_file.csv': {'size': 100, 'count': 1},
            'nonexistent_file.csv': {'size': 200, 'count': 2}
        }
        
        # Mock os.path.exists to simulate mixed scenario
        def exists_side_effect(path):
            if 'nonexistent' in path:
                return False  # This file doesn't exist (line 422)
            return True
        
        with patch('os.path.exists', side_effect=exists_side_effect):
            monitor.cleanup_tracking()
            
            # Should have removed the nonexistent file (line 423)
            assert 'nonexistent_file.csv' not in monitor.file_tracking
            assert 'existing_file.csv' in monitor.file_tracking
            
            # Test exception handling in cleanup (lines 426-427)
            with patch('os.path.exists', side_effect=Exception("OS Error")):
                monitor.cleanup_tracking()
                mock_logger.error.assert_called()

    def test_main_module_execution(self):
        """Test line 464: main function when module is executed directly"""
        import file_monitor
        
        # Directly test the main function
        with patch.object(file_monitor, 'FileMonitor') as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor_class.return_value = mock_monitor
            
            # Call main - this hits the actual main function
            file_monitor.main()
            
            # Verify FileMonitor was created and run was called
            mock_monitor_class.assert_called_once()
            mock_monitor.run.assert_called_once()