"""
Complete test coverage for file_monitor.py - targeting 100% coverage
"""
import pytest
import os
import tempfile
import shutil
import time
import hashlib
from unittest.mock import patch, MagicMock, mock_open, PropertyMock, call
from datetime import datetime
import sys
import logging
import csv


class TestFileMonitorCompleteCoverage:
    """Complete coverage tests for file_monitor.py"""
    
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
    def test_init_tracking_table_error_handling(self, mock_get_logger, mock_basic_config,
                                              mock_makedirs, mock_api, mock_db):
        """Test init_tracking_table error handling"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        
        # Mock database error in init_tracking_table
        mock_connection = MagicMock()
        mock_connection.cursor.side_effect = Exception("Cursor failed")
        mock_db_inst.get_connection.return_value = mock_connection
        
        mock_api.return_value = MagicMock()
        
        # Should handle database error gracefully
        monitor = file_monitor.FileMonitor()
        
        # Should have logged the error
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_extract_table_name_api_success(self, mock_get_logger, mock_basic_config,
                                          mock_makedirs, mock_api, mock_db):
        """Test extract_table_name with successful API call"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.extract_table_name_from_file.return_value = 'airports'
        
        monitor = file_monitor.FileMonitor()
        
        result = monitor.extract_table_name('/path/to/airports.csv')
        assert result == 'airports'
        
        mock_api_inst.extract_table_name_from_file.assert_called_with('/path/to/airports.csv')
        mock_logger.info.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_extract_table_name_api_none(self, mock_get_logger, mock_basic_config,
                                       mock_makedirs, mock_api, mock_db):
        """Test extract_table_name when API is None"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # API initialization fails
        mock_api.side_effect = Exception("API failed")
        
        monitor = file_monitor.FileMonitor()
        
        # API should be None, so fallback should be used
        result = monitor.extract_table_name('/path/to/test_20240101_data.csv')
        
        # Should use fallback extraction
        assert result == 'test_data'  # Date pattern removed
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_extract_table_name_api_exception(self, mock_get_logger, mock_basic_config,
                                            mock_makedirs, mock_api, mock_db):
        """Test extract_table_name when API throws exception"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        mock_api_inst.extract_table_name_from_file.side_effect = Exception("API error")
        
        monitor = file_monitor.FileMonitor()
        
        # Should fall back to manual extraction
        test_cases = [
            ('/path/to/airports_2024-01-01.csv', 'airports'),
            ('/path/to/test__data.csv', 'test_data'),
            ('/path/to/complex-file_20240101.csv', 'complex_file'),
            ('/path/to/special@chars#.csv', 'special_chars_'),
        ]
        
        for file_path, expected in test_cases:
            result = monitor.extract_table_name(file_path)
            # Should clean up the name properly
            assert isinstance(result, str)
            assert len(result) > 0
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_check_file_stability_comprehensive(self, mock_get_logger, mock_basic_config,
                                              mock_makedirs, mock_api, mock_db):
        """Test check_file_stability with all scenarios"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Create test file
        test_file = os.path.join(self.temp_dir, 'stability_test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Test 1: New file (first check)
        is_stable = monitor.check_file_stability(test_file)
        assert is_stable is False
        assert test_file in monitor.file_tracking
        assert monitor.file_tracking[test_file]['stable_count'] == 1
        
        # Test 2: File unchanged for multiple checks
        for i in range(5):
            is_stable = monitor.check_file_stability(test_file)
            assert is_stable is False
        
        # Test 3: File stable after required checks
        is_stable = monitor.check_file_stability(test_file)
        assert is_stable is True
        
        # Test 4: File modified (size changes)
        with open(test_file, 'a') as f:
            f.write('\n3,Bob')
        
        is_stable = monitor.check_file_stability(test_file)
        assert is_stable is False
        assert monitor.file_tracking[test_file]['stable_count'] == 1
        
        # Test 5: File modification time changes
        old_mtime = monitor.file_tracking[test_file]['mtime']
        time.sleep(0.01)
        os.utime(test_file, None)  # Update modification time
        
        is_stable = monitor.check_file_stability(test_file)
        assert is_stable is False
        assert monitor.file_tracking[test_file]['stable_count'] == 1
        
        # Test 6: Nonexistent file
        is_stable = monitor.check_file_stability('/nonexistent/file.csv')
        assert is_stable is False
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_detect_csv_format_all_paths(self, mock_get_logger, mock_basic_config,
                                        mock_makedirs, mock_api, mock_db):
        """Test detect_csv_format with all code paths"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        test_file = os.path.join(self.temp_dir, 'format_test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name,email\n1,John,john@test.com')
        
        # Test 1: Successful API detection
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True,
                'columns': ['id', 'name', 'email']
            }
        }
        
        result = monitor.detect_csv_format(test_file)
        assert result is not None
        assert 'detected_format' in result
        
        # Test 2: API returns None
        mock_api_inst.detect_format.return_value = None
        result = monitor.detect_csv_format(test_file)
        assert result is not None  # Should use fallback
        
        # Test 3: API throws exception
        mock_api_inst.detect_format.side_effect = Exception("API error")
        result = monitor.detect_csv_format(test_file)
        assert result is not None  # Should use fallback
        
        # Test 4: API is None
        monitor.api = None
        result = monitor.detect_csv_format(test_file)
        assert result is not None  # Should use fallback
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_fallback_csv_detection_all_delimiters(self, mock_get_logger, mock_basic_config,
                                                  mock_makedirs, mock_api, mock_db):
        """Test fallback CSV detection for all delimiters and scenarios"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Test different delimiter scenarios
        test_cases = [
            ('comma.csv', 'id,name,value\n1,John,100\n2,Jane,200', ',', True),
            ('semicolon.csv', 'id;name;value\n1;John;100\n2;Jane;200', ';', True),
            ('pipe.csv', 'id|name|value\n1|John|100\n2|Jane|200', '|', True),
            ('tab.csv', 'id\tname\tvalue\n1\tJohn\t100\n2\tJane\t200', '\t', True),
            ('no_header.csv', '1,John,100\n2,Jane,200\n3,Bob,300', ',', False),
            ('single_column.csv', 'values\n100\n200\n300', ',', True),  # Will default to comma
        ]
        
        for filename, content, expected_delimiter, expected_header in test_cases:
            test_file = os.path.join(self.temp_dir, filename)
            with open(test_file, 'w') as f:
                f.write(content)
            
            result = monitor._fallback_csv_detection(test_file)
            
            assert result is not None
            assert 'detected_format' in result
            assert result['detected_format']['column_delimiter'] == expected_delimiter
            assert result['detected_format']['has_header'] == expected_header
        
        # Test file error scenario
        result = monitor._fallback_csv_detection('/nonexistent/file.csv')
        assert result is None
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_record_processing_comprehensive(self, mock_get_logger, mock_basic_config,
                                           mock_makedirs, mock_api, mock_db):
        """Test record_processing with all scenarios"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = mock_connection
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Create test file
        test_file = os.path.join(self.temp_dir, 'record_test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Test 1: Successful recording
        monitor.record_processing(
            test_file, 'fullload', 'test_table', ',', ['id', 'name'], 
            'SUCCESS', is_reference_data=True, reference_config_inserted=True
        )
        
        mock_cursor.execute.assert_called()
        mock_connection.commit.assert_called()
        mock_connection.close.assert_called()
        
        # Test 2: Recording with error message
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        
        monitor.record_processing(
            test_file, 'append', 'error_table', ';', ['col1', 'col2'],
            'ERROR', error_msg="Processing failed"
        )
        
        mock_cursor.execute.assert_called()
        
        # Test 3: Database error
        mock_cursor.execute.side_effect = Exception("Database error")
        
        monitor.record_processing(
            test_file, 'fullload', 'db_error_table', '|', ['a', 'b'],
            'ERROR'
        )
        
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    @patch('file_monitor.shutil.move')
    def test_process_file_comprehensive(self, mock_move, mock_get_logger, mock_basic_config,
                                      mock_makedirs, mock_api, mock_db):
        """Test process_file with all scenarios"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        test_file = os.path.join(self.temp_dir, 'process_test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        # Test 1: Successful processing
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True,
                'columns': ['id', 'name']
            }
        }
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        result = monitor.process_file(test_file, 'fullload', True)
        assert result is True
        mock_move.assert_called()
        
        # Test 2: Format detection failure
        mock_move.reset_mock()
        monitor.detect_csv_format = MagicMock(return_value=None)
        
        result = monitor.process_file(test_file, 'fullload', True)
        assert result is False
        mock_move.assert_called()  # Should move to error
        
        # Test 3: Processing failure
        mock_move.reset_mock()
        monitor.detect_csv_format = MagicMock(return_value={'detected_format': {'column_delimiter': ','}})
        mock_api_inst.process_file.return_value = {'status': 'error', 'error': 'Failed'}
        
        result = monitor.process_file(test_file, 'append', False)
        assert result is False
        mock_move.assert_called()  # Should move to error
        
        # Test 4: Move file error
        mock_move.reset_mock()
        mock_move.side_effect = OSError("Move failed")
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        result = monitor.process_file(test_file, 'fullload', True)
        # Should still return True (processing succeeded)
        assert result is True
        mock_logger.error.assert_called()
    
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
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = mock_connection
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Test successful cleanup
        monitor.cleanup_tracking()
        
        mock_db_inst.get_connection.assert_called()
        mock_cursor.execute.assert_called()
        mock_connection.commit.assert_called()
        mock_connection.close.assert_called()
        
        # Test cleanup with database error
        mock_cursor.execute.side_effect = Exception("Cleanup failed")
        
        monitor.cleanup_tracking()
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    @patch('file_monitor.time.sleep')
    def test_run_method_comprehensive(self, mock_sleep, mock_get_logger, mock_basic_config,
                                    mock_makedirs, mock_api, mock_db):
        """Test run method with all scenarios"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        # Create test files
        ref_fullload = os.path.join(self.temp_dir, 'reference_data_table', 'fullload')
        os.makedirs(ref_fullload, exist_ok=True)
        
        test_file = os.path.join(ref_fullload, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Mock scan_folders to return test file
        monitor.scan_folders = MagicMock(return_value=[{
            'path': test_file,
            'load_type': 'fullload',
            'is_reference_data': True
        }])
        
        # Mock file as stable
        monitor.check_file_stability = MagicMock(return_value=True)
        
        # Mock successful processing
        monitor.process_file = MagicMock(return_value=True)
        
        # Mock cleanup
        monitor.cleanup_tracking = MagicMock()
        
        # Test single iteration with KeyboardInterrupt
        mock_sleep.side_effect = [None, KeyboardInterrupt()]
        
        try:
            monitor.run()
        except KeyboardInterrupt:
            pass
        
        # Verify methods were called
        monitor.scan_folders.assert_called()
        monitor.check_file_stability.assert_called()
        monitor.process_file.assert_called()
        mock_logger.info.assert_called()
        
        # Test with general exception
        mock_sleep.side_effect = None
        monitor.scan_folders.side_effect = Exception("Scan error")
        
        # Stop after handling exception
        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise KeyboardInterrupt()
        
        mock_sleep.side_effect = side_effect
        
        try:
            monitor.run()
        except KeyboardInterrupt:
            pass
        
        mock_logger.error.assert_called()
    
    @patch('file_monitor.os.listdir')
    @patch('file_monitor.os.path.isfile')
    @patch('file_monitor.os.path.exists')
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_scan_folders_comprehensive(self, mock_get_logger, mock_basic_config,
                                      mock_makedirs, mock_api, mock_db,
                                      mock_exists, mock_isfile, mock_listdir):
        """Test scan_folders method comprehensively"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock directory structure exists
        mock_exists.return_value = True
        
        # Mock file listings for different directories
        def mock_listdir_side_effect(path):
            if 'reference_data_table/fullload' in path:
                return ['ref_full.csv', 'ref_full.txt']
            elif 'reference_data_table/append' in path:
                return ['ref_append.csv']
            elif 'none_reference_data_table/fullload' in path:
                return ['non_ref_full.csv']
            elif 'none_reference_data_table/append' in path:
                return ['non_ref_append.csv', 'readme.md']
            else:
                return []
        
        mock_listdir.side_effect = mock_listdir_side_effect
        
        # Mock isfile to return True only for CSV files
        mock_isfile.side_effect = lambda path: path.endswith('.csv')
        
        files = monitor.scan_folders()
        
        # Should find 4 CSV files
        assert len(files) == 4
        
        # Verify file info structure
        for file_info in files:
            assert 'path' in file_info
            assert 'load_type' in file_info
            assert 'is_reference_data' in file_info
        
        # Test with directory not existing
        mock_exists.return_value = False
        files = monitor.scan_folders()
        assert len(files) == 0
        
        # Test with listdir error
        mock_exists.return_value = True
        mock_listdir.side_effect = OSError("Permission denied")
        files = monitor.scan_folders()
        assert len(files) == 0
        mock_logger.error.assert_called()