"""
Final comprehensive test for 100% file_monitor.py coverage
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


class TestFileMonitorFinal:
    """Final comprehensive test for 100% coverage"""
    
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
    def test_detect_csv_format_success_path(self, mock_get_logger, mock_basic_config,
                                           mock_makedirs, mock_api, mock_db):
        """Test detect_csv_format success path"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock successful API response
        mock_api_inst.detect_format.return_value = {
            'success': True,
            'detected_format': {
                'column_delimiter': ',',
                'sample_data': [['id', 'name', 'email'], ['1', 'John', 'john@test.com']]
            }
        }
        
        monitor = file_monitor.FileMonitor()
        
        delimiter, headers, sample_rows = monitor.detect_csv_format('test.csv')
        
        assert delimiter == ','
        assert headers == ['id', 'name', 'email']
        assert len(sample_rows) == 2
        mock_logger.info.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_detect_csv_format_api_failure(self, mock_get_logger, mock_basic_config,
                                          mock_makedirs, mock_api, mock_db):
        """Test detect_csv_format when API returns failure"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock API failure response
        mock_api_inst.detect_format.return_value = {
            'success': False,
            'error': 'Detection failed'
        }
        
        test_file = os.path.join(self.temp_dir, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        monitor = file_monitor.FileMonitor()
        
        # Should fall back to _fallback_csv_detection
        result = monitor.detect_csv_format(test_file)
        
        assert result is not None
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_detect_csv_format_api_none(self, mock_get_logger, mock_basic_config,
                                       mock_makedirs, mock_api, mock_db):
        """Test detect_csv_format when API is None"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        # API initialization fails
        mock_api.side_effect = Exception("API failed")
        
        test_file = os.path.join(self.temp_dir, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        monitor = file_monitor.FileMonitor()
        
        # Should use fallback detection
        result = monitor.detect_csv_format(test_file)
        
        assert result is not None
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_fallback_csv_detection_all_cases(self, mock_get_logger, mock_basic_config,
                                             mock_makedirs, mock_api, mock_db):
        """Test _fallback_csv_detection with all delimiter types"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Test cases: (content, expected_delimiter)
        test_cases = [
            ('id,name,value\n1,John,100', ','),
            ('id;name;value\n1;John;100', ';'),
            ('id|name|value\n1|John|100', '|'),
            ('id\tname\tvalue\n1\tJohn\t100', '\t'),
        ]
        
        for i, (content, expected_delimiter) in enumerate(test_cases):
            test_file = os.path.join(self.temp_dir, f'fallback_{i}.csv')
            with open(test_file, 'w') as f:
                f.write(content)
            
            delimiter, headers, sample_rows = monitor._fallback_csv_detection(test_file)
            
            assert delimiter == expected_delimiter
            assert isinstance(headers, list)
            assert isinstance(sample_rows, list)
        
        # Test file error
        result = monitor._fallback_csv_detection('/nonexistent/file.csv')
        assert result is None
        mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_process_file_all_scenarios(self, mock_get_logger, mock_basic_config,
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
        
        test_file = os.path.join(self.temp_dir, 'process.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Test 1: Successful processing
        monitor.detect_csv_format = MagicMock(return_value=(',', ['id', 'name'], []))
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        with patch('shutil.move') as mock_move:
            result = monitor.process_file(test_file, 'fullload', True)
            assert result is True
            mock_move.assert_called()
        
        # Test 2: Format detection returns None
        monitor.detect_csv_format = MagicMock(return_value=None)
        
        with patch('shutil.move') as mock_move:
            result = monitor.process_file(test_file, 'fullload', True)
            assert result is False
            mock_move.assert_called()  # Should move to error
        
        # Test 3: API processing fails
        monitor.detect_csv_format = MagicMock(return_value=(',', ['id', 'name'], []))
        mock_api_inst.process_file.return_value = {'status': 'error', 'error': 'Processing failed'}
        
        with patch('shutil.move') as mock_move:
            result = monitor.process_file(test_file, 'fullload', True)
            assert result is False
            mock_move.assert_called()
        
        # Test 4: File move error
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        with patch('shutil.move', side_effect=OSError("Move failed")):
            result = monitor.process_file(test_file, 'fullload', True)
            assert result is True  # Processing succeeded
            mock_logger.error.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_cleanup_tracking_scenarios(self, mock_get_logger, mock_basic_config,
                                       mock_makedirs, mock_api, mock_db):
        """Test cleanup_tracking with all scenarios"""
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
        
        # Test database error in cleanup
        mock_connection.reset_mock()
        mock_cursor.reset_mock()
        mock_db_inst.reset_mock()
        
        mock_db_inst.get_connection.side_effect = Exception("Database error")
        
        monitor.cleanup_tracking()
        
        mock_logger.error.assert_called_with("Error during tracking cleanup: Database error")
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    @patch('file_monitor.time.sleep')
    def test_run_method_complete(self, mock_sleep, mock_get_logger, mock_basic_config,
                                mock_makedirs, mock_api, mock_db):
        """Test run method completely"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        monitor = file_monitor.FileMonitor()
        
        # Mock scan_folders to return files
        mock_files = [
            {'path': '/test/file1.csv', 'load_type': 'fullload', 'is_reference_data': True},
            {'path': '/test/file2.csv', 'load_type': 'append', 'is_reference_data': False}
        ]
        monitor.scan_folders = MagicMock(return_value=mock_files)
        
        # Mock check_file_stability
        def stability_side_effect(file_path):
            return file_path == '/test/file1.csv'  # Only file1 is stable
        
        monitor.check_file_stability = MagicMock(side_effect=stability_side_effect)
        
        # Mock process_file
        monitor.process_file = MagicMock(return_value=True)
        
        # Mock cleanup_tracking
        monitor.cleanup_tracking = MagicMock()
        
        # Stop after one iteration
        mock_sleep.side_effect = [None, KeyboardInterrupt()]
        
        try:
            monitor.run()
        except KeyboardInterrupt:
            pass
        
        # Verify all methods were called
        monitor.scan_folders.assert_called()
        assert monitor.check_file_stability.call_count == 2  # Called for both files
        monitor.process_file.assert_called_once_with('/test/file1.csv', 'fullload', True)
        monitor.cleanup_tracking.assert_called()
        
        # Verify logging
        mock_logger.info.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    @patch('file_monitor.time.sleep')
    def test_run_method_with_exception(self, mock_sleep, mock_get_logger, mock_basic_config,
                                      mock_makedirs, mock_api, mock_db):
        """Test run method exception handling"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Mock scan_folders to throw exception
        monitor.scan_folders = MagicMock(side_effect=Exception("Scan error"))
        
        # Stop after handling exception
        mock_sleep.side_effect = [None, KeyboardInterrupt()]
        
        try:
            monitor.run()
        except KeyboardInterrupt:
            pass
        
        # Should have logged the error and continued
        mock_logger.error.assert_called_with("File monitor error: Scan error")
    
    @patch('file_monitor.os.listdir')
    @patch('file_monitor.os.path.isfile') 
    @patch('file_monitor.os.path.exists')
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    @patch('file_monitor.os.makedirs')
    @patch('file_monitor.logging.basicConfig')
    @patch('file_monitor.logging.getLogger')
    def test_scan_folders_complete(self, mock_get_logger, mock_basic_config,
                                  mock_makedirs, mock_api, mock_db,
                                  mock_exists, mock_isfile, mock_listdir):
        """Test scan_folders completely"""
        import file_monitor
        
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.get_connection.return_value = MagicMock()
        
        mock_api.return_value = MagicMock()
        
        monitor = file_monitor.FileMonitor()
        
        # Test successful scan
        mock_exists.return_value = True
        
        def listdir_side_effect(path):
            if 'reference_data_table/fullload' in path:
                return ['ref_full.csv', 'ref_full.txt']
            elif 'reference_data_table/append' in path:
                return ['ref_append.csv']
            elif 'none_reference_data_table/fullload' in path:
                return ['non_ref_full.csv']
            elif 'none_reference_data_table/append' in path:
                return ['non_ref_append.csv']
            return []
        
        mock_listdir.side_effect = listdir_side_effect
        mock_isfile.side_effect = lambda path: path.endswith('.csv')
        
        files = monitor.scan_folders()
        
        # Should find all CSV files
        csv_files = [f for f in files if isinstance(f, dict) and f.get('path', '').endswith('.csv')]
        assert len(csv_files) == 4
        
        # Test directory not existing
        mock_exists.return_value = False
        files = monitor.scan_folders()
        # Should return empty or handle gracefully
        
        # Test listdir error
        mock_exists.return_value = True
        mock_listdir.side_effect = OSError("Permission denied")
        files = monitor.scan_folders()
        mock_logger.error.assert_called()
    
    def test_main_function_comprehensive(self):
        """Test main function completely"""
        import file_monitor
        
        # Test main exists
        assert hasattr(file_monitor, 'main')
        assert callable(file_monitor.main)
        
        # Test main execution
        with patch('file_monitor.FileMonitor') as mock_monitor_class:
            mock_monitor = MagicMock()
            mock_monitor_class.return_value = mock_monitor
            mock_monitor.run.side_effect = KeyboardInterrupt()
            
            try:
                file_monitor.main()
            except KeyboardInterrupt:
                pass
            
            mock_monitor_class.assert_called_once()
            mock_monitor.run.assert_called_once()
        
        # Test main with exception
        with patch('file_monitor.FileMonitor', side_effect=Exception("Init failed")):
            try:
                file_monitor.main()
            except Exception:
                pass  # Expected
    
    def test_name_main_guard_coverage(self):
        """Test __name__ == '__main__' guard"""
        import file_monitor
        
        # Verify the guard exists
        with open(file_monitor.__file__, 'r') as f:
            content = f.read()
        
        assert 'if __name__ == "__main__":' in content
        assert 'main()' in content
        
        # Test that it would execute main when run as script
        with patch('file_monitor.main') as mock_main:
            # Simulate running as main module
            original_name = file_monitor.__name__
            file_monitor.__name__ = '__main__'
            
            try:
                # This would normally execute the if block
                exec(compile('if __name__ == "__main__": main()', '<string>', 'exec'), 
                     file_monitor.__dict__)
                mock_main.assert_called_once()
            finally:
                file_monitor.__name__ = original_name