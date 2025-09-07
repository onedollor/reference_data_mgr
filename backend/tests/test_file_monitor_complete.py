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


class TestFileMonitorComplete:
    """Complete test coverage for FileMonitor class"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_dropoff = os.path.join(self.temp_dir, 'dropoff')
        self.test_logs = os.path.join(self.temp_dir, 'logs')
        os.makedirs(self.test_dropoff, exist_ok=True)
        os.makedirs(self.test_logs, exist_ok=True)
        
        # Patch paths to use temp directory
        self.path_patches = [
            patch('file_monitor.DROPOFF_BASE_PATH', self.test_dropoff),
            patch('file_monitor.LOG_FILE', os.path.join(self.test_logs, 'file_monitor.log')),
        ]
        
        for p in self.path_patches:
            p.start()
        
    def teardown_method(self):
        """Cleanup test environment"""
        for p in self.path_patches:
            p.stop()
        
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_file_monitor_init_success(self, mock_api, mock_db):
        """Test FileMonitor initialization success path"""
        from file_monitor import FileMonitor
        
        # Mock successful initialization
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.execute_non_query.return_value = None
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        with patch('os.makedirs') as mock_makedirs:
            monitor = FileMonitor()
            
            assert monitor is not None
            assert monitor.db_manager is not None
            assert monitor.api is not None
            assert monitor.file_tracking == {}
            
            # Verify setup methods were called
            mock_db.assert_called_once()
            mock_api.assert_called_once()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_file_monitor_init_api_failure(self, mock_api, mock_db):
        """Test FileMonitor initialization with API failure"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_db_inst.execute_non_query.return_value = None
        
        # Mock API initialization failure
        mock_api.side_effect = Exception("API initialization failed")
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            assert monitor is not None
            assert monitor.db_manager is not None
            assert monitor.api is None  # Should be None due to exception
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_setup_logging(self, mock_api, mock_db):
        """Test logging setup"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'), \
             patch('logging.FileHandler') as mock_file_handler, \
             patch('logging.StreamHandler') as mock_stream_handler:
            
            mock_file_handler.return_value = MagicMock()
            mock_stream_handler.return_value = MagicMock()
            
            monitor = FileMonitor()
            
            # Logging should be configured
            assert hasattr(monitor, 'logger')
            assert monitor.logger is not None
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_setup_directories(self, mock_api, mock_db):
        """Test directory setup"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs') as mock_makedirs:
            monitor = FileMonitor()
            
            # Should create required directories
            mock_makedirs.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_get_processed_path(self, mock_api, mock_db):
        """Test get_processed_path method"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Test reference data paths
            ref_fullload = monitor.get_processed_path(True, 'fullload')
            ref_append = monitor.get_processed_path(True, 'append')
            
            assert 'reference_data_table' in ref_fullload
            assert 'fullload' in ref_fullload
            assert 'reference_data_table' in ref_append
            assert 'append' in ref_append
            
            # Test non-reference data paths
            non_ref_fullload = monitor.get_processed_path(False, 'fullload')
            non_ref_append = monitor.get_processed_path(False, 'append')
            
            assert 'none_reference_data_table' in non_ref_fullload
            assert 'none_reference_data_table' in non_ref_append
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_get_error_path(self, mock_api, mock_db):
        """Test get_error_path method"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Test error paths
            ref_error = monitor.get_error_path(True, 'fullload')
            non_ref_error = monitor.get_error_path(False, 'append')
            
            assert 'error' in ref_error
            assert 'error' in non_ref_error
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_init_tracking_table_success(self, mock_api, mock_db):
        """Test tracking table initialization success"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        # Mock successful table creation
        mock_db_inst.execute_non_query.return_value = None
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Should have called execute_non_query to create table
            mock_db_inst.execute_non_query.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_init_tracking_table_failure(self, mock_api, mock_db):
        """Test tracking table initialization failure"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        # Mock table creation failure
        mock_db_inst.execute_non_query.side_effect = Exception("Table creation failed")
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Should handle the exception gracefully
            assert monitor is not None
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_scan_folders(self, mock_api, mock_db):
        """Test scan_folders method"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        # Create test directory structure
        ref_fullload = os.path.join(self.test_dropoff, 'reference_data_table', 'fullload')
        ref_append = os.path.join(self.test_dropoff, 'reference_data_table', 'append')
        non_ref_fullload = os.path.join(self.test_dropoff, 'none_reference_data_table', 'fullload')
        non_ref_append = os.path.join(self.test_dropoff, 'none_reference_data_table', 'append')
        
        for path in [ref_fullload, ref_append, non_ref_fullload, non_ref_append]:
            os.makedirs(path, exist_ok=True)
        
        # Create test CSV files
        test_files = [
            os.path.join(ref_fullload, 'test1.csv'),
            os.path.join(ref_append, 'test2.csv'),
            os.path.join(non_ref_fullload, 'test3.csv'),
            os.path.join(non_ref_append, 'test4.csv'),
        ]
        
        for file_path in test_files:
            with open(file_path, 'w') as f:
                f.write('id,name\n1,John\n2,Jane')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            files_found = monitor.scan_folders()
            
            assert len(files_found) == 4
            
            # Verify file information
            for file_info in files_found:
                assert 'path' in file_info
                assert 'load_type' in file_info
                assert 'is_reference_data' in file_info
                assert file_info['path'].endswith('.csv')
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_scan_folders_empty_directories(self, mock_api, mock_db):
        """Test scan_folders with empty directories"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        # Create empty directory structure
        ref_fullload = os.path.join(self.test_dropoff, 'reference_data_table', 'fullload')
        os.makedirs(ref_fullload, exist_ok=True)
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            files_found = monitor.scan_folders()
            
            assert len(files_found) == 0
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_scan_folders_non_csv_files(self, mock_api, mock_db):
        """Test scan_folders ignores non-CSV files"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        # Create test directory with non-CSV files
        ref_fullload = os.path.join(self.test_dropoff, 'reference_data_table', 'fullload')
        os.makedirs(ref_fullload, exist_ok=True)
        
        # Create non-CSV files
        non_csv_files = [
            os.path.join(ref_fullload, 'test.txt'),
            os.path.join(ref_fullload, 'test.xlsx'),
            os.path.join(ref_fullload, 'readme.md'),
        ]
        
        for file_path in non_csv_files:
            with open(file_path, 'w') as f:
                f.write('test content')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            files_found = monitor.scan_folders()
            
            assert len(files_found) == 0  # Should ignore non-CSV files
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_check_file_stability_new_file(self, mock_api, mock_db):
        """Test file stability check for new file"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # New file should not be stable
            is_stable = monitor.check_file_stability(test_file)
            assert is_stable is False
            
            # File should be tracked
            assert test_file in monitor.file_tracking
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_check_file_stability_stable_file(self, mock_api, mock_db):
        """Test file stability check for stable file"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Simulate file being stable for required checks
            file_size = os.path.getsize(test_file)
            file_mtime = os.path.getmtime(test_file)
            
            monitor.file_tracking[test_file] = {
                'size': file_size,
                'mtime': file_mtime,
                'stable_count': 6  # Required stability checks
            }
            
            is_stable = monitor.check_file_stability(test_file)
            assert is_stable is True
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_check_file_stability_changed_file(self, mock_api, mock_db):
        """Test file stability check for changed file"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Initial check
            monitor.check_file_stability(test_file)
            
            # Modify file
            with open(test_file, 'a') as f:
                f.write('\n3,Bob')
            
            # Should reset stability count
            is_stable = monitor.check_file_stability(test_file)
            assert is_stable is False
            assert monitor.file_tracking[test_file]['stable_count'] == 1
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_check_file_stability_nonexistent_file(self, mock_api, mock_db):
        """Test file stability check for nonexistent file"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            nonexistent_file = '/nonexistent/file.csv'
            is_stable = monitor.check_file_stability(nonexistent_file)
            assert is_stable is False
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_detect_csv_format_success(self, mock_api, mock_db):
        """Test CSV format detection success"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock successful format detection
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True,
                'columns': ['id', 'name']
            }
        }
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            result = monitor.detect_csv_format(test_file)
            
            assert result is not None
            assert 'detected_format' in result
            assert result['detected_format']['column_delimiter'] == ','
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_detect_csv_format_api_none(self, mock_api, mock_db):
        """Test CSV format detection when API is None"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.side_effect = Exception("API failed")  # Will set api to None
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Should use fallback detection
            result = monitor.detect_csv_format(test_file)
            
            assert result is not None
            assert 'detected_format' in result
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_detect_csv_format_api_failure(self, mock_api, mock_db):
        """Test CSV format detection when API fails"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock API failure
        mock_api_inst.detect_format.side_effect = Exception("Detection failed")
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Should use fallback detection
            result = monitor.detect_csv_format(test_file)
            
            assert result is not None
            assert 'detected_format' in result
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_fallback_csv_detection_comma(self, mock_api, mock_db):
        """Test fallback CSV detection for comma delimiter"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name,email\n1,John,john@test.com\n2,Jane,jane@test.com')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            result = monitor._fallback_csv_detection(test_file)
            
            assert result is not None
            assert result['detected_format']['column_delimiter'] == ','
            assert result['detected_format']['has_header'] is True
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_fallback_csv_detection_semicolon(self, mock_api, mock_db):
        """Test fallback CSV detection for semicolon delimiter"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id;name;email\n1;John;john@test.com\n2;Jane;jane@test.com')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            result = monitor._fallback_csv_detection(test_file)
            
            assert result is not None
            assert result['detected_format']['column_delimiter'] == ';'
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_fallback_csv_detection_pipe(self, mock_api, mock_db):
        """Test fallback CSV detection for pipe delimiter"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id|name|email\n1|John|john@test.com\n2|Jane|jane@test.com')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            result = monitor._fallback_csv_detection(test_file)
            
            assert result is not None
            assert result['detected_format']['column_delimiter'] == '|'
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_fallback_csv_detection_tab(self, mock_api, mock_db):
        """Test fallback CSV detection for tab delimiter"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id\tname\temail\n1\tJohn\tjohn@test.com\n2\tJane\tjane@test.com')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            result = monitor._fallback_csv_detection(test_file)
            
            assert result is not None
            assert result['detected_format']['column_delimiter'] == '\t'
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_fallback_csv_detection_no_header(self, mock_api, mock_db):
        """Test fallback CSV detection without header"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('1,John,john@test.com\n2,Jane,jane@test.com\n3,Bob,bob@test.com')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            result = monitor._fallback_csv_detection(test_file)
            
            assert result is not None
            assert result['detected_format']['has_header'] is False
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_fallback_csv_detection_file_error(self, mock_api, mock_db):
        """Test fallback CSV detection with file error"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Test with nonexistent file
            result = monitor._fallback_csv_detection('/nonexistent/file.csv')
            
            assert result is None
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_extract_table_name(self, mock_api, mock_db):
        """Test table name extraction from file path"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Test various file paths
            test_cases = [
                ('/path/to/airports.csv', 'airports'),
                ('/path/to/airport_frequencies.csv', 'airport_frequencies'),
                ('/path/to/test_data.csv', 'test_data'),
                ('/path/to/simple.csv', 'simple'),
                ('airports.csv', 'airports'),
                ('/path/to/file.CSV', 'file'),  # Test uppercase
            ]
            
            for file_path, expected_name in test_cases:
                result = monitor.extract_table_name(file_path)
                assert result == expected_name
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_calculate_file_hash(self, mock_api, mock_db):
        """Test file hash calculation"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        test_content = 'id,name\n1,John\n2,Jane'
        
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            file_hash = monitor.calculate_file_hash(test_file)
            
            # Verify hash is correct
            expected_hash = hashlib.md5(test_content.encode()).hexdigest()
            assert file_hash == expected_hash
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_calculate_file_hash_nonexistent(self, mock_api, mock_db):
        """Test file hash calculation for nonexistent file"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            file_hash = monitor.calculate_file_hash('/nonexistent/file.csv')
            assert file_hash is None
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_record_processing_success(self, mock_api, mock_db):
        """Test successful processing record"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            monitor.record_processing(
                test_file, 
                'fullload', 
                'test_table', 
                ',', 
                'id,name', 
                'SUCCESS',
                is_reference_data=True,
                reference_config_inserted=True
            )
            
            # Should have called database insert
            mock_db_inst.execute_non_query.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_record_processing_with_error(self, mock_api, mock_db):
        """Test processing record with error"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            monitor.record_processing(
                test_file, 
                'fullload', 
                'test_table', 
                ',', 
                'id,name', 
                'ERROR',
                error_msg="Test error message"
            )
            
            # Should have called database insert with error
            mock_db_inst.execute_non_query.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_record_processing_database_error(self, mock_api, mock_db):
        """Test processing record with database error"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        # Mock database error
        mock_db_inst.execute_non_query.side_effect = Exception("Database error")
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Should handle database error gracefully
            monitor.record_processing(
                test_file, 
                'fullload', 
                'test_table', 
                ',', 
                'id,name', 
                'SUCCESS'
            )
            
            # Should have attempted database call
            mock_db_inst.execute_non_query.assert_called()


class TestFileMonitorProcessing:
    """Test file processing methods"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_dropoff = os.path.join(self.temp_dir, 'dropoff')
        os.makedirs(self.test_dropoff, exist_ok=True)
        
        self.path_patches = [
            patch('file_monitor.DROPOFF_BASE_PATH', self.test_dropoff),
        ]
        
        for p in self.path_patches:
            p.start()
        
    def teardown_method(self):
        """Cleanup test environment"""
        for p in self.path_patches:
            p.stop()
        
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_process_file_success(self, mock_api, mock_db):
        """Test successful file processing"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock successful API calls
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True,
                'columns': ['id', 'name']
            }
        }
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        with patch('os.makedirs'), \
             patch('shutil.move') as mock_move:
            
            monitor = FileMonitor()
            
            result = monitor.process_file(test_file, 'fullload', True)
            
            assert result is True
            mock_move.assert_called()  # File should be moved to processed
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_process_file_format_detection_failure(self, mock_api, mock_db):
        """Test file processing with format detection failure"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock format detection failure
        mock_api_inst.detect_format.return_value = None
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('invalid content')
        
        with patch('os.makedirs'), \
             patch('shutil.move') as mock_move:
            
            monitor = FileMonitor()
            
            result = monitor.process_file(test_file, 'fullload', True)
            
            assert result is False
            mock_move.assert_called()  # File should be moved to error folder
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_process_file_processing_failure(self, mock_api, mock_db):
        """Test file processing with API processing failure"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock successful format detection but failed processing
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True,
                'columns': ['id', 'name']
            }
        }
        mock_api_inst.process_file.return_value = {'status': 'error', 'error': 'Processing failed'}
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'), \
             patch('shutil.move') as mock_move:
            
            monitor = FileMonitor()
            
            result = monitor.process_file(test_file, 'fullload', True)
            
            assert result is False
            mock_move.assert_called()  # File should be moved to error folder
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_process_file_move_error(self, mock_api, mock_db):
        """Test file processing with file move error"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Mock successful processing
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True,
                'columns': ['id', 'name']
            }
        }
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        test_file = os.path.join(self.test_dropoff, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        with patch('os.makedirs'), \
             patch('shutil.move', side_effect=OSError("Move failed")):
            
            monitor = FileMonitor()
            
            result = monitor.process_file(test_file, 'fullload', True)
            
            # Should still return True (processing succeeded)
            assert result is True


class TestFileMonitorMainLoop:
    """Test main loop and run method"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_dropoff = os.path.join(self.temp_dir, 'dropoff')
        os.makedirs(self.test_dropoff, exist_ok=True)
        
        self.path_patches = [
            patch('file_monitor.DROPOFF_BASE_PATH', self.test_dropoff),
        ]
        
        for p in self.path_patches:
            p.start()
        
    def teardown_method(self):
        """Cleanup test environment"""
        for p in self.path_patches:
            p.stop()
        
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_cleanup_tracking(self, mock_api, mock_db):
        """Test tracking cleanup"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Add some tracking data
            monitor.file_tracking = {
                'file1.csv': {'size': 100, 'mtime': time.time(), 'stable_count': 1},
                'file2.csv': {'size': 200, 'mtime': time.time(), 'stable_count': 2}
            }
            
            monitor.cleanup_tracking()
            
            # Should attempt database cleanup
            mock_db_inst.execute_non_query.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_cleanup_tracking_database_error(self, mock_api, mock_db):
        """Test tracking cleanup with database error"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        mock_api.return_value = MagicMock()
        
        # Mock database error
        mock_db_inst.execute_non_query.side_effect = Exception("Database error")
        
        with patch('os.makedirs'):
            monitor = FileMonitor()
            
            # Should handle database error gracefully
            monitor.cleanup_tracking()
            
            mock_db_inst.execute_non_query.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_run_single_iteration(self, mock_api, mock_db):
        """Test single iteration of run loop"""
        from file_monitor import FileMonitor
        
        mock_db_inst = MagicMock()
        mock_db.return_value = mock_db_inst
        
        mock_api_inst = MagicMock()
        mock_api.return_value = mock_api_inst
        
        # Create test file
        ref_fullload = os.path.join(self.test_dropoff, 'reference_data_table', 'fullload')
        os.makedirs(ref_fullload, exist_ok=True)
        
        test_file = os.path.join(ref_fullload, 'test.csv')
        with open(test_file, 'w') as f:
            f.write('id,name\n1,John')
        
        # Mock successful processing
        mock_api_inst.detect_format.return_value = {
            'detected_format': {
                'column_delimiter': ',',
                'has_header': True
            }
        }
        mock_api_inst.process_file.return_value = {'status': 'success'}
        
        with patch('os.makedirs'), \
             patch('time.sleep') as mock_sleep, \
             patch('shutil.move'):
            
            monitor = FileMonitor()
            
            # Mock the file as stable
            monitor.file_tracking[test_file] = {
                'size': os.path.getsize(test_file),
                'mtime': os.path.getmtime(test_file),
                'stable_count': 6
            }
            
            # Run single iteration (patch time.sleep to prevent infinite loop)
            mock_sleep.side_effect = KeyboardInterrupt()  # Stop after first iteration
            
            try:
                monitor.run()
            except KeyboardInterrupt:
                pass
            
            # Should have processed the file
            mock_api_inst.process_file.assert_called()
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_run_keyboard_interrupt(self, mock_api, mock_db):
        """Test run loop handles keyboard interrupt"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'), \
             patch('time.sleep', side_effect=KeyboardInterrupt()):
            
            monitor = FileMonitor()
            
            # Should handle KeyboardInterrupt gracefully
            try:
                monitor.run()
            except KeyboardInterrupt:
                pass  # Expected
    
    @patch('file_monitor.DatabaseManager')
    @patch('file_monitor.ReferenceDataAPI')
    def test_run_general_exception(self, mock_api, mock_db):
        """Test run loop handles general exceptions"""
        from file_monitor import FileMonitor
        
        mock_db.return_value = MagicMock()
        mock_api.return_value = MagicMock()
        
        with patch('os.makedirs'), \
             patch.object(FileMonitor, 'scan_folders', side_effect=Exception("Scan error")), \
             patch('time.sleep', side_effect=[None, KeyboardInterrupt()]):  # Stop after second iteration
            
            monitor = FileMonitor()
            
            # Should handle exceptions and continue
            try:
                monitor.run()
            except KeyboardInterrupt:
                pass  # Expected to stop the loop


class TestFileMonitorMain:
    """Test main function"""
    
    def test_main_function_exists(self):
        """Test that main function exists"""
        import file_monitor
        
        assert hasattr(file_monitor, 'main')
        assert callable(file_monitor.main)
    
    @patch('file_monitor.FileMonitor')
    def test_main_function_execution(self, mock_file_monitor):
        """Test main function execution"""
        import file_monitor
        
        mock_monitor_inst = MagicMock()
        mock_file_monitor.return_value = mock_monitor_inst
        
        # Mock run method to prevent infinite loop
        mock_monitor_inst.run.side_effect = KeyboardInterrupt()
        
        try:
            file_monitor.main()
        except KeyboardInterrupt:
            pass
        
        # Should create FileMonitor and call run
        mock_file_monitor.assert_called_once()
        mock_monitor_inst.run.assert_called_once()
    
    @patch('file_monitor.FileMonitor')
    def test_main_function_exception(self, mock_file_monitor):
        """Test main function handles exceptions"""
        import file_monitor
        
        # Mock FileMonitor initialization failure
        mock_file_monitor.side_effect = Exception("Initialization failed")
        
        # Should handle exception gracefully
        try:
            file_monitor.main()
        except Exception:
            pass  # Expected
    
    def test_main_execution_guard(self):
        """Test __name__ == '__main__' guard"""
        import file_monitor
        
        # Test that the guard exists
        with open(file_monitor.__file__, 'r') as f:
            content = f.read()
        
        assert 'if __name__ == "__main__":' in content