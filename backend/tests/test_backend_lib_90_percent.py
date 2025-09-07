"""
Additional tests to push backend_lib.py to >90% coverage
Targeting missing lines: 51, 147-198, 249, 290-292, 303, 386
"""

import pytest
import os
import tempfile
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from pathlib import Path
from datetime import datetime
import json


class TestBackendLib90Percent:
    """Additional tests to reach >90% coverage for backend_lib.py"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
    
    @patch('backend_lib.CSVFormatDetector')
    @patch('backend_lib.Logger')
    def test_detect_format_success_return_structure(self, mock_logger, mock_detector):
        """Test detect_format success return structure - covers line 51"""
        import backend_lib
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock detector success
        mock_detector_instance = MagicMock()
        mock_detector.return_value = mock_detector_instance
        mock_detector_instance.detect_format.return_value = {
            'column_delimiter': ',',
            'has_header': True,
            'text_qualifier': '"'
        }
        
        # Create backend lib instance
        backend = backend_lib.ReferenceDataAPI()
        
        result = backend.detect_format('/test/file.csv')
        
        # Verify the success return structure (line 51-54)
        assert result['success'] is True
        assert result['file_path'] == '/test/file.csv'
        assert 'detected_format' in result
        assert result['detected_format']['column_delimiter'] == ','
    
    @patch('backend_lib.CSVFormatDetector')
    @patch('backend_lib.Logger')
    @patch('builtins.open', new_callable=mock_open)
    @patch('json.dump')
    def test_process_file_sync_format_creation(self, mock_json_dump, mock_file, mock_logger, mock_detector):
        """Test process_file_sync format file creation - covers lines 147-198"""
        import backend_lib
        from pathlib import Path
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock detector
        mock_detector_instance = MagicMock()
        mock_detector.return_value = mock_detector_instance
        mock_detector_instance.detect_format.return_value = {
            'column_delimiter': ',',
            'has_header': True,
            'text_qualifier': '"',
            'row_delimiter': '\n'
        }
        
        # Mock DataIngester
        with patch('backend_lib.DataIngester') as mock_ingester:
            mock_ingester_instance = MagicMock()
            mock_ingester.return_value = mock_ingester_instance
            
            # Mock the async ingest_data method to return a result
            mock_ingester_instance.ingest_data = AsyncMock(return_value={
                'success': True,
                'rows_processed': 100
            })
            
            # Create backend lib instance
            backend = backend_lib.ReferenceDataAPI()
            
            # Test file processing
            result = backend.process_file_sync(
                file_path='/test/data.csv',
                load_type='fullload',
                table_name='test_table',
                target_schema='ref'
            )
            
            # Verify format file creation was attempted (lines 147-198)
            mock_file.assert_called()
            mock_json_dump.assert_called()
            
            # Verify the format config structure was created
            format_config_call = mock_json_dump.call_args[0][0]
            assert 'file_info' in format_config_call
            assert 'csv_format' in format_config_call
            assert format_config_call['csv_format']['column_delimiter'] == ','
    
    @patch('backend_lib.CSVFormatDetector')
    @patch('backend_lib.Logger')
    def test_process_file_sync_with_existing_event_loop(self, mock_logger, mock_detector):
        """Test process_file_sync with existing event loop - covers line 249"""
        import backend_lib
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock detector
        mock_detector_instance = MagicMock()
        mock_detector.return_value = mock_detector_instance
        mock_detector_instance.detect_format.return_value = {'column_delimiter': ','}
        
        # Create a mock event loop that exists but is not running
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = False
        mock_loop.run_until_complete.return_value = {'success': True}
        
        with patch('asyncio.get_event_loop', return_value=mock_loop):
            with patch('backend_lib.DataIngester'):
                backend = backend_lib.ReferenceDataAPI()
                
                result = backend.process_file_sync('/test.csv', 'fullload', 'test_table')
                
                # Verify run_until_complete was called (line 249-253)
                mock_loop.run_until_complete.assert_called_once()
    
    @patch('backend_lib.DatabaseManager')
    @patch('backend_lib.Logger')
    def test_get_table_info_exception_handling(self, mock_logger, mock_db_manager):
        """Test get_table_info exception handling - covers lines 290-292"""
        import backend_lib
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock database manager to raise exception
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.table_exists.side_effect = Exception("Database connection failed")
        
        backend = backend_lib.ReferenceDataAPI()
        
        result = backend.get_table_info('test_table', 'ref')
        
        # Verify exception handling return structure (lines 290-297)
        assert result['success'] is False
        assert 'error' in result
        assert result['table_name'] == 'test_table'
        assert result['schema'] == 'ref'
        assert 'Database connection failed' in result['error']
        
        # Verify error was logged (line 291)
        mock_logger_instance.error.assert_called()
    
    @patch('backend_lib.DatabaseManager')
    @patch('backend_lib.Logger')
    def test_get_all_tables_exception_handling(self, mock_logger, mock_db_manager):
        """Test get_all_tables exception handling - covers line 303"""
        import backend_lib
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock database manager to raise exception
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.get_all_tables.side_effect = Exception("Database error")
        
        backend = backend_lib.ReferenceDataAPI()
        
        result = backend.get_all_tables()
        
        # Verify exception handling (around line 303)
        assert 'success' in result
        assert result['success'] is False
    
    @patch('backend_lib.DatabaseManager')
    @patch('backend_lib.Logger')
    def test_health_check_database_failure(self, mock_logger, mock_db_manager):
        """Test health_check with database failure - covers line 386"""
        import backend_lib
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock database manager test_connection to fail
        mock_db_instance = MagicMock()
        mock_db_manager.return_value = mock_db_instance
        mock_db_instance.test_connection.return_value = {
            'success': False,
            'error': 'Connection failed'
        }
        
        backend = backend_lib.ReferenceDataAPI()
        
        result = backend.health_check()
        
        # Verify health check failure handling (line 386)
        assert result['status'] == 'unhealthy'
        assert 'database' in result['checks']
        assert result['checks']['database']['status'] == 'fail'
    
    @patch('backend_lib.CSVFormatDetector')
    @patch('backend_lib.Logger')
    @patch('builtins.open', new_callable=mock_open)
    def test_process_file_sync_complete_format_config(self, mock_file, mock_logger, mock_detector):
        """Test complete format config creation - covers remaining lines in 147-198 range"""
        import backend_lib
        
        # Mock logger
        mock_logger_instance = MagicMock()
        mock_logger.return_value = mock_logger_instance
        
        # Mock detector with complete format info
        mock_detector_instance = MagicMock()
        mock_detector.return_value = mock_detector_instance
        mock_detector_instance.detect_format.return_value = {
            'column_delimiter': ';',
            'has_header': True,
            'text_qualifier': "'",
            'row_delimiter': '\r\n',
            'has_trailer': True,
            'trailer_line': 'END'
        }
        
        with patch('json.dump') as mock_json_dump:
            with patch('backend_lib.DataIngester') as mock_ingester:
                # Mock ingester
                mock_ingester_instance = MagicMock()
                mock_ingester.return_value = mock_ingester_instance
                mock_ingester_instance.ingest_data = AsyncMock(return_value={'success': True})
                
                backend = backend_lib.ReferenceDataAPI()
                
                result = backend.process_file_sync(
                    file_path='/test/complex.csv',
                    load_type='append',
                    table_name='complex_table',
                    target_schema='staging'
                )
                
                # Verify complete format config was created
                format_config = mock_json_dump.call_args[0][0]
                
                # Test all format config fields (lines 147-198)
                assert format_config['file_info']['original_filename'] == 'complex.csv'
                assert 'upload_timestamp' in format_config['file_info']
                assert format_config['file_info']['format_version'] == '1.0'
                
                csv_format = format_config['csv_format']
                assert csv_format['header_delimiter'] == ';'
                assert csv_format['column_delimiter'] == ';' 
                assert csv_format['row_delimiter'] == '\r\n'
                assert csv_format['text_qualifier'] == "'"
                assert csv_format['has_header'] is True
                assert csv_format['has_trailer'] is True
                assert csv_format['trailer_line'] == 'END'
                
                processing_opts = format_config['processing_options']
                assert processing_opts['encoding'] == 'utf-8'
                assert processing_opts['skip_blank_lines'] is True
                assert processing_opts['strip_whitespace'] is True