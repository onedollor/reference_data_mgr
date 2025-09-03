"""
Working test coverage for utils/file_handler.py focusing on synchronous methods
"""

import os
import tempfile
import pytest
from unittest.mock import patch, mock_open, MagicMock
from utils.file_handler import FileHandler


class TestFileHandlerWorking:
    """Working tests for file handler focusing on synchronous methods"""
    
    def setup_method(self):
        """Setup for each test"""
        # Use temporary directories for testing
        self.temp_dir = tempfile.mkdtemp()
        
    def test_init_with_environment_variables(self):
        """Test FileHandler initialization with environment variables"""
        with patch.dict('os.environ', {
            'temp_location': '/tmp/test_temp',
            'archive_location': '/tmp/test_archive', 
            'format_location': '/tmp/test_format'
        }):
            with patch('os.makedirs') as mock_makedirs:
                handler = FileHandler()
                
                assert handler.temp_location == '/tmp/test_temp'
                assert handler.archive_location == '/tmp/test_archive'
                assert handler.format_location == '/tmp/test_format'
                
                # Should call makedirs for each directory
                assert mock_makedirs.call_count == 3
    
    def test_init_with_default_values(self):
        """Test FileHandler initialization with default values"""
        with patch('os.makedirs') as mock_makedirs:
            handler = FileHandler()
            
            # Should use default Windows paths
            assert "reference_data" in handler.temp_location
            assert "reference_data" in handler.archive_location
            assert "reference_data" in handler.format_location
    
    def test_ensure_directories_success(self):
        """Test _ensure_directories method success case"""
        with patch('os.makedirs') as mock_makedirs:
            handler = FileHandler()
            
            # makedirs should be called during init
            assert mock_makedirs.call_count == 3
            
            # Check that exist_ok=True is passed
            for call in mock_makedirs.call_args_list:
                assert call[1]['exist_ok'] is True
    
    def test_ensure_directories_with_exception(self):
        """Test _ensure_directories method with exception"""
        with patch('os.makedirs', side_effect=OSError("Permission denied")):
            with patch('builtins.print') as mock_print:
                handler = FileHandler()
                
                # Should print warning messages
                assert mock_print.call_count == 3
                for call in mock_print.call_args_list:
                    assert "Warning: Could not create directory" in str(call)
    
    def test_sanitize_filename(self):
        """Test _sanitize_filename method"""
        handler = FileHandler()
        
        # Test basic sanitization
        result = handler._sanitize_filename('test<file>.csv')
        assert result == 'test_file_.csv'
        
        # Test with multiple unsafe characters
        result = handler._sanitize_filename('bad:file|name?.csv')
        assert result == 'bad_file_name_.csv'
        
        # Test with path components (should be removed)
        result = handler._sanitize_filename('/path/to/file.csv')
        assert result == 'file.csv'
        
        # Test normal filename (should be unchanged)
        result = handler._sanitize_filename('normal_file.csv')
        assert result == 'normal_file.csv'
    
    def test_extract_table_base_name_simple(self):
        """Test extract_table_base_name with simple filename"""
        handler = FileHandler()
        
        # Test simple CSV filename
        result = handler.extract_table_base_name('test_table.csv')
        assert result == 'test_table'
        
        # Test filename without extension
        result = handler.extract_table_base_name('test_table')
        assert result == 'test_table'
    
    def test_extract_table_base_name_with_timestamps(self):
        """Test extract_table_base_name with timestamp patterns"""
        handler = FileHandler()
        
        # Test with yyyyMMdd pattern
        result = handler.extract_table_base_name('test_table.20240101.csv')
        assert result == 'test_table'
        
        # Test with yyyyMMdd underscore pattern
        result = handler.extract_table_base_name('test_table_20240101.csv')
        assert result == 'test_table'
        
        # Test with full timestamp pattern
        result = handler.extract_table_base_name('test_table.20240101.120000.csv')
        assert result == 'test_table'
        
        # Test with underscore full timestamp pattern
        result = handler.extract_table_base_name('test_table_20240101_120000.csv')
        assert result == 'test_table'
        
        # Test with 14-digit timestamp
        result = handler.extract_table_base_name('test_table.20240101120000.csv')
        assert result == 'test_table'
        
        # Test with underscore 14-digit timestamp
        result = handler.extract_table_base_name('test_table_20240101120000.csv')
        assert result == 'test_table'
    
    def test_extract_table_base_name_sanitization(self):
        """Test extract_table_base_name sanitization"""
        handler = FileHandler()
        
        # Test with invalid characters
        result = handler.extract_table_base_name('test-table@#$.csv')
        assert result == 'test_table___'
        
        # Test starting with number
        result = handler.extract_table_base_name('123_table.csv')
        assert result == 't_123_table'
        
        # Test empty result
        result = handler.extract_table_base_name('###.csv')
        assert result == 'unknown_table'
    
    def test_extract_table_base_name_exception_handling(self):
        """Test extract_table_base_name exception handling"""
        handler = FileHandler()
        
        # Mock re.search to raise exception
        with patch('re.search', side_effect=Exception("Regex error")):
            with pytest.raises(Exception, match="Failed to extract table name"):
                handler.extract_table_base_name('test.csv')
    
    def test_get_file_info_existing_file(self):
        """Test get_file_info with existing file"""
        handler = FileHandler()
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_file_path = tmp_file.name
        
        try:
            result = handler.get_file_info(tmp_file_path)
            
            assert result['file_path'] == tmp_file_path
            assert result['filename'] == os.path.basename(tmp_file_path)
            assert result['size_bytes'] > 0
            assert result['size_mb'] > 0
            assert 'modified_time' in result
            assert 'created_time' in result
        finally:
            os.unlink(tmp_file_path)
    
    def test_get_file_info_nonexistent_file(self):
        """Test get_file_info with nonexistent file"""
        handler = FileHandler()
        
        with pytest.raises(Exception, match="Failed to get file info"):
            handler.get_file_info('/nonexistent/file.csv')
    
    def test_validate_csv_file_valid(self):
        """Test validate_csv_file with valid CSV"""
        handler = FileHandler()
        
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("col1,col2\\nval1,val2")
            tmp_file_path = tmp_file.name
        
        try:
            result = handler.validate_csv_file(tmp_file_path, max_size_bytes=1024)
            
            assert result['valid'] is True
            assert len(result['errors']) == 0
            assert 'file_info' in result
        finally:
            os.unlink(tmp_file_path)
    
    def test_validate_csv_file_too_large(self):
        """Test validate_csv_file with file too large"""
        handler = FileHandler()
        
        # Create a temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_file_path = tmp_file.name
        
        try:
            result = handler.validate_csv_file(tmp_file_path, max_size_bytes=1)  # Very small limit
            
            assert result['valid'] is False
            assert any("exceeds maximum limit" in error for error in result['errors'])
        finally:
            os.unlink(tmp_file_path)
    
    def test_validate_csv_file_wrong_extension(self):
        """Test validate_csv_file with wrong file extension"""
        handler = FileHandler()
        
        # Create a temporary file without CSV extension
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_file_path = tmp_file.name
        
        try:
            result = handler.validate_csv_file(tmp_file_path)
            
            assert result['valid'] is False
            assert any("must have .csv extension" in error for error in result['errors'])
        finally:
            os.unlink(tmp_file_path)
    
    def test_validate_csv_file_with_exception(self):
        """Test validate_csv_file with exception"""
        handler = FileHandler()
        
        # Test with nonexistent file
        result = handler.validate_csv_file('/nonexistent/file.csv')
        
        assert result['valid'] is False
        assert any("Validation failed" in error for error in result['errors'])
        assert result['file_info'] is None
    
    def test_move_to_archive_success(self):
        """Test move_to_archive successful case"""
        handler = FileHandler()
        
        # Create temporary source file and archive directory
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as src_file:
            src_file.write("test content")
            src_file_path = src_file.name
        
        archive_dir = tempfile.mkdtemp()
        handler.archive_location = archive_dir
        
        try:
            result = handler.move_to_archive(src_file_path, "original_file.csv")
            
            # Check that file was moved
            assert os.path.exists(result)
            assert not os.path.exists(src_file_path)
            assert result.startswith(archive_dir)
            assert "original_file" in result
            assert result.endswith(".csv")
        finally:
            # Clean up
            if os.path.exists(result):
                os.unlink(result)
            os.rmdir(archive_dir)
    
    def test_move_to_archive_with_exception(self):
        """Test move_to_archive with exception"""
        handler = FileHandler()
        
        # Test with nonexistent source file
        with pytest.raises(Exception, match="Failed to archive file"):
            handler.move_to_archive('/nonexistent/file.csv', 'original.csv')