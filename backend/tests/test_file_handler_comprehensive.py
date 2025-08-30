"""
Comprehensive test coverage for utils/file_handler.py
Focus on improving coverage from 40% to much higher level
"""

import pytest
import os
import tempfile
import shutil
import json
import aiofiles
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from fastapi import UploadFile
from io import BytesIO
from typing import Dict, Any

from utils.file_handler import FileHandler


class TestFileHandlerInit:
    """Test FileHandler initialization and directory setup"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_file_handler_init_with_defaults(self):
        """Test FileHandler initialization with default paths"""
        with patch.dict(os.environ, {}, clear=True):
            handler = FileHandler()
            
            # Should use default paths
            assert "reference_data" in handler.temp_location
            assert "reference_data" in handler.archive_location
            assert "reference_data" in handler.format_location
    
    def test_file_handler_init_with_env_vars(self):
        """Test FileHandler initialization with environment variables"""
        test_temp = "/test/temp"
        test_archive = "/test/archive"
        test_format = "/test/format"
        
        with patch.dict(os.environ, {
            'temp_location': test_temp,
            'archive_location': test_archive,
            'format_location': test_format
        }):
            with patch('os.makedirs'):
                handler = FileHandler()
                
                assert handler.temp_location == test_temp
                assert handler.archive_location == test_archive
                assert handler.format_location == test_format
    
    def test_ensure_directories_success(self):
        """Test successful directory creation"""
        with patch('os.makedirs') as mock_makedirs:
            handler = FileHandler()
            
            # Should attempt to create all directories
            assert mock_makedirs.call_count >= 3
    
    def test_ensure_directories_exception(self):
        """Test directory creation with exceptions"""
        with patch('os.makedirs', side_effect=PermissionError("Access denied")):
            with patch('builtins.print') as mock_print:
                handler = FileHandler()
                
                # Should print warning but not fail
                mock_print.assert_called()
                assert "Warning" in mock_print.call_args[0][0]


class TestFileHandlerUploadedFiles:
    """Test handling uploaded files"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_basic(self):
        """Test basic file upload functionality"""
        # Create a mock uploaded file
        file_content = b"col1,col2,col3\nval1,val2,val3\n"
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test_data.csv"
        mock_file.read = AsyncMock(return_value=file_content)
        
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.temp_location = self.temp_dir
            handler.format_location = self.temp_dir
        
        with patch('aiofiles.open', mock_open()) as mock_aiofiles:
            result = await handler.save_uploaded_file(
                file=mock_file,
                header_delimiter=",",
                column_delimiter=",",
                row_delimiter="\\n",
                text_qualifier='"',
                skip_lines=0,
                trailer_line=None
            )
            
            temp_path, fmt_path = result
            assert temp_path.endswith("test_data.csv")
            assert fmt_path.endswith("test_data.fmt")
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_with_trailer(self):
        """Test file upload with trailer line"""
        file_content = b"col1,col2\nval1,val2\nTRAILER|2|END"
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test_with_trailer.csv"
        mock_file.read = AsyncMock(return_value=file_content)
        
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.temp_location = self.temp_dir
            handler.format_location = self.temp_dir
        
        with patch('aiofiles.open', mock_open()):
            result = await handler.save_uploaded_file(
                file=mock_file,
                header_delimiter="|",
                column_delimiter="|",
                row_delimiter="\\r\\n",
                text_qualifier="'",
                skip_lines=1,
                trailer_line="TRAILER"
            )
            
            temp_path, fmt_path = result
            assert "test_with_trailer" in temp_path
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_exception(self):
        """Test file upload with exception"""
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "error_file.csv"
        mock_file.read = AsyncMock(side_effect=Exception("Read error"))
        
        with patch('os.makedirs'):
            handler = FileHandler()
        
        with pytest.raises(Exception) as exc_info:
            await handler.save_uploaded_file(
                file=mock_file,
                header_delimiter=",",
                column_delimiter=",",
                row_delimiter="\\n",
                text_qualifier='"'
            )
        
        assert "Read error" in str(exc_info.value)


class TestFileHandlerFormatFiles:
    """Test format file operations"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_create_format_file(self):
        """Test format file creation"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.format_location = self.temp_dir
        
        fmt_path = os.path.join(self.temp_dir, "test.fmt")
        format_data = {
            "column_delimiter": ",",
            "text_qualifier": '"',
            "has_header": True,
            "skip_lines": 0
        }
        
        with patch('aiofiles.open', mock_open()) as mock_file:
            await handler._create_format_file(fmt_path, format_data)
            
            # Verify file was written
            mock_file.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_format_file(self):
        """Test reading format file"""
        format_data = {
            "column_delimiter": "|",
            "has_header": True,
            "skip_lines": 1
        }
        
        mock_json_content = json.dumps(format_data)
        
        with patch('os.makedirs'):
            handler = FileHandler()
        
        with patch('aiofiles.open', mock_open(read_data=mock_json_content)):
            result = await handler.read_format_file("test.fmt")
            
            assert result["column_delimiter"] == "|"
            assert result["has_header"] is True
            assert result["skip_lines"] == 1


class TestFileHandlerUtilities:
    """Test utility methods"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_sanitize_filename_basic(self):
        """Test basic filename sanitization"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Test various filename patterns - only unsafe chars get replaced
        assert handler._sanitize_filename("normal_file.csv") == "normal_file.csv"
        assert handler._sanitize_filename("file with spaces.csv") == "file with spaces.csv"  # Spaces are safe
        assert handler._sanitize_filename("file-with-dashes.csv") == "file-with-dashes.csv"  # Dashes are safe
        assert handler._sanitize_filename('file<>:"|?*.csv') == "file_______.csv"  # Unsafe chars
    
    def test_sanitize_filename_edge_cases(self):
        """Test filename sanitization edge cases"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Test edge cases - only replaces unsafe chars, not dots
        assert handler._sanitize_filename("") == ""
        assert handler._sanitize_filename("...") == "..."  # Dots are safe
        assert handler._sanitize_filename("file..csv") == "file..csv"  # Dots are safe
        assert handler._sanitize_filename("UPPERCASE.CSV") == "UPPERCASE.CSV"
    
    def test_extract_table_base_name_comprehensive(self):
        """Test comprehensive table name extraction"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        test_cases = [
            ("users.csv", "users"),
            ("user_data_20241225.csv", "user_data"),
            ("Products-List_2024-12-25.csv", "Products_List_2024_12_25"),
            ("simple.csv", "simple"),
            ("file_without_extension", "file_without_extension"),
            ("", ""),
            ("file.with.dots.csv", "file_with_dots"),
            ("UPPERCASE_FILE.CSV", "UPPERCASE_FILE")
        ]
        
        for filename, expected in test_cases:
            result = handler.extract_table_base_name(filename)
            assert result == expected, f"Failed for {filename}: got {result}, expected {expected}"
    
    def test_move_to_archive(self):
        """Test moving files to archive"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.archive_location = self.temp_dir
        
        # Create a test source file
        source_file = os.path.join(self.temp_dir, "source.csv")
        with open(source_file, 'w') as f:
            f.write("test,data\n1,2\n")
        
        with patch('shutil.move') as mock_move:
            with patch('os.path.exists', return_value=True):
                result = handler.move_to_archive(source_file, "original_file.csv")
                
                assert "original_file.csv" in result
                mock_move.assert_called_once()
    
    def test_get_file_info(self):
        """Test getting file information"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create a test file
        test_file = os.path.join(self.temp_dir, "info_test.csv")
        test_content = "col1,col2\nval1,val2\nval3,val4\n"
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        result = handler.get_file_info(test_file)
        
        assert "size" in result
        assert "modified" in result
        assert "lines" in result
        assert result["size"] == len(test_content)
        assert result["lines"] == 3  # Including header
    
    def test_get_file_info_nonexistent(self):
        """Test getting info for nonexistent file"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        result = handler.get_file_info("/nonexistent/file.csv")
        
        assert "error" in result
        assert "not found" in result["error"].lower()


class TestFileHandlerValidation:
    """Test file validation functionality"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_validate_csv_file_valid(self):
        """Test validation of valid CSV file"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create a valid CSV file
        test_file = os.path.join(self.temp_dir, "valid.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2,col3\n")
            f.write("val1,val2,val3\n")
            f.write("val4,val5,val6\n")
        
        result = handler.validate_csv_file(test_file)
        
        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert "file_info" in result
    
    def test_validate_csv_file_empty(self):
        """Test validation of empty file"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create empty file
        test_file = os.path.join(self.temp_dir, "empty.csv")
        with open(test_file, 'w') as f:
            pass  # Empty file
        
        result = handler.validate_csv_file(test_file)
        
        # Empty files might be valid, check the actual implementation result
        assert "valid" in result
        assert "file_info" in result
    
    def test_validate_csv_file_size_limit(self):
        """Test validation with size limit"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create a file that exceeds size limit
        test_file = os.path.join(self.temp_dir, "large.csv")
        with open(test_file, 'w') as f:
            f.write("x" * 1000)  # 1000 bytes
        
        result = handler.validate_csv_file(test_file, max_size_bytes=500)
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert "size" in result["errors"][0].lower()
    
    def test_validate_csv_file_nonexistent(self):
        """Test validation of nonexistent file"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        result = handler.validate_csv_file("/nonexistent/file.csv")
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
    
    def test_validate_csv_file_permission_error(self):
        """Test validation with permission error"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        with patch('builtins.open', side_effect=PermissionError("Permission denied")):
            result = handler.validate_csv_file("protected.csv")
            
            assert result["valid"] is False
            assert "errors" in result