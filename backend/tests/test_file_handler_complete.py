"""
Complete test coverage for utils/file_handler.py to achieve high coverage
Focuses on missing coverage areas: async file operations, utility methods, and edge cases
"""

import pytest
import os
import tempfile
import shutil
import json
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from fastapi import UploadFile
from io import BytesIO

from utils.file_handler import FileHandler


class TestFileHandlerAsyncOperations:
    """Test async file operations with proper mocking"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_complete_workflow(self):
        """Test complete save_uploaded_file workflow"""
        # Create mock uploaded file
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "test_data.csv"
        mock_file.read = AsyncMock(return_value=b"col1,col2\nval1,val2\n")
        
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.temp_location = self.temp_dir
            handler.format_location = self.temp_dir
        
        # Mock aiofiles operations
        mock_write_file = AsyncMock()
        mock_write_fmt = AsyncMock()
        
        async def aiofiles_open_side_effect(path, mode, **kwargs):
            mock_file_obj = AsyncMock()
            if path.endswith('.csv'):
                mock_file_obj.write = mock_write_file
            elif path.endswith('.fmt'):
                mock_file_obj.write = mock_write_fmt
            return mock_file_obj
        
        with patch('aiofiles.open', side_effect=aiofiles_open_side_effect):
            with patch('utils.file_handler.datetime') as mock_datetime:
                mock_datetime.now.return_value.strftime.return_value = "20241225_120000"
                mock_datetime.now.return_value.isoformat.return_value = "2024-12-25T12:00:00"
                
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
                assert "20241225_120000_test_data.csv" in temp_path
                assert "20241225_120000_test_data.fmt" in fmt_path
                
                # Verify file operations were called
                mock_write_file.assert_called_once_with(b"col1,col2\nval1,val2\n")
                mock_write_fmt.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_with_trailer(self):
        """Test save_uploaded_file with trailer line"""
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "data_with_trailer.csv"
        mock_file.read = AsyncMock(return_value=b"header,data\nval1,val2\nTRAILER|2\n")
        
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.temp_location = self.temp_dir
            handler.format_location = self.temp_dir
        
        mock_write_file = AsyncMock()
        mock_write_fmt = AsyncMock()
        
        async def aiofiles_open_side_effect(path, mode, **kwargs):
            mock_file_obj = AsyncMock()
            if path.endswith('.csv'):
                mock_file_obj.write = mock_write_file
            elif path.endswith('.fmt'):
                mock_file_obj.write = mock_write_fmt
            return mock_file_obj
        
        with patch('aiofiles.open', side_effect=aiofiles_open_side_effect):
            with patch('utils.file_handler.datetime') as mock_datetime:
                mock_datetime.now.return_value.strftime.return_value = "20241225_130000"
                mock_datetime.now.return_value.isoformat.return_value = "2024-12-25T13:00:00"
                
                result = await handler.save_uploaded_file(
                    file=mock_file,
                    header_delimiter="|",
                    column_delimiter=",",
                    row_delimiter="\\r\\n",
                    text_qualifier="'",
                    skip_lines=1,
                    trailer_line="TRAILER"
                )
                
                temp_path, fmt_path = result
                assert "data_with_trailer.csv" in temp_path
                
                # Verify format file contains trailer info
                fmt_call_args = mock_write_fmt.call_args[0][0]
                fmt_config = json.loads(fmt_call_args)
                assert fmt_config["csv_format"]["has_trailer"] is True
                assert fmt_config["csv_format"]["trailer_line"] == "TRAILER"
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_exception_handling(self):
        """Test save_uploaded_file exception handling"""
        mock_file = MagicMock(spec=UploadFile)
        mock_file.filename = "error_file.csv"
        mock_file.read = AsyncMock(side_effect=Exception("Read failed"))
        
        with patch('os.makedirs'):
            handler = FileHandler()
        
        with pytest.raises(Exception, match="Failed to save uploaded file: Read failed"):
            await handler.save_uploaded_file(
                file=mock_file,
                header_delimiter=",",
                column_delimiter=",",
                row_delimiter="\\n",
                text_qualifier='"'
            )
    
    @pytest.mark.asyncio
    async def test_create_format_file_detailed(self):
        """Test _create_format_file method in detail"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.format_location = self.temp_dir
        
        mock_write = AsyncMock()
        
        async def mock_aiofiles_open(path, mode, **kwargs):
            mock_file = AsyncMock()
            mock_file.write = mock_write
            return mock_file
        
        with patch('aiofiles.open', side_effect=mock_aiofiles_open):
            with patch('utils.file_handler.datetime') as mock_datetime:
                mock_datetime.now.return_value.isoformat.return_value = "2024-12-25T14:00:00"
                
                result = await handler._create_format_file(
                    filename="test.csv",
                    header_delimiter="|",
                    column_delimiter=",",
                    row_delimiter="\\r\\n",
                    text_qualifier="'",
                    skip_lines=2,
                    trailer_line="TRAILER_TEXT",
                    timestamp="20241225_140000"
                )
                
                assert result.endswith("20241225_140000_test.fmt")
                
                # Verify format configuration
                mock_write.assert_called_once()
                config_json = mock_write.call_args[0][0]
                config = json.loads(config_json)
                
                assert config["file_info"]["original_filename"] == "test.csv"
                assert config["file_info"]["format_version"] == "1.0"
                assert config["csv_format"]["header_delimiter"] == "|"
                assert config["csv_format"]["column_delimiter"] == ","
                assert config["csv_format"]["row_delimiter"] == "\\r\\n"
                assert config["csv_format"]["text_qualifier"] == "'"
                assert config["csv_format"]["skip_lines"] == 2
                assert config["csv_format"]["has_header"] is True
                assert config["csv_format"]["has_trailer"] is True
                assert config["csv_format"]["trailer_line"] == "TRAILER_TEXT"
                assert config["processing_options"]["encoding"] == "utf-8"
    
    @pytest.mark.asyncio
    async def test_read_format_file_success(self):
        """Test reading format file successfully"""
        format_data = {
            "csv_format": {
                "column_delimiter": "|",
                "has_header": True,
                "skip_lines": 1
            }
        }
        
        mock_read = AsyncMock(return_value=json.dumps(format_data))
        
        async def mock_aiofiles_open(path, mode, **kwargs):
            mock_file = AsyncMock()
            mock_file.read = mock_read
            return mock_file
        
        with patch('os.makedirs'):
            handler = FileHandler()
        
        with patch('aiofiles.open', side_effect=mock_aiofiles_open):
            result = await handler.read_format_file("test.fmt")
            
            assert result["csv_format"]["column_delimiter"] == "|"
            assert result["csv_format"]["has_header"] is True
            assert result["csv_format"]["skip_lines"] == 1


class TestFileHandlerUtilityMethods:
    """Test utility methods comprehensively"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_extract_table_base_name_comprehensive(self):
        """Test extract_table_base_name with all scenarios"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        test_cases = [
            ("users.csv", "users"),
            ("user_data_20241225.csv", "user_data"),
            ("Products-List_2024-12-25.csv", "Products_List_2024_12_25"),
            ("simple_file.csv", "simple_file"),
            ("file_without_extension", "file_without_extension"),
            ("", "unknown_table"),  # This is the actual behavior for empty string
            ("file.with.many.dots.csv", "file_with_many_dots"),
            ("UPPERCASE_FILE.CSV", "UPPERCASE_FILE"),
            ("file-with-dashes.csv", "file_with_dashes"),
            ("file@#$%special.csv", "file____special"),
            ("a.csv", "a"),  # Single character
            (".csv", "unknown_table"),  # Just extension
        ]
        
        for filename, expected in test_cases:
            result = handler.extract_table_base_name(filename)
            assert result == expected, f"Failed for '{filename}': got '{result}', expected '{expected}'"
    
    def test_move_to_archive_success(self):
        """Test successful move to archive"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.archive_location = self.temp_dir
        
        # Create a test source file
        source_file = os.path.join(self.temp_dir, "source.csv")
        with open(source_file, 'w') as f:
            f.write("test,data\n1,2\n")
        
        with patch('shutil.move') as mock_move:
            with patch('os.path.exists', return_value=True):
                with patch('utils.file_handler.datetime') as mock_datetime:
                    mock_datetime.now.return_value.strftime.return_value = "20241225_150000"
                    
                    result = handler.move_to_archive(source_file, "original.csv")
                    
                    assert "original_20241225_150000.csv" in result
                    mock_move.assert_called_once()
    
    def test_move_to_archive_file_not_exists(self):
        """Test move to archive when source file doesn't exist"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.archive_location = self.temp_dir
        
        with patch('os.path.exists', return_value=False):
            result = handler.move_to_archive("nonexistent.csv", "original.csv")
            
            assert result is None
    
    def test_get_file_info_success(self):
        """Test getting file info successfully"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create a test file
        test_file = os.path.join(self.temp_dir, "info_test.csv")
        test_content = "col1,col2\nval1,val2\nval3,val4\n"
        with open(test_file, 'w') as f:
            f.write(test_content)
        
        result = handler.get_file_info(test_file)
        
        assert "size_bytes" in result
        assert "size_mb" in result
        assert "modified_time" in result
        assert "created_time" in result
        assert "filename" in result
        assert "file_path" in result
        assert result["size_bytes"] == len(test_content)
        assert result["filename"] == "info_test.csv"
        assert result["file_path"] == test_file
    
    def test_get_file_info_file_not_exists(self):
        """Test get_file_info when file doesn't exist"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        with pytest.raises(Exception, match="Failed to get file info"):
            handler.get_file_info("/nonexistent/file.csv")


class TestFileHandlerValidation:
    """Test file validation methods"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_validate_csv_file_valid_file(self):
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
        assert result["file_info"]["filename"] == "valid.csv"
    
    def test_validate_csv_file_size_exceeded(self):
        """Test validation when file size exceeds limit"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create a large file
        test_file = os.path.join(self.temp_dir, "large.csv")
        with open(test_file, 'w') as f:
            f.write("x" * 1000)  # 1000 bytes
        
        result = handler.validate_csv_file(test_file, max_size_bytes=500)
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert "size" in result["errors"][0].lower()
    
    def test_validate_csv_file_wrong_extension(self):
        """Test validation of file without .csv extension"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Create a file without .csv extension
        test_file = os.path.join(self.temp_dir, "notcsv.txt")
        with open(test_file, 'w') as f:
            f.write("col1,col2\nval1,val2\n")
        
        result = handler.validate_csv_file(test_file)
        
        assert result["valid"] is False
        assert len(result["errors"]) > 0
        assert "csv extension" in result["errors"][0].lower()
    
    def test_validate_csv_file_with_default_size_limit(self):
        """Test validation using default size limit from environment"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        test_file = os.path.join(self.temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2\nval1,val2\n")
        
        with patch('os.getenv', return_value="1048576"):  # 1MB
            result = handler.validate_csv_file(test_file)  # No max_size_bytes parameter
            
            assert result["valid"] is True
            assert len(result["errors"]) == 0
    
    def test_validate_csv_file_exception_handling(self):
        """Test validation exception handling"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Mock get_file_info to raise exception
        with patch.object(handler, 'get_file_info', side_effect=Exception("File access error")):
            result = handler.validate_csv_file("some_file.csv")
            
            assert result["valid"] is False
            assert len(result["errors"]) > 0
            assert "file access error" in result["errors"][0].lower()


class TestFileHandlerEdgeCases:
    """Test edge cases and error conditions"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_sanitize_filename_comprehensive(self):
        """Test filename sanitization with all edge cases"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        test_cases = [
            ("normal_file.csv", "normal_file.csv"),
            ("file with spaces.csv", "file with spaces.csv"),  # Spaces are allowed
            ("file-with-dashes.csv", "file-with-dashes.csv"),  # Dashes are allowed
            ('file<>:"|?*.csv', "file_______.csv"),  # Unsafe chars replaced
            ("file/with\\path.csv", "file_with_path.csv"),  # Path separators replaced
            ("", ""),  # Empty string
            ("...csv", "...csv"),  # Only dots and extension
            ("file.multiple.dots.csv", "file.multiple.dots.csv"),  # Multiple dots allowed
            ("UPPERCASE.CSV", "UPPERCASE.CSV"),  # Case preserved
            ("файл.csv", "файл.csv"),  # Unicode characters preserved
            ("file\t\n\r.csv", "file\t\n\r.csv"),  # Whitespace preserved (only unsafe chars replaced)
        ]
        
        for input_filename, expected in test_cases:
            result = handler._sanitize_filename(input_filename)
            assert result == expected, f"Failed for '{input_filename}': got '{result}', expected '{expected}'"
    
    def test_create_format_file_no_trailer(self):
        """Test format file creation without trailer"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.format_location = self.temp_dir
        
        mock_write = AsyncMock()
        
        async def mock_aiofiles_open(path, mode, **kwargs):
            mock_file = AsyncMock()
            mock_file.write = mock_write
            return mock_file
        
        async def run_test():
            with patch('aiofiles.open', side_effect=mock_aiofiles_open):
                with patch('utils.file_handler.datetime') as mock_datetime:
                    mock_datetime.now.return_value.isoformat.return_value = "2024-12-25T16:00:00"
                    
                    await handler._create_format_file(
                        filename="no_trailer.csv",
                        header_delimiter=",",
                        column_delimiter=",",
                        row_delimiter="\\n",
                        text_qualifier='"',
                        skip_lines=0,
                        trailer_line="",  # Empty trailer
                        timestamp="20241225_160000"
                    )
                    
                    # Verify format configuration for no trailer
                    config_json = mock_write.call_args[0][0]
                    config = json.loads(config_json)
                    
                    assert config["csv_format"]["has_trailer"] is False
                    assert config["csv_format"]["trailer_line"] == ""
        
        asyncio.run(run_test())
    
    def test_create_format_file_whitespace_trailer(self):
        """Test format file creation with whitespace-only trailer"""
        with patch('os.makedirs'):
            handler = FileHandler()
            handler.format_location = self.temp_dir
        
        mock_write = AsyncMock()
        
        async def mock_aiofiles_open(path, mode, **kwargs):
            mock_file = AsyncMock()
            mock_file.write = mock_write
            return mock_file
        
        async def run_test():
            with patch('aiofiles.open', side_effect=mock_aiofiles_open):
                with patch('utils.file_handler.datetime') as mock_datetime:
                    mock_datetime.now.return_value.isoformat.return_value = "2024-12-25T17:00:00"
                    
                    await handler._create_format_file(
                        filename="whitespace_trailer.csv",
                        header_delimiter=",",
                        column_delimiter=",",
                        row_delimiter="\\n",
                        text_qualifier='"',
                        skip_lines=0,
                        trailer_line="   \t  ",  # Whitespace-only trailer
                        timestamp="20241225_170000"
                    )
                    
                    # Verify format configuration treats whitespace-only as no trailer
                    config_json = mock_write.call_args[0][0]
                    config = json.loads(config_json)
                    
                    assert config["csv_format"]["has_trailer"] is False