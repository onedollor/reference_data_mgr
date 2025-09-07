"""
Additional async tests to push utils/file_handler.py to >90% coverage
Targeting missing lines: 50-76, 91-120, 136-141
"""

import os
import tempfile
import pytest
import json
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch, mock_open
from utils.file_handler import FileHandler


class MockUploadFile:
    """Mock FastAPI UploadFile for testing"""
    
    def __init__(self, filename: str, content: bytes):
        self.filename = filename
        self.content = content
    
    async def read(self):
        return self.content


class TestFileHandlerAsyncMethods:
    """Tests for async methods to reach >90% coverage"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Mock environment variables for temp locations
        with patch.dict('os.environ', {
            'temp_location': self.temp_dir,
            'archive_location': os.path.join(self.temp_dir, 'archive'),
            'format_location': os.path.join(self.temp_dir, 'format')
        }):
            self.handler = FileHandler()
    
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_success(self):
        """Test save_uploaded_file async method success - covers lines 50-76"""
        # Create mock uploaded file
        mock_file = MockUploadFile("test_data.csv", b"col1,col2\nval1,val2")
        
        # Test the async method
        temp_file_path, fmt_file_path = await self.handler.save_uploaded_file(
            file=mock_file,
            header_delimiter=",",
            column_delimiter=",",
            row_delimiter="\n",
            text_qualifier='"',
            skip_lines=0,
            trailer_line=None
        )
        
        # Verify file paths are generated correctly (lines 51-54)
        assert os.path.basename(temp_file_path).endswith("_test_data.csv")
        assert temp_file_path.startswith(self.temp_dir)
        
        # Verify files were created (lines 57-59, 62-71)
        assert os.path.exists(temp_file_path)
        assert os.path.exists(fmt_file_path)
        
        # Verify file content was written correctly (lines 57-59)
        with open(temp_file_path, 'rb') as f:
            assert f.read() == b"col1,col2\nval1,val2"
        
        # Verify format file was created (lines 62-71)
        assert fmt_file_path.endswith('.fmt')
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_with_trailer(self):
        """Test save_uploaded_file with trailer - covers lines 50-76"""
        mock_file = MockUploadFile("data_with_trailer.csv", b"name,age\nJohn,25\nTotal: 1")
        
        temp_file_path, fmt_file_path = await self.handler.save_uploaded_file(
            file=mock_file,
            header_delimiter=",",
            column_delimiter=",", 
            row_delimiter="\n",
            text_qualifier='"',
            skip_lines=1,
            trailer_line="Total: 1"
        )
        
        # Verify both files created successfully (lines 50-73)
        assert os.path.exists(temp_file_path)
        assert os.path.exists(fmt_file_path)
    
    @pytest.mark.asyncio
    async def test_save_uploaded_file_exception_handling(self):
        """Test save_uploaded_file exception handling - covers lines 75-76"""
        # Create mock file that will cause an exception
        mock_file = MagicMock()
        mock_file.filename = "error_file.csv"
        mock_file.read = AsyncMock(side_effect=Exception("File read error"))
        
        # Test exception handling
        with pytest.raises(Exception) as exc_info:
            await self.handler.save_uploaded_file(
                file=mock_file,
                header_delimiter=",",
                column_delimiter=",",
                row_delimiter="\n", 
                text_qualifier='"'
            )
        
        # Verify exception handling (lines 75-76)
        assert "Failed to save uploaded file" in str(exc_info.value)
        assert "File read error" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_create_format_file_comprehensive(self):
        """Test _create_format_file method comprehensively - covers lines 91-120"""
        # Call the async format file creation method directly
        fmt_file_path = await self.handler._create_format_file(
            filename="test_data.csv",
            header_delimiter=";",
            column_delimiter=";",
            row_delimiter="\r\n",
            text_qualifier="'",
            skip_lines=2,
            trailer_line="END OF DATA",
            timestamp="20240101_120000"
        )
        
        # Verify format file path generation (lines 91-92)
        assert fmt_file_path.endswith("20240101_120000_test_data.fmt")
        assert os.path.exists(fmt_file_path)
        
        # Read and verify format configuration (lines 94-118)
        with open(fmt_file_path, 'r', encoding='utf-8') as f:
            format_config = json.load(f)
        
        # Verify file_info section (lines 95-99)
        file_info = format_config['file_info']
        assert file_info['original_filename'] == "test_data.csv"
        assert 'upload_timestamp' in file_info
        assert file_info['format_version'] == "1.0"
        
        # Verify csv_format section (lines 100-109)
        csv_format = format_config['csv_format']
        assert csv_format['header_delimiter'] == ";"
        assert csv_format['column_delimiter'] == ";"
        assert csv_format['row_delimiter'] == "\r\n"
        assert csv_format['text_qualifier'] == "'"
        assert csv_format['skip_lines'] == 2
        assert csv_format['has_header'] is True  # Always true per line 106
        assert csv_format['has_trailer'] is True  # trailer_line provided
        assert csv_format['trailer_line'] == "END OF DATA"
        
        # Verify processing_options section (lines 110-115)
        processing_opts = format_config['processing_options']
        assert processing_opts['encoding'] == "utf-8"
        assert processing_opts['skip_blank_lines'] is True
        assert processing_opts['strip_whitespace'] is True
    
    @pytest.mark.asyncio
    async def test_create_format_file_no_trailer(self):
        """Test _create_format_file without trailer - covers lines 91-120"""
        fmt_file_path = await self.handler._create_format_file(
            filename="no_trailer.csv",
            header_delimiter="|",
            column_delimiter="|",
            row_delimiter="\n",
            text_qualifier="",
            skip_lines=0,
            trailer_line=None,  # No trailer
            timestamp="20240101_130000"
        )
        
        # Verify format configuration
        with open(fmt_file_path, 'r', encoding='utf-8') as f:
            format_config = json.load(f)
        
        # Verify no trailer configuration (lines 107-108)
        csv_format = format_config['csv_format']
        assert csv_format['has_trailer'] is False  # No trailer line
        assert csv_format['trailer_line'] is None
    
    @pytest.mark.asyncio
    async def test_create_format_file_empty_trailer(self):
        """Test _create_format_file with empty trailer - covers line 107"""
        fmt_file_path = await self.handler._create_format_file(
            filename="empty_trailer.csv",
            header_delimiter=",",
            column_delimiter=",",
            row_delimiter="\n",
            text_qualifier='"',
            skip_lines=0,
            trailer_line="   ",  # Whitespace only trailer
            timestamp="20240101_140000"
        )
        
        with open(fmt_file_path, 'r', encoding='utf-8') as f:
            format_config = json.load(f)
        
        # Verify empty trailer handling (line 107: bool(trailer_line and trailer_line.strip()))
        csv_format = format_config['csv_format']
        assert csv_format['has_trailer'] is False  # Whitespace-only trailer should be False
    
    @pytest.mark.asyncio
    async def test_read_format_file_success(self):
        """Test read_format_file async method success - covers lines 136-141"""
        # Create a test format file first
        test_config = {
            "file_info": {"original_filename": "test.csv", "format_version": "1.0"},
            "csv_format": {"column_delimiter": ",", "has_header": True},
            "processing_options": {"encoding": "utf-8"}
        }
        
        fmt_file_path = os.path.join(self.temp_dir, "test_format.fmt")
        with open(fmt_file_path, 'w', encoding='utf-8') as f:
            json.dump(test_config, f)
        
        # Test reading the format file (lines 137-139)
        result = await self.handler.read_format_file(fmt_file_path)
        
        # Verify successful reading and parsing (lines 137-139)
        assert result == test_config
        assert result['file_info']['original_filename'] == "test.csv"
        assert result['csv_format']['column_delimiter'] == ","
        assert result['processing_options']['encoding'] == "utf-8"
    
    @pytest.mark.asyncio
    async def test_read_format_file_nonexistent_file(self):
        """Test read_format_file with nonexistent file - covers lines 140-141"""
        nonexistent_path = os.path.join(self.temp_dir, "nonexistent.fmt")
        
        # Test exception handling for nonexistent file (lines 140-141)
        with pytest.raises(Exception) as exc_info:
            await self.handler.read_format_file(nonexistent_path)
        
        # Verify exception handling (lines 140-141)
        assert "Failed to read format file" in str(exc_info.value)
        assert nonexistent_path in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_read_format_file_malformed_json(self):
        """Test read_format_file with malformed JSON - covers lines 140-141"""
        # Create a format file with malformed JSON
        malformed_path = os.path.join(self.temp_dir, "malformed.fmt")
        with open(malformed_path, 'w', encoding='utf-8') as f:
            f.write("{ invalid json content }")
        
        # Test exception handling for malformed JSON (lines 140-141)
        with pytest.raises(Exception) as exc_info:
            await self.handler.read_format_file(malformed_path)
        
        # Verify exception handling (lines 140-141)
        assert "Failed to read format file" in str(exc_info.value)
    
    @pytest.mark.asyncio
    async def test_filename_sanitization_in_async_methods(self):
        """Test filename sanitization within async methods - covers line 53"""
        # Test with unsafe filename
        mock_file = MockUploadFile("unsafe<>file|name?.csv", b"data")
        
        temp_file_path, fmt_file_path = await self.handler.save_uploaded_file(
            file=mock_file,
            header_delimiter=",",
            column_delimiter=",",
            row_delimiter="\n",
            text_qualifier='"'
        )
        
        # Verify filename sanitization (line 53)
        assert "<" not in temp_file_path
        assert ">" not in temp_file_path
        assert "|" not in temp_file_path
        assert "?" not in temp_file_path
        assert "_" in os.path.basename(temp_file_path)  # Unsafe chars replaced with underscore
    
    @pytest.mark.asyncio 
    async def test_timestamp_generation_in_async_methods(self):
        """Test timestamp generation in async methods - covers line 52"""
        mock_file = MockUploadFile("timestamped.csv", b"data")
        
        temp_file_path, fmt_file_path = await self.handler.save_uploaded_file(
            file=mock_file,
            header_delimiter=",",
            column_delimiter=",",
            row_delimiter="\n", 
            text_qualifier='"'
        )
        
        # Verify timestamp is included in filename (line 52)
        filename = os.path.basename(temp_file_path)
        # Should match pattern: YYYYMMDD_HHMMSS_filename
        assert len(filename.split('_')[0]) == 8  # YYYYMMDD
        assert len(filename.split('_')[1]) == 6  # HHMMSS
        assert filename.endswith("_timestamped.csv")