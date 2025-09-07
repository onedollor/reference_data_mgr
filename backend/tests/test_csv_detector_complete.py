"""
Complete test coverage for utils/csv_detector.py to achieve 85%+ coverage
Focuses on missing coverage areas: exception handling, edge cases, and specific detection logic
"""

import pytest
import os
import tempfile
from unittest.mock import patch, mock_open
from collections import Counter

from utils.csv_detector import CSVFormatDetector


class TestCSVDetectorExceptionHandling:
    """Test exception handling and edge cases"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = CSVFormatDetector()
        
    def test_detect_format_empty_file(self):
        """Test detection with empty file (line 43)"""
        # Create empty file
        empty_file = os.path.join(self.temp_dir, "empty.csv")
        with open(empty_file, 'w') as f:
            f.write("")  # Completely empty
        
        result = self.detector.detect_format(empty_file)
        
        assert 'error' in result
        assert 'empty' in result['error'].lower()
    
    def test_detect_format_whitespace_only_file(self):
        """Test detection with whitespace-only file (line 43)"""
        # Create file with only whitespace
        whitespace_file = os.path.join(self.temp_dir, "whitespace.csv")
        with open(whitespace_file, 'w') as f:
            f.write("   \t  \n  \n  \t  ")  # Only whitespace
        
        result = self.detector.detect_format(whitespace_file)
        
        assert 'error' in result
        assert 'empty' in result['error'].lower()
    
    def test_detect_format_single_line_file(self):
        """Test detection with single line file (line 47)"""
        # Create file with only one line
        single_line_file = os.path.join(self.temp_dir, "single.csv")
        with open(single_line_file, 'w') as f:
            f.write("col1,col2,col3")  # Only one line
        
        result = self.detector.detect_format(single_line_file)
        
        assert 'error' in result
        assert 'at least 2 lines' in result['error'].lower()
    
    def test_detect_format_file_not_found(self):
        """Test detection with non-existent file"""
        result = self.detector.detect_format("/nonexistent/file.csv")
        
        assert 'error' in result
    
    def test_detect_format_statistics_module_fallback(self):
        """Test statistics.mode exception fallback (lines 79-80)"""
        # Create a file that will trigger the trailer detection logic
        test_file = os.path.join(self.temp_dir, "trailer_test.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2,col3\n")
            f.write("val1,val2,val3\n")
            f.write("val4,val5,val6\n")
            f.write("TOTAL,100\n")  # Different column count - potential trailer
        
        # Mock statistics.mode to raise an exception, forcing Counter fallback
        with patch('statistics.mode', side_effect=Exception("Statistics error")):
            result = self.detector.detect_format(test_file)
            
            # Should not error - should use Counter fallback
            assert 'error' not in result
            assert 'column_delimiter' in result
    
    def test_detect_format_parse_row_exceptions(self):
        """Test parse_row exception handling (lines 72-73, 84-85)"""
        test_file = os.path.join(self.temp_dir, "malformed.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2,col3\n")
            f.write('val1,val2,"unclosed quote\n')  # Malformed row
            f.write("val3,val4,val5\n")
            f.write("TOTAL,sum\n")
        
        # The detector should handle parsing errors gracefully
        result = self.detector.detect_format(test_file)
        
        assert 'error' not in result  # Should not crash
        assert 'column_delimiter' in result
    
    def test_detect_format_full_file_read_exception(self):
        """Test full file reading exception handling (lines 91-94)"""
        test_file = os.path.join(self.temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("col1,col2\n")
            f.write("val1,val2\n")
            f.write("TRAILER\n")
        
        # Mock the full file read to raise an exception
        original_open = open
        def mock_open_side_effect(file, *args, **kwargs):
            if 'f_full' in str(locals()) or len(args) > 0:
                raise PermissionError("Access denied")
            return original_open(file, *args, **kwargs)
        
        with patch('builtins.open', side_effect=mock_open_side_effect):
            result = self.detector.detect_format(test_file)
            
            # Should fallback to sample-based trailer detection
            assert 'error' not in result
            assert 'has_trailer' in result


class TestCSVDetectorTextQualifierEdgeCases:
    """Test text qualifier detection edge cases"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = CSVFormatDetector()
    
    def test_detect_text_qualifier_double_quotes_win(self):
        """Test double quotes winning over single quotes (line 196)"""
        content = '''name,description,value
"John Doe","He said 'hello'",100
"Jane Smith","She's nice",200'''
        
        result = self.detector._detect_text_qualifier(content, ',')
        
        assert result == '"'  # Double quotes should win
    
    def test_detect_text_qualifier_single_quotes_only(self):
        """Test single quotes when they're the only option (line 198)"""
        content = '''name,description,value
'John Doe','Simple text',100
'Jane Smith','Another text',200'''
        
        result = self.detector._detect_text_qualifier(content, ',')
        
        assert result == "'"  # Single quotes should be detected
    
    def test_detect_text_qualifier_delimiter_in_fields(self):
        """Test default to double quotes when fields contain delimiters (line 202)"""
        content = '''name,description
Company Inc.,Tech, Software
Another Corp.,Sales, Marketing'''
        
        # Fields contain commas, so should default to double quotes
        result = self.detector._detect_text_qualifier(content, ',')
        
        assert result == '"'
    
    def test_detect_text_qualifier_no_qualifier_needed(self):
        """Test no text qualifier when none needed (line 202)"""
        content = '''name,age,city
John,25,NYC
Jane,30,LA'''
        
        result = self.detector._detect_text_qualifier(content, ',')
        
        assert result == ''  # No qualifier needed


class TestCSVDetectorHeaderDetection:
    """Test header detection edge cases"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = CSVFormatDetector()
    
    def test_detect_header_single_line(self):
        """Test header detection with single line (line 207)"""
        lines = ["col1,col2,col3"]
        
        result = self.detector._detect_header(lines, ',', '')
        
        assert result is True  # Should assume header with single line
    
    def test_detect_header_common_patterns(self):
        """Test header detection with common header patterns (lines 227-232)"""
        # Test with many header indicators
        lines = [
            "id,name,date,time,description,amount",  # Many header indicators
            "1,John,2024-01-01,10:00,Test,100.00"
        ]
        
        result = self.detector._detect_header(lines, ',', '')
        
        assert result is True  # Should detect as header due to common patterns
    
    def test_detect_header_few_patterns(self):
        """Test header detection with few header patterns (line 234)"""
        # Test with few header indicators
        lines = [
            "col_a,col_b,col_c,col_d,col_e",  # No common header patterns
            "val1,val2,val3,val4,val5"
        ]
        
        result = self.detector._detect_header(lines, ',', '')
        
        assert result is False  # Should not detect as header
    
    def test_detect_header_parse_exception(self):
        """Test header detection with parsing exception (line 237)"""
        lines = [
            'col1,col2,"unclosed',  # Malformed header
            "val1,val2,val3"
        ]
        
        result = self.detector._detect_header(lines, ',', '"')
        
        assert result is True  # Should default to True on parse errors


class TestCSVDetectorTrailerDetection:
    """Test trailer detection edge cases"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = CSVFormatDetector()
    
    def test_detect_trailer_insufficient_lines(self):
        """Test trailer detection with insufficient lines (lines 246-247)"""
        lines = ["col1,col2"]  # Only one line
        
        result = self.detector._detect_trailer(lines, ',', '')
        
        has_trailer, trailer_line = result
        assert has_trailer is False
        assert trailer_line is None
    
    def test_detect_trailer_empty_lines_filtered(self):
        """Test trailer detection filters empty lines (line 245)"""
        lines = [
            "col1,col2,col3",
            "val1,val2,val3",
            "",  # Empty line
            "  ",  # Whitespace line
            "TOTAL,100"  # Actual last line
        ]
        
        result = self.detector._detect_trailer(lines, ',', '')
        
        has_trailer, trailer_line = result
        # Should compare "TOTAL,100" (2 cols) vs "val1,val2,val3" (3 cols)
        assert has_trailer is True
        assert trailer_line == "TOTAL,100"
    
    def test_detect_trailer_parse_exceptions(self):
        """Test trailer detection with parsing exceptions (lines 271-272, 282-283)"""
        lines = [
            "col1,col2,col3",
            "val1,val2,val3",
            '"unclosed,quote'  # Malformed last line
        ]
        
        result = self.detector._detect_trailer(lines, ',', '"')
        
        has_trailer, trailer_line = result
        # Should handle parsing errors gracefully
        assert has_trailer is False  # Can't parse, so assume no trailer
    
    def test_detect_trailer_column_count_match(self):
        """Test trailer when column counts match (line 292)"""
        lines = [
            "col1,col2,col3",
            "val1,val2,val3",
            "val4,val5,val6"  # Same column count as previous
        ]
        
        result = self.detector._detect_trailer(lines, ',', '')
        
        has_trailer, trailer_line = result
        assert has_trailer is False  # Same column count, no trailer
    
    def test_detect_trailer_column_count_differs(self):
        """Test trailer when column counts differ (line 296)"""
        lines = [
            "col1,col2,col3",
            "val1,val2,val3",
            "TOTAL,150"  # Different column count
        ]
        
        result = self.detector._detect_trailer(lines, ',', '')
        
        has_trailer, trailer_line = result
        assert has_trailer is True
        assert trailer_line == "TOTAL,150"


class TestCSVDetectorIntegrationEdgeCases:
    """Test integration edge cases and complex scenarios"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.detector = CSVFormatDetector()
    
    def test_detect_format_complex_trailer_scenario(self):
        """Test complex trailer detection with full file analysis"""
        # Create a file that will trigger the complex trailer detection logic
        test_file = os.path.join(self.temp_dir, "complex.csv")
        with open(test_file, 'w') as f:
            f.write("id,name,amount\n")
            for i in range(10):  # Multiple data rows to establish pattern
                f.write(f"{i},Item{i},{i*10}\n")
            f.write("TOTAL,Sum,500\n")  # Same column count but different content
            f.write("END\n")  # Different column count - trailer
        
        result = self.detector.detect_format(test_file)
        
        assert 'error' not in result
        assert result['has_trailer'] is True
        assert result['trailer_line'] == 'END'
    
    def test_detect_format_no_trailer_same_pattern(self):
        """Test no trailer detection when last line matches pattern"""
        test_file = os.path.join(self.temp_dir, "no_trailer.csv")
        with open(test_file, 'w') as f:
            f.write("id,name,amount\n")
            f.write("1,Item1,100\n")
            f.write("2,Item2,200\n")
            f.write("3,Item3,300\n")  # Same pattern as others
        
        result = self.detector.detect_format(test_file)
        
        assert 'error' not in result
        assert result['has_trailer'] is False
    
    def test_detect_format_with_encoding_issues(self):
        """Test detection with encoding issues that are ignored"""
        test_file = os.path.join(self.temp_dir, "encoding.csv")
        # Write with some non-UTF8 bytes that should be ignored
        with open(test_file, 'wb') as f:
            f.write(b"col1,col2,col3\n")
            f.write(b"val1,val2,val3\n")
            f.write(b"val\xff\xfe,test,val3\n")  # Invalid UTF-8 bytes
        
        result = self.detector.detect_format(test_file)
        
        # Should handle encoding errors gracefully due to errors='ignore'
        assert 'error' not in result
        assert result['column_delimiter'] == ','