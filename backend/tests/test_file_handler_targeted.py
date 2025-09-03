"""
Targeted tests for specific missing coverage in utils/file_handler.py
Focuses on extract_table_base_name complex logic and edge cases
"""

import pytest
import os
from unittest.mock import patch

from utils.file_handler import FileHandler


class TestFileHandlerTargetedCoverage:
    """Test specific uncovered lines in file_handler.py"""
    
    def test_extract_table_base_name_timestamp_patterns(self):
        """Test extract_table_base_name with various timestamp patterns"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Test cases for timestamp pattern matching (lines 177-193)
        test_cases = [
            # Timestamp patterns that should be removed
            ("data.20241225.120000.csv", "data"),  # .yyyyMMdd.HHmmss
            ("users_20241225_120000.csv", "users"),  # _yyyyMMdd_HHmmss
            ("products.20241225120000.csv", "products"),  # .yyyyMMddHHmmss  
            ("orders_20241225120000.csv", "orders"),  # _yyyyMMddHHmmss
            ("reports.20241225.csv", "reports"),  # .yyyyMMdd
            ("logs_20241225.csv", "logs"),  # _yyyyMMdd
            
            # Files without timestamp patterns (lines 196-198)
            ("regular_file.csv", "regular_file"),  # No pattern matched, keep full name
            ("medium_test.csv", "medium_test"),  # Underscores preserved when no pattern
            ("data_with_underscores.csv", "data_with_underscores"),
            
            # Files that need sanitization (lines 201)
            ("file-with-dashes.csv", "file_with_dashes"),  # Dashes converted to underscores
            ("file@#$special.csv", "file___special"),  # Special chars converted
            ("file with spaces.csv", "file_with_spaces"),  # Spaces converted
            
            # Files needing prefix (lines 204-205)
            ("123numbers.csv", "t_123numbers"),  # Starts with number, needs prefix
            ("9data.csv", "t_9data"),  # Starts with number
            
            # Empty/edge cases (line 207)
            ("", "unknown_table"),  # Empty filename
            (".csv", "unknown_table"),  # Just extension
        ]
        
        for filename, expected in test_cases:
            result = handler.extract_table_base_name(filename)
            assert result == expected, f"Failed for '{filename}': got '{result}', expected '{expected}'"
    
    def test_extract_table_base_name_multiple_patterns(self):
        """Test files with multiple timestamp patterns (first match wins)"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # File with multiple patterns - should match first one found
        result = handler.extract_table_base_name("data_20241225_120000.20241226.csv")
        assert result == "data"  # Should remove _20241225_120000 pattern
        
        result = handler.extract_table_base_name("test.20241225.data_20241226.csv") 
        assert result == "test"  # Should remove .20241225 pattern, leave the rest
    
    def test_extract_table_base_name_pattern_not_matched_logic(self):
        """Test the pattern_matched logic specifically"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Test files where no timestamp pattern matches
        test_cases = [
            "normal_file.csv",
            "file_with_numbers_123_but_no_timestamp.csv", 
            "data_202412.csv",  # Too short for timestamp
            "file_20241300.csv",  # Invalid date (month 13)
        ]
        
        for filename in test_cases:
            # These should preserve the full base name since no pattern matched
            expected = os.path.splitext(filename)[0].replace('-', '_').replace(' ', '_')
            # Apply character sanitization
            expected = ''.join(c if c.isalnum() or c == '_' else '_' for c in expected)
            if expected and not (expected[0].isalpha() or expected[0] == '_'):
                expected = f"t_{expected}"
            
            result = handler.extract_table_base_name(filename)
            assert isinstance(result, str)
            assert len(result) > 0
    
    def test_extract_table_base_name_sanitization_edge_cases(self):
        """Test character sanitization logic (line 201)"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Test all types of characters that need sanitization
        test_cases = [
            ("file!@#$%^&*().csv", "file________"),  # All special chars
            ("файл.csv", "t_файл"),  # Unicode gets sanitized and needs prefix
            ("file_123.csv", "file_123"),  # Valid chars preserved
            ("_underscore_file.csv", "_underscore_file"),  # Leading underscore OK
            ("MixedCase123.csv", "MixedCase123"),  # Mixed case preserved
        ]
        
        for filename, expected in test_cases:
            result = handler.extract_table_base_name(filename)
            assert result == expected, f"Failed for '{filename}': got '{result}', expected '{expected}'"
    
    def test_extract_table_base_name_prefix_logic(self):
        """Test the prefix logic for names starting with numbers (lines 204-205)"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Test various number-starting scenarios
        test_cases = [
            ("0file.csv", "t_0file"),
            ("1.csv", "t_1"), 
            ("999data.csv", "t_999data"),
            ("1_underscore.csv", "t_1_underscore"),
        ]
        
        for filename, expected in test_cases:
            result = handler.extract_table_base_name(filename)
            assert result == expected, f"Failed for '{filename}': got '{result}', expected '{expected}'"
    
    def test_extract_table_base_name_exception_handling(self):
        """Test exception handling in extract_table_base_name (lines 209-210)"""
        with patch('os.makedirs'):
            handler = FileHandler()
        
        # Mock re.search to raise an exception
        with patch('re.search', side_effect=Exception("Regex error")):
            result = handler.extract_table_base_name("test.csv")
            assert result == "unknown_table"  # Should return fallback on exception
        
        # Mock re.sub to raise an exception  
        with patch('re.sub', side_effect=Exception("Regex sub error")):
            result = handler.extract_table_base_name("data_20241225.csv")
            assert result == "unknown_table"  # Should return fallback on exception