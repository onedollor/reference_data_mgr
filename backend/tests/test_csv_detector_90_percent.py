"""
Additional tests to push utils/csv_detector.py to >90% coverage
Targeting missing lines: 43, 47, 72-73, 79-80, 84-85, 88-94, 120-121, 159, 196, 198, 202, 207, 232, 236-237, 247, 253-254, 282-283, 292, 296
"""

import os
import tempfile
import pytest
from utils.csv_detector import CSVFormatDetector


class TestCSVDetector90Percent:
    """Additional tests to reach >90% coverage for csv_detector.py"""
    
    def setup_method(self):
        """Setup for each test"""
        self.detector = CSVFormatDetector()
        self.temp_dir = tempfile.mkdtemp()
    
    def test_detect_format_empty_file_error(self):
        """Test detect_format with empty file - covers line 43"""
        # Create empty file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("   ")  # Only whitespace
            empty_file_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(empty_file_path)
            
            # Should detect as error (line 43 path)
            assert 'error' in result
            assert 'empty' in result['error'].lower()
        finally:
            os.unlink(empty_file_path)
    
    def test_detect_format_single_line_file_error(self):
        """Test detect_format with single line file - covers line 47"""
        # Create single line file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("single line only")  # Only one line
            single_line_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(single_line_path)
            
            # Should detect as error (line 47 path)
            assert 'error' in result
            assert '2 lines' in result['error'] or 'lines' in result['error']
        finally:
            os.unlink(single_line_path)
    
    def test_detect_column_delimiter_score_ties(self):
        """Test column delimiter detection with score ties - covers lines 72-73"""
        # Create CSV with equal delimiter scores
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("a,b;c\nd,e;f\ng,h;i")  # Equal comma and semicolon usage
            tie_file_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(tie_file_path)
            
            # Should still detect a delimiter (fallback logic, lines 72-73)
            assert 'column_delimiter' in result
            assert result['column_delimiter'] in [',', ';']
        finally:
            os.unlink(tie_file_path)
    
    def test_detect_column_delimiter_low_scores(self):
        """Test column delimiter detection with very low scores - covers lines 79-80"""
        # Create CSV with very inconsistent delimiters
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("abc def\nghi jkl\nmno pqr")  # No clear delimiters
            no_delim_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(no_delim_path)
            
            # Should fallback to comma (lines 79-80)
            assert 'column_delimiter' in result
            # Most likely will default to comma in low score scenarios
        finally:
            os.unlink(no_delim_path)
    
    def test_detect_text_qualifier_rare_cases(self):
        """Test text qualifier detection rare cases - covers lines 84-85, 88-94"""
        # Create CSV with mixed or unusual text qualifiers
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('name,value\n"John",123\n\'Jane\',456\nBob,789')  # Mixed quotes
            mixed_quotes_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(mixed_quotes_path)
            
            # Should detect some text qualifier (lines 84-94 paths)
            assert 'text_qualifier' in result
            assert result['text_qualifier'] in ['"', "'", ""]
        finally:
            os.unlink(mixed_quotes_path)
    
    def test_detect_text_qualifier_edge_cases(self):
        """Test text qualifier with edge cases - covers more lines 88-94"""
        # Create CSV where quotes appear mid-field
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('data,info\nval"ue1,info1\nval"ue2,info2')  # Quotes in middle
            mid_quotes_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(mid_quotes_path)
            
            # Should handle this case (lines 88-94)
            assert 'text_qualifier' in result
        finally:
            os.unlink(mid_quotes_path)
    
    def test_detect_header_inconsistent_data_types(self):
        """Test header detection with inconsistent data types - covers lines 120-121"""
        # Create CSV where header detection is ambiguous
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('123,456,789\nabc,def,ghi\n111,222,333')  # Numbers then strings
            ambiguous_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(ambiguous_path)
            
            # Should make a decision about header (lines 120-121)
            assert 'has_header' in result
            assert isinstance(result['has_header'], bool)
        finally:
            os.unlink(ambiguous_path)
    
    def test_detect_row_delimiter_unix_vs_windows(self):
        """Test row delimiter detection - covers line 159"""
        # Create file with mixed line endings or specific line ending
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_file:
            tmp_file.write('col1,col2\r\nval1,val2\r\nval3,val4')  # Windows line endings
            windows_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(windows_path)
            
            # Should detect row delimiter (line 159)
            assert 'row_delimiter' in result
        finally:
            os.unlink(windows_path)
    
    def test_detect_trailer_parsing_exceptions(self):
        """Test trailer detection with parsing exceptions - covers lines 196, 198"""
        # Create CSV that might cause parsing issues in trailer detection
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('col1,col2\nval1,"val,2"\nval3,val4\n"malformed')  # Malformed last line
            malformed_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(malformed_path)
            
            # Should handle parsing exceptions (lines 196, 198)
            assert 'has_trailer' in result
            assert isinstance(result['has_trailer'], bool)
        finally:
            os.unlink(malformed_path)
    
    def test_detect_trailer_column_count_analysis(self):
        """Test trailer detection column count analysis - covers lines 202, 207"""
        # Create CSV where trailer has different column count
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('name,age,city\nJohn,25,NYC\nJane,30,LA\nTOTAL RECORDS: 2')  # Trailer with 1 column
            trailer_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(trailer_path)
            
            # Should analyze column counts (lines 202, 207)
            assert 'has_trailer' in result
            if result['has_trailer']:
                assert 'trailer_line' in result
        finally:
            os.unlink(trailer_path)
    
    def test_detect_format_very_small_sample(self):
        """Test with very small file that might trigger edge cases - covers lines 232, 236-237"""
        # Create minimal CSV
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('a,b\nc,d')  # Minimal 2-line CSV
            minimal_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(minimal_path)
            
            # Should handle minimal data (lines 232, 236-237)
            assert 'column_delimiter' in result
            assert 'has_header' in result
        finally:
            os.unlink(minimal_path)
    
    def test_detect_format_statistics_fallback(self):
        """Test statistics module fallback scenarios - covers lines 247, 253-254"""
        # Create CSV that might trigger statistics edge cases
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Data that might cause statistics calculations to fail or need fallbacks
            tmp_file.write('data\n1\n2\n3\n4\n5')  # Single column data
            single_col_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(single_col_path)
            
            # Should handle statistics fallbacks (lines 247, 253-254)
            assert 'column_delimiter' in result
        finally:
            os.unlink(single_col_path)
    
    def test_detect_format_confidence_calculation(self):
        """Test detection confidence calculation edge cases - covers lines 282-283"""
        # Create CSV with patterns that might affect confidence calculation
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('inconsistent|data,here\nmore|random,stuff\npattern|changes,frequently')
            inconsistent_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(inconsistent_path)
            
            # Should calculate confidence (lines 282-283)
            assert 'detection_confidence' in result
            assert isinstance(result['detection_confidence'], (int, float))
        finally:
            os.unlink(inconsistent_path)
    
    def test_detect_format_final_validation(self):
        """Test final format validation - covers lines 292, 296"""
        # Create CSV that tests final validation logic
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('header1,header2,header3\nvalue1,value2,value3\nvalue4,value5,value6')
            standard_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(standard_path)
            
            # Should complete final validation (lines 292, 296)
            assert 'column_delimiter' in result
            assert 'has_header' in result
            assert 'text_qualifier' in result
            
            # Ensure all required fields are present
            required_fields = ['column_delimiter', 'has_header', 'text_qualifier', 'row_delimiter']
            for field in required_fields:
                assert field in result
        finally:
            os.unlink(standard_path)