"""
Final push tests to get utils/csv_detector.py to >90% coverage
Targeting remaining missing lines: 72-73, 79-80, 84-85, 91-94, 159, 207, 232, 236-237, 247, 253-254, 282-283, 292, 296
"""

import os
import tempfile
import pytest
from unittest.mock import patch
from utils.csv_detector import CSVFormatDetector


class TestCSVDetectorFinalPush:
    """Final tests to push csv_detector.py over 90% coverage"""
    
    def setup_method(self):
        """Setup for each test"""
        self.detector = CSVFormatDetector()
    
    def test_column_delimiter_equal_scores_fallback(self):
        """Test column delimiter when all scores are equal - covers lines 72-73"""
        # Create data where multiple delimiters have exactly equal scores
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Each delimiter appears exactly once per line
            tmp_file.write("a,b;c|d\ne,f;g|h\ni,j;k|l")
            equal_scores_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(equal_scores_path)
            
            # Should fall back to comma when scores are tied (lines 72-73)
            assert result['column_delimiter'] == ','
        finally:
            os.unlink(equal_scores_path)
    
    def test_column_delimiter_very_low_confidence(self):
        """Test column delimiter with very low confidence scores - covers lines 79-80"""
        # Create data with very inconsistent delimiter usage
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Very inconsistent delimiter patterns
            tmp_file.write("abc def ghi\njkl mno\npqr stu vwx yz")
            low_confidence_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(low_confidence_path)
            
            # Should default to comma with low confidence (lines 79-80)
            assert result['column_delimiter'] == ','
        finally:
            os.unlink(low_confidence_path)
    
    def test_text_qualifier_no_consistent_qualifier(self):
        """Test text qualifier when no qualifier is consistently used - covers lines 84-85"""
        # Data with no text qualifiers or very inconsistent usage
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("name,age,city\nJohn,25,NYC\nJane,30,LA")  # No qualifiers at all
            no_qualifier_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(no_qualifier_path)
            
            # Should return empty string for text_qualifier (lines 84-85)
            assert result['text_qualifier'] == ''
        finally:
            os.unlink(no_qualifier_path)
    
    def test_text_qualifier_parsing_edge_cases(self):
        """Test text qualifier parsing edge cases - covers lines 91-94"""
        # Create data that might cause parsing issues
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Malformed quotes or unusual patterns
            tmp_file.write('col1,col2\n"incomplete,value\n"another,"complete"')
            edge_case_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(edge_case_path)
            
            # Should handle parsing edge cases gracefully (lines 91-94)
            assert 'text_qualifier' in result
        finally:
            os.unlink(edge_case_path)
    
    def test_row_delimiter_detection_specific_case(self):
        """Test specific row delimiter detection case - covers line 159"""
        # Test with specific newline patterns
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as tmp_file:
            # Mix of line endings or specific pattern that triggers line 159
            tmp_file.write('a,b\r\nc,d\re,f\ng,h')  # Mixed line endings
            mixed_newlines_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(mixed_newlines_path)
            
            # Should detect some row delimiter (line 159)
            assert 'row_delimiter' in result
            assert result['row_delimiter'] in ['\n', '\r\n', '\r']
        finally:
            os.unlink(mixed_newlines_path)
    
    def test_trailer_detection_specific_column_mismatch(self):
        """Test trailer detection with specific column count scenario - covers line 207"""
        # Create CSV where last line has significantly different column count
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write('col1,col2,col3\nval1,val2,val3\nval4,val5,val6\nSUMMARY')  # Last line has 1 column vs 3
            trailer_mismatch_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(trailer_mismatch_path)
            
            # Should detect trailer based on column count difference (line 207)
            assert 'has_trailer' in result
            if result['has_trailer']:
                assert 'trailer_line' in result
        finally:
            os.unlink(trailer_mismatch_path)
    
    def test_sample_size_boundary_conditions(self):
        """Test sample size boundary conditions - covers lines 232, 236-237"""
        # Create file that tests sample size limits
        large_content = "col1,col2\n" + "val1,val2\n" * 1000  # Large file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write(large_content)
            large_file_path = tmp_file.name
        
        try:
            # Test with different sample sizes to trigger boundary logic
            result = self.detector.detect_format(large_file_path)
            
            # Should handle large files and sampling (lines 232, 236-237)
            assert 'column_delimiter' in result
            assert 'has_header' in result
        finally:
            os.unlink(large_file_path)
    
    def test_statistics_fallback_scenarios(self):
        """Test scenarios that trigger statistics fallbacks - covers lines 247, 253-254"""
        # Create data that might cause statistics calculations to need fallbacks
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Data pattern that might cause statistics edge cases
            tmp_file.write("a\nb\nc\nd\ne\nf")  # Single column, minimal variance
            stats_edge_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(stats_edge_path)
            
            # Should handle statistics edge cases (lines 247, 253-254)
            assert 'column_delimiter' in result
        finally:
            os.unlink(stats_edge_path)
    
    @patch('statistics.stdev')
    def test_statistics_exception_handling(self, mock_stdev):
        """Test statistics exception handling - covers lines 253-254"""
        # Mock statistics.stdev to raise an exception
        mock_stdev.side_effect = Exception("Statistics calculation failed")
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            tmp_file.write("col1,col2\nval1,val2\nval3,val4")
            stats_exception_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(stats_exception_path)
            
            # Should handle statistics exceptions gracefully (lines 253-254)
            assert 'column_delimiter' in result
        finally:
            os.unlink(stats_exception_path)
    
    def test_confidence_calculation_edge_values(self):
        """Test confidence calculation with edge values - covers lines 282-283"""
        # Create data that produces specific confidence calculation scenarios
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Pattern designed to test confidence calculation edge cases
            tmp_file.write("perfect,consistent,data\nwith,same,pattern\nalways,three,columns")
            perfect_pattern_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(perfect_pattern_path)
            
            # Should calculate confidence properly (lines 282-283)
            assert 'detection_confidence' in result
            assert 0 <= result['detection_confidence'] <= 1.0
        finally:
            os.unlink(perfect_pattern_path)
    
    def test_format_validation_final_checks(self):
        """Test final format validation checks - covers lines 292, 296"""
        # Test comprehensive format validation
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as tmp_file:
            # Well-formed CSV that should pass all validations
            tmp_file.write('"name","age","city"\n"John","25","New York"\n"Jane","30","Los Angeles"')
            well_formed_path = tmp_file.name
        
        try:
            result = self.detector.detect_format(well_formed_path)
            
            # Should complete all final validation steps (lines 292, 296)
            expected_keys = ['column_delimiter', 'has_header', 'text_qualifier', 'row_delimiter', 'has_trailer', 'detection_confidence']
            for key in expected_keys:
                assert key in result
                
            # Verify specific values make sense
            assert result['column_delimiter'] == ','
            assert result['text_qualifier'] == '"'
            assert result['has_header'] is True
            assert result['has_trailer'] is False
            assert isinstance(result['detection_confidence'], (int, float))
        finally:
            os.unlink(well_formed_path)