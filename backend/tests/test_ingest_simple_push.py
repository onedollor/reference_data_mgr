"""
Simple targeted tests to push utils/ingest.py coverage focusing on key missing lines
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from datetime import datetime

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterSimplePush:
    """Simple targeted tests for missing lines"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        # Make logger async methods
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    def test_init_environment_variables(self):
        """Test initialization with environment variables - covers lines 31-32, 37-38"""
        with patch.dict('os.environ', {
            'INGEST_PROGRESS_INTERVAL': 'invalid',  # Should trigger ValueError line 31-32
            'INGEST_TYPE_SAMPLE_ROWS': 'invalid'     # Should trigger ValueError line 37-38
        }):
            ingester = DataIngester(self.mock_db, self.mock_logger)
            # Should use default values when parsing fails
            assert ingester.progress_batch_interval == 5
            assert ingester.type_sample_rows == 5000
    
    def test_sanitize_headers_comprehensive(self):
        """Test header sanitization with various edge cases"""
        test_cases = [
            # (input_headers, expected_behavior)
            (["123start", "with space", "with-dash", "", None], "edge_cases"),
            (["normal_header", "UPPER", "lower", "Mixed_Case"], "normal_cases"),
            (["special!@#", "with.dot", "with/slash"], "special_chars")
        ]
        
        for headers, case_type in test_cases:
            result = self.ingester._sanitize_headers(headers)
            assert len(result) == len(headers), f"Length mismatch for {case_type}"
            # Should handle all cases without throwing errors
            assert isinstance(result, list)
    
    def test_deduplicate_headers_edge_cases(self):
        """Test header deduplication with edge cases"""
        # Test with many duplicates to trigger different paths
        headers = ["col"] * 10  # 10 identical headers
        result = self.ingester._deduplicate_headers(headers)
        
        # Should make them all unique
        assert len(set(result)) == len(result)
        assert len(result) == 10
    
    def test_infer_types_with_various_data(self):
        """Test type inference with various data patterns"""
        # Test with different data patterns
        df_cases = [
            # Case 1: Mixed numeric and text
            pd.DataFrame({
                'col1': ['1', '2', 'text', '4', '5'],
                'col2': ['1.1', '2.2', '3.3', '4.4', '5.5']
            }),
            # Case 2: Date-like strings  
            pd.DataFrame({
                'date_col': ['2023-01-01', '2023-01-02', 'invalid', '2023-01-04', '2023-01-05']
            }),
            # Case 3: Empty/null values
            pd.DataFrame({
                'sparse_col': ['', None, '3', '', '5']
            })
        ]
        
        for i, df in enumerate(df_cases):
            result = self.ingester._infer_types(df, list(df.columns))
            assert isinstance(result, dict), f"Case {i+1} failed"
            assert len(result) == len(df.columns), f"Case {i+1} column count mismatch"
    
    def test_persist_inferred_schema_edge_cases(self):
        """Test schema persistence with edge cases"""
        fmt_file_path = os.path.join(self.temp_dir, 'test.fmt')
        
        # Case 1: Valid file with existing config
        initial_config = {"csv_format": {"delimiter": ","}, "existing_key": "value"}
        
        with patch('builtins.open', mock_open(read_data='{"csv_format": {"delimiter": ","}}')) as mock_file, \
             patch('json.load', return_value=initial_config), \
             patch('json.dump') as mock_json_dump:
            
            inferred_types = {'col1': 'varchar(100)', 'col2': 'int'}
            
            # Should not raise exception
            self.ingester._persist_inferred_schema(fmt_file_path, inferred_types)
            mock_json_dump.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_read_csv_file_basic_functionality(self):
        """Test basic CSV reading without complex mocking"""
        file_path = os.path.join(self.temp_dir, 'simple.csv')
        
        # Create simple CSV file
        with open(file_path, 'w') as f:
            f.write('col1,col2,col3\nval1,val2,val3\nval4,val5,val6\n')
        
        csv_format = {
            'delimiter': ',',
            'has_header': True,
            'has_trailer': False
        }
        
        # Read the file
        result = await self.ingester._read_csv_file(file_path, csv_format)
        
        # Verify results
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ['col1', 'col2', 'col3']
    
    @pytest.mark.asyncio
    async def test_read_csv_file_with_trailer(self):
        """Test CSV reading with trailer removal"""
        file_path = os.path.join(self.temp_dir, 'with_trailer.csv')
        
        # Create CSV with trailer
        with open(file_path, 'w') as f:
            f.write('col1,col2\ndata1,data2\ndata3,data4\nTRAILER,END\n')
        
        csv_format = {
            'delimiter': ',',
            'has_header': True,
            'has_trailer': True
        }
        
        # Read the file
        result = await self.ingester._read_csv_file(file_path, csv_format)
        
        # Should exclude trailer row
        assert len(result) == 2  # 3 data rows - 1 trailer = 2 rows
        assert 'TRAILER' not in result['col1'].values
    
    def test_sql_escape_string_all_cases(self):
        """Test SQL string escaping comprehensively"""
        test_cases = [
            ("normal_string", "normal_string"),
            ("string'with'quotes", "string''with''quotes"),
            ("string\nwith\nnewlines", "string\\nwith\\nnewlines"),
            ("string\twith\ttabs", "string\\twith\\ttabs"),
            ("string\\with\\backslashes", "string\\\\with\\\\backslashes"),
            ("", ""),  # Empty string
            (None, ""),  # None handling
            ("mixed'content\nwith\ttabs\\and\"quotes", "mixed''content\\nwith\\ttabs\\\\and\"\"quotes")
        ]
        
        for input_val, expected in test_cases:
            result = self.ingester._sql_escape_string(input_val)
            assert result == expected, f"Failed for input: '{input_val}'"
    
    @pytest.mark.asyncio
    async def test_basic_ingest_workflow(self):
        """Test basic ingestion workflow to hit main paths"""
        file_path = os.path.join(self.temp_dir, 'workflow.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'workflow.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,John\n2,Jane\n')
        
        format_config = {
            "csv_format": {
                "delimiter": ",",
                "has_header": True,
                "has_trailer": False
            }
        }
        
        # Mock dependencies
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='test_table'), \
             patch.object(self.ingester, '_load_dataframe_to_table') as mock_load:
            
            # Mock async generator for load
            async def mock_load_gen(*args):
                yield "Loading started"
                yield "Loading complete"
            mock_load.return_value = mock_load_gen()
            
            # Run ingestion
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'workflow.csv'
            ):
                messages.append(message)
            
            # Should have completed successfully
            assert len(messages) > 5  # Multiple progress messages
            assert any("Database connection established" in msg for msg in messages)
            assert any("Loading complete" in msg for msg in messages)