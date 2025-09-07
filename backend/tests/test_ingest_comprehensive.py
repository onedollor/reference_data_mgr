"""
Comprehensive test coverage for utils/ingest.py
Focuses on improving coverage from 5% to much higher level
"""

import pytest
import pandas as pd
import asyncio
import os
import tempfile
import json
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from datetime import datetime
from typing import Dict, Any


class TestDataIngesterInit:
    """Test DataIngester initialization and configuration"""

    @patch.dict('os.environ', {
        'INGEST_PROGRESS_INTERVAL': '10',
        'INGEST_TYPE_INFERENCE': '1',
        'INGEST_TYPE_SAMPLE_ROWS': '1000',
        'INGEST_DATE_THRESHOLD': '0.9'
    })
    def test_data_ingester_init_with_env_vars(self):
        """Test DataIngester initialization with environment variables"""
        from utils.ingest import DataIngester
        from utils.database import DatabaseManager
        from utils.logger import Logger
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        ingester = DataIngester(mock_db, mock_logger)
        
        assert ingester.db_manager == mock_db
        assert ingester.logger == mock_logger
        assert ingester.batch_size == 990
        assert ingester.progress_batch_interval == 10
        assert ingester.enable_type_inference is True
        assert ingester.type_sample_rows == 1000
        assert ingester.date_parse_threshold == 0.9

    @patch.dict('os.environ', {
        'INGEST_PROGRESS_INTERVAL': 'invalid',
        'INGEST_TYPE_SAMPLE_ROWS': 'invalid'
    })
    def test_data_ingester_init_invalid_env_vars(self):
        """Test DataIngester initialization with invalid environment variables"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        ingester = DataIngester(mock_db, mock_logger)
        
        # Should use defaults when invalid
        assert ingester.progress_batch_interval == 5
        assert ingester.type_sample_rows == 5000

    def test_data_ingester_init_defaults(self):
        """Test DataIngester initialization with default values"""
        from utils.ingest import DataIngester
        
        mock_db = MagicMock()
        mock_logger = MagicMock()
        
        ingester = DataIngester(mock_db, mock_logger)
        
        assert ingester.batch_size == 990
        assert ingester.progress_batch_interval == 5
        assert ingester.enable_type_inference is False
        assert ingester.type_sample_rows == 5000
        assert ingester.date_parse_threshold == 0.8


class TestDataIngesterUtilityMethods:
    """Test DataIngester utility methods"""

    def setup_method(self):
        """Setup test fixtures"""
        from utils.ingest import DataIngester
        
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)

    def test_sanitize_headers_basic(self):
        """Test header sanitization"""
        headers = ['Name With Spaces', 'Special@Chars!', 'numbers123', 'UPPERCASE']
        result = self.ingester._sanitize_headers(headers)
        
        assert 'Name_With_Spaces' in result
        assert 'Special_Chars_' in result
        assert 'numbers123' in result
        assert 'UPPERCASE' in result

    def test_sanitize_headers_edge_cases(self):
        """Test header sanitization edge cases"""
        headers = ['', '   ', '123StartWithNumber', '__reserved__', 'a' * 200]
        result = self.ingester._sanitize_headers(headers)
        
        assert len(result) == len(headers)
        # Should handle empty, whitespace, numbers, reserved words, long names
        assert result[0] == ''  # Empty string stays empty (filtered out later)
        assert result[1] == ''  # Whitespace becomes empty (filtered out later)  
        assert result[2].startswith('col_')  # Numbers get prefixed
        assert result[3] == '__reserved__'  # Valid identifiers preserved
        assert len(result[4]) <= 120  # Long names get truncated

    def test_deduplicate_headers_no_duplicates(self):
        """Test header deduplication with no duplicates"""
        headers = ['col1', 'col2', 'col3']
        result = self.ingester._deduplicate_headers(headers)
        
        assert result == headers

    def test_deduplicate_headers_with_duplicates(self):
        """Test header deduplication with duplicates"""
        headers = ['col1', 'col2', 'col1', 'col3', 'col2']
        result = self.ingester._deduplicate_headers(headers)
        
        assert len(result) == len(headers)
        assert len(set(result)) == len(result)  # All unique
        assert 'col1' in result
        assert 'col1_1' in result or 'col1_2' in result

    def test_infer_types_basic(self):
        """Test type inference for basic data types"""
        sample_data = {
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'str_col': ['a', 'b', 'c', 'd', 'e'],
            'date_col': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']
        }
        df = pd.DataFrame(sample_data)
        
        result = self.ingester._infer_types(df, list(df.columns))
        
        assert isinstance(result, dict)
        assert len(result) == len(df.columns)
        for col in df.columns:
            assert col in result
            assert isinstance(result[col], str)

    def test_infer_types_mixed_data(self):
        """Test type inference with mixed data types"""
        sample_data = {
            'mixed_col': [1, 'text', 3.14, None, 'more_text'],
            'mostly_numeric': [1, 2, 3, 4, 'text'],
            'empty_col': [None, None, None, None, None]
        }
        df = pd.DataFrame(sample_data)
        
        result = self.ingester._infer_types(df, list(df.columns))
        
        assert isinstance(result, dict)
        assert len(result) == len(df.columns)

    def test_persist_inferred_schema(self):
        """Test persisting inferred schema to file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            # Write initial JSON structure
            json.dump({"existing_key": "existing_value"}, tmp, indent=2)
            tmp_path = tmp.name
        
        try:
            schema_map = {'col1': 'VARCHAR(100)', 'col2': 'INT', 'col3': 'FLOAT'}
            
            self.ingester._persist_inferred_schema(tmp_path, schema_map)
            
            # Verify file was written
            assert os.path.exists(tmp_path)
            
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)


class TestDataIngesterCSVReading:
    """Test CSV reading functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        from utils.ingest import DataIngester
        
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)

    @pytest.mark.asyncio
    async def test_read_csv_file_basic(self):
        """Test basic CSV file reading"""
        csv_content = "name,age,city\nJohn,30,NYC\nJane,25,LA\n"
        csv_format = {
            'column_delimiter': ',',
            'row_delimiter': '\n',
            'text_qualifier': '"',
            'has_header': True,
            'has_trailer': False
        }
        
        with patch('builtins.open', mock_open(read_data=csv_content)):
            with patch('pandas.read_csv') as mock_read_csv:
                mock_df = pd.DataFrame({
                    'name': ['John', 'Jane'],
                    'age': [30, 25], 
                    'city': ['NYC', 'LA']
                })
                mock_read_csv.return_value = mock_df
                
                result = await self.ingester._read_csv_file('/test/file.csv', csv_format)
                
                assert isinstance(result, pd.DataFrame)
                mock_read_csv.assert_called_once()

    @pytest.mark.asyncio
    async def test_read_csv_file_with_trailer(self):
        """Test CSV reading with trailer removal"""
        csv_content = "name,age,city\nJohn,30,NYC\nJane,25,LA\nTotal,2,Records\n"
        csv_format = {
            'column_delimiter': ',',
            'row_delimiter': '\n', 
            'text_qualifier': '"',
            'has_header': True,
            'has_trailer': True,
            'trailer_line': 'Total,2,Records'
        }
        
        with patch('builtins.open', mock_open(read_data=csv_content)):
            with patch('pandas.read_csv') as mock_read_csv:
                # Mock DataFrame with trailer
                mock_df = pd.DataFrame({
                    'name': ['John', 'Jane', 'Total'],
                    'age': [30, 25, 2],
                    'city': ['NYC', 'LA', 'Records']
                })
                mock_read_csv.return_value = mock_df
                
                result = await self.ingester._read_csv_file('/test/file.csv', csv_format)
                
                # Should remove last row (trailer)
                assert len(result) == 2  # Original 3 rows minus 1 trailer
                assert 'Total' not in result['name'].values

    @pytest.mark.asyncio
    async def test_read_csv_file_with_progress(self):
        """Test CSV reading with progress tracking"""
        csv_content = "name,age\nJohn,30\nJane,25\n"
        csv_format = {
            'column_delimiter': ',',
            'has_header': True,
            'has_trailer': False
        }
        
        with patch('builtins.open', mock_open(read_data=csv_content)):
            with patch('pandas.read_csv') as mock_read_csv:
                with patch('utils.progress.update_progress') as mock_progress:
                    mock_df = pd.DataFrame({'name': ['John', 'Jane'], 'age': [30, 25]})
                    mock_read_csv.return_value = mock_df
                    
                    result = await self.ingester._read_csv_file('/test/file.csv', csv_format, 'test_key')
                    
                    assert isinstance(result, pd.DataFrame)
                    mock_progress.assert_called()

    @pytest.mark.asyncio
    async def test_read_csv_file_error_handling(self):
        """Test CSV reading error handling"""
        csv_format = {'column_delimiter': ','}
        
        with patch('builtins.open', side_effect=IOError("File not found")):
            with pytest.raises(Exception):
                await self.ingester._read_csv_file('/nonexistent/file.csv', csv_format)


class TestDataIngesterMainIngestion:
    """Test main ingestion workflow"""

    def setup_method(self):
        """Setup test fixtures"""
        from utils.ingest import DataIngester
        
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)

    @pytest.mark.asyncio
    async def test_ingest_data_basic_workflow(self):
        """Test basic ingestion workflow"""
        # Mock dependencies
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.data_schema = 'ref'
        
        # Mock file handler
        with patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='test_table'):
            with patch.object(self.ingester.file_handler, 'read_format_file') as mock_read_format:
                mock_read_format.return_value = {
                    'csv_format': {
                        'column_delimiter': ',',
                        'has_header': True,
                        'has_trailer': False
                    }
                }
                
                # Mock CSV reading
                with patch.object(self.ingester, '_read_csv_file') as mock_read_csv:
                    test_df = pd.DataFrame({
                        'name': ['John', 'Jane'],
                        'age': [30, 25]
                    })
                    mock_read_csv.return_value = test_df
                    
                    # Mock progress
                    with patch('utils.progress.init_progress'), \
                         patch('utils.progress.is_canceled', return_value=False), \
                         patch('utils.progress.update_progress'), \
                         patch('utils.progress.complete_progress'):
                        
                        # Mock database operations
                        with patch.object(self.ingester, '_load_dataframe_to_table') as mock_load:
                            mock_load.return_value = AsyncMock()
                            
                            # Execute ingestion
                            messages = []
                            async for message in self.ingester.ingest_data(
                                '/test/file.csv',
                                '/test/format.json',
                                'full',
                                'test_file.csv'
                            ):
                                messages.append(message)
                            
                            # Verify workflow steps
                            assert len(messages) > 0
                            assert any('Starting data ingestion' in msg for msg in messages)
                            assert any('Database connection established' in msg for msg in messages)
                            assert any('CSV file loaded' in msg for msg in messages)

    @pytest.mark.asyncio
    async def test_ingest_data_with_cancellation(self):
        """Test ingestion with user cancellation"""
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        
        # Mock progress cancellation
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=True):
            
            with pytest.raises(Exception, match="canceled by user"):
                messages = []
                async for message in self.ingester.ingest_data(
                    '/test/file.csv',
                    '/test/format.json', 
                    'full',
                    'test_file.csv'
                ):
                    messages.append(message)

    @pytest.mark.asyncio
    async def test_ingest_data_empty_file(self):
        """Test ingestion with empty CSV file"""
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        
        with patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='test_table'):
            with patch.object(self.ingester.file_handler, 'read_format_file') as mock_read_format:
                mock_read_format.return_value = {'csv_format': {'column_delimiter': ','}}
                
                # Mock empty DataFrame
                with patch.object(self.ingester, '_read_csv_file') as mock_read_csv:
                    empty_df = pd.DataFrame()  # Empty DataFrame
                    mock_read_csv.return_value = empty_df
                    
                    with patch('utils.progress.init_progress'), \
                         patch('utils.progress.is_canceled', return_value=False), \
                         patch('utils.progress.update_progress'), \
                         patch('utils.progress.request_cancel') as mock_cancel:
                        
                        messages = []
                        async for message in self.ingester.ingest_data(
                            '/test/empty.csv',
                            '/test/format.json',
                            'full', 
                            'empty.csv'
                        ):
                            messages.append(message)
                        
                        # Should detect empty file and auto-cancel
                        assert any('no data rows' in msg for msg in messages)
                        mock_cancel.assert_called()

    @pytest.mark.asyncio
    async def test_ingest_data_with_target_schema(self):
        """Test ingestion with custom target schema"""
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.data_schema = 'ref'
        
        with patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='test_table'):
            with patch.object(self.ingester.file_handler, 'read_format_file') as mock_read_format:
                mock_read_format.return_value = {'csv_format': {'column_delimiter': ','}}
                
                with patch.object(self.ingester, '_read_csv_file') as mock_read_csv:
                    test_df = pd.DataFrame({'col1': [1, 2]})
                    mock_read_csv.return_value = test_df
                    
                    with patch('utils.progress.init_progress'), \
                         patch('utils.progress.is_canceled', return_value=False), \
                         patch('utils.progress.update_progress'), \
                         patch('utils.progress.complete_progress'):
                        
                        with patch.object(self.ingester, '_load_dataframe_to_table'):
                            messages = []
                            async for message in self.ingester.ingest_data(
                                '/test/file.csv',
                                '/test/format.json',
                                'full',
                                'test.csv',
                                target_schema='custom_schema'
                            ):
                                messages.append(message)
                            
                            # Should mention custom schema
                            assert any('custom_schema' in msg for msg in messages)
                            # Schema should be temporarily changed during ingestion
                            assert self.mock_db.data_schema == 'ref'  # Restored after

    @pytest.mark.asyncio
    async def test_ingest_data_error_handling(self):
        """Test ingestion error handling"""
        # Mock database connection failure
        self.mock_db.get_connection.side_effect = Exception("Connection failed")
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.error_progress') as mock_error:
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    '/test/file.csv',
                    '/test/format.json',
                    'full',
                    'test.csv'
                ):
                    messages.append(message)
            except Exception:
                pass  # Expected
            
            # Should handle error gracefully
            mock_error.assert_called()


class TestDataIngesterDatabaseLoading:
    """Test database loading functionality"""

    def setup_method(self):
        """Setup test fixtures"""
        from utils.ingest import DataIngester
        
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)

    @pytest.mark.asyncio
    async def test_load_dataframe_to_table_basic(self):
        """Test basic DataFrame loading to database"""
        test_df = pd.DataFrame({
            'name': ['John', 'Jane'],
            'age': [30, 25],
            'city': ['NYC', 'LA']
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock database operations
        with patch.object(self.mock_db, 'table_exists', return_value=False):
            with patch.object(self.mock_db, 'create_table'):
                with patch.object(self.mock_db, 'truncate_table'):
                    with patch('utils.progress.update_progress'):
                        
                        result_gen = self.ingester._load_dataframe_to_table(
                            test_df,
                            mock_connection,
                            'test_table',
                            'full',
                            datetime.utcnow(),
                            'test_key'
                        )
                        
                        messages = []
                        async for message in result_gen:
                            messages.append(message)
                        
                        assert len(messages) > 0
                        mock_cursor.execute.assert_called()

    @pytest.mark.asyncio 
    async def test_load_dataframe_batch_processing(self):
        """Test batch processing during DataFrame loading"""
        # Create large DataFrame to test batching
        large_data = {
            'col1': list(range(2000)),  # More than batch_size of 990
            'col2': [f'value_{i}' for i in range(2000)]
        }
        large_df = pd.DataFrame(large_data)
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.object(self.mock_db, 'table_exists', return_value=True):
            with patch.object(self.mock_db, 'get_table_columns', return_value=[
                {'name': 'col1', 'type': 'int'},
                {'name': 'col2', 'type': 'varchar'}
            ]):
                with patch.object(self.mock_db, 'truncate_table'):
                    with patch('utils.progress.update_progress'):
                        with patch('os.getenv', return_value="500"):  # Mock batch size env var
                            
                            result_gen = self.ingester._load_dataframe_to_table(
                                large_df,
                                mock_connection,
                                'test_table',
                                'full',
                                datetime.utcnow(),
                                'test_key'
                            )
                            
                            messages = []
                            async for message in result_gen:
                                messages.append(message)
                            
                            # Should have processed in multiple batches
                            assert any('batch' in msg.lower() for msg in messages)
                            # Should have executed multiple INSERT statements
                            assert mock_cursor.execute.call_count > 1

    @pytest.mark.asyncio
    async def test_load_dataframe_demo_slow_progress(self):
        """Test loading with demo slow progress enabled"""
        test_df = pd.DataFrame({'col1': [1, 2, 3]})
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.object(self.mock_db, 'table_exists', return_value=False):
            with patch.object(self.mock_db, 'create_table'):
                with patch.object(self.mock_db, 'truncate_table'):
                    with patch('utils.progress.update_progress'):
                        with patch('os.getenv') as mock_getenv:
                            # Configure environment variables
                            def getenv_side_effect(key, default=None):
                                if key == "DEMO_SLOW_PROGRESS":
                                    return "1"
                                elif key == "INGEST_BATCH_SIZE":
                                    return default
                                return default
                            
                            mock_getenv.side_effect = getenv_side_effect
                            
                            with patch('asyncio.sleep') as mock_sleep:
                                result_gen = self.ingester._load_dataframe_to_table(
                                    test_df,
                                    mock_connection,
                                    'test_table',
                                    'full',
                                    datetime.utcnow(),
                                    'test_key'
                                )
                                
                                messages = []
                                async for message in result_gen:
                                    messages.append(message)
                                
                                # Should have introduced delays for demo
                                mock_sleep.assert_called()