"""
Comprehensive tests to push utils/ingest.py from 20% to >90% coverage
Targeting key ingestion functionality and async generators
"""

import pytest
import asyncio
import os
import tempfile
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock, call
from datetime import datetime

# Mock dependencies before importing
import sys

# Mock pandas
mock_pd = MagicMock()
sys.modules['pandas'] = mock_pd

# Mock progress module
mock_progress = MagicMock()
sys.modules['utils.progress'] = mock_progress

from utils.ingest import DataIngester


class MockDatabaseManager:
    """Mock database manager for testing"""
    
    def __init__(self, simulate_error=False):
        self.simulate_error = simulate_error
        self.data_schema = 'ref'
        
    def get_connection(self):
        if self.simulate_error:
            raise Exception("Database connection failed")
        return MockConnection()
    
    def ensure_schemas_exist(self, connection):
        if self.simulate_error:
            raise Exception("Schema creation failed")
    
    def determine_load_type(self, connection, table_name, load_mode, override_load_type):
        if override_load_type:
            return 'F' if override_load_type == 'full' else 'A'
        return 'F' if load_mode == 'full' else 'A'
    
    def table_exists(self, connection, table_name):
        return table_name in ['existing_table', 'existing_stage']
    
    def get_row_count(self, connection, table_name):
        return 100 if table_name == 'existing_table' else 0
    
    def create_table(self, connection, table_name, columns, add_metadata_columns=True):
        pass
    
    def ensure_metadata_columns(self, connection, table_name):
        return {'added': []}
    
    def sync_main_table_columns(self, connection, table_name, columns):
        return {'added': [], 'mismatched': []}
    
    def truncate_table(self, connection, table_name):
        pass


class MockConnection:
    """Mock database connection"""
    
    def __init__(self):
        self.closed = False
        
    def close(self):
        self.closed = True


class MockFileHandler:
    """Mock file handler for testing"""
    
    def __init__(self):
        pass
    
    def extract_table_base_name(self, filename):
        return filename.replace('.csv', '').replace('.', '_')
    
    async def read_format_file(self, fmt_file_path):
        return {
            'csv_format': {
                'column_delimiter': ',',
                'row_delimiter': '\n',
                'text_qualifier': '"',
                'has_header': True,
                'has_trailer': False,
                'skip_lines': 0
            }
        }


class MockLogger:
    """Mock logger for testing"""
    
    def __init__(self):
        self.logs = []
    
    async def log_info(self, action_step, message, metadata=None):
        self.logs.append(('INFO', action_step, message, metadata))
    
    async def log_error(self, action_step, message, traceback_info=None, metadata=None):
        self.logs.append(('ERROR', action_step, message, traceback_info, metadata))


class MockDataFrame:
    """Mock pandas DataFrame for testing"""
    
    def __init__(self, data=None, columns=None):
        self.data = data or [['val1', 'val2'], ['val3', 'val4']]
        self.columns = columns or ['col1', 'col2']
        self._index = 0
    
    def __len__(self):
        return len(self.data)
    
    def head(self, n=5):
        return MockDataFrame(self.data[:n], self.columns)
    
    def rename(self, columns=None):
        if columns:
            new_columns = [columns.get(col, col) for col in self.columns]
            return MockDataFrame(self.data, new_columns)
        return self
    
    def iloc(self, indices):
        if isinstance(indices, slice):
            return MockDataFrame(self.data[indices], self.columns)
        return self.data[indices]
    
    def iterrows(self):
        for i, row in enumerate(self.data):
            yield i, dict(zip(self.columns, row))
    
    def to_dict(self, orient='records'):
        if orient == 'records':
            return [dict(zip(self.columns, row)) for row in self.data]
        return {}


class TestDataIngester90Percent:
    """Comprehensive tests for DataIngester class"""
    
    def setup_method(self):
        """Setup for each test"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create mock dependencies
        self.mock_db_manager = MockDatabaseManager()
        self.mock_logger = MockLogger()
        
        # Setup environment variables
        self.env_vars = {
            'INGEST_PROGRESS_INTERVAL': '5',
            'INGEST_TYPE_INFERENCE': '1',
            'INGEST_TYPE_SAMPLE_ROWS': '5000',
            'INGEST_DATE_THRESHOLD': '0.8'
        }
        
    def teardown_method(self):
        """Cleanup after each test"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @patch.dict('os.environ', {'INGEST_PROGRESS_INTERVAL': '10', 'INGEST_TYPE_INFERENCE': 'true'})
    def test_init_with_environment_variables(self):
        """Test DataIngester initialization with environment variables"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        assert ingester.batch_size == 990
        assert ingester.progress_batch_interval == 10
        assert ingester.enable_type_inference is True
        assert ingester.type_sample_rows == 5000
        assert ingester.date_parse_threshold == 0.8
    
    @patch.dict('os.environ', {'INGEST_PROGRESS_INTERVAL': 'invalid', 'INGEST_TYPE_SAMPLE_ROWS': 'invalid'})
    def test_init_with_invalid_environment_variables(self):
        """Test initialization with invalid environment variables"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        # Should use defaults when invalid
        assert ingester.progress_batch_interval == 5
        assert ingester.type_sample_rows == 5000
    
    @patch.dict('os.environ', {})
    def test_init_with_default_values(self):
        """Test initialization with default values"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        assert ingester.batch_size == 990
        assert ingester.progress_batch_interval == 5
        assert ingester.enable_type_inference is False
        assert ingester.type_sample_rows == 5000
        assert ingester.date_parse_threshold == 0.8
    
    @pytest.mark.asyncio
    async def test_ingest_data_basic_flow(self):
        """Test basic ingestion flow"""
        # Mock file handler
        mock_file_handler = MockFileHandler()
        
        # Create test CSV file
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        with open(test_csv, 'w') as f:
            f.write('col1,col2\nval1,val2\nval3,val4')
        
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        # Mock progress functions
        with patch('utils.progress.init_progress') as mock_init, \
             patch('utils.progress.is_canceled', return_value=False) as mock_canceled, \
             patch('utils.progress.update_progress') as mock_update, \
             patch.object(DataIngester, '_read_csv_file') as mock_read_csv:
            
            # Mock CSV reading to return test data
            mock_df = MockDataFrame([['val1', 'val2'], ['val3', 'val4']], ['col1', 'col2'])
            mock_read_csv.return_value = mock_df
            
            ingester = DataIngester(self.mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            # Collect all yielded messages
            messages = []
            async for message in ingester.ingest_data(
                file_path=test_csv,
                fmt_file_path=test_fmt,
                load_mode='full',
                filename='test.csv'
            ):
                messages.append(message)
            
            # Verify key steps were executed
            assert any('Starting data ingestion process' in msg for msg in messages)
            assert any('Connecting to database' in msg for msg in messages)
            assert any('Reading CSV file' in msg for msg in messages)
            assert any('Processing CSV headers' in msg for msg in messages)
            
            # Verify mocks were called
            mock_init.assert_called_once()
            mock_read_csv.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ingest_data_with_cancellation(self):
        """Test ingestion with early cancellation"""
        mock_file_handler = MockFileHandler()
        
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=True):  # Always canceled
            
            ingester = DataIngester(self.mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            messages = []
            with pytest.raises(Exception, match="Ingestion canceled by user"):
                async for message in ingester.ingest_data(
                    file_path=test_csv,
                    fmt_file_path=test_fmt,
                    load_mode='full',
                    filename='test.csv'
                ):
                    messages.append(message)
            
            assert any('Cancellation requested' in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_database_connection_failure(self):
        """Test ingestion with database connection failure"""
        mock_file_handler = MockFileHandler()
        mock_db_manager = MockDatabaseManager(simulate_error=True)
        
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False):
            
            ingester = DataIngester(mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            with pytest.raises(Exception, match="Database connection failed"):
                async for message in ingester.ingest_data(
                    file_path=test_csv,
                    fmt_file_path=test_fmt,
                    load_mode='full',
                    filename='test.csv'
                ):
                    pass
    
    @pytest.mark.asyncio
    async def test_ingest_data_empty_file(self):
        """Test ingestion with empty CSV file"""
        mock_file_handler = MockFileHandler()
        
        test_csv = os.path.join(self.temp_dir, 'empty.csv')
        test_fmt = os.path.join(self.temp_dir, 'empty.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel') as mock_cancel, \
             patch.object(DataIngester, '_read_csv_file') as mock_read_csv:
            
            # Mock empty CSV
            mock_df = MockDataFrame([], [])
            mock_read_csv.return_value = mock_df
            
            ingester = DataIngester(self.mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            messages = []
            async for message in ingester.ingest_data(
                file_path=test_csv,
                fmt_file_path=test_fmt,
                load_mode='full',
                filename='empty.csv'
            ):
                messages.append(message)
            
            assert any('CSV file contains no data rows' in msg for msg in messages)
            assert any('automatically canceled due to empty file' in msg for msg in messages)
            mock_cancel.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ingest_data_with_target_schema(self):
        """Test ingestion with custom target schema"""
        mock_file_handler = MockFileHandler()
        
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch.object(DataIngester, '_read_csv_file') as mock_read_csv:
            
            mock_df = MockDataFrame([['val1', 'val2']], ['col1', 'col2'])
            mock_read_csv.return_value = mock_df
            
            ingester = DataIngester(self.mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            original_schema = self.mock_db_manager.data_schema
            
            messages = []
            async for message in ingester.ingest_data(
                file_path=test_csv,
                fmt_file_path=test_fmt,
                load_mode='full',
                filename='test.csv',
                target_schema='custom_schema'
            ):
                messages.append(message)
                if 'Using target schema' in message:
                    break
            
            assert any('Using target schema: custom_schema' in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_with_type_inference_disabled(self):
        """Test ingestion with type inference disabled"""
        mock_file_handler = MockFileHandler()
        
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch.object(DataIngester, '_read_csv_file') as mock_read_csv:
            
            mock_df = MockDataFrame([['val1', 'val2']], ['col1', 'col2'])
            mock_read_csv.return_value = mock_df
            
            ingester = DataIngester(self.mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            ingester.enable_type_inference = False  # Disable type inference
            
            messages = []
            async for message in ingester.ingest_data(
                file_path=test_csv,
                fmt_file_path=test_fmt,
                load_mode='append',
                filename='test.csv'
            ):
                messages.append(message)
            
            # Should not see type inference messages when disabled
            assert not any('Inferring column data types' in msg for msg in messages)
            assert any('Column definitions prepared' in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_with_trailer(self):
        """Test ingestion with CSV trailer"""
        mock_file_handler = MockFileHandler()
        
        # Mock format config with trailer
        async def mock_read_format_file(fmt_file_path):
            return {
                'csv_format': {
                    'column_delimiter': ',',
                    'row_delimiter': '\n',
                    'text_qualifier': '"',
                    'has_header': True,
                    'has_trailer': True,  # Has trailer
                    'skip_lines': 0
                }
            }
        
        mock_file_handler.read_format_file = mock_read_format_file
        
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch.object(DataIngester, '_read_csv_file') as mock_read_csv:
            
            mock_df = MockDataFrame([['val1', 'val2']], ['col1', 'col2'])
            mock_read_csv.return_value = mock_df
            
            ingester = DataIngester(self.mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            messages = []
            async for message in ingester.ingest_data(
                file_path=test_csv,
                fmt_file_path=test_fmt,
                load_mode='full',
                filename='test.csv'
            ):
                messages.append(message)
            
            assert any('Trailer detected' in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_append_mode_existing_table(self):
        """Test ingestion in append mode with existing table"""
        mock_file_handler = MockFileHandler()
        
        # Mock database manager that returns existing table
        mock_db_manager = MockDatabaseManager()
        mock_db_manager.table_exists = lambda conn, table_name: table_name == 'existing_table'
        
        test_csv = os.path.join(self.temp_dir, 'test.csv')
        test_fmt = os.path.join(self.temp_dir, 'test.fmt')
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch.object(DataIngester, '_read_csv_file') as mock_read_csv:
            
            mock_df = MockDataFrame([['val1', 'val2']], ['col1', 'col2'])
            mock_read_csv.return_value = mock_df
            
            ingester = DataIngester(mock_db_manager, self.mock_logger)
            ingester.file_handler = mock_file_handler
            
            messages = []
            async for message in ingester.ingest_data(
                file_path=test_csv,
                fmt_file_path=test_fmt,
                load_mode='append',
                filename='existing_table.csv'
            ):
                messages.append(message)
            
            assert any('append mode' in msg for msg in messages)
    
    def test_sanitize_headers(self):
        """Test header sanitization"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        # Test various problematic headers
        headers = [
            'Valid_Header',
            'With Spaces',
            'With-Hyphens',
            'With.Dots',
            'With@Special#Chars!',
            '123NumericStart',
            '',
            None,
            'VERY_LONG_HEADER_' + 'X' * 100
        ]
        
        sanitized = ingester._sanitize_headers(headers)
        
        # Check results
        assert sanitized[0] == 'Valid_Header'
        assert sanitized[1] == 'With_Spaces'
        assert sanitized[2] == 'With_Hyphens'
        assert sanitized[3] == 'With_Dots'
        assert '_123NumericStart' in sanitized[5] or sanitized[5].startswith('col_')  # Numeric start
        assert sanitized[6] == ''  # Empty header
        assert sanitized[7] == ''  # None header
        assert len(sanitized[8]) <= 128  # Truncated long header
    
    def test_deduplicate_headers(self):
        """Test header deduplication"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        headers = ['col1', 'col2', 'col1', 'col2', 'col3', 'col1']
        
        deduplicated = ingester._deduplicate_headers(headers)
        
        assert deduplicated == ['col1', 'col2', 'col1_1', 'col2_1', 'col3', 'col1_2']
    
    def test_infer_types_basic(self):
        """Test basic type inference"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        # Create mock DataFrame with different data types
        mock_df = MagicMock()
        mock_df.__len__ = lambda: 3
        mock_df.iloc = [
            {'col1': '123', 'col2': 'text', 'col3': '2023-01-01'},
            {'col1': '456', 'col2': 'more text', 'col3': '2023-02-01'},
            {'col1': '789', 'col2': 'even more', 'col3': '2023-03-01'}
        ]
        
        columns = ['col1', 'col2', 'col3']
        
        result = ingester._infer_types(mock_df, columns)
        
        # All should be varchar since that's the default
        for col in columns:
            assert 'varchar' in result[col]
    
    @patch('builtins.open', create=True)
    @patch('json.load')
    @patch('json.dump')
    def test_persist_inferred_schema(self, mock_json_dump, mock_json_load, mock_open):
        """Test persisting inferred schema to format file"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        # Mock existing format config
        mock_json_load.return_value = {
            'csv_format': {'column_delimiter': ','},
            'file_info': {'version': '1.0'}
        }
        
        mock_file = MagicMock()
        mock_open.return_value.__enter__ = lambda x: mock_file
        mock_open.return_value.__exit__ = lambda x, y, z, w: None
        
        inferred_map = {'col1': 'varchar(255)', 'col2': 'varchar(100)'}
        
        ingester._persist_inferred_schema('/test/format.fmt', inferred_map)
        
        # Verify file operations
        assert mock_open.call_count == 2  # One read, one write
        mock_json_dump.assert_called_once()
    
    @patch('builtins.open', side_effect=Exception("File error"))
    def test_persist_inferred_schema_error(self, mock_open):
        """Test error handling in schema persistence"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        inferred_map = {'col1': 'varchar(255)'}
        
        # Should not raise exception, just handle gracefully
        try:
            ingester._persist_inferred_schema('/bad/path.fmt', inferred_map)
        except Exception:
            pytest.fail("_persist_inferred_schema should handle file errors gracefully")
    
    @pytest.mark.asyncio
    @patch('pandas.read_csv')
    async def test_read_csv_file_basic(self, mock_read_csv):
        """Test basic CSV file reading"""
        # Mock pandas read_csv
        mock_df = MockDataFrame([['val1', 'val2'], ['val3', 'val4']], ['col1', 'col2'])
        mock_read_csv.return_value = mock_df
        
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        csv_format = {
            'column_delimiter': ',',
            'row_delimiter': '\n',
            'text_qualifier': '"',
            'has_header': True,
            'has_trailer': False,
            'skip_lines': 0
        }
        
        result = await ingester._read_csv_file('/test/file.csv', csv_format, 'test_key')
        
        assert result is not None
        mock_read_csv.assert_called_once()
    
    @pytest.mark.asyncio
    @patch('pandas.read_csv')
    async def test_read_csv_file_with_trailer(self, mock_read_csv):
        """Test CSV file reading with trailer removal"""
        # Mock pandas with trailer data
        mock_df = MockDataFrame([['val1', 'val2'], ['val3', 'val4'], ['TOTAL', '100']], ['col1', 'col2'])
        mock_read_csv.return_value = mock_df
        
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        csv_format = {
            'column_delimiter': ',',
            'row_delimiter': '\n', 
            'text_qualifier': '"',
            'has_header': True,
            'has_trailer': True,  # Has trailer
            'skip_lines': 0
        }
        
        result = await ingester._read_csv_file('/test/file.csv', csv_format, 'test_key')
        
        # Should have removed last row (trailer)
        assert len(result) == 2  # Original had 3 rows, trailer removed
    
    @pytest.mark.asyncio
    @patch('pandas.read_csv', side_effect=Exception("CSV read error"))
    async def test_read_csv_file_error(self, mock_read_csv):
        """Test CSV file reading with error"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        csv_format = {
            'column_delimiter': ',',
            'has_header': True,
            'has_trailer': False
        }
        
        with pytest.raises(Exception, match="CSV read error"):
            await ingester._read_csv_file('/bad/file.csv', csv_format, 'test_key')
    
    def test_sql_escape_string(self):
        """Test SQL string escaping"""
        ingester = DataIngester(self.mock_db_manager, self.mock_logger)
        
        test_cases = [
            ("normal text", "normal text"),
            ("text with 'quotes'", "text with ''quotes''"),
            ("text with \"double quotes\"", "text with \"double quotes\""),
            ("", ""),
            (None, ""),
        ]
        
        for input_val, expected in test_cases:
            result = ingester._sql_escape_string(input_val)
            assert result == expected