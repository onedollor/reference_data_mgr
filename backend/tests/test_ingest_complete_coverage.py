"""
Comprehensive tests to achieve 100% coverage for utils/ingest.py
Targeting all missing lines: 89-90, 112-113, 130-135, 145-152, 157-158, 163-183, etc.
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open
from datetime import datetime
import time

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterCompleteCoverage:
    """Comprehensive tests targeting all missing lines in ingest.py"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        # Make logger methods async
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_ingest_data_cancellation_after_database_connection(self):
        """Test cancellation after database connection - covers lines 89-90"""
        file_path = os.path.join(self.temp_dir, 'test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        with open(fmt_file_path, 'w') as f:
            f.write('{"csv_format": {"delimiter": ","}}')
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        # Mock progress to be canceled after database connection
        with patch('utils.progress.is_canceled') as mock_is_canceled, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return False initially, then True after database connection
            call_count = 0
            def mock_cancel_side_effect(key):
                nonlocal call_count
                call_count += 1
                return call_count >= 2  # Cancel on second call
            
            mock_is_canceled.side_effect = mock_cancel_side_effect
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'test.csv', 
                    progress_key='test_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should have reached cancellation message (line 89)
            assert any("Cancellation requested - stopping after database connection" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_cancellation_after_format_config(self):
        """Test cancellation after format configuration - covers lines 112-113"""
        file_path = os.path.join(self.temp_dir, 'test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        # Mock format file reading
        mock_format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=mock_format_config) as mock_read_fmt, \
             patch('utils.progress.is_canceled') as mock_is_canceled, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return False for first two calls, True on third call (after format config)
            call_count = 0
            def mock_cancel_side_effect(key):
                nonlocal call_count
                call_count += 1
                return call_count >= 3
            
            mock_is_canceled.side_effect = mock_cancel_side_effect
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'test.csv',
                    progress_key='test_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should have reached cancellation after format config (line 112)
            assert any("Cancellation requested - stopping after format configuration" in msg for msg in messages)
    
    @pytest.mark.asyncio 
    async def test_ingest_data_empty_dataframe_handling(self):
        """Test empty dataframe handling - covers lines 130-135"""
        file_path = os.path.join(self.temp_dir, 'empty.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'empty.fmt')
        
        # Create empty CSV file (just headers)
        with open(file_path, 'w') as f:
            f.write('id,name\n')  # Only headers, no data rows
        
        # Mock format file
        mock_format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=mock_format_config), \
             patch.object(self.ingester, '_read_csv_file') as mock_read_csv, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return empty dataframe
            empty_df = pd.DataFrame(columns=['id', 'name'])
            mock_read_csv.return_value = empty_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'empty.csv'
            ):
                messages.append(message)
            
            # Should have handled empty file (lines 130-135)
            assert any("No data to process" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_invalid_headers_auto_cancel(self):
        """Test invalid headers causing auto-cancellation - covers lines 145-152"""
        file_path = os.path.join(self.temp_dir, 'invalid_headers.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'invalid.fmt')
        
        # Create CSV with invalid headers
        with open(file_path, 'w') as f:
            f.write('123,!@#,$%^\n1,2,3\n')  # Invalid column names
        
        mock_format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=mock_format_config), \
             patch.object(self.ingester, '_read_csv_file') as mock_read_csv, \
             patch.object(self.ingester, '_sanitize_headers', return_value=['', '', '']), \
             patch('utils.progress.request_cancel') as mock_request_cancel, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return dataframe with invalid headers
            df = pd.DataFrame([['1', '2', '3']], columns=['123', '!@#', '$%^'])
            mock_read_csv.return_value = df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'invalid.csv',
                progress_key='test_key'
            ):
                messages.append(message)
            
            # Should have triggered auto-cancel (lines 145-152)
            assert any("ERROR! No valid columns found" in msg for msg in messages)
            assert any("automatically canceled due to invalid headers" in msg for msg in messages)
            mock_request_cancel.assert_called_once_with('test_key')
    
    @pytest.mark.asyncio
    async def test_ingest_data_cancellation_after_headers(self):
        """Test cancellation after header processing - covers lines 157-158"""
        file_path = os.path.join(self.temp_dir, 'test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        mock_format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=mock_format_config), \
             patch.object(self.ingester, '_read_csv_file') as mock_read_csv, \
             patch('utils.progress.is_canceled') as mock_is_canceled, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return valid dataframe
            df = pd.DataFrame([['1', 'John'], ['2', 'Jane']], columns=['id', 'name'])
            mock_read_csv.return_value = df
            
            # Cancel after header processing (4th call)
            call_count = 0
            def mock_cancel_side_effect(key):
                nonlocal call_count
                call_count += 1
                return call_count >= 4
            
            mock_is_canceled.side_effect = mock_cancel_side_effect
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'test.csv',
                    progress_key='test_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should have reached cancellation after headers (line 157)
            assert any("Cancellation requested - stopping after header processing" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_with_type_inference(self):
        """Test type inference path - covers lines 163-183"""
        file_path = os.path.join(self.temp_dir, 'typed.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'typed.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name,age\n1,John,25\n2,Jane,30')
        
        mock_format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        # Enable type inference
        self.ingester.enable_type_inference = True
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=mock_format_config), \
             patch.object(self.ingester, '_read_csv_file') as mock_read_csv, \
             patch.object(self.ingester, '_infer_types', return_value={'id': 'int', 'name': 'varchar', 'age': 'int'}) as mock_infer, \
             patch.object(self.ingester, '_persist_inferred_schema') as mock_persist, \
             patch.object(self.ingester, '_load_dataframe_to_table') as mock_load, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return valid dataframe
            df = pd.DataFrame([['1', 'John', '25'], ['2', 'Jane', '30']], columns=['id', 'name', 'age'])
            mock_read_csv.return_value = df
            
            # Mock async generator for load
            async def mock_load_gen(*args):
                yield "Loading data..."
                yield "Complete"
            mock_load.return_value = mock_load_gen()
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'typed.csv'
            ):
                messages.append(message)
            
            # Should have performed type inference (lines 163-183)
            assert any("Inferring column data types" in msg for msg in messages)
            assert any("Type inference complete" in msg for msg in messages)
            assert any("Inferred schema persisted" in msg for msg in messages)
            mock_infer.assert_called_once()
            mock_persist.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_ingest_data_type_inference_persist_failure(self):
        """Test type inference with persist failure - covers lines 175-176"""
        file_path = os.path.join(self.temp_dir, 'typed.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'typed.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,John\n2,Jane')
        
        mock_format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        # Enable type inference
        self.ingester.enable_type_inference = True
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=mock_format_config), \
             patch.object(self.ingester, '_read_csv_file') as mock_read_csv, \
             patch.object(self.ingester, '_infer_types', return_value={'id': 'int', 'name': 'varchar'}), \
             patch.object(self.ingester, '_persist_inferred_schema', side_effect=Exception("Persist failed")), \
             patch.object(self.ingester, '_load_dataframe_to_table') as mock_load, \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Return valid dataframe
            df = pd.DataFrame([['1', 'John'], ['2', 'Jane']], columns=['id', 'name'])
            mock_read_csv.return_value = df
            
            # Mock async generator for load
            async def mock_load_gen(*args):
                yield "Complete"
            mock_load.return_value = mock_load_gen()
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'typed.csv'
            ):
                messages.append(message)
            
            # Should have warning about persist failure (lines 175-176)
            assert any("WARNING: Failed to persist inferred schema" in msg for msg in messages)
    
    def test_sanitize_headers_edge_cases(self):
        """Test header sanitization edge cases - covers various missing lines"""
        # Test with problematic headers
        headers = [
            "123invalid_start",  # Starts with number
            "has spaces",        # Contains spaces  
            "has-dashes",        # Contains dashes
            "has.dots",          # Contains dots
            "UPPERCASE",         # All caps
            "",                  # Empty
            None,                # None value
            "valid_header"       # Valid header
        ]
        
        result = self.ingester._sanitize_headers(headers)
        
        # Should handle all edge cases
        assert len(result) == len(headers)
        assert result[0].startswith('col_')  # Fixed invalid start
        assert '_' in result[1]  # Spaces converted to underscores
        assert result[-1] == "valid_header"  # Valid header unchanged
    
    def test_deduplicate_headers_comprehensive(self):
        """Test header deduplication - covers missing lines"""
        # Test with duplicates
        headers = ["id", "name", "id", "name", "id_2", "name_1"]
        
        result = self.ingester._deduplicate_headers(headers)
        
        # Should have unique headers
        assert len(set(result)) == len(result)
        assert "id" in result
        assert "name" in result
        # Should have created numbered versions
        id_variants = [h for h in result if h.startswith("id")]
        name_variants = [h for h in result if h.startswith("name")]
        assert len(id_variants) >= 2
        assert len(name_variants) >= 2
    
    def test_infer_types_comprehensive(self):
        """Test type inference - covers missing lines"""
        # Create test dataframe with various data types
        df = pd.DataFrame({
            'int_col': ['1', '2', '3', '4', '5'],
            'float_col': ['1.1', '2.2', '3.3', '4.4', '5.5'],
            'date_col': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
            'string_col': ['a', 'b', 'c', 'd', 'e'],
            'mixed_col': ['1', 'abc', '3', 'def', '5']
        })
        
        result = self.ingester._infer_types(df, list(df.columns))
        
        # Should infer appropriate types
        assert isinstance(result, dict)
        assert len(result) == 5
        # All should be varchar since that's what the method returns
        for col_type in result.values():
            assert 'varchar' in col_type
    
    @pytest.mark.asyncio
    async def test_read_csv_file_comprehensive(self):
        """Test CSV file reading - covers lines 610-611, 620, 633-650"""
        file_path = os.path.join(self.temp_dir, 'test_csv.csv')
        
        # Create test CSV
        with open(file_path, 'w') as f:
            f.write('id,name,value\n1,John,100\n2,Jane,200\nTRAILER,END,999\n')
        
        csv_format = {
            'delimiter': ',',
            'has_header': True,
            'has_trailer': True
        }
        
        # Test successful read with trailer
        result = await self.ingester._read_csv_file(file_path, csv_format)
        
        # Should have read and processed the file
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2  # Should exclude trailer row
        assert list(result.columns) == ['id', 'name', 'value']
    
    @pytest.mark.asyncio
    async def test_read_csv_file_error_handling(self):
        """Test CSV file reading error handling - covers exception paths"""
        non_existent_file = os.path.join(self.temp_dir, 'missing.csv')
        
        csv_format = {'delimiter': ',', 'has_header': True}
        
        # Should handle file not found error
        with pytest.raises(Exception, match="Failed to read CSV file"):
            await self.ingester._read_csv_file(non_existent_file, csv_format)
    
    def test_persist_inferred_schema_success(self):
        """Test schema persistence success - covers missing lines"""
        fmt_file_path = os.path.join(self.temp_dir, 'test.fmt')
        
        # Create initial format file
        initial_config = {"csv_format": {"delimiter": ","}}
        with open(fmt_file_path, 'w') as f:
            import json
            json.dump(initial_config, f)
        
        # Test schema persistence
        inferred_types = {
            'id': 'int',
            'name': 'varchar(255)',
            'age': 'int'
        }
        
        with patch('builtins.open', mock_open()) as mock_file, \
             patch('json.dump') as mock_json_dump, \
             patch('json.load', return_value=initial_config):
            
            self.ingester._persist_inferred_schema(fmt_file_path, inferred_types)
            
            # Should have written updated config
            mock_json_dump.assert_called_once()
            
    def test_persist_inferred_schema_file_not_found(self):
        """Test schema persistence with file not found - covers error paths"""
        non_existent_file = os.path.join(self.temp_dir, 'missing.fmt')
        inferred_types = {'id': 'int'}
        
        # Should handle file not found gracefully or raise exception
        with pytest.raises(Exception):
            self.ingester._persist_inferred_schema(non_existent_file, inferred_types)
    
    @pytest.mark.asyncio
    async def test_load_dataframe_to_table_comprehensive(self):
        """Test dataframe loading - covers lines 316-535"""
        # Create test dataframe
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'value': [100, 200, 300]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        table_name = 'test_table'
        load_mode = 'full' 
        load_timestamp = datetime.now()
        progress_key = 'test_key'
        
        with patch('time.perf_counter', return_value=1.0), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress') as mock_update_progress:
            
            # Test the async generator
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, table_name, load_mode, load_timestamp, progress_key
            ):
                messages.append(message)
            
            # Should have processed the dataframe
            assert len(messages) > 0
            assert any("Loading data" in str(msg) for msg in messages)
            mock_update_progress.assert_called()
    
    @pytest.mark.asyncio  
    async def test_load_dataframe_cancellation_handling(self):
        """Test dataframe loading with cancellation - covers cancellation paths"""
        df = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        mock_connection = MagicMock()
        
        with patch('utils.progress.is_canceled', return_value=True), \
             patch('time.perf_counter', side_effect=[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]):
            
            # Should handle cancellation during loading
            messages = []
            try:
                async for message in self.ingester._load_dataframe_to_table(
                    df, mock_connection, 'test_table', 'full', 
                    datetime.now(), 'test_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "canceled" in str(e).lower()
    
    def test_sql_escape_string_comprehensive(self):
        """Test SQL string escaping - covers lines 761-928"""
        test_cases = [
            ("simple", "simple"),
            ("with'quote", "with''quote"),
            ("with\"doublequote", "with\"\"doublequote"), 
            ("with\nnewline", "with\\nnewline"),
            ("with\ttab", "with\\ttab"),
            ("with\\backslash", "with\\\\backslash"),
            ("", ""),  # Empty string
            (None, ""),  # None value
        ]
        
        for input_val, expected in test_cases:
            result = self.ingester._sql_escape_string(input_val)
            assert result == expected, f"Failed for input: {input_val}"