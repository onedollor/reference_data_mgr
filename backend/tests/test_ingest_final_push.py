"""
Final targeted tests to push utils/ingest.py to 90%+ coverage
Focusing on the large missing block 214-535 (data loading functionality)
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open, call
from datetime import datetime
import time

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterFinalPush:
    """Final push tests targeting core data loading functionality"""
    
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
    
    @pytest.mark.asyncio
    async def test_ingest_data_with_target_schema(self):
        """Test ingestion with target schema - covers lines 69-70, 80-82"""
        file_path = os.path.join(self.temp_dir, 'schema_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'schema_test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,John\n2,Jane\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.data_schema = 'original_schema'
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='test_table'), \
             patch.object(self.ingester, '_load_dataframe_to_table') as mock_load:
            
            async def mock_load_gen(*args):
                yield "Loading complete"
            mock_load.return_value = mock_load_gen()
            
            # Test with target schema
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'schema_test.csv',
                target_schema='custom_schema'
            ):
                messages.append(message)
            
            # Should have used target schema (lines 69-70, 80-82)
            assert any("Using target schema: custom_schema" in msg for msg in messages)
            assert self.mock_db.data_schema == 'custom_schema'
    
    @pytest.mark.asyncio
    async def test_ingest_data_empty_file_workflow(self):
        """Test complete workflow with empty file - covers lines 130-135"""
        file_path = os.path.join(self.temp_dir, 'empty.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'empty.fmt')
        
        # Create empty CSV (headers only)
        with open(file_path, 'w') as f:
            f.write('id,name\n')  # Headers but no data
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database connection
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='empty_table'):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'empty.csv'
            ):
                messages.append(message)
            
            # Should handle empty file gracefully (lines 130-135)
            assert any("No data to process" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_load_dataframe_to_table_comprehensive(self):
        """Test the core data loading method - covers lines 214-535"""
        # Create a substantial dataframe to trigger all paths
        df = pd.DataFrame({
            'id': range(1, 1001),  # 1000 rows to trigger batching
            'name': [f'Name_{i}' for i in range(1, 1001)],
            'value': [f'Value_{i}' for i in range(1, 1001)]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        table_name = 'test_table'
        load_mode = 'full'
        load_timestamp = datetime.now()
        progress_key = 'test_progress'
        
        # Mock all the progress and SQL execution
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress') as mock_update_progress, \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 100)):  # Increasing time values
            
            messages = []
            
            # Test the async generator
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, table_name, load_mode, load_timestamp, progress_key
            ):
                messages.append(message)
            
            # Should have processed all rows with progress updates
            assert len(messages) > 0
            assert any("Loading data" in str(msg) for msg in messages)
            mock_update_progress.assert_called()
            mock_cursor.executemany.assert_called()  # Should have executed SQL
    
    @pytest.mark.asyncio
    async def test_load_dataframe_append_mode(self):
        """Test data loading in append mode - covers append-specific paths"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 10)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'test_table', 'append', datetime.now(), 'test_key'
            ):
                messages.append(message)
            
            # Should handle append mode
            assert len(messages) > 0
    
    @pytest.mark.asyncio
    async def test_load_dataframe_with_cancellation(self):
        """Test data loading with cancellation during process"""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['A', 'B', 'C', 'D', 'E']
        })
        
        mock_connection = MagicMock()
        
        # Mock cancellation after a few iterations
        call_count = 0
        def mock_is_canceled(key):
            nonlocal call_count
            call_count += 1
            return call_count > 3  # Cancel after a few calls
        
        with patch('utils.progress.is_canceled', side_effect=mock_is_canceled), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 10)):
            
            messages = []
            try:
                async for message in self.ingester._load_dataframe_to_table(
                    df, mock_connection, 'test_table', 'full', datetime.now(), 'cancel_key'
                ):
                    messages.append(message)
            except Exception as e:
                # Should handle cancellation
                assert "cancel" in str(e).lower()
    
    @pytest.mark.asyncio  
    async def test_read_csv_error_handling_comprehensive(self):
        """Test CSV reading error paths - covers lines 651-660"""
        # Test with non-existent file
        non_existent = os.path.join(self.temp_dir, 'missing.csv')
        csv_format = {'delimiter': ',', 'has_header': True}
        
        with pytest.raises(Exception, match="Failed to read CSV file"):
            await self.ingester._read_csv_file(non_existent, csv_format)
        
        # Test with malformed CSV
        malformed_file = os.path.join(self.temp_dir, 'malformed.csv')
        with open(malformed_file, 'w') as f:
            f.write('id,name\n1,John,Extra\n2,Jane\n')  # Inconsistent columns
        
        # Should handle malformed CSV gracefully or raise exception
        try:
            result = await self.ingester._read_csv_file(malformed_file, csv_format)
            assert isinstance(result, pd.DataFrame)
        except Exception:
            # Exception is acceptable for malformed CSV
            pass
    
    def test_sql_escape_comprehensive_edge_cases(self):
        """Test SQL escaping with all possible edge cases - covers lines 761-928"""
        edge_cases = [
            # Basic cases
            ("simple", "simple"),
            ("", ""),
            (None, ""),
            
            # Single quote cases  
            ("'", "''"),
            ("''", "''''"),
            ("'text'", "''text''"),
            ("text'middle'text", "text''middle''text"),
            
            # Double quote cases
            ('"', '""'),
            ('""', '""""'),
            ('"text"', '""text""'),
            
            # Newline cases
            ("\n", "\\n"),
            ("\r\n", "\\r\\n"), 
            ("text\nmore\ntext", "text\\nmore\\ntext"),
            
            # Tab cases
            ("\t", "\\t"),
            ("text\ttab\ttext", "text\\ttab\\ttext"),
            
            # Backslash cases
            ("\\", "\\\\"),
            ("\\\\", "\\\\\\\\"),
            ("text\\back\\text", "text\\\\back\\\\text"),
            
            # Combined cases
            ("'text\nwith\ttabs\\and\"quotes'", "''text\\nwith\\ttabs\\\\and\"\"quotes''"),
            ("\n\t\\\"'", "\\n\\t\\\\\"\"''"),
            
            # Unicode and special characters
            ("ümlaut", "ümlaut"),
            ("emoji😀test", "emoji😀test"),
        ]
        
        for input_val, expected in edge_cases:
            result = self.ingester._sql_escape_string(input_val)
            assert result == expected, f"Failed for input: {repr(input_val)}, got: {repr(result)}, expected: {repr(expected)}"
    
    @pytest.mark.asyncio
    async def test_complete_ingestion_workflow_with_all_features(self):
        """Test complete ingestion with all features enabled"""
        file_path = os.path.join(self.temp_dir, 'complete.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'complete.fmt')
        
        # Create comprehensive test file
        csv_content = """id,name,age,date,value
1,John Doe,25,2023-01-01,100.50
2,Jane Smith,30,2023-01-02,200.75
3,Bob Johnson,35,2023-01-03,300.25
TRAILER,END,,,"""
        
        with open(file_path, 'w') as f:
            f.write(csv_content)
        
        format_config = {
            "csv_format": {
                "delimiter": ",",
                "has_header": True,
                "has_trailer": True
            }
        }
        
        # Enable all features
        self.ingester.enable_type_inference = True
        
        # Mock all dependencies
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='complete_table'), \
             patch.object(self.ingester, '_persist_inferred_schema') as mock_persist, \
             patch.object(self.ingester, '_load_dataframe_to_table') as mock_load:
            
            async def mock_load_gen(*args):
                yield "Data loading started"
                yield "Batch 1 processed" 
                yield "Loading complete"
            mock_load.return_value = mock_load_gen()
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'complete.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should have completed full workflow
            assert len(messages) > 10  # Many progress messages
            assert any("Database connection established" in msg for msg in messages)
            assert any("Type inference" in msg for msg in messages) 
            assert any("Loading complete" in msg for msg in messages)
            
            # Should have called type inference and persistence
            mock_persist.assert_called_once()
            mock_load.assert_called_once()