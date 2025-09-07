"""
Specialized tests targeting the core data loading block (lines 214-535) in utils/ingest.py
This is the main table management and data loading functionality
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, call
from datetime import datetime

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterCoreLoading:
    """Tests targeting the core data loading functionality (lines 214-535)"""
    
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
    async def test_load_dataframe_full_mode_new_table(self):
        """Test full load mode with new table creation - covers lines 214-280"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'value': [100, 200, 300]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock database responses
        self.mock_db.table_exists.return_value = False  # New table
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.create_table.return_value = None
        
        table_name = 'new_table'
        load_mode = 'full'
        load_timestamp = datetime.now()
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, table_name, load_mode, load_timestamp, 'test_key'
            ):
                messages.append(message)
            
            # Should handle new table creation in full mode
            assert len(messages) > 0
            self.mock_db.table_exists.assert_called()
            self.mock_db.create_table.assert_called()
    
    @pytest.mark.asyncio
    async def test_load_dataframe_full_mode_existing_table(self):
        """Test full load mode with existing table - covers backup paths"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C']
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock existing table with data
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 100  # Existing rows
        self.mock_db.create_backup_table.return_value = None
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'existing_table', 'full', datetime.now(), 'test_key'
            ):
                messages.append(message)
            
            # Should handle existing table with backup
            assert any("Found 100 existing rows that will be backed up" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_load_dataframe_append_mode(self):
        """Test append mode - covers lines 215-217"""
        df = pd.DataFrame({
            'id': [4, 5, 6],
            'name': ['D', 'E', 'F']
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock existing table for append
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 50  # Existing rows
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'append_table', 'append', datetime.now(), 'test_key'
            ):
                messages.append(message)
            
            # Should handle append mode correctly
            assert any("Append mode: main table already has 50 rows" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_load_dataframe_cancellation_during_table_ops(self):
        """Test cancellation during table operations - covers lines 220-222, 234-236"""
        df = pd.DataFrame({'id': [1], 'name': ['Test']})
        
        mock_connection = MagicMock()
        
        # Mock cancellation during table operations
        call_count = 0
        def mock_is_canceled(key):
            nonlocal call_count
            call_count += 1
            # Cancel on the call that happens during table operations
            return call_count >= 2  # Cancel during table ops
        
        with patch('utils.progress.is_canceled', side_effect=mock_is_canceled), \
             patch('time.perf_counter', side_effect=range(1, 20)):
            
            messages = []
            try:
                async for message in self.ingester._load_dataframe_to_table(
                    df, mock_connection, 'cancel_table', 'full', datetime.now(), 'cancel_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should have reached one of the cancellation points
            assert any("Cancellation requested - stopping" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_load_dataframe_stage_table_operations(self):
        """Test stage table creation and operations - covers stage table paths"""
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['A', 'B', 'C', 'D', 'E'],
            'category': ['X', 'Y', 'Z', 'X', 'Y']
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock table states
        def mock_table_exists(conn, table_name):
            if 'stage' in table_name:
                return True  # Stage table exists
            return False  # Main table doesn't exist
        
        self.mock_db.table_exists.side_effect = mock_table_exists
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.create_table.return_value = None
        self.mock_db.drop_table_if_exists.return_value = None
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'stage_test_table', 'full', datetime.now(), 'stage_key'
            ):
                messages.append(message)
            
            # Should handle stage table operations
            self.mock_db.table_exists.assert_called()  # Should check for stage table
    
    @pytest.mark.asyncio
    async def test_load_dataframe_with_batch_processing(self):
        """Test batch processing with large dataset"""
        # Create large dataframe to trigger batching (batch_size = 990)
        large_df = pd.DataFrame({
            'id': range(1, 2001),  # 2000 rows > batch_size
            'name': [f'Name_{i}' for i in range(1, 2001)],
            'value': [f'Value_{i}' for i in range(1, 2001)]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock table operations
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress') as mock_update, \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                large_df, mock_connection, 'batch_table', 'full', datetime.now(), 'batch_key'
            ):
                messages.append(message)
            
            # Should have processed multiple batches
            mock_cursor.executemany.assert_called()  # Should execute SQL
            mock_update.assert_called()  # Should update progress multiple times
    
    @pytest.mark.asyncio 
    async def test_load_dataframe_sql_execution_paths(self):
        """Test actual SQL execution paths within the loading method"""
        df = pd.DataFrame({
            'id': [1, 2],
            'name': ['Test1', 'Test2'],
            'special_chars': ["O'Reilly", 'Smith\nJones']  # Test SQL escaping
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock successful table operations
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'sql_table', 'full', datetime.now(), 'sql_key'
            ):
                messages.append(message)
            
            # Should have executed SQL with proper escaping
            mock_cursor.executemany.assert_called()
            
            # Check that SQL was built (get the actual SQL from the call)
            calls = mock_cursor.executemany.call_args_list
            if calls:
                sql, params = calls[0][0]
                assert 'INSERT INTO' in sql
                assert len(params) == len(df)  # Should have params for all rows
    
    @pytest.mark.asyncio
    async def test_load_dataframe_progress_reporting(self):
        """Test detailed progress reporting during loading"""
        df = pd.DataFrame({
            'id': range(1, 101),  # 100 rows
            'data': [f'data_{i}' for i in range(1, 101)]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock table operations
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        progress_calls = []
        def capture_progress(*args, **kwargs):
            progress_calls.append((args, kwargs))
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress', side_effect=capture_progress), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'progress_table', 'full', datetime.now(), 'progress_key'
            ):
                messages.append(message)
            
            # Should have made progress updates
            assert len(progress_calls) > 0
            assert any("Loading" in str(msg) for msg in messages)
    
    @pytest.mark.asyncio
    async def test_load_dataframe_error_handling(self):
        """Test error handling during data loading"""
        df = pd.DataFrame({'id': [1], 'name': ['Test']})
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("SQL execution failed")
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock table operations
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            try:
                async for message in self.ingester._load_dataframe_to_table(
                    df, mock_connection, 'error_table', 'full', datetime.now(), 'error_key'
                ):
                    messages.append(message)
            except Exception as e:
                # Should handle SQL execution errors
                assert "SQL execution failed" in str(e)
            
            # Should have attempted SQL execution
            mock_cursor.executemany.assert_called()
    
    @pytest.mark.asyncio
    async def test_load_dataframe_metadata_columns(self):
        """Test handling of metadata columns during loading"""
        df = pd.DataFrame({
            'business_col1': ['A', 'B'], 
            'business_col2': [1, 2]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        # Mock table operations
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        load_timestamp = datetime(2023, 1, 1, 12, 0, 0)
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('asyncio.sleep', new_callable=AsyncMock), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester._load_dataframe_to_table(
                df, mock_connection, 'metadata_table', 'full', load_timestamp, 'meta_key'
            ):
                messages.append(message)
            
            # Should include metadata columns in SQL
            mock_cursor.executemany.assert_called()
            if mock_cursor.executemany.call_args_list:
                sql, params = mock_cursor.executemany.call_args_list[0][0]
                # SQL should include metadata columns
                assert 'ref_data_loadtime' in sql
                assert 'ref_data_loadtype' in sql