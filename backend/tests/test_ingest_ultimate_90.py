"""
ULTIMATE test to achieve exactly 90% coverage for utils/ingest.py
Targeting all remaining missing edge cases and error handling paths
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock, mock_open
from utils.ingest import DataIngester


class TestIngestUltimate90:
    """Ultimate comprehensive tests targeting 90% coverage"""
    
    def setup_method(self):
        """Setup test environment"""
        self.mock_db_manager = MagicMock()
        self.mock_logger = MagicMock()
        
        # Make logger methods async compatible  
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.mock_logger.log_warning = AsyncMock()
        
        # Setup database manager methods
        self.mock_db_manager.get_connection.return_value = MagicMock()
        self.mock_db_manager.ensure_schemas_exist = MagicMock()
        self.mock_db_manager.data_schema = "dbo"
        self.mock_db_manager.stage_schema = "stage"
        
        self.ingester = DataIngester(self.mock_db_manager, self.mock_logger)

    @pytest.mark.asyncio
    async def test_complete_workflow_with_backup_and_archiving(self):
        """Complete workflow including backup operations and file archiving"""
        
        # File handler setup
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {
                "column_delimiter": ",",
                "text_qualifier": '"',
                "has_header": True,
                "has_trailer": False
            }
        })
        self.ingester.file_handler.move_to_archive = MagicMock(return_value="/archive/test.csv")
        
        # CSV data
        mock_df = pd.DataFrame({
            'name': ['John', 'Jane'],
            'age': ['30', '25'], 
            'city': ['NYC', 'LA']
        })
        
        # Full database setup
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 100
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_table = AsyncMock()
        self.mock_db_manager.backup_existing_data = MagicMock(return_value=150)
        
        # Table preparation mocks
        metadata_actions = {
            'added': [{'column': 'load_timestamp', 'type': 'datetime2'}]
        }
        self.mock_db_manager.ensure_metadata_columns = MagicMock(return_value=metadata_actions)
        
        column_sync_actions = {
            'added': [{'column': 'new_col', 'type': 'varchar(100)'}],
            'mismatched': [{'column': 'age', 'existing_type': 'int', 'file_type': 'varchar(10)'}]
        }
        self.mock_db_manager.sync_main_table_columns = MagicMock(return_value=column_sync_actions)
        self.mock_db_manager.truncate_table = MagicMock()
        
        # Stage loading and validation
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
            
            # Table columns for data transfer
            main_columns = [
                {'name': 'name', 'type': 'varchar(100)'},
                {'name': 'age', 'type': 'varchar(10)'},
                {'name': 'city', 'type': 'varchar(100)'},
                {'name': 'load_timestamp', 'type': 'datetime2'}
            ]
            stage_columns = [
                {'name': 'name', 'type': 'varchar(100)'},
                {'name': 'age', 'type': 'varchar(10)'},
                {'name': 'city', 'type': 'varchar(100)'}
            ]
            
            def mock_get_table_columns(connection, table_name, schema):
                if 'stage' in table_name:
                    return stage_columns
                else:
                    return main_columns
                    
            self.mock_db_manager.get_table_columns.side_effect = mock_get_table_columns
            
            # Connection and cursor
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 2
            mock_connection.cursor.return_value = mock_cursor
            self.mock_db_manager.get_connection.return_value = mock_connection
            
            # Time sequence
            time_calls = 0
            def mock_time_sequence():
                nonlocal time_calls
                result = time_calls * 0.5
                time_calls += 1
                return result
            
            with patch('builtins.open', mock_open(read_data="csv_content")), \
                 patch('pandas.read_csv', return_value=mock_df), \
                 patch('utils.progress.init_progress'), \
                 patch('utils.progress.is_canceled', return_value=False), \
                 patch('utils.progress.update_progress'), \
                 patch('utils.progress.mark_done'), \
                 patch('time.perf_counter', side_effect=mock_time_sequence):
                
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/data.csv",
                    fmt_file_path="/test/format.json",
                    load_mode="full",
                    filename="test_data.csv"
                ):
                    results.append(message)
        
        # Verify complete workflow
        assert len(results) > 30
        assert any("Data validation passed" in msg for msg in results)
        assert any("Data successfully loaded to main table" in msg for msg in results)
        assert any("Backup table" in msg for msg in results)
        assert any("File archived" in msg for msg in results)

    @pytest.mark.asyncio
    async def test_csv_processing_edge_cases(self):
        """Test CSV processing with trailers and various edge cases"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {
                "column_delimiter": ",",
                "text_qualifier": '"',
                "has_header": True,
                "has_trailer": True  # Test trailer processing
            }
        })
        
        # CSV with trailer
        mock_df = pd.DataFrame({
            'name': ['John', 'Jane', 'TOTAL_ROWS:2'],  # Trailer row
            'age': ['30', '25', ''],
            'city': ['NYC', 'LA', '']
        })
        
        self.mock_db_manager.table_exists.return_value = False
        self.mock_db_manager.create_table = MagicMock()
        
        # Time sequence
        time_calls = 0
        def mock_time_sequence():
            nonlocal time_calls
            result = time_calls * 0.5
            time_calls += 1
            return result
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=mock_time_sequence):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json", 
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify trailer processing
        assert any("trailer" in msg.lower() for msg in results)
        assert any("Trailer row removed" in msg for msg in results)

    @pytest.mark.asyncio
    async def test_progress_cancellation_scenarios(self):
        """Test progress cancellation at various points in workflow"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 50
        
        # Time sequence
        time_calls = 0
        def mock_time_sequence():
            nonlocal time_calls
            result = time_calls * 0.5
            time_calls += 1
            return result
        
        # Simulate cancellation after CSV loading
        cancel_calls = 0
        def mock_is_canceled(progress_key):
            nonlocal cancel_calls
            cancel_calls += 1
            return cancel_calls > 5  # Cancel after a few calls
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', side_effect=mock_is_canceled), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=mock_time_sequence):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify cancellation handling
        assert any("Cancellation requested" in msg for msg in results)
        assert any("Ingestion canceled" in msg for msg in results)

    def test_header_processing_comprehensive(self):
        """Comprehensive header processing with all edge cases"""
        
        # Test with completely invalid headers
        invalid_headers = ['', '123', '@#$', '   ', 'normal_col']
        sanitized = self.ingester._sanitize_headers(invalid_headers)
        
        # Should create valid column names
        assert len(sanitized) == 5
        assert 'normal_col' in sanitized
        
        # Test deduplication with many duplicates
        duplicate_headers = ['col1', 'col1', 'col1', 'col2', 'col2', 'col1']
        deduplicated = self.ingester._deduplicate_headers(duplicate_headers)
        
        assert len(deduplicated) == 6
        assert deduplicated.count('col1') == 1
        assert 'col1_1' in deduplicated
        assert 'col1_2' in deduplicated
        assert 'col1_3' in deduplicated

    @pytest.mark.asyncio
    async def test_error_handling_comprehensive(self):
        """Test comprehensive error handling scenarios"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        # Mock pandas to raise an exception
        with patch('pandas.read_csv', side_effect=Exception("CSV read error")), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify error handling
        assert any("ERROR!" in msg for msg in results)
        assert any("Data ingestion failed" in msg for msg in results)

    @pytest.mark.asyncio
    async def test_stage_loading_failure_handling(self):
        """Test stage loading failure scenarios"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 50
        self.mock_db_manager.create_table = MagicMock()
        
        # Mock stage loading to FAIL
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = False  # Stage loading fails
            
            # Time sequence
            time_calls = 0
            def mock_time_sequence():
                nonlocal time_calls
                result = time_calls * 0.5
                time_calls += 1
                return result
            
            with patch('pandas.read_csv', return_value=mock_df), \
                 patch('utils.progress.init_progress'), \
                 patch('utils.progress.is_canceled', return_value=False), \
                 patch('utils.progress.update_progress'), \
                 patch('time.perf_counter', side_effect=mock_time_sequence):
                
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/data.csv",
                    fmt_file_path="/test/format.json",
                    load_mode="full",
                    filename="test_data.csv"
                ):
                    results.append(message)
        
        # Verify stage loading failure handling
        assert any("Failed to load data to stage table" in msg for msg in results)
        assert any("ERROR!" in msg for msg in results)

    def test_empty_dataframe_handling(self):
        """Test handling of empty dataframes"""
        
        # Test with empty dataframe
        empty_df = pd.DataFrame()
        
        # Should not crash
        sanitized = self.ingester._sanitize_headers([])
        deduplicated = self.ingester._deduplicate_headers([])
        
        assert sanitized == []
        assert deduplicated == []
        
        # Test with dataframe that has columns but no data
        empty_with_cols = pd.DataFrame(columns=['name', 'age'])
        columns = ['name', 'age']
        result = self.ingester._infer_types(empty_with_cols, columns)
        
        assert len(result) == 2
        assert 'varchar' in result['name']

    @pytest.mark.asyncio
    async def test_table_schema_sync_edge_cases(self):
        """Test table schema synchronization edge cases"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({
            'name': ['John'], 
            'completely_new_col': ['data'],  # New column
            'another_new': ['more_data']     # Another new column
        })
        
        # Existing table
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 500
        
        # Mock extensive metadata operations
        metadata_actions = {
            'added': [
                {'column': 'load_timestamp', 'type': 'datetime2'},
                {'column': 'data_source', 'type': 'varchar(100)'},
                {'column': 'batch_id', 'type': 'varchar(50)'}
            ]
        }
        self.mock_db_manager.ensure_metadata_columns = MagicMock(return_value=metadata_actions)
        
        # Mock extensive column sync
        column_sync_actions = {
            'added': [
                {'column': 'completely_new_col', 'type': 'varchar(100)'},
                {'column': 'another_new', 'type': 'varchar(100)'}
            ],
            'mismatched': [
                {'column': 'name', 'existing_type': 'varchar(50)', 'file_type': 'varchar(100)'}
            ]
        }
        self.mock_db_manager.sync_main_table_columns = MagicMock(return_value=column_sync_actions)
        
        # Time sequence
        time_calls = 0
        def mock_time_sequence():
            nonlocal time_calls
            result = time_calls * 0.5
            time_calls += 1
            return result
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=mock_time_sequence):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify extensive schema sync messages
        assert any("Added missing metadata columns" in msg and "3" in msg for msg in results)
        assert any("Added 2 missing columns from input file" in msg for msg in results)
        assert any("1 columns have different types" in msg for msg in results)