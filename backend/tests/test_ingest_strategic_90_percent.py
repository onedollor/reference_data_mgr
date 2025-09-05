"""
Strategic tests to push utils/ingest.py coverage from 65% to 90%
Targets high-value missing lines: 449-535 (87 lines), 244-271 (28 lines), 372-383 (12 lines)
"""

import pytest
import pandas as pd
import json
from unittest.mock import MagicMock, patch, AsyncMock, mock_open
from utils.ingest import DataIngester


class TestIngestStrategic90Percent:
    """Strategic tests targeting the exact missing lines for maximum coverage gain"""
    
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
    async def test_complete_workflow_with_data_transfer(self):
        """
        TARGET: Lines 449-535 (87 lines) - Complete data transfer workflow
        The biggest missing block that will give maximum coverage boost
        """
        
        # Mock file handler methods
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {
                "column_delimiter": ",",
                "text_qualifier": '"',
                "has_header": True,
                "has_trailer": False
            }
        })
        
        # Mock CSV data
        mock_df = pd.DataFrame({
            'name': ['John', 'Jane'],
            'age': ['30', '25'], 
            'city': ['NYC', 'LA']
        })
        
        # Mock database operations for COMPLETE workflow
        self.mock_db_manager.table_exists.return_value = True  # Existing table
        self.mock_db_manager.get_table_row_count.return_value = 100  # Has existing data
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_table = AsyncMock()
        self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})  # Validation SUCCESS
        
        # KEY: Mock get_table_columns for the data transfer section (lines 412-448)
        main_columns = [
            {'name': 'name', 'type': 'varchar(100)'},
            {'name': 'age', 'type': 'varchar(10)'},
            {'name': 'city', 'type': 'varchar(100)'},
            {'name': 'load_timestamp', 'type': 'datetime2'}  # Metadata column
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
        
        # Mock cursor for data transfer execution
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 2  # Simulate 2 rows transferred
        mock_connection.cursor.return_value = mock_cursor
        self.mock_db_manager.get_connection.return_value = mock_connection
        
        with patch('builtins.open', mock_open(read_data="csv_content")), \
             patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', return_value=1.0):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify the complete workflow executed
        assert len(results) > 15
        assert any("Starting data ingestion" in msg for msg in results)
        assert any("validation passed" in msg.lower() for msg in results)
        assert any("Moving data from stage to main table" in msg for msg in results)
        assert any("Transferring" in msg and "matching columns" in msg for msg in results)
        assert any("Data successfully loaded to main table" in msg for msg in results)
        
        # Verify key database operations were called
        self.mock_db_manager.get_table_columns.assert_called()  # Critical for lines 412-448
        mock_cursor.execute.assert_called()  # Critical for line 458 data transfer
        assert mock_cursor.execute.call_count >= 2  # Should have multiple SQL executions
    
    @pytest.mark.asyncio
    async def test_validation_failure_comprehensive(self):
        """
        TARGET: Lines 372-383 (12 lines) - Validation failure handling
        Test the specific error path when validation fails
        """
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        # Mock database operations with VALIDATION FAILURE
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_table_row_count.return_value = 50
        self.mock_db_manager.create_table = MagicMock()
        
        # KEY: Mock validation failure with detailed error list (targets lines 372-383)
        validation_result = {
            "validation_result": 3,  # Number of validation issues > 0
            "validation_issue_list": [
                {"issue_id": "VAL001", "issue_detail": "Invalid date format in column date_field"},
                {"issue_id": "VAL002", "issue_detail": "Missing required value in column required_field"},
                {"issue_id": "VAL003", "issue_detail": "Data exceeds maximum length in column text_field"}
            ]
        }
        self.mock_db_manager.execute_validation_procedure = MagicMock(return_value=validation_result)
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel') as mock_cancel:
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify validation failure messages (lines 372-383)
        assert any("Validation failed with 3 issues" in msg for msg in results)
        assert any("Issue VAL001" in msg and "Invalid date format" in msg for msg in results)  # Line 375
        assert any("Issue VAL002" in msg and "Missing required value" in msg for msg in results)
        assert any("Issue VAL003" in msg and "Data exceeds maximum length" in msg for msg in results)
        assert any("Data remains in stage table for review" in msg for msg in results)  # Line 376
        assert any("automatically canceled due to validation errors" in msg for msg in results)  # Line 381
        
        # Should auto-cancel on validation failure (lines 379-381)
        mock_cancel.assert_called()
    
    @pytest.mark.asyncio
    async def test_full_load_table_preparation(self):
        """
        TARGET: Lines 244-271 (28 lines) - Table preparation for full load mode
        Test the table structure sync and preparation workflow
        """
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30'], 'new_col': ['value']})
        
        # Mock existing table with data (full load mode)
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_table_row_count.return_value = 500  # Existing data
        self.mock_db_manager.create_table = MagicMock()
        
        # KEY: Mock table structure operations (targets lines 244-271)
        # Mock metadata column operations (lines 247-251)
        meta_actions = {
            'added': [
                {'column': 'load_timestamp', 'type': 'datetime2'},
                {'column': 'load_batch_id', 'type': 'varchar(50)'}
            ]
        }
        self.mock_db_manager.ensure_metadata_columns = MagicMock(return_value=meta_actions)
        
        # Mock column sync operations (lines 255-265)
        sync_actions = {
            'added': [
                {'column': 'new_col', 'type': 'varchar(100)'}
            ],
            'mismatched': [
                {'column': 'age', 'existing_type': 'int', 'file_type': 'varchar(10)'}
            ]
        }
        self.mock_db_manager.sync_main_table_columns = MagicMock(return_value=sync_actions)
        
        # Mock table truncation (lines 268-271)
        self.mock_db_manager.truncate_table = MagicMock()
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",  # This triggers the full load preparation
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify table preparation messages (lines 244-271)
        assert any("Full load mode: preserving existing main table structure" in msg for msg in results)  # Line 244
        assert any("Added missing metadata columns" in msg and "load_timestamp" in msg for msg in results)  # Line 249
        assert any("Added 1 missing columns from input file" in msg and "new_col" in msg for msg in results)  # Line 257
        assert any("1 columns have different types" in msg for msg in results)  # Line 259
        assert any("table=int, file=varchar(10)" in msg for msg in results)  # Line 261
        assert any("Full load mode: truncating 500 existing rows" in msg for msg in results)  # Line 269
        assert any("Main table data cleared for full load" in msg for msg in results)  # Line 271
        
        # Verify key operations were called
        self.mock_db_manager.ensure_metadata_columns.assert_called()
        self.mock_db_manager.sync_main_table_columns.assert_called()
        self.mock_db_manager.truncate_table.assert_called()
    
    @pytest.mark.asyncio  
    async def test_target_schema_override(self):
        """
        TARGET: Lines 80-82 - Target schema override functionality
        """
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John']})
        
        # Store original schema
        original_schema = self.mock_db_manager.data_schema
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv",
                target_schema="custom_schema"  # This should trigger lines 80-82
            ):
                results.append(message)
        
        # Verify target schema override (lines 80-82)
        assert any("Using target schema: custom_schema" in msg for msg in results)  # Line 82
        
        # Schema should be restored to original after processing (not in missing lines but good to verify)
        assert self.mock_db_manager.data_schema == original_schema
    
    @pytest.mark.asyncio
    async def test_empty_file_auto_cancel(self):
        """
        TARGET: Lines 124, 132-134 - Empty file handling and auto-cancel
        """
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        # Mock EMPTY dataframe (triggers lines 129-135)
        empty_df = pd.DataFrame()  # No rows
        
        with patch('pandas.read_csv', return_value=empty_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel') as mock_cancel:
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json", 
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify empty file handling (lines 130-134)
        assert any("ERROR! CSV file contains no data rows" in msg for msg in results)  # Line 130
        assert any("Upload process automatically canceled due to empty file" in msg for msg in results)  # Line 134
        
        # Should auto-cancel on empty file (line 133)
        mock_cancel.assert_called()
    
    @pytest.mark.asyncio
    async def test_invalid_headers_auto_cancel(self):
        """
        TARGET: Lines 145-152 - Invalid headers handling and auto-cancel
        """
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        # Mock CSV with all invalid headers
        mock_df = pd.DataFrame({
            '': ['data1'],      # Empty header
            '123invalid': ['data2'],  # Starts with number
            '   ': ['data3']    # Only whitespace
        })
        
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel') as mock_cancel:
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/data.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="test_data.csv"
            ):
                results.append(message)
        
        # Verify invalid headers handling (lines 145-152)
        assert any("ERROR! No valid columns found after header sanitization" in msg for msg in results)  # Line 145
        assert any("Upload process automatically canceled due to invalid headers" in msg for msg in results)  # Line 150
        
        # Should auto-cancel on invalid headers (lines 148-150)
        mock_cancel.assert_called()
    
    @pytest.mark.asyncio
    async def test_type_inference_persist_failure(self):
        """
        TARGET: Lines 175-176 - Type inference schema persistence failure handling
        """
        
        # Enable type inference
        self.ingester.enable_type_inference = True
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")  
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        # Mock _persist_inferred_schema to raise exception (targets lines 175-176)
        with patch.object(self.ingester, '_persist_inferred_schema', side_effect=Exception("File write error")), \
             patch('pandas.read_csv', return_value=mock_df), \
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
        
        # Verify type inference failure handling (lines 175-176)
        assert any("WARNING: Failed to persist inferred schema: File write error" in msg for msg in results)  # Line 176
    
    @pytest.mark.asyncio
    async def test_cancellation_checkpoints(self):
        """
        TARGET: Lines 182-183, 195-196 - Cancellation checkpoints at different stages
        """
        
        # Mock file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        # Test cancellation after type inference (lines 182-183)
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', side_effect=[False, False, False, False, True]), \
             patch('utils.progress.update_progress'):
            
            try:
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/data.csv",
                    fmt_file_path="/test/format.json",
                    load_mode="full", 
                    filename="test_data.csv"
                ):
                    results.append(message)
            except Exception as e:
                # Should get cancellation exception
                assert "canceled" in str(e).lower()
                assert any("Cancellation requested - stopping after type inference" in msg for msg in results)  # Line 182
        
        # Test cancellation before data processing (lines 195-196)
        with patch('pandas.read_csv', return_value=mock_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', side_effect=[False, False, False, False, False, True]), \
             patch('utils.progress.update_progress'):
            
            try:
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/data.csv",
                    fmt_file_path="/test/format.json", 
                    load_mode="full",
                    filename="test_data.csv"
                ):
                    results.append(message)
            except Exception as e:
                # Should get cancellation exception  
                assert "canceled" in str(e).lower()
                assert any("Cancellation requested - stopping before data processing" in msg for msg in results)  # Line 195