"""
BREAKTHROUGH test to achieve 90% coverage for utils/ingest.py
Focus: Properly mock ALL dependencies to reach validation and data transfer sections
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock, mock_open, call
from utils.ingest import DataIngester
import time


class TestIngestBreakthrough90:
    """Breakthrough test to hit 90% coverage targeting lines 372-535"""
    
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
    async def test_complete_workflow_with_validation_and_transfer(self):
        """
        BREAKTHROUGH: Target lines 372-535 (163+ lines) - Complete validation + data transfer
        This test MUST execute the full workflow to hit the missing coverage
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
            'name': ['John', 'Jane', 'Bob'],
            'age': ['30', '25', '35'],
            'city': ['NYC', 'LA', 'CHI']
        })
        
        # COMPLETE database setup for full workflow
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 100
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_table = AsyncMock()
        
        # CRITICAL: Mock the stage loading method to return True
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            
            # Mock validation SUCCESS (this triggers lines 385-386)
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
            
            # Mock table columns for data transfer (lines 412-448)
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
            
            # Mock connection and cursor for data transfer (lines 458-464)
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 3  # 3 rows transferred
            mock_connection.cursor.return_value = mock_cursor
            self.mock_db_manager.get_connection.return_value = mock_connection
            
            # CRITICAL: Proper time sequence to avoid division by zero
            time_calls = 0
            def mock_time_sequence():
                nonlocal time_calls
                result = time_calls * 0.5  # 0.0, 0.5, 1.0, 1.5, 2.0, etc.
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
                    print(f"BREAKTHROUGH: {message}")
        
        # Verify we hit the critical workflow sections
        print(f"BREAKTHROUGH: Total messages: {len(results)}")
        
        # Verify key workflow messages
        assert any("CSV file loaded: 3 rows" in msg for msg in results), "Should load CSV"
        assert any("Executing data validation" in msg for msg in results), "Should execute validation (line 365)"
        assert any("Data validation passed" in msg for msg in results), "Should pass validation (line 385)"
        assert any("Moving data from stage to main table" in msg for msg in results), "Should start data transfer (line 407)"
        assert any("Transferring 3 matching columns" in msg for msg in results), "Should transfer columns (line 457)"
        assert any("Data successfully loaded to main table: 3 rows" in msg for msg in results), "Should complete transfer (line 463)"
        
        # Verify critical database calls were made
        self.mock_db_manager.execute_validation_procedure.assert_called_once()
        self.mock_db_manager.get_table_columns.assert_called()
        mock_cursor.execute.assert_called()
        
        print("BREAKTHROUGH: Test completed successfully!")

    @pytest.mark.asyncio 
    async def test_validation_failure_workflow(self):
        """
        TARGET: Lines 372-383 (12 lines) - Validation failure path
        """
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['invalid']})
        
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 50
        
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            
            # VALIDATION FAILURE
            validation_result = {
                "validation_result": 2,  # 2 issues
                "validation_issue_list": [
                    {"issue_id": "VAL001", "issue_detail": "Invalid data type"},
                    {"issue_id": "VAL002", "issue_detail": "Missing required field"}
                ]
            }
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value=validation_result)
            
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
                 patch('utils.progress.request_cancel') as mock_cancel, \
                 patch('time.perf_counter', side_effect=mock_time_sequence):
                
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/data.csv",
                    fmt_file_path="/test/format.json",
                    load_mode="full",
                    filename="test_data.csv"
                ):
                    results.append(message)
        
        # Verify validation failure handling
        assert any("ERROR! Validation failed with 2 issues:" in msg for msg in results)
        assert any("Issue VAL001:" in msg for msg in results) 
        assert any("Issue VAL002:" in msg for msg in results)
        assert any("Data remains in stage table for review" in msg for msg in results)
        mock_cancel.assert_called()

    @pytest.mark.asyncio
    async def test_full_load_table_preparation(self):
        """
        TARGET: Lines 244-271 (28 lines) - fullload table preparation
        """
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({
            'name': ['John'], 
            'new_col': ['data']  # This should trigger column addition
        })
        
        # Existing table with data (triggers fullload preparation)
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 1500
        
        # Mock metadata operations
        metadata_actions = {
            'added': [{'column': 'load_timestamp', 'type': 'datetime2'}]
        }
        self.mock_db_manager.ensure_metadata_columns = MagicMock(return_value=metadata_actions)
        
        # Mock column sync
        column_sync_actions = {
            'added': [{'column': 'new_col', 'type': 'varchar(100)'}]
        }
        self.mock_db_manager.sync_main_table_columns = MagicMock(return_value=column_sync_actions)
        self.mock_db_manager.truncate_table = MagicMock()
        
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
        
        # Verify table preparation messages
        assert any("fullload mode: preserving existing main table structure" in msg for msg in results)
        assert any("Added missing metadata columns" in msg for msg in results)
        assert any("Added 1 missing columns from input file" in msg for msg in results)
        assert any("fullload mode: truncating 1500 existing rows" in msg for msg in results)
        
        # Verify database operations
        self.mock_db_manager.ensure_metadata_columns.assert_called()
        self.mock_db_manager.sync_main_table_columns.assert_called()
        self.mock_db_manager.truncate_table.assert_called()