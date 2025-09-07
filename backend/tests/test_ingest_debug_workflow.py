"""
Debug test to understand exactly where the ingest workflow is stopping
and why we're not hitting the target lines 372-535
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock, mock_open
from utils.ingest import DataIngester


class TestIngestDebugWorkflow:
    """Debug the complete workflow to understand execution path"""
    
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
    async def test_debug_complete_workflow(self):
        """Debug complete workflow to see exact execution path"""
        
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
        
        # Mock database operations - COMPLETE SETUP
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_table_row_count.return_value = 100
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_table = AsyncMock()
        
        # Mock the stage table loading method (this is crucial!)
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_load_stage:
            mock_load_stage.return_value = True  # Stage loading succeeds
            
            # Mock validation - SUCCESS
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
            
            # Mock final data transfer components
            main_columns = [{'name': 'name'}, {'name': 'age'}, {'name': 'city'}]
            stage_columns = [{'name': 'name'}, {'name': 'age'}, {'name': 'city'}] 
            
            def mock_get_table_columns(connection, table_name, schema):
                if 'stage' in table_name:
                    return stage_columns
                else:
                    return main_columns
                    
            self.mock_db_manager.get_table_columns.side_effect = mock_get_table_columns
            
            # Mock connection and cursor for final data transfer
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 2
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
                    print(f"DEBUG: {message}")  # Print each message to see workflow progress
        
        print(f"DEBUG: Total messages: {len(results)}")
        print("DEBUG: All messages:")
        for i, msg in enumerate(results):
            print(f"{i+1}: {msg}")
        
        # This is just for debugging - let's see what we get
        assert len(results) > 0