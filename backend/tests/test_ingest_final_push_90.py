"""
FINAL PUSH to achieve exactly 90% coverage for utils/ingest.py
Combined most effective scenarios identified from breakthrough testing
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock, mock_open
from utils.ingest import DataIngester


class TestIngestFinalPush90:
    """Final push tests targeting exactly 90% coverage"""
    
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
    async def test_complete_full_workflow_success(self):
        """Complete full workflow with validation and data transfer - HIGH COVERAGE"""
        
        # Setup file handler
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {
                "column_delimiter": ",",
                "text_qualifier": '"',
                "has_header": True,
                "has_trailer": False
            }
        })
        
        # CSV data
        mock_df = pd.DataFrame({
            'name': ['John', 'Jane', 'Bob'],
            'age': ['30', '25', '35'],
            'city': ['NYC', 'LA', 'CHI']
        })
        
        # Database setup
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 100
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_table = AsyncMock()
        self.mock_db_manager.backup_existing_data = MagicMock(return_value=50)
        
        # Stage loading
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            
            # Validation success
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
            
            # Table columns
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
            mock_cursor.rowcount = 3
            mock_connection.cursor.return_value = mock_cursor
            self.mock_db_manager.get_connection.return_value = mock_connection
            
            # Mock file archiving to avoid errors
            self.ingester.file_handler.move_to_archive = MagicMock(return_value="/archive/test.csv")
            
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
        
        # Verify key workflow steps
        assert any("Data validation passed" in msg for msg in results)
        assert any("Data successfully loaded to main table: 3 rows" in msg for msg in results)
        assert len(results) > 30

    @pytest.mark.asyncio
    async def test_append_mode_workflow(self):
        """Test append mode workflow - TARGET specific missing lines"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        # Append mode setup
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 500
        self.mock_db_manager.create_table = MagicMock()
        
        # Stage loading
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            
            # Validation success
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
            
            # Table columns for append
            main_columns = [{'name': 'name', 'type': 'varchar(100)'}, {'name': 'age', 'type': 'varchar(10)'}]
            stage_columns = [{'name': 'name', 'type': 'varchar(100)'}, {'name': 'age', 'type': 'varchar(10)'}]
            
            def mock_get_table_columns(connection, table_name, schema):
                if 'stage' in table_name:
                    return stage_columns
                else:
                    return main_columns
                    
            self.mock_db_manager.get_table_columns.side_effect = mock_get_table_columns
            
            # Connection
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 1
            mock_connection.cursor.return_value = mock_cursor
            self.mock_db_manager.get_connection.return_value = mock_connection
            
            # Mock archiving
            self.ingester.file_handler.move_to_archive = MagicMock(return_value="/archive/test.csv")
            
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
                 patch('utils.progress.mark_done'), \
                 patch('time.perf_counter', side_effect=mock_time_sequence):
                
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/data.csv",
                    fmt_file_path="/test/format.json",
                    load_mode="append",
                    filename="test_data.csv"
                ):
                    results.append(message)
        
        # Verify append mode
        assert any("append mode" in msg.lower() for msg in results)
        assert any("main table already has 500 rows" in msg for msg in results)

    @pytest.mark.asyncio
    async def test_validation_failure_with_issues(self):
        """Test validation failure with detailed issues - TARGET lines 372-383"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="test_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['invalid']})
        
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 50
        
        # Stage loading success
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            
            # VALIDATION FAILURE with issues
            validation_result = {
                "validation_result": 2,
                "validation_issue_list": [
                    {"issue_id": "VAL001", "issue_detail": "Invalid data type"},
                    {"issue_id": "VAL002", "issue_detail": "Missing field"}
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
        assert any("Data remains in stage table for review" in msg for msg in results)
        mock_cancel.assert_called()

    def test_header_sanitization_edge_cases(self):
        """Test header sanitization with various edge cases - TARGET missing utility lines"""
        
        # Test invalid headers that should be sanitized
        invalid_headers = ['', '123starts_with_num', '   ', 'valid_header', 'special@chars']
        result = self.ingester._sanitize_headers(invalid_headers)
        
        # Should sanitize problematic headers
        assert 'valid_header' in result
        assert any('col_123starts_with_num' in h for h in result)
        
        # Test deduplication
        duplicate_headers = ['name', 'age', 'name', 'city', 'age']
        dedupe_result = self.ingester._deduplicate_headers(duplicate_headers)
        
        # Should handle duplicates
        assert len(dedupe_result) == 5
        assert dedupe_result.count('name') == 1

    def test_csv_type_inference_edge_cases(self):
        """Test CSV type inference with various data types - TARGET missing type inference lines"""
        
        # Test dataframe with various data types
        df = pd.DataFrame({
            'int_col': [1, 2, 3],
            'float_col': [1.5, 2.5, 3.5],
            'str_col': ['a', 'b', 'c'],
            'mixed_col': [1, 'text', 3.5],
            'date_col': ['2023-01-01', '2023-01-02', '2023-01-03']
        })
        
        # Test type inference
        columns = ['int_col', 'float_col', 'str_col', 'mixed_col', 'date_col']
        result = self.ingester._infer_types(df, columns)
        
        # Should infer appropriate SQL types (all as varchar with different lengths)
        assert 'varchar' in result['int_col'].lower()
        assert 'varchar' in result['str_col'].lower()
        assert len(result) == 5

    @pytest.mark.asyncio  
    async def test_new_table_creation_workflow(self):
        """Test new table creation workflow - TARGET table creation lines"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="new_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        mock_df = pd.DataFrame({'name': ['John'], 'age': ['30']})
        
        # New table scenario (no existing table)
        self.mock_db_manager.table_exists.return_value = False
        self.mock_db_manager.create_table = MagicMock()
        
        # Mock stage loading and validation
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
        
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
        
        # Should handle new table creation
        print("DEBUG new table messages:", results[-10:])  # Debug recent messages
        assert any("Creating" in msg and "table" in msg for msg in results)
        self.mock_db_manager.create_table.assert_called()