"""
FINAL 90% PUSH - Target the remaining 88 lines needed for 90% coverage
Current: 74% (409/551) | Target: 90% (496/551) | Need: 87 more lines
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock, mock_open, call
from utils.ingest import DataIngester


class TestIngestFinal90Percent:
    """Final focused tests to achieve exactly 90% coverage"""
    
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
    async def test_complete_workflow_all_paths(self):
        """Ultimate comprehensive workflow hitting maximum code paths"""
        
        # File handler setup
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="comprehensive_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {
                "column_delimiter": ",",
                "text_qualifier": '"',
                "has_header": True,
                "has_trailer": True,  # Trailer processing
                "quote_all_fields": True
            }
        })
        self.ingester.file_handler.move_to_archive = MagicMock(return_value="/archive/comprehensive.csv")
        
        # Large CSV data to trigger various processing paths
        mock_df = pd.DataFrame({
            'id': range(1, 101),  # 100 rows for performance stats
            'name': [f'Person_{i}' for i in range(1, 101)],
            'email': [f'person{i}@example.com' for i in range(1, 101)],
            'age': [str(20 + i % 50) for i in range(1, 101)],
            'city': [f'City_{i%10}' for i in range(1, 101)],
            'salary': [str(30000 + i * 1000) for i in range(1, 101)],
            'department': [f'Dept_{i%5}' for i in range(1, 101)],
            'TRAILER': [''] * 100  # Last row will be trailer
        })
        
        # Set trailer row
        mock_df.iloc[-1] = ['TOTAL', '', '', '', '', '', '', 'TRAILER_ROW']
        
        # Full database setup for complete workflow
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 500
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_table = AsyncMock()
        self.mock_db_manager.backup_existing_data = MagicMock(return_value=500)
        
        # Comprehensive table preparation - trigger ALL schema sync paths
        metadata_actions = {
            'added': [
                {'column': 'load_timestamp', 'type': 'datetime2'},
                {'column': 'data_source', 'type': 'varchar(255)'},
                {'column': 'batch_id', 'type': 'varchar(100)'},
                {'column': 'processed_date', 'type': 'date'},
                {'column': 'version', 'type': 'int'}
            ]
        }
        self.mock_db_manager.ensure_metadata_columns = MagicMock(return_value=metadata_actions)
        
        column_sync_actions = {
            'added': [
                {'column': 'email', 'type': 'varchar(255)'},
                {'column': 'salary', 'type': 'varchar(50)'},
                {'column': 'department', 'type': 'varchar(100)'}
            ],
            'mismatched': [
                {'column': 'age', 'existing_type': 'int', 'file_type': 'varchar(10)'},
                {'column': 'name', 'existing_type': 'varchar(50)', 'file_type': 'varchar(100)'}
            ]
        }
        self.mock_db_manager.sync_main_table_columns = MagicMock(return_value=column_sync_actions)
        self.mock_db_manager.truncate_table = MagicMock()
        
        # Stage and validation setup
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            
            # Validation with warnings (but success)
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={
                "validation_result": 0,
                "validation_warnings": 3  # Some warnings but not failures
            })
            
            # Complex table columns for comprehensive data transfer
            main_columns = [
                {'name': 'id', 'type': 'bigint'},
                {'name': 'name', 'type': 'varchar(100)'},
                {'name': 'email', 'type': 'varchar(255)'},
                {'name': 'age', 'type': 'varchar(10)'},
                {'name': 'city', 'type': 'varchar(100)'},
                {'name': 'salary', 'type': 'varchar(50)'},
                {'name': 'department', 'type': 'varchar(100)'},
                {'name': 'load_timestamp', 'type': 'datetime2'},
                {'name': 'data_source', 'type': 'varchar(255)'},
                {'name': 'batch_id', 'type': 'varchar(100)'}
            ]
            stage_columns = [
                {'name': 'id', 'type': 'bigint'},
                {'name': 'name', 'type': 'varchar(100)'},
                {'name': 'email', 'type': 'varchar(255)'},
                {'name': 'age', 'type': 'varchar(10)'},
                {'name': 'city', 'type': 'varchar(100)'},
                {'name': 'salary', 'type': 'varchar(50)'},
                {'name': 'department', 'type': 'varchar(100)'}
            ]
            
            def mock_get_table_columns(connection, table_name, schema):
                if 'stage' in table_name:
                    return stage_columns
                else:
                    return main_columns
                    
            self.mock_db_manager.get_table_columns.side_effect = mock_get_table_columns
            
            # Connection and cursor with large rowcount
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 99  # 99 data rows (excluding trailer)
            mock_connection.cursor.return_value = mock_cursor
            self.mock_db_manager.get_connection.return_value = mock_connection
            
            # Complex time sequence for realistic performance metrics
            time_calls = 0
            def mock_time_sequence():
                nonlocal time_calls
                time_calls += 1
                # Simulate realistic processing times
                base_times = [0.0, 0.1, 0.5, 1.2, 2.0, 2.8, 3.5, 4.2, 5.0, 6.1, 7.3, 8.5, 9.7, 10.9, 12.1, 13.4, 14.7, 16.0]
                return base_times[min(time_calls - 1, len(base_times) - 1)]
            
            with patch('builtins.open', mock_open(read_data="comprehensive_csv_content")), \
                 patch('pandas.read_csv', return_value=mock_df), \
                 patch('utils.progress.init_progress'), \
                 patch('utils.progress.is_canceled', return_value=False), \
                 patch('utils.progress.update_progress'), \
                 patch('utils.progress.mark_done'), \
                 patch('time.perf_counter', side_effect=mock_time_sequence):
                
                results = []
                progress_key = "test_progress"
                async for message in self.ingester.ingest_data(
                    file_path="/test/comprehensive_data.csv",
                    fmt_file_path="/test/comprehensive_format.json",
                    load_mode="full",
                    filename="comprehensive_data.csv",
                    progress_key=progress_key
                ):
                    results.append(message)
        
        # Comprehensive verification - should hit maximum code paths
        print(f"FINAL PUSH: Total messages: {len(results)}")
        
        # Verify ALL major workflow sections
        assert any("CSV file loaded: 100 rows" in msg for msg in results), "Should load 100 CSV rows"
        assert any("Trailer row removed" in msg for msg in results), "Should remove trailer"
        assert any("Added missing metadata columns" in msg for msg in results), "Should add metadata columns"
        assert any("Added 3 missing columns from input file" in msg for msg in results), "Should add missing columns"
        assert any("2 columns have different types" in msg for msg in results), "Should report type mismatches"
        assert any("fullload mode: truncating 500 existing rows" in msg for msg in results), "Should truncate"
        assert any("Data validation passed" in msg for msg in results), "Should validate data"
        assert any("Moving data from stage to main table" in msg for msg in results), "Should transfer data"
        assert any("Transferring 7 matching columns" in msg for msg in results), "Should transfer columns"
        assert any("Data successfully loaded to main table: 99 rows" in msg for msg in results), "Should load to main"
        assert any("Current main table state backed up" in msg for msg in results), "Should backup data"
        assert any("File archived" in msg for msg in results), "Should archive file"
        
        # Should have many messages for comprehensive workflow
        assert len(results) > 50, f"Expected >50 messages, got {len(results)}"

    @pytest.mark.asyncio
    async def test_empty_csv_edge_case(self):
        """Test completely empty CSV file handling"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="empty_table")
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        
        # Completely empty dataframe
        empty_df = pd.DataFrame()
        
        self.mock_db_manager.table_exists.return_value = False
        
        # Time sequence
        time_calls = 0
        def mock_time_sequence():
            nonlocal time_calls
            result = time_calls * 0.5
            time_calls += 1
            return result
        
        with patch('pandas.read_csv', return_value=empty_df), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=mock_time_sequence):
            
            results = []
            async for message in self.ingester.ingest_data(
                file_path="/test/empty.csv",
                fmt_file_path="/test/format.json",
                load_mode="full",
                filename="empty.csv"
            ):
                results.append(message)
        
        # Should handle empty CSV gracefully
        assert any("CSV file loaded: 0 rows" in msg for msg in results)
        assert any("ERROR!" in msg for msg in results)  # Should error on empty CSV

    @pytest.mark.asyncio
    async def test_large_dataset_performance_paths(self):
        """Test large dataset to trigger performance calculation paths"""
        
        self.ingester.file_handler.extract_table_base_name = MagicMock(return_value="large_table") 
        self.ingester.file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"column_delimiter": ","}
        })
        self.ingester.file_handler.move_to_archive = MagicMock(return_value="/archive/large.csv")
        
        # Large dataset (1000 rows) to trigger performance stats
        large_df = pd.DataFrame({
            'id': range(1, 1001),
            'data': [f'data_{i}' for i in range(1, 1001)]
        })
        
        self.mock_db_manager.table_exists.return_value = True
        self.mock_db_manager.get_row_count.return_value = 2000
        self.mock_db_manager.create_table = MagicMock()
        self.mock_db_manager.backup_existing_data = MagicMock(return_value=2000)
        
        # Stage and validation
        with patch.object(self.ingester, '_load_dataframe_to_table', new_callable=AsyncMock) as mock_stage_load:
            mock_stage_load.return_value = True
            self.mock_db_manager.execute_validation_procedure = MagicMock(return_value={"validation_result": 0})
            
            # Simple columns for data transfer
            main_columns = [{'name': 'id', 'type': 'bigint'}, {'name': 'data', 'type': 'varchar(100)'}]
            stage_columns = [{'name': 'id', 'type': 'bigint'}, {'name': 'data', 'type': 'varchar(100)'}]
            
            def mock_get_table_columns(connection, table_name, schema):
                if 'stage' in table_name:
                    return stage_columns
                else:
                    return main_columns
                    
            self.mock_db_manager.get_table_columns.side_effect = mock_get_table_columns
            
            # Connection
            mock_connection = MagicMock()
            mock_cursor = MagicMock()
            mock_cursor.rowcount = 1000
            mock_connection.cursor.return_value = mock_cursor
            self.mock_db_manager.get_connection.return_value = mock_connection
            
            # Time sequence with varying intervals for performance calculations
            time_calls = 0
            def mock_time_sequence():
                nonlocal time_calls
                time_calls += 1
                # Varying time intervals: fast CSV read, slower stage load, fast validation, medium transfer
                intervals = [0.0, 1.0, 2.0, 3.5, 5.0, 6.0, 8.0, 9.5, 11.0, 12.2, 13.5, 15.0]
                return intervals[min(time_calls - 1, len(intervals) - 1)]
            
            with patch('pandas.read_csv', return_value=large_df), \
                 patch('utils.progress.init_progress'), \
                 patch('utils.progress.is_canceled', return_value=False), \
                 patch('utils.progress.update_progress'), \
                 patch('utils.progress.mark_done'), \
                 patch('time.perf_counter', side_effect=mock_time_sequence):
                
                results = []
                async for message in self.ingester.ingest_data(
                    file_path="/test/large.csv",
                    fmt_file_path="/test/format.json",
                    load_mode="append",
                    filename="large.csv"
                ):
                    results.append(message)
        
        # Verify large dataset processing with performance stats
        assert any("CSV file loaded: 1000 rows" in msg and "rows/s" in msg for msg in results)
        assert any("Data loaded to stage table: 1000 rows" in msg and "rows/s" in msg for msg in results)
        assert any("Data successfully loaded to main table: 1000 rows" in msg for msg in results)

    def test_comprehensive_header_edge_cases(self):
        """Test all possible header edge cases and sanitization paths"""
        
        # Every possible problematic header scenario
        problematic_headers = [
            '',  # Empty
            '   ',  # Whitespace only
            '123column',  # Starts with number
            'column with spaces',  # Spaces
            'column@with#special$chars',  # Special characters
            'column-with-dashes',  # Dashes
            'column.with.dots',  # Dots
            'COLUMN_WITH_UPPER',  # Upper case
            'column_with_underscore',  # Already valid
            'très_long_column_name_that_exceeds_normal_expectations_and_keeps_going',  # Very long
            '中文列名',  # Unicode
            'null',  # SQL keyword
            'select',  # SQL keyword
            'from',  # SQL keyword
            'where'  # SQL keyword
        ]
        
        # Test sanitization
        sanitized = self.ingester._sanitize_headers(problematic_headers)
        assert len(sanitized) == len(problematic_headers)
        
        # Test with massive duplication
        massive_duplicates = ['col'] * 50 + ['another'] * 30 + ['third'] * 20
        deduplicated = self.ingester._deduplicate_headers(massive_duplicates)
        
        assert len(deduplicated) == 100
        assert deduplicated.count('col') == 1
        assert 'col_49' in deduplicated  # Last duplicate should be col_49
        
        # Test type inference on complex data
        complex_df = pd.DataFrame({
            'pure_int': [1, 2, 3, 4, 5],
            'pure_float': [1.1, 2.2, 3.3, 4.4, 5.5], 
            'mixed_numeric': [1, 2.5, 3, 4.7, 5],
            'date_strings': ['2023-01-01', '2023-02-15', '2023-03-30', '2023-04-12', '2023-05-28'],
            'long_text': ['This is a very long text field that contains multiple words and punctuation!'] * 5,
            'empty_strings': ['', '', '', '', ''],
            'null_values': [None, None, None, None, None],
            'mixed_nulls': [1, None, 3, None, 5]
        })
        
        columns = list(complex_df.columns)
        inferred_types = self.ingester._infer_types(complex_df, columns)
        
        # All should be varchar with appropriate lengths
        assert all('varchar' in str(dtype).lower() for dtype in inferred_types.values())
        assert len(inferred_types) == 8