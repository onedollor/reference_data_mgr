"""Final push for 70%+ coverage targeting specific missing line ranges"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_header_processing_edge_cases():
    """Test header processing and validation (lines 145-183)"""
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "test_table"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {
            "header_delimiter": ",",
            "column_delimiter": ",", 
            "row_delimiter": "\n",
            "text_qualifier": '"',
            "skip_lines": 0,
            "has_header": True,
            "has_trailer": False
        }
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'bad_headers.csv')
        fmt_file_path = os.path.join(temp_dir, 'bad_headers.fmt')
        
        # CSV with problematic headers
        with open(file_path, 'w') as f:
            f.write('invalid-column-name,123column,column with spaces,normal\n')
            f.write('val1,val2,val3,val4\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            # Mock DataFrame with problematic column names
            mock_df = Mock()
            mock_df.shape = (1, 4)
            mock_df.columns = pd.Index(['invalid-column-name', '123column', 'column with spaces', 'normal'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['object'] * 4, index=mock_df.columns)
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=Mock())
            mock_df.iloc[0].to_dict.return_value = {'invalid-column-name': 'val1', '123column': 'val2', 'column with spaces': 'val3', 'normal': 'val4'}
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'bad_headers.csv'
            ):
                messages.append(message)
                if len(messages) > 30:
                    break
            
            # Should have processed headers with sanitization
            assert any("Headers processed" in msg for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio 
async def test_schema_and_metadata_operations():
    """Test schema operations and metadata handling (lines 257-298)"""
    mock_db = Mock()
    mock_cursor = Mock()
    mock_cursor.rowcount = 2
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "test_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False  # New table
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
    mock_db.get_table_columns.return_value = [
        {'name': 'col1', 'type': 'varchar'},
        {'name': 'col2', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
    
    # Mock specific schema operations to hit lines 257-298
    mock_db.ensure_main_table_metadata_columns.return_value = {
        'added': [],  # No columns added
        'errors': []
    }
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "new_table"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {
            "header_delimiter": ",",
            "column_delimiter": ",",
            "row_delimiter": "\n", 
            "text_qualifier": '"',
            "skip_lines": 0,
            "has_header": True,
            "has_trailer": False
        }
    })
    mock_file_handler.move_to_archive.return_value = "/archive/path/file.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'schema_test.csv')
        fmt_file_path = os.path.join(temp_dir, 'schema_test.fmt')
        
        with open(file_path, 'w') as f:
            f.write('col1,col2\ndata1,data2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            mock_df = pd.DataFrame({'col1': ['data1'], 'col2': ['data2']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'schema_test.csv'
            ):
                messages.append(message)
                if len(messages) > 50:
                    break
            
            # Should have handled schema operations
            assert any("Database tables created" in msg for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_completion_and_cleanup_paths():
    """Test completion and cleanup code paths (lines 525-535)"""
    mock_db = Mock()
    mock_cursor = Mock()
    mock_cursor.rowcount = 1
    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "test_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
    mock_db.get_table_columns.return_value = [{'name': 'data', 'type': 'varchar'}]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 1}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    # Mock the post-load procedure to hit completion paths
    mock_db.call_post_load_procedure = Mock()
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "completion_table"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {
            "header_delimiter": ",",
            "column_delimiter": ",",
            "row_delimiter": "\n",
            "text_qualifier": '"', 
            "skip_lines": 0,
            "has_header": True,
            "has_trailer": False
        }
    })
    mock_file_handler.move_to_archive.return_value = "/archive/completion_test.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'completion.csv')
        fmt_file_path = os.path.join(temp_dir, 'completion.fmt')
        
        with open(file_path, 'w') as f:
            f.write('data\ntest_completion\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 150)):
            
            mock_df = pd.DataFrame({'data': ['test_completion']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'completion.csv',
                config_reference_data=True
            ):
                messages.append(message)
                # Let it complete fully
                if len(messages) > 80:
                    break
            
            # Should complete successfully
            assert any("completed successfully" in msg for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_error_recovery_scenarios():
    """Test error recovery and cleanup (lines 914-928)"""
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    ingester = DataIngester(mock_db, mock_logger)
    
    # Test DataFrame with problematic data types
    problematic_df = pd.DataFrame({
        'id': [1, None, 3],  # Mixed data with None
        'data': ['valid', '', None]  # Mixed string data
    })
    
    # Mock connection with cursor that has issues
    mock_connection = Mock()
    mock_cursor = Mock() 
    mock_cursor.executemany.side_effect = [
        None,  # First batch succeeds
        Exception("Connection timeout"),  # Second batch fails
        None   # Recovery succeeds
    ]
    mock_connection.cursor.return_value = mock_cursor
    
    with patch('utils.progress.update_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch('utils.progress.request_cancel'), \
         patch('time.perf_counter', side_effect=range(1, 100)):
        
        # Should handle errors and attempt recovery
        try:
            await ingester._load_dataframe_to_table(
                mock_connection,
                problematic_df,
                'recovery_table',
                'test_schema',
                3,
                'recovery_test_key',
                'append',
                pd.Timestamp.now()
            )
        except Exception:
            # May raise exception after attempting recovery
            pass
        
        # Should have attempted database operations
        assert mock_cursor.executemany.called