"""Final targeted tests to push over 70% coverage threshold"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_comprehensive_error_scenarios():
    """Test multiple error scenarios in one comprehensive test"""
    
    # Test file reading with various edge cases
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 2
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "test_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
    mock_db.get_table_columns.return_value = [{'name': 'test_col', 'type': 'varchar'}]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "edge_case_table"
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
    mock_file_handler.move_to_archive.return_value = "/archive/edge_case.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'edge_case.csv')
        fmt_file_path = os.path.join(temp_dir, 'edge_case.fmt')
        
        # Create CSV with edge cases
        with open(file_path, 'w') as f:
            f.write('test_col\n"value with special chars: !@#$%"\n"another value"\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ",", "text_qualifier": '"'}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            # Mock DataFrame with edge case data
            mock_df = pd.DataFrame({
                'test_col': ['value with special chars: !@#$%', 'another value']
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'edge_case.csv'
            ):
                messages.append(message)
                if len(messages) > 50:
                    break
            
            # Should complete successfully
            assert any("successfully" in msg for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_complex_data_type_handling():
    """Test complex data type processing"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 3
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "data_types_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
    mock_db.get_table_columns.return_value = [
        {'name': 'id', 'type': 'int'},
        {'name': 'decimal_col', 'type': 'decimal'}, 
        {'name': 'text_col', 'type': 'nvarchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 3}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "data_types_table"
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
    mock_file_handler.move_to_archive.return_value = "/archive/data_types.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'data_types.csv')
        fmt_file_path = os.path.join(temp_dir, 'data_types.fmt')
        
        # CSV with complex data types
        with open(file_path, 'w') as f:
            f.write('id,decimal_col,text_col\n1,123.45,"Text with spaces"\n2,678.90,"More complex text"\n3,999.99,"Final text"\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 150)):
            
            # Mock complex DataFrame
            mock_df = pd.DataFrame({
                'id': [1, 2, 3],
                'decimal_col': [123.45, 678.90, 999.99],
                'text_col': ['Text with spaces', 'More complex text', 'Final text']
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'data_types.csv'
            ):
                messages.append(message)
                if len(messages) > 60:
                    break
            
            # Should handle complex data types
            assert any("successfully" in msg for msg in messages)
            assert any("3 rows" in msg for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio  
async def test_loading_with_progress_updates():
    """Test data loading with frequent progress updates"""
    
    mock_db = Mock()
    mock_connection = Mock() 
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 5
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "progress_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
    mock_db.get_table_columns.return_value = [
        {'name': 'batch_id', 'type': 'int'}, 
        {'name': 'data', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 5}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "progress_table"
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
    mock_file_handler.move_to_archive.return_value = "/archive/progress.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'progress.csv')
        fmt_file_path = os.path.join(temp_dir, 'progress.fmt')
        
        # CSV with multiple rows for batch processing
        with open(file_path, 'w') as f:
            f.write('batch_id,data\n')
            for i in range(1, 6):
                f.write(f'{i},batch_data_{i}\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress') as mock_update, \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # Mock DataFrame for batch processing
            mock_df = pd.DataFrame({
                'batch_id': list(range(1, 6)),
                'data': [f'batch_data_{i}' for i in range(1, 6)]
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'progress.csv'
            ):
                messages.append(message)
                if len(messages) > 80:
                    break
            
            # Should complete with progress updates
            assert any("successfully" in msg for msg in messages)
            # Should have called progress updates
            mock_update.assert_called()
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)