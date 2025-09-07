"""Critical lines tests to push from 67% to 75%+ coverage on utils/ingest.py"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_header_line_processing_edge_cases():
    """Target lines 145-183: Header processing with problematic column names"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 3
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "header_test"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'invalid_header', 'type': 'varchar'},
        {'name': 'number_start', 'type': 'varchar'},
        {'name': 'column_with_spaces', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 3}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "header_edge_test"
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
    mock_file_handler.move_to_archive.return_value = "/archive/header_edge.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'header_edge.csv')
        fmt_file_path = os.path.join(temp_dir, 'header_edge.fmt')
        
        # CSV with problematic headers that need processing
        with open(file_path, 'w') as f:
            f.write('invalid@header,123number_start,Column With Spaces\nval1,val2,val3\ntest1,test2,test3\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({
                "csv_format": {
                    "header_delimiter": ",",
                    "column_delimiter": ",",
                    "text_qualifier": '"'
                }
            }, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # Mock DataFrame with problematic headers
            mock_df = pd.DataFrame({
                'invalid@header': ['val1', 'test1'], 
                '123number_start': ['val2', 'test2'],
                'Column With Spaces': ['val3', 'test3']
            })
            # Force column processing paths
            mock_df.columns = pd.Index(['invalid@header', '123number_start', 'Column With Spaces'])
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'header_edge.csv'
            ):
                messages.append(message)
                if len(messages) > 100:
                    break
            
            # Should process headers and complete successfully
            assert any("successfully" in msg.lower() for msg in messages)
            # Should have column processing logic executed
            assert mock_db.create_stage_table.called
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_schema_metadata_operations():
    """Target lines 257-298: Schema operations with metadata columns"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock() 
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 2
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "metadata_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = True  # Existing table
    mock_db.get_row_count.return_value = 100  # Has existing data
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'data_col', 'type': 'varchar'}
    ]
    
    # Mock metadata operations to hit lines 257-298
    mock_db.ensure_main_table_metadata_columns.return_value = {
        'added': ['last_modified', 'created_date', 'source_file']  # Force metadata addition
    }
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "metadata_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {
            "delimiter": ",",
            "has_header": True
        }
    })
    mock_file_handler.move_to_archive.return_value = "/archive/metadata.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'metadata.csv')
        fmt_file_path = os.path.join(temp_dir, 'metadata.fmt')
        
        with open(file_path, 'w') as f:
            f.write('data_col\ntest_data_1\ntest_data_2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 300)):
            
            mock_df = pd.DataFrame({'data_col': ['test_data_1', 'test_data_2']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'metadata.csv'
            ):
                messages.append(message)
                if len(messages) > 80:
                    break
            
            # Should hit metadata processing paths
            assert mock_db.ensure_main_table_metadata_columns.called
            assert any("successfully" in msg.lower() for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_validation_failure_paths():
    """Target lines 372-391: Validation failure handling"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "validation_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    
    # Mock validation to fail with issues - this hits lines 374-383, 390-391
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 3,  # Non-zero indicates validation failures
        "validation_issue_list": [
            "Row 1: Invalid data format in column 'amount'",
            "Row 3: Required field 'id' is null",
            "Row 5: Value exceeds maximum length for field 'description'"
        ]
    }
    
    mock_db.get_table_columns.return_value = [
        {'name': 'id', 'type': 'int'},
        {'name': 'amount', 'type': 'decimal'}, 
        {'name': 'description', 'type': 'varchar'}
    ]
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "validation_fail_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'validation_fail.csv')
        fmt_file_path = os.path.join(temp_dir, 'validation_fail.fmt')
        
        with open(file_path, 'w') as f:
            f.write('id,amount,description\n1,abc,valid desc\n,50.00,another\n3,100.50,way too long description that exceeds limit\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 150)):
            
            mock_df = pd.DataFrame({
                'id': [1, None, 3],
                'amount': ['abc', 50.00, 100.50],
                'description': ['valid desc', 'another', 'way too long description that exceeds limit']
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'validation_fail.csv'
            ):
                messages.append(message)
                if "validation issues found" in str(messages).lower() or len(messages) > 60:
                    break
            
            # Should hit validation failure paths with detailed error reporting
            assert any("validation" in msg.lower() for msg in messages)
            assert any("issue" in msg.lower() for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio  
async def test_data_loading_with_complex_batch_processing():
    """Target lines 771-794: Complex data loading with multiple batches"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 8  # Multiple batches
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "batch_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'batch_id', 'type': 'int'},
        {'name': 'data_field', 'type': 'varchar'},
        {'name': 'timestamp_field', 'type': 'datetime'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 8}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "batch_loading_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    mock_file_handler.move_to_archive.return_value = "/archive/batch.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'batch_loading.csv')
        fmt_file_path = os.path.join(temp_dir, 'batch_loading.fmt')
        
        # Large dataset to trigger batch processing paths
        with open(file_path, 'w') as f:
            f.write('batch_id,data_field,timestamp_field\n')
            for i in range(1, 9):  # 8 rows to trigger batching logic
                f.write(f'{i},data_{i},2025-01-0{i % 7 + 1} 12:00:00\n')
        
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress') as mock_progress, \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 400)):
            
            # Large DataFrame to test batch processing
            mock_df = pd.DataFrame({
                'batch_id': list(range(1, 9)),
                'data_field': [f'data_{i}' for i in range(1, 9)],
                'timestamp_field': [f'2025-01-0{i % 7 + 1} 12:00:00' for i in range(1, 9)]
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'batch_loading.csv'
            ):
                messages.append(message)
                if len(messages) > 120:
                    break
            
            # Should complete batch processing successfully 
            assert any("successfully" in msg.lower() for msg in messages)
            assert any("8 rows" in msg for msg in messages)
            # Should have called progress updates multiple times for batching
            assert mock_progress.call_count > 5
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_precise_cancellation_points():
    """Target lines 332-333, 497-498: Specific cancellation checkpoints"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "cancel_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'test_col', 'type': 'varchar'}
    ]
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "cancel_points_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'cancel_points.csv')
        fmt_file_path = os.path.join(temp_dir, 'cancel_points.fmt')
        
        with open(file_path, 'w') as f:
            f.write('test_col\ncancel_test_data\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Mock cancellation at specific checkpoints
        cancel_calls = [False] * 15 + [True]  # Cancel after several progress checks
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', side_effect=cancel_calls), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            mock_df = pd.DataFrame({'test_col': ['cancel_test_data']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            with pytest.raises(Exception, match="Ingestion canceled by user"):
                async for message in ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_points.csv'
                ):
                    messages.append(message)
                    if len(messages) > 50:
                        break
            
            # Should hit cancellation checkpoints
            assert any("cancel" in msg.lower() for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)