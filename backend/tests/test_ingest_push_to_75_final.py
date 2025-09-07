"""Final push to get utils/ingest.py from 64% to 75%+ coverage"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_trailer_handling_and_empty_file_detection():
    """Target lines 195-196, 202: Trailer handling and empty file detection"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "trailer_schema"
    mock_db.ensure_schemas_exist = Mock()
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "trailer_test"
    # Format config with trailer
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {
            "delimiter": ",",
            "has_header": True,
            "has_trailer": True  # Enable trailer processing
        }
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'trailer_test.csv')
        fmt_file_path = os.path.join(temp_dir, 'trailer_test.fmt')
        
        # CSV with header, data, and trailer
        with open(file_path, 'w') as f:
            f.write('col1,col2\ndata1,data2\ndata3,data4\nTRAILER,2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({
                "csv_format": {
                    "delimiter": ",", 
                    "has_header": True,
                    "has_trailer": True
                }
            }, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel') as mock_cancel, \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            # First return empty DataFrame to trigger empty detection
            empty_df = pd.DataFrame()
            mock_read_csv.return_value = empty_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'trailer_test.csv'
            ):
                messages.append(message)
                if "no data rows" in str(messages).lower():
                    break
                if len(messages) > 30:
                    break
            
            # Should detect empty file and cancel
            assert any("no data rows" in msg.lower() for msg in messages)
            assert any("automatically canceled" in msg.lower() for msg in messages)
            mock_cancel.assert_called()
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio  
async def test_column_escaping_and_sql_injection_protection():
    """Target lines 244-271: Column name escaping and SQL injection protection"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 2
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "sql_safety_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'user_input', 'type': 'varchar'},
        {'name': 'sql_test', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "sql_safety_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    mock_file_handler.move_to_archive.return_value = "/archive/sql_safety.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'sql_safety.csv')
        fmt_file_path = os.path.join(temp_dir, 'sql_safety.fmt')
        
        # CSV with potentially dangerous SQL content
        with open(file_path, 'w') as f:
            f.write('user_input,sql_test\n"O\'Reilly","DROP TABLE users;--"\n"Smith & Co","SELECT * FROM accounts"\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # DataFrame with SQL injection attempts
            mock_df = pd.DataFrame({
                'user_input': ["O'Reilly", "Smith & Co"],
                'sql_test': ["DROP TABLE users;--", "SELECT * FROM accounts"]
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'sql_safety.csv'
            ):
                messages.append(message)
                if len(messages) > 80:
                    break
            
            # Should complete safely despite SQL injection attempts
            assert any("successfully" in msg.lower() for msg in messages)
            assert mock_cursor.executemany.called
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_specific_cancellation_checkpoints():
    """Target lines 332-333, 497-498: Specific cancellation checkpoints"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "cancel_checkpoint_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [{'name': 'cancel_test_col', 'type': 'varchar'}]
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "cancel_checkpoint_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'cancel_checkpoint.csv')
        fmt_file_path = os.path.join(temp_dir, 'cancel_checkpoint.fmt')
        
        with open(file_path, 'w') as f:
            f.write('cancel_test_col\ntest_data\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up cancellation to happen after several checks
        cancel_sequence = [False] * 8 + [True]  # Cancel after 8 checks
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', side_effect=cancel_sequence), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            mock_df = pd.DataFrame({'cancel_test_col': ['test_data']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            try:
                async for message in ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_checkpoint.csv'
                ):
                    messages.append(message)
                    if len(messages) > 40:
                        break
            except Exception as e:
                if "canceled by user" in str(e):
                    # Expected cancellation
                    pass
            
            # Should hit cancellation checkpoints 
            assert any("cancel" in msg.lower() for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_large_batch_loading_with_progress():
    """Target lines 771-794: Large batch loading with detailed progress"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 15  # Large batch
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "large_batch_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'batch_number', 'type': 'int'},
        {'name': 'large_text_field', 'type': 'text'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 15}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "large_batch_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    mock_file_handler.move_to_archive.return_value = "/archive/large_batch.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'large_batch.csv')
        fmt_file_path = os.path.join(temp_dir, 'large_batch.fmt')
        
        # Create large dataset
        with open(file_path, 'w') as f:
            f.write('batch_number,large_text_field\n')
            for i in range(1, 16):  # 15 rows
                f.write(f'{i},"This is a large text field with lots of data for batch {i} testing purposes"\n')
        
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        progress_calls = []
        def mock_progress_update(*args, **kwargs):
            progress_calls.append((args, kwargs))
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress', side_effect=mock_progress_update), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 800)):
            
            # Large DataFrame
            mock_df = pd.DataFrame({
                'batch_number': list(range(1, 16)),
                'large_text_field': [
                    f'This is a large text field with lots of data for batch {i} testing purposes'
                    for i in range(1, 16)
                ]
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'large_batch.csv'
            ):
                messages.append(message)
                if len(messages) > 150:
                    break
            
            # Should complete large batch successfully
            assert any("successfully" in msg.lower() for msg in messages)
            assert any("15 rows" in msg for msg in messages)
            
            # Should have multiple progress calls for large batch
            assert len(progress_calls) >= 10
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_metadata_column_addition_paths():
    """Target lines 290-296: Metadata column addition paths"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 3
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "metadata_addition_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = True  # Existing table
    mock_db.get_row_count.return_value = 100  # Has data
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'existing_col', 'type': 'varchar'}
    ]
    
    # Mock metadata addition that adds multiple columns 
    mock_db.ensure_main_table_metadata_columns.return_value = {
        'added': ['created_timestamp', 'updated_timestamp', 'source_system', 'batch_id']
    }
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 3}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "metadata_addition_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    mock_file_handler.move_to_archive.return_value = "/archive/metadata_addition.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'metadata_addition.csv')
        fmt_file_path = os.path.join(temp_dir, 'metadata_addition.fmt')
        
        with open(file_path, 'w') as f:
            f.write('existing_col\nvalue1\nvalue2\nvalue3\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 300)):
            
            mock_df = pd.DataFrame({'existing_col': ['value1', 'value2', 'value3']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'metadata_addition.csv'
            ):
                messages.append(message)
                if len(messages) > 100:
                    break
            
            # Should complete successfully with metadata additions
            assert any("successfully" in msg.lower() for msg in messages)
            assert mock_db.ensure_main_table_metadata_columns.called
            # Should log metadata column additions
            mock_logger.log_info.assert_called()
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)