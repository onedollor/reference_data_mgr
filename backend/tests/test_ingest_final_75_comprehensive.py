"""Final comprehensive test to push utils/ingest.py from 59% to 75%+ coverage"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_comprehensive_ingest_workflow_75_plus():
    """Comprehensive test covering multiple code paths systematically"""
    
    # Setup complete mock database with all operations
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 5
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "comprehensive_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = True  # Table exists to trigger more paths
    mock_db.get_row_count.return_value = 50  # Has data
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    
    # Setup validation to pass with warnings (hits more paths)
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, 
        "validation_issue_list": []
    }
    
    # Setup columns to trigger column processing logic
    mock_db.get_table_columns.return_value = [
        {'name': 'id', 'type': 'int'},
        {'name': 'data_text', 'type': 'varchar'},
        {'name': 'amount', 'type': 'decimal'},
        {'name': 'created_date', 'type': 'datetime'}
    ]
    
    # Setup metadata operations
    mock_db.ensure_main_table_metadata_columns.return_value = {
        'added': ['last_modified', 'source_file']
    }
    
    mock_db.move_stage_to_main.return_value = {
        'success': True, 
        'rows_affected': 5
    }
    
    # Setup complete logger
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    # Setup file handler
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "comprehensive_test_table"
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
    mock_file_handler.move_to_archive.return_value = "/archive/comprehensive.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'comprehensive.csv')
        fmt_file_path = os.path.join(temp_dir, 'comprehensive.fmt')
        
        # Create CSV that exercises multiple paths
        csv_content = '''id,data_text,amount,created_date
1,"First record with quotes",123.45,"2025-01-01 10:00:00"
2,"Second record, with comma",678.90,"2025-01-02 11:00:00"
3,"Third record with 'single quotes'",999.99,"2025-01-03 12:00:00"
4,"Fourth record with \"escaped quotes\"",234.56,"2025-01-04 13:00:00"
5,"Final record with multiple, commas, in, text",567.89,"2025-01-05 14:00:00"'''
        
        with open(file_path, 'w') as f:
            f.write(csv_content)
        
        with open(fmt_file_path, 'w') as f:
            json.dump({
                "csv_format": {
                    "header_delimiter": ",",
                    "column_delimiter": ",",
                    "row_delimiter": "\n",
                    "text_qualifier": '"',
                    "skip_lines": 0,
                    "has_header": True,
                    "has_trailer": False
                }
            }, f)
        
        # Mock progress tracking
        progress_calls = []
        def track_progress(progress_key, current, total, message):
            progress_calls.append((progress_key, current, total, message))
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress', side_effect=track_progress), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 500)):
            
            # Create comprehensive DataFrame
            mock_df = pd.DataFrame({
                'id': [1, 2, 3, 4, 5],
                'data_text': [
                    'First record with quotes',
                    'Second record, with comma', 
                    'Third record with \'single quotes\'',
                    'Fourth record with "escaped quotes"',
                    'Final record with multiple, commas, in, text'
                ],
                'amount': [123.45, 678.90, 999.99, 234.56, 567.89],
                'created_date': [
                    '2025-01-01 10:00:00',
                    '2025-01-02 11:00:00',
                    '2025-01-03 12:00:00', 
                    '2025-01-04 13:00:00',
                    '2025-01-05 14:00:00'
                ]
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'comprehensive.csv',
                config_reference_data=True
            ):
                messages.append(message)
                if len(messages) > 150:
                    break
            
            # Verify comprehensive workflow completed
            assert any("successfully" in msg.lower() for msg in messages)
            assert any("5 rows" in msg for msg in messages)
            
            # Verify database operations were called
            assert mock_db.ensure_schemas_exist.called
            assert mock_db.table_exists.called
            assert mock_db.get_row_count.called
            assert mock_db.determine_load_type.called
            assert mock_db.create_stage_table.called
            assert mock_db.execute_validation_procedure.called
            assert mock_db.get_table_columns.called
            assert mock_db.ensure_main_table_metadata_columns.called
            assert mock_db.move_stage_to_main.called
            
            # Verify file operations
            assert mock_file_handler.read_format_file.called
            assert mock_file_handler.move_to_archive.called
            
            # Verify progress tracking
            assert len(progress_calls) > 10  # Multiple progress updates
            
            # Verify logging operations
            assert mock_logger.log_info.call_count > 5
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_complex_header_processing_paths():
    """Test complex header processing to hit lines 145-183"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 2
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "header_processing_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = False
    mock_db.get_row_count.return_value = 0
    mock_db.determine_load_type.return_value = 'full'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'invalid_column', 'type': 'varchar'},
        {'name': 'numeric_start_column', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "header_processing_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {
            "delimiter": ",",
            "has_header": True
        }
    })
    mock_file_handler.move_to_archive.return_value = "/archive/header_processing.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'header_processing.csv')
        fmt_file_path = os.path.join(temp_dir, 'header_processing.fmt')
        
        # CSV with headers that need processing
        with open(file_path, 'w') as f:
            f.write('Invalid@Column#Name,123NumericStartColumn\ndata1,data2\nvalue1,value2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 300)):
            
            # DataFrame with problematic column names
            mock_df = pd.DataFrame({
                'Invalid@Column#Name': ['data1', 'value1'],
                '123NumericStartColumn': ['data2', 'value2']
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'header_processing.csv'
            ):
                messages.append(message)
                if len(messages) > 100:
                    break
            
            # Should complete successfully despite problematic headers
            assert any("successfully" in msg.lower() for msg in messages)
            assert any("2 rows" in msg for msg in messages)
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_schema_operations_with_target_override():
    """Test schema operations with target schema override"""
    
    mock_db = Mock()
    mock_connection = Mock() 
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 3
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    
    # Test target schema override logic (lines around 80-82)
    original_schema = "original_schema"
    mock_db.data_schema = original_schema
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = True
    mock_db.get_row_count.return_value = 25
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.execute_validation_procedure.return_value = {
        "validation_result": 0, "validation_issue_list": []
    }
    mock_db.get_table_columns.return_value = [
        {'name': 'test_column', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 3}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': ['modified_date']}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "schema_override_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    mock_file_handler.move_to_archive.return_value = "/archive/schema_override.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'schema_override.csv')
        fmt_file_path = os.path.join(temp_dir, 'schema_override.fmt')
        
        with open(file_path, 'w') as f:
            f.write('test_column\ndata1\ndata2\ndata3\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            mock_df = pd.DataFrame({'test_column': ['data1', 'data2', 'data3']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'schema_override.csv',
                target_schema="custom_target_schema"  # Test target schema override
            ):
                messages.append(message)
                if len(messages) > 80:
                    break
            
            # Should complete successfully
            assert any("successfully" in msg.lower() for msg in messages)
            
            # Should have restored original schema in finally block
            assert mock_db.data_schema == original_schema
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_data_loading_with_batching():
    """Test data loading with batching logic (lines 771-794)"""
    
    mock_db = Mock()
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.executemany = Mock()
    mock_cursor.rowcount = 10  # Large dataset
    mock_connection.cursor.return_value = mock_cursor
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "batching_schema"
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
        {'name': 'batch_data', 'type': 'varchar'}
    ]
    mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 10}
    mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "batching_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    mock_file_handler.move_to_archive.return_value = "/archive/batching.csv"
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'batching.csv')
        fmt_file_path = os.path.join(temp_dir, 'batching.fmt')
        
        # Large dataset to test batching
        with open(file_path, 'w') as f:
            f.write('batch_id,batch_data\n')
            for i in range(1, 11):
                f.write(f'{i},batch_data_row_{i}\n')
        
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress') as mock_progress, \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 600)):
            
            # Large DataFrame
            mock_df = pd.DataFrame({
                'batch_id': list(range(1, 11)),
                'batch_data': [f'batch_data_row_{i}' for i in range(1, 11)]
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'batching.csv'
            ):
                messages.append(message)
                if len(messages) > 120:
                    break
            
            # Should complete successfully with all rows
            assert any("successfully" in msg.lower() for msg in messages)
            assert any("10 rows" in msg for msg in messages)
            
            # Should have multiple progress updates for batching
            assert mock_progress.call_count >= 8
            
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)