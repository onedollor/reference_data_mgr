"""Focused error path testing to increase coverage"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_database_connection_error_simple():
    """Test simple database connection error"""
    mock_db = Mock()
    mock_db.get_connection.side_effect = Exception("Connection failed")
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "test_table"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'test.csv')
        fmt_file_path = os.path.join(temp_dir, 'test.fmt')
        
        with open(file_path, 'w') as f:
            f.write('col1\nval1\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel') as mock_cancel, \
             patch('utils.progress.mark_error') as mock_mark_error, \
             patch('time.perf_counter', side_effect=range(1, 20)):
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'test.csv'
            ):
                messages.append(message)
                if len(messages) > 10:
                    break
            
            # Should hit error handling paths
            error_messages = [msg for msg in messages if "ERROR!" in msg]
            assert len(error_messages) > 0
            
            # Should call error logging
            mock_logger.log_error.assert_called()
            mock_mark_error.assert_called()
    
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio 
async def test_early_cancellation_error():
    """Test early cancellation scenario"""
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "cancel_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'cancel.csv')
        fmt_file_path = os.path.join(temp_dir, 'cancel.fmt')
        
        with open(file_path, 'w') as f:
            f.write('data\ntest\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Mock early cancellation
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=True), \
             patch('time.perf_counter', side_effect=range(1, 10)):
            
            messages = []
            with pytest.raises(Exception, match="Ingestion canceled by user"):
                async for message in ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel.csv'
                ):
                    messages.append(message)
                    if len(messages) > 5:
                        break
            
            # Should hit early cancellation path
            assert any("Cancellation requested" in msg for msg in messages)
    
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_csv_file_error_handling():
    """Test CSV file processing errors"""
    mock_db = Mock()
    mock_db.get_connection.return_value = Mock()
    mock_db.ensure_schemas_exist = Mock()
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "csv_error_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'bad.csv')
        fmt_file_path = os.path.join(temp_dir, 'bad.fmt')
        
        with open(file_path, 'w') as f:
            f.write('header\ndata\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Make pandas fail
        with patch('pandas.read_csv', side_effect=Exception("Pandas parsing error")), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 30)):
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'bad.csv'
            ):
                messages.append(message)
                if len(messages) > 15:
                    break
            
            # Should hit pandas error handling
            assert any("ERROR!" in msg for msg in messages)
            mock_logger.log_error.assert_called()
    
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_target_schema_restoration():
    """Test target schema restoration in finally block"""
    mock_db = Mock()
    mock_db.data_schema = "original_schema"
    mock_db.get_connection.return_value = Mock()
    # Make this fail to trigger finally block
    mock_db.ensure_schemas_exist.side_effect = Exception("Schema error")
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "schema_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'schema.csv')
        fmt_file_path = os.path.join(temp_dir, 'schema.fmt')
        
        with open(file_path, 'w') as f:
            f.write('col\nval\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 20)):
            
            mock_read_csv.return_value = pd.DataFrame({'col': ['val']})
            
            # Store original schema value
            original_schema = mock_db.data_schema
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'schema.csv',
                target_schema="custom_target"
            ):
                messages.append(message)
                if len(messages) > 10:
                    break
            
            # Should have restored original schema even after error
            assert mock_db.data_schema == original_schema
            assert any("ERROR!" in msg for msg in messages)
    
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_empty_dataframe_error():
    """Test empty DataFrame error handling"""
    mock_db = Mock()
    mock_db.get_connection.return_value = Mock()
    mock_db.ensure_schemas_exist = Mock()
    
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    mock_file_handler = Mock()
    mock_file_handler.extract_table_base_name.return_value = "empty_test"
    mock_file_handler.read_format_file = AsyncMock(return_value={
        "csv_format": {"delimiter": ",", "has_header": True}
    })
    
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    temp_dir = tempfile.mkdtemp()
    try:
        file_path = os.path.join(temp_dir, 'empty.csv')
        fmt_file_path = os.path.join(temp_dir, 'empty.fmt')
        
        with open(file_path, 'w') as f:
            f.write('header\n')  # Only header, no data
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 30)):
            
            # Return empty DataFrame
            mock_read_csv.return_value = pd.DataFrame({'header': []})
            
            messages = []
            async for message in ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'empty.csv'
            ):
                messages.append(message)
                if len(messages) > 20:
                    break
            
            # Should detect empty file
            assert any("no data rows" in msg for msg in messages)
            assert any("automatically canceled" in msg for msg in messages)
    
    finally:
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)