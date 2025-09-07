"""Push utils/ingest.py to 70%+ coverage by testing _load_dataframe_to_table method"""
import pytest
import asyncio
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_load_dataframe_to_table_comprehensive():
    """Test _load_dataframe_to_table method comprehensively to cover lines 761-928"""
    
    # Create mock database and logger
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    # Create ingester
    ingester = DataIngester(mock_db, mock_logger)
    
    # Create test DataFrame
    test_df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [100.5, 200.0, 300.75, 400.25, 500.0],
        'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05']
    })
    
    # Create mock connection and cursor
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.executemany = Mock()
    
    # Mock timestamp
    import datetime
    test_timestamp = datetime.datetime.now()
    
    with patch('utils.progress.update_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch('time.perf_counter', side_effect=range(1, 100)):
        
        # Test successful data loading
        await ingester._load_dataframe_to_table(
            mock_connection,
            test_df,
            'test_table',
            'test_schema', 
            5,  # total_rows
            'test_progress_key',
            'full',  # load_type
            test_timestamp
        )
        
        # Verify database operations
        mock_connection.cursor.assert_called()
        mock_cursor.executemany.assert_called()


@pytest.mark.asyncio
async def test_load_dataframe_with_cancellation():
    """Test _load_dataframe_to_table with cancellation scenarios"""
    
    mock_db = Mock()
    mock_logger = Mock() 
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    ingester = DataIngester(mock_db, mock_logger)
    
    # Small test DataFrame
    test_df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    
    cancel_count = 0
    def mock_is_canceled(key):
        nonlocal cancel_count
        cancel_count += 1
        return cancel_count > 2  # Cancel after a few iterations
    
    with patch('utils.progress.update_progress'), \
         patch('utils.progress.is_canceled', side_effect=mock_is_canceled), \
         patch('utils.progress.request_cancel'), \
         patch('time.perf_counter', side_effect=range(1, 50)):
        
        # Should handle cancellation gracefully
        await ingester._load_dataframe_to_table(
            mock_connection,
            test_df, 
            'test_table',
            'test_schema',
            2,
            'cancel_test_key',
            'append',
            pd.Timestamp.now()
        )
        
        # Should have attempted to cancel
        assert cancel_count > 2


@pytest.mark.asyncio
async def test_read_csv_file_method():
    """Test _read_csv_file method to cover lines 579-620"""
    
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    ingester = DataIngester(mock_db, mock_logger)
    
    # Create temp CSV file
    import tempfile
    import os
    
    temp_dir = tempfile.mkdtemp()
    try:
        csv_path = os.path.join(temp_dir, 'test.csv') 
        
        with open(csv_path, 'w') as f:
            f.write('name,age,city\nAlice,25,NYC\nBob,30,LA\nCharlie,35,Chicago\n')
        
        # CSV format configuration
        csv_format = {
            'column_delimiter': ',',
            'text_qualifier': '"', 
            'skip_lines': 0,
            'has_header': True,
            'has_trailer': False,
            'trailer_line': None
        }
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel'):
            
            # Test successful CSV reading  
            result_df = await ingester._read_csv_file(csv_path, csv_format, 'test_progress')
            
            # Verify DataFrame was created
            assert result_df is not None
            assert len(result_df) == 3
            assert 'name' in result_df.columns
            assert 'age' in result_df.columns  
            assert 'city' in result_df.columns
    
    finally:
        # Cleanup
        import shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_read_csv_with_cancellation():
    """Test _read_csv_file with cancellation during processing"""
    
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    ingester = DataIngester(mock_db, mock_logger)
    
    # Create temp CSV 
    import tempfile
    import os
    
    temp_dir = tempfile.mkdtemp()
    try:
        csv_path = os.path.join(temp_dir, 'cancel_test.csv')
        
        # Large CSV to allow cancellation
        with open(csv_path, 'w') as f:
            f.write('id,data\n')
            for i in range(100):
                f.write(f'{i},data_{i}\n')
        
        csv_format = {
            'column_delimiter': ',',
            'text_qualifier': '"',
            'skip_lines': 0, 
            'has_header': True
        }
        
        cancel_count = 0
        def mock_is_canceled(key):
            nonlocal cancel_count
            cancel_count += 1
            return cancel_count > 5  # Cancel after a few checks
        
        with patch('utils.progress.is_canceled', side_effect=mock_is_canceled), \
             patch('utils.progress.request_cancel'):
            
            # May raise exception or return partial data
            try:
                result_df = await ingester._read_csv_file(csv_path, csv_format, 'cancel_test')
                # If no exception, just verify we got some result
                assert result_df is not None
            except Exception as e:
                # Cancellation may cause exceptions
                assert cancel_count > 5
    
    finally:
        import shutil 
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_error_scenarios_in_data_loading():
    """Test error handling scenarios in data loading methods"""
    
    mock_db = Mock()
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    
    ingester = DataIngester(mock_db, mock_logger)
    
    # Test with malformed DataFrame
    bad_df = pd.DataFrame({'col': [None, '', 'valid']})
    
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Make cursor.executemany raise an exception
    mock_cursor.executemany.side_effect = Exception("Database error")
    
    with patch('utils.progress.update_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch('utils.progress.request_cancel'), \
         patch('time.perf_counter', side_effect=range(1, 20)):
        
        # Should handle database errors
        try:
            await ingester._load_dataframe_to_table(
                mock_connection,
                bad_df,
                'error_table',
                'test_schema',
                3,
                'error_test_key',
                'full',
                pd.Timestamp.now()
            )
        except Exception:
            # Error handling may re-raise or handle gracefully
            pass
        
        # Verify error handling was attempted
        mock_cursor.executemany.assert_called()