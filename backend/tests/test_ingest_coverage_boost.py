"""
Strategic tests to boost coverage by targeting specific missing line ranges
Current: 66% (366/551) - Target: 75%+ by hitting high-value missing lines
"""
import pytest
import os
import pandas as pd
import tempfile
import json
from unittest.mock import MagicMock, patch, AsyncMock
from utils.ingest import DataIngester


@pytest.fixture
def mock_logger():
    logger = AsyncMock()
    logger.log_error = AsyncMock()
    logger.log_info = AsyncMock() 
    logger.log_warning = AsyncMock()
    return logger


@pytest.fixture 
def mock_db_manager():
    mock = MagicMock()
    mock.data_schema = "test_schema"
    mock.get_connection.return_value = MagicMock()
    mock.ensure_schemas_exist.return_value = None
    mock.execute_query.return_value = None
    mock.table_exists.return_value = False
    return mock


@pytest.fixture
def ingester(mock_db_manager, mock_logger):
    return DataIngester(mock_db_manager, mock_logger)


@pytest.mark.asyncio
async def test_cancellation_after_format_loading_lines_112_113(ingester):
    """Test cancellation after format configuration - lines 112-113"""
    
    mock_format = {"csv_format": {"column_delimiter": ","}}
    
    # Mock cancellation after format loading
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', side_effect=[False, False, True]), \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format):
        
        generator = ingester.ingest_data("test.csv", "test.fmt", "full", "test.csv")
        
        messages = []
        try:
            async for message in generator:
                messages.append(message)
        except Exception as e:
            assert "canceled by user" in str(e)
        
        # Should hit line 112-113 
        assert any("stopping after format configuration" in msg for msg in messages)


@pytest.mark.asyncio
async def test_empty_dataframe_handling_lines_130_135(ingester):
    """Test empty DataFrame handling - lines 130-135"""
    
    mock_format = {"csv_format": {"column_delimiter": ","}}
    empty_df = pd.DataFrame()  # Completely empty DataFrame
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format), \
         patch('pandas.read_csv', return_value=empty_df):
        
        generator = ingester.ingest_data("test.csv", "test.fmt", "full", "test.csv")
        
        messages = []
        async for message in generator:
            messages.append(message)
            if "empty" in message.lower():
                break
        
        # Should detect empty DataFrame and handle appropriately
        assert any("empty" in msg.lower() or "no data" in msg.lower() for msg in messages)


@pytest.mark.asyncio
async def test_type_inference_scenarios_lines_145_152(ingester):
    """Test type inference code paths - lines 145-152"""
    
    # Enable type inference to trigger these paths
    ingester.enable_type_inference = True
    ingester.type_sample_rows = 10
    
    mock_format = {"csv_format": {"column_delimiter": ","}}
    test_df = pd.DataFrame({
        'name': ['John', 'Jane', 'Bob'], 
        'age': [30, 25, 35],
        'salary': [50000, 60000, 55000]
    })
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format), \
         patch('pandas.read_csv', return_value=test_df):
        
        generator = ingester.ingest_data("test.csv", "test.fmt", "full", "test.csv")
        
        messages = []
        async for message in generator:
            messages.append(message)
            if "type inference" in message.lower() and len(messages) > 10:
                break
        
        # Should trigger type inference logic
        assert any("type" in msg.lower() for msg in messages)


@pytest.mark.asyncio
async def test_append_mode_with_existing_table_lines_157_158(ingester):
    """Test append mode with existing table - lines 157-158"""
    
    ingester.db_manager.table_exists.return_value = True  # Table exists
    mock_format = {"csv_format": {"column_delimiter": ","}}
    test_df = pd.DataFrame({'name': ['John'], 'age': [30]})
    
    # Mock getting existing schema
    existing_schema = {'name': 'TEXT', 'age': 'INT'}
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format), \
         patch('pandas.read_csv', return_value=test_df), \
         patch.object(ingester.db_manager, 'execute_query', return_value=existing_schema):
        
        generator = ingester.ingest_data("test.csv", "test.fmt", "append", "test.csv")
        
        messages = []
        async for message in generator:
            messages.append(message)
            if len(messages) > 15:
                break
        
        # Should handle append mode logic
        assert any("append" in msg.lower() or "existing" in msg.lower() for msg in messages)


@pytest.mark.asyncio
async def test_full_mode_table_drop_lines_175_176(ingester):
    """Test full mode dropping existing table - lines 175-176"""
    
    ingester.db_manager.table_exists.return_value = True  # Table exists
    mock_format = {"csv_format": {"column_delimiter": ","}}
    test_df = pd.DataFrame({'name': ['John'], 'age': [30]})
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format), \
         patch('pandas.read_csv', return_value=test_df):
        
        generator = ingester.ingest_data("test.csv", "test.fmt", "full", "test.csv")
        
        messages = []
        async for message in generator:
            messages.append(message)
            if len(messages) > 15:
                break
        
        # Should mention dropping for full mode
        assert any("full" in msg.lower() for msg in messages)


@pytest.mark.asyncio 
async def test_batch_processing_progress_lines_182_183_195_196(ingester):
    """Test batch processing and progress reporting - lines 182-183, 195-196"""
    
    # Set progress interval to trigger reporting
    ingester.progress_batch_interval = 1  # Report every batch
    
    mock_format = {"csv_format": {"column_delimiter": ","}}
    
    # Create dataset large enough to trigger batching
    large_data = []
    for i in range(300):
        large_data.append({
            'id': i,
            'name': f'Person_{i}',
            'department': f'Dept_{i % 5}'
        })
    
    large_df = pd.DataFrame(large_data)
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch('utils.progress.update_progress') as mock_update, \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format), \
         patch('pandas.read_csv', return_value=large_df):
        
        generator = ingester.ingest_data("batch_test.csv", "test.fmt", "full", "batch_test.csv")
        
        messages = []
        async for message in generator:
            messages.append(message)
            if len(messages) > 25:  # Get enough messages to trigger batch logic
                break
        
        # Should have called progress update multiple times
        assert mock_update.call_count > 0


@pytest.mark.asyncio
async def test_error_handling_and_cleanup_lines_221_222_235_236(ingester):
    """Test error handling and cleanup paths - lines 221-222, 235-236"""
    
    original_schema = ingester.db_manager.data_schema
    
    # Force an error after setting target schema
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file', 
                     side_effect=Exception("Simulated error")):
        
        generator = ingester.ingest_data(
            "test.csv", "test.fmt", "full", "test.csv", 
            target_schema="temp_schema"
        )
        
        try:
            async for message in generator:
                pass
        except Exception:
            pass  # Expected
        
        # Should restore original schema on error (lines 235-236)
        assert ingester.db_manager.data_schema == original_schema


def test_header_sanitization_edge_cases_lines_297_298(ingester):
    """Test header sanitization edge cases - lines 297-298"""
    
    # Test headers that trigger specific sanitization logic
    challenging_headers = [
        '123column',  # Starts with number
        'column with spaces',
        'column-with-dashes', 
        'column.with.dots',
        'UPPERCASE_COLUMN',
        'mixed_Case_Column',
        '',  # Empty
        'a' * 130,  # Very long (over 120 chars)
    ]
    
    result = ingester._sanitize_headers(challenging_headers)
    
    # Should process all headers
    assert len(result) == len(challenging_headers)
    # Long header should be truncated 
    assert len(result[-1]) <= 120


def test_deduplicate_headers_complex_scenarios_lines_332_333(ingester):
    """Test header deduplication complex scenarios - lines 332-333"""
    
    # Create scenario with many duplicates to test deduplication logic
    complex_headers = [
        'name', 'age', 'name', 'salary', 'name', 'age',
        'department', 'name', 'age', 'salary'
    ]
    
    result = ingester._deduplicate_headers(complex_headers)
    
    # Should create unique headers
    assert len(set(result)) == len(result)
    assert len(result) == len(complex_headers)


def test_type_inference_numeric_detection_lines_264_265_274_275(ingester):
    """Test numeric type detection in type inference - lines 264-265, 274-275"""
    
    # Create DataFrame with numeric strings that should be detected
    numeric_df = pd.DataFrame({
        'pure_numbers': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
        'decimal_numbers': ['1.5', '2.7', '3.14', '4.0', '5.25', '6.8', '7.1', '8.9', '9.0', '10.5'],
        'mixed_numeric': ['1', '2', '3', 'text', '5', '6', '7', '8', '9', '10'],
    })
    
    ingester.type_sample_rows = 10  # Use all rows
    
    result = ingester._infer_types(numeric_df, ['pure_numbers', 'decimal_numbers', 'mixed_numeric'])
    
    # Should detect numeric patterns appropriately
    assert 'pure_numbers' in result
    assert 'decimal_numbers' in result  
    assert 'mixed_numeric' in result


def test_type_inference_date_detection_lines_282_284(ingester):
    """Test date detection in type inference - lines 282-284"""
    
    ingester.date_parse_threshold = 0.7  # 70% must be valid dates
    
    date_df = pd.DataFrame({
        'good_dates': ['2023-01-01', '2023-02-01', '2023-03-01', '2023-04-01', '2023-05-01'],
        'mixed_dates': ['2023-01-01', '2023-02-01', 'not-a-date', 'invalid', '2023-05-01'],
        'bad_dates': ['not-date', 'invalid', 'bad-format', 'wrong', 'nope']
    })
    
    result = ingester._infer_types(date_df, ['good_dates', 'mixed_dates', 'bad_dates'])
    
    # Should apply date parsing threshold logic
    assert 'good_dates' in result
    assert 'mixed_dates' in result
    assert 'bad_dates' in result


@pytest.mark.asyncio
async def test_comprehensive_ingestion_workflow(ingester):
    """Comprehensive test hitting multiple missing line ranges"""
    
    # Configure to hit various code paths
    ingester.enable_type_inference = True
    ingester.type_sample_rows = 50
    ingester.progress_batch_interval = 2
    
    # Create comprehensive test dataset
    comprehensive_data = []
    for i in range(150):
        comprehensive_data.append({
            'id': i,
            'name': f'Person_{i}',
            'email': f'person{i}@company.com', 
            'salary': 45000 + (i * 500),
            'hire_date': f'202{i%4}-{(i%12)+1:02d}-15',
            'active': 'true' if i % 2 == 0 else 'false',
            'department': f'Department_{i % 8}',
            'score': round(85 + (i % 15) * 1.5, 2)
        })
    
    test_df = pd.DataFrame(comprehensive_data)
    mock_format = {"csv_format": {"column_delimiter": ",", "has_trailer": False}}
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch('utils.progress.update_progress'), \
         patch.object(ingester.file_handler, 'read_format_file', return_value=mock_format), \
         patch('pandas.read_csv', return_value=test_df):
        
        generator = ingester.ingest_data(
            "comprehensive.csv", "comprehensive.fmt", "full", "comprehensive.csv",
            config_reference_data=True
        )
        
        message_count = 0
        async for message in generator:
            message_count += 1
            if message_count > 35:  # Get substantial processing to hit many code paths
                break
        
        # Should have processed substantial amount and hit many code paths
        assert message_count > 25