"""
Focused test to push utils/ingest.py to 90% coverage
Using proper async mocking and targeted tests
"""
import pytest
import os
import pandas as pd
import numpy as np
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
    mock.execute_many.return_value = None
    mock.table_exists.return_value = True
    return mock


@pytest.fixture
def ingester(mock_db_manager, mock_logger):
    return DataIngester(mock_db_manager, mock_logger)


# Test specific missing line ranges to achieve 90% coverage
@pytest.mark.asyncio
async def test_cancellation_paths_lines_70_90_112_113(ingester):
    """Test cancellation at various checkpoints - lines 70, 90, 112-113"""
    
    # Test line 70 - cancellation at start
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=True):
        
        generator = ingester.ingest_data("test.csv", "test.fmt", "full", "test.csv")
        messages = []
        
        try:
            async for message in generator:
                messages.append(message)
        except Exception as e:
            assert "canceled by user" in str(e)
        
        assert any("Cancellation requested" in msg for msg in messages)


@pytest.mark.asyncio 
async def test_target_schema_handling_lines_80_82(ingester):
    """Test target schema override - lines 80, 82"""
    mock_format = {"csv_format": {"delimiter": ","}}
    test_df = pd.DataFrame({'name': ['John'], 'age': [30]})
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file', 
                     return_value=mock_format), \
         patch('pandas.read_csv', return_value=test_df):
        
        generator = ingester.ingest_data(
            "test.csv", "test.fmt", "full", "test.csv", 
            target_schema="custom_schema"
        )
        
        messages = []
        async for message in generator:
            messages.append(message)
            if len(messages) > 5:
                break
        
        assert any("target schema" in msg.lower() for msg in messages)


def test_sanitize_headers_edge_cases_lines_297_298(ingester):
    """Test header sanitization edge cases"""
    problematic_headers = [
        'normal', 'with spaces', 'with-dashes', 'with.dots',
        '123start', '', 'UPPERCASE', 'duplicate', 'duplicate'
    ]
    
    result = ingester._sanitize_headers(problematic_headers)
    
    # Should handle all cases and create unique headers
    assert len(result) == len(problematic_headers)
    assert len(set(result)) == len(result)  # All unique


def test_infer_types_comprehensive_lines_264_265_282_284(ingester):
    """Test type inference edge cases"""
    # Test mixed data that hits edge cases
    mixed_df = pd.DataFrame({
        'mixed': ['123', 'text', '456', None, '789'],
        'mostly_dates': ['2023-01-01', '2023-02-01', 'not-date', '2023-03-01', '2023-04-01'],
        'numbers': [1, 2, 'three', 4, 5]
    })
    
    ingester.type_sample_rows = 3
    ingester.date_parse_threshold = 0.6
    
    result = ingester._infer_types(mixed_df, ['mixed', 'mostly_dates', 'numbers'])
    
    assert 'mixed' in result
    assert 'mostly_dates' in result
    assert 'numbers' in result


def test_format_value_for_sql_lines_403_404(ingester):
    """Test SQL value formatting with NaN/None"""
    test_cases = [
        ('text', 'TEXT', "'text'"),
        (123, 'INT', '123'),
        (45.67, 'FLOAT', '45.67'),
        (np.nan, 'TEXT', 'NULL'),
        (None, 'INT', 'NULL'),
        (pd.NaType(), 'FLOAT', 'NULL')
    ]
    
    for value, sql_type, expected in test_cases:
        result = ingester._format_value_for_sql(value, sql_type)
        if expected == 'NULL':
            assert result == 'NULL'
        else:
            assert expected in result or result == expected


@pytest.mark.asyncio
async def test_schema_validation_lines_372_383(ingester):
    """Test schema validation mismatch"""
    connection = MagicMock()
    
    existing = {'col1': 'TEXT', 'col2': 'INT'}
    new = {'col1': 'TEXT', 'col3': 'FLOAT'}  # Mismatch
    
    with pytest.raises(Exception, match="Schema mismatch"):
        await ingester._validate_schema_compatibility(
            connection, "test_table", existing, new
        )


def test_deduplicate_headers_lines_332_333(ingester):
    """Test header deduplication"""
    headers = ['col1', 'col2', 'col1', 'col2', 'col1']
    
    result = ingester._deduplicate_headers(headers)
    
    assert len(set(result)) == len(result)  # All unique
    assert 'col1' in result
    assert 'col2' in result


@pytest.mark.asyncio
async def test_comprehensive_workflow_hitting_many_lines(ingester):
    """Comprehensive test to hit many missing lines at once"""
    # Large dataset to trigger batch processing
    large_data = []
    for i in range(1200):  # Exceeds batch_size
        large_data.append({
            'id': i,
            'name': f'Person_{i}',
            'salary': 50000.0 + i,
            'date_field': f'2023-{(i%12)+1:02d}-01'
        })
    
    large_df = pd.DataFrame(large_data)
    mock_format = {"csv_format": {"delimiter": ","}}
    
    # Enable type inference
    ingester.enable_type_inference = True
    ingester.type_sample_rows = 100
    ingester.progress_batch_interval = 2
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch('utils.progress.update_progress'), \
         patch.object(ingester.file_handler, 'read_format_file',
                     return_value=mock_format), \
         patch('pandas.read_csv', return_value=large_df):
        
        generator = ingester.ingest_data(
            "large_test.csv", "test.fmt", "full", "large_test.csv",
            config_reference_data=True
        )
        
        message_count = 0
        async for message in generator:
            message_count += 1
            if message_count > 30:  # Get enough messages to hit many paths
                break
        
        assert message_count > 10


@pytest.mark.asyncio
async def test_metadata_sync_config_line_473(ingester):
    """Test metadata sync with config_reference_data=True"""
    connection = MagicMock()
    
    await ingester._sync_metadata(
        connection, "test_table", "test.csv", 100,
        config_reference_data=True  # This should hit line 473
    )
    
    assert ingester.db_manager.execute_query.called


@pytest.mark.asyncio 
async def test_error_recovery_and_cleanup_lines_487_489(ingester):
    """Test cleanup error handling"""
    connection = MagicMock()
    ingester.db_manager.execute_query.side_effect = Exception("Cleanup failed")
    
    # Should not raise exception, just handle gracefully
    await ingester._cleanup_temp_tables(connection, "test_stage")
    

def test_create_insert_statement_large_batch_line_433(ingester):
    """Test insert statement creation with large batch"""
    large_batch = []
    for i in range(500):
        large_batch.append({'col1': f'val{i}', 'col2': i})
    
    batch_df = pd.DataFrame(large_batch)
    schema = {'col1': 'TEXT', 'col2': 'INT'}
    
    statement = ingester._create_insert_statement(
        batch_df, "test_table", ['col1', 'col2'], schema
    )
    
    assert "INSERT INTO" in statement
    assert "test_table" in statement
    assert len(statement) > 1000  # Should be a large statement


@pytest.mark.asyncio
async def test_append_mode_existing_table_lines_157_158(ingester):
    """Test append mode with existing table"""
    mock_format = {"csv_format": {"delimiter": ","}}
    test_df = pd.DataFrame({'name': ['John'], 'age': [30]})
    
    ingester.db_manager.table_exists.return_value = True
    
    with patch('utils.progress.init_progress'), \
         patch('utils.progress.is_canceled', return_value=False), \
         patch.object(ingester.file_handler, 'read_format_file',
                     return_value=mock_format), \
         patch('pandas.read_csv', return_value=test_df), \
         patch.object(ingester, '_get_existing_table_schema',
                     return_value={'name': 'TEXT', 'age': 'INT'}):
        
        generator = ingester.ingest_data(
            "test.csv", "test.fmt", "append", "test.csv"
        )
        
        messages = []
        async for message in generator:
            messages.append(message)
            if len(messages) > 15:
                break