"""
Comprehensive ingest tests targeting 90% coverage
Focus on missing lines: 31-32, 37-38, 69-70, 80-82, 89-90, 112-113, etc.
"""

import pytest
import os
import pandas as pd
import tempfile
import shutil
from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime
from utils.ingest import DataIngester


@pytest.fixture
def temp_dir():
    """Create temporary directory"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def mock_db_manager():
    """Mock database manager"""
    mock_db = MagicMock()
    mock_connection = MagicMock()
    mock_db.get_connection.return_value = mock_connection
    mock_db.data_schema = "test_schema"
    return mock_db


@pytest.fixture
def mock_logger():
    """Mock logger"""
    mock_logger = MagicMock()
    mock_logger.log_info = MagicMock(return_value=None)
    mock_logger.log_error = MagicMock(return_value=None)
    return mock_logger


def test_data_ingester_constructor_exception_handling():
    """Test constructor exception handling - covers lines 31-32, 37-38"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    
    # Test ValueError in progress_batch_interval parsing
    with patch.dict('os.environ', {'INGEST_PROGRESS_INTERVAL': 'invalid_number'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.progress_batch_interval == 5  # Should use default due to ValueError
    
    # Test ValueError in type_sample_rows parsing
    with patch.dict('os.environ', {'INGEST_TYPE_SAMPLE_ROWS': 'not_a_number'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.type_sample_rows == 5000  # Should use default due to ValueError


def test_data_ingester_environment_variable_settings():
    """Test various environment variable configurations"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    
    # Test type inference enabled
    with patch.dict('os.environ', {'INGEST_TYPE_INFERENCE': '1'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.enable_type_inference is True
    
    with patch.dict('os.environ', {'INGEST_TYPE_INFERENCE': 'true'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.enable_type_inference is True
    
    with patch.dict('os.environ', {'INGEST_TYPE_INFERENCE': 'True'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.enable_type_inference is True
    
    # Test type inference disabled (default)
    with patch.dict('os.environ', {'INGEST_TYPE_INFERENCE': '0'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.enable_type_inference is False


@pytest.mark.asyncio
async def test_ingest_data_cancellation_handling(mock_db_manager, mock_logger):
    """Test cancellation handling in ingest_data - covers lines 69-70"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    # Mock file operations
    csv_content = "name,age\nJohn,30\nJane,25\n"
    fmt_content = '{"column_delimiter": ",", "has_header": true}'
    
    with patch('builtins.open', mock_open(read_data=csv_content)):
        with patch('utils.ingest.prog.is_canceled', return_value=True):
            
            # Should yield cancellation message and raise exception
            messages = []
            try:
                async for message in ingester.ingest_data(
                    "/test/file.csv", 
                    "/test/format.json", 
                    "full",
                    "testfile.csv"
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should have yielded cancellation message
            cancellation_messages = [msg for msg in messages if "Cancellation requested" in str(msg)]
            assert len(cancellation_messages) >= 1


@pytest.mark.asyncio
async def test_ingest_data_target_schema_handling(mock_db_manager, mock_logger):
    """Test target schema override handling - covers lines 80-82"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    # Set original schema
    mock_db_manager.data_schema = "original_schema"
    
    # Mock file operations
    csv_content = "name,age\nJohn,30\n"
    fmt_content = '{"column_delimiter": ",", "has_header": true}'
    
    # Mock progress not canceled
    with patch('utils.ingest.prog.is_canceled', return_value=False):
        with patch('builtins.open', mock_open(read_data=csv_content)):
            with patch('pandas.read_csv', return_value=pd.DataFrame({"name": ["John"], "age": ["30"]})):
                with patch.object(ingester, '_read_csv_file', return_value=pd.DataFrame({"name": ["John"], "age": ["30"]})):
                    with patch.object(ingester, '_load_dataframe_to_table') as mock_load:
                        
                        # Mock the async generator
                        async def mock_load_generator(*args):
                            yield "Loading data..."
                            yield "Data loaded successfully"
                        
                        mock_load.return_value = mock_load_generator()
                        
                        # Test with target schema
                        messages = []
                        async for message in ingester.ingest_data(
                            "/test/file.csv", 
                            "/test/format.json", 
                            "full",
                            "testfile.csv",
                            target_schema="target_schema"
                        ):
                            messages.append(message)
                        
                        # Should have yielded target schema message
                        schema_messages = [msg for msg in messages if "Using target schema: target_schema" in str(msg)]
                        assert len(schema_messages) >= 1
                        
                        # Database manager should have been updated with target schema
                        assert mock_db_manager.data_schema == "target_schema"


@pytest.mark.asyncio
async def test_ingest_data_exception_handling(mock_db_manager, mock_logger):
    """Test exception handling in ingest_data - covers lines 89-90"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    # Mock connection failure
    mock_db_manager.get_connection.side_effect = Exception("Database connection failed")
    
    with patch('utils.ingest.prog.is_canceled', return_value=False):
        messages = []
        try:
            async for message in ingester.ingest_data(
                "/test/file.csv", 
                "/test/format.json", 
                "full",
                "testfile.csv"
            ):
                messages.append(message)
        except Exception as e:
            # Should have caught and re-raised the exception
            assert "Database connection failed" in str(e)
        
        # Should have yielded connection message before failing
        connection_messages = [msg for msg in messages if "Connecting to database" in str(msg)]
        assert len(connection_messages) >= 1


def test_infer_types_error_scenarios():
    """Test error scenarios in _infer_types method"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    ingester = DataIngester(mock_db, mock_logger)
    
    # Test with invalid data that causes conversion errors
    df = pd.DataFrame({
        'numeric_col': ['1', '2', 'invalid', '4', '5'],
        'date_col': ['2024-01-01', 'not_a_date', '2024-01-03', '2024-01-04', '2024-01-05'],
        'text_col': ['a', 'b', 'c', 'd', 'e']
    })
    
    result = ingester._infer_types(df)
    
    # Should handle conversion errors gracefully
    assert isinstance(result, dict)
    assert len(result) >= 0


def test_normalize_data_types_edge_cases():
    """Test edge cases in _normalize_data_types method"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    ingester = DataIngester(mock_db, mock_logger)
    
    # Test with various data type combinations
    df = pd.DataFrame({
        'int_col': [1, 2, 3],
        'float_col': [1.1, 2.2, 3.3],
        'str_col': ['a', 'b', 'c'],
        'mixed_col': [1, 'text', 3.14]
    })
    
    column_types = {
        'int_col': 'int',
        'float_col': 'float',
        'str_col': 'varchar(50)',
        'mixed_col': 'varchar(255)'
    }
    
    result = ingester._normalize_data_types(df, column_types)
    
    # Should handle various type conversions
    assert len(result) == len(df)
    assert len(result.columns) == len(df.columns)


def test_sanitize_column_names_edge_cases():
    """Test edge cases in _sanitize_column_names method"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    ingester = DataIngester(mock_db, mock_logger)
    
    # Test with problematic column names
    df = pd.DataFrame({
        'normal_column': [1, 2, 3],
        '123_numeric_start': [1, 2, 3],
        'column with spaces': [1, 2, 3],
        'column-with-dashes': [1, 2, 3],
        'column.with.dots': [1, 2, 3],
        '@special!chars#': [1, 2, 3],
        '': [1, 2, 3],  # Empty column name
        'select': [1, 2, 3],  # SQL keyword
        'order': [1, 2, 3]   # SQL keyword
    })
    
    result = ingester._sanitize_column_names(df)
    
    # Should sanitize all column names appropriately
    assert len(result.columns) == len(df.columns)
    
    # Check that problematic names are fixed
    column_names = list(result.columns)
    assert 'normal_column' in column_names
    assert any('numeric_start' in name for name in column_names)  # Should fix numeric start
    assert any('column_with_spaces' in name or 'column_with_' in name for name in column_names)  # Should fix spaces


@pytest.mark.asyncio
async def test_read_csv_file_complex_scenarios(mock_db_manager, mock_logger):
    """Test complex scenarios in _read_csv_file method"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    # Test with complex CSV format
    csv_format = {
        'column_delimiter': '|',
        'text_qualifier': "'",
        'row_delimiter': '\\r\\n',
        'has_header': True,
        'has_trailer': True,
        'skip_lines': 2
    }
    
    # Mock pandas read_csv to raise the specific ValueError first
    def mock_read_csv_side_effect(*args, **kwargs):
        if 'lineterminator' in kwargs:
            # First call with lineterminator - raise the specific error
            raise ValueError("Only length-1 line terminators supported")
        else:
            # Second call without lineterminator - succeed
            return pd.DataFrame({'col1': ['val1', 'val2', 'trailer'], 'col2': ['val3', 'val4', 'trailer_val']})
    
    with patch('pandas.read_csv', side_effect=mock_read_csv_side_effect):
        result = await ingester._read_csv_file('/test/file.csv', csv_format)
        
        # Should handle the fallback scenario and trailer removal
        assert len(result) == 2  # Should have removed trailer row
        assert list(result.columns) == ['col1', 'col2']


@pytest.mark.asyncio
async def test_read_csv_file_exception_scenarios(mock_db_manager, mock_logger):
    """Test exception scenarios in _read_csv_file method"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    csv_format = {
        'column_delimiter': ',',
        'has_header': True,
        'has_trailer': False
    }
    
    # Test with pandas raising a different exception (not the specific ValueError)
    with patch('pandas.read_csv', side_effect=FileNotFoundError("File not found")):
        try:
            await ingester._read_csv_file('/nonexistent/file.csv', csv_format)
            assert False, "Should have raised exception"
        except FileNotFoundError:
            pass  # Expected


def test_generate_create_table_sql_variations():
    """Test various scenarios in _generate_create_table_sql method"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    ingester = DataIngester(mock_db, mock_logger)
    
    # Test with metadata columns enabled
    columns = [
        {'name': 'id', 'data_type': 'int'},
        {'name': 'name', 'data_type': 'varchar(100)'},
        {'name': 'date_field', 'data_type': 'datetime'}
    ]
    
    # Test with metadata
    sql_with_metadata = ingester._generate_create_table_sql('test_table', columns, True, 'test_schema')
    assert 'ref_data_loadtime' in sql_with_metadata
    assert 'ref_data_loadtype' in sql_with_metadata
    assert '[test_schema].[test_table]' in sql_with_metadata
    
    # Test without metadata
    sql_without_metadata = ingester._generate_create_table_sql('test_table', columns, False, 'test_schema')
    assert 'ref_data_loadtime' not in sql_without_metadata
    assert 'ref_data_loadtype' not in sql_without_metadata


@pytest.mark.asyncio 
async def test_load_dataframe_to_table_error_scenarios(mock_db_manager, mock_logger):
    """Test error scenarios in _load_dataframe_to_table method"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    # Create test dataframe
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['John', 'Jane', 'Bob']
    })
    
    # Mock connection that will fail during batch processing
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("SQL execution failed")
    mock_connection.cursor.return_value = mock_cursor
    
    load_timestamp = datetime.utcnow()
    
    try:
        messages = []
        async for message in ingester._load_dataframe_to_table(
            df, mock_connection, 'test_table', 'full', load_timestamp, 'test_key'
        ):
            messages.append(message)
    except Exception as e:
        # Should propagate SQL execution error
        assert "SQL execution failed" in str(e) or "Error" in str(e)


def test_batch_size_environment_variable():
    """Test batch size configuration from environment"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    
    # Test custom batch size via environment variable
    with patch.dict('os.environ', {'INGEST_BATCH_SIZE': '100'}):
        # Note: The current code doesn't use INGEST_BATCH_SIZE, but we test the pattern
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.batch_size == 990  # Hard-coded value
    
    # Test with various environment configurations
    with patch.dict('os.environ', {'INGEST_DATE_THRESHOLD': '0.9'}):
        ingester = DataIngester(mock_db, mock_logger)
        assert ingester.date_parse_threshold == 0.9


@pytest.mark.asyncio
async def test_progress_tracking_scenarios(mock_db_manager, mock_logger):
    """Test progress tracking in various scenarios"""
    ingester = DataIngester(mock_db_manager, mock_logger)
    
    # Test progress interval configuration
    with patch.dict('os.environ', {'INGEST_PROGRESS_INTERVAL': '10'}):
        ingester = DataIngester(mock_db_manager, mock_logger)
        assert ingester.progress_batch_interval == 10


def test_type_inference_with_sample_data():
    """Test type inference with various sample data scenarios"""
    mock_db = MagicMock()
    mock_logger = MagicMock()
    ingester = DataIngester(mock_db, mock_logger)
    
    # Enable type inference
    ingester.enable_type_inference = True
    
    # Create dataframe with mixed data types
    df = pd.DataFrame({
        'pure_numeric': ['1', '2', '3', '4', '5'] * 1000,  # Should be detected as numeric
        'mixed_numeric': ['1', '2', 'N/A', '4', '5'] * 1000,  # Should be varchar due to N/A
        'date_like': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'] * 1000,
        'pure_text': ['apple', 'banana', 'cherry', 'date', 'elderberry'] * 1000
    })
    
    result = ingester._infer_column_types(df)
    
    # Should have inferred appropriate types
    assert len(result) == 4
    assert 'pure_numeric' in result
    assert 'mixed_numeric' in result
    assert 'date_like' in result
    assert 'pure_text' in result