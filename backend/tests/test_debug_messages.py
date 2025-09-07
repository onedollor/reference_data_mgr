"""Debug test to see what messages are generated"""
import pytest
import asyncio
import tempfile
import os
import json
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from utils.ingest import DataIngester


@pytest.mark.asyncio
async def test_debug_messages():
    """Debug test to see actual messages"""
    # Create mock database manager
    mock_db = Mock()
    mock_db.get_connection.return_value = Mock()
    mock_db.data_schema = "test_schema"
    mock_db.ensure_schemas_exist = Mock()
    mock_db.table_exists.return_value = True
    mock_db.get_table_row_count.return_value = 50  # Main table has 50 rows
    mock_db.get_row_count.return_value = 50  # Main table has 50 rows
    mock_db.backup_existing_data.return_value = 75  # Backup operation returns 75 rows
    mock_db.determine_load_type.return_value = 'append'
    mock_db.create_stage_table = Mock()
    mock_db.create_backup_table = Mock()
    mock_db.ensure_backup_table_metadata_columns.return_value = {
        'added': [{'column': 'ref_data_version_id'}]
    }
    
    # Create mock logger
    mock_logger = Mock()
    mock_logger.log_info = AsyncMock()
    mock_logger.log_error = AsyncMock()
    mock_logger.log_warning = AsyncMock()
    
    # Create mock file handler
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
    mock_file_handler.move_to_archive.return_value = "/archive/path/file.csv"
    
    # Create ingester with mocks
    ingester = DataIngester(mock_db, mock_logger)
    ingester.file_handler = mock_file_handler
    
    # Create temp CSV file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write('name,age\nJohn,25\nJane,30\n')
        file_path = f.name
    
    # Create temp fmt file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.fmt', delete=False) as f:
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
        fmt_file_path = f.name
    
    try:
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # Mock pandas DataFrame
            import pandas as pd
            mock_df = Mock()
            mock_df.shape = (2, 2) 
            mock_df.columns = ['name', 'age']
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=2)
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=Mock())
            mock_df.iloc[0].to_dict.return_value = {'name': 'John', 'age': 25}
            mock_df.dtypes = pd.Series(['object', 'int64'], index=['name', 'age'])
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in ingester.ingest_data(file_path, fmt_file_path, 'full', 'test.csv'):
                messages.append(message)
                print(f"MESSAGE: {message}")
                if len(messages) > 20:  # Prevent infinite loop
                    break
    
    finally:
        # Cleanup
        if os.path.exists(file_path):
            os.unlink(file_path)
        if os.path.exists(fmt_file_path):
            os.unlink(fmt_file_path)


if __name__ == "__main__":
    asyncio.run(test_debug_messages())