"""
Final push to get utils/ingest.py to 70%+ coverage
Targeting the remaining missing block 372-535 and other gaps
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, call
from datetime import datetime

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterFinalPush70:
    """Tests to push coverage to 70%+"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        # Make logger async methods
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_ingest_stage_table_creation_paths(self):
        """Test stage table creation paths - covers lines 250-275"""
        file_path = os.path.join(self.temp_dir, 'stage_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'stage_test.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,category\n1,TestData,A\n2,MoreData,B\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database operations
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        # Mock stage table scenario: main table doesn't exist, stage table exists
        def mock_table_exists(conn, table_name):
            if '_stage' in table_name:
                return True  # Stage table exists
            return False  # Main table doesn't exist
        
        self.mock_db.table_exists.side_effect = mock_table_exists
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.create_table.return_value = None
        self.mock_db.drop_table_if_exists.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='stage_test'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'stage_test.csv'
            ):
                messages.append(message)
            
            # Should handle stage table operations
            assert any("Existing tables found, validating schema" in msg for msg in messages)
            self.mock_db.drop_table_if_exists.assert_called()  # Should clean up stage table
    
    @pytest.mark.asyncio
    async def test_ingest_table_creation_full_mode_paths(self):
        """Test table creation in full mode - covers lines 240-265"""
        file_path = os.path.join(self.temp_dir, 'create_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'create_test.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,value\n1,Create,100\n2,Table,200\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database operations for new table creation
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False  # No existing tables
        self.mock_db.create_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='create_test'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'create_test.csv'
            ):
                messages.append(message)
            
            # Should create new main table
            assert any("fullload mode: main table does not exist, creating new main table" in msg for msg in messages)
            self.mock_db.create_table.assert_called()
    
    @pytest.mark.asyncio
    async def test_ingest_with_type_inference_comprehensive(self):
        """Test complete type inference workflow - covers lines 163-183"""
        file_path = os.path.join(self.temp_dir, 'type_infer.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'type_infer.fmt')
        
        # Create test file with various data types
        csv_content = """id,name,age,date,price,active
1,John Smith,25,2023-01-01,99.99,true
2,Jane Doe,30,2023-01-02,149.50,false
3,Bob Wilson,35,2023-01-03,199.99,true
4,Alice Brown,28,2023-01-04,299.75,false
5,Charlie Davis,32,2023-01-05,399.00,true"""
        
        with open(file_path, 'w') as f:
            f.write(csv_content)
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Enable type inference
        self.ingester.enable_type_inference = True
        self.ingester.type_sample_rows = 3  # Use small sample for test
        
        # Mock database operations
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        # Mock file operations for schema persistence
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='type_infer'), \
             patch('builtins.open', MagicMock()), \
             patch('json.load', return_value=format_config), \
             patch('json.dump'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'type_infer.csv'
            ):
                messages.append(message)
            
            # Should perform type inference
            assert any("Inferring column data types" in msg for msg in messages)
            assert any("Type inference complete" in msg for msg in messages)
            assert any("Inferred schema persisted" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_data_processing_with_large_dataset(self):
        """Test data processing with large dataset to hit batch processing - covers loading paths"""
        file_path = os.path.join(self.temp_dir, 'large_dataset.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'large_dataset.fmt')
        
        # Create larger dataset to trigger batch processing
        csv_lines = ['id,name,category,value']
        for i in range(1, 1001):  # 1000 rows > batch_size (990)
            csv_lines.append(f'{i},Product_{i},Category_{i%5},{i*10.5}')
        
        with open(file_path, 'w') as f:
            f.write('\n'.join(csv_lines))
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database operations
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='large_dataset'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress') as mock_progress, \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'large_dataset.csv'
            ):
                messages.append(message)
            
            # Should process large dataset with progress updates
            assert any("1000 rows" in msg for msg in messages)
            mock_progress.assert_called()  # Should update progress during processing
            mock_cursor.executemany.assert_called()  # Should execute SQL in batches
    
    @pytest.mark.asyncio
    async def test_ingest_data_sql_execution_error_handling(self):
        """Test SQL execution error handling during data loading"""
        file_path = os.path.join(self.temp_dir, 'sql_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'sql_error.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,TestError\n2,MoreError\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database operations with SQL error
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("SQL execution error")
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='sql_error'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'sql_error.csv'
                ):
                    messages.append(message)
            except Exception as e:
                # Should handle SQL execution errors
                assert "SQL execution error" in str(e)
            
            # Should have attempted SQL execution
            mock_cursor.executemany.assert_called()
    
    @pytest.mark.asyncio
    async def test_ingest_metadata_column_handling(self):
        """Test metadata column handling during ingestion"""
        file_path = os.path.join(self.temp_dir, 'metadata_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'metadata_test.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('business_col1,business_col2\nvalue1,value2\nvalue3,value4\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database operations
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='metadata_test'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'metadata_test.csv'
            ):
                messages.append(message)
            
            # Should handle metadata columns
            mock_cursor.executemany.assert_called()
            
            # Check that the SQL includes metadata columns
            if mock_cursor.executemany.call_args_list:
                sql_call = mock_cursor.executemany.call_args_list[0]
                sql_statement = sql_call[0][0]  # First argument is the SQL
                # Should include ref_data metadata columns
                assert 'ref_data_loadtime' in sql_statement
                assert 'ref_data_loadtype' in sql_statement
    
    def test_persist_inferred_schema_file_operations(self):
        """Test schema persistence with file operations - covers lines 702-748"""
        fmt_file_path = os.path.join(self.temp_dir, 'persist_test.fmt')
        
        # Create initial format file
        initial_config = {
            "csv_format": {"delimiter": ",", "has_header": True},
            "existing_data": "should_be_preserved"
        }
        
        import json
        with open(fmt_file_path, 'w') as f:
            json.dump(initial_config, f)
        
        # Test schema persistence
        inferred_types = {
            'col1': 'varchar(100)',
            'col2': 'varchar(50)', 
            'col3': 'varchar(255)'
        }
        
        # Call the method
        self.ingester._persist_inferred_schema(fmt_file_path, inferred_types)
        
        # Verify the file was updated correctly
        with open(fmt_file_path, 'r') as f:
            updated_config = json.load(f)
        
        # Should preserve existing data and add inferred schema
        assert updated_config['csv_format']['delimiter'] == ','
        assert updated_config['existing_data'] == 'should_be_preserved'
        assert 'inferred_schema' in updated_config
        assert updated_config['inferred_schema'] == inferred_types
    
    def test_persist_inferred_schema_error_handling(self):
        """Test schema persistence error handling - covers exception paths"""
        non_existent_file = os.path.join(self.temp_dir, 'missing.fmt')
        inferred_types = {'col1': 'varchar(100)'}
        
        # Should handle missing file gracefully
        with pytest.raises(Exception):
            self.ingester._persist_inferred_schema(non_existent_file, inferred_types)
    
    def test_sanitize_and_deduplicate_headers_edge_cases(self):
        """Test header processing edge cases - covers remaining header logic"""
        
        # Test extreme cases
        extreme_headers = [
            "1234567890" * 20,  # Very long header starting with numbers
            "!@#$%^&*()",        # All special characters
            "    spaces    ",     # Leading/trailing spaces
            "ALLCAPS",           # All caps
            "mixedCASE",         # Mixed case
            "",                  # Empty string
            "中文字符",            # Unicode characters
            "emoji🚀test"        # Emoji in header
        ]
        
        sanitized = self.ingester._sanitize_headers(extreme_headers)
        
        # Should handle all cases
        assert len(sanitized) == len(extreme_headers)
        for header in sanitized:
            if header:  # Non-empty headers
                # Should be valid SQL identifiers
                assert header.replace('_', '').replace('col', '').isalnum() or any(c.isalpha() for c in header)
        
        # Test deduplication with many duplicates
        many_dupes = ['duplicate'] * 50
        deduplicated = self.ingester._deduplicate_headers(many_dupes)
        
        # Should create 50 unique headers
        assert len(set(deduplicated)) == 50
        assert len(deduplicated) == 50