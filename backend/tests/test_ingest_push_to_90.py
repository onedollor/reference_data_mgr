"""
Targeted tests to push utils/ingest.py from 67% to 90% coverage
Focusing on the exact missing lines identified in the coverage report
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock, call, Mock
from datetime import datetime

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterPushTo90:
    """Laser-focused tests to achieve 90% coverage"""
    
    def setup_method(self):
        """Setup test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_db = MagicMock()
        self.mock_logger = MagicMock()
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        
    def teardown_method(self):
        """Cleanup test environment"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
    
    @pytest.mark.asyncio
    async def test_target_schema_restoration(self):
        """Test target schema restoration - covers lines 69-70"""
        file_path = os.path.join(self.temp_dir, 'schema_restore.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'schema_restore.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.data_schema = 'original_schema'
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='schema_restore'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'schema_restore.csv',
                target_schema='custom_schema'
            ):
                messages.append(message)
            
            # Should restore original schema after completion (line 69-70)
            # The method should set and restore the schema
            assert self.mock_db.data_schema == 'original_schema'  # Restored
    
    @pytest.mark.asyncio
    async def test_stage_table_dropping_path(self):
        """Test stage table dropping - covers lines 302-305"""
        file_path = os.path.join(self.temp_dir, 'stage_drop.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'stage_drop.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,value\n1,TestDrop,100\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup where stage table exists
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        # Mock table existence: main table doesn't exist, stage table exists
        def mock_table_exists(conn, table_name):
            if '_stage' in table_name:
                return True  # Stage table exists
            return False  # Main table doesn't exist
        
        self.mock_db.table_exists.side_effect = mock_table_exists
        self.mock_db.create_table.return_value = None
        self.mock_db.drop_table_if_exists.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='stage_drop'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'stage_drop.csv'
            ):
                messages.append(message)
            
            # Should hit stage table dropping path (lines 302-305)
            assert any("Stage table exists, dropping and recreating" in msg for msg in messages)
            assert any("Existing stage table dropped" in msg for msg in messages)
            self.mock_db.drop_table_if_exists.assert_called()
    
    @pytest.mark.asyncio
    async def test_new_stage_table_creation_path(self):
        """Test new stage table creation - covers lines 306-313"""
        file_path = os.path.join(self.temp_dir, 'new_stage.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'new_stage.fmt')
        
        # Create test file with multiple columns
        with open(file_path, 'w') as f:
            f.write('id,name,category,price,description,status\n1,Product,Electronics,99.99,Description,Active\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup - no existing tables
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False  # No existing tables
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='new_stage'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'new_stage.csv'
            ):
                messages.append(message)
            
            # Should hit new stage table creation path (lines 306-313)
            assert any("Creating new stage table to match input file columns" in msg for msg in messages)
            assert any("Stage table recreated with 6 data columns" in msg for msg in messages)
            assert any("Stage table ready for data loading" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_data_processing_and_column_mapping(self):
        """Test data processing and column mapping - covers lines 320-328"""
        file_path = os.path.join(self.temp_dir, 'data_process.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'data_process.fmt')
        
        # Create test file with mixed data types and NaN values
        with open(file_path, 'w') as f:
            f.write('id with spaces,name-with-dashes,value.with.dots\n1,John,100.5\n2,Jane,nan\n3,Bob,None\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='data_process'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'data_process.csv'
            ):
                messages.append(message)
            
            # Should hit data processing path (lines 320-328)
            assert any("Processing CSV data" in msg for msg in messages)
            assert any("Data processing completed" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_cancellation_after_data_processing(self):
        """Test cancellation after data processing - covers lines 332-333"""
        file_path = os.path.join(self.temp_dir, 'cancel_data_process.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_data_process.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock cancellation after data processing
        call_count = 0
        def mock_cancel_after_processing(key):
            nonlocal call_count
            call_count += 1
            return call_count >= 8  # Cancel after data processing
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_data_process'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_after_processing), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_data_process.csv',
                    progress_key='cancel_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should hit cancellation after data processing (lines 332-333)
            assert any("Cancellation requested - stopping after data processing" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_cancellation_after_stage_loading(self):
        """Test cancellation after stage loading - covers lines 356-357"""
        file_path = os.path.join(self.temp_dir, 'cancel_stage_load.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_stage_load.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock cancellation after stage loading
        call_count = 0
        def mock_cancel_after_stage_load(key):
            nonlocal call_count
            call_count += 1
            return call_count >= 10  # Cancel after stage loading
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_stage_load'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_after_stage_load), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_stage_load.csv',
                    progress_key='cancel_stage_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should hit cancellation after stage loading (lines 356-357)
            assert any("Cancellation requested - stopping after stage table loading" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_cancellation_before_validation(self):
        """Test cancellation before validation - covers lines 361-362"""
        file_path = os.path.join(self.temp_dir, 'cancel_before_valid.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_before_valid.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock cancellation before validation
        call_count = 0
        def mock_cancel_before_validation(key):
            nonlocal call_count
            call_count += 1
            return call_count >= 11  # Cancel before validation
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_before_valid'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_before_validation), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_before_valid.csv',
                    progress_key='cancel_valid_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should hit cancellation before validation (lines 361-362)
            assert any("Cancellation requested - stopping before validation" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_validation_execution_with_issues(self):
        """Test validation execution with issues - covers lines 368-383"""
        file_path = os.path.join(self.temp_dir, 'validation_issues.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'validation_issues.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,status\n1,Invalid Data,BAD\n2,More Invalid,ERROR\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock validation with issues
        validation_result = {
            'validation_result': 3,  # 3 issues
            'validation_issue_list': [
                {'issue_id': 'V001', 'issue_detail': 'Invalid status value BAD'},
                {'issue_id': 'V002', 'issue_detail': 'Invalid status value ERROR'},  
                {'issue_id': 'V003', 'issue_detail': 'Missing required field'}
            ]
        }
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='validation_issues'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel') as mock_cancel, \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'validation_issues.csv',
                progress_key='validation_key', config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit validation with issues path (lines 368-383)
            assert any("Executing data validation" in msg for msg in messages)
            assert any("ERROR! Validation failed with 3 issues" in msg for msg in messages)
            assert any("Issue V001: Invalid status value BAD" in msg for msg in messages)
            assert any("Issue V002: Invalid status value ERROR" in msg for msg in messages)
            assert any("Issue V003: Missing required field" in msg for msg in messages)
            assert any("Data remains in stage table for review" in msg for msg in messages)
            assert any("automatically canceled due to validation errors" in msg for msg in messages)
            
            # Should have auto-canceled
            mock_cancel.assert_called_with('validation_key')
    
    @pytest.mark.asyncio
    async def test_validation_success_path(self):
        """Test validation success path - covers lines 385-386"""
        file_path = os.path.join(self.temp_dir, 'validation_success.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'validation_success.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,status\n1,Valid Data,GOOD\n2,More Valid,OK\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        self.mock_db.get_table_columns.return_value = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'name', 'data_type': 'varchar'},
            {'name': 'status', 'data_type': 'varchar'}
        ]
        
        # Mock successful validation
        validation_result = {
            'validation_result': 0,  # No issues
            'validation_issue_list': []
        }
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='validation_success'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'validation_success.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit validation success path (lines 385-386)
            assert any("Data validation passed" in msg for msg in messages)
    
    def test_initialization_edge_cases(self):
        """Test initialization edge cases - covers lines 31-32, 37-38"""
        # Test with various environment variable configurations
        test_cases = [
            # Case 1: Invalid progress interval (should use default)
            {'INGEST_PROGRESS_INTERVAL': 'not_a_number'},
            # Case 2: Invalid sample rows (should use default)
            {'INGEST_TYPE_SAMPLE_ROWS': 'invalid'},
            # Case 3: Both invalid
            {'INGEST_PROGRESS_INTERVAL': 'bad', 'INGEST_TYPE_SAMPLE_ROWS': 'bad'},
        ]
        
        for env_vars in test_cases:
            with patch.dict('os.environ', env_vars):
                ingester = DataIngester(self.mock_db, self.mock_logger)
                # Should use default values when parsing fails
                assert ingester.progress_batch_interval == 5  # Default
                assert ingester.type_sample_rows == 5000  # Default
    
    def test_header_processing_comprehensive_edge_cases(self):
        """Test comprehensive header processing - covers remaining header logic"""
        # Test extreme edge cases that might not be covered
        extreme_cases = [
            # Headers with numbers, spaces, special chars
            ["123", "456", "789"],  # All numeric
            ["", "", ""],  # All empty
            [" ", "  ", "   "],  # All spaces
            ["🚀", "💻", "🎯"],  # All emoji
            ["a" * 100, "b" * 100, "c" * 100],  # Very long headers
        ]
        
        for headers in extreme_cases:
            sanitized = self.ingester._sanitize_headers(headers)
            deduplicated = self.ingester._deduplicate_headers(sanitized)
            
            # Should handle all cases without errors
            assert len(deduplicated) == len(headers)
            assert len(set(deduplicated)) == len(deduplicated)  # All unique
    
    def test_type_inference_edge_cases(self):
        """Test type inference edge cases - covers lines 182-183"""
        # Test with problematic data that might cause inference issues
        problematic_df = pd.DataFrame({
            'all_null': [None, None, None, None, None],
            'mixed_null': [1, None, 'text', None, 2.5],
            'all_same': ['same', 'same', 'same', 'same', 'same'],
            'empty_strings': ['', '', '', '', ''],
            'mixed_types': [1, 'text', 3.14, True, None]
        })
        
        # Should handle problematic data gracefully
        result = self.ingester._infer_types(problematic_df, list(problematic_df.columns))
        
        assert isinstance(result, dict)
        assert len(result) == len(problematic_df.columns)
        # All should be varchar since that's what the method returns
        for col_type in result.values():
            assert 'varchar' in col_type