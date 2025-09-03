"""
Targeted test to push utils/ingest.py to 70%+ by focusing on lines 372-535
This covers validation logic, stage-to-main table data movement, and final processing
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
from unittest.mock import patch, MagicMock, AsyncMock, call
from datetime import datetime

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterFinal70Plus:
    """Laser-focused tests targeting lines 372-535"""
    
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
    async def test_ingest_with_validation_failure(self):
        """Test validation failure scenario - covers lines 372-383"""
        file_path = os.path.join(self.temp_dir, 'validation_fail.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'validation_fail.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,value\n1,Invalid,BAD_DATA\n2,Data,MORE_BAD\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock validation API to return validation failures
        mock_validation_api = MagicMock()
        validation_result = {
            'status': 'validation_failed',
            'validation_issues': 2,
            'validation_issue_list': [
                {'issue_id': 'V001', 'issue_detail': 'Invalid data format in value column'},
                {'issue_id': 'V002', 'issue_detail': 'Missing required data in name column'}
            ]
        }
        mock_validation_api.validate_data.return_value = validation_result
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='validation_fail'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel') as mock_cancel, \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)), \
             patch('utils.ingest.ReferenceDataAPI', return_value=mock_validation_api):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'validation_fail.csv',
                config_reference_data=True, progress_key='validation_key'
            ):
                messages.append(message)
            
            # Should hit validation failure paths (lines 372-383)
            assert any("ERROR! Validation failed with 2 issues" in msg for msg in messages)
            assert any("Issue V001: Invalid data format" in msg for msg in messages)
            assert any("Issue V002: Missing required data" in msg for msg in messages)
            assert any("Data remains in stage table for review" in msg for msg in messages)
            assert any("automatically canceled due to validation errors" in msg for msg in messages)
            
            # Should have auto-canceled due to validation failure
            mock_cancel.assert_called_with('validation_key')
    
    @pytest.mark.asyncio
    async def test_ingest_with_validation_success(self):
        """Test validation success and data movement - covers lines 385-411"""
        file_path = os.path.join(self.temp_dir, 'validation_success.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'validation_success.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,category\n1,Valid Product,Electronics\n2,Another Product,Books\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock validation API to return success
        mock_validation_api = MagicMock()
        validation_result = {
            'status': 'validation_passed',
            'validation_issues': 0,
            'validation_issue_list': []
        }
        mock_validation_api.validate_data.return_value = validation_result
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.get_table_columns.return_value = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'name', 'data_type': 'varchar'}, 
            {'name': 'category', 'data_type': 'varchar'}
        ]
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='validation_success'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)), \
             patch('utils.ingest.ReferenceDataAPI', return_value=mock_validation_api):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'validation_success.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit validation success paths (lines 385-411)
            assert any("Data validation passed" in msg for msg in messages)
            assert any("Preparing for full load" in msg for msg in messages)
            assert any("Moving data from stage to main table" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_append_mode_with_validation(self):
        """Test append mode with validation - covers lines 396-397"""
        file_path = os.path.join(self.temp_dir, 'append_validate.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'append_validate.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n100,Append1\n101,Append2\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock validation API to return success
        mock_validation_api = MagicMock()
        validation_result = {
            'status': 'validation_passed',
            'validation_issues': 0
        }
        mock_validation_api.validate_data.return_value = validation_result
        
        # Mock database setup for append mode
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = True  # Existing table for append
        self.mock_db.get_row_count.return_value = 50
        self.mock_db.get_table_columns.return_value = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'name', 'data_type': 'varchar'}
        ]
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='append_validate'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)), \
             patch('utils.ingest.ReferenceDataAPI', return_value=mock_validation_api):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'append_validate.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit append mode message (lines 396-397)
            assert any("Append mode: will insert new rows into existing main table" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_cancellation_after_validation(self):
        """Test cancellation after validation - covers lines 389-391"""
        file_path = os.path.join(self.temp_dir, 'cancel_after_validation.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_after_validation.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock validation API to return success
        mock_validation_api = MagicMock()
        validation_result = {
            'status': 'validation_passed',
            'validation_issues': 0
        }
        mock_validation_api.validate_data.return_value = validation_result
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        # Mock cancellation after validation
        call_count = 0
        def mock_cancel_after_validation(key):
            nonlocal call_count
            call_count += 1
            # Cancel after validation (multiple progress checks happen)
            return call_count >= 10  # Cancel later in the process
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_after_validation'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_after_validation), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)), \
             patch('utils.ingest.ReferenceDataAPI', return_value=mock_validation_api):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_after_validation.csv',
                    config_reference_data=True, progress_key='cancel_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should hit one of the cancellation points after validation
            should_have_cancel_msg = any("Cancellation requested - stopping after validation" in msg for msg in messages) or \
                                   any("Cancellation requested - stopping before final data move" in msg for msg in messages)
            assert should_have_cancel_msg
    
    @pytest.mark.asyncio
    async def test_ingest_cancellation_before_data_move(self):
        """Test cancellation before final data move - covers lines 402-404"""
        file_path = os.path.join(self.temp_dir, 'cancel_before_move.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_before_move.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock validation API to return success
        mock_validation_api = MagicMock()
        validation_result = {
            'status': 'validation_passed',
            'validation_issues': 0
        }
        mock_validation_api.validate_data.return_value = validation_result
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        # Mock cancellation before data move (lines 402-404)
        call_count = 0
        def mock_cancel_before_move(key):
            nonlocal call_count
            call_count += 1
            # Cancel right before data move
            return call_count >= 12  # Cancel at the data move check
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_before_move'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_before_move), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)), \
             patch('utils.ingest.ReferenceDataAPI', return_value=mock_validation_api):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_before_move.csv',
                    config_reference_data=True, progress_key='cancel_move_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should hit cancellation before data move (lines 402-404)
            assert any("Cancellation requested - stopping before final data move" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_ingest_stage_to_main_data_movement(self):
        """Test stage to main table data movement - covers lines 406-535"""
        file_path = os.path.join(self.temp_dir, 'stage_to_main.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'stage_to_main.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,product_name,category,price\n1,Laptop,Electronics,999.99\n2,Book,Education,29.99\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock validation API to return success
        mock_validation_api = MagicMock()
        validation_result = {
            'status': 'validation_passed',
            'validation_issues': 0
        }
        mock_validation_api.validate_data.return_value = validation_result
        
        # Mock database setup with detailed column information
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        
        # Mock column information for both tables
        table_columns = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'product_name', 'data_type': 'varchar'},
            {'name': 'category', 'data_type': 'varchar'},
            {'name': 'price', 'data_type': 'varchar'},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'},
            {'name': 'ref_data_loadtype', 'data_type': 'varchar'},
            {'name': 'ref_data_version_id', 'data_type': 'varchar'}
        ]
        
        self.mock_db.get_table_columns.return_value = table_columns
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='stage_to_main'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)), \
             patch('utils.ingest.ReferenceDataAPI', return_value=mock_validation_api):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'stage_to_main.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit stage to main data movement (lines 406-535)
            assert any("Moving data from stage to main table" in msg for msg in messages)
            assert any("Data loading completed" in msg for msg in messages)
            
            # Should have executed SQL for data movement
            mock_cursor.execute.assert_called()  # Various SQL operations
            self.mock_db.get_table_columns.assert_called()  # Get column information
    
    @pytest.mark.asyncio
    async def test_ingest_without_reference_data_validation(self):
        """Test ingestion without reference data validation - covers non-validation paths"""
        file_path = os.path.join(self.temp_dir, 'no_validation.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'no_validation.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,simple_data\n1,value1\n2,value2\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.get_table_columns.return_value = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'simple_data', 'data_type': 'varchar'}
        ]
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='no_validation'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'no_validation.csv',
                config_reference_data=False  # No validation
            ):
                messages.append(message)
            
            # Should skip validation and proceed directly to data loading
            assert not any("validation" in msg.lower() for msg in messages)
            assert any("Data loading completed" in msg for msg in messages)