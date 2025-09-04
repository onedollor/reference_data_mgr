"""
Ultra-focused test to push utils/ingest.py from 75% to 90% coverage
Specifically targeting lines 468-535 and remaining edge cases
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


class TestDataIngesterFinal90Push:
    """Ultra-focused tests to achieve 90% coverage"""
    
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
    async def test_backup_table_creation_with_existing_backup(self):
        """Test backup table creation when backup exists - covers lines 468-482"""
        file_path = os.path.join(self.temp_dir, 'backup_existing.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'backup_existing.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,value\n1,Test,100\n2,Data,200\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock complex database setup for backup scenario
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock existing main table with data that needs backup
        def mock_table_exists(conn, table_name, schema=None):
            if '_backup' in table_name:
                return True  # Backup table exists
            elif '_stage' in table_name:
                return False  # Stage table doesn't exist
            else:
                return True  # Main table exists
        
        self.mock_db.table_exists.side_effect = mock_table_exists
        self.mock_db.get_row_count.return_value = 50  # Main table has data
        
        # Mock table columns for backup schema validation
        main_table_columns = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'name', 'data_type': 'varchar'},
            {'name': 'value', 'data_type': 'varchar'},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'},
            {'name': 'ref_data_loadtype', 'data_type': 'varchar'}
        ]
        self.mock_db.get_table_columns.return_value = main_table_columns
        self.mock_db.create_backup_table.return_value = None
        
        # Mock successful validation
        validation_result = {'validation_result': 0}
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='backup_existing'), \
             patch.object(self.ingester.file_handler, 'move_to_archive', return_value='/archive/backup_existing.csv'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'backup_existing.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit backup table creation paths (lines 468-482)
            assert any("Data changes detected in main table (50 rows affected), creating backup" in msg for msg in messages)
            assert any("Backup table exists, validating and adjusting schema if needed" in msg for msg in messages)
            assert any("Backup table schema validated and synchronized with main table" in msg for msg in messages)
            
            # Should have called backup operations
            self.mock_db.create_backup_table.assert_called()
    
    @pytest.mark.asyncio
    async def test_backup_table_creation_new_backup(self):
        """Test backup table creation when backup doesn't exist - covers lines 474-476"""
        file_path = os.path.join(self.temp_dir, 'backup_new.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'backup_new.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,product,category\n1,Laptop,Electronics\n2,Book,Education\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup for new backup scenario
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock main table exists, but backup doesn't
        def mock_table_exists(conn, table_name, schema=None):
            if '_backup' in table_name:
                return False  # Backup table doesn't exist
            elif '_stage' in table_name:
                return False  # Stage table doesn't exist
            else:
                return True  # Main table exists
        
        self.mock_db.table_exists.side_effect = mock_table_exists
        self.mock_db.get_row_count.return_value = 25  # Main table has data
        
        # Mock table columns
        main_table_columns = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'product', 'data_type': 'varchar'},
            {'name': 'category', 'data_type': 'varchar'},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'},
            {'name': 'ref_data_loadtype', 'data_type': 'varchar'}
        ]
        self.mock_db.get_table_columns.return_value = main_table_columns
        self.mock_db.create_backup_table.return_value = None
        
        # Mock successful validation
        validation_result = {'validation_result': 0}
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='backup_new'), \
             patch.object(self.ingester.file_handler, 'move_to_archive', return_value='/archive/backup_new.csv'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'backup_new.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit new backup creation path (lines 474-476)
            assert any("Creating new backup table" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_backup_metadata_columns_handling(self):
        """Test backup metadata columns handling - covers lines 484-489"""
        file_path = os.path.join(self.temp_dir, 'backup_metadata.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'backup_metadata.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,MetaTest\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock existing main and backup tables
        def mock_table_exists(conn, table_name, schema=None):
            return True  # All tables exist
        
        self.mock_db.table_exists.side_effect = mock_table_exists
        self.mock_db.get_row_count.return_value = 10  # Main table has data
        
        # Mock table columns
        main_table_columns = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'name', 'data_type': 'varchar'},
            {'name': 'ref_data_loadtime', 'data_type': 'datetime'},
            {'name': 'ref_data_loadtype', 'data_type': 'varchar'}
        ]
        self.mock_db.get_table_columns.return_value = main_table_columns
        self.mock_db.create_backup_table.return_value = None
        
        # Mock backup metadata column operations
        backup_meta_actions = {
            'added': [
                {'column': 'ref_data_version_id', 'type': 'varchar(50)'}
            ]
        }
        self.mock_db.ensure_backup_table_metadata_columns.return_value = backup_meta_actions
        self.mock_db.backup_existing_data.return_value = 10  # 10 rows backed up
        
        # Mock successful validation
        validation_result = {'validation_result': 0}
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='backup_metadata'), \
             patch.object(self.ingester.file_handler, 'move_to_archive', return_value='/archive/backup_metadata.csv'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'backup_metadata.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit backup metadata handling (lines 484-489)
            assert any("Added missing metadata columns to backup table: ['ref_data_version_id']" in msg for msg in messages)
            
            # Should have ensured backup metadata columns
            self.mock_db.ensure_backup_table_metadata_columns.assert_called()
    
    @pytest.mark.asyncio
    async def test_backup_existing_data_operation(self):
        """Test backup existing data operation - covers lines 491-493"""
        file_path = os.path.join(self.temp_dir, 'backup_data.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'backup_data.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,status\n1,BackupTest,Active\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 75  # Main table has data to backup
        
        # Mock table columns
        main_table_columns = [
            {'name': 'id', 'data_type': 'varchar'},
            {'name': 'name', 'data_type': 'varchar'},
            {'name': 'status', 'data_type': 'varchar'}
        ]
        self.mock_db.get_table_columns.return_value = main_table_columns
        self.mock_db.create_backup_table.return_value = None
        self.mock_db.ensure_backup_table_metadata_columns.return_value = {'added': []}
        
        # Mock backup operation returns row count
        self.mock_db.backup_existing_data.return_value = 75
        
        # Mock successful validation
        validation_result = {'validation_result': 0}
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='backup_data'), \
             patch.object(self.ingester.file_handler, 'move_to_archive', return_value='/archive/backup_data.csv'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'backup_data.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit backup data operation (lines 491-493)
            assert any("Current main table state backed up: 75 rows with version tracking" in msg for msg in messages)
            
            # Should have called backup operation
            self.mock_db.backup_existing_data.assert_called()
    
    @pytest.mark.asyncio
    async def test_cancellation_before_archiving(self):
        """Test cancellation before archiving - covers lines 496-498"""
        file_path = os.path.join(self.temp_dir, 'cancel_archive.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_archive.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,CancelTest\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False
        self.mock_db.create_table.return_value = None
        self.mock_db.create_validation_procedure.return_value = None
        
        # Mock successful validation
        validation_result = {'validation_result': 0}
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        # Mock cancellation before archiving
        call_count = 0
        def mock_cancel_before_archive(key):
            nonlocal call_count
            call_count += 1
            return call_count >= 15  # Cancel before archiving
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_archive'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_before_archive), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_archive.csv',
                    config_reference_data=True
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should hit cancellation before archiving (lines 496-498)
            assert any("Cancellation requested - stopping before archiving" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_file_archiving_operation(self):
        """Test file archiving operation - covers lines 500-505"""
        file_path = os.path.join(self.temp_dir, 'archive_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'archive_test.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name,category\n1,ArchiveTest,TestCategory\n')
        
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
        self.mock_db.get_table_columns.return_value = []
        
        # Mock successful validation
        validation_result = {'validation_result': 0}
        self.mock_db.execute_validation_procedure.return_value = validation_result
        
        # Mock file archiving
        archive_path = '/archive/processed/archive_test_20231201_120000.csv'
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='archive_test'), \
             patch.object(self.ingester.file_handler, 'move_to_archive', return_value=archive_path), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'archive_test.csv',
                config_reference_data=True
            ):
                messages.append(message)
            
            # Should hit file archiving operation (lines 500-505)
            assert any("Archiving processed file" in msg for msg in messages)
            assert any("File archived to: archive_test_20231201_120000.csv" in msg for msg in messages)
    
    def test_comprehensive_edge_cases(self):
        """Test comprehensive edge cases for remaining missing lines"""
        
        # Test various initialization scenarios
        test_configs = [
            {'INGEST_TYPE_INFERENCE': '0'},  # Disabled
            {'INGEST_TYPE_INFERENCE': 'false'},  # False string
            {'INGEST_TYPE_INFERENCE': 'False'},  # False with capital
            {'INGEST_DATE_THRESHOLD': '1.0'},  # 100% threshold
            {'INGEST_DATE_THRESHOLD': '0.0'},  # 0% threshold
        ]
        
        for config in test_configs:
            with patch.dict('os.environ', config):
                ingester = DataIngester(self.mock_db, self.mock_logger)
                # Should handle all configurations without errors
                assert hasattr(ingester, 'enable_type_inference')
                assert hasattr(ingester, 'date_parse_threshold')
    
    def test_extremely_long_headers(self):
        """Test with extremely long headers that might cause edge cases"""
        # Create headers that are very long and might cause issues
        long_headers = [
            "a" * 500 + "1",  # Very long header
            "b" * 500 + "2",  # Another very long header
            "special_chars_" + "ñüéíóá" * 50,  # Unicode characters
        ]
        
        sanitized = self.ingester._sanitize_headers(long_headers)
        deduplicated = self.ingester._deduplicate_headers(sanitized)
        
        # Should handle very long headers
        assert len(deduplicated) == len(long_headers)
        assert len(set(deduplicated)) == len(deduplicated)  # All unique
    
    def test_type_inference_with_minimal_data(self):
        """Test type inference with minimal data"""
        # Test with dataframe that has very little data
        minimal_df = pd.DataFrame({
            'single_row': ['only_one_value'],
            'empty_col': [None],
            'zero_col': [0]
        })
        
        result = self.ingester._infer_types(minimal_df, list(minimal_df.columns))
        
        # Should handle minimal data gracefully
        assert isinstance(result, dict)
        assert len(result) == 3
        for col_type in result.values():
            assert 'varchar' in col_type