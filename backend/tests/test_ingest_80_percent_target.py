"""Target 80% coverage for utils/ingest.py by covering missing lines 325-535"""
import pytest
import asyncio
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from utils.ingest import DataIngester


class TestDataIngester80Percent:
    """Tests specifically targeting lines 325-535 for 80% coverage"""

    def setup_method(self):
        """Set up test environment"""
        # Create comprehensive mock database manager
        self.mock_db = Mock()
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.data_schema = "test_schema"
        self.mock_db.backup_schema = "backup_schema"
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists = Mock()
        self.mock_db.get_row_count.return_value = 50  # Existing data to trigger backup
        self.mock_db.determine_load_type.return_value = 'full'
        self.mock_db.create_stage_table = Mock()
        self.mock_db.create_backup_table = Mock()
        self.mock_db.backup_existing_data.return_value = 75
        self.mock_db.ensure_backup_table_metadata_columns.return_value = {
            'added': [{'column': 'ref_data_version_id'}]
        }
        self.mock_db.ensure_main_table_metadata_columns.return_value = {
            'added': [{'column': 'ref_data_load_timestamp'}]
        }
        self.mock_db.drop_table = Mock()
        self.mock_db.validate_stage_data.return_value = (True, [])
        self.mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
        
        # Create mock logger
        self.mock_logger = Mock()
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.mock_logger.log_warning = AsyncMock()
        
        # Create mock file handler
        self.mock_file_handler = Mock()
        self.mock_file_handler.extract_table_base_name.return_value = "test_table"
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {
                "header_delimiter": ",",
                "column_delimiter": ",",
                "row_delimiter": "\n",
                "text_qualifier": '"',
                "skip_lines": 0,
                "has_header": True,
                "has_trailer": False,
                "trailer_line": None
            }
        })
        self.mock_file_handler.move_to_archive.return_value = "/archive/path/file.csv"
        
        # Create ingester
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        self.ingester.file_handler = self.mock_file_handler
        
        # Create temp directory
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_full_ingestion_with_backup_path(self):
        """Test complete ingestion covering backup creation (lines 325-535)"""
        file_path = os.path.join(self.temp_dir, 'full_backup.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'full_backup.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name,value\n1,Test,100\n2,Data,200\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ",", "has_header": True}}, f)
        
        # Set up complex scenario to hit backup paths
        self.mock_db.table_exists.side_effect = lambda conn, table, schema=None: True  # Main table exists
        self.mock_db.get_row_count.return_value = 50  # Existing data
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 300)):
            
            # Complete DataFrame mock
            mock_df = Mock()
            mock_df.shape = (2, 3)
            mock_df.columns = pd.Index(['id', 'name', 'value'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=2)
            mock_df.dtypes = pd.Series(['int64', 'object', 'int64'], index=['id', 'name', 'value'])
            
            # Mock iloc and iterrows
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'id': 1, 'name': 'Test', 'value': 100}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            mock_df.iterrows.return_value = [
                (0, {'id': 1, 'name': 'Test', 'value': 100}),
                (1, {'id': 2, 'name': 'Data', 'value': 200})
            ]
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'full_backup.csv', 
                config_reference_data=True
            ):
                messages.append(message)
                # Let it run longer to hit backup paths
                if len(messages) > 100:
                    break
            
            # Verify backup-related operations were called
            self.mock_db.create_backup_table.assert_called()
            self.mock_db.backup_existing_data.assert_called()
            
            # Check for backup messages
            message_text = ' '.join(messages)
            assert 'backup' in message_text.lower() or 'archive' in message_text.lower()

    @pytest.mark.asyncio
    async def test_stage_table_operations(self):
        """Test stage table creation and operations"""
        file_path = os.path.join(self.temp_dir, 'stage_ops.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'stage_ops.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('col1,col2\nval1,val2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up to test stage table paths
        self.mock_db.table_exists.side_effect = lambda conn, table, schema=None: 'stage' not in table
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            mock_df = Mock()
            mock_df.shape = (1, 2)
            mock_df.columns = pd.Index(['col1', 'col2'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['object', 'object'], index=['col1', 'col2'])
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'col1': 'val1', 'col2': 'val2'}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            mock_df.iterrows.return_value = [(0, {'col1': 'val1', 'col2': 'val2'})]
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'stage_ops.csv'
            ):
                messages.append(message)
                if len(messages) > 50:
                    break
            
            # Verify stage operations
            self.mock_db.create_stage_table.assert_called()

    @pytest.mark.asyncio
    async def test_data_validation_scenarios(self):
        """Test data validation paths"""
        file_path = os.path.join(self.temp_dir, 'validation.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'validation.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n2,Validate\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up validation scenario
        self.mock_db.table_exists.return_value = False
        self.mock_db.validate_stage_data.return_value = (True, [])
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('time.perf_counter', side_effect=range(1, 80)):
            
            mock_df = Mock()
            mock_df.shape = (2, 2)
            mock_df.columns = pd.Index(['id', 'name'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=2)
            mock_df.dtypes = pd.Series(['int64', 'object'], index=['id', 'name'])
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'id': 1, 'name': 'Test'}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            mock_df.iterrows.return_value = [
                (0, {'id': 1, 'name': 'Test'}),
                (1, {'id': 2, 'name': 'Validate'})
            ]
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'validation.csv'
            ):
                messages.append(message)
                if len(messages) > 40:
                    break
            
            # Verify validation was called
            self.mock_db.validate_stage_data.assert_called()

    @pytest.mark.asyncio
    async def test_archive_operations(self):
        """Test file archiving functionality"""
        file_path = os.path.join(self.temp_dir, 'archive_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'archive_test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('data,info\ntest,archive\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Mock successful completion to reach archiving
        self.mock_db.table_exists.return_value = False
        self.mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 1}
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 150)):
            
            mock_df = Mock()
            mock_df.shape = (1, 2)
            mock_df.columns = pd.Index(['data', 'info'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['object', 'object'], index=['data', 'info'])
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'data': 'test', 'info': 'archive'}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            mock_df.iterrows.return_value = [(0, {'data': 'test', 'info': 'archive'})]
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'archive_test.csv'
            ):
                messages.append(message)
                # Let it complete to reach archiving
                if len(messages) > 60:
                    break
            
            # Verify archiving was attempted
            self.mock_file_handler.move_to_archive.assert_called()

    @pytest.mark.asyncio
    async def test_metadata_column_handling(self):
        """Test metadata column operations"""
        file_path = os.path.join(self.temp_dir, 'metadata.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'metadata.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('name,value\nMeta,Data\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up to test metadata operations
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 10  # Some existing data
        self.mock_db.ensure_main_table_metadata_columns.return_value = {
            'added': [{'column': 'ref_data_load_timestamp'}]
        }
        self.mock_db.ensure_backup_table_metadata_columns.return_value = {
            'added': [{'column': 'ref_data_version_id'}]
        }
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('time.perf_counter', side_effect=range(1, 120)):
            
            mock_df = Mock()
            mock_df.shape = (1, 2)
            mock_df.columns = pd.Index(['name', 'value'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['object', 'object'], index=['name', 'value'])
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'name': 'Meta', 'value': 'Data'}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            mock_df.iterrows.return_value = [(0, {'name': 'Meta', 'value': 'Data'})]
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'metadata.csv', 
                config_reference_data=True
            ):
                messages.append(message)
                if len(messages) > 80:
                    break
            
            # Verify metadata operations
            self.mock_db.ensure_backup_table_metadata_columns.assert_called()
            
            # Check for metadata-related messages
            message_text = ' '.join(messages)
            assert 'metadata' in message_text.lower() or 'column' in message_text.lower()