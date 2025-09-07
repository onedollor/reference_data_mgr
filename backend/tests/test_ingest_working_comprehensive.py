"""Working comprehensive test for utils/ingest.py focused on 90% coverage"""
import pytest
import asyncio
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from utils.ingest import DataIngester


class TestDataIngesterWorking:
    """Comprehensive working tests for DataIngester"""

    def setup_method(self):
        """Set up test environment"""
        # Create mock database manager with all necessary methods
        self.mock_db = Mock()
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.data_schema = "test_schema"
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = False  # Start with no existing table
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.determine_load_type.return_value = 'append'
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
        
        # Create temp directory for test files
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup test environment"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_comprehensive_ingest_flow(self):
        """Test complete ingestion flow covering maximum code paths"""
        file_path = os.path.join(self.temp_dir, 'test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'test.fmt')
        
        # Create test CSV file
        with open(file_path, 'w') as f:
            f.write('id,name,value,date\n1,Test,100,2023-01-01\n2,Data,200,2023-01-02\n')
        
        # Create format file
        with open(fmt_file_path, 'w') as f:
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
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # Create proper DataFrame mock
            mock_df = Mock()
            mock_df.shape = (2, 4)
            mock_df.columns = pd.Index(['id', 'name', 'value', 'date'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=2)
            mock_df.dtypes = pd.Series(['int64', 'object', 'int64', 'object'], 
                                     index=['id', 'name', 'value', 'date'])
            
            # Mock iloc for first row access
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'id': 1, 'name': 'Test', 'value': 100, 'date': '2023-01-01'}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            
            # Mock iterrows for data processing
            mock_df.iterrows.return_value = [
                (0, {'id': 1, 'name': 'Test', 'value': 100, 'date': '2023-01-01'}),
                (1, {'id': 2, 'name': 'Data', 'value': 200, 'date': '2023-01-02'})
            ]
            
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'test.csv', config_reference_data=True
            ):
                messages.append(message)
                if len(messages) > 50:  # Prevent infinite loops
                    break
            
            # Verify we got key messages
            assert any("Starting data ingestion process" in msg for msg in messages)
            assert any("Database connection established" in msg for msg in messages)
            assert any("Table name extracted" in msg for msg in messages)
            
            # Verify database interactions
            self.mock_db.get_connection.assert_called()
            self.mock_db.ensure_schemas_exist.assert_called()

    @pytest.mark.asyncio 
    async def test_backup_scenario_with_existing_data(self):
        """Test backup creation when existing data is present"""
        file_path = os.path.join(self.temp_dir, 'backup_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'backup_test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('name,age\nJohn,25\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up for existing data scenario
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 50  # Existing data
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            # Simple DataFrame mock
            mock_df = Mock()
            mock_df.shape = (1, 2)
            mock_df.columns = pd.Index(['name', 'age'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['object', 'int64'], index=['name', 'age'])
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=Mock())
            mock_df.iloc[0].to_dict.return_value = {'name': 'John', 'age': 25}
            mock_df.iterrows.return_value = [(0, {'name': 'John', 'age': 25})]
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'backup_test.csv'
            ):
                messages.append(message)
                if len(messages) > 30:
                    break
            
            # Check for backup-related messages
            assert any("Found 50 existing rows" in msg for msg in messages)

    @pytest.mark.asyncio
    async def test_cancellation_scenarios(self):
        """Test various cancellation points in the ingestion process"""
        file_path = os.path.join(self.temp_dir, 'cancel_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel_test.fmt')
        
        # Create minimal test files
        with open(file_path, 'w') as f:
            f.write('a,b\n1,2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        cancellation_count = 0
        def mock_is_canceled(key):
            nonlocal cancellation_count
            cancellation_count += 1
            # Cancel after a few checks to test cancellation handling
            return cancellation_count > 5
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', side_effect=mock_is_canceled), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            mock_df = Mock()
            mock_df.shape = (1, 2)
            mock_df.columns = pd.Index(['a', 'b'])
            mock_df.empty = False
            mock_read_csv.return_value = mock_df
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel_test.csv'
                ):
                    messages.append(message)
                    if len(messages) > 10:  # Prevent infinite loops
                        break
            except Exception as e:
                # Cancellation may or may not raise an exception
                assert "cancel" in str(e).lower() or "cancel" in messages[-1].lower()
            
            # At minimum verify cancellation logic was invoked
            assert cancellation_count > 5

    @pytest.mark.asyncio
    async def test_type_inference_functionality(self):
        """Test type inference functionality in the ingester"""
        file_path = os.path.join(self.temp_dir, 'types_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'types_test.fmt')
        
        # Create test CSV with various data types
        with open(file_path, 'w') as f:
            f.write('id,name,amount,date,flag\n1,Test,100.50,2023-01-01,true\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('time.perf_counter', side_effect=range(1, 20)):
            
            # Mock DataFrame with different types
            mock_df = Mock()
            mock_df.shape = (1, 5)
            mock_df.columns = pd.Index(['id', 'name', 'amount', 'date', 'flag'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['int64', 'object', 'float64', 'object', 'bool'], 
                                     index=['id', 'name', 'amount', 'date', 'flag'])
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {'id': 1, 'name': 'Test', 'amount': 100.50, 'date': '2023-01-01', 'flag': True}
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'types_test.csv'
            ):
                messages.append(message)
                if len(messages) > 10:
                    break
            
            # Verify type processing occurred
            assert any("Headers processed" in msg for msg in messages)

    @pytest.mark.asyncio
    async def test_error_handling_scenarios(self):
        """Test error handling in various scenarios"""
        file_path = os.path.join(self.temp_dir, 'error_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'error_test.fmt')
        
        # Create test files
        with open(file_path, 'w') as f:
            f.write('col1,col2\nval1,val2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Make database connection fail
        self.mock_db.get_connection.side_effect = Exception("Database connection failed")
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 10)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'error_test.csv'
            ):
                messages.append(message)
                if "ERROR!" in message:
                    break
            
            # Check error handling
            assert any("ERROR!" in msg for msg in messages)

    @pytest.mark.asyncio
    async def test_load_dataframe_to_table_method(self):
        """Test the _load_dataframe_to_table method directly"""
        # Create a simple DataFrame to load
        test_df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['A', 'B', 'C'],
            'value': [100, 200, 300]
        })
        
        connection = Mock()
        cursor = Mock()
        connection.cursor.return_value = cursor
        
        # Mock database operations needed by _load_dataframe_to_table
        cursor.executemany = Mock()
        
        with patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            # Call the method directly
            await self.ingester._load_dataframe_to_table(
                test_df, 
                connection,
                'test_table_stage',
                progress_key='test_load',
                total_rows=3,
                static_load_timestamp=pd.Timestamp.now()
            )
            
            # Verify database interactions
            connection.cursor.assert_called()
            cursor.executemany.assert_called()

    @pytest.mark.asyncio 
    async def test_read_csv_file_method(self):
        """Test the _read_csv_file method with various scenarios"""
        file_path = os.path.join(self.temp_dir, 'read_test.csv')
        
        # Create test CSV 
        with open(file_path, 'w') as f:
            f.write('col1,col2,col3\nval1,val2,val3\nval4,val5,val6\n')
        
        csv_format = {
            'column_delimiter': ',',
            'text_qualifier': '"',
            'skip_lines': 0,
            'has_header': True
        }
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.is_canceled', return_value=False):
            
            # Mock DataFrame 
            mock_df = pd.DataFrame({
                'col1': ['val1', 'val4'],
                'col2': ['val2', 'val5'], 
                'col3': ['val3', 'val6']
            })
            mock_read_csv.return_value = mock_df
            
            # Call method directly
            result_df = await self.ingester._read_csv_file(file_path, csv_format, 'test_progress')
            
            # Verify result
            assert result_df is not None
            mock_read_csv.assert_called_once()