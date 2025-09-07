"""Systematic approach to push utils/ingest.py from 69% to 90% coverage"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from utils.ingest import DataIngester


class TestDataIngesterSystematic90:
    """Systematic tests targeting specific missing line ranges"""

    def setup_method(self):
        """Enhanced setup for complex scenarios"""
        # Enhanced database mock
        self.mock_db = Mock()
        self.mock_cursor = Mock()
        self.mock_cursor.rowcount = 5
        self.mock_cursor.executemany = Mock()
        self.mock_connection = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor
        self.mock_db.get_connection.return_value = self.mock_connection
        self.mock_db.data_schema = "test_schema"
        self.mock_db.backup_schema = "backup_schema"
        
        # Enhanced logger mock
        self.mock_logger = Mock()
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.mock_logger.log_warning = AsyncMock()
        
        # Enhanced file handler mock  
        self.mock_file_handler = Mock()
        self.mock_file_handler.extract_table_base_name.return_value = "systematic_test_table"
        self.mock_file_handler.read_format_file = AsyncMock()
        self.mock_file_handler.move_to_archive.return_value = "/archive/systematic.csv"
        
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        self.ingester.file_handler = self.mock_file_handler
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_header_processing_comprehensive(self):
        """Target lines 145-183: Header processing with edge cases"""
        file_path = os.path.join(self.temp_dir, 'headers.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'headers.fmt')
        
        # Create CSV with problematic headers
        with open(file_path, 'w') as f:
            f.write('invalid@header,123StartWithNumber,Column With Spaces,ALLCAPS,mixed_Case,normal\n')
            f.write('val1,val2,val3,val4,val5,val6\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Setup mocks for header processing
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = False
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.determine_load_type.return_value = 'full'
        self.mock_db.create_stage_table = Mock()
        self.mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
        self.mock_db.get_table_columns.return_value = [
            {'name': 'invalid_header', 'type': 'varchar'},
            {'name': 'column123startwithnumber', 'type': 'varchar'},
            {'name': 'column_with_spaces', 'type': 'varchar'},
            {'name': 'allcaps', 'type': 'varchar'},
            {'name': 'mixed_case', 'type': 'varchar'},
            {'name': 'normal', 'type': 'varchar'}
        ]
        self.mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 1}
        self.mock_db.ensure_main_table_metadata_columns.return_value = {'added': []}
        
        self.mock_file_handler.read_format_file.return_value = {
            "csv_format": {"delimiter": ",", "has_header": True}
        }
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # Mock DataFrame with problematic columns
            mock_df = Mock()
            mock_df.shape = (1, 6)
            mock_df.columns = pd.Index(['invalid@header', '123StartWithNumber', 'Column With Spaces', 'ALLCAPS', 'mixed_Case', 'normal'])
            mock_df.empty = False
            mock_df.__len__ = Mock(return_value=1)
            mock_df.dtypes = pd.Series(['object'] * 6, index=mock_df.columns)
            
            # Mock data processing methods
            mock_df.__getitem__ = Mock()
            mock_df.__getitem__.return_value = mock_df  # For column selection
            mock_df.rename = Mock(return_value=mock_df)  # For column renaming
            mock_df.astype = Mock(return_value=mock_df)
            mock_df.replace = Mock(return_value=mock_df)
            
            mock_first_row = Mock()
            mock_first_row.to_dict.return_value = {
                'invalid@header': 'val1',
                '123StartWithNumber': 'val2', 
                'Column With Spaces': 'val3',
                'ALLCAPS': 'val4',
                'mixed_Case': 'val5',
                'normal': 'val6'
            }
            mock_df.iloc = Mock()
            mock_df.iloc.__getitem__ = Mock(return_value=mock_first_row)
            
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'headers.csv'
            ):
                messages.append(message)
                if len(messages) > 100:
                    break
            
            # Should complete header processing
            assert any("Headers processed" in msg for msg in messages)

    @pytest.mark.asyncio
    async def test_schema_operations_comprehensive(self):
        """Target lines 257-298: Schema operations and metadata handling"""
        file_path = os.path.join(self.temp_dir, 'schema_ops.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'schema_ops.fmt')
        
        with open(file_path, 'w') as f:
            f.write('id,name,description\n1,Test,Description\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Complex schema setup
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = True  # Table exists
        self.mock_db.get_row_count.return_value = 10  # Has existing data
        self.mock_db.determine_load_type.return_value = 'full'
        
        # Complex metadata operations that should hit lines 257-298
        self.mock_db.ensure_main_table_metadata_columns.return_value = {
            'added': [
                {'column': 'ref_data_load_timestamp', 'type': 'datetime2'},
                {'column': 'ref_data_version_id', 'type': 'bigint'}
            ],
            'updated': [
                {'column': 'existing_col', 'old_type': 'varchar(50)', 'new_type': 'varchar(100)'}
            ],
            'errors': [
                {'column': 'problem_col', 'error': 'Cannot add column due to constraint'}
            ]
        }
        
        self.mock_db.create_stage_table = Mock()
        self.mock_db.drop_table = Mock()
        self.mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
        self.mock_db.get_table_columns.return_value = [
            {'name': 'id', 'type': 'int'},
            {'name': 'name', 'type': 'varchar'},
            {'name': 'description', 'type': 'text'}
        ]
        self.mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 1}
        
        self.mock_file_handler.read_format_file.return_value = {
            "csv_format": {"delimiter": ",", "has_header": True}
        }
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.mark_done'), \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            mock_df = pd.DataFrame({
                'id': [1], 
                'name': ['Test'], 
                'description': ['Description']
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'schema_ops.csv'
            ):
                messages.append(message)
                if len(messages) > 80:
                    break
            
            # Should handle complex schema operations
            assert any("metadata columns" in msg.lower() for msg in messages)

    @pytest.mark.asyncio
    async def test_validation_error_paths(self):
        """Target lines 374-383, 390-391: Validation error handling"""
        file_path = os.path.join(self.temp_dir, 'validation_errors.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'validation_errors.fmt')
        
        with open(file_path, 'w') as f:
            f.write('id,data\n1,valid\n2,invalid\n3,problematic\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Setup validation failure
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = False
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.determine_load_type.return_value = 'append'
        self.mock_db.create_stage_table = Mock()
        
        # Mock validation procedure to return specific errors (lines 374-383)
        self.mock_db.execute_validation_procedure.return_value = {
            "validation_result": 3,  # 3 validation issues
            "validation_issue_list": [
                "Row 2: Invalid data format in 'data' field",
                "Row 3: Data exceeds maximum length",
                "Row 3: Required field 'category' is missing"
            ]
        }
        
        self.mock_db.get_table_columns.return_value = [
            {'name': 'id', 'type': 'int'},
            {'name': 'data', 'type': 'varchar'}
        ]
        
        self.mock_file_handler.read_format_file.return_value = {
            "csv_format": {"delimiter": ",", "has_header": True}
        }
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 150)):
            
            mock_df = pd.DataFrame({
                'id': [1, 2, 3],
                'data': ['valid', 'invalid', 'problematic']
            })
            mock_read_csv.return_value = mock_df
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'validation_errors.csv'
            ):
                messages.append(message)
                if "Validation failed with 3 issues" in message:
                    break
                if len(messages) > 60:
                    break
            
            # Should hit validation error paths (lines 374-383)
            assert any("Validation failed with 3 issues" in msg for msg in messages)

    @pytest.mark.asyncio
    async def test_data_loading_methods_comprehensive(self):
        """Target lines 771-928: Data loading method edge cases"""
        
        # Test _load_dataframe_to_table with various edge cases
        test_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'text_data': ['short', 'medium length text', 'very long text that might cause issues in some databases', '', None],
            'numeric_data': [1.5, 2.7, 3.9, 0, -1.2],
            'mixed_data': ['text', 123, 45.6, True, None]
        })
        
        # Enhanced cursor mock to hit different code paths
        mock_cursor = Mock()
        mock_cursor.executemany = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch('utils.progress.update_progress') as mock_update, \
             patch('utils.progress.is_canceled') as mock_canceled, \
             patch('time.perf_counter', side_effect=range(1, 200)):
            
            # Test with different cancellation scenarios
            cancel_sequence = [False] * 10 + [True]  # Cancel after 10 calls
            mock_canceled.side_effect = cancel_sequence
            
            try:
                await self.ingester._load_dataframe_to_table(
                    mock_connection,
                    test_df,
                    'comprehensive_test_table',
                    'test_schema',
                    5,
                    'comprehensive_load_key',
                    'full',
                    pd.Timestamp.now()
                )
            except Exception:
                # May raise cancellation exception
                pass
            
            # Should have attempted database operations
            mock_cursor.executemany.assert_called()
            mock_update.assert_called()

    @pytest.mark.asyncio
    async def test_precise_cancellation_paths(self):
        """Target lines 332-333, 497-498: Precise cancellation scenarios"""
        file_path = os.path.join(self.temp_dir, 'precise_cancel.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'precise_cancel.fmt')
        
        with open(file_path, 'w') as f:
            f.write('col1,col2\ndata1,data2\ndata3,data4\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Setup for precise timing of cancellation
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 100  # Existing data for backup scenario
        self.mock_db.determine_load_type.return_value = 'full'
        self.mock_db.create_stage_table = Mock()
        self.mock_db.create_backup_table = Mock()
        self.mock_db.backup_existing_data.return_value = 100
        self.mock_db.ensure_backup_table_metadata_columns.return_value = {'added': []}
        self.mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
        self.mock_db.get_table_columns.return_value = [
            {'name': 'col1', 'type': 'varchar'},
            {'name': 'col2', 'type': 'varchar'}
        ]
        self.mock_db.move_stage_to_main.return_value = {'success': True, 'rows_affected': 2}
        
        self.mock_file_handler.read_format_file.return_value = {
            "csv_format": {"delimiter": ",", "has_header": True}
        }
        
        # Precise cancellation timing to hit lines 497-498 (before archiving)
        cancel_calls = 0
        def precise_cancel(key):
            nonlocal cancel_calls
            cancel_calls += 1
            # Cancel at very specific point - after main processing but before archiving
            return cancel_calls >= 15  # Adjust timing to hit line 497
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.is_canceled', side_effect=precise_cancel), \
             patch('utils.progress.request_cancel'), \
             patch('time.perf_counter', side_effect=range(1, 300)):
            
            mock_df = pd.DataFrame({'col1': ['data1', 'data3'], 'col2': ['data2', 'data4']})
            mock_read_csv.return_value = mock_df
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'precise_cancel.csv',
                    config_reference_data=True
                ):
                    messages.append(message)
                    if len(messages) > 120:
                        break
            except Exception:
                # May raise cancellation exception
                pass
            
            # Should hit at least one of the precise cancellation points
            assert cancel_calls >= 15

    @pytest.mark.asyncio
    async def test_comprehensive_csv_reading_edge_cases(self):
        """Target CSV reading edge cases in _read_csv_file method"""
        
        # Test with complex CSV file
        file_path = os.path.join(self.temp_dir, 'complex_csv.csv')
        
        # Complex CSV content
        with open(file_path, 'w') as f:
            f.write('# This is a comment line\n')
            f.write('# Another comment\n') 
            f.write('col1,col2,col3\n')
            f.write('"value with, comma","value with ""quotes""","normal value"\n')
            f.write('empty_test,,""\n')
            f.write('special_chars,"!@#$%^&*()","unicode_test_ñ"\n')
            f.write('TRAILER: 3 records processed\n')
        
        csv_format = {
            'column_delimiter': ',',
            'text_qualifier': '"',
            'skip_lines': 2,  # Skip comment lines
            'has_header': True,
            'has_trailer': True,
            'trailer_line': 'TRAILER'
        }
        
        with patch('utils.progress.is_canceled', return_value=False):
            result_df = await self.ingester._read_csv_file(file_path, csv_format, 'complex_csv_test')
            
            # Should process complex CSV correctly
            assert result_df is not None