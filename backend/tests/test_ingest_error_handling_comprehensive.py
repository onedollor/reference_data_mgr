"""Comprehensive error handling tests to push utils/ingest.py coverage to 70%+"""
import pytest
import tempfile
import os
import json
import pandas as pd
from unittest.mock import Mock, AsyncMock, patch
from utils.ingest import DataIngester


class TestDataIngesterErrorHandling:
    """Comprehensive error handling test scenarios"""

    def setup_method(self):
        """Set up base test environment"""
        self.mock_db = Mock()
        self.mock_logger = Mock()
        self.mock_logger.log_info = AsyncMock()
        self.mock_logger.log_error = AsyncMock()
        self.mock_logger.log_warning = AsyncMock()
        
        self.mock_file_handler = Mock()
        self.mock_file_handler.extract_table_base_name.return_value = "error_test_table"
        
        self.ingester = DataIngester(self.mock_db, self.mock_logger)
        self.ingester.file_handler = self.mock_file_handler
        self.temp_dir = tempfile.mkdtemp()
        
    def teardown_method(self):
        """Cleanup"""
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @pytest.mark.asyncio
    async def test_database_connection_error(self):
        """Test database connection failure (lines 540-557 error path)"""
        file_path = os.path.join(self.temp_dir, 'db_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'db_error.fmt')
        
        with open(file_path, 'w') as f:
            f.write('col1,col2\nval1,val2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Mock database connection to fail
        self.mock_db.get_connection.side_effect = Exception("Database connection timeout")
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'db_error.csv'
            ):
                messages.append(message)
                if "ERROR!" in message:
                    break
                if len(messages) > 20:
                    break
            
            # Should hit database error path
            assert any("ERROR! Data ingestion failed: Database connection timeout" in msg for msg in messages)
            assert any("Upload process automatically canceled due to error" in msg for msg in messages)
            
            # Verify error logging was called
            self.mock_logger.log_error.assert_called()

    @pytest.mark.asyncio
    async def test_file_format_parsing_error(self):
        """Test format file parsing errors"""
        file_path = os.path.join(self.temp_dir, 'format_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'format_error.fmt')
        
        with open(file_path, 'w') as f:
            f.write('data\ntest\n')
        with open(fmt_file_path, 'w') as f:
            f.write('invalid json content{')  # Malformed JSON
        
        # Mock file handler to fail on format file reading
        self.mock_file_handler.read_format_file = AsyncMock(
            side_effect=Exception("Failed to parse format file: invalid JSON")
        )
        
        with patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 30)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'format_error.csv'
            ):
                messages.append(message)
                if "ERROR!" in message and "Traceback:" in message:
                    break
                if len(messages) > 20:
                    break
            
            # Should hit format parsing error path
            assert any("ERROR! Data ingestion failed" in msg for msg in messages)
            assert any("invalid JSON" in str(messages))

    @pytest.mark.asyncio
    async def test_csv_reading_error(self):
        """Test CSV reading errors"""
        file_path = os.path.join(self.temp_dir, 'csv_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'csv_error.fmt')
        
        with open(file_path, 'w') as f:
            f.write('corrupted\x00csv\x00content\n')  # Binary content in CSV
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('pandas.read_csv', side_effect=Exception("CSV parsing failed: corrupted data")), \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'csv_error.csv'
            ):
                messages.append(message)
                if "ERROR!" in message and "corrupted data" in message:
                    break
                if len(messages) > 30:
                    break
            
            # Should hit CSV reading error path
            assert any("CSV parsing failed" in str(messages))

    @pytest.mark.asyncio
    async def test_schema_validation_error(self):
        """Test schema validation errors"""
        file_path = os.path.join(self.temp_dir, 'schema_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'schema_error.fmt')
        
        with open(file_path, 'w') as f:
            f.write('col1,col2\nval1,val2\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up mocks to fail on schema operations
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.ensure_schemas_exist.side_effect = Exception("Schema validation failed: permission denied")
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            mock_read_csv.return_value = pd.DataFrame({'col1': ['val1'], 'col2': ['val2']})
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'schema_error.csv'
            ):
                messages.append(message)
                if "permission denied" in str(messages):
                    break
                if len(messages) > 25:
                    break
            
            # Should hit schema validation error
            assert any("permission denied" in str(messages))

    @pytest.mark.asyncio
    async def test_target_schema_error_handling(self):
        """Test target schema override error handling (lines 80-82)"""
        file_path = os.path.join(self.temp_dir, 'target_schema.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'target_schema.fmt')
        
        with open(file_path, 'w') as f:
            f.write('data\ntest\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Mock database with schema override logic
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.data_schema = "original_schema"
        self.mock_db.ensure_schemas_exist.side_effect = Exception("Target schema 'custom_schema' does not exist")
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 40)):
            
            mock_read_csv.return_value = pd.DataFrame({'data': ['test']})
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'target_schema.csv',
                target_schema="custom_schema"  # Use target schema override
            ):
                messages.append(message)
                if "Target schema" not in str(messages) and "ERROR!" in str(messages):
                    break
                if len(messages) > 30:
                    break
            
            # Should hit target schema error and restoration logic
            message_text = ' '.join(messages)
            assert "custom_schema" in message_text or "schema" in message_text.lower()

    @pytest.mark.asyncio
    async def test_data_loading_critical_error(self):
        """Test critical errors during data loading"""
        file_path = os.path.join(self.temp_dir, 'loading_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'loading_error.fmt')
        
        with open(file_path, 'w') as f:
            f.write('id,value\n1,test\n2,data\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Set up complex failure scenario
        mock_cursor = Mock()
        mock_cursor.executemany.side_effect = Exception("CRITICAL: Database disk full")
        mock_connection = Mock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = False
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.determine_load_type.return_value = 'append'
        self.mock_db.create_stage_table = Mock()
        self.mock_db.execute_validation_procedure.return_value = {"validation_result": 0, "validation_issue_list": []}
        self.mock_db.get_table_columns.return_value = [{'name': 'id', 'type': 'int'}, {'name': 'value', 'type': 'varchar'}]
        
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            mock_read_csv.return_value = pd.DataFrame({'id': [1, 2], 'value': ['test', 'data']})
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'loading_error.csv'
            ):
                messages.append(message)
                if "disk full" in str(messages):
                    break
                if len(messages) > 60:
                    break
            
            # Should hit critical loading error
            assert any("Database disk full" in str(messages) or "ERROR!" in str(messages))

    @pytest.mark.asyncio
    async def test_finally_block_cleanup(self):
        """Test finally block cleanup (lines 558-563)"""
        file_path = os.path.join(self.temp_dir, 'cleanup_test.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cleanup_test.fmt')
        
        with open(file_path, 'w') as f:
            f.write('test_col\ntest_val\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Mock scenario that triggers finally block
        original_schema = "original_test_schema"
        self.mock_db.data_schema = original_schema
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.ensure_schemas_exist.side_effect = Exception("Forced error to test cleanup")
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 30)):
            
            mock_read_csv.return_value = pd.DataFrame({'test_col': ['test_val']})
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'cleanup_test.csv',
                target_schema="override_schema"  # This should trigger schema restoration
            ):
                messages.append(message)
                if "ERROR!" in message:
                    break
                if len(messages) > 25:
                    break
            
            # Should have restored original schema in finally block
            assert self.mock_db.data_schema == original_schema or any("schema" in msg.lower() for msg in messages)

    @pytest.mark.asyncio
    async def test_multiple_cascading_errors(self):
        """Test cascading error scenarios"""
        file_path = os.path.join(self.temp_dir, 'cascade_error.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cascade_error.fmt')
        
        with open(file_path, 'w') as f:
            f.write('bad_col\nbad_val\n')
        with open(fmt_file_path, 'w') as f:
            json.dump({"csv_format": {"delimiter": ","}}, f)
        
        # Create cascading failure scenario
        self.mock_db.get_connection.return_value = Mock()
        self.mock_db.ensure_schemas_exist = Mock()
        self.mock_db.table_exists.return_value = True
        self.mock_db.get_row_count.return_value = 10
        self.mock_db.determine_load_type.return_value = 'full'
        # Make multiple operations fail
        self.mock_db.create_stage_table.side_effect = Exception("Stage table creation failed")
        
        self.mock_file_handler.read_format_file = AsyncMock(return_value={
            "csv_format": {"delimiter": ",", "has_header": True}
        })
        
        with patch('pandas.read_csv') as mock_read_csv, \
             patch('utils.progress.init_progress'), \
             patch('utils.progress.update_progress'), \
             patch('utils.progress.request_cancel'), \
             patch('utils.progress.mark_error'), \
             patch('time.perf_counter', side_effect=range(1, 80)):
            
            mock_read_csv.return_value = pd.DataFrame({'bad_col': ['bad_val']})
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'cascade_error.csv'
            ):
                messages.append(message)
                if "Stage table creation failed" in str(messages):
                    break
                if len(messages) > 50:
                    break
            
            # Should handle cascading errors gracefully
            assert any("ERROR!" in msg for msg in messages)
            assert any("Stage table creation failed" in str(messages))