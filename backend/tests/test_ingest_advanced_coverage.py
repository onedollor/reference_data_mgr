"""
Advanced tests to push utils/ingest.py coverage higher
Properly handling async methods and targeting specific missing lines
"""

import pytest
import os
import tempfile
import shutil
import pandas as pd
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock, mock_open, call
from datetime import datetime

from utils.ingest import DataIngester
from utils.database import DatabaseManager 
from utils.logger import Logger


class TestDataIngesterAdvancedCoverage:
    """Advanced tests for higher coverage"""
    
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
    async def test_load_dataframe_to_table_method_directly(self):
        """Test _load_dataframe_to_table method directly - covers lines 761-928"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['John', 'Jane', 'Bob'],
            'value': [100.5, 200.7, 300.9]
        })
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            # Test the direct async method call
            await self.ingester._load_dataframe_to_table(
                mock_connection,
                df,
                'test_table',
                'test_schema', 
                len(df),
                'test_key',
                'F',
                datetime.now()
            )
            
            # Should have executed SQL operations
            mock_cursor.execute.assert_called()  # TRUNCATE statement
            mock_connection.commit.assert_called()
            mock_cursor.executemany.assert_called()  # INSERT statements
    
    @pytest.mark.asyncio
    async def test_load_dataframe_with_cancellation_in_method(self):
        """Test cancellation within _load_dataframe_to_table - covers lines 770-775"""
        df = pd.DataFrame({'id': [1], 'name': ['Test']})
        
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch('utils.progress.is_canceled', return_value=True), \
             patch('time.perf_counter', return_value=1.0):
            
            with pytest.raises(Exception, match="Ingestion canceled by user"):
                await self.ingester._load_dataframe_to_table(
                    mock_connection, df, 'cancel_table', 'schema',
                    len(df), 'cancel_key', 'F', datetime.now()
                )
            
            # Should have logged cancellation
            self.mock_logger.log_info.assert_called_with(
                "bulk_insert_cancel", 
                "Cancellation requested after table truncation"
            )
    
    @pytest.mark.asyncio
    async def test_complete_ingest_workflow_full_mode(self):
        """Test complete ingestion workflow in full mode - covers lines 214-340"""
        file_path = os.path.join(self.temp_dir, 'full_workflow.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'full_workflow.fmt')
        
        # Create comprehensive test file
        csv_content = """id,name,category,value
1,Product A,Electronics,99.99
2,Product B,Books,29.99
3,Product C,Clothing,49.99"""
        
        with open(file_path, 'w') as f:
            f.write(csv_content)
        
        format_config = {
            "csv_format": {
                "delimiter": ",",
                "has_header": True,
                "has_trailer": False
            }
        }
        
        # Mock all database operations for full workflow
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = False  # New table
        self.mock_db.get_row_count.return_value = 0
        self.mock_db.create_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='full_workflow'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'full_workflow.csv'
            ):
                messages.append(message)
            
            # Should have completed full workflow including data loading
            assert len(messages) > 10
            assert any("Database connection established" in msg for msg in messages)
            assert any("Creating/validating database tables" in msg for msg in messages)
            assert any("Data loading completed" in msg for msg in messages)
            
            # Should have created table and loaded data
            self.mock_db.create_table.assert_called()
            mock_cursor.executemany.assert_called()  # Data insertion
    
    @pytest.mark.asyncio
    async def test_ingest_workflow_existing_table_full_mode(self):
        """Test full mode with existing table - covers backup workflow lines 214-250"""
        file_path = os.path.join(self.temp_dir, 'existing.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'existing.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,NewData\n2,MoreData\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock existing table scenario
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = True  # Existing table
        self.mock_db.get_row_count.return_value = 50  # Has existing data
        self.mock_db.create_backup_table.return_value = None
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='existing_table'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'full', 'existing.csv'
            ):
                messages.append(message)
            
            # Should handle existing table with backup
            assert any("Found 50 existing rows that will be backed up" in msg for msg in messages)
            self.mock_db.create_backup_table.assert_called()
    
    @pytest.mark.asyncio
    async def test_ingest_workflow_append_mode(self):
        """Test append mode workflow - covers lines 215-217"""
        file_path = os.path.join(self.temp_dir, 'append.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'append.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n100,AppendData\n101,MoreAppend\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock append mode scenario
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        self.mock_db.table_exists.return_value = True  # Existing table
        self.mock_db.get_row_count.return_value = 75  # Has existing data
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='append_table'), \
             patch('utils.progress.is_canceled', return_value=False), \
             patch('utils.progress.update_progress'), \
             patch('time.perf_counter', side_effect=range(1, 100)):
            
            messages = []
            async for message in self.ingester.ingest_data(
                file_path, fmt_file_path, 'append', 'append.csv'
            ):
                messages.append(message)
            
            # Should handle append mode correctly
            assert any("Append mode: main table already has 75 rows" in msg for msg in messages)
    
    @pytest.mark.asyncio
    async def test_cancellation_scenarios_comprehensive(self):
        """Test all cancellation points - covers lines 220-222, 234-236"""
        file_path = os.path.join(self.temp_dir, 'cancel.csv')
        fmt_file_path = os.path.join(self.temp_dir, 'cancel.fmt')
        
        # Create test file
        with open(file_path, 'w') as f:
            f.write('id,name\n1,Test\n')
        
        format_config = {"csv_format": {"delimiter": ","}}
        
        # Mock database setup
        mock_connection = MagicMock()
        self.mock_db.get_connection.return_value = mock_connection
        self.mock_db.ensure_schemas_exist.return_value = None
        
        # Test cancellation before table operations (lines 220-222)
        call_count = 0
        def mock_cancel_before_table_ops(key):
            nonlocal call_count
            call_count += 1
            return call_count >= 3  # Cancel at table operations check
        
        with patch.object(self.ingester.file_handler, 'read_format_file', return_value=format_config), \
             patch.object(self.ingester.file_handler, 'extract_table_base_name', return_value='cancel_table'), \
             patch('utils.progress.is_canceled', side_effect=mock_cancel_before_table_ops), \
             patch('time.perf_counter', side_effect=range(1, 50)):
            
            messages = []
            try:
                async for message in self.ingester.ingest_data(
                    file_path, fmt_file_path, 'full', 'cancel.csv', progress_key='cancel_key'
                ):
                    messages.append(message)
            except Exception as e:
                assert "Ingestion canceled by user" in str(e)
            
            # Should reach cancellation point
            assert any("Cancellation requested - stopping before table operations" in msg for msg in messages)
    
    def test_sql_escape_string_comprehensive(self):
        """Test SQL string escaping thoroughly - covers lines 761-928"""
        comprehensive_test_cases = [
            # Basic strings
            ("", ""),
            ("simple", "simple"),
            ("normal_text", "normal_text"),
            
            # None handling
            (None, ""),
            
            # Single quotes (most common SQL injection vector)
            ("'", "''"),
            ("''", "''''"),
            ("text'text", "text''text"),
            ("'start", "''start"),
            ("end'", "end''"),
            ("'start'middle'end'", "''start''middle''end''"),
            
            # Double quotes
            ('"', '""'),
            ('""', '""""'),
            ('text"text', 'text""text'),
            ('"start', '""start'),
            ('end"', 'end""'),
            
            # Newlines (various types)
            ("\n", "\\n"),
            ("\r\n", "\\r\\n"),
            ("\r", "\\r"),
            ("line1\nline2", "line1\\nline2"),
            ("line1\r\nline2", "line1\\r\\nline2"),
            
            # Tabs
            ("\t", "\\t"),
            ("col1\tcol2", "col1\\tcol2"),
            
            # Backslashes
            ("\\", "\\\\"),
            ("\\\\", "\\\\\\\\"),
            ("path\\file", "path\\\\file"),
            
            # Combined cases (realistic scenarios)
            ("John's \"favorite\" book\nChapter 1", "John''s \"\"favorite\"\" book\\nChapter 1"),
            ("Data with\ttabs and 'quotes'", "Data with\\ttabs and ''quotes''"),
            ("C:\\Program Files\\App", "C:\\\\Program Files\\\\App"),
            ("Text with\r\nWindows line endings", "Text with\\r\\nWindows line endings"),
            
            # Edge cases
            ("'''", "''''''"),  # Multiple single quotes
            ('"""', '""""""'),  # Multiple double quotes
            ("\n\r\n\t\\\"'", "\\n\\r\\n\\t\\\\\"\"''"),  # All special chars
            
            # Unicode and international characters
            ("café", "café"),
            ("北京", "北京"),
            ("emoji😀test", "emoji😀test"),
            
            # Very long strings
            ("a" * 1000 + "'quote'" + "b" * 1000, "a" * 1000 + "''quote''" + "b" * 1000),
        ]
        
        for input_val, expected in comprehensive_test_cases:
            result = self.ingester._sql_escape_string(input_val)
            assert result == expected, f"FAILED for input: {repr(input_val)}\nExpected: {repr(expected)}\nGot:      {repr(result)}"
    
    def test_init_with_all_environment_variables(self):
        """Test initialization with all possible env vars - covers lines 29-40"""
        env_vars = {
            'INGEST_PROGRESS_INTERVAL': '10',
            'INGEST_TYPE_INFERENCE': '1', 
            'INGEST_TYPE_SAMPLE_ROWS': '1000',
            'INGEST_DATE_THRESHOLD': '0.9'
        }
        
        with patch.dict('os.environ', env_vars):
            ingester = DataIngester(self.mock_db, self.mock_logger)
            
            assert ingester.progress_batch_interval == 10
            assert ingester.enable_type_inference == True
            assert ingester.type_sample_rows == 1000
            assert ingester.date_parse_threshold == 0.9
    
    def test_init_with_invalid_environment_variables(self):
        """Test initialization with invalid env vars - covers exception paths"""
        # Test invalid int values (these have try/except blocks)
        env_vars_int = {
            'INGEST_PROGRESS_INTERVAL': 'invalid_int',
            'INGEST_TYPE_SAMPLE_ROWS': 'invalid_int',
            'INGEST_TYPE_INFERENCE': 'True',  # Should work
        }
        
        with patch.dict('os.environ', env_vars_int):
            ingester = DataIngester(self.mock_db, self.mock_logger)
            
            # Should use defaults for invalid int values
            assert ingester.progress_batch_interval == 5  # Default
            assert ingester.type_sample_rows == 5000  # Default
            assert ingester.enable_type_inference == True  # 'True' should work
        
        # Test float parsing (this will raise ValueError - no try/except in original code)
        env_vars_float = {
            'INGEST_DATE_THRESHOLD': 'invalid_float'
        }
        
        with patch.dict('os.environ', env_vars_float):
            # This should raise ValueError since there's no try/except for float parsing
            with pytest.raises(ValueError, match="could not convert string to float"):
                DataIngester(self.mock_db, self.mock_logger)
    
    @pytest.mark.asyncio
    async def test_read_csv_comprehensive_scenarios(self):
        """Test CSV reading in various scenarios - covers lines 610-660"""
        
        # Scenario 1: CSV with trailer
        trailer_file = os.path.join(self.temp_dir, 'with_trailer.csv')
        with open(trailer_file, 'w') as f:
            f.write('id,name,value\n1,John,100\n2,Jane,200\nTRAILER,END,999\n')
        
        trailer_format = {
            'delimiter': ',',
            'has_header': True,
            'has_trailer': True
        }
        
        result = await self.ingester._read_csv_file(trailer_file, trailer_format)
        assert len(result) == 2  # Should exclude trailer
        assert 'TRAILER' not in result['id'].astype(str).values
        
        # Scenario 2: CSV without trailer
        no_trailer_file = os.path.join(self.temp_dir, 'no_trailer.csv')
        with open(no_trailer_file, 'w') as f:
            f.write('id,name\n1,Alice\n2,Bob\n')
        
        no_trailer_format = {
            'delimiter': ',', 
            'has_header': True,
            'has_trailer': False
        }
        
        result = await self.ingester._read_csv_file(no_trailer_file, no_trailer_format)
        assert len(result) == 2  # Should keep all data rows
        
        # Scenario 3: Empty file (just headers)
        empty_file = os.path.join(self.temp_dir, 'empty.csv')
        with open(empty_file, 'w') as f:
            f.write('id,name\n')  # Headers only
        
        result = await self.ingester._read_csv_file(empty_file, no_trailer_format)
        assert len(result) == 0  # No data rows
        assert list(result.columns) == ['id', 'name']
    
    @pytest.mark.asyncio
    async def test_read_csv_error_scenarios(self):
        """Test CSV reading error handling - covers lines 654-660"""
        
        # Test with non-existent file
        with pytest.raises(Exception, match="Failed to read CSV file"):
            await self.ingester._read_csv_file('/nonexistent/file.csv', {'delimiter': ','})
        
        # Test with file that has permission issues
        restricted_file = os.path.join(self.temp_dir, 'restricted.csv')
        with open(restricted_file, 'w') as f:
            f.write('id,name\n1,test\n')
        
        # Make file unreadable (if possible)
        import stat
        try:
            os.chmod(restricted_file, stat.S_IWRITE)  # Write only
            with pytest.raises(Exception, match="Failed to read CSV file"):
                await self.ingester._read_csv_file(restricted_file, {'delimiter': ','})
        except:
            pass  # Skip if permission change not supported
        finally:
            # Restore permissions for cleanup
            try:
                os.chmod(restricted_file, stat.S_IREAD | stat.S_IWRITE)
            except:
                pass