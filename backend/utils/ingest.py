"""
Data ingestion utilities for processing CSV files and loading into SQL Server
"""

import os
import re
import pandas as pd
import traceback
import time
from typing import AsyncGenerator, Dict, Any, List
from datetime import datetime

from utils.database import DatabaseManager
from utils.file_handler import FileHandler
from utils.logger import Logger
from utils import progress as prog


class DataIngester:
    """Handles CSV data ingestion into SQL Server database"""
    
    def __init__(self, db_manager: DatabaseManager, logger: Logger):
        self.db_manager = db_manager
        self.logger = logger
        self.file_handler = FileHandler()
        # Batch / progress parameters (only multi-row batching retained)
        # Fixed batch size (multi-row VALUES per insert) explicitly set to 990
        self.batch_size = 990
        try:
            self.progress_batch_interval = int(os.getenv("INGEST_PROGRESS_INTERVAL", "5"))
        except ValueError:
            self.progress_batch_interval = 5
        # Type inference
        self.enable_type_inference = os.getenv("INGEST_TYPE_INFERENCE", "0") in ("1", "true", "True")
        try:
            self.type_sample_rows = int(os.getenv("INGEST_TYPE_SAMPLE_ROWS", "5000"))
        except ValueError:
            self.type_sample_rows = 5000
        self.date_parse_threshold = float(os.getenv("INGEST_DATE_THRESHOLD", "0.8"))  # 80% rows
        # Legacy bulk/stream settings removed.
    
    async def ingest_data(
        self, 
        file_path: str, 
        fmt_file_path: str, 
        load_mode: str, 
        filename: str
    ) -> AsyncGenerator[str, None]:
        """Main ingestion function.
        Simplified: always reads full file then performs multi-row INSERT batching (≤990 rows per statement).
        load_mode: 'full' or 'append'.
        """
        connection = None
        overall_start = time.perf_counter()

        # Progress key (simple filename-based token)
        progress_key = re.sub(r'[^a-zA-Z0-9_]', '_', filename)
        prog.init_progress(progress_key)
        try:
            yield "Starting data ingestion process..."

            # Check for cancellation at the start
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping ingestion"
                raise Exception("Ingestion canceled by user")

            # Step 1: Get database connection
            yield "Connecting to database..."
            t_connect_start = time.perf_counter()
            connection = self.db_manager.get_connection()
            self.db_manager.ensure_schemas_exist(connection)
            yield f"Database connection established (took {(time.perf_counter()-t_connect_start):.2f}s)"

            # Check for cancellation after database connection
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping after database connection"
                raise Exception("Ingestion canceled by user")

            # Step 2: Extract table base name
            yield "Extracting table information..."
            t_extract_name = time.perf_counter()
            table_base_name = self.file_handler.extract_table_base_name(filename)
            yield f"Table name extracted in {(time.perf_counter()-t_extract_name):.2f}s"

            table_name = table_base_name
            stage_table_name = f"{table_base_name}_stage"

            yield f"Table names: main={table_name}, stage={stage_table_name}"

            # Step 3: Read format configuration
            yield "Reading CSV format configuration..."
            t_fmt = time.perf_counter()
            format_config = await self.file_handler.read_format_file(fmt_file_path)
            csv_format = format_config["csv_format"]
            yield f"Format configuration loaded ({(time.perf_counter()-t_fmt):.2f}s)"

            # Check for cancellation after format loading
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping after format configuration"
                raise Exception("Ingestion canceled by user")


            # Step 4: Read and validate CSV file (always full read)
            yield "Reading CSV file..."
            t_read = time.perf_counter()
            df = await self._read_csv_file(file_path, csv_format, progress_key)
            
            # Trailer handling is now done inside _read_csv_file
            has_trailer = csv_format.get('has_trailer', False)
            if has_trailer:
                yield "Trailer detected: last row removed during CSV read"
            
            total_rows = len(df)
            prog.update_progress(progress_key, total=total_rows, inserted=0, stage='read_csv')
            yield f"CSV file loaded: {total_rows} rows (read in {(time.perf_counter()-t_read):.2f}s, {(total_rows/(time.perf_counter()-t_read) if total_rows else 0):.0f} rows/s)"
            if total_rows == 0:
                yield "ERROR! CSV file contains no data rows"
                
                # Auto-cancel on empty file
                if progress_key:
                    prog.request_cancel(progress_key)
                    yield "Upload process automatically canceled due to empty file"
                
                return

            # Step 5: Process headers
            yield "Processing CSV headers..."
            t_headers = time.perf_counter()
            original_headers = list(df.columns)
            sanitized_headers = self._sanitize_headers(original_headers)
            sanitized_headers = self._deduplicate_headers(sanitized_headers)
            valid_headers = [(orig, san) for orig, san in zip(original_headers, sanitized_headers) if san]
            if not valid_headers:
                yield "ERROR! No valid columns found after header sanitization"
                
                # Auto-cancel on no valid headers
                if progress_key:
                    prog.request_cancel(progress_key)
                    yield "Upload process automatically canceled due to invalid headers"
                
                return
            yield f"Headers processed: {len(valid_headers)} valid columns ({(time.perf_counter()-t_headers):.2f}s)"

            # Check for cancellation after header processing
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping after header processing"
                raise Exception("Ingestion canceled by user")

            # Step 6: Prepare column definitions
            columns = []
            if self.enable_type_inference:
                yield "Inferring column data types..."
                t_infer = time.perf_counter()
                sample_df = df.head(self.type_sample_rows)
                # Rename sample_df columns to sanitized headers so inference uses consistent keys
                rename_map = {orig: san for orig, san in valid_headers}
                sample_df_renamed = sample_df.rename(columns=rename_map)
                inferred_map = self._infer_types(sample_df_renamed, [san for _, san in valid_headers])
                elapsed_inf = time.perf_counter()-t_infer
                yield f"Type inference complete in {elapsed_inf:.2f}s"
                try:
                    self._persist_inferred_schema(fmt_file_path, inferred_map)
                    yield "Inferred schema persisted to format file"
                except Exception as _e:
                    yield f"WARNING: Failed to persist inferred schema: {_e}"
                # All columns are varchar only - no type validation needed
                yield "All columns configured as varchar with appropriate lengths"
                
                # Check for cancellation after type inference
                if progress_key and prog.is_canceled(progress_key):
                    yield "Cancellation requested - stopping after type inference"
                    raise Exception("Ingestion canceled by user")
            else:
                inferred_map = {san: 'varchar(4000)' for _, san in valid_headers}
            for _, sanitized_header in valid_headers:
                dtype = inferred_map.get(sanitized_header, 'varchar(4000)')
                columns.append({'name': sanitized_header,'data_type': dtype})
            # All columns are varchar - no numeric validation needed
            yield "Column definitions prepared"
            prog.update_progress(progress_key, stage='prepared')

            # Check for cancellation before processing data
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping before data processing"
                raise Exception("Ingestion canceled by user")

            # Step 7: Check existing data for backup (BEFORE creating tables)
            existing_rows = 0
            table_exists = self.db_manager.table_exists(connection, table_name)

            if table_exists and load_mode == "full":
                yield "Checking existing data for backup..."
                existing_rows = self.db_manager.get_row_count(connection, table_name)
                if existing_rows > 0:
                    yield f"Found {existing_rows} existing rows that will be backed up"
            elif table_exists and load_mode == "append":
                existing_rows = self.db_manager.get_row_count(connection, table_name)
                yield f"Append mode: main table already has {existing_rows} rows (will preserve and append)"

            # Check for cancellation before table operations
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping before table operations"
                raise Exception("Ingestion canceled by user")

            # Step 8: Create/validate tables
            yield "Creating/validating database tables..."
            t_tables = time.perf_counter()

            stage_exists = self.db_manager.table_exists(connection, stage_table_name)
            if table_exists or stage_exists:
                yield "Existing tables found, validating schema..."

            backup_exists = self.db_manager.table_exists(connection, table_base_name + '_backup')
            if backup_exists:
                yield "Backup table exists, validating schema..."
            else:
                yield "No backup table found, creating new backup..."
                self.db_manager.create_backup_table(connection, table_name, columns)
            
            # Backup existing data BEFORE dropping table
            if existing_rows > 0 and load_mode == "full":
                yield "Backing up existing data before table recreation..."
                backup_rows = self.db_manager.backup_existing_data(connection, table_name, table_base_name)
                yield f"Existing data backed up: {backup_rows} rows with version tracking"

            # Check for cancellation before main table operations
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping before main table operations"
                raise Exception("Ingestion canceled by user")

            # Main table handling
            if load_mode == "full":
                self.db_manager.create_table(connection, table_name, columns, add_metadata_columns=True)
            else:  # append
                if not table_exists:
                    yield "Append mode: main table does not exist yet, creating new main table..."
                    self.db_manager.create_table(connection, table_name, columns, add_metadata_columns=True)
                else:
                    yield "Append mode: preserving existing main table schema"
                    # Sync main table schema to handle wider varchar columns
                    if self.enable_type_inference:
                        try:
                            sync_actions = self.db_manager.sync_table_schema(connection, table_name, columns)
                            yield f"Main table schema sync: added={len(sync_actions['added'])}, widened={len(sync_actions['widened'])}, skipped={len(sync_actions['skipped'])}"
                        except Exception as _e:
                            yield f"WARNING: Main table schema sync failed: {_e}"

            # Handle stage table - check for schema compatibility before creation
            if stage_exists:
                yield "Stage table exists, checking schema compatibility..."
                try:
                    # Always try to sync stage table schema to handle wider varchar columns
                    sync_actions = self.db_manager.sync_table_schema(connection, stage_table_name, columns)
                    if sync_actions['added'] or sync_actions['widened']:
                        yield f"Stage table schema updated: added={len(sync_actions['added'])}, widened={len(sync_actions['widened'])}"
                    else:
                        yield "Stage table schema is compatible, reusing existing table"
                        # Clear existing data since we're reusing the table
                        self.db_manager.truncate_table(connection, stage_table_name)
                except Exception as e:
                    yield f"Stage table schema sync failed, recreating table: {str(e)}"
                    # If sync fails, drop and recreate
                    self.db_manager.create_table(connection, stage_table_name, columns, add_metadata_columns=True)
            else:
                # Create new stage table
                self.db_manager.create_table(connection, stage_table_name, columns, add_metadata_columns=True)

            # Create/validate backup table with schema compatibility check
            self.db_manager.create_backup_table(connection, table_name, columns)
            self.db_manager.create_validation_procedure(connection, table_base_name)
            yield f"Database tables created/validated ({(time.perf_counter()-t_tables):.2f}s)"
            prog.update_progress(progress_key, stage='tables_ready')

            # Step 9: Process and load data to stage table
            yield "Processing CSV data..."
            t_process = time.perf_counter()
            column_mapping = {orig: san for orig, san in valid_headers}
            df_processed = df[list(column_mapping.keys())].rename(columns=column_mapping)
            
            for col in df_processed.columns:
                df_processed[col] = df_processed[col].astype(str).replace({'nan':'','None':''})
            # All columns are varchar - no numeric validation needed
            yield f"Data processing completed ({(time.perf_counter()-t_process):.2f}s)"
            
            # Check for cancellation after data processing
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping after data processing"
                raise Exception("Ingestion canceled by user")
            
            yield "Loading data to stage table..."
            t_load = time.perf_counter()
            # Update progress to show we're starting the insert phase
            prog.update_progress(progress_key, inserted=0, total=total_rows, stage='loading')
            await self._load_dataframe_to_table(
                connection,
                df_processed,
                stage_table_name,
                self.db_manager.data_schema,
                total_rows,
                progress_key
            )
            elapsed_load = time.perf_counter()-t_load
            rps = (total_rows/elapsed_load) if elapsed_load>0 else 0
            yield f"Data loaded to stage table: {total_rows} rows in {elapsed_load:.2f}s ({rps:.0f} rows/s)"
            prog.update_progress(progress_key, stage='loaded_stage')

            # Check for cancellation after stage loading
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping after stage table loading"
                raise Exception("Ingestion canceled by user")

            # Check for cancellation before validation
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping before validation"
                raise Exception("Ingestion canceled by user")

            # Step 10: Validate data
            yield "Executing data validation..."
            t_validate = time.perf_counter()

            validation_result = self.db_manager.execute_validation_procedure(connection, table_base_name)
            validation_issues = validation_result.get("validation_result", 0)

            if validation_issues > 0:
                issue_list = validation_result.get("validation_issue_list", [])
                yield f"ERROR! Validation failed with {validation_issues} issues:"
                for issue in issue_list:
                    yield f"  - Issue {issue.get('issue_id')}: {issue.get('issue_detail')}"
                yield "Data remains in stage table for review"
                
                # Auto-cancel on validation failure
                if progress_key:
                    prog.request_cancel(progress_key)
                    yield "Upload process automatically canceled due to validation errors"
                
                return

            yield f"Data validation passed ({(time.perf_counter()-t_validate):.2f}s)"
            prog.update_progress(progress_key, stage='validated')

            # Check for cancellation after validation
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping after validation"
                raise Exception("Ingestion canceled by user")

            # Step 11: Prepare for data load (full load mode already handled backup)
            if load_mode == "full":
                yield "Preparing for full load (existing data already backed up if any existed)"
            else:
                yield "Append mode: will insert new rows into existing main table"

            # All columns are varchar - no numeric sanitation needed

            # Check for cancellation before final data move
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping before final data move"
                raise Exception("Ingestion canceled by user")

            # Step 12: Move data from stage to main table
            yield "Moving data from stage to main table..."
            t_move = time.perf_counter()
            cursor = connection.cursor()
            # Use dynamic SQL with proper quoting to prevent SQL injection
            insert_sql = (
                "INSERT INTO [" + self.db_manager.data_schema + "].[" + table_name + "] "
                "SELECT * FROM [" + self.db_manager.data_schema + "].[" + stage_table_name + "]"
            )
            cursor.execute(insert_sql)
            final_rows = cursor.rowcount
            if load_mode == "append":
                yield f"Data successfully appended: {final_rows} new rows ({(time.perf_counter()-t_move):.2f}s). Total rows now may be ~{existing_rows + final_rows if existing_rows else final_rows}"
            else:
                yield f"Data successfully loaded to main table: {final_rows} rows ({(time.perf_counter()-t_move):.2f}s)"
                prog.update_progress(progress_key, stage='moved_main')

            # Check for cancellation before archiving
            if progress_key and prog.is_canceled(progress_key):
                yield "Cancellation requested - stopping before archiving"
                raise Exception("Ingestion canceled by user")

            # Step 13: Archive the file
            yield "Archiving processed file..."
            t_archive = time.perf_counter()
            archive_path = self.file_handler.move_to_archive(file_path, filename)
            yield f"File archived to: {os.path.basename(archive_path)} ({(time.perf_counter()-t_archive):.2f}s)"
            prog.update_progress(progress_key, stage='archived')

            total_time = time.perf_counter()-overall_start
            yield f"Data ingestion completed successfully! Total time {total_time:.2f}s ({(total_rows/total_time) if total_time>0 else 0:.0f} rows/s overall)"
            prog.mark_done(progress_key)

            await self.logger.log_info(
                "data_ingestion",
                f"Successfully ingested {final_rows} rows from {filename} to table {table_name}"
            )

        except Exception as e:
            error_msg = f"Data ingestion failed: {str(e)}"
            traceback_info = traceback.format_exc()
            yield f"ERROR! {error_msg}"
            yield f"ERROR! Traceback: {traceback_info}"
            
            # Auto-cancel on any error to stop the upload process
            if progress_key:
                prog.request_cancel(progress_key)
                yield "Upload process automatically canceled due to error"
            
            await self.logger.log_error(
                "data_ingestion",
                error_msg,
                traceback_info,
                {"filename": filename, "table_name": table_base_name if 'table_base_name' in locals() else None}
            )
            prog.mark_error(progress_key, error_msg)
        finally:
            if connection:
                connection.close()
    
    async def _read_csv_file(self, file_path: str, csv_format: Dict[str, Any], progress_key: str = None) -> pd.DataFrame:
        """Read CSV file with specified format parameters"""
        try:
            # Extract format parameters
            delimiter = csv_format.get("column_delimiter", ",")
            text_qualifier = csv_format.get("text_qualifier", '"')
            skip_lines = csv_format.get("skip_lines", 0)
            
            # Handle row delimiter (pandas uses lineterminator)
            row_delimiter = csv_format.get("row_delimiter", "\n")
            
            # Pandas only supports single-character line terminators
            # Map complex delimiters to standard ones
            if len(row_delimiter) > 1 or row_delimiter in ['|""\\r\\n', '\\r\\n', '\\n', '\\r']:
                if '\\r\\n' in row_delimiter or '\r\n' in row_delimiter:
                    row_delimiter = "\r\n"
                elif '\\n' in row_delimiter or '\n' in row_delimiter:
                    row_delimiter = "\n"
                elif '\\r' in row_delimiter or '\r' in row_delimiter:
                    row_delimiter = "\r"
                else:
                    row_delimiter = "\n"  # Safe fallback
            
            # Read CSV with pandas
            # For complex row delimiters, let pandas auto-detect line endings
            pandas_kwargs = {
                'delimiter': delimiter,
                'quotechar': text_qualifier if text_qualifier else None,
                'skiprows': skip_lines,
                'dtype': str,  # Read everything as string
                'keep_default_na': False,  # Don't convert empty strings to NaN
                'na_values': [],  # Don't convert anything to NaN
                'encoding': 'utf-8'
            }
            
            # Only set lineterminator for simple single-character delimiters
            if len(row_delimiter) == 1 and row_delimiter in ['\n', '\r']:
                pandas_kwargs['lineterminator'] = row_delimiter
            
            try:
                df = pd.read_csv(file_path, **pandas_kwargs)
            except Exception as e:
                # Fallback: if multi-character (or problematic) line terminator slipped through
                # or pandas raises the specific ValueError, retry without lineterminator so it auto-detects
                if 'Only length-1 line terminators supported' in str(e):
                    pandas_kwargs.pop('lineterminator', None)
                    df = pd.read_csv(file_path, **pandas_kwargs)
                else:
                    raise
            
            # Handle trailer removal immediately after successful CSV read
            has_trailer = csv_format.get('has_trailer', False)
            if has_trailer and len(df) > 0:
                original_size = len(df)
                df = df.iloc[:-1]  # Remove last row (trailer)
                await self.logger.log_info(
                    "trailer_removal",
                    f"Trailer removed: DataFrame size reduced from {original_size} to {len(df)} rows"
                )
            
            await self.logger.log_info(
                "csv_processing",
                f"Final DataFrame size after all CSV processing: {len(df)} rows, has_trailer: {has_trailer}"
            )
            
            # Log first and last rows for verification
            if len(df) > 0:
                first_row = df.iloc[0].to_dict()
                await self.logger.log_info(
                    "csv_data_sample",
                    f"First row after processing: {first_row}"
                )
                
                if len(df) > 1:
                    last_row = df.iloc[-1].to_dict()
                    await self.logger.log_info(
                        "csv_data_sample",
                        f"Last row after processing: {last_row}"
                    )
                else:
                    await self.logger.log_info(
                        "csv_data_sample",
                        "Only one row in DataFrame after processing"
                    )
            
            return df
        except Exception as e:
            # Auto-cancel on CSV reading errors
            if progress_key:
                prog.request_cancel(progress_key)
                await self.logger.log_info(
                    "auto_cancel",
                    f"Upload process automatically canceled due to CSV reading error: {str(e)}"
                )
            
            raise Exception(f"Failed to read CSV file {file_path}: {str(e)}")
    
    def _sanitize_headers(self, headers: List[str]) -> List[str]:
        """Sanitize column headers to be valid SQL identifiers"""
        sanitized = []
        
        for header in headers:
            if not header or header.strip() == '':
                sanitized.append('')  # Will be filtered out later
                continue
            
            # Remove extra whitespace
            clean_header = header.strip()
            
            # Replace invalid characters with underscore
            clean_header = re.sub(r'[^a-zA-Z0-9_]', '_', clean_header)
            
            # Ensure it starts with letter or underscore
            if clean_header and not (clean_header[0].isalpha() or clean_header[0] == '_'):
                clean_header = f"col_{clean_header}"
            
            # Limit length (SQL Server identifier limit is 128 characters)
            if len(clean_header) > 120:
                clean_header = clean_header[:120]
            
            sanitized.append(clean_header)
        
        return sanitized
    
    def _deduplicate_headers(self, headers: List[str]) -> List[str]:
        """Ensure unique sanitized header names by appending incremental suffixes."""
        seen = {}
        result = []
        for h in headers:
            if h == '':
                result.append(h)
                continue
            base = h
            if base not in seen:
                seen[base] = 0
                result.append(base)
            else:
                seen[base] += 1
                new_name = f"{base}_{seen[base]}"
                while new_name in seen:
                    seen[base] += 1
                    new_name = f"{base}_{seen[base]}"
                seen[new_name] = 0
                result.append(new_name)
        return result
    
    def _infer_types(self, sample_df: pd.DataFrame, columns: List[str]) -> Dict[str, str]:
        """Determine varchar lengths from sample dataframe (all columns as varchar only)."""
        inferred = {}
        for col in columns:
            series = sample_df[col].dropna().astype(str)
            series = series[series != '']
            if series.empty:
                inferred[col] = 'varchar(1024)'
                continue
            
            # Only detect string length - no type inference
            # Sample more rows for better length detection accuracy
            sample_size = min(len(series), 1000)  # Sample up to 1000 rows
            max_len = int(series.head(sample_size).map(len).max())
            
            # Conservative varchar sizing based on detected max length
            if max_len <= 500:
                size = 1024
            elif max_len > 500 and max_len <= 1000:
                size = 4000
            elif max_len > 1000 and max_len <= 4000:
                size = 8000
            else:
                size = 'MAX'  # Use varchar(MAX) for very long text
            
            inferred[col] = f'varchar({size})' if size != 'MAX' else 'varchar(MAX)'
        return inferred
    
    def _persist_inferred_schema(self, fmt_file_path: str, inferred_map: Dict[str, str]) -> None:
        """Append inferred schema info into existing .fmt file under key inferred_schema."""
        import json
        if not os.path.exists(fmt_file_path):
            return
        with open(fmt_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        data['inferred_schema'] = inferred_map
        with open(fmt_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
    
    async def _load_dataframe_to_table(
        self, 
        connection, 
        df: pd.DataFrame, 
        table_name: str, 
        schema: str,
        total_rows: int,
        progress_key: str | None = None
    ) -> None:
        try:
            cursor = connection.cursor()

            # Clear stage table first - use dynamic SQL with proper quoting
            truncate_sql = "TRUNCATE TABLE [" + schema + "].[" + table_name + "]"
            cursor.execute(truncate_sql)
            connection.commit()

            # Check for cancellation after table truncation
            if progress_key and prog.is_canceled(progress_key):
                await self.logger.log_info(
                    "bulk_insert_cancel",
                    "Cancellation requested after table truncation"
                )
                raise Exception("Ingestion canceled by user")

            # Prepare column list and SQL statement once
            data_columns = [col for col in df.columns]
            column_list = ', '.join([f'[{col}]' for col in data_columns])
            # Trailer removal is now handled before this function is called
            
            # Batch sizing
            # SQL Server supports maximum 1000 row value expressions per VALUES clause.
            # We intentionally cap at 990 to keep headroom and avoid edge-case failures if additional
            # rows (e.g., metadata) were ever appended in future logic.
            try:
                effective_batch = int(os.getenv("INGEST_BATCH_SIZE", "500"))
                # Cap at 990 for SQL Server safety
                if effective_batch > 990:
                    effective_batch = 990
            except ValueError:
                effective_batch = 500
            total = len(df)
            start_time = time.perf_counter()
            await self.logger.log_info(
                "bulk_insert",
                f"Starting multi-row VALUES insert: {total} rows, batch_rows={effective_batch}"
            )
            inserted = 0
            batch_count = 0

            # All columns are varchar - no datetime processing needed

            def prepare_value(val, col_name):
                """Prepare value for parameterized query - all columns as varchar strings"""
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                s = str(val).strip()
                if s == '' or s.lower() in {'none', 'nan', 'null'}:
                    return None
                return s

            while inserted < total:
                # Cancellation check
                if progress_key and prog.is_canceled(progress_key):
                    await self.logger.log_info(
                        "bulk_insert_cancel",
                        f"Cancellation requested after {inserted} rows; stopping batch loop"
                    )
                    break
                slice_df = df.iloc[inserted: inserted + effective_batch]
                batch_size = len(slice_df)
                
                # Use parameterized queries with multi-row VALUES to prevent SQL injection
                # Create placeholders for all rows in this batch: (?,?), (?,?), (?,?)...
                single_row_placeholders = '(' + ', '.join(['?' for _ in data_columns]) + ')'
                all_rows_placeholders = ', '.join([single_row_placeholders for _ in range(batch_size)])
                sql = f"INSERT INTO [{schema}].[{table_name}] ({column_list}) VALUES {all_rows_placeholders}"
                
                # Flatten all row values into a single parameter list
                batch_params = []
                for _, row in slice_df.iterrows():
                    row_values = [prepare_value(row[c], c) for c in data_columns]
                    batch_params.extend(row_values)  # Flatten into single list
                
                # Debug: Check parameter count before execution
                expected_params = batch_size * len(data_columns)
                actual_params = len(batch_params)
                placeholder_count = sql.count('?')
                
                if expected_params != actual_params or expected_params != placeholder_count:
                    error_msg = f"Parameter mismatch: expected={expected_params}, actual={actual_params}, placeholders={placeholder_count}, batch_size={batch_size}, columns={len(data_columns)}"
                    await self.logger.log_error("parameter_mismatch", error_msg)
                    raise Exception(error_msg)
                
                # Temporarily use individual INSERT statements to avoid SQL Server issues
                single_sql = f"INSERT INTO [{schema}].[{table_name}] ({column_list}) VALUES ({', '.join(['?' for _ in data_columns])})"

                connection.autocommit = False
                for row_idx, row in slice_df.iterrows():
                    row_values = []
                    try:
                        for col in data_columns:
                            value = prepare_value(row[col], col)
                            row_values.append(value)
                        cursor.execute(single_sql, row_values)
                    except Exception as e:
                        # Log detailed error information for debugging
                        error_details = f"Error at row {row_idx}: {str(e)}\n"
                        error_details += f"Column values: {dict(zip(data_columns, row_values))}\n"
                        error_details += f"Raw row data: {dict(row)}"
                        await self.logger.log_error("insert_row_error", error_details)
                        raise e
                    
                connection.commit()  # Commit after each batch of rows
                connection.autocommit = True

                inserted += batch_size
                batch_count += 1
                if progress_key:
                    prog.update_progress(progress_key, inserted=inserted, stage='inserting')
                    # Update progress every batch for real-time feedback
                    elapsed = time.perf_counter() - start_time
                    rate = inserted / elapsed if elapsed > 0 else 0
                    
                # Add small delay for demo purposes (remove in production)
                import asyncio
                if os.getenv("DEMO_SLOW_PROGRESS", "0") == "1":
                    await asyncio.sleep(0.5)  # Half second delay per batch for demonstration
                    
                # Log progress less frequently to avoid spam
                if batch_count % max(1, self.progress_batch_interval) == 0:
                    elapsed = time.perf_counter() - start_time
                    rate = inserted / elapsed if elapsed > 0 else 0
                    await self.logger.log_info(
                        "bulk_insert_progress",
                        f"Inserted {inserted}/{total} rows ({rate:.0f} rows/s)"
                    )
            connection.commit()
            if progress_key and prog.is_canceled(progress_key):
                raise Exception("Ingestion canceled by user")
            elapsed_total = time.perf_counter() - start_time
            rate_total = inserted / elapsed_total if elapsed_total > 0 else 0
            await self.logger.log_info(
                "bulk_insert",
                f"VALUES insert complete: {inserted} rows in {elapsed_total:.2f}s ({rate_total:.0f} rows/s, batches={batch_count})"
            )

        except Exception as e:
            try:
                connection.rollback()
            except Exception:
                pass
            
            # Auto-cancel on loading errors
            if progress_key:
                prog.request_cancel(progress_key)
                await self.logger.log_info(
                    "auto_cancel",
                    f"Upload process automatically canceled due to loading error: {str(e)}"
                )
            
            raise Exception(f"Failed to load data to table {table_name}: {str(e)}")
    
    # _insert_chunk removed: streaming mode deprecated.
