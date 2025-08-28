#!/usr/bin/env python3
"""
File Monitor Process for Reference Data Management
Monitors dropoff folders for CSV files and processes them automatically.
"""

import os
import sys
import time
import hashlib
import logging
from datetime import datetime
from pathlib import Path
import csv
import re

# Import the backend library
from backend_lib import ReferenceDataAPI
from utils.database import DatabaseManager

# Configuration
DROPOFF_BASE_PATH = "/home/lin/repo/reference_data_mgr/data/reference_data/dropoff"

# New folder structure with reference data categorization
REF_DATA_BASE_PATH = os.path.join(DROPOFF_BASE_PATH, "reference_data_table")
NON_REF_DATA_BASE_PATH = os.path.join(DROPOFF_BASE_PATH, "none_reference_data_table")

# Processing folders - now organized by reference data type and load type

# Monitor settings
MONITOR_INTERVAL = 15  # seconds
STABILITY_CHECKS = 6   # number of consecutive checks without size change
LOG_FILE = "/home/lin/repo/reference_data_mgr/logs/file_monitor.log"

# Database settings - using SQL Server ref schema instead of SQLite
TRACKING_TABLE = "File_Monitor_Tracking"
TRACKING_SCHEMA = "ref"

class FileMonitor:
    def __init__(self):
        self.setup_logging()
        self.setup_directories()
        self.db_manager = DatabaseManager()
        self.init_tracking_table()
        self.file_tracking = {}  # {file_path: {'size': int, 'mtime': float, 'stable_count': int}}
        
        # Initialize backend API
        try:
            self.api = ReferenceDataAPI()
            self.logger.info("Backend API initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize backend API: {e}")
            self.api = None
        
    def setup_logging(self):
        """Setup logging configuration"""
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def setup_directories(self):
        """Create necessary directories - processed and error folders in each load type"""
        # Create all necessary subdirectories
        for ref_type in ['reference_data_table', 'none_reference_data_table']:
            for load_type in ['fullload', 'append']:
                base_path = os.path.join(DROPOFF_BASE_PATH, ref_type, load_type)
                processed_path = os.path.join(base_path, 'processed')
                error_path = os.path.join(base_path, 'error')
                os.makedirs(processed_path, exist_ok=True)
                os.makedirs(error_path, exist_ok=True)
    
    def get_processed_path(self, is_reference_data, load_type):
        """Get the processed folder path for a specific file type"""
        ref_type = 'reference_data_table' if is_reference_data else 'none_reference_data_table'
        return os.path.join(DROPOFF_BASE_PATH, ref_type, load_type, 'processed')
    
    def get_error_path(self, is_reference_data, load_type):
        """Get the error folder path for a specific file type"""
        ref_type = 'reference_data_table' if is_reference_data else 'none_reference_data_table'
        return os.path.join(DROPOFF_BASE_PATH, ref_type, load_type, 'error')
            
    def init_tracking_table(self):
        """Initialize SQL Server table for tracking files in ref schema"""
        try:
            connection = self.db_manager.get_connection()
            cursor = connection.cursor()
            
            # Create file monitoring tracking table
            create_table_sql = f"""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{TRACKING_TABLE}' AND schema_id = SCHEMA_ID('{TRACKING_SCHEMA}'))
            BEGIN
                CREATE TABLE [{TRACKING_SCHEMA}].[{TRACKING_TABLE}] (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    file_path NVARCHAR(500) UNIQUE NOT NULL,
                    file_name NVARCHAR(255),
                    file_size BIGINT,
                    file_hash NVARCHAR(64),
                    load_type NVARCHAR(50),
                    table_name NVARCHAR(255),
                    detected_delimiter NVARCHAR(5),
                    detected_headers NVARCHAR(MAX),
                    is_reference_data BIT,
                    reference_config_inserted BIT DEFAULT 0,
                    status NVARCHAR(50),
                    created_at DATETIME2 DEFAULT GETDATE(),
                    processed_at DATETIME2,
                    error_message NVARCHAR(MAX)
                )
            END
            """
            cursor.execute(create_table_sql)
            connection.commit()
            connection.close()
            self.logger.info(f"File tracking table initialized in {TRACKING_SCHEMA}.{TRACKING_TABLE}")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize tracking table: {e}")
            raise
            
    def scan_folders(self):
        """Scan all folders for CSV files with reference data classification"""
        csv_files = []
        
        # Scan reference data table folders
        for load_type in ['fullload', 'append']:
            ref_data_path = os.path.join(REF_DATA_BASE_PATH, load_type)
            if os.path.exists(ref_data_path):
                for file_name in os.listdir(ref_data_path):
                    if file_name.lower().endswith('.csv'):
                        file_path = os.path.join(ref_data_path, file_name)
                        csv_files.append((file_path, load_type, True))  # True = reference data
        
        # Scan non-reference data table folders
        for load_type in ['fullload', 'append']:
            non_ref_data_path = os.path.join(NON_REF_DATA_BASE_PATH, load_type)
            if os.path.exists(non_ref_data_path):
                for file_name in os.listdir(non_ref_data_path):
                    if file_name.lower().endswith('.csv'):
                        file_path = os.path.join(non_ref_data_path, file_name)
                        csv_files.append((file_path, load_type, False))  # False = not reference data
                        
        return csv_files
        
    def check_file_stability(self, file_path):
        """Check if file is stable (not being written to)"""
        try:
            stat = os.stat(file_path)
            current_size = stat.st_size
            current_mtime = stat.st_mtime
            
            if file_path not in self.file_tracking:
                # New file discovered
                self.file_tracking[file_path] = {
                    'size': current_size,
                    'mtime': current_mtime,
                    'stable_count': 0
                }
                self.logger.info(f"New file detected: {file_path} (size: {current_size})")
                return False
                
            # Check if file has changed
            prev_info = self.file_tracking[file_path]
            if current_size != prev_info['size'] or current_mtime != prev_info['mtime']:
                # File changed, reset stability counter
                self.file_tracking[file_path] = {
                    'size': current_size,
                    'mtime': current_mtime,
                    'stable_count': 0
                }
                self.logger.info(f"File changed: {file_path} (new size: {current_size})")
                return False
            else:
                # File unchanged, increment stability counter
                self.file_tracking[file_path]['stable_count'] += 1
                stable_count = self.file_tracking[file_path]['stable_count']
                
                if stable_count >= STABILITY_CHECKS:
                    self.logger.info(f"File stable: {file_path} (checked {stable_count} times)")
                    return True
                else:
                    self.logger.debug(f"File stability check {stable_count}/{STABILITY_CHECKS}: {file_path}")
                    return False
                    
        except OSError as e:
            self.logger.error(f"Error checking file {file_path}: {e}")
            return False
            
    def detect_csv_format(self, file_path):
        """Auto-detect CSV format using backend API"""
        try:
            if self.api is None:
                raise Exception("Backend API not available")
                
            # Use the backend API for format detection
            result = self.api.detect_format(file_path)
            
            if result["success"]:
                detected_format = result["detected_format"]
                
                # Extract the information we need
                delimiter = detected_format.get("delimiter", ",")
                headers = detected_format.get("headers", [])
                sample_rows = detected_format.get("sample_rows", [])
                
                self.logger.info(f"Backend detected format - Delimiter: '{delimiter}', Headers: {len(headers)} columns")
                return delimiter, headers, sample_rows
            else:
                raise Exception(f"Backend format detection failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            self.logger.error(f"Error detecting CSV format for {file_path}: {e}")
            # Fallback to simple detection
            return self._fallback_csv_detection(file_path)
    
    def _fallback_csv_detection(self, file_path):
        """Fallback CSV detection method"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                first_line = f.readline().strip()
                if not first_line:
                    return ',', [], []
                
                # Simple delimiter detection
                delimiters = [',', ';', '\t', '|']
                best_delimiter = ','
                max_columns = 0
                
                for delimiter in delimiters:
                    columns = len(first_line.split(delimiter))
                    if columns > max_columns:
                        max_columns = columns
                        best_delimiter = delimiter
                
                # Extract headers
                headers = [col.strip('"').strip() for col in first_line.split(best_delimiter)]
                
                return best_delimiter, headers, []
                
        except Exception as e:
            self.logger.error(f"Fallback CSV detection failed for {file_path}: {e}")
            return ',', [], []
        
    def extract_table_name(self, file_path):
        """Extract table name using backend API"""
        try:
            if self.api is None:
                raise Exception("Backend API not available")
                
            table_name = self.api.extract_table_name_from_file(file_path)
            self.logger.info(f"Backend extracted table name: {table_name}")
            return table_name
            
        except Exception as e:
            self.logger.error(f"Error extracting table name for {file_path}: {e}")
            # Fallback to simple extraction
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0]
            
            # Remove date patterns and clean up
            table_name = re.sub(r'[-_.]\d{8}', '', table_name)
            table_name = re.sub(r'[-_.]\d{4}-\d{2}-\d{2}', '', table_name)
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
            table_name = re.sub(r'_+', '_', table_name)
            table_name = table_name.strip('_').lower()
            
            return table_name
        
    def calculate_file_hash(self, file_path):
        """Calculate file hash to detect duplicates"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.logger.error(f"Error calculating hash for {file_path}: {e}")
            return None
            
    def record_processing(self, file_path, load_type, table_name, delimiter, headers, status, is_reference_data=False, reference_config_inserted=False, error_msg=None):
        """Record file processing in SQL Server database"""
        try:
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            file_hash = self.calculate_file_hash(file_path)
            headers_str = ','.join(headers) if headers else ''
            
            connection = self.db_manager.get_connection()
            cursor = connection.cursor()
            
            # Use MERGE to insert or update record
            merge_sql = f"""
            MERGE [{TRACKING_SCHEMA}].[{TRACKING_TABLE}] AS target
            USING (SELECT ? AS file_path) AS source ON target.file_path = source.file_path
            WHEN MATCHED THEN
                UPDATE SET 
                    file_name = ?, file_size = ?, file_hash = ?, load_type = ?, 
                    table_name = ?, detected_delimiter = ?, detected_headers = ?,
                    is_reference_data = ?, reference_config_inserted = ?, status = ?,
                    processed_at = GETDATE(), error_message = ?
            WHEN NOT MATCHED THEN
                INSERT (file_path, file_name, file_size, file_hash, load_type, table_name,
                       detected_delimiter, detected_headers, is_reference_data, reference_config_inserted,
                       status, processed_at, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?);
            """
            
            cursor.execute(merge_sql, (
                file_path,  # for USING clause
                file_name, file_size, file_hash, load_type, table_name,
                delimiter, headers_str, is_reference_data, reference_config_inserted,
                status, error_msg,
                file_path, file_name, file_size, file_hash, load_type, table_name,  # for INSERT
                delimiter, headers_str, is_reference_data, reference_config_inserted,
                status, error_msg
            ))
            
            connection.commit()
            connection.close()
                      
        except Exception as e:
            self.logger.error(f"Error recording processing for {file_path}: {e}")
            
    def process_file(self, file_path, load_type, is_reference_data):
        """Process a stable CSV file using backend API"""
        self.logger.info(f"Processing file: {file_path} (load_type: {load_type}, reference_data: {is_reference_data})")
        
        try:
            if self.api is None:
                raise Exception("Backend API not available")
            
            # Detect format first for logging and tracking
            delimiter, headers, sample_rows = self.detect_csv_format(file_path)
            table_name = self.extract_table_name(file_path)
            
            self.logger.info(f"Detected format - Table: {table_name}, Delimiter: '{delimiter}', Headers: {headers}")
            
            # Process the file using backend API
            process_result = self.api.process_file_sync(
                file_path=file_path,
                load_type=load_type,
                table_name=table_name,
                target_schema="ref",
                config_reference_data=(load_type == "fullload")  # Enable backup for fullload
            )
            
            if process_result["success"]:
                self.logger.info(f"Backend processing successful for: {file_path}")
                
                # Handle reference data config insertion
                reference_config_inserted = False
                if is_reference_data:
                    try:
                        config_result = self.api.insert_reference_data_cfg_record(table_name)
                        if config_result["success"]:
                            reference_config_inserted = True
                            self.logger.info(f"Reference data config record inserted for table: {table_name}")
                        else:
                            self.logger.warning(f"Failed to insert reference data config: {config_result.get('error', 'Unknown error')}")
                    except Exception as config_error:
                        self.logger.error(f"Error inserting reference data config: {config_error}")
                
                # Move file to processed folder
                processed_folder = self.get_processed_path(is_reference_data, load_type)
                processed_file_path = os.path.join(processed_folder, os.path.basename(file_path))
                os.rename(file_path, processed_file_path)
                
                # Record successful processing
                self.record_processing(
                    processed_file_path, load_type, table_name, delimiter, headers, 
                    'completed', is_reference_data, reference_config_inserted
                )
                
                self.logger.info(f"Successfully processed and moved: {file_path}")
                
            else:
                # Backend processing failed
                error_msg = process_result.get("error", "Unknown backend error")
                raise Exception(f"Backend processing failed: {error_msg}")
            
            # Remove from tracking
            if file_path in self.file_tracking:
                del self.file_tracking[file_path]
                
        except Exception as e:
            error_msg = f"Error processing {file_path}: {e}"
            self.logger.error(error_msg)
            
            # Move file to error folder
            try:
                error_folder = self.get_error_path(is_reference_data, load_type)
                error_file_path = os.path.join(error_folder, os.path.basename(file_path))
                os.rename(file_path, error_file_path)
                
                # Record failed processing
                try:
                    delimiter, headers, _ = self.detect_csv_format(error_file_path)
                    table_name = self.extract_table_name(error_file_path)
                except:
                    delimiter, headers, table_name = ',', [], 'unknown'
                    
                self.record_processing(error_file_path, load_type, table_name, delimiter, headers, 'error', is_reference_data, False, str(e))
                
            except Exception as move_error:
                self.logger.error(f"Error moving failed file: {move_error}")
                
            # Remove from tracking
            if file_path in self.file_tracking:
                del self.file_tracking[file_path]
                
    def cleanup_tracking(self):
        """Remove tracking for files that no longer exist"""
        missing_files = []
        for file_path in self.file_tracking:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
                
        for file_path in missing_files:
            del self.file_tracking[file_path]
            self.logger.info(f"Removed tracking for missing file: {file_path}")
            
    def run(self):
        """Main monitoring loop"""
        self.logger.info("File monitor started")
        self.logger.info(f"Monitoring reference data folders:")
        self.logger.info(f"  - Reference data: {REF_DATA_BASE_PATH}")
        self.logger.info(f"  - Non-reference data: {NON_REF_DATA_BASE_PATH}")
        self.logger.info(f"Check interval: {MONITOR_INTERVAL}s, Stability checks: {STABILITY_CHECKS}")
        
        try:
            while True:
                # Scan for CSV files
                csv_files = self.scan_folders()
                
                # Check each file for stability
                for file_path, load_type, is_reference_data in csv_files:
                    if self.check_file_stability(file_path):
                        self.process_file(file_path, load_type, is_reference_data)
                        
                # Cleanup tracking for removed files
                self.cleanup_tracking()
                
                # Wait for next check
                time.sleep(MONITOR_INTERVAL)
                
        except KeyboardInterrupt:
            self.logger.info("File monitor stopped by user")
        except Exception as e:
            self.logger.error(f"File monitor error: {e}")
            raise

def main():
    monitor = FileMonitor()
    monitor.run()

if __name__ == "__main__":
    main()