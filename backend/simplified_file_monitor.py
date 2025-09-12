#!/usr/bin/env python3
"""
Simplified File Monitor for Reference Data Management
Monitors single dropoff folder for CSV files and triggers Excel workflow instead of immediate processing.
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

# Import the backend library and new utilities
from backend_lib import ReferenceDataAPI
from utils.database import DatabaseManager
from utils.workflow_manager import WorkflowManager
from utils.excel_generator import ExcelFormGenerator
from utils.csv_detector import CSVFormatDetector

# Configuration
SIMPLIFIED_DROPOFF_PATH = "/home/lin/repo/reference_data_mgr/data/reference_data/dropoff"

# Monitor settings (maintain existing performance characteristics)
MONITOR_INTERVAL = 15  # seconds - same as original
STABILITY_CHECKS = 6   # number of consecutive checks without size change
LOG_FILE = "/home/lin/repo/reference_data_mgr/logs/simplified_file_monitor.log"

class SimplifiedFileMonitor:
    """Monitors single dropoff directory and triggers Excel workflow"""
    
    def __init__(self):
        self.setup_logging()
        self.setup_directories()
        
        # Initialize components
        self.db_manager = DatabaseManager()
        self.workflow_manager = WorkflowManager(self.db_manager, self.logger)
        self.excel_generator = ExcelFormGenerator(self.logger)
        self.csv_detector = CSVFormatDetector()
        
        # File tracking for stability checks
        self.file_tracking = {}  # {file_path: {'size': int, 'mtime': float, 'stable_count': int}}
        
        # Initialize backend API for format detection
        try:
            self.api = ReferenceDataAPI()
            self.logger.info("Backend API initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize backend API: {str(e)}")
            raise
    
    def setup_logging(self):
        """Set up logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = Path(LOG_FILE).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure logging (check for debug environment variable)
        log_level = logging.DEBUG if os.getenv('DEBUG', '').lower() in ['true', '1', 'yes'] else logging.INFO
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Simplified file monitor logging initialized")
    
    def setup_directories(self):
        """Ensure the simplified dropoff directory exists"""
        try:
            dropoff_path = Path(SIMPLIFIED_DROPOFF_PATH)
            dropoff_path.mkdir(parents=True, exist_ok=True)
            
            # Create processed and error subdirectories for file management
            (dropoff_path / "processed").mkdir(exist_ok=True)
            (dropoff_path / "error").mkdir(exist_ok=True)
            
            self.logger.info(f"Simplified dropoff directory ensured: {SIMPLIFIED_DROPOFF_PATH}")
            
        except Exception as e:
            self.logger.error(f"Failed to setup directories: {str(e)}")
            raise
    
    def run(self):
        """Main monitoring loop - simplified version of original FileMonitor"""
        self.logger.info("Starting simplified file monitor...")
        
        try:
            while True:
                try:
                    # Scan for new files
                    self.scan_simplified_directory()
                    
                    # Update file stability tracking
                    self.update_file_stability()
                    
                    # Clean up old tracking entries
                    self.cleanup_tracking()
                    
                    # Sleep for monitoring interval
                    time.sleep(MONITOR_INTERVAL)
                    
                except KeyboardInterrupt:
                    self.logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    self.logger.error(f"Error in monitoring loop: {str(e)}")
                    time.sleep(MONITOR_INTERVAL)  # Continue monitoring despite errors
                    
        finally:
            self.logger.info("Simplified file monitor stopped")
    
    def scan_simplified_directory(self):
        """Scan the simplified dropoff directory for CSV files"""
        try:
            dropoff_path = Path(SIMPLIFIED_DROPOFF_PATH)
            
            # Look for CSV files in the main dropoff directory
            csv_files = list(dropoff_path.glob("*.csv"))
            
            for csv_file in csv_files:
                self.handle_detected_file(str(csv_file))
                
        except Exception as e:
            self.logger.error(f"Error scanning simplified directory: {str(e)}")
    
    def handle_detected_file(self, file_path: str):
        """Handle a detected CSV file"""
        try:
            # Skip if file is already being tracked and processed
            if self.is_file_being_processed(file_path):
                self.logger.debug(f"File {file_path} already being processed, skipping")
                return
            
            # Check file stability
            if not self.is_file_stable(file_path):
                self.logger.debug(f"File {file_path} not yet stable, waiting...")
                return
            
            # File is stable and ready for processing
            self.logger.info(f"Stable file detected: {file_path}")
            
            # Create new workflow for this file
            workflow_id = self.handle_new_file(file_path)
            
            if workflow_id:
                self.logger.info(f"Created workflow {workflow_id} for file: {file_path}")
            else:
                self.logger.error(f"Failed to create workflow for file: {file_path}")
                
        except Exception as e:
            self.logger.error(f"Error handling detected file {file_path}: {str(e)}")
    
    def handle_new_file(self, csv_path: str) -> str:
        """
        Create Excel workflow for new CSV file
        
        Args:
            csv_path: Path to the detected CSV file
            
        Returns:
            Workflow ID if successful, None if failed
        """
        try:
            # Create workflow in database
            workflow_id = self.workflow_manager.create_workflow(csv_path)
            
            # Detect CSV format
            self.logger.info(f"Detecting format for: {csv_path}")
            format_data = self.csv_detector.detect_format(csv_path)
            
            # Add file size information
            file_size = os.path.getsize(csv_path) / (1024 * 1024)  # MB
            format_data['file_size_mb'] = round(file_size, 2)
            
            # Generate Excel form
            self.logger.info(f"Generating Excel form for workflow: {workflow_id}")
            excel_path = self.excel_generator.generate_form(csv_path, format_data)
            
            # Update workflow with Excel path
            self.workflow_manager.update_status(
                workflow_id,
                self.workflow_manager.STATES['EXCEL_GENERATED'],
                excel_path=excel_path,
                original_csv_size_mb=file_size
            )
            
            self.logger.info(f"Excel form generated successfully: {excel_path}")
            return workflow_id
            
        except Exception as e:
            self.logger.error(f"Failed to handle new file {csv_path}: {str(e)}")
            
            # Update workflow to error state if workflow was created
            if 'workflow_id' in locals():
                self.workflow_manager.update_status(
                    workflow_id,
                    self.workflow_manager.STATES['ERROR'],
                    error_message=str(e)
                )
            return None
    
    def is_file_being_processed(self, file_path: str) -> bool:
        """Check if file is already in a workflow"""
        try:
            # Query database for existing workflow with this file
            query_sql = """
            SELECT workflow_id FROM ref.Excel_Workflow_Tracking
            WHERE csv_file_path = ? AND status NOT IN ('completed', 'error')
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query_sql, (file_path,))
                result = cursor.fetchone()
            
            return result is not None
            
        except Exception as e:
            self.logger.error(f"Error checking file processing status: {str(e)}")
            return False  # Assume not being processed if we can't check
    
    def is_file_stable(self, file_path: str) -> bool:
        """
        Check if file is stable (same size and mtime for STABILITY_CHECKS consecutive checks)
        Reuses logic from original FileMonitor
        """
        try:
            stat = os.stat(file_path)
            current_size = stat.st_size
            current_mtime = stat.st_mtime
            
            if file_path not in self.file_tracking:
                # First time seeing this file
                self.file_tracking[file_path] = {
                    'size': current_size,
                    'mtime': current_mtime,
                    'stable_count': 1,
                    'last_check': datetime.now()
                }
                return False
            
            tracking_info = self.file_tracking[file_path]
            
            # Check if file has changed
            if (current_size != tracking_info['size'] or 
                current_mtime != tracking_info['mtime']):
                # File changed, reset stability counter
                tracking_info['size'] = current_size
                tracking_info['mtime'] = current_mtime
                tracking_info['stable_count'] = 1
                tracking_info['last_check'] = datetime.now()
                return False
            
            # File hasn't changed, increment stability counter
            tracking_info['stable_count'] += 1
            tracking_info['last_check'] = datetime.now()
            
            # File is stable if it hasn't changed for STABILITY_CHECKS
            return tracking_info['stable_count'] >= STABILITY_CHECKS
            
        except Exception as e:
            self.logger.error(f"Error checking file stability for {file_path}: {str(e)}")
            return False
    
    def update_file_stability(self):
        """Update stability tracking for all monitored files"""
        try:
            # This is called each monitoring cycle to maintain tracking state
            # The actual stability checking is done in is_file_stable()
            current_time = datetime.now()
            
            # Log stability status periodically
            if hasattr(self, '_last_stability_log'):
                if (current_time - self._last_stability_log).seconds > 300:  # Every 5 minutes
                    self._log_stability_status()
                    self._last_stability_log = current_time
            else:
                self._last_stability_log = current_time
                
        except Exception as e:
            self.logger.error(f"Error updating file stability: {str(e)}")
    
    def _log_stability_status(self):
        """Log current file stability status"""
        if self.file_tracking:
            self.logger.info(f"Currently tracking {len(self.file_tracking)} files for stability")
            for file_path, info in self.file_tracking.items():
                self.logger.debug(f"File {file_path}: stable_count={info['stable_count']}")
    
    def cleanup_tracking(self):
        """Remove old tracking entries for files that no longer exist or are processed"""
        try:
            current_time = datetime.now()
            files_to_remove = []
            
            for file_path, info in self.file_tracking.items():
                # Remove if file no longer exists
                if not os.path.exists(file_path):
                    files_to_remove.append(file_path)
                    continue
                
                # Remove if file is being processed (has workflow)
                if self.is_file_being_processed(file_path):
                    files_to_remove.append(file_path)
                    continue
                
                # Remove if tracking is very old (more than 1 hour)
                if (current_time - info['last_check']).seconds > 3600:
                    files_to_remove.append(file_path)
            
            # Remove identified files from tracking
            for file_path in files_to_remove:
                del self.file_tracking[file_path]
                self.logger.debug(f"Removed {file_path} from stability tracking")
                
        except Exception as e:
            self.logger.error(f"Error during tracking cleanup: {str(e)}")


def main():
    """Main entry point for the simplified file monitor"""
    try:
        monitor = SimplifiedFileMonitor()
        monitor.run()
    except KeyboardInterrupt:
        print("\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()