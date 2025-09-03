"""
Comprehensive test coverage combining all working test modules
"""

# Import all the working test classes from existing test files
import sys
import os
sys.path.append(os.path.dirname(__file__))

# Import from our working test files
from test_backend_lib_comprehensive import TestBackendLibComprehensive
from test_csv_detector_final import TestCSVDetectorFinalCoverage
from test_file_handler_working import TestFileHandlerWorking
from test_database_simple import TestDatabaseBasics
from test_file_monitor_complete import TestFileMonitorComplete, TestFileMonitorProcessing, TestFileMonitorMainLoop
from test_ingest_comprehensive import TestDataIngesterCSVReading, TestDataIngesterMainIngestion, TestDataIngesterDatabaseLoading
from test_logger_comprehensive import TestLoggerBasicLogging, TestLoggerWriteLog, TestLoggerReading, TestLoggerMaintenance, TestDatabaseLogger

# Re-export all test classes so pytest can discover them
__all__ = [
    'TestBackendLibComprehensive',
    'TestCSVDetectorFinalCoverage', 
    'TestFileHandlerWorking',
    'TestDatabaseBasics',
    'TestFileMonitorComplete',
    'TestFileMonitorProcessing', 
    'TestFileMonitorMainLoop',
    'TestDataIngesterCSVReading',
    'TestDataIngesterMainIngestion',
    'TestDataIngesterDatabaseLoading',
    'TestLoggerBasicLogging',
    'TestLoggerWriteLog',
    'TestLoggerReading',
    'TestLoggerMaintenance',
    'TestDatabaseLogger'
]