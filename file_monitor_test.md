# File Monitor Test Cases

This document outlines comprehensive test cases for the file monitoring system that processes CSV files in the reference data management system.

## Test Environment Setup

**Test Directories:**
- Reference data: `/home/lin/repo/reference_data_mgr/data/reference_data/dropoff/reference_data_table/`
- Non-reference data: `/home/lin/repo/reference_data_mgr/data/reference_data/dropoff/none_reference_data_table/`
- Each contains `fullload/` and `append/` subfolders

**Expected Behavior:**
- Files monitored every 15 seconds
- File stability: 6 consecutive checks without size change (90 seconds)
- Reference data files: Insert config record in Reference_Data_Cfg table
- Non-reference data files: Skip config record insertion
- Successful files: Move to `processed/` folder
- Failed files: Move to `error/` folder

---

## Basic File Detection Tests

### Test 1: Single CSV File Drop
**Objective:** Verify monitor detects single CSV file  
**Steps:** Drop one CSV file in `reference_data_table/fullload/`  
**Expected:** File detected, tracked, and processed after stability period  
**Status:** [ ]

### Test 2: Multiple CSV Files Drop
**Objective:** Verify monitor handles multiple files simultaneously  
**Steps:** Drop 3-5 CSV files at once in different folders  
**Expected:** All files detected and processed independently  
**Status:** [ ]

### Test 3: Non-CSV File Ignore
**Objective:** Verify non-CSV files are ignored  
**Steps:** Drop `.txt`, `.xlsx`, `.json` files in monitored folders  
**Expected:** Files ignored, no processing attempted  
**Status:** [ ]

### Test 4: Empty File Handling
**Objective:** Verify handling of 0-byte CSV files  
**Steps:** Create and drop empty CSV file  
**Expected:** File detected but processing fails gracefully, moved to error folder  
**Status:** [ ]

### Test 5: Large File Handling
**Objective:** Verify processing of large CSV files (>100MB)  
**Steps:** Create and drop large CSV file  
**Expected:** File processed successfully with appropriate progress logging  
**Status:** [ ]

---

## File Stability Tests

### Test 6: Incomplete File Upload
**Objective:** Verify monitor waits for file upload completion  
**Steps:** Use `dd` or slow copy to simulate gradual file writing  
**Expected:** File not processed until size stabilizes for 90 seconds  
**Status:** [ ]

### Test 7: File Stability Detection
**Objective:** Verify 6-check stability requirement  
**Steps:** Drop file, monitor tracking database for stability status  
**Expected:** File marked stable only after 6 consecutive unchanged checks  
**Status:** [ ]

### Test 8: File Modification During Monitoring
**Objective:** Verify handling of file changes during monitoring  
**Steps:** Drop file, modify it before stability period ends  
**Expected:** Stability counter resets, file processed after new stability period  
**Status:** [ ]

### Test 9: File Deletion During Monitoring
**Objective:** Verify handling of file deletion during monitoring  
**Steps:** Drop file, delete it before processing  
**Expected:** File removed from tracking, no processing attempted  
**Status:** [ ]

### Test 10: Simultaneous File Operations
**Objective:** Verify handling of multiple files with different stability states  
**Steps:** Drop multiple files at different times  
**Expected:** Each file processed independently based on its stability  
**Status:** [ ]

---

## Folder Structure Tests

### Test 11: Reference Data Fullload
**Objective:** Verify processing of files in reference_data_table/fullload/  
**Steps:** Drop CSV file in reference_data_table/fullload/  
**Expected:** File processed with fullload mode, config record inserted  
**Status:** [ ]

### Test 12: Reference Data Append
**Objective:** Verify processing of files in reference_data_table/append/  
**Steps:** Drop CSV file in reference_data_table/append/  
**Expected:** File processed with append mode, config record inserted  
**Status:** [ ]

### Test 13: Non-Reference Data Fullload
**Objective:** Verify processing of files in none_reference_data_table/fullload/  
**Steps:** Drop CSV file in none_reference_data_table/fullload/  
**Expected:** File processed with fullload mode, no config record inserted  
**Status:** [ ]

### Test 14: Non-Reference Data Append
**Objective:** Verify processing of files in none_reference_data_table/append/  
**Steps:** Drop CSV file in none_reference_data_table/append/  
**Expected:** File processed with append mode, no config record inserted  
**Status:** [ ]

### Test 15: Wrong Folder Placement
**Objective:** Verify files in wrong locations are ignored  
**Steps:** Drop CSV files directly in dropoff/ or other incorrect locations  
**Expected:** Files ignored by monitor  
**Status:** [ ]

---

## CSV Format Detection Tests

### Test 16: Comma-Delimited CSV
**Objective:** Verify detection of standard comma-delimited CSV  
**Steps:** Drop CSV file with comma delimiters  
**Expected:** Delimiter detected as ',', proper parsing  
**Status:** [ ]

### Test 17: Semicolon-Delimited CSV
**Objective:** Verify detection of semicolon-delimited CSV  
**Steps:** Drop CSV file with semicolon delimiters  
**Expected:** Delimiter detected as ';', proper parsing  
**Status:** [ ]

### Test 18: Pipe-Delimited CSV
**Objective:** Verify detection of pipe-delimited CSV  
**Steps:** Drop CSV file with pipe delimiters  
**Expected:** Delimiter detected as '|', proper parsing  
**Status:** [ ]

### Test 19: Tab-Delimited CSV
**Objective:** Verify detection of tab-delimited CSV  
**Steps:** Drop CSV file with tab delimiters  
**Expected:** Delimiter detected as '\t', proper parsing  
**Status:** [ ]

### Test 20: CSV With Headers
**Objective:** Verify detection of CSV files with header row  
**Steps:** Drop CSV file with proper column headers  
**Expected:** Headers detected and used for column mapping  
**Status:** [ ]

### Test 21: CSV Without Headers
**Objective:** Verify handling of CSV files without header row  
**Steps:** Drop CSV file without headers (data only)  
**Expected:** Default column names generated or processing handles gracefully  
**Status:** [ ]

### Test 22: Malformed CSV
**Objective:** Verify handling of invalid CSV structure  
**Steps:** Drop file with inconsistent column counts, embedded quotes  
**Expected:** Error detected, file moved to error folder with appropriate logging  
**Status:** [ ]

### Test 23: Mixed Delimiter CSV
**Objective:** Verify handling of CSV with inconsistent delimiters  
**Steps:** Drop CSV file mixing commas and semicolons  
**Expected:** Error detected or best-effort parsing with warning  
**Status:** [ ]

---

## Table Name Extraction Tests

### Test 24: Standard Table Name
**Objective:** Verify extraction from simple filename  
**Steps:** Drop file named `airports.csv`  
**Expected:** Table name extracted as `airports`  
**Status:** [ ]

### Test 25: Underscored Table Name
**Objective:** Verify extraction from underscored filename  
**Steps:** Drop file named `airport_frequencies.csv`  
**Expected:** Table name extracted as `airport_frequencies`  
**Status:** [ ]

### Test 26: Hyphenated Table Name
**Objective:** Verify extraction from hyphenated filename  
**Steps:** Drop file named `airport-frequencies.csv`  
**Expected:** Table name extracted as `airport_frequencies` (hyphens converted)  
**Status:** [ ]

### Test 27: Complex Filename
**Objective:** Verify extraction from complex filename  
**Steps:** Drop file named `test_data_2024.csv`  
**Expected:** Table name extracted as `test_data`  
**Status:** [ ]

### Test 28: Uppercase Filename
**Objective:** Verify extraction from uppercase filename  
**Steps:** Drop file named `AIRPORTS.CSV`  
**Expected:** Table name extracted as `airports` (normalized to lowercase)  
**Status:** [ ]

---

## Reference Data Config Tests

### Test 29: Reference Data Config Insertion
**Objective:** Verify config record creation for reference data files  
**Steps:** Drop CSV in reference_data_table folder, check Reference_Data_Cfg table  
**Expected:** Config record inserted with correct table name  
**Status:** [ ]

### Test 30: Non-Reference Data Skip Config
**Objective:** Verify no config record for non-reference data files  
**Steps:** Drop CSV in none_reference_data_table folder, check Reference_Data_Cfg table  
**Expected:** No config record inserted  
**Status:** [ ]

### Test 31: Duplicate Config Handling
**Objective:** Verify handling of existing config records  
**Steps:** Process file that already has config record  
**Expected:** Existing record preserved, no duplicate created  
**Status:** [ ]

---

## Data Processing Tests

### Test 32: Successful Data Load
**Objective:** Verify complete end-to-end processing  
**Steps:** Drop valid CSV file with known data  
**Expected:** Data successfully loaded to database, file moved to processed folder  
**Status:** [ ]

### Test 33: Database Connection Failure
**Objective:** Verify handling of database unavailability  
**Steps:** Stop database service, drop CSV file  
**Expected:** Connection error handled, file moved to error folder  
**Status:** [ ]

### Test 34: Data Validation Errors
**Objective:** Verify handling of invalid data types/constraints  
**Steps:** Drop CSV with data that violates database constraints  
**Expected:** Validation error caught, file moved to error folder  
**Status:** [ ]

### Test 35: Large Dataset Processing
**Objective:** Verify processing of large datasets  
**Steps:** Drop CSV file with 100K+ rows  
**Expected:** File processed successfully with progress logging  
**Status:** [ ]

### Test 36: Empty CSV Processing
**Objective:** Verify handling of CSV with headers but no data  
**Steps:** Drop CSV file with only header row  
**Expected:** Processing completes successfully or fails gracefully  
**Status:** [ ]

---

## Error Handling Tests

### Test 37: Corrupted CSV File
**Objective:** Verify handling of corrupted files  
**Steps:** Drop file with binary content or corruption  
**Expected:** Corruption detected, file moved to error folder  
**Status:** [ ]

### Test 38: Permission Denied
**Objective:** Verify handling of files with restricted permissions  
**Steps:** Drop CSV file with read-only or no-access permissions  
**Expected:** Permission error handled gracefully  
**Status:** [ ]

### Test 39: Disk Space Full
**Objective:** Verify handling of insufficient disk space  
**Steps:** Fill disk space, attempt processing  
**Expected:** Disk space error handled, processing stopped safely  
**Status:** [ ]

### Test 40: Database Schema Mismatch
**Objective:** Verify handling when target table doesn't exist  
**Steps:** Drop CSV for non-existent table  
**Expected:** Schema error detected, file moved to error folder  
**Status:** [ ]

### Test 41: Network Interruption
**Objective:** Verify handling of network/DB connection drops  
**Steps:** Interrupt network during processing  
**Expected:** Connection error handled, transaction rolled back  
**Status:** [ ]

---

## File Lifecycle Tests

### Test 42: Successful Processing Flow
**Objective:** Verify successful file lifecycle  
**Steps:** Drop valid CSV file, monitor complete flow  
**Expected:** File moved to processed/ folder with timestamp  
**Status:** [ ]

### Test 43: Error Processing Flow
**Objective:** Verify error file lifecycle  
**Steps:** Drop invalid CSV file, monitor error flow  
**Expected:** File moved to error/ folder with error details  
**Status:** [ ]

### Test 44: Processed Folder Cleanup
**Objective:** Verify processed files are archived properly  
**Steps:** Check processed folder structure and file organization  
**Expected:** Files organized by date/time, proper archival  
**Status:** [ ]

### Test 45: Error Folder Analysis
**Objective:** Verify error files contain sufficient debugging info  
**Steps:** Examine error files and associated logs  
**Expected:** Clear error messages, debugging information available  
**Status:** [ ]

---

## Monitoring System Tests

### Test 46: Monitor Startup/Shutdown
**Objective:** Verify clean monitor lifecycle  
**Steps:** Start and stop monitor multiple times  
**Expected:** Clean startup/shutdown, no resource leaks  
**Status:** [ ]

### Test 47: Log File Creation
**Objective:** Verify proper logging functionality  
**Steps:** Run monitor, check log files  
**Expected:** Comprehensive logging with appropriate levels  
**Status:** [ ]

### Test 48: SQLite Tracking Database
**Objective:** Verify tracking database functionality  
**Steps:** Check tracking records during processing  
**Expected:** Accurate tracking of all file states and timestamps  
**Status:** [ ]

### Test 49: Concurrent File Processing
**Objective:** Verify handling of multiple files simultaneously  
**Steps:** Drop multiple files requiring different processing times  
**Expected:** Files processed concurrently without interference  
**Status:** [ ]

### Test 50: Long-Running Stability
**Objective:** Verify monitor stability over extended periods  
**Steps:** Run monitor for 24+ hours with periodic file drops  
**Expected:** Monitor remains stable, no memory leaks or crashes  
**Status:** [ ]

---

## Edge Cases

### Test 51: Special Characters in Filename
**Objective:** Verify handling of filenames with special characters  
**Steps:** Drop files with spaces, unicode, special characters in names  
**Expected:** Files processed correctly, proper escaping/handling  
**Status:** [ ]

### Test 52: Very Long Filename
**Objective:** Verify handling of extremely long filenames  
**Steps:** Drop file with filename approaching filesystem limits  
**Expected:** Filename handled correctly or appropriate error  
**Status:** [ ]

### Test 53: Nested Folder Creation
**Objective:** Verify handling of unexpected folder structures  
**Steps:** Create subfolders within monitored directories  
**Expected:** Subfolders ignored, no interference with monitoring  
**Status:** [ ]

### Test 54: Symbolic Link Handling
**Objective:** Verify handling of symbolic links to CSV files  
**Steps:** Create symlinks to CSV files in monitored folders  
**Expected:** Symlinks handled appropriately (processed or ignored)  
**Status:** [ ]

### Test 55: File Rename During Processing
**Objective:** Verify handling of file renames during monitoring  
**Steps:** Rename file while it's being monitored  
**Expected:** Original tracking maintained or graceful error handling  
**Status:** [ ]

---

## Integration Tests

### Test 56: Backend API Integration
**Objective:** Verify all backend API calls work correctly  
**Steps:** Test format detection, data ingestion, health checks  
**Expected:** All API calls successful, proper integration  
**Status:** [ ]

### Test 57: Database Transaction Handling
**Objective:** Verify proper commit/rollback behavior  
**Steps:** Simulate failures during different transaction phases  
**Expected:** Proper transaction management, no data corruption  
**Status:** [ ]

### Test 58: Progress Tracking
**Objective:** Verify processing progress is tracked accurately  
**Steps:** Monitor large file processing progress  
**Expected:** Accurate progress reporting and logging  
**Status:** [ ]

### Test 59: Audit Trail Verification
**Objective:** Verify complete processing history is maintained  
**Steps:** Process multiple files, examine audit records  
**Expected:** Complete audit trail with timestamps and status  
**Status:** [ ]

### Test 60: Performance Benchmarks
**Objective:** Verify processing speed meets requirements  
**Steps:** Process various file sizes, measure processing times  
**Expected:** Processing speed within acceptable limits  
**Status:** [ ]

---

## Test Execution Guidelines

### For Each Test Case:
- [ ] Record start time and test conditions
- [ ] Document actual vs expected results
- [ ] Note any error messages or logs
- [ ] Verify database state changes
- [ ] Check file movements (processed/error folders)
- [ ] Update test status (Pass/Fail/Pending)

### Verification Points:
- **File Detection:** Monitor logs show file discovery
- **Stability Waiting:** 90-second stability period observed
- **Format Detection:** Correct delimiter and headers identified
- **Table Name Extraction:** Proper table name derived from filename
- **Data Processing:** Successful data load or appropriate error handling
- **File Movement:** Correct placement in processed/error folders
- **Config Records:** Reference data config inserted when appropriate
- **Logging:** Comprehensive logs with sufficient detail
- **Database Tracking:** Accurate records in SQLite tracking database

### Test Environment:
- Monitor running with 15-second intervals
- Database connection available
- Sufficient disk space for file operations
- Proper permissions on all directories
- Clean state before each test (remove previous test files)

### Success Criteria:
All test cases must pass for the file monitor to be considered production-ready. Any failing tests must be investigated and resolved before deployment.