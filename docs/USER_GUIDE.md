# User Guide

## Overview

The Reference Data Auto Ingest System is a web-based application that automates the process of uploading and processing CSV files into a SQL Server database. This guide provides comprehensive instructions for end users to effectively use the system for their data management needs.

## Getting Started

### Accessing the System

1. **Open your web browser** (Chrome, Firefox, Safari, or Edge recommended)
2. **Navigate to the application URL**: `http://your-server:3000` (or the URL provided by your administrator)
3. **Verify the system is running**: You should see the Reference Data Auto Ingest System interface

### System Overview

The application consists of several key components:

- **File Upload Area**: Drag-and-drop interface for CSV files
- **Configuration Panel**: Settings for CSV format and processing options
- **Progress Display**: Real-time progress tracking during file processing
- **System Logs**: Detailed information about processing activities
- **Status Indicators**: Visual feedback on system health and operations

## File Upload Process

### Step 1: Prepare Your CSV File

#### Supported File Formats
- **File Type**: CSV files only (`.csv` extension)
- **File Size**: Maximum 20MB per file
- **Encoding**: UTF-8 recommended (other encodings may work but are not guaranteed)

#### CSV Format Requirements
Your CSV file should follow these guidelines:
- **Header Row**: First row should contain column names
- **Data Consistency**: Each row should have the same number of columns
- **Text Qualification**: Text containing special characters should be quoted
- **Line Endings**: Standard line endings (Windows, Unix, or Mac formats supported)

#### Example CSV Structure
```csv
name,age,city,country
"John Doe",30,"New York","USA"
"Jane Smith",25,"London","UK"  
"Bob Johnson",35,"Paris","France"
```

### Step 2: Upload Your File

#### Using Drag and Drop
1. **Locate your CSV file** in your file explorer
2. **Drag the file** from your file explorer
3. **Drop it** onto the upload area (highlighted in blue when ready)
4. **Wait for confirmation** that the file has been selected

#### Using File Selection
1. **Click the upload area** or the "Browse Files" button
2. **Navigate** to your CSV file location
3. **Select your file** and click "Open"
4. **Confirm** the file appears in the upload interface

### Step 3: Configure Processing Options

#### CSV Format Settings

##### Basic Settings
- **Header Delimiter**: Character separating header columns (usually same as column delimiter)
- **Column Delimiter**: Character separating data columns
  - `,` (comma) - Most common
  - `;` (semicolon) - Common in European formats
  - `|` (pipe) - Used for data containing commas
  - Custom - Enter your own delimiter

- **Row Delimiter**: How rows are separated
  - `\n` (Unix/Linux line ending)
  - `\r\n` (Windows line ending)
  - `|""\r\n` (Custom format with quotes)

- **Text Qualifier**: Character used to quote text fields
  - `"` (double quote) - Standard
  - `'` (single quote) - Alternative
  - `""` (double double-quote) - For escaped quotes

##### Advanced Settings
- **Skip Lines**: Number of lines to skip after the header (default: 0)
- **Trailer Pattern**: Optional pattern to identify trailer/footer lines
- **Load Mode**: How data should be processed
  - **Full Load**: Replace all existing data (creates backup)
  - **Append Load**: Add new data to existing table

#### Auto-Detection Feature

The system can automatically detect your CSV format:

1. **Upload your file** first
2. **Click "Auto-Detect Format"** button
3. **Review the detected settings** in the configuration panel
4. **Adjust any settings** if the detection isn't perfect
5. **Proceed with processing** using detected or modified settings

**Auto-Detection Results Include:**
- Confidence score (higher is better)
- Detected delimiters and qualifiers
- Sample data preview
- Column count estimation
- Header/trailer detection

### Step 4: Start Processing

#### Begin Upload and Processing
1. **Review your settings** one final time
2. **Click "Upload and Process"** button
3. **Wait for confirmation** that upload has started
4. **Monitor progress** in the Progress Display section

#### During Processing
- **Progress Bar**: Shows overall completion percentage
- **Status Messages**: Real-time updates on current processing stage
- **Estimated Time**: Remaining time estimate (when available)
- **Cancel Option**: Stop processing if needed (click "Cancel" button)

## Monitoring Progress

### Progress Display Components

#### Progress Bar
- **Green**: Normal processing
- **Yellow**: Warning or slow processing
- **Red**: Error occurred
- **Blue**: Paused or waiting

#### Status Messages
Common status messages you'll see:
- "Starting data ingestion process..." - Initial setup
- "Connecting to database..." - Database connection establishment
- "Processing CSV data..." - File reading and parsing
- "Inserting data..." - Database insertion with row counts
- "Running validation..." - Data quality checks
- "Completing ingestion..." - Final steps
- "Ingestion completed successfully" - Finished successfully

#### Progress Details
- **Inserted Rows**: Number of rows processed so far
- **Total Rows**: Total number of rows to process
- **Processing Stage**: Current activity (reading, inserting, validating)
- **Speed**: Rows processed per second (when available)

### Handling Progress Issues

#### Slow Processing
If processing seems slow:
- **Large files** naturally take longer
- **Complex data** requires more processing time
- **Database load** may affect speed
- **Network conditions** can impact performance

#### Cancelling Operations
To cancel a running operation:
1. **Click the "Cancel" button** in the progress section
2. **Wait for confirmation** that cancellation is processing
3. **Allow cleanup** to complete (may take a few moments)
4. **Check logs** for cancellation confirmation

## Understanding Results

### Successful Processing

When processing completes successfully, you'll see:
- **Green progress bar** at 100%
- **"Completed successfully"** status message
- **Final row count** showing total processed records
- **Processing time** summary
- **Table information** showing where data was stored

### Data Storage

Your processed data is stored in:
- **Main Table**: `[schema].[filename_without_extension]`
- **Backup Table**: `[backup_schema].[filename_without_extension_backup]` (for full loads)
- **Processing Metadata**: Stored with timestamps and processing details

**Example**: If you upload `airports.csv`, the data will be stored in table `ref.airports`

### Validation Results

After data insertion, the system runs validation checks:
- **Validation Passed**: All data quality checks successful
- **Validation Issues**: Specific problems identified and reported
- **Issue Details**: Detailed information about any problems found

## Working with Different File Types

### Standard CSV Files
Most common format with comma separators:
```csv
name,age,city
John,30,New York
Jane,25,London
```
**Settings**: Column delimiter = `,`, Text qualifier = `"`

### Semicolon-Separated Files
Common in European Excel exports:
```csv
name;age;city
John;30;New York
Jane;25;London
```
**Settings**: Column delimiter = `;`, Text qualifier = `"`

### Pipe-Separated Files
Used when data contains commas:
```csv
name|age|city
"Doe, John"|30|"New York, NY"
"Smith, Jane"|25|"London, UK"
```
**Settings**: Column delimiter = `|`, Text qualifier = `"`

### Files with Trailers
Files ending with summary or count lines:
```csv
name,age,city
John,30,New York
Jane,25,London
TOTAL,2,Records
```
**Settings**: Enable trailer detection, specify pattern if known

## Troubleshooting Common Issues

### File Upload Problems

#### "File Too Large" Error
- **Problem**: File exceeds 20MB limit
- **Solution**: 
  - Split large files into smaller chunks
  - Contact administrator to increase limit if needed
  - Remove unnecessary columns or rows

#### "Invalid File Type" Error
- **Problem**: File is not a CSV file
- **Solution**:
  - Ensure file has `.csv` extension
  - Convert Excel files to CSV format
  - Verify file is actually CSV, not just renamed

#### "File Upload Failed" Error
- **Problem**: Network or server issue
- **Solution**:
  - Check internet connection
  - Try refreshing the page
  - Contact administrator if problem persists

### Processing Errors

#### "Database Connection Failed"
- **Problem**: Cannot connect to database
- **Solution**:
  - Wait a few minutes and try again
  - Contact administrator
  - Check if system maintenance is in progress

#### "Invalid Column Names"
- **Problem**: CSV headers contain invalid characters
- **Solution**:
  - Remove special characters from column headers
  - Ensure headers don't start with numbers
  - Use underscores instead of spaces

#### "Validation Failed"
- **Problem**: Data doesn't meet quality requirements
- **Solution**:
  - Review error messages in logs
  - Check data format and consistency
  - Fix identified issues in source file

#### "Processing Canceled by User"
- **Problem**: You or someone else canceled the operation
- **Solution**:
  - This is normal if you clicked Cancel
  - Restart upload if cancellation was unintentional

### Format Detection Issues

#### Low Confidence Score
- **Problem**: Auto-detection isn't sure about format
- **Solution**:
  - Manually adjust detected settings
  - Check first few rows of your CSV file
  - Ensure consistent formatting throughout file

#### Wrong Delimiter Detected
- **Problem**: System detects wrong separator
- **Solution**:
  - Manually override the detected delimiter
  - Verify your file uses consistent delimiters
  - Check for mixed delimiters in the file

## Best Practices

### File Preparation

#### Data Quality
- **Clean Headers**: Use simple, descriptive column names
- **Consistent Data**: Ensure all rows have same number of columns
- **Remove Empty Rows**: Delete blank lines at end of file
- **Check Encoding**: Save files in UTF-8 encoding when possible

#### File Organization
- **Descriptive Names**: Use meaningful filenames (e.g., `customer_data_2025.csv`)
- **Version Control**: Include dates or versions in filenames
- **Size Management**: Keep files under 20MB for optimal performance

### Processing Strategy

#### Load Modes
- **Use Full Load** when:
  - Completely replacing existing data
  - First time loading a dataset
  - Data structure has changed significantly

- **Use Append Load** when:
  - Adding new records to existing data
  - Performing incremental updates
  - Preserving historical data

#### Timing Considerations
- **Off-Peak Hours**: Process large files during low-usage times
- **Business Hours**: Avoid processing during critical business operations
- **Backup Windows**: Consider backup schedules when planning loads

### Monitoring and Validation

#### Progress Monitoring
- **Stay Connected**: Keep browser tab active during processing
- **Monitor Logs**: Check system logs for warnings or errors
- **Note Performance**: Track processing times for future planning

#### Post-Processing Verification
- **Check Row Counts**: Verify expected number of records processed
- **Sample Data**: Spot-check a few records in the database
- **Run Reports**: Execute standard reports to verify data quality

## System Information

### Browser Compatibility
- **Chrome 90+** ✅ Fully supported
- **Firefox 88+** ✅ Fully supported
- **Safari 14+** ✅ Fully supported
- **Edge 90+** ✅ Fully supported
- **Internet Explorer** ❌ Not supported

### Performance Expectations

#### Processing Speed
- **Small files** (< 1MB): 10-30 seconds
- **Medium files** (1-5MB): 30 seconds - 2 minutes
- **Large files** (5-20MB): 2-10 minutes

*Actual times depend on data complexity, system load, and database performance*

#### System Capacity
- **Concurrent Users**: Multiple users can upload simultaneously
- **File Queue**: Files are processed in background
- **Resource Management**: System automatically manages memory and connections

### Getting Help

#### Self-Service Options
1. **Check System Logs**: Review detailed error messages
2. **Try Auto-Detection**: Use automatic format detection
3. **Restart Process**: Cancel and retry if needed
4. **Review This Guide**: Check relevant troubleshooting sections

#### Contact Information
When contacting support, please provide:
- **File name and size** you're trying to process
- **Error messages** from the system logs
- **Browser type and version**
- **Steps you followed** before encountering the issue
- **Screenshot** of any error messages (if possible)

#### Administrator Assistance
Your system administrator can help with:
- Increasing file size limits
- Database connectivity issues
- Performance optimization
- Custom validation requirements
- Advanced configuration changes

## Frequently Asked Questions

### General Questions

**Q: Can I upload multiple files at once?**
A: Currently, the system processes one file at a time. Upload and process each file individually.

**Q: What happens to my data if processing fails?**
A: Failed uploads don't affect existing data. The system uses staging tables to ensure data integrity.

**Q: Can I see what's in my uploaded file before processing?**
A: Yes, the auto-detection feature shows a sample of your data for verification.

**Q: How do I know if my data was processed correctly?**
A: Check the validation results and final row counts. The system also maintains detailed logs.

### Technical Questions

**Q: Why is my file taking so long to process?**
A: Large files, complex data, or high system load can slow processing. Check the progress display for details.

**Q: Can I modify data after it's been uploaded?**
A: The system doesn't provide data editing. You'll need to fix your CSV file and re-upload if changes are needed.

**Q: What's the difference between full and append mode?**
A: Full mode replaces all existing data (with backup), while append mode adds new records to existing data.

**Q: Why was my file format detected incorrectly?**
A: Complex or inconsistent CSV formats can confuse detection. Manually adjust settings after auto-detection.

### Data Questions

**Q: What data types are supported?**
A: The system stores all data as text (varchar) for maximum compatibility and flexibility.

**Q: Can I upload files with formulas or macros?**
A: No, only plain CSV text files are supported. Convert Excel files to CSV format first.

**Q: What happens to empty cells in my CSV?**
A: Empty cells are stored as empty strings in the database.

This user guide provides comprehensive information for successfully using the Reference Data Auto Ingest System. Follow these guidelines to ensure smooth and successful data processing operations.