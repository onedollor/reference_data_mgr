# Product Requirements Document (PRD)

## 1. Overview
The Reference Data Auto Ingest System is designed to automate the ingestion of reference data from CSV files into a SQL Server database. The system supports both local file ingestion and web-based file uploads, ensuring flexibility and ease of use. It adheres to the CSV format specifications outlined in [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt).

---

## 2. Functional Requirements

### 2.1 Input File Format
- **Supported Format**: CSV
- **Configurable Parameters**:
  - Header delimiter
  - Column delimiter
  - Row delimiter
  - Text qualifier
  - Skip lines after the header
  - Trailer support with configurable trailer format pattern
  - auto guess csv format for header, header delimiter ,column delimiter, 
  row delimiter , has trailer or not
- **Reference**: [RFC 4180](https://www.ietf.org/rfc/rfc4180.txt)

### 2.2 Input Channels
- **Local Folder**: Files can be ingested from a designated folder on a Windows system.
- **Web Interface**: Users can upload files via a web interface.

### 2.3 Backend Database
- **Database**: SQL Server (only supported backend).

---

## 3. Features

### 3.1 Web Interface Upload Tracking
- Track user ID and timestamp for every file uploaded via the web interface.

### 3.2 Table and Stage Table Management
- **New Table and Stage Table Creation**:
  - Both the main table and a stage table (with `_stage` suffix) are created with the same schema.
  - Column names are derived from the CSV header.
  - All columns are assigned the `varchar(4000)` data type.
  - Table name is derived from the CSV file name, with the schema defaulting to `ref`:
    - Patterns supported:
      - `abcdef.20250801000000.csv`
      - `abcdef.20250801.csv`
      - `abcdef.csv` (defaults to the current datetime).
  - Automatically create both the main table and stage table if they do not exist, using the schema from the CSV file.
  - when I upload abc.yyyyMMdd.csv or 
  abc_yyyyMMdd.csv or abc.yyyyMMddHHmmss.csv or abc.yyyyMMdd.HHmmss.csv 
  or abc_yyyyMMdd_HHmmss.csv or abc_yyyyMMddHHmmss.csv all just target to
   table abc

- **Existing Table Validation**:
  - Validate the schema of the existing table and stage table against the CSV file.
  - Allow users to decide whether to recreate the table and stage table with the new schema if there is a mismatch.

### 3.3 Data Load Modes
- **Full Load**:
  - Backup the existing table into the `bkp` schema with the format `tablename_yyyyMMddHHmmss` before ingestion.
  - Truncate the table before loading new data.
- **Append Load**:
  - Add a `dataset_id` column with an auto-incrementing sequence number.
  - Append new data with a unique `dataset_id`.
  - If the schema does not align with the CSV file, prompt the user to reinitialize both the table and stage table.
  - Backup the current table into the `bkp` schema with the format `tablename_yyyyMMddHHmmss` before reinitialization.

### 3.4 Data Validation
- **Validation Stored Procedure**:
  - Automatically create an empty stored procedure for validation with the name `sp_ref_validate_{tablename}` in the `ref` schema.
  - The stored procedure returns a JSON string with the following structure:
    ```json
    {
        "validation_result": 2,
        "validation_issue_list": [
            {
                "issue_id": 1,
                "issue_detail": "detail issue msg"
            },
            {
                "issue_id": 2,
                "issue_detail": "detail issue msg"
            }
        ]
    }
    ```
  - `validation_result` indicates the number of issues found (0 means success).

- **Validation Workflow**:
  - Always load data into the stage table (`tablename_stage`) for validation.
  - Only move data into the target table after validation succeeds.
  - Leave data in the stage table if validation fails.

### 3.5 Logging
- **Unified Logging**:
  - Design a single log table to capture all activities and results, including:
    - Error messages
    - Row counts
    - File name
    - Table name
    - Action step name
    - Timestamp
    - Other relevant details

---

## 4. Non-Functional Requirements

### 4.1 Limitations
- Input file size must not exceed **100MB**.
- Only reference data is allowed; transactional data is not supported.
- CSV files must always include a header row.

### 4.2 Performance
- The system should handle concurrent uploads efficiently.
- Validation and ingestion processes should complete within a reasonable time for files up to the size limit.

### 4.3 Security
- Ensure secure file uploads via HTTPS.
- Implement user authentication and role-based access control for the web interface.

### 4.4 Auditability
- Maintain detailed logs for all file uploads, schema changes, data validation, and ingestion activities.

### 4.5 Error msg
- import traceback and if catch any exception include traceback.format_exc() in error msg and in log

---

## 5. Additional Considerations
- **Error Handling**:
  - Provide detailed error messages for file format issues, schema mismatches, and database errors.
  - Allow users to download error logs for failed uploads.
- **Configuration Management**:
  - Allow administrators to configure input parameters (e.g., delimiters, text qualifiers, trailer format) via a settings interface.
- **Backup Retention**:
  - Define a retention policy for backup tables in the `bkp` schema to manage storage usage.

## 6. .env
### Database configuration
db_host=localhost
db_name=test
db_user=tester
db_password=121@abc!

### Database schema configuration
data_schema=ref
backup_schema=bkp
validation_sp_schema=ref

### File locations
data_drop_location=C:\data\reference_data
temp_location=C:\data\reference_data\temp
archive_location=C:\data\reference_data\archive
format_location=C:\data\reference_data\format

### max size of uploaded files (in bytes)
max_upload_size=20971520
---

# Application Build Plan
1. **Backend Development (Python):**
  - 1.1 Use FastAPI for defining APIs.
  - 1.2 Implement endpoints for:
    - 1.2.1 File upload via web interface.
    - 1.2.2 Triggering data ingestion from the local folder.
    - 1.2.3 Managing configurations (e.g., delimiters, text qualifiers).
    - 1.2.4 Logging and validation.
  - 1.3 Use pyodbc or SQLAlchemy for connecting to the SQL Server database.
  - 1.4 Implement logic for:
    - 1.4.1 Creating and validating tables (main and stage).
    - 1.4.2 Data ingestion (Full Load and Append Load).
    - 1.4.3 Data validation using stored procedures.
    - 1.4.4 Logging activities.

2. **Frontend Development (React):**
  - 2.1 Create a React app for the web interface.
  - 2.2 Implement features for:
    - 2.2.1 File upload.
    - 2.2.2 Viewing logs and validation results.
    - 2.2.3 Managing configurations.

3. **Database Setup:**
  - 3.1 Define SQL scripts for:
    - 3.1.1 Creating the unified logging table.
    - 3.1.2 Creating stored procedures for validation.

4. **File Management:**
  - 4.1 Use Python to handle file operations:
    - 4.1.1 Monitor the local folder for new files.
    - 4.1.2 Move files to the archive after processing.
    - 4.1.3 Use the temp folder for intermediate operations if needed.
5. **Configuration:**
  - 5.1 Store configurable parameters (e.g., delimiters, text qualifiers) in a JSON or database table.
6. **Deployment:**
  - 6.1 Package the backend and frontend for deployment.
  - 6.2 Provide instructions for running the app locally or on a server.

---

# Application Build Tool and lib repos
  1. npm set registry https://rp.td.com/repository/3rd-party-npm-central/
  2. npm get registry

# Application Build Steps
  1. Set up the project structure in the current folder.
  2. npm install react-scripts
  3. Create the backend and frontend scaffolding.
  4. Set up the database connection and implement basic API endpoints in the backend
  5. Create requirements file and add required packages.
  6. Install dependencies
  7. integrate frontend functions with the backend
  8. frontend add index.html in public and ensure the React app renders properly
    - 8.1 frontend "Header Delimiter" input allow users to either select from a predefined list [,;|] or input a custom value if "Custom" is selected, default to |
    - 8.2 frontend "Column Delimiter" input allow users to either select from a predefined list [,;|] or input a custom value if "Custom" is selected, default to |
    - 8.3 frontend "Row Delimiter" input allow users to either select from a predefined list ["\r", "\n", "\r\n", '|""\r\n'] or input a custom value if "Custom" is selected, default to '|""\r\n'
    - 8.4 frontend "Text Qualifier" input allow users to either select from a predefined list ['"', "'", '""'] or input a custom value if "Custom" is selected, default to "
    - 8.5 frontend if progressMessage from backend start with "ERROR!" or inlcuded "ERROR!" make it red and bold in index page, setFormattedMessage function split msg into lines each line put in a span and only highlight the line with "ERROR!". setFormattedMessage lines split each line if it's over 80 chars add a <br/> as line change

  9. complete backend code to prod deployment ready
  10. create .env set values based on information from PRD file
  11. backend implement the get_db_connection function and test it
  12. backend implement the upload_file function and test it
    - 12.1 file upload into temp folder.
    - 12.2 create a .fmt file in format folder with the same name as the uploaded CSV file but with a .fmt extension. This file will include the format parameters provided during the upload. Let me update the backend code.   
    - 12.3 validate file size reject if over max_upload_size 
  13. backend implement the ingest_data function
    - 13.1 connet to database and create a cursor set auto commit true
    - 13.2 initial table_base_name how to extract table_base_name reference => 3.2 Table and Stage Table Management. and based on table_base_name set those variables [table_name, stage_table_name, backup_table_name, validation_proc_name]
    - 13.3 read csv file format info from .fmt file
    - 13.4 read csv header
    - 13.5 Sanitize headers using regex to include only valid characters (a-z, A-Z, 0-9, _, -)
    - 13.6 Drop any column with an empty name after sanitization
    - 13.7 build a list of columns based on Sanitize headers with default data type varchar(4000) and add column ref_data_loadtime datatype datetime
    - 13.8 create target table, stage_table, backup_table and validation_proc, in backup_table add column version_id
    - 13.9 load csv as a pandas df, keep empty as empty not nan, all data column load as string
    - 13.10 Map original columns to sanitized headers
    - 13.11 Rename columns in the DataFrame based on the sanitized headers
    - 13.12 Filter the DataFrame to include only sanitized headers
    - 13.13 Write DataFrame to the stage table,add a row_count , yield every 100 rows ingest and a percent of progress based on row_count/df.count()
    - 13.14 Backup existing table with max_version as (SELECT max(version_id) as max_version_id FROM {backup_table_name})
    cursor.execute(f"""with max_version as (SELECT max(version_id) as max_version_id FROM {backup_table_name})
                    INSERT INTO {backup_table_name}                            
                    SELECT source_data.*, COALESCE(max_version.max_version_id, 0) + 1 
                      FROM (SELECT * FROM {table_name}) AS source_data,
                          max_version                             
                    """)
    - 13.15 exec validation_proc to Validate data in stage table and get validation_result
    - 13.16 Convert validation_result tuple containing a JSON string to a dictionary
    - 13.17 verify and log validation_result
    - 13.18 on pass validation Move data from stage table to main table
    - 13.19 Move data file from temp to archive and append a current timestamp in filename.
  14. add progress bar and steps status msg for upload file and trigger ingestion, media_type="text/plain". and not fake progress , real progress bar based on backend progress.

---

# Application Test Steps
  1. Test the Root Endpoint
  2. Test File Upload
  3. Test Data Ingestion
