# Reference Data Management System - Requirements Specification

## Project Overview
The Reference Data Management System is an automated CSV file processing solution that monitors designated dropoff folders, performs intelligent format detection, and ingests data into SQL Server databases with comprehensive error handling and audit capabilities.

## Business Requirements

### BR-001: Automated File Processing
**Description**: The system must automatically detect, validate, and process CSV files dropped into monitored directories without manual intervention.

**Acceptance Criteria**:
- Files are detected within 15 seconds of being dropped
- Processing begins automatically after file stability is confirmed
- No manual intervention required for standard CSV formats
- Support for multiple concurrent file processing

### BR-002: Reference Data Classification
**Description**: The system must distinguish between reference data files and non-reference data files, applying different processing rules.

**Acceptance Criteria**:
- Reference data files generate configuration records in Reference_Data_Cfg table
- Non-reference data files are processed without configuration record generation
- Classification based on dropoff folder structure
- Proper audit trail for both data types

### BR-003: Data Quality Assurance
**Description**: The system must ensure data integrity through validation, error handling, and comprehensive audit trails.

**Acceptance Criteria**:
- File stability verification (6 consecutive checks without changes)
- Format validation before processing
- Transaction rollback on processing failures
- Complete audit trail in tracking database

## Functional Requirements

### File Monitoring Requirements

#### FR-001: Folder Monitoring
**Description**: Monitor designated dropoff folders for new CSV files
**Priority**: Critical
**Dependencies**: None

**Detailed Requirements**:
- Scan folders every 15 seconds
- Support nested folder structure: `dropoff/{reference_data_table|none_reference_data_table}/{fullload|append}/`
- Ignore non-CSV files
- Handle file system permissions gracefully

#### FR-002: File Stability Detection
**Description**: Ensure files are completely uploaded before processing
**Priority**: Critical
**Dependencies**: FR-001

**Detailed Requirements**:
- Monitor file size and modification time
- Require 6 consecutive unchanged checks (90-second stability period)
- Reset counter on any file changes
- Remove tracking for deleted files

#### FR-003: File Classification
**Description**: Classify files by location and processing type
**Priority**: High
**Dependencies**: FR-001

**Detailed Requirements**:
- Detect reference data vs non-reference data from folder path
- Identify fullload vs append operation from folder structure
- Extract load type for processing configuration

### Format Detection Requirements

#### FR-004: CSV Format Detection
**Description**: Automatically detect CSV file format characteristics
**Priority**: Critical
**Dependencies**: FR-002

**Detailed Requirements**:
- Auto-detect delimiter (comma, semicolon, pipe, tab)
- Identify presence of header row
- Handle various text qualifiers (quotes, double quotes)
- Support multiple encodings (UTF-8, Latin-1, etc.)
- Fallback detection mechanisms for edge cases

#### FR-005: Table Name Extraction
**Description**: Extract database table name from filename
**Priority**: High
**Dependencies**: FR-001

**Detailed Requirements**:
- Remove file extension (.csv)
- Strip date patterns (YYYYMMDD, YYYY-MM-DD)
- Convert special characters to underscores
- Normalize to lowercase
- Handle complex filename patterns

#### FR-006: Schema Analysis
**Description**: Analyze file headers against existing database schemas
**Priority**: Medium
**Dependencies**: FR-004

**Detailed Requirements**:
- Compare file headers with existing table columns
- Calculate schema match percentage
- Identify best matching tables (>70% match threshold)
- Report column mismatches and suggestions

### Data Processing Requirements

#### FR-007: Data Ingestion
**Description**: Load CSV data into SQL Server database tables
**Priority**: Critical
**Dependencies**: FR-004, FR-005

**Detailed Requirements**:
- Support fullload (truncate and reload) and append modes
- Process data in configurable batches (default 990 rows)
- Handle large files (100MB+) efficiently
- Maintain transaction integrity
- Support multiple target schemas (ref, staging, etc.)

#### FR-008: Progress Tracking
**Description**: Provide real-time progress information for long-running operations
**Priority**: Medium
**Dependencies**: FR-007

**Detailed Requirements**:
- Track processing progress by percentage
- Support operation cancellation
- Log progress at configurable intervals
- Store progress state persistently

#### FR-009: Type Inference
**Description**: Automatically infer appropriate data types for columns
**Priority**: Low
**Dependencies**: FR-007

**Detailed Requirements**:
- Analyze sample data for type patterns
- Detect date/time formats
- Identify numeric vs text columns
- Handle mixed-type columns gracefully
- Configurable type detection thresholds

### Database Requirements

#### FR-010: Connection Management
**Description**: Manage database connections efficiently and reliably
**Priority**: Critical
**Dependencies**: None

**Detailed Requirements**:
- Connection pooling with configurable pool size
- Automatic retry with exponential backoff
- Handle connection timeouts gracefully
- Support SQL Server authentication
- Validate connections before use

#### FR-011: Schema Management
**Description**: Manage database schemas and table structures
**Priority**: High
**Dependencies**: FR-010

**Detailed Requirements**:
- Create schemas if they don't exist (ref, bkp)
- Validate table existence before operations
- Handle table creation for new data sources
- Support dynamic schema targeting

#### FR-012: Transaction Management
**Description**: Ensure data consistency through proper transaction handling
**Priority**: Critical
**Dependencies**: FR-010

**Detailed Requirements**:
- Use database transactions for all data modifications
- Automatic rollback on processing failures
- Commit only after successful completion
- Handle deadlocks and transaction conflicts

### File Management Requirements

#### FR-013: Processed File Handling
**Description**: Manage successfully processed files
**Priority**: High
**Dependencies**: FR-007

**Detailed Requirements**:
- Move processed files to `processed/` subfolder
- Maintain original filename with timestamp prefix
- Preserve file metadata and permissions
- Create processed folder structure if missing

#### FR-014: Error File Handling
**Description**: Manage files that failed processing
**Priority**: High
**Dependencies**: FR-007

**Detailed Requirements**:
- Move failed files to `error/` subfolder
- Include error information in filename or separate log
- Preserve original file for analysis
- Create error folder structure if missing

#### FR-015: File Tracking
**Description**: Maintain comprehensive tracking of all file operations
**Priority**: High
**Dependencies**: FR-010

**Detailed Requirements**:
- Record all file activities in tracking database
- Store file metadata (size, hash, timestamps)
- Track processing status and error messages
- Support file history queries

### Configuration Requirements

#### FR-016: Reference Data Configuration
**Description**: Manage reference data configuration records
**Priority**: Medium
**Dependencies**: FR-007, FR-003

**Detailed Requirements**:
- Insert records into Reference_Data_Cfg table for reference data files
- Skip configuration insertion for non-reference data files
- Handle duplicate configuration records gracefully
- Validate configuration record requirements

#### FR-017: Environment Configuration
**Description**: Support flexible system configuration
**Priority**: Medium
**Dependencies**: None

**Detailed Requirements**:
- Load configuration from environment variables
- Support .env file configuration
- Validate required configuration parameters
- Provide sensible defaults for optional parameters

### Integration Requirements

#### FR-018: Backend API
**Description**: Provide unified API for system operations
**Priority**: High
**Dependencies**: Multiple

**Detailed Requirements**:
- Synchronous and asynchronous processing methods
- Health check endpoints
- Table information queries
- Format detection services
- Operation cancellation support

#### FR-019: Logging and Monitoring
**Description**: Comprehensive system logging and monitoring
**Priority**: High
**Dependencies**: None

**Detailed Requirements**:
- Multi-level logging (DEBUG, INFO, WARNING, ERROR)
- File and console output
- Log rotation and cleanup
- Performance metrics logging
- Error notification capabilities

#### FR-020: System Health
**Description**: Monitor and report system health status
**Priority**: Medium
**Dependencies**: FR-010

**Detailed Requirements**:
- Database connectivity checks
- File system accessibility validation
- System resource monitoring
- Health status reporting
- Automatic recovery attempts

## Non-Functional Requirements

### Performance Requirements

#### NFR-001: Processing Speed
**Description**: System must process files efficiently
**Requirements**:
- Process files up to 100MB within acceptable timeframes
- Support concurrent processing of multiple files
- Batch processing with configurable batch sizes
- Memory usage optimization for large files

#### NFR-002: Monitoring Responsiveness
**Description**: File monitoring must be responsive and reliable
**Requirements**:
- File detection within 15 seconds
- Low CPU usage during monitoring periods
- Efficient file system scanning
- Minimal false positives in file detection

### Reliability Requirements

#### NFR-003: System Availability
**Description**: System must operate reliably with minimal downtime
**Requirements**:
- 99.5% uptime during business hours
- Graceful handling of system restart
- Recovery from database connection failures
- Resilient to file system issues

#### NFR-004: Data Integrity
**Description**: Ensure data accuracy and consistency
**Requirements**:
- No data loss during processing failures
- Accurate file stability detection
- Complete transaction rollback on errors
- Comprehensive audit trail maintenance

### Scalability Requirements

#### NFR-005: File Volume
**Description**: Handle varying file volumes efficiently
**Requirements**:
- Process multiple simultaneous file drops
- Handle files ranging from KB to 100MB+
- Scale processing based on system resources
- Queue management for high-volume periods

#### NFR-006: Concurrent Operations
**Description**: Support multiple concurrent operations
**Requirements**:
- Multiple file processing streams
- Thread-safe operations
- Resource sharing without conflicts
- Efficient resource utilization

### Security Requirements

#### NFR-007: Database Security
**Description**: Secure database access and operations
**Requirements**:
- Encrypted database connections
- Secure credential management
- SQL injection prevention
- Access logging and monitoring

#### NFR-008: File System Security
**Description**: Secure file operations
**Requirements**:
- Appropriate file permissions
- Secure temporary file handling
- Protection against malicious files
- Access control validation

### Maintainability Requirements

#### NFR-009: Code Quality
**Description**: Maintain high code quality standards
**Requirements**:
- Comprehensive error handling
- Clear logging and debugging information
- Modular architecture
- Unit test coverage

#### NFR-010: Operations Support
**Description**: Support operational monitoring and maintenance
**Requirements**:
- Clear operational procedures
- Monitoring and alerting capabilities
- Log analysis tools
- Performance tuning capabilities

## Constraints and Assumptions

### Technical Constraints
- Must use SQL Server as the primary database
- Python-based implementation required
- File-based monitoring (no API-based upload)
- Windows/Linux compatibility required

### Business Constraints
- 24/7 operation capability required
- Minimal manual intervention acceptable
- Must integrate with existing database schema
- Budget constraints for infrastructure scaling

### Assumptions
- CSV files follow standard formatting conventions
- Database connectivity is generally reliable
- File system permissions are properly configured
- Adequate disk space available for processing and archival

## Success Criteria

### Primary Success Metrics
- 99%+ successful file processing rate
- Average processing time under defined thresholds
- Zero data loss incidents
- Comprehensive audit trail coverage

### Secondary Success Metrics
- Reduced manual intervention by 95%
- Processing error rate under 1%
- System availability above 99.5%
- Complete operational visibility

## Risk Assessment

### High Risk Items
- Database connectivity failures
- Large file processing timeouts
- File system permission issues
- Concurrent processing conflicts

### Mitigation Strategies
- Connection retry and failover mechanisms
- Progress tracking and resumption capabilities
- Automated permission validation
- Thread-safe processing implementation

This requirements specification provides the foundation for implementing a robust, scalable, and reliable reference data management system that meets both current operational needs and future growth requirements.