# Requirements Document

## Introduction

The Simplified Dropoff System transforms the current automated CSV processing pipeline from a fully automated approach to a human-in-the-loop validation system. This feature eliminates the complex subfolder structure (`reference_data_table` and `none_reference_data_table`) and replaces it with a single dropoff point that generates interactive PDF forms for user validation and configuration before processing.

The system will detect CSV files, analyze their format, generate modifiable PDF configuration forms, and wait for human approval before proceeding with data ingestion. This approach provides greater control and validation while maintaining the existing processing engine's reliability.

## Alignment with Product Vision

This feature aligns with key product principles while introducing controlled manual intervention:

- **Reliability First**: Maintains data integrity through human validation while preserving the robust processing engine
- **Intelligent Automation**: Retains smart format detection but adds human oversight for critical processing decisions  
- **Comprehensive Observability**: Provides complete visibility into processing decisions through PDF audit trail
- **Enterprise Scalability**: Maintains ability to process large files while adding governance controls

The feature addresses the need for greater control over processing decisions while preserving the system's core automation capabilities and audit requirements.

## Requirements

### Requirement 1: Single Dropoff Directory Structure

**User Story:** As a data operations manager, I want to drop CSV files into a single directory without worrying about subfolder categorization, so that I can simplify the file ingestion process and reduce user errors.

#### Acceptance Criteria

1. WHEN a user drops a CSV file into the dropoff directory THEN the system SHALL detect the file without requiring subfolder placement
2. WHEN the existing subfolder structure exists THEN the system SHALL ignore `reference_data_table` and `none_reference_data_table` directories
3. WHEN multiple CSV files are dropped THEN the system SHALL process each file independently with separate PDF forms

### Requirement 2: Automated CSV Format Detection and PDF Generation

**User Story:** As a data operations manager, I want the system to automatically analyze CSV files and generate configuration forms, so that I can review and approve processing settings without manual format analysis.

#### Acceptance Criteria

1. WHEN a CSV file is detected THEN the system SHALL analyze file format including delimiter, encoding, and headers within 15 seconds
2. WHEN format detection completes THEN the system SHALL generate a PDF form with the same base filename as the CSV file
3. WHEN the PDF is generated THEN the system SHALL populate detected format information as pre-filled values
4. IF format detection fails THEN the system SHALL generate a PDF with default values and error notification

### Requirement 3: Interactive PDF Configuration Form

**User Story:** As a data operations manager, I want to review and modify processing configuration through a PDF form, so that I can ensure correct processing settings before data ingestion begins.

#### Acceptance Criteria

1. WHEN the PDF form is generated THEN the system SHALL include the following interactive elements:
   - Detected format information (delimiter, encoding, headers) as editable fields
   - Checkbox for processing mode selection (fullload OR append)
   - Checkbox for reference data table creation (yes/no)
   - Final confirmation checkbox to proceed with processing
2. WHEN the user opens the PDF THEN all detected values SHALL be pre-populated based on format analysis
3. WHEN the user modifies values THEN the PDF SHALL save changes when the document is saved
4. WHEN the user selects conflicting options THEN the PDF SHALL display validation warnings

### Requirement 4: PDF-Based Processing Control

**User Story:** As a data operations manager, I want the system to wait for my PDF approval before processing files, so that I can ensure all settings are correct and provide explicit approval for data operations.

#### Acceptance Criteria

1. WHEN a PDF is generated THEN the system SHALL pause processing and wait for user modification
2. WHEN the PDF is saved with the confirmation checkbox checked THEN the system SHALL proceed with file processing using PDF-specified settings
3. WHEN the PDF remains unconfirmed THEN the system SHALL continue monitoring without processing the CSV file
4. WHEN processing begins THEN the system SHALL use PDF values to override any default system detection
5. WHEN processing completes THEN the system SHALL archive both CSV and PDF files with processing audit trail

### Requirement 5: Processing Mode Configuration

**User Story:** As a database administrator, I want to specify whether files should be processed as fullload or append operations through the PDF form, so that I can control data loading behavior without folder-based conventions.

#### Acceptance Criteria

1. WHEN fullload mode is selected THEN the system SHALL truncate existing table data before loading new data
2. WHEN append mode is selected THEN the system SHALL add new data to existing table without truncation
3. WHEN reference data table option is enabled THEN the system SHALL create configuration records in `ref.Reference_Data_Cfg`
4. WHEN reference data table option is disabled THEN the system SHALL skip configuration record creation
5. IF conflicting processing modes are selected THEN the system SHALL display error in PDF and require correction

### Requirement 6: Backward Compatibility with Existing Processing Engine

**User Story:** As a system administrator, I want the new dropoff system to use the existing processing engine, so that data quality and performance characteristics remain consistent.

#### Acceptance Criteria

1. WHEN PDF approval is completed THEN the system SHALL invoke existing `ReferenceDataAPI.process_file` method
2. WHEN processing occurs THEN the system SHALL maintain existing database schema (`ref` and `bkp` schemas)
3. WHEN files are processed THEN the system SHALL preserve existing audit trail and logging functionality
4. WHEN errors occur THEN the system SHALL use existing error handling and recovery mechanisms

## Non-Functional Requirements

### Code Architecture and Modularity
- **Single Responsibility Principle**: PDF generation, form processing, and file monitoring should be separate modules
- **Modular Design**: PDF form components should be reusable and configurable for different file types
- **Dependency Management**: Minimize dependencies on external PDF libraries while maintaining functionality
- **Clear Interfaces**: Define clean contracts between PDF generation, form processing, and existing backend systems

### Performance
- **PDF Generation Time**: Generate PDF forms within 30 seconds of CSV file detection
- **File Detection Latency**: Maintain existing <15 second detection time for CSV files
- **Processing Throughput**: Preserve existing >1,000 rows/second processing performance
- **Memory Usage**: PDF generation should not exceed 100MB memory overhead per file

### Security
- **File Access Control**: PDF files should have appropriate read/write permissions for authorized users only
- **Data Privacy**: PDF forms should not contain actual CSV data, only metadata and configuration options
- **Audit Trail**: All PDF generation and modification events should be logged with timestamps and user identification

### Reliability
- **PDF Generation Success Rate**: >99% successful PDF generation for valid CSV files
- **Form Validation**: PDF forms should validate user inputs and prevent invalid configuration combinations
- **Recovery Capability**: System should recover gracefully from PDF generation failures and continue monitoring
- **Atomic Operations**: PDF generation and CSV processing should be atomic operations with rollback capability

### Usability
- **PDF Form Clarity**: Forms should be intuitive with clear labels, helpful tooltips, and logical field organization
- **Error Messaging**: PDF validation errors should provide clear, actionable feedback to users
- **Form Persistence**: PDF forms should save user inputs automatically to prevent data loss
- **Processing Status**: Users should have clear indication of processing status and completion