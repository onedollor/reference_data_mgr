# Tasks Document

<!-- AI Instructions: For each task, generate a _Prompt field with structured AI guidance following this format:
_Prompt: Role: [specialized developer role] | Task: [clear task description with context references] | Restrictions: [what not to do, constraints] | Success: [specific completion criteria]_
This helps provide better AI agent guidance beyond simple "work on this task" prompts. -->

- [ ] 1. Create PDF form generation utility
  - File: backend/utils/pdf_generator.py
  - Implement PDFFormGenerator class for creating interactive PDF configuration forms
  - Add form field population from CSV detection results
  - Purpose: Generate user-editable PDF forms from CSV analysis
  - _Leverage: backend/utils/csv_detector.py, backend/utils/file_handler.py_
  - _Requirements: 2.1, 2.2_
  - _Prompt: Role: Python Developer specializing in PDF generation and form creation | Task: Create PDFFormGenerator class that generates interactive PDF forms from CSV detection results following requirements 2.1 and 2.2, using existing patterns from csv_detector.py and file_handler.py | Restrictions: Must use reportlab or fpdf library, follow existing utility module patterns, ensure forms are interactive and fillable | Success: PDFFormGenerator creates functional PDF forms with editable fields, integrates seamlessly with existing CSV detection, follows project coding standards_

- [ ] 2. Create PDF form processing utility  
  - File: backend/utils/pdf_processor.py
  - Implement PDFProcessor class for validating and extracting data from completed PDF forms
  - Add form validation with clear error messages
  - Purpose: Extract user configuration from completed PDF forms
  - _Leverage: backend/utils/logger.py for validation errors_
  - _Requirements: 3.1, 3.2_
  - _Prompt: Role: Python Developer with expertise in PDF parsing and data validation | Task: Create PDFProcessor class that validates and extracts configuration from PDF forms following requirements 3.1 and 3.2, using existing logging patterns from utils/logger.py | Restrictions: Must use PyPDF2 or pdfplumber library, provide clear validation error messages, handle corrupted or incomplete PDF files gracefully | Success: PDFProcessor reliably extracts form data, validates user inputs with helpful error messages, handles edge cases robustly_

- [ ] 3. Create workflow management utility
  - File: backend/utils/workflow_manager.py
  - Implement WorkflowManager class for orchestrating PDF workflow states
  - Add database integration for workflow state persistence
  - Purpose: Coordinate PDF workflow lifecycle and state transitions
  - _Leverage: backend/utils/database.py, backend/utils/logger.py_
  - _Requirements: 4.1, 4.2_
  - _Prompt: Role: Python Developer specializing in state management and database integration | Task: Create WorkflowManager class for PDF workflow orchestration following requirements 4.1 and 4.2, using existing DatabaseManager and Logger patterns | Restrictions: Must follow existing database connection patterns, ensure atomic state transitions, handle concurrent workflow management | Success: WorkflowManager correctly tracks workflow states, persists data reliably, handles concurrent operations safely_

- [ ] 4. Create database schema for PDF workflows
  - File: sql/pdf_workflow_schema.sql
  - Define ref.PDF_Workflow_Tracking table structure
  - Add indexes and constraints for performance and data integrity
  - Purpose: Provide database foundation for workflow state tracking
  - _Leverage: existing SQL schema patterns in sql/ directory_
  - _Requirements: Design document data models_
  - _Prompt: Role: Database Administrator with expertise in SQL Server and schema design | Task: Create database schema for PDF workflow tracking following design document data models, using existing SQL patterns and conventions from sql/ directory | Restrictions: Must use ref schema, follow existing naming conventions, ensure proper indexing and constraints | Success: Schema supports all workflow operations efficiently, maintains data integrity, follows project database standards_

- [ ] 5. Update existing file monitor for simplified dropoff
  - File: backend/simplified_file_monitor.py
  - Create new SimplifiedFileMonitor class extending existing FileMonitor patterns
  - Modify directory scanning to work with single dropoff directory
  - Purpose: Adapt file monitoring to trigger PDF workflow instead of immediate processing
  - _Leverage: backend/file_monitor.py, backend/utils/workflow_manager.py_
  - _Requirements: 1.1, 1.2, 1.3_
  - _Prompt: Role: Python Developer with expertise in file system monitoring and asynchronous programming | Task: Create SimplifiedFileMonitor extending existing FileMonitor patterns to trigger PDF workflows following requirements 1.1-1.3, integrating with WorkflowManager | Restrictions: Must preserve 90% of existing FileMonitor logic, maintain 15-second detection interval, ensure backward compatibility | Success: SimplifiedFileMonitor detects files correctly, triggers PDF generation instead of processing, maintains existing performance characteristics_

- [ ] 6. Integrate PDF generation with CSV detection
  - File: backend/simplified_file_monitor.py (continue from task 5)
  - Connect PDF generation with existing CSVFormatDetector output
  - Add error handling for PDF generation failures
  - Purpose: Create seamless integration between format detection and PDF form creation
  - _Leverage: backend/utils/csv_detector.py, backend/utils/pdf_generator.py_
  - _Requirements: 2.3_
  - _Prompt: Role: Integration Developer with expertise in Python module integration and error handling | Task: Integrate PDF generation with CSV detection following requirement 2.3, ensuring seamless data flow from CSVFormatDetector to PDFFormGenerator | Restrictions: Must handle CSV detection failures gracefully, preserve existing detection accuracy, ensure PDF generation doesn't block file monitoring | Success: Integration works smoothly, handles errors appropriately, maintains system responsiveness during PDF generation_

- [ ] 7. Create PDF approval monitoring loop
  - File: backend/pdf_approval_monitor.py
  - Implement monitoring service that checks for completed PDF forms
  - Add integration with existing ReferenceDataAPI.process_file method
  - Purpose: Monitor PDF forms for user approval and trigger processing
  - _Leverage: backend/backend_lib.py ReferenceDataAPI, backend/utils/pdf_processor.py_
  - _Requirements: 5.1, 5.2, 6.1_
  - _Prompt: Role: Python Developer specializing in background services and API integration | Task: Create PDF approval monitoring service following requirements 5.1, 5.2, and 6.1, integrating with existing ReferenceDataAPI.process_file method | Restrictions: Must use existing ReferenceDataAPI unchanged, monitor PDF files efficiently, handle concurrent approvals | Success: Monitoring service detects PDF approvals correctly, triggers processing with proper configuration, maintains existing processing performance_

- [ ] 8. Add configuration validation and error handling
  - File: backend/utils/pdf_processor.py (extend task 2)
  - Implement comprehensive form validation for conflicting options
  - Add clear error messages and recovery suggestions
  - Purpose: Ensure user configurations are valid before processing
  - _Leverage: existing validation patterns from backend/utils/ingest.py_
  - _Requirements: 5.3, 5.4_
  - _Prompt: Role: Python Developer with expertise in data validation and user experience | Task: Extend PDFProcessor with comprehensive validation following requirements 5.3 and 5.4, using existing validation patterns from utils/ingest.py | Restrictions: Must provide actionable error messages, handle all edge cases gracefully, maintain user-friendly validation experience | Success: Validation catches all configuration conflicts, provides clear guidance to users, prevents invalid processing attempts_

- [ ] 9. Create startup script for simplified system
  - File: start_simplified_monitor.sh
  - Create shell script that starts both simplified file monitor and PDF approval monitor
  - Add proper process management and logging
  - Purpose: Provide single command to start the simplified dropoff system
  - _Leverage: existing start_monitor.sh patterns_
  - _Requirements: System operational requirements_
  - _Prompt: Role: DevOps Engineer with expertise in shell scripting and process management | Task: Create startup script for simplified system following existing patterns from start_monitor.sh, managing both file monitoring and PDF approval processes | Restrictions: Must handle process failures gracefully, ensure proper logging setup, provide status monitoring capabilities | Success: Script reliably starts all required processes, handles failures appropriately, provides clear status information_

- [ ] 10. Create unit tests for PDF utilities
  - File: backend/tests/test_pdf_generator.py, backend/tests/test_pdf_processor.py
  - Write comprehensive unit tests for PDF generation and processing
  - Test error scenarios and edge cases
  - Purpose: Ensure reliability of PDF workflow components
  - _Leverage: existing test patterns from backend/tests/_
  - _Requirements: All PDF-related requirements_
  - _Prompt: Role: QA Engineer with expertise in Python unit testing and PDF testing | Task: Create comprehensive unit tests for PDF utilities covering all PDF-related requirements, using existing test patterns and frameworks | Restrictions: Must test both success and failure scenarios, mock external dependencies appropriately, ensure tests are maintainable | Success: Tests provide good coverage for PDF functionality, catch edge cases and errors, run reliably in isolation_

- [ ] 11. Create workflow integration tests
  - File: backend/tests/test_simplified_workflow.py
  - Implement end-to-end tests for complete PDF workflow
  - Test file detection → PDF generation → user approval → processing flow
  - Purpose: Validate complete simplified dropoff workflow
  - _Leverage: backend/tests/test_integration.py patterns_
  - _Requirements: All workflow requirements_
  - _Prompt: Role: Integration Test Engineer with expertise in end-to-end testing and workflow validation | Task: Create comprehensive integration tests for simplified workflow covering all requirements, extending existing integration test patterns | Restrictions: Must test real workflow scenarios, use appropriate test data, ensure tests are repeatable and reliable | Success: Tests validate complete workflow end-to-end, cover all user scenarios, provide confidence in system reliability_

- [ ] 12. Update documentation for simplified system
  - File: README.md (update existing)
  - Document new simplified dropoff process and PDF workflow
  - Add setup and usage instructions for simplified system
  - Purpose: Provide clear guidance for using the simplified dropoff system
  - _Leverage: existing README.md structure and style_
  - _Requirements: Usability requirements_
  - _Prompt: Role: Technical Writer with expertise in software documentation and user guides | Task: Update README.md with comprehensive documentation for simplified system following usability requirements, maintaining existing documentation style | Restrictions: Must not remove existing documentation, ensure clarity for different user types, provide troubleshooting guidance | Success: Documentation is clear and comprehensive, users can successfully set up and use the simplified system, troubleshooting information is helpful_

- [ ] 13. Create database migration script
  - File: sql/migrate_to_simplified_system.sql
  - Create migration script for new PDF workflow tables
  - Add data preservation for existing tracking tables
  - Purpose: Enable smooth transition to simplified system
  - _Leverage: existing database schema in sql/ directory_
  - _Requirements: Backward compatibility requirements_
  - _Prompt: Role: Database Administrator with expertise in SQL Server migrations and data preservation | Task: Create database migration script for simplified system following backward compatibility requirements, preserving existing data and schema | Restrictions: Must not lose existing data, ensure migration is reversible, test migration thoroughly | Success: Migration script successfully creates new schema, preserves existing data, can be executed safely in production_

- [ ] 14. Performance testing and optimization
  - File: backend/tests/test_performance_simplified.py
  - Test PDF generation time with various file sizes
  - Validate system performance meets requirements (<30s PDF generation, <15s detection)
  - Purpose: Ensure simplified system meets performance requirements
  - _Leverage: existing performance test patterns_
  - _Requirements: Performance requirements (PDF <30s, detection <15s)_
  - _Prompt: Role: Performance Engineer with expertise in Python performance testing and optimization | Task: Create performance tests for simplified system validating PDF generation and detection time requirements, using existing performance test patterns | Restrictions: Must test realistic file sizes, measure actual performance metrics, identify bottlenecks if requirements not met | Success: System consistently meets performance requirements, bottlenecks are identified and addressed, performance is monitored and measurable_

- [ ] 15. Final integration and system validation
  - File: Multiple files - final integration
  - Integrate all components and validate complete system functionality
  - Perform final testing and bug fixes
  - Purpose: Ensure complete simplified dropoff system works correctly
  - _Leverage: All previously created components_
  - _Requirements: All requirements_
  - _Prompt: Role: Senior Developer with expertise in system integration and quality assurance | Task: Complete final integration of all simplified dropoff system components, performing comprehensive system validation covering all requirements | Restrictions: Must not break existing functionality, ensure all requirements are met, maintain system reliability and performance | Success: Complete simplified dropoff system functions correctly, all requirements are satisfied, system is ready for production use_