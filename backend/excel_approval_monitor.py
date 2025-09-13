#!/usr/bin/env python3
"""
Excel Approval Monitor for Simplified Dropoff System
Monitors Excel forms for user approval and triggers processing via ReferenceDataAPI
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any

# Import existing backend and new utilities
from backend_lib import ReferenceDataAPI
from utils.database import DatabaseManager
from utils.workflow_manager import WorkflowManager
from utils.excel_processor import ExcelProcessor
from utils.pdf_report_generator import PDFReportGenerator
from utils.report_data_collector import ReportDataCollector

# Monitor settings
APPROVAL_CHECK_INTERVAL = 30  # seconds - check for approvals every 30 seconds
EXCEL_MODIFICATION_CHECK_INTERVAL = 60  # seconds - check for Excel modifications every minute
LOG_FILE = "/home/lin/repo/reference_data_mgr/logs/excel_approval_monitor.log"
MAX_CONCURRENT_PROCESSING = 3  # Maximum number of files to process simultaneously

class ExcelApprovalMonitor:
    """Monitors Excel forms for user approval and triggers processing"""

    def __init__(self):
        self.setup_logging()

        # Initialize components
        self.db_manager = DatabaseManager()
        self.workflow_manager = WorkflowManager(self.db_manager, self.logger)
        self.excel_processor = ExcelProcessor()
        self.pdf_generator = PDFReportGenerator()
        self.report_collector = ReportDataCollector(self.db_manager)

        # Initialize backend API for processing
        try:
            self.api = ReferenceDataAPI()
            self.logger.info("Backend API initialized successfully for Excel approval monitor")
        except Exception as e:
            self.logger.error(f"Failed to initialize backend API: {str(e)}")
            raise

        # Track active processing
        self.active_processing = {}  # {workflow_id: processing_start_time}

    def setup_logging(self):
        """Set up logging configuration"""
        # Create logs directory if it doesn't exist
        log_dir = Path(LOG_FILE).parent
        log_dir.mkdir(parents=True, exist_ok=True)

        # Configure logging (check for debug configuration)
        from utils.config_loader import config
        debug_enabled = config.get('enabled', False, 'debug')
        log_level = logging.DEBUG if debug_enabled else logging.INFO

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Excel approval monitor logging initialized")

    def run(self):
        """Main monitoring loop for Excel approval workflow"""
        self.logger.info("Starting Excel approval monitor...")

        try:
            while True:
                try:
                    # Check for approved Excel forms
                    self.check_for_approvals()

                    # Check for Excel form modifications
                    self.check_for_modifications()

                    # Clean up completed processing
                    self.cleanup_completed_processing()

                    # Sleep for approval check interval
                    time.sleep(APPROVAL_CHECK_INTERVAL)

                except KeyboardInterrupt:
                    self.logger.info("Received interrupt signal, shutting down Excel approval monitor...")
                    break
                except Exception as e:
                    self.logger.error(f"Error in Excel approval monitoring loop: {str(e)}")
                    time.sleep(APPROVAL_CHECK_INTERVAL)  # Continue monitoring despite errors

        finally:
            self.logger.info("Excel approval monitor stopped")

    def check_for_approvals(self):
        """Check for Excel forms that are ready for processing"""
        try:
            # Get workflows in excel_generated state
            pending_workflows = self.workflow_manager.get_pending_workflows()

            if not pending_workflows:
                return

            self.logger.info(f"Checking {len(pending_workflows)} workflows for Excel approvals")

            for workflow in pending_workflows:
                self.check_workflow_approval(workflow)

        except Exception as e:
            self.logger.error(f"Error checking for Excel approvals: {str(e)}")

    def check_workflow_approval(self, workflow: Dict[str, Any]):
        """Check if a specific workflow's Excel form is approved"""
        try:
            workflow_id = workflow['workflow_id']
            excel_path = workflow.get('excel_file_path')

            if not excel_path or not os.path.exists(excel_path):
                self.logger.warning(f"Excel file not found for workflow {workflow_id}: {excel_path}")
                return

            # Check if Excel is ready for processing
            if self.excel_processor.is_excel_ready_for_processing(excel_path):
                self.logger.info(f"Excel form approved for workflow {workflow_id}: {excel_path}")
                self.process_approved_excel(workflow_id, excel_path)

        except Exception as e:
            self.logger.error(f"Error checking workflow approval {workflow.get('workflow_id')}: {str(e)}")

    def process_approved_excel(self, workflow_id: str, excel_path: str):
        """Process an approved Excel form"""
        try:
            # Check concurrent processing limit
            if len(self.active_processing) >= MAX_CONCURRENT_PROCESSING:
                self.logger.info(f"Max concurrent processing limit reached ({MAX_CONCURRENT_PROCESSING}), skipping workflow {workflow_id}")
                return

            # Mark workflow as approved and start processing
            self.workflow_manager.update_status(
                workflow_id,
                self.workflow_manager.STATES['APPROVED']
            )

            # Add to active processing
            self.active_processing[workflow_id] = datetime.now()

            # Extract processing configuration from Excel
            processing_config = self.excel_processor.get_processing_configuration(excel_path)

            self.logger.info(f"Starting processing for workflow {workflow_id}")
            self.logger.info(f"Processing config: {processing_config}")

            # Update workflow to processing state
            self.workflow_manager.update_status(
                workflow_id,
                self.workflow_manager.STATES['PROCESSING'],
                processed_by=processing_config.get('processed_by', 'Unknown'),
                processing_started_at=datetime.now()
            )

            # Process the file using existing ReferenceDataAPI
            # Extract and prepare table name - derive from filename if empty
            table_name = processing_config.get('table_name', '').strip()
            if not table_name:
                # Derive table name from CSV filename
                from pathlib import Path
                csv_path = Path(processing_config['csv_file_path'])
                table_name = csv_path.stem.replace('-', '_').replace(' ', '_')
                self.logger.info(f"Derived table name from filename: {table_name}")
            
            result = self.api.process_file_sync(
                file_path=processing_config['csv_file_path'],
                load_type=processing_config['load_type'],
                table_name=table_name,
                target_schema=processing_config.get('target_schema', 'ref'),
                config_reference_data=processing_config.get('is_reference_data', False)
            )

            # Process the result and update workflow
            if result.get('success', False):
                self.logger.info(f"Successfully processed file for workflow {workflow_id}")

                # Update workflow to completed
                self.workflow_manager.update_status(
                    workflow_id,
                    self.workflow_manager.STATES['COMPLETED'],
                    processing_completed_at=datetime.now(),
                    rows_processed=result.get('rows_processed', 0),
                    processing_notes=result.get('message', 'Processing completed successfully')
                )

                # Generate success PDF report with detailed information
                report_path = None
                try:
                    from utils.config_loader import config
                    file_config = config.get_file_config()
                    
                    # Collect detailed information
                    detailed_info = self.report_collector.collect_success_details(
                        table_name=table_name,
                        schema_name=processing_config.get('target_schema', 'ref'),
                        csv_file_path=processing_config['csv_file_path'],
                        processing_config=processing_config,
                        result=result
                    )
                    
                    report_path = self.pdf_generator.generate_success_report(
                        workflow_id=workflow_id,
                        processing_config=processing_config,
                        result=result,
                        output_dir=file_config['processed_location'],
                        detailed_info=detailed_info
                    )
                    self.logger.info(f"Generated detailed success report: {report_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to generate success report: {str(e)}")

                # Move files to processed directory
                self.move_processed_files(processing_config['csv_file_path'], excel_path, report_path)

            else:
                error_message = result.get('error', 'Unknown processing error')
                self.logger.error(f"Processing failed for workflow {workflow_id}: {error_message}")

                # Update workflow to error state
                self.workflow_manager.update_status(
                    workflow_id,
                    self.workflow_manager.STATES['ERROR'],
                    error_message=error_message,
                    processing_completed_at=datetime.now()
                )

                # Generate error PDF report with detailed information
                report_path = None
                try:
                    # Collect detailed error information
                    detailed_error_info = self.report_collector.collect_error_details(
                        error_message=error_message,
                        processing_config=processing_config
                    )
                    
                    report_path = self.pdf_generator.generate_error_report(
                        workflow_id=workflow_id,
                        processing_config=processing_config,
                        error_message=error_message,
                        result=result,
                        detailed_error_info=detailed_error_info
                    )
                    self.logger.info(f"Generated detailed error report: {report_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to generate error report: {str(e)}")

                # Move files to error directory
                self.move_error_files(processing_config['csv_file_path'], excel_path, error_message, report_path)

        except Exception as e:
            error_msg = f"Failed to process approved Excel for workflow {workflow_id}: {str(e)}"
            self.logger.error(error_msg)

            # Generate error PDF report for exception
            report_path = None
            try:
                # Collect detailed error information including exception
                detailed_error_info = self.report_collector.collect_error_details(
                    error_message=error_msg,
                    processing_config=processing_config,
                    exception=e
                )
                
                report_path = self.pdf_generator.generate_error_report(
                    workflow_id=workflow_id,
                    processing_config=processing_config,
                    error_message=error_msg,
                    detailed_error_info=detailed_error_info
                )
                self.logger.info(f"Generated detailed exception error report: {report_path}")
            except Exception as report_e:
                self.logger.warning(f"Failed to generate exception error report: {str(report_e)}")

            # Move files to error directory
            try:
                self.move_error_files(processing_config['csv_file_path'], excel_path, error_msg, report_path)
            except Exception as move_e:
                self.logger.warning(f"Failed to move error files: {str(move_e)}")

            # Update workflow to error state
            try:
                self.workflow_manager.update_status(
                    workflow_id,
                    self.workflow_manager.STATES['ERROR'],
                    error_message=error_msg,
                    processing_completed_at=datetime.now()
                )
            except:
                pass  # Don't fail on workflow update errors

        finally:
            # Remove from active processing
            if workflow_id in self.active_processing:
                del self.active_processing[workflow_id]

    def check_for_modifications(self):
        """Check for modifications to Excel forms that might need re-evaluation"""
        try:
            # This is less critical than Excel because Excel files are more obvious when modified
            # For now, we'll just check pending workflows periodically
            reviewing_workflows = self.workflow_manager.get_pending_workflows()

            for workflow in reviewing_workflows:
                excel_path = workflow.get('excel_file_path')
                if excel_path and os.path.exists(excel_path):
                    # Check if Excel is now ready for processing
                    if self.excel_processor.is_excel_ready_for_processing(excel_path):
                        # Move back to excel_generated state for processing
                        self.workflow_manager.update_status(
                            workflow['workflow_id'],
                            self.workflow_manager.STATES['EXCEL_GENERATED']
                        )

        except Exception as e:
            self.logger.error(f"Error checking for Excel modifications: {str(e)}")

    def cleanup_completed_processing(self):
        """Clean up tracking for workflows that have been processing too long"""
        try:
            current_time = datetime.now()
            timeout_threshold = timedelta(hours=2)  # 2 hour timeout for processing

            timed_out_workflows = []
            for workflow_id, start_time in self.active_processing.items():
                if current_time - start_time > timeout_threshold:
                    timed_out_workflows.append(workflow_id)

            for workflow_id in timed_out_workflows:
                self.logger.warning(f"Processing timeout for workflow {workflow_id}, marking as error")

                # Update workflow to error state
                try:
                    self.workflow_manager.update_status(
                        workflow_id,
                        self.workflow_manager.STATES['ERROR'],
                        error_message=f"Processing timeout after {timeout_threshold}",
                        processing_completed_at=current_time
                    )
                except:
                    pass

                # Remove from active processing
                del self.active_processing[workflow_id]

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    def move_processed_files(self, csv_path: str, excel_path: str, pdf_path: str = None):
        """Move successfully processed files to processed directory with timestamp"""
        try:
            from utils.config_loader import config
            file_config = config.get_file_config()
            processed_dir = Path(file_config['processed_location'])
            processed_dir.mkdir(parents=True, exist_ok=True)

            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Process CSV file
            csv_file = Path(csv_path)
            if csv_file.exists():
                csv_stem = csv_file.stem
                csv_suffix = csv_file.suffix
                csv_processed_name = f"{csv_stem}_{timestamp}{csv_suffix}"
                csv_processed_path = processed_dir / csv_processed_name
                csv_file.rename(csv_processed_path)

            # Process Excel file
            excel_file = Path(excel_path)
            if excel_file.exists():
                excel_stem = excel_file.stem
                excel_suffix = excel_file.suffix
                excel_processed_name = f"{excel_stem}_{timestamp}{excel_suffix}"
                excel_processed_path = processed_dir / excel_processed_name
                excel_file.rename(excel_processed_path)

            # Process PDF file if provided (PDF already has timestamp, just move it)
            if pdf_path:
                pdf_file = Path(pdf_path)
                if pdf_file.exists():
                    pdf_processed_path = processed_dir / pdf_file.name
                    pdf_file.rename(pdf_processed_path)

            self.logger.info(f"Moved processed files with timestamp {timestamp} to: {processed_dir}")

        except Exception as e:
            self.logger.error(f"Failed to move processed files: {str(e)}")

    def move_error_files(self, csv_path: str, excel_path: str, error_message: str, pdf_path: str = None):
        """Move failed files to error directory with timestamp"""
        try:
            from utils.config_loader import config
            file_config = config.get_file_config()
            error_dir = Path(file_config['error_location'])
            error_dir.mkdir(parents=True, exist_ok=True)

            # Generate timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            # Process CSV file
            csv_file = Path(csv_path)
            if csv_file.exists():
                csv_stem = csv_file.stem
                csv_suffix = csv_file.suffix
                csv_error_name = f"{csv_stem}_{timestamp}{csv_suffix}"
                csv_error_path = error_dir / csv_error_name
                csv_file.rename(csv_error_path)

            # Process Excel file
            excel_file = Path(excel_path)
            if excel_file.exists():
                excel_stem = excel_file.stem
                excel_suffix = excel_file.suffix
                excel_error_name = f"{excel_stem}_{timestamp}{excel_suffix}"
                excel_error_path = error_dir / excel_error_name
                excel_file.rename(excel_error_path)

            # Process PDF file if provided (PDF already has timestamp, just move it)
            if pdf_path:
                pdf_file = Path(pdf_path)
                if pdf_file.exists():
                    pdf_error_path = error_dir / pdf_file.name
                    pdf_file.rename(pdf_error_path)

            # Create error log file with timestamp
            error_log_path = error_dir / f"{csv_file.stem}_error_{timestamp}.txt"
            with open(error_log_path, 'w') as f:
                f.write(f"Processing Error: {datetime.now().isoformat()}\n")
                f.write(f"CSV File: {csv_file.name}\n")
                f.write(f"Excel File: {excel_file.name}\n")
                f.write(f"Error: {error_message}\n")

            self.logger.info(f"Moved error files with timestamp {timestamp} to: {error_dir}")

        except Exception as e:
            self.logger.error(f"Failed to move error files: {str(e)}")

def main():
    """Main entry point for Excel approval monitor"""
    try:
        monitor = ExcelApprovalMonitor()
        monitor.run()
    except KeyboardInterrupt:
        print("\nExcel approval monitor stopped by user")
    except Exception as e:
        print(f"Excel approval monitor failed to start: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()