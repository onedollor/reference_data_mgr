"""
Workflow Management Utility
Orchestrates Excel workflow states and coordinates with processing pipeline
"""

import os
import uuid
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pathlib import Path
import json

from .database import DatabaseManager
from .logger import Logger

class WorkflowManager:
    """Manages Excel workflow lifecycle and state transitions (using legacy database field names)"""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None, logger: Optional[Logger] = None):
        """Initialize the workflow manager"""
        self.db_manager = db_manager or DatabaseManager()
        self.logger = logger or Logger()
        
        # Workflow states
        self.STATES = {
            'PENDING_EXCEL': 'pending_excel',
            'EXCEL_GENERATED': 'excel_generated', 
            'USER_REVIEWING': 'user_reviewing',
            'APPROVED': 'approved',
            'PROCESSING': 'processing',
            'COMPLETED': 'completed',
            'ERROR': 'error'
        }
        
        # In-memory workflow tracking for active workflows
        self.active_workflows = {}  # {workflow_id: WorkflowState}
        
        # Initialize database schema if needed
        self._ensure_workflow_table()
    
    def _ensure_workflow_table(self):
        """Ensure the Excel workflow tracking table exists"""
        try:
            create_table_sql = """
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Excel_Workflow_Tracking' AND xtype='U')
            CREATE TABLE ref.Excel_Workflow_Tracking (
                workflow_id VARCHAR(50) PRIMARY KEY,
                csv_file_path NVARCHAR(500) NOT NULL,
                excel_file_path NVARCHAR(500),
                status VARCHAR(50) NOT NULL,
                created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
                excel_generated_at DATETIME2,
                approved_at DATETIME2,
                completed_at DATETIME2,
                error_message NVARCHAR(MAX),
                user_config NVARCHAR(MAX),
                processed_by NVARCHAR(100),
                retry_count INT DEFAULT 0
            )
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                conn.commit()
                
            self.logger.info("Excel workflow tracking table ensured")
            
        except Exception as e:
            self.logger.error(f"Failed to create workflow tracking table: {str(e)}")
            raise
    
    def create_workflow(self, csv_path: str) -> str:
        """
        Create new Excel workflow for a CSV file
        
        Args:
            csv_path: Path to the CSV file to be processed
            
        Returns:
            Workflow ID for tracking
        """
        try:
            workflow_id = str(uuid.uuid4())
            
            # Insert into database
            insert_sql = """
            INSERT INTO ref.Excel_Workflow_Tracking 
            (workflow_id, csv_file_path, status, created_at)
            VALUES (?, ?, ?, ?)
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(insert_sql, (
                    workflow_id,
                    csv_path,
                    self.STATES['PENDING_EXCEL'],
                    datetime.now()
                ))
                conn.commit()
            
            # Add to active workflows
            self.active_workflows[workflow_id] = {
                'workflow_id': workflow_id,
                'csv_path': csv_path,
                'excel_path': None,
                'current_status': self.STATES['PENDING_EXCEL'],
                'format_detection': {},
                'last_checked': datetime.now(),
                'retry_count': 0
            }
            
            self.logger.info(f"Created workflow {workflow_id} for file: {csv_path}")
            return workflow_id
            
        except Exception as e:
            self.logger.error(f"Failed to create workflow for {csv_path}: {str(e)}")
            raise
    
    def update_status(self, workflow_id: str, status: str, **kwargs) -> bool:
        """
        Update workflow status with optional additional data
        
        Args:
            workflow_id: Workflow identifier
            status: New status from STATES
            **kwargs: Additional fields to update (excel_path, error_message, user_config, etc.)
            
        Returns:
            True if update successful
        """
        try:
            if status not in self.STATES.values():
                raise ValueError(f"Invalid workflow status: {status}")
            
            # Build update query dynamically based on provided kwargs
            update_fields = ['status = ?']
            update_values = [status]
            
            # Add timestamp fields based on status
            if status == self.STATES['EXCEL_GENERATED']:
                update_fields.append('excel_generated_at = ?')
                update_values.append(datetime.now())
            elif status == self.STATES['APPROVED']:
                update_fields.append('approved_at = ?')
                update_values.append(datetime.now())
            elif status == self.STATES['COMPLETED']:
                update_fields.append('completed_at = ?')
                update_values.append(datetime.now())
            
            # Add optional fields
            field_mapping = {
                'excel_path': 'excel_file_path',
                'error_message': 'error_message',
                'user_config': 'user_config',
                'processed_by': 'processed_by',
                'retry_count': 'retry_count'
            }
            
            for key, db_field in field_mapping.items():
                if key in kwargs:
                    update_fields.append(f'{db_field} = ?')
                    value = kwargs[key]
                    # JSON encode user_config if it's a dict
                    if key == 'user_config' and isinstance(value, dict):
                        value = json.dumps(value)
                    update_values.append(value)
            
            update_values.append(workflow_id)
            
            update_sql = f"""
            UPDATE ref.Excel_Workflow_Tracking 
            SET {', '.join(update_fields)}
            WHERE workflow_id = ?
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(update_sql, update_values)
                rows_affected = cursor.rowcount
                conn.commit()
            
            if rows_affected == 0:
                self.logger.warning(f"No workflow found with ID: {workflow_id}")
                return False
            
            # Update in-memory tracking
            if workflow_id in self.active_workflows:
                self.active_workflows[workflow_id]['current_status'] = status
                self.active_workflows[workflow_id]['last_checked'] = datetime.now()
                
                if 'excel_path' in kwargs:
                    self.active_workflows[workflow_id]['excel_path'] = kwargs['excel_path']
                if 'retry_count' in kwargs:
                    self.active_workflows[workflow_id]['retry_count'] = kwargs['retry_count']
            
            self.logger.info(f"Updated workflow {workflow_id} to status: {status}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update workflow {workflow_id}: {str(e)}")
            return False
    
    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """
        Get current workflow status and details
        
        Args:
            workflow_id: Workflow identifier
            
        Returns:
            Dictionary with workflow details or None if not found
        """
        try:
            query_sql = """
            SELECT workflow_id, csv_file_path, excel_file_path, status,
                   created_at, excel_generated_at, approved_at, completed_at,
                   error_message, user_config, processed_by, retry_count
            FROM ref.Excel_Workflow_Tracking
            WHERE workflow_id = ?
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query_sql, (workflow_id,))
                row = cursor.fetchone()
            
            if not row:
                return None
            
            # Convert row to dictionary
            columns = [desc[0] for desc in cursor.description]
            workflow_data = dict(zip(columns, row))
            
            # Parse JSON config if present
            if workflow_data.get('user_config'):
                try:
                    workflow_data['user_config'] = json.loads(workflow_data['user_config'])
                except json.JSONDecodeError:
                    pass
            
            return workflow_data
            
        except Exception as e:
            self.logger.error(f"Failed to get workflow status {workflow_id}: {str(e)}")
            return None
    
    def process_approved_file(self, workflow_id: str, excel_config: Dict[str, Any]) -> bool:
        """
        Trigger actual file processing after Excel approval
        
        Args:
            workflow_id: Workflow identifier
            excel_config: Configuration extracted from approved Excel form
            
        Returns:
            True if processing initiated successfully
        """
        try:
            # Update workflow to processing status
            self.update_status(
                workflow_id,
                self.STATES['PROCESSING'],
                user_config=excel_config,
                processed_by=excel_config.get('processed_by', 'Unknown')
            )
            
            # The actual processing will be handled by the approval monitor
            # This method just updates the workflow state
            self.logger.info(f"Workflow {workflow_id} marked for processing")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process approved file {workflow_id}: {str(e)}")
            self.update_status(workflow_id, self.STATES['ERROR'], error_message=str(e))
            return False
    
    def get_pending_workflows(self) -> List[Dict[str, Any]]:
        """
        Get all workflows that are pending Excel generation or user review
        
        Returns:
            List of workflow dictionaries
        """
        try:
            query_sql = """
            SELECT workflow_id, csv_file_path, excel_file_path, status, created_at
            FROM ref.Excel_Workflow_Tracking
            WHERE status IN (?, ?)
            ORDER BY created_at ASC
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query_sql, (
                    self.STATES['PENDING_EXCEL'],
                    self.STATES['EXCEL_GENERATED']
                ))
                rows = cursor.fetchall()
            
            # Convert rows to dictionaries
            columns = [desc[0] for desc in cursor.description]
            workflows = [dict(zip(columns, row)) for row in rows]
            
            return workflows
            
        except Exception as e:
            self.logger.error(f"Failed to get pending workflows: {str(e)}")
            return []
    
    def get_approved_workflows(self) -> List[Dict[str, Any]]:
        """
        Get all workflows that are approved and ready for processing
        
        Returns:
            List of workflow dictionaries with approved status
        """
        try:
            query_sql = """
            SELECT workflow_id, csv_file_path, excel_file_path, status, 
                   approved_at, user_config, processed_by
            FROM ref.Excel_Workflow_Tracking
            WHERE status = ?
            ORDER BY approved_at ASC
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(query_sql, (self.STATES['APPROVED'],))
                rows = cursor.fetchall()
            
            # Convert rows to dictionaries
            columns = [desc[0] for desc in cursor.description]
            workflows = []
            
            for row in rows:
                workflow_data = dict(zip(columns, row))
                
                # Parse JSON config
                if workflow_data.get('user_config'):
                    try:
                        workflow_data['user_config'] = json.loads(workflow_data['user_config'])
                    except json.JSONDecodeError:
                        workflow_data['user_config'] = {}
                
                workflows.append(workflow_data)
            
            return workflows
            
        except Exception as e:
            self.logger.error(f"Failed to get approved workflows: {str(e)}")
            return []
    
    def cleanup_completed_workflows(self, days_old: int = 7) -> int:
        """
        Clean up completed workflows older than specified days
        
        Args:
            days_old: Number of days to keep completed workflows
            
        Returns:
            Number of workflows cleaned up
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)
            
            delete_sql = """
            DELETE FROM ref.Excel_Workflow_Tracking
            WHERE status = ? AND completed_at < ?
            """
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(delete_sql, (self.STATES['COMPLETED'], cutoff_date))
                deleted_count = cursor.rowcount
                conn.commit()
            
            self.logger.info(f"Cleaned up {deleted_count} completed workflows older than {days_old} days")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup completed workflows: {str(e)}")
            return 0
    
    def retry_failed_workflow(self, workflow_id: str, max_retries: int = 3) -> bool:
        """
        Retry a failed workflow
        
        Args:
            workflow_id: Workflow to retry
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if retry was initiated
        """
        try:
            workflow = self.get_workflow_status(workflow_id)
            if not workflow:
                return False
            
            current_retries = workflow.get('retry_count', 0)
            if current_retries >= max_retries:
                self.logger.warning(f"Workflow {workflow_id} exceeded maximum retries ({max_retries})")
                return False
            
            # Reset to pending status and increment retry count
            success = self.update_status(
                workflow_id,
                self.STATES['PENDING_EXCEL'],
                retry_count=current_retries + 1,
                error_message=None
            )
            
            if success:
                self.logger.info(f"Retrying workflow {workflow_id} (attempt {current_retries + 1})")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to retry workflow {workflow_id}: {str(e)}")
            return False