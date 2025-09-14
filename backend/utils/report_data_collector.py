"""
Report Data Collector for PDF Reports
Collects detailed information about processing operations for comprehensive reporting
"""

import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from .database import DatabaseManager
from .config_loader import config


class ReportDataCollector:
    """Collects detailed data for comprehensive PDF reports"""

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

    def collect_success_details(
        self,
        table_name: str,
        schema_name: str,
        csv_file_path: str,
        processing_config: Dict[str, Any],
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Collect detailed information for successful processing reports
        
        Returns comprehensive data including table structure, backup info, 
        validation details, reference data config, and sample data
        """
        detailed_info = {}

        try:
            with self.db_manager.get_connection() as connection:
                # 1. Table Structure Information
                detailed_info['table_columns'] = self._get_table_structure(
                    connection, table_name, schema_name
                )

                # 2. Backup Information
                detailed_info['backup_info'] = self._get_backup_information(
                    connection, table_name, schema_name, result
                )

                # 3. Validation Stored Procedure Information
                detailed_info['validation_sp_info'] = self._get_validation_sp_info(
                    connection, table_name, schema_name
                )

                # 4. Reference Data Configuration
                if processing_config.get('is_reference_data'):
                    detailed_info['ref_data_cfg'] = self._get_reference_data_config(
                        connection, table_name, schema_name
                    )

                # 5. Sample Data
                detailed_info['sample_data'] = self._get_sample_data(
                    connection, table_name, schema_name
                )

        except Exception as e:
            # Log error but don't fail report generation
            print(f"Error collecting detailed report data: {str(e)}")

        return detailed_info

    def collect_error_details(
        self,
        error_message: str,
        processing_config: Dict[str, Any],
        exception: Optional[Exception] = None
    ) -> Dict[str, Any]:
        """
        Collect detailed error information for failed processing reports
        
        Returns comprehensive error data including database errors,
        file errors, validation errors, and stack traces
        """
        detailed_error_info = {}

        try:
            # 1. Parse and categorize error message
            detailed_error_info['database_errors'] = self._parse_database_errors(error_message)
            detailed_error_info['file_errors'] = self._parse_file_errors(error_message)
            detailed_error_info['validation_errors'] = self._parse_validation_errors(error_message)

            # 2. Stack trace if available
            if exception:
                import traceback
                detailed_error_info['stack_trace'] = traceback.format_exc()

            # 3. File analysis if possible
            csv_file_path = processing_config.get('csv_file_path')
            if csv_file_path and Path(csv_file_path).exists():
                detailed_error_info['file_analysis'] = self._analyze_problematic_file(csv_file_path)

        except Exception as e:
            print(f"Error collecting detailed error data: {str(e)}")

        return detailed_error_info

    def _get_table_structure(self, connection, table_name: str, schema_name: str) -> List[Dict[str, Any]]:
        """Get table column structure and data types"""
        try:
            return self.db_manager.get_table_columns(connection, table_name, schema_name)
        except Exception:
            return []

    def _get_backup_information(
        self, 
        connection, 
        table_name: str, 
        schema_name: str, 
        result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Get backup table information"""
        backup_info = {
            'backup_created': False,
            'reason': 'No backup information available'
        }

        try:
            # Check if backup table exists
            backup_table_name = f"{table_name}_backup"
            backup_schema = config.get('backup_schema', 'bkp', 'schemas')
            
            if self.db_manager.table_exists(connection, backup_table_name, backup_schema):
                backup_info['backup_created'] = True
                backup_info['backup_table_name'] = backup_table_name
                backup_info['backup_schema'] = backup_schema
                backup_info['backup_timestamp'] = datetime.now().isoformat()
                
                # Get backup row count
                try:
                    backup_info['backup_rows'] = self.db_manager.get_row_count(
                        connection, backup_table_name, backup_schema
                    )
                except Exception:
                    backup_info['backup_rows'] = 'Unknown'
            else:
                backup_info['reason'] = 'Backup table not found or not created'

        except Exception as e:
            backup_info['reason'] = f"Error checking backup: {str(e)}"

        return backup_info

    def _get_validation_sp_info(
        self, 
        connection, 
        table_name: str, 
        schema_name: str
    ) -> Dict[str, Any]:
        """Get validation stored procedure information"""
        validation_sp_info = {}

        try:
            # Get validation SP configuration
            validation_sp_schema = config.get('validation_sp_schema', 'ref', 'schemas')
            staff_database = config.get('staff_database', 'StaffDatabase')
            
            validation_sp_info['database'] = staff_database
            validation_sp_info['schema'] = validation_sp_schema
            validation_sp_info['procedure_name'] = f"sp_{table_name}_validation"
            
            # Check if validation SP exists
            cursor = connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM INFORMATION_SCHEMA.ROUTINES
                WHERE ROUTINE_CATALOG = ? AND ROUTINE_SCHEMA = ? AND ROUTINE_NAME = ?
            """, staff_database, validation_sp_schema, f"sp_{table_name}_validation")
            
            exists = cursor.fetchone()[0] > 0
            validation_sp_info['exists'] = exists
            validation_sp_info['executed'] = False  # This would need to be tracked during processing
            validation_sp_info['result'] = 'Not executed' if not exists else 'Unknown'

        except Exception as e:
            validation_sp_info['error'] = str(e)

        return validation_sp_info

    def _get_reference_data_config(
        self, 
        connection, 
        table_name: str, 
        schema_name: str
    ) -> Dict[str, Any]:
        """Get Reference_Data_Cfg table information"""
        ref_data_cfg = {'updated': False}

        try:
            staff_database = config.get('staff_database', 'StaffDatabase')
            
            cursor = connection.cursor()
            cursor.execute(f"""
                SELECT 
                    Table_Name, Schema_Name, Description, Load_Frequency,
                    Source_System, Business_Owner, Technical_Owner, 
                    Last_Updated_Date
                FROM [{staff_database}].dbo.Reference_Data_Cfg
                WHERE Table_Name = ? AND Schema_Name = ?
            """, table_name, schema_name)
            
            result = cursor.fetchone()
            if result:
                ref_data_cfg['updated'] = True
                ref_data_cfg['table_name'] = result[0]
                ref_data_cfg['schema_name'] = result[1]
                ref_data_cfg['description'] = result[2]
                ref_data_cfg['load_frequency'] = result[3]
                ref_data_cfg['source_system'] = result[4]
                ref_data_cfg['business_owner'] = result[5]
                ref_data_cfg['technical_owner'] = result[6]
                ref_data_cfg['last_updated'] = result[7].isoformat() if result[7] else 'Unknown'

        except Exception as e:
            ref_data_cfg['error'] = str(e)

        return ref_data_cfg

    def _get_sample_data(
        self, 
        connection, 
        table_name: str, 
        schema_name: str
    ) -> List[Dict[str, Any]]:
        """Get sample data from the processed table"""
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT TOP 5 * FROM [{schema_name}].[{table_name}]")
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Get sample rows
            sample_data = []
            for row in cursor.fetchall():
                row_dict = {}
                for i, value in enumerate(row):
                    if isinstance(value, (bytes, bytearray)):
                        row_dict[columns[i]] = '<binary data>'
                    elif value is None:
                        row_dict[columns[i]] = '<NULL>'
                    else:
                        row_dict[columns[i]] = str(value)
                sample_data.append(row_dict)

            return sample_data

        except Exception:
            return []

    def _parse_database_errors(self, error_message: str) -> List[Dict[str, Any]]:
        """Parse database-specific errors from error message"""
        database_errors = []
        
        # Look for SQL Server error patterns
        import re
        
        # SQL Server error pattern: [Error Code] [Severity] [State] [Procedure] [Line] Message
        sql_error_pattern = r'.*?(\d{4,5}).*?Level (\d+).*?State (\d+).*?Line (\d+)'
        
        if 'Msg ' in error_message or 'Error ' in error_message:
            # This looks like a SQL Server error
            database_errors.append({
                'error_code': 'Unknown',
                'severity': 'Unknown',
                'state': 'Unknown',
                'message': error_message,
                'procedure': 'Unknown',
                'line': 'Unknown'
            })

        return database_errors

    def _parse_file_errors(self, error_message: str) -> List[Dict[str, Any]]:
        """Parse file processing errors from error message"""
        file_errors = []
        
        if any(keyword in error_message.lower() for keyword in ['file', 'csv', 'encoding', 'delimiter']):
            file_errors.append({
                'error_description': error_message,
                'file_path': 'Unknown',
                'row_number': 'Unknown',
                'column_name': 'Unknown'
            })

        return file_errors

    def _parse_validation_errors(self, error_message: str) -> List[Dict[str, Any]]:
        """Parse data validation errors from error message"""
        validation_errors = []
        
        if any(keyword in error_message.lower() for keyword in ['constraint', 'validation', 'duplicate', 'foreign key']):
            validation_errors.append({
                'rule_name': 'Unknown',
                'error_description': error_message,
                'constraint_type': 'Unknown'
            })

        return validation_errors

    def _analyze_problematic_file(self, csv_file_path: str) -> Dict[str, Any]:
        """Analyze the CSV file for potential issues"""
        analysis = {}
        
        try:
            file_path = Path(csv_file_path)
            if not file_path.exists():
                return {'error': 'File not found'}

            # Basic file information
            analysis['file_size'] = file_path.stat().st_size
            analysis['file_modified'] = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

            # Try to read a small sample to detect issues
            try:
                sample_df = pd.read_csv(csv_file_path, nrows=10)
                analysis['detected_columns'] = len(sample_df.columns)
                analysis['sample_rows'] = len(sample_df)
                analysis['column_names'] = list(sample_df.columns)
            except Exception as e:
                analysis['read_error'] = str(e)

        except Exception as e:
            analysis['analysis_error'] = str(e)

        return analysis