"""
PDF Report Generator for Reference Data Processing
Generates detailed reports for successful and failed processing operations
"""

import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT


class PDFReportGenerator:
    """Generates PDF reports for data processing operations"""

    def __init__(self):
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()

    def _setup_custom_styles(self):
        """Setup custom paragraph styles for the reports"""
        self.styles.add(ParagraphStyle(
            name='CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=18,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=colors.HexColor('#2F5597')
        ))
        
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceBefore=20,
            spaceAfter=10,
            textColor=colors.HexColor('#4472C4')
        ))
        
        self.styles.add(ParagraphStyle(
            name='SubHeader',
            parent=self.styles['Heading3'],
            fontSize=12,
            spaceBefore=15,
            spaceAfter=8,
            textColor=colors.HexColor('#5B9BD5')
        ))

        self.styles.add(ParagraphStyle(
            name='ErrorText',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.red,
            spaceBefore=5,
            spaceAfter=5
        ))

        self.styles.add(ParagraphStyle(
            name='SuccessText',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#228B22'),
            spaceBefore=5,
            spaceAfter=5
        ))

    def generate_success_report(
        self,
        workflow_id: str,
        processing_config: Dict[str, Any],
        result: Dict[str, Any],
        output_dir: str,
        detailed_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate PDF report for successful processing
        
        Args:
            workflow_id: Unique workflow identifier
            processing_config: Configuration used for processing
            result: Processing result data
            output_dir: Directory to save the report
            
        Returns:
            Path to generated PDF report
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate report filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = Path(processing_config['csv_file_path']).stem
            report_filename = f"{csv_filename}_success_report_{timestamp}.pdf"
            report_path = os.path.join(output_dir, report_filename)

            # Create PDF document
            doc = SimpleDocTemplate(
                report_path,
                pagesize=A4,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=18
            )

            # Build report content
            story = []
            
            # Title
            title = f"Data Processing Success Report"
            story.append(Paragraph(title, self.styles['CustomTitle']))
            story.append(Spacer(1, 20))

            # Workflow Information
            story.append(Paragraph("Workflow Information", self.styles['SectionHeader']))
            workflow_data = [
                ['Workflow ID:', workflow_id],
                ['Processing Date:', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ['Status:', 'SUCCESS'],
                ['Processed By:', processing_config.get('processed_by', 'Unknown')]
            ]
            story.extend(self._create_info_table(workflow_data))
            story.append(Spacer(1, 15))

            # File Information
            story.append(Paragraph("File Information", self.styles['SectionHeader']))
            csv_path = Path(processing_config['csv_file_path'])
            file_data = [
                ['CSV File:', csv_path.name],
                ['File Path:', str(csv_path)],
                ['Table Name:', processing_config.get('table_name', 'N/A')],
                ['Target Schema:', processing_config.get('target_schema', 'ref')],
                ['Load Type:', processing_config.get('load_type', 'N/A')]
            ]
            story.extend(self._create_info_table(file_data))
            story.append(Spacer(1, 15))

            # Processing Results
            story.append(Paragraph("Processing Results", self.styles['SectionHeader']))
            result_data = [
                ['Rows Processed:', str(result.get('rows_processed', 'N/A'))],
                ['Processing Time:', result.get('processing_time', 'N/A')],
                ['Message:', result.get('message', 'Processing completed successfully')]
            ]
            story.extend(self._create_info_table(result_data))
            story.append(Spacer(1, 15))

            # Configuration Details
            story.append(Paragraph("Configuration Details", self.styles['SectionHeader']))
            config_data = [
                ['Reference Data:', 'Yes' if processing_config.get('is_reference_data') else 'No'],
                ['Delimiter:', processing_config.get('format_overrides', {}).get('delimiter', 'N/A')],
                ['Encoding:', processing_config.get('format_overrides', {}).get('encoding', 'N/A')],
                ['Has Headers:', 'Yes' if processing_config.get('format_overrides', {}).get('has_headers') else 'No']
            ]
            story.extend(self._create_info_table(config_data))
            story.append(Spacer(1, 15))

            # Add detailed information if available
            if detailed_info:
                # Table Structure
                if detailed_info.get('table_columns'):
                    story.append(Paragraph("Table Structure", self.styles['SectionHeader']))
                    story.extend(self._create_table_structure_section(detailed_info['table_columns']))
                    story.append(Spacer(1, 15))

                # Backup Information
                if detailed_info.get('backup_info'):
                    story.append(Paragraph("Data Backup Information", self.styles['SectionHeader']))
                    story.extend(self._create_backup_info_section(detailed_info['backup_info']))
                    story.append(Spacer(1, 15))

                # Validation Stored Procedure
                if detailed_info.get('validation_sp_info'):
                    story.append(Paragraph("Validation Stored Procedure", self.styles['SectionHeader']))
                    story.extend(self._create_validation_sp_section(detailed_info['validation_sp_info']))
                    story.append(Spacer(1, 15))

                # Reference Data Configuration
                if detailed_info.get('ref_data_cfg'):
                    story.append(Paragraph("Reference_Data_Cfg Updates", self.styles['SectionHeader']))
                    story.extend(self._create_ref_data_cfg_section(detailed_info['ref_data_cfg']))
                    story.append(Spacer(1, 15))

                # Sample Data
                if detailed_info.get('sample_data'):
                    story.append(Paragraph("Sample Data (First 5 Rows)", self.styles['SectionHeader']))
                    story.extend(self._create_sample_data_section(detailed_info['sample_data']))
                    story.append(Spacer(1, 15))

            # Success message
            story.append(Spacer(1, 20))
            success_msg = "✓ Data processing completed successfully. All records have been loaded into the database."
            story.append(Paragraph(success_msg, self.styles['SuccessText']))

            # Build PDF
            doc.build(story)
            return report_path

        except Exception as e:
            raise Exception(f"Failed to generate success report: {str(e)}")

    def generate_error_report(
        self,
        workflow_id: str,
        processing_config: Dict[str, Any],
        error_message: str,
        result: Optional[Dict[str, Any]] = None,
        output_dir: str = None,
        detailed_error_info: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate PDF report for failed processing
        
        Args:
            workflow_id: Unique workflow identifier
            processing_config: Configuration used for processing
            error_message: Error message describing the failure
            result: Processing result data (if any)
            output_dir: Directory to save the report
            
        Returns:
            Path to generated PDF report
        """
        try:
            if output_dir is None:
                from .config_loader import config
                file_config = config.get_file_config()
                output_dir = file_config['error_location']
                
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate report filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = Path(processing_config['csv_file_path']).stem
            report_filename = f"{csv_filename}_error_report_{timestamp}.pdf"
            report_path = os.path.join(output_dir, report_filename)

            # Create PDF document
            doc = SimpleDocTemplate(
                report_path,
                pagesize=A4,
                rightMargin=72,
                leftMargin=72,
                topMargin=72,
                bottomMargin=18
            )

            # Build report content
            story = []
            
            # Title
            title = f"Data Processing Error Report"
            story.append(Paragraph(title, self.styles['CustomTitle']))
            story.append(Spacer(1, 20))

            # Workflow Information
            story.append(Paragraph("Workflow Information", self.styles['SectionHeader']))
            workflow_data = [
                ['Workflow ID:', workflow_id],
                ['Processing Date:', datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                ['Status:', 'ERROR'],
                ['Processed By:', processing_config.get('processed_by', 'Unknown')]
            ]
            story.extend(self._create_info_table(workflow_data))
            story.append(Spacer(1, 15))

            # File Information
            story.append(Paragraph("File Information", self.styles['SectionHeader']))
            csv_path = Path(processing_config['csv_file_path'])
            file_data = [
                ['CSV File:', csv_path.name],
                ['File Path:', str(csv_path)],
                ['Table Name:', processing_config.get('table_name', 'N/A')],
                ['Target Schema:', processing_config.get('target_schema', 'ref')],
                ['Load Type:', processing_config.get('load_type', 'N/A')]
            ]
            story.extend(self._create_info_table(file_data))
            story.append(Spacer(1, 15))

            # Error Details
            story.append(Paragraph("Error Details", self.styles['SectionHeader']))
            error_data = [
                ['Error Message:', error_message],
                ['Error Time:', datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            ]
            
            if result:
                if result.get('rows_processed'):
                    error_data.append(['Rows Processed Before Error:', str(result['rows_processed'])])
                if result.get('processing_time'):
                    error_data.append(['Processing Time:', result['processing_time']])
                    
            story.extend(self._create_info_table(error_data))
            story.append(Spacer(1, 15))

            # Configuration Details
            story.append(Paragraph("Configuration Details", self.styles['SectionHeader']))
            config_data = [
                ['Reference Data:', 'Yes' if processing_config.get('is_reference_data') else 'No'],
                ['Delimiter:', processing_config.get('format_overrides', {}).get('delimiter', 'N/A')],
                ['Encoding:', processing_config.get('format_overrides', {}).get('encoding', 'N/A')],
                ['Has Headers:', 'Yes' if processing_config.get('format_overrides', {}).get('has_headers') else 'No']
            ]
            story.extend(self._create_info_table(config_data))

            # Add detailed error information if available
            if detailed_error_info:
                # Database Error Details
                if detailed_error_info.get('database_errors'):
                    story.append(Paragraph("Database Error Details", self.styles['SectionHeader']))
                    story.extend(self._create_database_error_section(detailed_error_info['database_errors']))
                    story.append(Spacer(1, 15))

                # File Processing Errors
                if detailed_error_info.get('file_errors'):
                    story.append(Paragraph("File Processing Errors", self.styles['SectionHeader']))
                    story.extend(self._create_file_error_section(detailed_error_info['file_errors']))
                    story.append(Spacer(1, 15))

                # Validation Errors
                if detailed_error_info.get('validation_errors'):
                    story.append(Paragraph("Data Validation Errors", self.styles['SectionHeader']))
                    story.extend(self._create_validation_error_section(detailed_error_info['validation_errors']))
                    story.append(Spacer(1, 15))

                # Stack Trace
                if detailed_error_info.get('stack_trace'):
                    story.append(Paragraph("Technical Details", self.styles['SectionHeader']))
                    story.append(Paragraph("Stack Trace:", self.styles['SubHeader']))
                    stack_trace = detailed_error_info['stack_trace']
                    if len(stack_trace) > 2000:  # Truncate very long stack traces
                        stack_trace = stack_trace[:2000] + "\n... (truncated)"
                    story.append(Paragraph(f"<pre>{stack_trace}</pre>", self.styles['Normal']))
                    story.append(Spacer(1, 15))

            # Troubleshooting Section
            story.append(Paragraph("Troubleshooting Steps", self.styles['SectionHeader']))
            
            troubleshooting_steps = self._get_troubleshooting_steps(error_message)
            for i, step in enumerate(troubleshooting_steps, 1):
                story.append(Paragraph(f"{i}. {step}", self.styles['Normal']))
                story.append(Spacer(1, 5))

            # Error message highlight
            story.append(Spacer(1, 20))
            error_msg = f"✗ Processing failed: {error_message}"
            story.append(Paragraph(error_msg, self.styles['ErrorText']))

            # Build PDF
            doc.build(story)
            return report_path

        except Exception as e:
            raise Exception(f"Failed to generate error report: {str(e)}")

    def _create_info_table(self, data: List[List[str]]) -> List:
        """Create a formatted table for information display"""
        table = Table(data, colWidths=[2*inch, 4*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#F2F2F2')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        
        return [table, Spacer(1, 10)]

    def _get_troubleshooting_steps(self, error_message: str) -> List[str]:
        """Get relevant troubleshooting steps based on error message"""
        steps = []
        
        error_lower = error_message.lower()
        
        if 'connection' in error_lower or 'database' in error_lower:
            steps.extend([
                "Check database connection settings in config.yaml",
                "Verify database server is running and accessible",
                "Confirm database credentials are correct"
            ])
        
        if 'permission' in error_lower or 'access' in error_lower:
            steps.extend([
                "Verify database user has necessary permissions",
                "Check schema access rights",
                "Confirm table creation/modification permissions"
            ])
        
        if 'column' in error_lower or 'field' in error_lower:
            steps.extend([
                "Review CSV file structure and column names",
                "Check for extra or missing columns in CSV",
                "Verify data types match expected schema"
            ])
        
        if 'duplicate' in error_lower or 'constraint' in error_lower:
            steps.extend([
                "Check for duplicate keys in the data",
                "Review referential integrity constraints",
                "Consider using 'append' mode instead of 'fullload'"
            ])
        
        if 'encoding' in error_lower or 'character' in error_lower:
            steps.extend([
                "Try different file encoding (UTF-8, UTF-16, etc.)",
                "Check for special characters in the data",
                "Review delimiter and text qualifier settings"
            ])
        
        # Default troubleshooting steps
        if not steps:
            steps = [
                "Review the error message for specific details",
                "Check the CSV file format and structure",
                "Verify database connection and permissions",
                "Review configuration settings in the Excel form",
                "Contact system administrator if the issue persists"
            ]
        
        return steps

    def _create_table_structure_section(self, table_columns: List[Dict[str, Any]]) -> List:
        """Create table structure section showing columns and data types"""
        if not table_columns:
            return [Paragraph("No column information available", self.styles['Normal'])]

        # Create table data
        table_data = [['Column Name', 'Data Type', 'Length', 'Nullable']]
        
        for col in table_columns:
            length = ''
            if col.get('max_length'):
                length = str(col['max_length'])
            elif col.get('numeric_precision'):
                length = f"{col['numeric_precision']}"
                if col.get('numeric_scale'):
                    length += f",{col['numeric_scale']}"
                    
            nullable = 'Yes' if col.get('nullable') else 'No'
            
            table_data.append([
                col.get('name', ''),
                col.get('data_type', ''),
                length,
                nullable
            ])

        table = Table(table_data, colWidths=[1.5*inch, 1.5*inch, 1*inch, 0.8*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472C4')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('ALTERNATEROWCOLORS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F8F8')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))

        return [table, Spacer(1, 10)]

    def _create_backup_info_section(self, backup_info: Dict[str, Any]) -> List:
        """Create backup information section"""
        backup_data = []
        
        if backup_info.get('backup_created'):
            backup_data.append(['Backup Created:', 'Yes'])
            if backup_info.get('backup_table_name'):
                backup_data.append(['Backup Table:', backup_info['backup_table_name']])
            if backup_info.get('backup_schema'):
                backup_data.append(['Backup Schema:', backup_info['backup_schema']])
            if backup_info.get('backup_rows'):
                backup_data.append(['Rows Backed Up:', str(backup_info['backup_rows'])])
            if backup_info.get('backup_timestamp'):
                backup_data.append(['Backup Timestamp:', backup_info['backup_timestamp']])
        else:
            backup_data.append(['Backup Created:', 'No'])
            if backup_info.get('reason'):
                backup_data.append(['Reason:', backup_info['reason']])

        return self._create_info_table(backup_data)

    def _create_validation_sp_section(self, validation_sp_info: Dict[str, Any]) -> List:
        """Create validation stored procedure section"""
        sp_data = []
        
        if validation_sp_info.get('database'):
            sp_data.append(['Database:', validation_sp_info['database']])
        if validation_sp_info.get('schema'):
            sp_data.append(['Schema:', validation_sp_info['schema']])
        if validation_sp_info.get('procedure_name'):
            sp_data.append(['Procedure Name:', validation_sp_info['procedure_name']])
        if validation_sp_info.get('exists'):
            sp_data.append(['Exists:', 'Yes' if validation_sp_info['exists'] else 'No'])
        if validation_sp_info.get('executed'):
            sp_data.append(['Executed:', 'Yes' if validation_sp_info['executed'] else 'No'])
        if validation_sp_info.get('result'):
            sp_data.append(['Validation Result:', validation_sp_info['result']])

        return self._create_info_table(sp_data)

    def _create_ref_data_cfg_section(self, ref_data_cfg: Dict[str, Any]) -> List:
        """Create Reference_Data_Cfg section"""
        if not ref_data_cfg.get('updated'):
            return [Paragraph("No updates made to Reference_Data_Cfg table", self.styles['Normal'])]

        elements = []
        elements.append(Paragraph("Configuration record was updated/created:", self.styles['Normal']))
        elements.append(Spacer(1, 8))

        cfg_data = []
        if ref_data_cfg.get('table_name'):
            cfg_data.append(['Table Name:', ref_data_cfg['table_name']])
        if ref_data_cfg.get('schema_name'):
            cfg_data.append(['Schema:', ref_data_cfg['schema_name']])
        if ref_data_cfg.get('description'):
            cfg_data.append(['Description:', ref_data_cfg['description']])
        if ref_data_cfg.get('load_frequency'):
            cfg_data.append(['Load Frequency:', ref_data_cfg['load_frequency']])
        if ref_data_cfg.get('source_system'):
            cfg_data.append(['Source System:', ref_data_cfg['source_system']])
        if ref_data_cfg.get('business_owner'):
            cfg_data.append(['Business Owner:', ref_data_cfg['business_owner']])
        if ref_data_cfg.get('technical_owner'):
            cfg_data.append(['Technical Owner:', ref_data_cfg['technical_owner']])
        if ref_data_cfg.get('last_updated'):
            cfg_data.append(['Last Updated:', ref_data_cfg['last_updated']])

        elements.extend(self._create_info_table(cfg_data))
        return elements

    def _create_sample_data_section(self, sample_data: List[Dict[str, Any]]) -> List:
        """Create sample data section"""
        if not sample_data:
            return [Paragraph("No sample data available", self.styles['Normal'])]

        # Get column headers
        if not sample_data[0]:
            return [Paragraph("No sample data available", self.styles['Normal'])]

        headers = list(sample_data[0].keys())
        
        # Limit columns to fit on page (max 6 columns)
        if len(headers) > 6:
            headers = headers[:6]
            
        table_data = [headers]
        
        # Add sample rows (up to 5)
        for i, row in enumerate(sample_data[:5]):
            row_data = []
            for header in headers:
                value = str(row.get(header, ''))
                # Truncate long values
                if len(value) > 30:
                    value = value[:27] + "..."
                row_data.append(value)
            table_data.append(row_data)

        # Calculate column widths
        col_width = 6.5 / len(headers)
        col_widths = [col_width * inch] * len(headers)

        table = Table(table_data, colWidths=col_widths)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4472C4')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#CCCCCC')),
            ('ALTERNATEROWCOLORS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F8F8')]),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))

        elements = [table, Spacer(1, 5)]
        
        if len(sample_data[0].keys()) > 6:
            elements.append(Paragraph("Note: Only first 6 columns shown due to space constraints", 
                                    self.styles['Normal']))
            
        return elements

    def _create_database_error_section(self, database_errors: List[Dict[str, Any]]) -> List:
        """Create database error details section"""
        elements = []
        
        for i, error in enumerate(database_errors, 1):
            elements.append(Paragraph(f"Error {i}:", self.styles['SubHeader']))
            
            error_data = []
            if error.get('error_code'):
                error_data.append(['Error Code:', str(error['error_code'])])
            if error.get('severity'):
                error_data.append(['Severity:', str(error['severity'])])
            if error.get('state'):
                error_data.append(['State:', str(error['state'])])
            if error.get('procedure'):
                error_data.append(['Procedure:', error['procedure']])
            if error.get('line'):
                error_data.append(['Line Number:', str(error['line'])])
            if error.get('message'):
                error_data.append(['Message:', error['message']])
            
            elements.extend(self._create_info_table(error_data))
            elements.append(Spacer(1, 10))
            
        return elements

    def _create_file_error_section(self, file_errors: List[Dict[str, Any]]) -> List:
        """Create file processing error details section"""
        elements = []
        
        for i, error in enumerate(file_errors, 1):
            elements.append(Paragraph(f"File Error {i}:", self.styles['SubHeader']))
            
            error_data = []
            if error.get('file_path'):
                error_data.append(['File Path:', error['file_path']])
            if error.get('row_number'):
                error_data.append(['Row Number:', str(error['row_number'])])
            if error.get('column_name'):
                error_data.append(['Column:', error['column_name']])
            if error.get('expected_type'):
                error_data.append(['Expected Type:', error['expected_type']])
            if error.get('actual_value'):
                error_data.append(['Actual Value:', str(error['actual_value'])])
            if error.get('error_description'):
                error_data.append(['Description:', error['error_description']])
            
            elements.extend(self._create_info_table(error_data))
            elements.append(Spacer(1, 10))
            
        return elements

    def _create_validation_error_section(self, validation_errors: List[Dict[str, Any]]) -> List:
        """Create validation error details section"""
        elements = []
        
        for i, error in enumerate(validation_errors, 1):
            elements.append(Paragraph(f"Validation Error {i}:", self.styles['SubHeader']))
            
            error_data = []
            if error.get('rule_name'):
                error_data.append(['Rule Name:', error['rule_name']])
            if error.get('table_name'):
                error_data.append(['Table:', error['table_name']])
            if error.get('column_name'):
                error_data.append(['Column:', error['column_name']])
            if error.get('constraint_type'):
                error_data.append(['Constraint Type:', error['constraint_type']])
            if error.get('failed_value'):
                error_data.append(['Failed Value:', str(error['failed_value'])])
            if error.get('error_description'):
                error_data.append(['Description:', error['error_description']])
            
            elements.extend(self._create_info_table(error_data))
            elements.append(Spacer(1, 10))
            
        return elements