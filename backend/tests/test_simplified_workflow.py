"""
Integration tests for Simplified Dropoff Workflow
Tests the complete end-to-end workflow from CSV detection to processing completion
"""

import unittest
import os
import tempfile
import time
import json
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Import components to test
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from utils.workflow_manager import WorkflowManager
    from utils.excel_generator import ExcelFormGenerator
    from utils.excel_processor import ExcelProcessor
    from utils.csv_detector import CSVFormatDetector
    from utils.database import DatabaseManager
    from simplified_file_monitor import SimplifiedFileMonitor
    from excel_approval_monitor import ExcelApprovalMonitor
except ImportError as e:
    print(f"Warning: Could not import all modules for integration tests: {e}")

class TestSimplifiedWorkflowIntegration(unittest.TestCase):
    """Integration tests for the complete simplified workflow"""
    
    def setUp(self):
        """Set up integration test fixtures"""
        # Create temporary directory structure
        self.test_dir = tempfile.mkdtemp()
        self.dropoff_dir = os.path.join(self.test_dir, "dropoff", "simplified")
        self.processed_dir = os.path.join(self.dropoff_dir, "processed")
        
        # Create directories
        os.makedirs(self.dropoff_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        
        # Create test CSV files
        self.create_test_csv_files()
        
        # Mock database manager to avoid actual database operations
        self.mock_db_manager = Mock(spec=DatabaseManager)
        self.mock_db_manager.get_connection.return_value.__enter__ = Mock()
        self.mock_db_manager.get_connection.return_value.__exit__ = Mock()
        
        # Mock logger
        self.mock_logger = Mock()
    
    def tearDown(self):
        """Clean up integration test fixtures"""
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def create_test_csv_files(self):
        """Create realistic test CSV files"""
        # Simple CSV file
        self.simple_csv = os.path.join(self.dropoff_dir, "simple_data.csv")
        with open(self.simple_csv, 'w') as f:
            f.write("id,name,value\n1,Test Item,100\n2,Sample Item,200\n")
        
        # Complex CSV file with different delimiters
        self.complex_csv = os.path.join(self.dropoff_dir, "complex_data.csv")
        with open(self.complex_csv, 'w') as f:
            f.write("id;description;category;price;date\n")
            f.write("1;Product A;Electronics;299.99;2023-01-01\n")
            f.write("2;Product B;Clothing;49.99;2023-01-02\n")
        
        # Large CSV file for performance testing
        self.large_csv = os.path.join(self.dropoff_dir, "large_data.csv")
        with open(self.large_csv, 'w') as f:
            f.write("id,data,timestamp,value\n")
            for i in range(1000):
                f.write(f"{i},Data item {i},2023-01-01 10:{i%60:02d}:00,{i * 10}\n")
    
    @unittest.skipIf('WorkflowManager' not in globals(), "WorkflowManager not available")
    def test_workflow_manager_lifecycle(self):
        """Test complete workflow manager lifecycle"""
        workflow_manager = WorkflowManager(self.mock_db_manager, self.mock_logger)
        
        # Mock database operations
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        self.mock_db_manager.get_connection.return_value.__enter__.return_value = mock_conn
        
        # Test workflow creation
        workflow_id = workflow_manager.create_workflow(self.simple_csv)
        self.assertIsNotNone(workflow_id)
        
        # Test status updates
        success = workflow_manager.update_status(workflow_id, 'pdf_generated', excel_path='/test/path.xlsx')
        self.assertTrue(success)
        
        success = workflow_manager.update_status(workflow_id, 'approved')
        self.assertTrue(success)
        
        success = workflow_manager.update_status(workflow_id, 'completed')
        self.assertTrue(success)
    
    @unittest.skipIf('ExcelFormGenerator' not in globals(), "ExcelFormGenerator not available")
    def test_excel_form_generation_workflow(self):
        """Test Excel form generation with real CSV data"""
        try:
            excel_generator = ExcelFormGenerator(self.mock_logger)
            csv_detector = CSVFormatDetector()
            
            # Detect format of simple CSV
            format_data = csv_detector.detect_format(self.simple_csv)
            self.assertIsNotNone(format_data)
            
            # Generate Excel form
            excel_path = excel_generator.generate_form(self.simple_csv, format_data)
            
            # Verify Excel was created
            self.assertTrue(os.path.exists(excel_path))
            self.assertTrue(excel_path.endswith('.xlsx'))
            
            # Verify file size is reasonable
            file_size = os.path.getsize(excel_path)
            self.assertGreater(file_size, 1000)  # At least 1KB
            
        except ImportError:
            self.skipTest("PDF generation dependencies not available")
    
    @unittest.skipIf('PDFProcessor' not in globals(), "PDFProcessor not available")
    def test_pdf_processing_workflow(self):
        """Test PDF processing and validation workflow"""
        pdf_processor = PDFProcessor(self.mock_logger)
        
        # Test configuration validation
        valid_config = {
            'delimiter': ',',
            'encoding': 'utf-8',
            'processing_mode': 'fullload',
            'is_reference_data': True,
            'table_name': 'test_table',
            'confirmed': True
        }
        
        is_valid, errors = pdf_processor._validate_configuration(valid_config)
        self.assertTrue(is_valid)
        self.assertEqual(len(errors), 0)
        
        # Test invalid configuration
        invalid_config = valid_config.copy()
        invalid_config['confirmed'] = False
        
        is_valid, errors = pdf_processor._validate_configuration(invalid_config)
        self.assertFalse(is_valid)
        self.assertGreater(len(errors), 0)
    
    def test_csv_detection_accuracy(self):
        """Test CSV detection accuracy across different file types"""
        if 'CSVFormatDetector' not in globals():
            self.skipTest("CSVFormatDetector not available")
            
        csv_detector = CSVFormatDetector()
        
        # Test simple comma-delimited file
        format_data = csv_detector.detect_format(self.simple_csv)
        self.assertEqual(format_data.get('delimiter'), ',')
        self.assertTrue(format_data.get('has_headers'))
        
        # Test semicolon-delimited file
        format_data = csv_detector.detect_format(self.complex_csv)
        self.assertEqual(format_data.get('delimiter'), ';')
        
        # Test large file detection performance
        start_time = time.time()
        format_data = csv_detector.detect_format(self.large_csv)
        detection_time = time.time() - start_time
        
        self.assertLess(detection_time, 5.0)  # Should complete within 5 seconds
        self.assertIsNotNone(format_data.get('row_count'))
    
    @patch('utils.database.DatabaseManager')
    def test_file_monitor_integration(self, mock_db_class):
        """Test simplified file monitor integration"""
        if 'SimplifiedFileMonitor' not in globals():
            self.skipTest("SimplifiedFileMonitor not available")
        
        # Mock database manager
        mock_db_class.return_value = self.mock_db_manager
        
        # Patch the dropoff path to use our test directory
        with patch('simplified_file_monitor.SIMPLIFIED_DROPOFF_PATH', self.dropoff_dir):
            monitor = SimplifiedFileMonitor()
            
            # Test file stability checking
            is_stable = monitor.is_file_stable(self.simple_csv)
            self.assertFalse(is_stable)  # First check should return False
            
            # Simulate multiple stability checks
            for _ in range(6):  # STABILITY_CHECKS = 6
                monitor.is_file_stable(self.simple_csv)
            
            is_stable = monitor.is_file_stable(self.simple_csv)
            self.assertTrue(is_stable)  # Should be stable after 6 checks
    
    def test_end_to_end_workflow_simulation(self):
        """Simulate complete end-to-end workflow without actual processing"""
        # This test simulates the workflow steps without requiring all dependencies
        
        workflow_states = [
            'pending_pdf',
            'pdf_generated',
            'user_reviewing',
            'approved', 
            'processing',
            'completed'
        ]
        
        # Simulate workflow progression
        current_state = 'pending_pdf'
        workflow_data = {
            'workflow_id': 'test_workflow_123',
            'csv_file_path': self.simple_csv,
            'status': current_state
        }
        
        # Simulate PDF generation
        self.assertEqual(current_state, 'pending_pdf')
        current_state = 'pdf_generated'
        workflow_data['status'] = current_state
        workflow_data['pdf_file_path'] = self.simple_csv.replace('.csv', '_config.pdf')
        
        # Simulate user review and approval
        current_state = 'approved'
        workflow_data['status'] = current_state
        workflow_data['user_config'] = {
            'processing_mode': 'fullload',
            'is_reference_data': True,
            'confirmed': True
        }
        
        # Simulate processing
        current_state = 'processing'
        workflow_data['status'] = current_state
        
        # Simulate completion
        current_state = 'completed'
        workflow_data['status'] = current_state
        workflow_data['completed_at'] = time.time()
        
        # Verify final state
        self.assertEqual(workflow_data['status'], 'completed')
        self.assertIn('user_config', workflow_data)
        self.assertTrue(workflow_data['user_config']['confirmed'])
    
    def test_error_handling_workflow(self):
        """Test error handling throughout the workflow"""
        # Test with non-existent CSV file
        non_existent_csv = os.path.join(self.dropoff_dir, "missing.csv")
        
        if 'CSVFormatDetector' in globals():
            csv_detector = CSVFormatDetector()
            
            with self.assertRaises(Exception):
                csv_detector.detect_format(non_existent_csv)
        
        # Test with invalid PDF path
        if 'PDFProcessor' in globals():
            pdf_processor = PDFProcessor(self.mock_logger)
            
            is_valid, config, errors = pdf_processor.validate_form("/invalid/path.pdf")
            self.assertFalse(is_valid)
            self.assertGreater(len(errors), 0)
    
    def test_concurrent_workflow_handling(self):
        """Test handling of multiple concurrent workflows"""
        if 'WorkflowManager' not in globals():
            self.skipTest("WorkflowManager not available")
        
        workflow_manager = WorkflowManager(self.mock_db_manager, self.mock_logger)
        
        # Mock database operations for multiple workflows
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        self.mock_db_manager.get_connection.return_value.__enter__.return_value = mock_conn
        
        # Create multiple workflows
        workflow_ids = []
        csv_files = [self.simple_csv, self.complex_csv, self.large_csv]
        
        for csv_file in csv_files:
            workflow_id = workflow_manager.create_workflow(csv_file)
            workflow_ids.append(workflow_id)
        
        # Verify all workflows were created
        self.assertEqual(len(workflow_ids), 3)
        self.assertTrue(all(wid is not None for wid in workflow_ids))
        
        # Test updating different workflows to different states
        for i, workflow_id in enumerate(workflow_ids):
            if i == 0:
                workflow_manager.update_status(workflow_id, 'pdf_generated')
            elif i == 1:
                workflow_manager.update_status(workflow_id, 'approved')
            else:
                workflow_manager.update_status(workflow_id, 'completed')


class TestWorkflowPerformance(unittest.TestCase):
    """Performance tests for workflow components"""
    
    def setUp(self):
        """Set up performance test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.performance_csv = os.path.join(self.test_dir, "performance_test.csv")
        
        # Create a larger CSV file for performance testing
        with open(self.performance_csv, 'w') as f:
            f.write("id,category,description,price,date,status\n")
            for i in range(10000):  # 10,000 rows
                f.write(f"{i},Category{i%10},Description for item {i},{i*0.99},2023-{(i%12)+1:02d}-01,active\n")
    
    def tearDown(self):
        """Clean up performance test fixtures"""
        import shutil
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_csv_detection_performance(self):
        """Test CSV detection performance with large files"""
        if 'CSVFormatDetector' not in globals():
            self.skipTest("CSVFormatDetector not available")
            
        csv_detector = CSVFormatDetector()
        
        # Test detection time
        start_time = time.time()
        format_data = csv_detector.detect_format(self.performance_csv)
        detection_time = time.time() - start_time
        
        # Should complete within 15 seconds (requirement)
        self.assertLess(detection_time, 15.0)
        
        # Verify detection accuracy
        self.assertIsNotNone(format_data)
        self.assertEqual(format_data.get('delimiter'), ',')
        self.assertTrue(format_data.get('has_headers'))
        
        print(f"CSV detection completed in {detection_time:.2f} seconds for 10k rows")
    
    @unittest.skipIf('ExcelFormGenerator' not in globals(), "ExcelFormGenerator not available")
    def test_pdf_generation_performance(self):
        """Test PDF generation performance"""
        try:
            pdf_generator = PDFFormGenerator()
            csv_detector = CSVFormatDetector()
            
            # Detect format first
            format_data = csv_detector.detect_format(self.performance_csv)
            
            # Test PDF generation time
            start_time = time.time()
            pdf_path = pdf_generator.generate_form(self.performance_csv, format_data)
            generation_time = time.time() - start_time
            
            # Should complete within 30 seconds (requirement)
            self.assertLess(generation_time, 30.0)
            
            # Verify PDF was created and has reasonable size
            self.assertTrue(os.path.exists(pdf_path))
            file_size = os.path.getsize(excel_path)
            self.assertGreater(file_size, 5000)  # At least 5KB
            self.assertLess(file_size, 5000000)  # Less than 5MB
            
            print(f"PDF generation completed in {generation_time:.2f} seconds")
            
        except ImportError:
            self.skipTest("PDF generation dependencies not available")
    
    def test_workflow_database_operations_performance(self):
        """Test database operations performance"""
        if 'WorkflowManager' not in globals():
            self.skipTest("WorkflowManager not available")
        
        # Mock database operations with timing
        mock_db_manager = Mock()
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_db_manager.get_connection.return_value.__enter__.return_value = mock_conn
        
        workflow_manager = WorkflowManager(mock_db_manager)
        
        # Test multiple rapid workflow operations
        start_time = time.time()
        
        for i in range(100):  # Create 100 workflows rapidly
            workflow_manager.create_workflow(f"/test/file_{i}.csv")
        
        batch_time = time.time() - start_time
        
        # Should handle 100 operations quickly
        self.assertLess(batch_time, 5.0)  # Within 5 seconds
        
        print(f"100 workflow operations completed in {batch_time:.2f} seconds")


if __name__ == '__main__':
    # Configure test runner for integration tests
    unittest.main(verbosity=2, buffer=True)