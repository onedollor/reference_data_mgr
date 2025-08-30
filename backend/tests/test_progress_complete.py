"""
Complete test coverage for utils/progress.py to achieve >90% coverage
Covers all functions: init_progress, update_progress, get_progress, mark_error, mark_done, mark_canceled, request_cancel, is_canceled
"""

import pytest
from unittest.mock import patch
import utils.progress as progress


class TestProgressComplete:
    """Comprehensive test coverage for all progress functions"""
    
    def setup_method(self):
        """Setup test environment - clear progress state"""
        progress._progress.clear()
        progress._cancel_flags.clear()
    
    def test_init_progress_basic(self):
        """Test basic progress initialization"""
        progress.init_progress("test_key")
        
        assert "test_key" in progress._progress
        assert progress._progress["test_key"]["inserted"] == 0
        assert progress._progress["test_key"]["total"] is None
        assert progress._progress["test_key"]["percent"] == 0.0
        assert progress._progress["test_key"]["stage"] == "starting"
        assert progress._progress["test_key"]["done"] is False
        assert progress._progress["test_key"]["error"] is None
        assert progress._progress["test_key"]["canceled"] is False
        assert progress._cancel_flags["test_key"] is False
    
    def test_init_progress_multiple_keys(self):
        """Test initializing multiple progress keys"""
        progress.init_progress("key1")
        progress.init_progress("key2")
        progress.init_progress("key3")
        
        assert len(progress._progress) == 3
        assert len(progress._cancel_flags) == 3
        assert all(key in progress._progress for key in ["key1", "key2", "key3"])
        assert all(key in progress._cancel_flags for key in ["key1", "key2", "key3"])
    
    def test_update_progress_with_existing_key(self):
        """Test updating progress with existing key"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=50, total=100, stage="processing")
        
        assert progress._progress["test_key"]["inserted"] == 50
        assert progress._progress["test_key"]["total"] == 100
        assert progress._progress["test_key"]["percent"] == 50.0
        assert progress._progress["test_key"]["stage"] == "processing"
        assert progress._progress["test_key"]["canceled"] is False  # Should sync from _cancel_flags
    
    def test_update_progress_with_new_key_auto_init(self):
        """Test updating progress with new key (auto-initialization)"""
        progress.update_progress("new_key", inserted=25, total=50)
        
        # Should auto-initialize
        assert "new_key" in progress._progress
        assert progress._progress["new_key"]["inserted"] == 25
        assert progress._progress["new_key"]["total"] == 50
        assert progress._progress["new_key"]["percent"] == 50.0
        assert progress._progress["new_key"]["stage"] == "starting"  # From init
        assert progress._progress["new_key"]["canceled"] is False
    
    def test_update_progress_percent_calculation_valid(self):
        """Test percentage calculation with valid integer values"""
        progress.init_progress("test_key")
        
        # Test various percentage calculations
        progress.update_progress("test_key", inserted=0, total=100)
        assert progress._progress["test_key"]["percent"] == 0.0
        
        progress.update_progress("test_key", inserted=33, total=100)
        assert progress._progress["test_key"]["percent"] == 33.0
        
        progress.update_progress("test_key", inserted=75, total=150)
        assert progress._progress["test_key"]["percent"] == 50.0
        
        progress.update_progress("test_key", inserted=100, total=100)
        assert progress._progress["test_key"]["percent"] == 100.0
    
    def test_update_progress_percent_calculation_invalid_cases(self):
        """Test percentage calculation with invalid cases"""
        progress.init_progress("test_key")
        
        # Case 1: total is zero (should not calculate percentage)
        progress.update_progress("test_key", inserted=10, total=0)
        assert progress._progress["test_key"]["percent"] == 0.0  # Should remain initial value
        
        # Case 2: total is negative (should not calculate percentage)
        progress.update_progress("test_key", inserted=10, total=-5)
        assert progress._progress["test_key"]["percent"] == 0.0  # Should remain initial value
        
        # Case 3: inserted is not integer
        progress.update_progress("test_key", inserted="fifty", total=100)
        assert progress._progress["test_key"]["percent"] == 0.0  # Should remain initial value
        
        # Case 4: total is not integer
        progress.update_progress("test_key", inserted=50, total="hundred")
        assert progress._progress["test_key"]["percent"] == 0.0  # Should remain initial value
        
        # Case 5: both are None
        progress.update_progress("test_key", inserted=None, total=None)
        assert progress._progress["test_key"]["percent"] == 0.0  # Should remain initial value
    
    def test_update_progress_with_cancel_flag_sync(self):
        """Test that update_progress syncs canceled status from _cancel_flags"""
        progress.init_progress("test_key")
        
        # Set cancel flag directly
        progress._cancel_flags["test_key"] = True
        
        # Update progress - should sync the canceled status
        progress.update_progress("test_key", inserted=10, total=100)
        
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_get_progress_existing_key(self):
        """Test getting progress for existing key"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=30, total=60, stage="loading", done=False)
        
        result = progress.get_progress("test_key")
        
        assert result["found"] is True
        assert result["inserted"] == 30
        assert result["total"] == 60
        assert result["percent"] == 50.0
        assert result["stage"] == "loading"
        assert result["done"] is False
        assert result["error"] is None
        assert result["canceled"] is False
    
    def test_get_progress_nonexistent_key(self):
        """Test getting progress for non-existent key"""
        result = progress.get_progress("nonexistent_key")
        
        assert result["found"] is False
        assert len(result) == 1  # Should only have 'found' key
    
    def test_get_progress_empty_data(self):
        """Test getting progress when data exists but is falsy"""
        # This tests the edge case where data might be None or empty dict
        progress._progress["test_key"] = None
        
        result = progress.get_progress("test_key")
        
        assert result["found"] is False
    
    def test_mark_error(self):
        """Test marking progress as error"""
        progress.init_progress("test_key")
        
        progress.mark_error("test_key", "Something went wrong")
        
        assert progress._progress["test_key"]["error"] == "Something went wrong"
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "error"
        assert progress._progress["test_key"]["percent"] == 100.0
    
    def test_mark_error_with_nonexistent_key(self):
        """Test marking error for non-existent key (should auto-initialize)"""
        progress.mark_error("new_key", "Error occurred")
        
        # Should auto-initialize through update_progress
        assert "new_key" in progress._progress
        assert progress._progress["new_key"]["error"] == "Error occurred"
        assert progress._progress["new_key"]["done"] is True
        assert progress._progress["new_key"]["stage"] == "error"
        assert progress._progress["new_key"]["percent"] == 100.0
    
    def test_mark_done(self):
        """Test marking progress as done"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=80, total=100)
        
        progress.mark_done("test_key")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "completed"
        assert progress._progress["test_key"]["percent"] == 100.0
        # Other fields should be preserved
        assert progress._progress["test_key"]["inserted"] == 80
        assert progress._progress["test_key"]["total"] == 100
    
    def test_mark_done_with_nonexistent_key(self):
        """Test marking done for non-existent key (should auto-initialize)"""
        progress.mark_done("new_key")
        
        # Should auto-initialize through update_progress
        assert "new_key" in progress._progress
        assert progress._progress["new_key"]["done"] is True
        assert progress._progress["new_key"]["stage"] == "completed"
        assert progress._progress["new_key"]["percent"] == 100.0
    
    def test_mark_canceled_with_default_message(self):
        """Test marking progress as canceled with default message"""
        progress.init_progress("test_key")
        
        progress.mark_canceled("test_key")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "canceled"
        assert progress._progress["test_key"]["error"] == "Canceled by user"
        assert progress._progress["test_key"]["percent"] == 100.0
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_mark_canceled_with_custom_message(self):
        """Test marking progress as canceled with custom message"""
        progress.init_progress("test_key")
        
        progress.mark_canceled("test_key", "User requested cancellation")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "canceled"
        assert progress._progress["test_key"]["error"] == "User requested cancellation"
        assert progress._progress["test_key"]["percent"] == 100.0
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_mark_canceled_with_nonexistent_key(self):
        """Test marking canceled for non-existent key (should auto-initialize)"""
        progress.mark_canceled("new_key", "Operation canceled")
        
        # Should auto-initialize through update_progress
        assert "new_key" in progress._progress
        assert progress._progress["new_key"]["done"] is True
        assert progress._progress["new_key"]["stage"] == "canceled"
        assert progress._progress["new_key"]["error"] == "Operation canceled"
        assert progress._progress["new_key"]["percent"] == 100.0
        assert progress._progress["new_key"]["canceled"] is True
    
    def test_request_cancel_existing_key(self):
        """Test requesting cancellation for existing key"""
        progress.init_progress("test_key")
        
        progress.request_cancel("test_key")
        
        assert progress._cancel_flags["test_key"] is True
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_request_cancel_nonexistent_key(self):
        """Test requesting cancellation for non-existent key"""
        progress.request_cancel("nonexistent_key")
        
        assert progress._cancel_flags["nonexistent_key"] is True
        # Should not auto-create progress entry, only cancel flag
        assert "nonexistent_key" not in progress._progress
    
    def test_request_cancel_key_not_in_progress(self):
        """Test requesting cancellation when key not in _progress but exists in _cancel_flags"""
        # Set up a scenario where cancel flag exists but progress doesn't
        progress._cancel_flags["test_key"] = False
        
        progress.request_cancel("test_key")
        
        assert progress._cancel_flags["test_key"] is True
        # Should not create progress entry since it didn't exist
        assert "test_key" not in progress._progress
    
    def test_is_canceled_true_case(self):
        """Test checking cancellation status - true case"""
        progress.init_progress("test_key")
        progress.request_cancel("test_key")
        
        result = progress.is_canceled("test_key")
        
        assert result is True
    
    def test_is_canceled_false_case_existing_key(self):
        """Test checking cancellation status - false case with existing key"""
        progress.init_progress("test_key")
        
        result = progress.is_canceled("test_key")
        
        assert result is False
    
    def test_is_canceled_false_case_nonexistent_key(self):
        """Test checking cancellation status - false case with non-existent key"""
        result = progress.is_canceled("nonexistent_key")
        
        assert result is False  # get() with default False
    
    def test_comprehensive_workflow(self):
        """Test a complete workflow with multiple operations"""
        # Initialize
        progress.init_progress("workflow_key")
        
        # Update progress multiple times
        progress.update_progress("workflow_key", inserted=10, total=100, stage="started")
        assert progress._progress["workflow_key"]["percent"] == 10.0
        
        progress.update_progress("workflow_key", inserted=50, stage="processing")
        assert progress._progress["workflow_key"]["percent"] == 50.0
        
        # Check progress
        result = progress.get_progress("workflow_key")
        assert result["found"] is True
        assert result["percent"] == 50.0
        assert result["stage"] == "processing"
        
        # Complete successfully
        progress.mark_done("workflow_key")
        assert progress._progress["workflow_key"]["done"] is True
        assert progress._progress["workflow_key"]["stage"] == "completed"
        assert progress._progress["workflow_key"]["percent"] == 100.0
    
    def test_comprehensive_workflow_with_cancellation(self):
        """Test a complete workflow ending with cancellation"""
        # Initialize and start work
        progress.init_progress("cancel_key")
        progress.update_progress("cancel_key", inserted=30, total=100, stage="processing")
        
        # Request cancellation
        progress.request_cancel("cancel_key")
        assert progress.is_canceled("cancel_key") is True
        
        # Update should reflect cancellation
        progress.update_progress("cancel_key", inserted=35)
        assert progress._progress["cancel_key"]["canceled"] is True
        
        # Mark as canceled
        progress.mark_canceled("cancel_key", "User canceled operation")
        assert progress._progress["cancel_key"]["stage"] == "canceled"
        assert progress._progress["cancel_key"]["done"] is True
        assert progress._progress["cancel_key"]["error"] == "User canceled operation"
    
    def test_thread_safety_simulation(self):
        """Test simulated concurrent operations to verify thread safety"""
        # Simulate multiple "concurrent" operations
        keys = ["thread1", "thread2", "thread3"]
        
        # Initialize all
        for key in keys:
            progress.init_progress(key)
        
        # Update all with different values
        for i, key in enumerate(keys):
            progress.update_progress(key, inserted=i*10, total=100, stage=f"stage_{i}")
        
        # Request cancellation for one
        progress.request_cancel("thread2")
        
        # Verify each has correct state
        for i, key in enumerate(keys):
            result = progress.get_progress(key)
            assert result["found"] is True
            assert result["inserted"] == i*10
            assert result["percent"] == i*10.0
            assert result["stage"] == f"stage_{i}"
            if key == "thread2":
                assert result["canceled"] is True
            else:
                assert result["canceled"] is False
    
    def test_edge_cases_and_error_conditions(self):
        """Test various edge cases and error conditions"""
        # Test with very large numbers
        progress.init_progress("large_numbers")
        progress.update_progress("large_numbers", inserted=999999999, total=1000000000)
        assert progress._progress["large_numbers"]["percent"] == 100.0  # 99.9999999 rounded to 100.0
        
        # Test with decimal numbers (should not calculate percentage)
        progress.init_progress("decimals")
        progress.update_progress("decimals", inserted=50.5, total=100.0)
        assert progress._progress["decimals"]["percent"] == 0.0  # Not integers, so no calculation
        
        # Test updating with mixed valid/invalid kwargs
        progress.init_progress("mixed")
        progress.update_progress("mixed", 
                               inserted=50, total=100,  # Valid for percentage
                               stage="processing", done=False,  # Valid other fields
                               custom_field="custom_value")  # Custom field should also be set
        assert progress._progress["mixed"]["percent"] == 50.0
        assert progress._progress["mixed"]["stage"] == "processing"
        assert progress._progress["mixed"]["done"] is False
        assert progress._progress["mixed"]["custom_field"] == "custom_value"