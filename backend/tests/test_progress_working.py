"""
Working progress test subset for comprehensive coverage
Based on fixed tests from test_progress_complete.py
"""

import pytest
import utils.progress as progress


class TestProgressWorking:
    """Working progress tests that achieve high coverage without timeouts"""
    
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
    
    def test_update_progress_with_existing_key(self):
        """Test updating progress with existing key"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=50, total=100, stage="processing")
        
        assert progress._progress["test_key"]["inserted"] == 50
        assert progress._progress["test_key"]["total"] == 100
        assert progress._progress["test_key"]["percent"] == 50.0
        assert progress._progress["test_key"]["stage"] == "processing"
        assert progress._progress["test_key"]["canceled"] is False
    
    def test_update_progress_with_new_key_auto_init(self):
        """Test updating progress with new key (auto-initialization)"""
        progress.update_progress("new_key", inserted=25, total=50)
        
        # Should auto-initialize
        assert "new_key" in progress._progress
        assert progress._progress["new_key"]["inserted"] == 25
        assert progress._progress["new_key"]["total"] == 50
        assert progress._progress["new_key"]["percent"] == 50.0
        assert progress._progress["new_key"]["stage"] == "starting"
        assert progress._progress["new_key"]["canceled"] is False
    
    def test_get_progress_existing_key(self):
        """Test getting progress for existing key"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=75, total=100, stage="almost_done")
        
        result = progress.get_progress("test_key")
        
        assert result["found"] is True
        assert result["inserted"] == 75
        assert result["total"] == 100
        assert result["percent"] == 75.0
        assert result["stage"] == "almost_done"
        assert result["done"] is False
        assert result["canceled"] is False
    
    def test_get_progress_nonexistent_key(self):
        """Test getting progress for non-existent key"""
        result = progress.get_progress("nonexistent_key")
        
        assert result["found"] is False
        assert len(result) == 1  # Should only contain 'found' key
    
    def test_mark_error(self):
        """Test marking progress as error"""
        progress.init_progress("test_key")
        
        progress.mark_error("test_key", "Something went wrong")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "error"
        assert progress._progress["test_key"]["error"] == "Something went wrong"
        assert progress._progress["test_key"]["percent"] == 100.0
    
    def test_mark_done(self):
        """Test marking progress as done"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=80, total=100)
        
        progress.mark_done("test_key")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "completed"
        # Note: percent is recalculated based on inserted/total, not forced to 100.0
        assert progress._progress["test_key"]["percent"] == 80.0
        # Other fields should be preserved
        assert progress._progress["test_key"]["inserted"] == 80
        assert progress._progress["test_key"]["total"] == 100
    
    def test_mark_canceled_with_default_message(self):
        """Test marking progress as canceled with default message"""
        progress.init_progress("test_key")
        
        # First request cancel to set the flag, then mark as canceled
        progress.request_cancel("test_key")
        progress.mark_canceled("test_key")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "canceled"
        assert progress._progress["test_key"]["error"] == "Canceled by user"
        assert progress._progress["test_key"]["percent"] == 100.0
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_request_cancel_existing_key(self):
        """Test requesting cancel for existing key"""
        progress.init_progress("test_key")
        
        progress.request_cancel("test_key")
        
        assert progress._cancel_flags["test_key"] is True
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_is_canceled_true_case(self):
        """Test is_canceled when key is canceled"""
        progress.init_progress("test_key")
        progress.request_cancel("test_key")
        
        assert progress.is_canceled("test_key") is True
    
    def test_is_canceled_false_case_existing_key(self):
        """Test is_canceled when key exists but not canceled"""
        progress.init_progress("test_key")
        
        assert progress.is_canceled("test_key") is False
    
    def test_is_canceled_false_case_nonexistent_key(self):
        """Test is_canceled for non-existent key"""
        assert progress.is_canceled("nonexistent_key") is False