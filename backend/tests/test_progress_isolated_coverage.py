"""
Isolated progress coverage test to avoid mocking conflicts from ingest tests.
This file runs progress tests independently to ensure proper coverage recording.
"""

import pytest
import utils.progress as progress


class TestProgressIsolated:
    """Isolated progress tests to ensure coverage recording without mocking conflicts"""
    
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
    
    def test_get_progress_existing_key(self):
        """Test getting progress for existing key"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=30, total=100, stage="active")
        
        result = progress.get_progress("test_key")
        assert result["found"] is True
        assert result["inserted"] == 30
        assert result["total"] == 100
        assert result["percent"] == 30.0
        assert result["stage"] == "active"
    
    def test_get_progress_nonexistent_key(self):
        """Test getting progress for non-existent key"""
        result = progress.get_progress("nonexistent")
        assert result["found"] is False
    
    def test_mark_error(self):
        """Test marking progress as error"""
        progress.init_progress("test_key")
        progress.mark_error("test_key", "Test error message")
        
        assert progress._progress["test_key"]["error"] == "Test error message"
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "error"
        assert progress._progress["test_key"]["percent"] == 100.0
    
    def test_mark_done(self):
        """Test marking progress as done"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=80, total=100)
        progress.mark_done("test_key")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "completed"
        assert progress._progress["test_key"]["percent"] == 100.0
    
    def test_mark_canceled(self):
        """Test marking progress as canceled"""
        progress.init_progress("test_key")
        progress.mark_canceled("test_key", "User canceled")
        
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["stage"] == "canceled"
        assert progress._progress["test_key"]["error"] == "User canceled"
        assert progress._progress["test_key"]["percent"] == 100.0
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_request_cancel(self):
        """Test requesting cancellation"""
        progress.init_progress("test_key")
        progress.request_cancel("test_key")
        
        assert progress._cancel_flags["test_key"] is True
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_is_canceled(self):
        """Test checking if progress is canceled"""
        progress.init_progress("test_key")
        
        # Initially not canceled
        assert progress.is_canceled("test_key") is False
        
        # After requesting cancel
        progress.request_cancel("test_key")
        assert progress.is_canceled("test_key") is True
    
    def test_is_canceled_nonexistent_key(self):
        """Test checking cancellation for non-existent key"""
        assert progress.is_canceled("nonexistent") is False
    
    def test_percent_calculation_edge_cases(self):
        """Test percentage calculation edge cases"""
        progress.init_progress("test_key")
        
        # Zero total should not cause division by zero
        progress.update_progress("test_key", inserted=10, total=0)
        assert progress._progress["test_key"]["percent"] == 0.0
        
        # None total should not update percentage
        progress.update_progress("test_key", inserted=10, total=None)
        assert progress._progress["test_key"]["percent"] == 0.0
        
        # Non-integer values should not update percentage
        progress.update_progress("test_key", inserted="10", total=100)
        assert progress._progress["test_key"]["percent"] == 0.0


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])