"""
Comprehensive test coverage for utils/progress.py
Focus on improving coverage from 32% to much higher level
"""

import pytest
from unittest.mock import patch
import utils.progress as progress


class TestProgressBasicOperations:
    """Test basic progress operations"""
    
    def setup_method(self):
        """Setup test environment"""
        # Clear progress state
        progress._progress.clear()
        progress._cancel_flags.clear()
    
    def test_init_progress(self):
        """Test progress initialization"""
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
    
    def test_update_progress_basic(self):
        """Test basic progress update"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=50, total=100)
        
        assert progress._progress["test_key"]["inserted"] == 50
        assert progress._progress["test_key"]["total"] == 100
        assert progress._progress["test_key"]["percent"] == 50.0
    
    def test_update_progress_auto_init(self):
        """Test progress update with auto-initialization"""
        progress.update_progress("auto_key", inserted=25, total=50)
        
        assert "auto_key" in progress._progress
        assert progress._progress["auto_key"]["inserted"] == 25
        assert progress._progress["auto_key"]["total"] == 50
        assert progress._progress["auto_key"]["percent"] == 50.0
    
    def test_update_progress_with_kwargs(self):
        """Test progress update with various kwargs"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", stage="processing", done=True, error="test error")
        
        assert progress._progress["test_key"]["stage"] == "processing"
        assert progress._progress["test_key"]["done"] is True
        assert progress._progress["test_key"]["error"] == "test error"
    
    def test_update_progress_percent_calculation(self):
        """Test percentage calculation in progress update"""
        progress.init_progress("test_key")
        
        # Test normal calculation
        progress.update_progress("test_key", inserted=75, total=150)
        assert progress._progress["test_key"]["percent"] == 50.0
        
        # Test with zero total (should not crash)
        progress.update_progress("test_key", inserted=10, total=0)
        # Percentage should remain unchanged from previous calculation
        assert progress._progress["test_key"]["percent"] == 50.0
        
        # Test with None values (should not crash)
        progress.update_progress("test_key", inserted=None, total=100)
        assert progress._progress["test_key"]["percent"] == 50.0
    
    def test_get_progress_existing(self):
        """Test getting progress for existing key"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=30, total=60, stage="loading")
        
        result = progress.get_progress("test_key")
        
        assert result["inserted"] == 30
        assert result["total"] == 60
        assert result["percent"] == 50.0
        assert result["stage"] == "loading"
    
    def test_get_progress_nonexistent(self):
        """Test getting progress for non-existent key"""
        result = progress.get_progress("nonexistent_key")
        
        assert result is None
    
    def test_cancel_progress(self):
        """Test progress cancellation"""
        progress.init_progress("test_key")
        
        progress.cancel_progress("test_key")
        
        assert progress._cancel_flags["test_key"] is True
        assert progress._progress["test_key"]["canceled"] is True
    
    def test_cancel_progress_nonexistent(self):
        """Test canceling progress for non-existent key"""
        # Should not crash
        progress.cancel_progress("nonexistent_key")
        
        # Should create the cancel flag
        assert progress._cancel_flags["nonexistent_key"] is True
    
    def test_is_canceled_true(self):
        """Test checking if progress is canceled (true case)"""
        progress.init_progress("test_key")
        progress.cancel_progress("test_key")
        
        result = progress.is_canceled("test_key")
        
        assert result is True
    
    def test_is_canceled_false(self):
        """Test checking if progress is canceled (false case)"""
        progress.init_progress("test_key")
        
        result = progress.is_canceled("test_key")
        
        assert result is False
    
    def test_is_canceled_nonexistent(self):
        """Test checking cancellation for non-existent key"""
        result = progress.is_canceled("nonexistent_key")
        
        assert result is False
    
    def test_remove_progress(self):
        """Test removing progress"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=10, total=20)
        
        progress.remove_progress("test_key")
        
        assert "test_key" not in progress._progress
        assert "test_key" not in progress._cancel_flags
    
    def test_remove_progress_nonexistent(self):
        """Test removing non-existent progress (should not crash)"""
        progress.remove_progress("nonexistent_key")
        
        # Should not raise any exception
        assert "nonexistent_key" not in progress._progress
        assert "nonexistent_key" not in progress._cancel_flags


class TestProgressThreadSafety:
    """Test progress thread safety"""
    
    def setup_method(self):
        """Setup test environment"""
        # Clear progress state
        progress._progress.clear()
        progress._cancel_flags.clear()
    
    def test_concurrent_operations_simulation(self):
        """Test simulated concurrent operations"""
        # Test multiple operations that would be concurrent
        progress.init_progress("key1")
        progress.init_progress("key2")
        progress.update_progress("key1", inserted=10, total=100)
        progress.update_progress("key2", inserted=20, total=200)
        progress.cancel_progress("key1")
        
        assert progress._progress["key1"]["inserted"] == 10
        assert progress._progress["key2"]["inserted"] == 20
        assert progress.is_canceled("key1") is True
        assert progress.is_canceled("key2") is False
    
    def test_lock_usage_with_mock(self):
        """Test that lock is being used properly"""
        with patch.object(progress._lock, 'acquire') as mock_acquire:
            with patch.object(progress._lock, 'release') as mock_release:
                # Mock the context manager behavior
                mock_acquire.return_value = True
                
                progress.init_progress("test_key")
                
                # Lock should have been used
                mock_acquire.assert_called()


class TestProgressEdgeCases:
    """Test edge cases and error conditions"""
    
    def setup_method(self):
        """Setup test environment"""
        # Clear progress state
        progress._progress.clear()
        progress._cancel_flags.clear()
    
    def test_large_numbers(self):
        """Test progress with large numbers"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=999999999, total=2000000000)
        
        result = progress.get_progress("test_key")
        
        assert result["inserted"] == 999999999
        assert result["total"] == 2000000000
        assert abs(result["percent"] - 50.0) < 0.01  # Allow for floating point precision
    
    def test_negative_numbers(self):
        """Test progress with negative numbers"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted=-10, total=100)
        
        result = progress.get_progress("test_key")
        
        # Should handle negative numbers gracefully
        assert result["inserted"] == -10
        assert result["total"] == 100
    
    def test_string_values_in_numeric_fields(self):
        """Test progress with string values in numeric fields"""
        progress.init_progress("test_key")
        progress.update_progress("test_key", inserted="fifty", total="hundred")
        
        result = progress.get_progress("test_key")
        
        # Should not crash, percentage calculation should be skipped
        assert result["inserted"] == "fifty"
        assert result["total"] == "hundred"
        assert result["percent"] == 0.0  # Should remain at initial value
    
    def test_multiple_updates_same_key(self):
        """Test multiple updates to the same progress key"""
        progress.init_progress("test_key")
        
        progress.update_progress("test_key", inserted=10, total=100, stage="stage1")
        progress.update_progress("test_key", inserted=50, stage="stage2")
        progress.update_progress("test_key", total=200, stage="stage3")
        
        result = progress.get_progress("test_key")
        
        assert result["inserted"] == 50
        assert result["total"] == 200
        assert result["stage"] == "stage3"
        assert result["percent"] == 25.0  # 50/200