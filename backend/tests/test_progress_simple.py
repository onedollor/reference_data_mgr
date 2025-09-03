"""
Simple comprehensive test coverage for utils/progress.py to achieve >90% coverage
Targeting missing lines: 10-21, 24-32, 35-39, 42, 45, 48, 51-54, 58-59
"""

import pytest
from utils.progress import (
    init_progress, update_progress, get_progress, mark_error, 
    mark_done, mark_canceled, request_cancel, is_canceled,
    _progress, _cancel_flags, _lock
)


class TestProgressSimple:
    """Simple comprehensive tests for progress tracking module"""
    
    def setup_method(self):
        """Clear progress data before each test"""
        with _lock:
            _progress.clear()
            _cancel_flags.clear()
    
    def test_init_progress_basic(self):
        """Test basic progress initialization - covers lines 10-21"""
        key = "test_init"
        init_progress(key)
        
        # Verify initialization data structure (lines 10-18)
        assert key in _progress
        data = _progress[key]
        assert data['inserted'] == 0
        assert data['total'] is None
        assert data['percent'] == 0.0
        assert data['stage'] == 'starting'
        assert data['done'] is False
        assert data['error'] is None
        assert data['canceled'] is False
        
        # Verify cancel flag initialization (lines 20-21)
        assert key in _cancel_flags
        assert _cancel_flags[key] is False
    
    def test_update_progress_new_key_auto_init(self):
        """Test update_progress with new key triggers init - covers lines 24-26"""
        key = "test_auto_init"
        update_progress(key, inserted=5, total=10)
        
        # Should auto-initialize (line 26)
        assert key in _progress
        assert _progress[key]['inserted'] == 5
        assert _progress[key]['total'] == 10
    
    def test_update_progress_existing_key(self):
        """Test update_progress with existing key - covers lines 27-32"""
        key = "test_update"
        init_progress(key)
        
        # Update with specific values to trigger percentage calculation (lines 27-31)
        update_progress(key, inserted=25, total=100, stage='processing')
        
        data = _progress[key]
        assert data['inserted'] == 25  # line 27: update kwargs
        assert data['total'] == 100    # line 27: update kwargs
        assert data['stage'] == 'processing'  # line 27: update kwargs
        
        # Test percentage calculation (lines 28-31)
        assert data['percent'] == 25.0  # round(25/100*100, 2) = 25.0
        
        # Test cancel flag propagation (line 32)
        assert data['canceled'] is False
    
    def test_update_progress_percentage_calculation_edge_cases(self):
        """Test percentage calculation edge cases - covers lines 28-31"""
        key = "test_percent"
        
        # Test with zero total (should not calculate percentage)
        update_progress(key, inserted=10, total=0)
        assert _progress[key]['percent'] == 0.0  # Default from init
        
        # Test with non-integer values (should not calculate percentage)
        update_progress(key, inserted="not_int", total=100)
        assert _progress[key]['percent'] == 0.0
        
        # Test with None total (should not calculate percentage)
        update_progress(key, inserted=10, total=None)
        assert _progress[key]['percent'] == 0.0
        
        # Test with valid values (should calculate percentage)
        update_progress(key, inserted=33, total=99)
        assert _progress[key]['percent'] == 33.33  # round(33/99*100, 2)
    
    def test_get_progress_not_found(self):
        """Test get_progress with non-existent key - covers lines 36-38"""
        result = get_progress("nonexistent")
        
        # Should return not found (lines 37-38)
        assert result == {'found': False}
    
    def test_get_progress_found(self):
        """Test get_progress with existing key - covers line 39"""
        key = "test_get"
        init_progress(key)
        update_progress(key, inserted=50, total=100, stage='halfway')
        
        result = get_progress(key)
        
        # Should return found data (line 39)
        assert result['found'] is True
        assert result['inserted'] == 50
        assert result['total'] == 100
        assert result['stage'] == 'halfway'
        assert result['percent'] == 50.0
    
    def test_mark_error(self):
        """Test mark_error function - covers line 42"""
        key = "test_error"
        init_progress(key)
        
        # Test mark_error (line 42)
        error_message = "Something went wrong"
        mark_error(key, error_message)
        
        data = _progress[key]
        assert data['error'] == error_message
        assert data['done'] is True
        assert data['stage'] == 'error'
        assert data['percent'] == 100.0
    
    def test_mark_done(self):
        """Test mark_done function - covers line 45"""
        key = "test_done"
        init_progress(key)
        
        # Test mark_done (line 45)
        mark_done(key)
        
        data = _progress[key]
        assert data['done'] is True
        assert data['stage'] == 'completed'
        assert data['percent'] == 100.0
    
    def test_mark_canceled_with_default_message(self):
        """Test mark_canceled with default message - covers line 48"""
        key = "test_cancel_default"
        init_progress(key)
        
        # Test mark_canceled with default message (line 48)
        mark_canceled(key)
        
        data = _progress[key]
        assert data['done'] is True
        assert data['stage'] == 'canceled'
        assert data['error'] == "Canceled by user"
        assert data['percent'] == 100.0
        assert data['canceled'] is True
    
    def test_mark_canceled_with_custom_message(self):
        """Test mark_canceled with custom message - covers line 48"""
        key = "test_cancel_custom"
        init_progress(key)
        
        # Test mark_canceled with custom message (line 48)
        custom_message = "User requested cancellation"
        mark_canceled(key, custom_message)
        
        data = _progress[key]
        assert data['done'] is True
        assert data['stage'] == 'canceled'
        assert data['error'] == custom_message
        assert data['percent'] == 100.0
        assert data['canceled'] is True
    
    def test_request_cancel_new_key(self):
        """Test request_cancel with new key - covers lines 51-52"""
        key = "test_cancel_new"
        
        # Test request_cancel with new key (lines 51-52)
        request_cancel(key)
        
        # Should create cancel flag (line 52)
        assert key in _cancel_flags
        assert _cancel_flags[key] is True
        # No progress data should exist yet
        assert key not in _progress
    
    def test_request_cancel_existing_key(self):
        """Test request_cancel with existing key - covers lines 51-54"""
        key = "test_cancel_existing"
        init_progress(key)
        
        # Test request_cancel with existing key (lines 51-54)
        request_cancel(key)
        
        # Should set cancel flag (line 52)
        assert _cancel_flags[key] is True
        
        # Should update progress data (lines 53-54)
        assert _progress[key]['canceled'] is True
    
    def test_is_canceled_true(self):
        """Test is_canceled returns True - covers line 59"""
        key = "test_is_canceled_true"
        request_cancel(key)
        
        # Test is_canceled (line 59)
        result = is_canceled(key)
        assert result is True
    
    def test_is_canceled_false(self):
        """Test is_canceled returns False - covers line 59"""
        key = "test_is_canceled_false"
        init_progress(key)
        
        # Test is_canceled with non-canceled key (line 59)
        result = is_canceled(key)
        assert result is False
    
    def test_is_canceled_nonexistent_key(self):
        """Test is_canceled with nonexistent key - covers line 59"""
        # Test is_canceled with nonexistent key (line 59)
        result = is_canceled("nonexistent")
        assert result is False  # .get() returns False as default
    
    def test_complex_workflow(self):
        """Test complex workflow covering multiple functions"""
        key = "test_workflow"
        
        # Start with initialization
        init_progress(key)
        assert get_progress(key)['stage'] == 'starting'
        
        # Update progress multiple times
        update_progress(key, inserted=25, total=100, stage='processing')
        data = get_progress(key)
        assert data['percent'] == 25.0
        assert data['stage'] == 'processing'
        
        # Test cancellation
        request_cancel(key)
        assert is_canceled(key) is True
        
        # Mark as canceled
        mark_canceled(key, "Workflow canceled")
        final_data = get_progress(key)
        assert final_data['done'] is True
        assert final_data['stage'] == 'canceled'
        assert final_data['error'] == "Workflow canceled"