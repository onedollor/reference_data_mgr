"""
Final targeted tests to achieve >90% coverage for utils/progress.py
Targets specific uncovered lines: 26, 38, 48
"""

import utils.progress as progress


def test_auto_init_in_update_progress():
    """Test line 26: auto-initialization when key doesn't exist in update_progress"""
    # Clear state
    progress._progress.clear()
    progress._cancel_flags.clear()
    
    # This should trigger line 26 (init_progress call within update_progress)
    progress.update_progress('auto_init_key', inserted=25, total=50)
    
    assert 'auto_init_key' in progress._progress
    assert progress._progress['auto_init_key']['inserted'] == 25
    assert progress._progress['auto_init_key']['total'] == 50
    assert progress._progress['auto_init_key']['percent'] == 50.0


def test_get_progress_nonexistent_key():
    """Test line 38: return {'found': False} when key doesn't exist"""
    # Clear state
    progress._progress.clear()
    progress._cancel_flags.clear()
    
    # This should trigger line 38
    result = progress.get_progress('nonexistent_key')
    
    assert result == {'found': False}


def test_mark_canceled():
    """Test line 48: mark_canceled function call"""
    # Clear state
    progress._progress.clear()
    progress._cancel_flags.clear()
    
    # Initialize a progress key
    progress.init_progress('cancel_key')
    
    # This should trigger line 48
    progress.mark_canceled('cancel_key', 'Test cancellation')
    
    assert progress._progress['cancel_key']['done'] is True
    assert progress._progress['cancel_key']['stage'] == 'canceled'
    assert progress._progress['cancel_key']['error'] == 'Test cancellation'
    assert progress._progress['cancel_key']['canceled'] is True


if __name__ == '__main__':
    test_auto_init_in_update_progress()
    test_get_progress_nonexistent_key()  
    test_mark_canceled()
    print("All tests passed!")