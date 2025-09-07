"""Simple progress test without complex setup"""
import utils.progress as progress

def test_progress_basic():
    """Basic progress test"""
    progress._progress.clear()
    progress._cancel_flags.clear()
    progress.init_progress("simple_test")
    progress.update_progress("simple_test", inserted=10, total=20)
    result = progress.get_progress("simple_test")
    assert result["found"] is True
    assert result["percent"] == 50.0

def test_progress_functions():
    """Test all progress functions"""
    progress._progress.clear()
    progress._cancel_flags.clear()
    key = "func_test"
    progress.init_progress(key)
    progress.update_progress(key, inserted=80, total=100)
    progress.mark_done(key)
    
    result = progress.get_progress(key)
    assert result["done"] is True
    assert result["percent"] == 80.0

def test_cancel_functions():
    """Test cancellation functions"""
    progress._progress.clear()
    progress._cancel_flags.clear()
    key = "cancel_test"
    progress.init_progress(key)
    progress.request_cancel(key)
    assert progress.is_canceled(key) is True
    
    progress.mark_canceled(key, "Test cancel")
    result = progress.get_progress(key)
    assert result["canceled"] is True
    assert result["error"] == "Test cancel"
