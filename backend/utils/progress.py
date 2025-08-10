"""In-memory progress tracking with cancel support (no external persistence)."""
from typing import Dict, Any
from threading import Lock

_progress: Dict[str, Dict[str, Any]] = {}
_cancel_flags: Dict[str, bool] = {}
_lock = Lock()

def init_progress(key: str):
    data = {
        'inserted': 0,
        'total': None,
        'percent': 0.0,
        'stage': 'starting',
        'done': False,
        'error': None,
        'canceled': False
    }
    with _lock:
        _progress[key] = data
        _cancel_flags[key] = False

def update_progress(key: str, **kwargs):
    with _lock:
        if key not in _progress:
            init_progress(key)
        _progress[key].update(kwargs)
        inserted = _progress[key].get('inserted')
        total = _progress[key].get('total')
        if isinstance(inserted, int) and isinstance(total, int) and total > 0:
            _progress[key]['percent'] = round(inserted / total * 100, 2)
        _progress[key]['canceled'] = _cancel_flags.get(key, False)

def get_progress(key: str) -> Dict[str, Any]:
    with _lock:
        data = _progress.get(key)
        if not data:
            return {'found': False}
        return {'found': True, **data}

def mark_error(key: str, message: str):
    update_progress(key, error=message, done=True, stage='error', percent=100.0)

def mark_done(key: str):
    update_progress(key, done=True, stage='completed', percent=100.0)

def request_cancel(key: str):
    with _lock:
        _cancel_flags[key] = True
        if key in _progress:
            _progress[key]['canceled'] = True
    # purely in-memory

def is_canceled(key: str) -> bool:
    with _lock:
        return _cancel_flags.get(key, False)
