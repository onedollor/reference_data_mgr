#!/usr/bin/env python3
"""Convenience launcher (duplicate of root start_backend.py) so you can run:
    python start_backend.py
from inside the backend directory.

If you maintain only one canonical script, prefer editing the root-level
start_backend.py; this wrapper just delegates to uvicorn directly.
"""

from __future__ import annotations
import sys
import subprocess
from pathlib import Path


def main() -> int:
    backend_dir = Path(__file__).parent
    # Ensure backend on path
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))

    print("Starting Reference Data Auto Ingest System Backend (backend/start_backend.py)...")
    print(f"Backend directory: {backend_dir}")
    try:
        subprocess.run([
            sys.executable, '-m', 'uvicorn', 'app.main:app',
            '--host', '0.0.0.0', '--port', '8000', '--reload'
        ], check=True)
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except subprocess.CalledProcessError as e:
        print(f"Error starting server: {e}")
        return 1
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
