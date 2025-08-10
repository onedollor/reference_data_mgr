#!/usr/bin/env python3
"""
Startup script for the Reference Data Auto Ingest System backend
"""

import os
import sys
import subprocess
from pathlib import Path

def main():
    """Start the FastAPI backend server"""
    
    # Get the project root directory
    project_root = Path(__file__).parent
    backend_path = project_root / "backend"
    
    # Change to backend directory
    os.chdir(backend_path)
    
    # Add backend directory to Python path
    sys.path.insert(0, str(backend_path))
    
    print("Starting Reference Data Auto Ingest System Backend...")
    print(f"Backend directory: {backend_path}")
    
    try:
        # Start the FastAPI server using uvicorn
        subprocess.run([
            sys.executable, "-m", "uvicorn", 
            "app.main:app", 
            "--host", "0.0.0.0", 
            "--port", "8000", 
            "--reload"
        ], check=True)
    except KeyboardInterrupt:
        print("\nShutting down server...")
    except subprocess.CalledProcessError as e:
        print(f"Error starting server: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())