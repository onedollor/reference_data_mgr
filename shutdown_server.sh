#!/bin/bash
set -euo pipefail

echo "Stopping backend (uvicorn) on port 8000..." 
pkill -f uvicorn || true && fuser -k 8000/tcp || true
echo "Done."
