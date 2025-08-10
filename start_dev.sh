#!/bin/bash

# Start both backend and frontend for development

echo "Starting Reference Data Auto Ingest System..."
echo "Backend will start on http://localhost:8000"
echo "Frontend will start on http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop both services"

# Function to cleanup background processes
cleanup() {
    echo ""
    echo "Shutting down services..."
    jobs -p | xargs -r kill
    exit 0
}

trap cleanup INT

# Start backend in background
echo "Starting backend..."
source venv/bin/activate && python start_backend.py &
BACKEND_PID=$!

# Wait a moment for backend to start
sleep 3

# Start frontend in background
echo "Starting frontend..."
cd frontend && npm start &
FRONTEND_PID=$!

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID
