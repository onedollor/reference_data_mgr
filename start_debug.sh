#!/bin/bash
# Debug startup script for Simplified Reference Data Management System

echo "🐛 STARTING DEBUG MODE 🐛"
echo "=========================="

# Set debug environment variable
export DEBUG=true

# Enable verbose Python warnings
export PYTHONWARNINGS=default

# Set Python to unbuffered mode for immediate output
export PYTHONUNBUFFERED=1

# Show environment
echo "Debug Environment:"
echo "  DEBUG=${DEBUG}"
echo "  PYTHONWARNINGS=${PYTHONWARNINGS}" 
echo "  PYTHONUNBUFFERED=${PYTHONUNBUFFERED}"
echo ""

# Start the system with debug enabled
echo "Starting system with debug logging enabled..."
./start_simplified_monitor.sh "$@"