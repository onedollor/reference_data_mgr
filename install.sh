#!/bin/bash

# Reference Data Auto Ingest System - Installation Script
# This script sets up the complete development environment

set -e

echo "=================================================="
echo "Reference Data Auto Ingest System - Installation"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Python 3 is installed
check_python() {
    print_status "Checking Python installation..."
    
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
        print_success "Python 3 found: $PYTHON_VERSION"
    else
        print_error "Python 3 not found. Please install Python 3.8+ before continuing."
        exit 1
    fi
}

# Check if Node.js is installed
check_node() {
    print_status "Checking Node.js installation..."
    
    if command -v node &> /dev/null; then
        NODE_VERSION=$(node --version)
        print_success "Node.js found: $NODE_VERSION"
    else
        print_error "Node.js not found. Please install Node.js 16+ before continuing."
        exit 1
    fi
}

# Install Python dependencies
install_backend_deps() {
    print_status "Installing Python dependencies..."
    
    cd backend
    
    # Create virtual environment if it doesn't exist
    if [ ! -d "../venv" ]; then
        print_status "Creating Python virtual environment..."
        python3 -m venv ../venv
    fi
    
    # Activate virtual environment
    source ../venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install dependencies
    pip install -r requirements.txt
    
    print_success "Python dependencies installed successfully"
    cd ..
}

# Install Node.js dependencies
install_frontend_deps() {
    print_status "Installing Node.js dependencies..."
    
    cd frontend
    
    # Install dependencies
    npm install
    
    print_success "Node.js dependencies installed successfully"
    cd ..
}

# Create required directories
create_directories() {
    print_status "Creating required directories..."
    
    # Create data directories (adjust paths for your system)
    DIRS=(
        "data/reference_data/temp"
        "data/reference_data/archive" 
        "data/reference_data/format"
        "backend/logs"
    )
    
    for dir in "${DIRS[@]}"; do
        mkdir -p "$dir"
        print_success "Created directory: $dir"
    done
}

# Update .env file paths for Linux
update_env_paths() {
    print_status "Updating .env file for Linux paths..."
    
    CURRENT_DIR=$(pwd)
    
    # Update .env file with Linux paths
    sed -i "s|C:\\\\data\\\\reference_data|${CURRENT_DIR}/data/reference_data|g" .env
    sed -i "s|\\\\|/|g" .env
    
    print_success "Updated .env file with Linux paths"
}

# Create startup scripts
create_startup_scripts() {
    print_status "Creating startup scripts..."
    
    # Make the backend startup script executable
    chmod +x start_backend.py
    
    # Create a combined startup script
    cat > start_dev.sh << 'EOF'
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
EOF
    
    chmod +x start_dev.sh
    
    print_success "Created startup scripts"
}

# Test database connection
test_database() {
    print_status "Testing database connection..."
    
    source venv/bin/activate
    
    cat > test_db.py << 'EOF'
import os
import sys
sys.path.append('backend')

from dotenv import load_dotenv
from backend.utils.database import DatabaseManager

load_dotenv()

db_manager = DatabaseManager()
result = db_manager.test_connection()

if result['status'] == 'success':
    print(f"✓ Database connection successful")
    print(f"  Server: {result.get('server_version', 'Unknown')}")
    print(f"  Time: {result.get('current_time', 'Unknown')}")
else:
    print(f"✗ Database connection failed: {result['error']}")
    sys.exit(1)
EOF
    
    if python test_db.py; then
        print_success "Database connection test passed"
    else
        print_warning "Database connection test failed. Please check your database configuration in .env"
        print_warning "The application will still work, but you'll need to configure the database before processing files"
    fi
    
    rm test_db.py
}

# Main installation process
main() {
    print_status "Starting installation process..."
    
    # Check prerequisites
    check_python
    check_node
    
    # Install dependencies
    install_backend_deps
    install_frontend_deps
    
    # Setup environment
    create_directories
    update_env_paths
    create_startup_scripts
    
    # Test database (optional)
    if [[ "${1:-}" != "--skip-db-test" ]]; then
        test_database
    fi
    
    print_success "Installation completed successfully!"
    echo ""
    echo "=================================================="
    echo "Next Steps:"
    echo "=================================================="
    echo "1. Configure your database settings in .env file"
    echo "2. Start the development servers:"
    echo "   ./start_dev.sh"
    echo ""
    echo "Or start services individually:"
    echo "   Backend:  python start_backend.py"
    echo "   Frontend: cd frontend && npm start"
    echo ""
    echo "Access the application at: http://localhost:3000"
    echo "API documentation at: http://localhost:8000/docs"
    echo "=================================================="
}

# Run main function
main "$@"