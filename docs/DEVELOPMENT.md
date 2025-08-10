# Development Guide

## Overview

This guide provides comprehensive information for developers working on the Reference Data Auto Ingest System. It covers development environment setup, coding standards, architecture patterns, testing procedures, and contribution guidelines.

## Quick Start

### Prerequisites

#### Required Software
- **Python 3.8+** (3.12+ recommended)
- **Node.js 16+** (18+ recommended)  
- **SQL Server** (Express, Developer, or Standard)
- **Git** (latest version)
- **VS Code or PyCharm** (recommended IDEs)

#### Optional Tools
- **Docker & Docker Compose** (for containerized development)
- **Postman or Insomnia** (for API testing)
- **SQL Server Management Studio** (for database management)

### Development Environment Setup

#### 1. Clone and Initial Setup
```bash
# Clone the repository
git clone <repository-url>
cd reference_data_mgr

# Create Python virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install Python dependencies
pip install -r backend/requirements.txt

# Install development dependencies
pip install -r backend/requirements-dev.txt
```

#### 2. Environment Configuration
```bash
# Copy environment template
cp .env.example .env

# Edit .env with your development settings
# Minimum required configuration:
db_host=localhost
db_name=reference_data_dev
db_user=dev_user
db_password=dev_password
temp_location=./temp
archive_location=./archive
format_location=./format
```

#### 3. Database Setup
```sql
-- Create development database
CREATE DATABASE reference_data_dev;
GO

-- Create development user
USE reference_data_dev;
CREATE LOGIN dev_user WITH PASSWORD = 'dev_password';
CREATE USER dev_user FOR LOGIN dev_user;

-- Grant permissions
GRANT db_owner TO dev_user;
```

#### 4. Frontend Setup
```bash
# Navigate to frontend directory
cd frontend

# Install Node.js dependencies
npm install

# Install development dependencies
npm install --save-dev
```

#### 5. Start Development Services
```bash
# Option 1: Use development script
./start_dev.sh

# Option 2: Start services manually
# Terminal 1 - Backend
cd backend && source venv/bin/activate && python start_backend.py

# Terminal 2 - Frontend
cd frontend && npm start

# Option 3: Docker development environment
docker-compose -f docker-compose.dev.yml up
```

## Project Structure

### Directory Layout
```
reference_data_mgr/
├── .env                           # Environment variables
├── .gitignore                     # Git ignore rules
├── README.md                      # Project documentation
├── start_dev.sh                   # Development startup script
├── docker-compose.dev.yml         # Development Docker setup
│
├── backend/                       # Python FastAPI Backend
│   ├── app/
│   │   ├── __init__.py
│   │   └── main.py                # FastAPI application
│   ├── utils/                     # Core utilities
│   │   ├── __init__.py
│   │   ├── database.py            # Database operations
│   │   ├── file_handler.py        # File processing
│   │   ├── ingest.py              # Data ingestion
│   │   ├── logger.py              # Logging system
│   │   ├── csv_detector.py        # CSV format detection
│   │   └── progress.py            # Progress tracking
│   ├── tests/                     # Test suite
│   │   ├── __init__.py
│   │   ├── conftest.py            # Test configuration
│   │   ├── test_database.py       # Database tests
│   │   ├── test_file_handler.py   # File handling tests
│   │   ├── test_ingest.py         # Ingestion tests
│   │   └── test_api.py            # API tests
│   ├── logs/                      # Application logs
│   ├── requirements.txt           # Production dependencies
│   ├── requirements-dev.txt       # Development dependencies
│   └── start_backend.py           # Backend startup script
│
├── frontend/                      # React Frontend
│   ├── public/
│   │   └── index.html             # HTML template
│   ├── src/
│   │   ├── components/            # React components
│   │   │   ├── FileUploadComponent.js
│   │   │   ├── ProgressDisplay.js
│   │   │   ├── LogsDisplay.js
│   │   │   └── ConfigurationPanel.js
│   │   ├── utils/                 # Frontend utilities
│   │   ├── App.js                 # Main application
│   │   └── index.js               # Entry point
│   ├── package.json               # Node.js dependencies
│   └── package-lock.json          # Dependency lock file
│
├── docs/                          # Documentation
│   ├── API_REFERENCE.md           # API documentation
│   ├── ARCHITECTURE.md            # System architecture
│   ├── DEPLOYMENT.md              # Deployment guide
│   ├── DEVELOPMENT.md             # This file
│   ├── SECURITY.md                # Security documentation
│   ├── USER_GUIDE.md              # User guide
│   ├── TROUBLESHOOTING.md         # Troubleshooting guide
│   └── CHANGELOG.md               # Version history
│
└── scripts/                       # Utility scripts
    ├── setup_dev_env.sh           # Development environment setup
    ├── run_tests.sh               # Test execution
    └── build_release.sh           # Release build
```

## Development Workflow

### Branch Strategy

#### Git Flow Model
```bash
# Main branches
main            # Production-ready code
develop         # Integration branch for features

# Supporting branches
feature/*       # New features
bugfix/*        # Bug fixes
hotfix/*        # Critical production fixes
release/*       # Release preparation
```

#### Feature Development Workflow
```bash
# Start new feature
git checkout develop
git pull origin develop
git checkout -b feature/new-csv-detector

# Work on feature
# ... make changes ...
git add .
git commit -m "feat: implement advanced CSV detection algorithm"

# Push feature branch
git push origin feature/new-csv-detector

# Create pull request to develop branch
# ... code review process ...

# After approval and merge
git checkout develop
git pull origin develop
git branch -d feature/new-csv-detector
```

### Coding Standards

#### Python Code Standards

##### Style Guide
- Follow **PEP 8** style guidelines
- Use **Black** for code formatting
- Use **isort** for import sorting
- Use **flake8** for linting

##### Configuration Files
```ini
# setup.cfg
[flake8]
max-line-length = 100
extend-ignore = E203, W503, E501
exclude = venv, .git, __pycache__

[isort]
profile = black
multi_line_output = 3
line_length = 100
```

```json
# pyproject.toml
[tool.black]
line-length = 100
target-version = ['py38']
include = '\.pyi?$'
extend-exclude = '''
/(
  # directories
  \.eggs
  | \.git
  | \.hg
  | \.mypy_cache
  | \.tox
  | \.venv
  | build
  | dist
)/
'''
```

##### Code Examples
```python
# Good: Proper function documentation
async def ingest_csv_data(
    file_path: str, 
    format_config: Dict[str, Any],
    load_mode: str = "full"
) -> AsyncGenerator[str, None]:
    """
    Ingest CSV data with progress streaming.
    
    Args:
        file_path: Path to CSV file to process
        format_config: CSV format configuration dictionary
        load_mode: Loading mode ("full" or "append")
        
    Yields:
        Progress messages for real-time updates
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
        DatabaseError: If database operations fail
    """
    try:
        # Implementation...
        yield "Starting ingestion process..."
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        raise

# Good: Proper error handling with context
def get_database_connection() -> pyodbc.Connection:
    """Get database connection with retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            connection = pyodbc.connect(self.connection_string)
            connection.autocommit = True
            return connection
        except pyodbc.Error as e:
            if attempt == max_retries - 1:
                raise DatabaseConnectionError(f"Failed to connect after {max_retries} attempts") from e
            time.sleep(2 ** attempt)  # Exponential backoff

# Good: Input validation
def validate_table_name(table_name: str) -> str:
    """Validate and sanitize table name for SQL usage."""
    if not isinstance(table_name, str):
        raise ValueError("Table name must be a string")
    
    # Remove invalid characters
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
    
    # Ensure it starts with letter or underscore
    if not re.match(r'^[a-zA-Z_]', sanitized):
        sanitized = f'table_{sanitized}'
    
    # Limit length
    if len(sanitized) > 50:
        sanitized = sanitized[:50]
    
    return sanitized
```

#### JavaScript/React Code Standards

##### Style Guide
- Use **Prettier** for code formatting
- Use **ESLint** for linting
- Follow **React Hooks** patterns
- Use **functional components** over class components

##### Configuration Files
```json
// .eslintrc.json
{
  "extends": [
    "react-app",
    "react-app/jest"
  ],
  "rules": {
    "no-unused-vars": "warn",
    "no-console": "warn",
    "prefer-const": "error",
    "react-hooks/exhaustive-deps": "warn"
  }
}
```

```json
// .prettierrc
{
  "semi": true,
  "trailingComma": "es5",
  "singleQuote": true,
  "printWidth": 100,
  "tabWidth": 2
}
```

##### React Component Examples
```javascript
// Good: Functional component with proper hooks usage
import React, { useState, useEffect, useCallback } from 'react';
import { Typography, CircularProgress, Alert } from '@mui/material';

const ProgressDisplay = ({ progressKey, onComplete }) => {
  const [progress, setProgress] = useState(null);
  const [error, setError] = useState(null);
  const [isLoading, setIsLoading] = useState(true);

  const fetchProgress = useCallback(async () => {
    try {
      const response = await fetch(`/api/progress/${progressKey}`);
      if (!response.ok) {
        throw new Error('Failed to fetch progress');
      }
      const data = await response.json();
      setProgress(data);
      
      if (data.done && onComplete) {
        onComplete(data);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setIsLoading(false);
    }
  }, [progressKey, onComplete]);

  useEffect(() => {
    if (!progressKey) return;

    fetchProgress();
    const interval = setInterval(fetchProgress, 2000);

    return () => clearInterval(interval);
  }, [progressKey, fetchProgress]);

  if (isLoading) {
    return <CircularProgress />;
  }

  if (error) {
    return <Alert severity="error">Error: {error}</Alert>;
  }

  return (
    <div>
      <Typography variant="h6">
        Progress: {progress?.percent || 0}%
      </Typography>
      <LinearProgress 
        variant="determinate" 
        value={progress?.percent || 0} 
      />
    </div>
  );
};

export default ProgressDisplay;
```

### Security Guidelines

#### Secure Coding Practices

##### Input Validation
```python
# Always validate and sanitize user inputs
def validate_csv_format_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """Validate CSV format parameters from user input."""
    validated = {}
    
    # Delimiter validation
    allowed_delimiters = [',', ';', '|', '\t']
    if 'column_delimiter' in params:
        delimiter = params['column_delimiter']
        if delimiter in allowed_delimiters:
            validated['column_delimiter'] = delimiter
        else:
            raise ValueError(f"Invalid delimiter. Allowed: {allowed_delimiters}")
    
    # Numeric validation
    if 'skip_lines' in params:
        try:
            skip = int(params['skip_lines'])
            if 0 <= skip <= 100:
                validated['skip_lines'] = skip
            else:
                raise ValueError("Skip lines must be between 0 and 100")
        except ValueError:
            raise ValueError("Skip lines must be a valid integer")
    
    return validated
```

##### SQL Security
```python
# ALWAYS use parameterized queries
def get_table_columns(self, table_name: str) -> List[Dict]:
    """Get table column information using parameterized query."""
    # Validate identifier first
    if not self.validate_sql_identifier(table_name):
        raise ValueError("Invalid table name")
    
    sql = """
    SELECT 
        COLUMN_NAME as column_name,
        DATA_TYPE as data_type,
        CHARACTER_MAXIMUM_LENGTH as max_length,
        IS_NULLABLE as is_nullable
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    ORDER BY ORDINAL_POSITION
    """
    
    cursor = self.get_connection().cursor()
    cursor.execute(sql, (self.data_schema, table_name))
    return [dict(zip([col[0] for col in cursor.description], row)) 
            for row in cursor.fetchall()]

# NEVER do this (vulnerable to injection):
# sql = f"SELECT * FROM {table_name} WHERE id = {user_id}"
```

##### File Security
```python
def get_safe_file_path(self, base_dir: str, filename: str) -> str:
    """Generate safe file path preventing directory traversal."""
    # Sanitize filename
    safe_name = os.path.basename(filename)
    safe_name = re.sub(r'[^\w\.-]', '_', safe_name)
    
    # Construct safe path
    safe_path = os.path.join(base_dir, safe_name)
    
    # Verify path is within base directory
    if not safe_path.startswith(os.path.abspath(base_dir)):
        raise ValueError("Invalid file path - potential directory traversal")
    
    return safe_path
```

## Testing Framework

### Test Structure

#### Backend Testing
```python
# tests/conftest.py - Test configuration
import pytest
import tempfile
import os
from utils.database import DatabaseManager
from utils.logger import DatabaseLogger

@pytest.fixture
def temp_dir():
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir

@pytest.fixture  
def test_db():
    """Create test database connection."""
    # Use test database settings
    os.environ['db_name'] = 'reference_data_test'
    db_manager = DatabaseManager()
    yield db_manager
    # Cleanup after tests

@pytest.fixture
def sample_csv_file(temp_dir):
    """Create sample CSV file for testing."""
    csv_content = """name,age,city
John Doe,30,New York
Jane Smith,25,London
Bob Johnson,35,Paris"""
    
    csv_path = os.path.join(temp_dir, 'sample.csv')
    with open(csv_path, 'w') as f:
        f.write(csv_content)
    
    return csv_path
```

```python
# tests/test_database.py - Database tests
import pytest
import pyodbc
from utils.database import DatabaseManager

class TestDatabaseManager:
    """Test database operations."""
    
    def test_connection_creation(self, test_db):
        """Test database connection creation."""
        conn = test_db.get_connection()
        assert conn is not None
        assert isinstance(conn, pyodbc.Connection)
        conn.close()
    
    def test_table_exists(self, test_db):
        """Test table existence check."""
        conn = test_db.get_connection()
        
        # Test non-existent table
        assert not test_db.table_exists(conn, 'non_existent_table')
        
        # Create test table
        conn.execute("""
        CREATE TABLE test_table (
            id int PRIMARY KEY,
            name varchar(100)
        )
        """)
        
        # Test existing table
        assert test_db.table_exists(conn, 'test_table')
        
        conn.close()
    
    def test_sql_injection_prevention(self, test_db):
        """Test SQL injection prevention in table operations."""
        conn = test_db.get_connection()
        
        # Attempt SQL injection
        malicious_input = "test'; DROP TABLE users; --"
        
        # Should not raise exception or execute malicious SQL
        result = test_db.table_exists(conn, malicious_input)
        assert result is False
        
        conn.close()
```

```python
# tests/test_file_handler.py - File handling tests
import pytest
import os
from utils.file_handler import FileHandler

class TestFileHandler:
    """Test file handling operations."""
    
    def test_safe_filename_generation(self):
        """Test safe filename generation."""
        handler = FileHandler()
        
        # Normal filename
        assert handler.get_safe_filename('test.csv') == 'test.csv'
        
        # Filename with dangerous characters
        assert handler.get_safe_filename('../../../etc/passwd') == 'passwd'
        assert handler.get_safe_filename('test<>|.csv') == 'test___.csv'
    
    def test_table_name_extraction(self):
        """Test table name extraction from filenames."""
        handler = FileHandler()
        
        test_cases = [
            ('airports.csv', 'airports'),
            ('airports.20250810.csv', 'airports'),
            ('airports_20250810_123456.csv', 'airports'),
            ('complex-name.test.csv', 'complex-name')
        ]
        
        for filename, expected in test_cases:
            result = handler.extract_table_base_name(filename)
            assert result == expected
    
    def test_csv_format_detection(self, sample_csv_file):
        """Test CSV format detection."""
        from utils.csv_detector import CSVFormatDetector
        
        detector = CSVFormatDetector()
        result = detector.detect_format(sample_csv_file)
        
        assert 'column_delimiter' in result
        assert 'has_header' in result
        assert result['has_header'] is True
        assert result['confidence'] > 0.5
```

#### Frontend Testing
```javascript
// src/components/__tests__/FileUploadComponent.test.js
import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import FileUploadComponent from '../FileUploadComponent';

// Mock fetch
global.fetch = jest.fn();

describe('FileUploadComponent', () => {
  beforeEach(() => {
    fetch.mockClear();
  });

  test('renders file upload interface', () => {
    render(<FileUploadComponent />);
    
    expect(screen.getByText(/drag.*drop.*csv file/i)).toBeInTheDocument();
    expect(screen.getByText(/or click to select/i)).toBeInTheDocument();
  });

  test('handles file selection', async () => {
    render(<FileUploadComponent />);
    
    const file = new File(['name,age\nJohn,30'], 'test.csv', {
      type: 'text/csv',
    });
    
    const input = screen.getByLabelText(/file upload/i);
    fireEvent.change(input, { target: { files: [file] } });

    await waitFor(() => {
      expect(screen.getByText('test.csv')).toBeInTheDocument();
    });
  });

  test('validates file type', async () => {
    render(<FileUploadComponent />);
    
    const file = new File(['content'], 'test.txt', {
      type: 'text/plain',
    });
    
    const input = screen.getByLabelText(/file upload/i);
    fireEvent.change(input, { target: { files: [file] } });

    await waitFor(() => {
      expect(screen.getByText(/only csv files are supported/i)).toBeInTheDocument();
    });
  });

  test('uploads file successfully', async () => {
    fetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        message: 'File uploaded successfully',
        progress_key: 'test_csv'
      })
    });

    render(<FileUploadComponent />);
    
    const file = new File(['name,age\nJohn,30'], 'test.csv', {
      type: 'text/csv',
    });
    
    const input = screen.getByLabelText(/file upload/i);
    fireEvent.change(input, { target: { files: [file] } });
    
    const uploadButton = screen.getByText(/upload.*process/i);
    fireEvent.click(uploadButton);

    await waitFor(() => {
      expect(fetch).toHaveBeenCalledWith('/upload', expect.any(Object));
    });
  });
});
```

### Running Tests

#### Backend Tests
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=utils --cov-report=html

# Run specific test file
pytest tests/test_database.py

# Run tests with verbose output
pytest -v

# Run tests matching pattern
pytest -k "test_sql_injection"
```

#### Frontend Tests
```bash
# Run all tests
npm test

# Run tests in watch mode
npm test -- --watch

# Run tests with coverage
npm test -- --coverage

# Run specific test file
npm test FileUploadComponent.test.js
```

### Test Data Management

#### Test Database Setup
```sql
-- Create test database
CREATE DATABASE reference_data_test;
GO

USE reference_data_test;
GO

-- Create test schemas
CREATE SCHEMA [ref];
CREATE SCHEMA [bkp];
GO

-- Create test data
INSERT INTO [ref].[test_data] (name, value) VALUES 
('config1', 'value1'),
('config2', 'value2');
```

#### Mock Data Generation
```python
# tests/fixtures/data_generator.py
import random
import string
from datetime import datetime, timedelta

def generate_csv_data(rows=100, columns=5):
    """Generate sample CSV data for testing."""
    headers = [f'column_{i}' for i in range(1, columns + 1)]
    data = [headers]
    
    for _ in range(rows):
        row = []
        for col in range(columns):
            if col == 0:  # ID column
                row.append(str(random.randint(1, 10000)))
            elif col == 1:  # Name column
                name = ''.join(random.choices(string.ascii_letters, k=10))
                row.append(name)
            elif col == 2:  # Date column
                date = datetime.now() - timedelta(days=random.randint(0, 365))
                row.append(date.strftime('%Y-%m-%d'))
            else:  # Random data
                row.append(''.join(random.choices(string.ascii_letters + string.digits, k=15)))
        data.append(row)
    
    return data

def save_test_csv(data, filename):
    """Save test data to CSV file."""
    import csv
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)
```

## API Development

### FastAPI Best Practices

#### Endpoint Structure
```python
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from typing import Optional

app = FastAPI()

# Request/Response models
class UploadRequest(BaseModel):
    load_mode: str = "full"
    header_delimiter: str = "|"
    column_delimiter: str = "|"
    
class UploadResponse(BaseModel):
    message: str
    filename: str
    progress_key: str
    status: str

# Dependency injection
async def get_database():
    db = DatabaseManager()
    try:
        yield db
    finally:
        # Cleanup if needed
        pass

# Endpoint implementation
@app.post("/upload", response_model=UploadResponse)
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    request: UploadRequest = Depends(),
    db: DatabaseManager = Depends(get_database)
):
    """
    Upload CSV file for processing.
    
    - **file**: CSV file to upload (max 20MB)
    - **load_mode**: Processing mode (full or append)
    - **header_delimiter**: Delimiter for header row
    """
    try:
        # File validation
        if not file.filename.endswith('.csv'):
            raise HTTPException(400, "Only CSV files supported")
        
        # Process upload
        result = await process_upload(file, request, db, background_tasks)
        
        return UploadResponse(
            message="File uploaded successfully",
            filename=file.filename,
            progress_key=result['key'],
            status="processing"
        )
    
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(500, f"Upload failed: {str(e)}")
```

#### Error Handling
```python
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

# Custom exception classes
class DatabaseError(Exception):
    """Database operation error."""
    pass

class ValidationError(Exception):
    """Input validation error."""
    pass

# Global exception handler
@app.exception_handler(DatabaseError)
async def database_error_handler(request: Request, exc: DatabaseError):
    return JSONResponse(
        status_code=500,
        content={"detail": f"Database error: {str(exc)}"}
    )

@app.exception_handler(ValidationError)
async def validation_error_handler(request: Request, exc: ValidationError):
    return JSONResponse(
        status_code=400,
        content={"detail": f"Validation error: {str(exc)}"}
    )
```

## Database Development

### Schema Migration System

#### Migration Structure
```python
# utils/migrations.py
from abc import ABC, abstractmethod
from typing import List

class Migration(ABC):
    """Base migration class."""
    
    @property
    @abstractmethod
    def version(self) -> str:
        """Migration version identifier."""
        pass
    
    @abstractmethod
    async def up(self, connection):
        """Apply migration."""
        pass
    
    @abstractmethod
    async def down(self, connection):
        """Revert migration."""
        pass

class Migration_001_Initial(Migration):
    """Initial database schema."""
    
    @property
    def version(self) -> str:
        return "001"
    
    async def up(self, connection):
        """Create initial schema."""
        cursor = connection.cursor()
        
        # Create schemas
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ref') EXEC('CREATE SCHEMA [ref]')")
        cursor.execute("IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bkp') EXEC('CREATE SCHEMA [bkp]')")
        
        # Create system log table
        cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'system_log' AND schema_id = SCHEMA_ID('ref'))
        BEGIN
            CREATE TABLE [ref].[system_log] (
                [id] bigint IDENTITY(1,1) PRIMARY KEY,
                [timestamp] datetime2 DEFAULT GETDATE(),
                [level] varchar(20) NOT NULL,
                [module] varchar(100) NOT NULL,
                [message] nvarchar(max) NOT NULL,
                [details] nvarchar(max) NULL
            );
        END
        """)
        
        connection.commit()
    
    async def down(self, connection):
        """Revert initial schema."""
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS [ref].[system_log]")
        cursor.execute("DROP SCHEMA IF EXISTS [bkp]")
        cursor.execute("DROP SCHEMA IF EXISTS [ref]")
        connection.commit()

class MigrationRunner:
    """Migration execution system."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.migrations: List[Migration] = [
            Migration_001_Initial()
        ]
    
    async def get_current_version(self) -> str:
        """Get current database version."""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM [ref].[app_metadata] WHERE [key] = 'db_version'")
            result = cursor.fetchone()
            return result[0] if result else "000"
        except:
            return "000"
    
    async def run_migrations(self):
        """Run pending migrations."""
        current_version = await self.get_current_version()
        
        for migration in self.migrations:
            if migration.version > current_version:
                print(f"Running migration {migration.version}")
                conn = self.db_manager.get_connection()
                try:
                    await migration.up(conn)
                    # Update version
                    cursor = conn.cursor()
                    cursor.execute(
                        "UPDATE [ref].[app_metadata] SET value = ? WHERE [key] = 'db_version'",
                        (migration.version,)
                    )
                    conn.commit()
                    print(f"Migration {migration.version} completed")
                except Exception as e:
                    conn.rollback()
                    print(f"Migration {migration.version} failed: {e}")
                    raise
                finally:
                    conn.close()
```

### Database Development Tools

#### Database Debugging
```python
# utils/db_debug.py
import time
from contextlib import contextmanager

class DatabaseProfiler:
    """Database query profiler for development."""
    
    def __init__(self):
        self.queries = []
    
    @contextmanager
    def profile_query(self, query_name: str):
        """Profile query execution time."""
        start_time = time.perf_counter()
        try:
            yield
        finally:
            end_time = time.perf_counter()
            duration = end_time - start_time
            self.queries.append({
                'name': query_name,
                'duration': duration,
                'timestamp': time.time()
            })
            print(f"Query {query_name}: {duration:.4f}s")
    
    def get_slow_queries(self, threshold: float = 1.0):
        """Get queries slower than threshold."""
        return [q for q in self.queries if q['duration'] > threshold]

# Usage in development
profiler = DatabaseProfiler()

def get_table_data(table_name: str):
    with profiler.profile_query(f"select_from_{table_name}"):
        # Execute query
        pass
```

## Debugging & Development Tools

### Logging Configuration

#### Development Logging
```python
# config/logging.py
import logging
import sys
from pathlib import Path

def setup_development_logging():
    """Configure logging for development environment."""
    
    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            # Console handler for development
            logging.StreamHandler(sys.stdout),
            # File handler for persistence
            logging.FileHandler(log_dir / "development.log")
        ]
    )
    
    # Configure specific loggers
    logging.getLogger("uvicorn").setLevel(logging.INFO)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    
    # Custom logger for application
    app_logger = logging.getLogger("reference_data_mgr")
    app_logger.setLevel(logging.DEBUG)
    
    return app_logger

# Use in development
logger = setup_development_logging()
```

### Development Utilities

#### Database Reset Script
```python
# scripts/reset_dev_database.py
"""Reset development database to clean state."""
import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

from utils.database import DatabaseManager
from utils.migrations import MigrationRunner

async def reset_database():
    """Reset development database."""
    db_manager = DatabaseManager()
    
    try:
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        
        # Drop all user tables
        cursor.execute("""
        SELECT TABLE_SCHEMA, TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA IN ('ref', 'bkp')
        """)
        
        tables = cursor.fetchall()
        for schema, table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
        
        conn.commit()
        print("Database reset completed")
        
        # Run migrations
        migration_runner = MigrationRunner(db_manager)
        await migration_runner.run_migrations()
        print("Migrations completed")
        
    except Exception as e:
        print(f"Database reset failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    import asyncio
    asyncio.run(reset_database())
```

#### Test Data Generator
```python
# scripts/generate_test_data.py
"""Generate test data for development."""
import csv
import random
import os
from datetime import datetime, timedelta

def generate_airports_data(count=1000):
    """Generate sample airports data."""
    data = [['code', 'name', 'city', 'country', 'latitude', 'longitude']]
    
    cities = ['New York', 'London', 'Paris', 'Tokyo', 'Sydney', 'Toronto']
    countries = ['USA', 'UK', 'France', 'Japan', 'Australia', 'Canada']
    
    for i in range(count):
        code = f"{random.choice(['LAX', 'JFK', 'LHR', 'CDG', 'NRT'])}{i:03d}"
        name = f"Sample Airport {i+1}"
        city = random.choice(cities)
        country = random.choice(countries)
        lat = round(random.uniform(-90, 90), 6)
        lng = round(random.uniform(-180, 180), 6)
        
        data.append([code, name, city, country, lat, lng])
    
    return data

def save_test_file(filename, data):
    """Save test data to CSV file."""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerows(data)
    
    print(f"Generated {filename} with {len(data)-1} records")

if __name__ == "__main__":
    # Create test data directory
    os.makedirs('test_data', exist_ok=True)
    
    # Generate different test datasets
    save_test_file('test_data/airports.csv', generate_airports_data(1000))
    save_test_file('test_data/large_airports.csv', generate_airports_data(10000))
```

## Performance Optimization

### Development Performance Tools

#### Query Performance Monitoring
```python
# utils/performance.py
import time
import functools
from typing import Dict, List

class PerformanceMonitor:
    """Monitor application performance during development."""
    
    def __init__(self):
        self.metrics: Dict[str, List[float]] = {}
    
    def time_function(self, func_name: str = None):
        """Decorator to time function execution."""
        def decorator(func):
            name = func_name or f"{func.__module__}.{func.__name__}"
            
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start_time = time.perf_counter()
                try:
                    result = func(*args, **kwargs)
                    return result
                finally:
                    end_time = time.perf_counter()
                    duration = end_time - start_time
                    
                    if name not in self.metrics:
                        self.metrics[name] = []
                    self.metrics[name].append(duration)
                    
                    if duration > 1.0:  # Log slow functions
                        print(f"SLOW: {name} took {duration:.4f}s")
            
            return wrapper
        return decorator
    
    def get_performance_report(self) -> Dict:
        """Generate performance report."""
        report = {}
        for func_name, times in self.metrics.items():
            report[func_name] = {
                'count': len(times),
                'total_time': sum(times),
                'avg_time': sum(times) / len(times),
                'min_time': min(times),
                'max_time': max(times)
            }
        return report

# Global performance monitor
perf_monitor = PerformanceMonitor()

# Usage
@perf_monitor.time_function("csv_processing")
def process_csv_file(file_path: str):
    # Function implementation
    pass
```

### Memory Management

#### Memory Profiling
```python
# utils/memory_profiler.py
import tracemalloc
import gc
from typing import Optional

class MemoryProfiler:
    """Memory usage profiler for development."""
    
    def __init__(self):
        self.snapshots = {}
    
    def start_profiling(self):
        """Start memory profiling."""
        tracemalloc.start()
    
    def take_snapshot(self, name: str):
        """Take memory snapshot."""
        if not tracemalloc.is_tracing():
            return
        
        snapshot = tracemalloc.take_snapshot()
        self.snapshots[name] = snapshot
        
        top_stats = snapshot.statistics('lineno')[:5]
        print(f"Memory snapshot '{name}':")
        for stat in top_stats:
            print(f"  {stat}")
    
    def compare_snapshots(self, name1: str, name2: str):
        """Compare two memory snapshots."""
        if name1 not in self.snapshots or name2 not in self.snapshots:
            return
        
        snapshot1 = self.snapshots[name1]
        snapshot2 = self.snapshots[name2]
        
        top_stats = snapshot2.compare_to(snapshot1, 'lineno')[:5]
        print(f"Memory comparison '{name1}' -> '{name2}':")
        for stat in top_stats:
            print(f"  {stat}")
    
    def force_gc(self):
        """Force garbage collection."""
        collected = gc.collect()
        print(f"Garbage collection freed {collected} objects")

# Usage in development
memory_profiler = MemoryProfiler()
memory_profiler.start_profiling()

def process_large_file(file_path: str):
    memory_profiler.take_snapshot("before_processing")
    
    # Process file
    data = read_large_csv(file_path)
    process_data(data)
    
    memory_profiler.take_snapshot("after_processing")
    memory_profiler.compare_snapshots("before_processing", "after_processing")
    
    # Clean up
    del data
    memory_profiler.force_gc()
```

## Build and Release Process

### Development Build Scripts

#### Backend Build
```bash
#!/bin/bash
# scripts/build_backend.sh

set -e  # Exit on error

echo "Building backend..."

# Activate virtual environment
source venv/bin/activate

# Install/update dependencies
pip install -r requirements.txt

# Run linting
echo "Running code quality checks..."
flake8 utils/ app/ --max-line-length=100
black --check utils/ app/
isort --check-only utils/ app/

# Run tests
echo "Running tests..."
pytest --cov=utils --cov-report=term-missing

# Security checks
pip-audit

echo "Backend build completed successfully!"
```

#### Frontend Build
```bash
#!/bin/bash
# scripts/build_frontend.sh

set -e

echo "Building frontend..."

cd frontend

# Install dependencies
npm ci

# Run linting
npm run lint

# Run tests
npm test -- --coverage --watchAll=false

# Build production bundle
npm run build

# Analyze bundle size
npm run analyze

echo "Frontend build completed successfully!"
```

### Release Process

#### Version Management
```python
# scripts/bump_version.py
"""Version management script."""
import re
import sys
from pathlib import Path

def get_current_version():
    """Get current version from package.json."""
    package_json = Path("frontend/package.json")
    content = package_json.read_text()
    match = re.search(r'"version":\s*"([^"]+)"', content)
    return match.group(1) if match else "0.0.0"

def bump_version(version_type: str):
    """Bump version number."""
    current = get_current_version()
    major, minor, patch = map(int, current.split('.'))
    
    if version_type == 'major':
        major += 1
        minor = 0
        patch = 0
    elif version_type == 'minor':
        minor += 1
        patch = 0
    elif version_type == 'patch':
        patch += 1
    else:
        raise ValueError("Version type must be 'major', 'minor', or 'patch'")
    
    new_version = f"{major}.{minor}.{patch}"
    
    # Update package.json
    package_json = Path("frontend/package.json")
    content = package_json.read_text()
    updated = re.sub(
        r'"version":\s*"[^"]+"',
        f'"version": "{new_version}"',
        content
    )
    package_json.write_text(updated)
    
    # Update backend version
    main_py = Path("backend/app/main.py")
    content = main_py.read_text()
    updated = re.sub(
        r'version="[^"]+"',
        f'version="{new_version}"',
        content
    )
    main_py.write_text(updated)
    
    return new_version

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python bump_version.py <major|minor|patch>")
        sys.exit(1)
    
    version_type = sys.argv[1]
    new_version = bump_version(version_type)
    print(f"Version bumped to {new_version}")
```

#### Release Checklist
```markdown
# Release Checklist

## Pre-Release
- [ ] All tests passing
- [ ] Code review completed
- [ ] Security scan completed
- [ ] Performance testing completed
- [ ] Documentation updated
- [ ] Version bumped
- [ ] Changelog updated

## Release Build
- [ ] Backend build successful
- [ ] Frontend build successful
- [ ] Docker images built
- [ ] Database migrations tested
- [ ] Configuration validated

## Deployment
- [ ] Staging deployment successful
- [ ] Smoke tests passed
- [ ] Production deployment completed
- [ ] Health checks passing
- [ ] Monitoring configured

## Post-Release
- [ ] Release notes published
- [ ] Team notified
- [ ] Monitoring verified
- [ ] Backup verified
- [ ] Next milestone planned
```

This comprehensive development guide provides all the necessary information for developers to effectively contribute to the Reference Data Auto Ingest System. It covers setup, coding standards, testing, debugging, performance optimization, and the complete build and release process.