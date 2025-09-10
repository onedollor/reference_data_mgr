# Project Structure

## Directory Organization

```
reference_data_mgr/
├── .spec-workflow/              # Project management and specifications
│   ├── steering/               # Project steering documents
│   │   ├── product.md         # Product vision and goals
│   │   ├── tech.md            # Technology decisions  
│   │   └── structure.md       # This file - codebase structure
│   ├── specs/                 # Feature specifications
│   │   └── reference-data-management/
│   │       ├── requirements.md # Feature requirements
│   │       ├── design.md      # Technical design
│   │       └── tasks.md       # Implementation tasks
│   └── approvals/             # Approval workflow data
├── backend/                    # Core application backend
│   ├── backend_lib.py         # Main unified API interface
│   ├── file_monitor.py        # File monitoring service
│   └── utils/                 # Specialized utility modules
│       ├── __init__.py        # Package initialization
│       ├── database.py        # Database connection management
│       ├── ingest.py          # Data ingestion engine  
│       ├── csv_detector.py    # CSV format detection
│       ├── file_handler.py    # File operations and naming
│       ├── logger.py          # Logging and audit utilities
│       └── progress.py        # Progress tracking utilities
├── config/                     # Configuration files (optional)
├── data/                       # Data directories and file structure
│   └── reference_data/
│       └── dropoff/           # Structured dropoff folders
│           ├── reference_data_table/    # Files requiring config records
│           └── none_reference_data_table/  # Files without config records
├── logs/                       # Application log files
├── sql/                        # Database scripts and schemas
├── .env                        # Environment configuration
├── start_monitor.sh           # Service startup script
├── test_integration.py        # Integration test suite
└── README.md                  # Project documentation
```

## Naming Conventions

### Files
- **Python Modules**: `snake_case` (e.g., `backend_lib.py`, `file_monitor.py`, `csv_detector.py`)
- **Python Classes**: `PascalCase` (e.g., `ReferenceDataAPI`, `DatabaseManager`, `CSVFormatDetector`)
- **Configuration Files**: `lowercase` with extensions (e.g., `.env`, `requirements.txt`)
- **Scripts**: `snake_case` with `.py` or `.sh` extensions (e.g., `start_monitor.sh`, `test_integration.py`)

### Code
- **Classes/Types**: `PascalCase` (e.g., `FileMonitor`, `DataIngester`, `DatabaseManager`)
- **Functions/Methods**: `snake_case` (e.g., `process_file`, `check_file_stability`, `detect_format`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `MONITOR_INTERVAL`, `STABILITY_CHECKS`, `DROPOFF_BASE_PATH`)
- **Variables**: `snake_case` (e.g., `file_path`, `load_type`, `is_reference_data`)

### Database Objects
- **Schemas**: `lowercase` (e.g., `ref`, `bkp`)
- **Tables**: `PascalCase` with underscores (e.g., `File_Monitor_Tracking`, `Reference_Data_Cfg`)
- **Columns**: `snake_case` (e.g., `file_path`, `created_at`, `is_reference_data`)

## Import Patterns

### Import Order
1. **Standard Library**: Built-in Python modules (os, sys, time, logging, etc.)
2. **Third-Party Dependencies**: External packages (pyodbc, pandas, asyncio extensions)
3. **Local Utilities**: Internal utility modules from utils/ package
4. **Local Modules**: Same-level or parent modules

### Module Organization Example
```python
# Standard library imports
import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Third-party imports  
import pyodbc
import pandas as pd

# Local utility imports
from utils.database import DatabaseManager
from utils.logger import Logger
from utils.progress import init_progress

# Local module imports
from backend_lib import ReferenceDataAPI
```

## Code Structure Patterns

### Module/Class Organization
```python
# File header with docstring
"""
Module description and purpose
"""

# 1. Imports (following import order above)
# 2. Module-level constants and configuration
MONITOR_INTERVAL = 15
STABILITY_CHECKS = 6

# 3. Main class definitions
class FileMonitor:
    def __init__(self):
        # Initialization
        
    def public_method(self):
        # Public interface methods first
        
    def _private_method(self):
        # Private methods after public ones

# 4. Module-level utility functions
def extract_table_name(filename: str) -> str:
    # Standalone utility functions

# 5. Main execution block
if __name__ == "__main__":
    main()
```

### Function/Method Organization
```python
def process_file(self, file_path: str, load_type: str, is_reference_data: bool):
    """
    Clear docstring describing function purpose and parameters
    """
    # 1. Input validation and logging
    self.logger.info(f"Processing file: {file_path}")
    
    try:
        # 2. Core logic implementation
        result = self._perform_processing(file_path, load_type)
        
        # 3. Success handling and cleanup
        self._move_to_processed(file_path)
        return result
        
    except Exception as e:
        # 4. Error handling and recovery
        self._handle_error(file_path, e)
        raise
```

### File Organization Principles
- **One primary class per file**: Each module focuses on a single main responsibility
- **Related functionality grouped**: Helper methods and utilities stay close to their usage
- **Public API at top**: Most important methods appear first in class definition
- **Implementation details hidden**: Private methods use underscore prefix and appear after public methods

## Code Organization Principles

1. **Single Responsibility**: Each module has one clear purpose (database.py = connection management, ingest.py = data processing)
2. **Modularity**: Utilities are isolated and reusable across the system (csv_detector, file_handler, logger)
3. **Testability**: Dependencies injected through constructors, clear interfaces between components
4. **Consistency**: All modules follow the same organizational patterns and naming conventions

## Module Boundaries

### Core vs Utilities Separation
- **Core Modules** (backend_lib.py, file_monitor.py): Main business logic and orchestration
- **Utility Modules** (utils/*): Reusable components with single responsibilities
- **Configuration Boundary**: Environment variables and .env file for all external configuration

### Dependencies Direction
```
file_monitor.py → backend_lib.py → utils/* modules
    ↓                ↓                ↓
startup script    API interface    specialized functions

No circular dependencies - utilities never import from core modules
```

### Public API vs Internal Implementation
- **Public API**: ReferenceDataAPI class methods, module-level convenience functions
- **Internal Implementation**: Private methods, utility classes, file processing logic
- **Configuration Interface**: Environment variables, command-line arguments, .env file

## Code Size Guidelines

- **File Size**: Maximum 500 lines per Python file - split larger files into focused modules
- **Function/Method Size**: Maximum 50 lines per function - break complex logic into smaller functions
- **Class Complexity**: Maximum 10-15 public methods per class - consider composition over inheritance  
- **Nesting Depth**: Maximum 4 levels of indentation - use early returns and guard clauses

## Database/Storage Structure
### Database Schema Organization
```sql
-- ref schema: Primary data and tracking
ref.File_Monitor_Tracking        # File processing audit trail
ref.Reference_Data_Cfg          # Reference data configuration
ref.[dynamic_table_names]       # Actual data tables from CSV files

-- bkp schema: Backup and recovery
bkp.[table_name_backup]         # Backup copies for fullload operations
```

### File System Organization
```
data/reference_data/dropoff/
├── reference_data_table/        # Files requiring config record insertion
│   ├── fullload/               # Truncate and reload operations
│   └── append/                 # Incremental data additions
└── none_reference_data_table/   # Files without config requirements  
    ├── fullload/
    └── append/
```

## Documentation Standards
- **All public APIs**: Comprehensive docstrings with parameter descriptions and return values
- **Complex Logic**: Inline comments explaining business rules and processing steps
- **Module Headers**: File-level docstrings describing module purpose and main classes
- **Configuration**: Environment variable documentation in .env file comments
- **README Files**: Project-level documentation for setup and usage instructions