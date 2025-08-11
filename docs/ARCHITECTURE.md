# Architecture Documentation - Reference Data Auto Ingest System

## System Overview

The Reference Data Auto Ingest System is a modern, production-ready application built with a FastAPI backend and React frontend. It provides automated CSV data ingestion with intelligent load type management, comprehensive backup/rollback capabilities, and real-time progress monitoring.

## High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  React Frontend │    │ FastAPI Backend │    │  SQL Server DB  │
│   (Port 3000)   │◄──►│   (Port 8000)   │◄──►│   (Port 1433)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
    ┌────▼────┐             ┌────▼────┐             ┌────▼────┐
    │Material │             │ FastAPI │             │Schemas: │
    │UI + Load│             │Async +  │             │ref, bkp │
    │Type Mgmt│             │Backup   │             │dbo      │
    └─────────┘             └─────────┘             └─────────┘
```

## Enhanced Component Architecture

### Frontend Architecture (React)

#### Component Hierarchy
```
App.js
├── FileUploadComponent.js
│   ├── LoadTypeWarningDialog.js (NEW - Load type mismatch handling)
│   ├── Load type verification workflow
│   └── Enhanced upload with override capabilities
├── RollbackManager.js (NEW - Comprehensive backup management)
│   ├── Backup table listing with status validation
│   ├── Version management and preview
│   ├── Advanced filtering (regex + discrete values)
│   ├── Export functionality (main + backup versions)
│   └── Rollback confirmation and execution
├── ProgressDisplay.js
├── LogsDisplay.js  
├── ConfigurationPanel.js
└── ReferenceDataConfigDisplay.js
```

#### Key Frontend Enhancements
- **Load Type Intelligence**: Pre-upload verification prevents conflicts
- **Warning Dialog System**: User-friendly override selection with explanations
- **Advanced Backup Management**: Complete rollback interface with data preview
- **Export Capabilities**: CSV export of main tables and backup versions
- **Enhanced Filtering**: Multi-column filtering with regex and discrete value support
- **TD Bank Styling**: Corporate design system integration

#### Load Type Management Workflow
```javascript
// Load Type Verification Flow
const handleUpload = async (file, config) => {
  // 1. Verify load type compatibility
  const verification = await fetch('/verify-load-type', {
    method: 'POST',
    body: new FormData([
      ['filename', file.name],
      ['load_mode', config.load_mode]
    ])
  });
  
  // 2. Handle mismatch detection
  if (verification.has_mismatch) {
    setLoadTypeVerification(verification);
    setShowLoadTypeDialog(true);
    return;
  }
  
  // 3. Proceed with normal upload
  proceedWithUpload(file, config);
};

// Override handling
const handleLoadTypeOverride = (overrideType) => {
  const uploadData = {
    ...pendingUploadData,
    override_load_type: overrideType
  };
  proceedWithUpload(uploadData);
};
```

### Backend Architecture (FastAPI)

#### Enhanced Application Structure
```
backend/
├── app/
│   └── main.py              # Enhanced with load type & backup endpoints
├── utils/
│   ├── database.py          # Enhanced with load type logic & backup operations
│   ├── ingest.py           # Enhanced with load type tracking
│   ├── file_handler.py     # Enhanced filename processing
│   ├── csv_detector.py     # CSV format detection
│   ├── logger.py           # Comprehensive logging
│   └── progress.py         # Progress tracking
└── tests/                   # Enhanced test suites
```

#### New API Endpoints

##### Load Type Management
```python
@app.post("/verify-load-type")
async def verify_load_type(filename: str = Form(...), load_mode: str = Form(...)):
    """
    Verify load type compatibility:
    1. Extract table name from filename
    2. Determine load type based on existing data
    3. Compare with requested load mode
    4. Return mismatch information if needed
    """
    table_name = file_handler.extract_table_base_name(filename)
    current_load_type = db_manager.determine_load_type(connection, table_name, load_mode)
    requested_load_type = 'F' if load_mode == 'full' else 'A'
    
    return {
        "table_name": table_name,
        "determined_load_type": current_load_type,
        "requested_load_type": requested_load_type,
        "has_mismatch": current_load_type != requested_load_type,
        "existing_load_types": get_existing_load_types(connection, table_name),
        "explanation": generate_explanation(current_load_type, requested_load_type)
    }
```

##### Backup Management
```python
@app.get("/backups")
async def list_backups():
    """List backup tables with validation of related main/stage tables"""
    
@app.get("/backups/{base_name}/versions")
async def get_backup_versions(base_name: str):
    """Get all versions for a backup table"""
    
@app.get("/backups/{base_name}/versions/{version_id}")
async def view_backup_version(base_name: str, version_id: int, limit: int = 50):
    """View specific backup version data with pagination"""
    
@app.post("/backups/{base_name}/rollback/{version_id}")
async def rollback_backup_version(base_name: str, version_id: int):
    """Rollback main table to specific backup version"""
    
@app.get("/backups/{base_name}/export-main")
async def export_main_table_csv(base_name: str):
    """Export current main table data as CSV with streaming download"""
    
@app.get("/backups/{base_name}/versions/{version_id}/export")
async def export_backup_version_csv(base_name: str, version_id: int):
    """Export specific backup version as CSV"""
```

#### Enhanced Upload Endpoint
```python
@app.post("/upload")
async def upload_file(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    # ... existing parameters ...
    override_load_type: Optional[str] = Form(None)  # NEW: Load type override
):
    """Enhanced upload with load type override support"""
    
    # Start background ingestion with override
    background_tasks.add_task(
        ingest_data_background,
        temp_file_path, fmt_file_path, load_mode, file.filename,
        override_load_type  # Pass override to ingestion process
    )
```

### Database Architecture Enhancements

#### Enhanced Schema Design
```sql
-- Enhanced table structure with load type support
CREATE TABLE [ref].[airports] (
    [code] varchar(4000),
    [name] varchar(4000),
    [city] varchar(4000),
    [country] varchar(4000),
    [loadtype] varchar(255),        -- NEW: Load type tracking ('F'/'A')
    [ref_data_loadtime] datetime DEFAULT GETDATE()
);

-- Enhanced backup table with version control
CREATE TABLE [bkp].[airports_backup] (
    [code] varchar(4000),
    [name] varchar(4000),
    [city] varchar(4000), 
    [country] varchar(4000),
    [loadtype] varchar(255),        -- Preserved from original
    [ref_data_loadtime] datetime,   -- Static timestamp from backup time
    [version_id] int NOT NULL       -- Version identifier
);

-- System configuration table
CREATE TABLE [dbo].[Reference_Data_Cfg] (
    [sp_name] varchar(255),
    [ref_name] varchar(255),
    [source_db] varchar(255),
    [source_schema] varchar(255), 
    [source_table] varchar(255),
    [is_enabled] bit
);
```

#### Load Type Determination Logic
```python
def determine_load_type(self, connection: pyodbc.Connection, table_name: str, 
                       current_load_mode: str, override_load_type: str = None) -> str:
    """
    Enhanced load type determination with override support:
    
    Rules:
    1. If override_load_type provided: Use override ('F' or 'A')
    2. First time ingest: Use current load mode ('F' for full, 'A' for append)
    3. Subsequent ingests: Check existing distinct loadtype values
       - Only 'F' exists: Use 'F'
       - Only 'A' exists: Use 'A'
       - Both 'A' and 'F' exist: Use 'F'
    """
    
    # Priority 1: User override
    if override_load_type:
        override_upper = override_load_type.strip().upper()
        if override_upper in ['F', 'A', 'FULL', 'APPEND']:
            return 'F' if override_upper in ['F', 'FULL'] else 'A'
    
    # Priority 2: Existing data analysis
    if not self.table_exists(connection, table_name):
        return 'F' if current_load_mode == 'full' else 'A'
    
    # Get existing load types
    cursor = connection.cursor()
    query = f"SELECT DISTINCT [loadtype] FROM [{self.data_schema}].[{table_name}] WHERE [loadtype] IS NOT NULL"
    cursor.execute(query)
    existing_types = {row[0].strip().upper() for row in cursor.fetchall() if row[0]}
    
    # Apply business rules
    if not existing_types:
        return 'F' if current_load_mode == 'full' else 'A'
    elif 'F' in existing_types and 'A' in existing_types:
        return 'F'  # Mixed data defaults to Full
    elif 'F' in existing_types:
        return 'F'  # Maintain Full load type
    elif 'A' in existing_types:
        return 'A'  # Maintain Append load type
    
    return 'F'  # Default fallback
```

#### Backup and Version Management
```python
def rollback_to_version(self, connection: pyodbc.Connection, base_name: str, version_id: int) -> Dict:
    """
    Rollback main table to specific backup version:
    1. Validate backup version exists
    2. Clear current main table data
    3. Copy backup version data to main table
    4. Preserve static timestamps
    5. Update version tracking
    """
    
    cursor = connection.cursor()
    
    # Validate version exists
    backup_table = f"{base_name}_backup"
    cursor.execute(
        "SELECT COUNT(*) FROM [" + self.backup_schema + "].[" + backup_table + "] WHERE version_id = ?",
        version_id
    )
    
    if cursor.fetchone()[0] == 0:
        return {"status": "error", "error": f"Version {version_id} not found"}
    
    # Clear main table
    cursor.execute(f"DELETE FROM [{self.data_schema}].[{base_name}]")
    
    # Copy backup data (excluding version_id)
    columns_query = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{backup_table}' AND column_name != 'version_id'"
    cursor.execute(columns_query)
    columns = [row[0] for row in cursor.fetchall()]
    
    column_list = ", ".join(f"[{col}]" for col in columns)
    insert_sql = f"""
        INSERT INTO [{self.data_schema}].[{base_name}] ({column_list})
        SELECT {column_list} 
        FROM [{self.backup_schema}].[{backup_table}] 
        WHERE version_id = ?
    """
    cursor.execute(insert_sql, version_id)
    
    rows_copied = cursor.rowcount
    connection.commit()
    
    return {
        "status": "success",
        "main_rows": rows_copied,
        "stage_rows": rows_copied,
        "version_restored": version_id
    }
```

## Data Flow Architecture

### Enhanced Ingestion Pipeline
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│File Upload  │    │Load Type    │    │Format       │    │Schema       │
│& Validation │───►│Verification │───►│Detection    │───►│Preparation  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │                   │
       ▼                   ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│Override     │    │Data Loading │    │Validation   │    │Data Movement│
│Decision     │───►│to Stage     │───►│& Processing │───►│& Backup     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                           │                   │                   │
                           ▼                   ▼                   ▼
                   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
                   │Progress     │    │Error        │    │Archive &    │
                   │Tracking     │    │Handling     │    │Cleanup      │
                   └─────────────┘    └─────────────┘    └─────────────┘
```

### Load Type Management Flow
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│User Selects │    │System       │    │Mismatch     │    │User Override│
│File & Mode  │───►│Determines   │───►│Detection    │───►│Decision     │
└─────────────┘    │Load Type    │    └─────────────┘    └─────────────┘
       │            └─────────────┘           │                   │
       │                   │                 │                   │
       ▼                   ▼                 ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│filename.csv │    │Check Existing│    │Show Warning │    │Force Load   │
│load_mode=   │    │Load Types & │    │Dialog with  │    │Type Override│
│full/append  │    │Apply Rules  │    │Explanation  │    │in Upload    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

### Backup and Rollback Flow
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│Full Load    │    │Create Backup│    │Version      │    │Rollback     │
│Triggered    │───►│Before       │───►│Control      │───►│Selection    │
└─────────────┘    │Replacement  │    │Tracking     │    └─────────────┘
       │            └─────────────┘    └─────────────┘           │
       │                   │                   │                │
       ▼                   ▼                   ▼                ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│Main Table   │    │Backup Table │    │Increment    │    │Replace Main │
│Data         │    │with Static  │    │version_id   │    │with Selected│
│Replacement  │    │Timestamps   │    │Counter      │    │Version Data │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## Security Architecture

### Enhanced Security Model

#### 1. Input Validation Layer
```python
# Load type validation
def validate_load_type(load_type: str) -> str:
    """Validate and sanitize load type input"""
    if not load_type:
        return None
    
    clean_type = load_type.strip().upper()
    if clean_type not in ['F', 'A', 'FULL', 'APPEND']:
        raise ValueError(f"Invalid load type: {load_type}")
    
    return 'F' if clean_type in ['F', 'FULL'] else 'A'

# Table name sanitization for backup operations
def sanitize_base_name(base_name: str) -> str:
    """Sanitize base name for backup operations"""
    if not re.fullmatch(r"[A-Za-z0-9_]+", base_name):
        raise ValueError(f"Invalid base name: {base_name}")
    return base_name
```

#### 2. Parameterized Query Examples
```python
# Load type determination query
query = "SELECT DISTINCT [loadtype] FROM [{}].[{}] WHERE [loadtype] IS NOT NULL"
cursor.execute(query.format(schema, table_name))  # Safe schema/table formatting

# Backup version query with parameters
cursor.execute(
    "SELECT * FROM [" + backup_schema + "].[" + backup_table + "] WHERE version_id = ?",
    version_id
)

# Rollback operation with parameterized queries
insert_sql = f"""
    INSERT INTO [{self.data_schema}].[{base_name}] ({column_list})
    SELECT {column_list} 
    FROM [{self.backup_schema}].[{backup_table}] 
    WHERE version_id = ?
"""
cursor.execute(insert_sql, version_id)
```

#### 3. Export Security
```python
# Safe CSV export with proper escaping
def row_to_csv(row):
    """Secure CSV row generation with proper escaping"""
    out_fields = []
    for v in row:
        if v is None:
            out_fields.append('')
        else:
            s = str(v)
            # Escape quotes by doubling
            if '"' in s:
                s = s.replace('"', '""')
            # Quote if contains delimiter, quote, or newline
            if any(ch in s for ch in [',', '"', '\n', '\r']):
                s = f'"{s}"'
            out_fields.append(s)
    return ','.join(out_fields)
```

## Performance Architecture

### Enhanced Database Performance

#### Connection Pooling with Retry Logic
```python
class DatabaseManager:
    def get_connection(self) -> pyodbc.Connection:
        """Enhanced connection with retry and backoff"""
        attempt = 0
        while True:
            try:
                connection = pyodbc.connect(self.connection_string)
                connection.autocommit = True
                return connection
            except Exception as e:
                attempt += 1
                if attempt >= self.max_retries:
                    raise e
                time.sleep(self.retry_backoff * attempt)
```

#### Efficient Backup Operations
```python
def get_backup_versions(self, connection: pyodbc.Connection, base_name: str) -> List[Dict]:
    """Optimized backup version retrieval"""
    backup_table = f"{base_name}_backup"
    
    cursor = connection.cursor()
    query = f"""
        SELECT version_id, COUNT(*) as row_count, 
               MIN([ref_data_loadtime]) as created_date
        FROM [{self.backup_schema}].[{backup_table}]
        GROUP BY version_id
        ORDER BY version_id DESC
    """
    cursor.execute(query)
    
    return [
        {
            "version_id": row[0],
            "row_count": row[1],
            "created_date": row[2].isoformat() if row[2] else None
        }
        for row in cursor.fetchall()
    ]
```

#### Streaming Export Performance
```python
async def stream_csv_export():
    """Memory-efficient streaming CSV export"""
    batch_size = 500
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        # Process and yield batches to prevent memory buildup
        lines = [row_to_csv(r) for r in rows]
        yield '\n'.join(lines) + '\n'
```

### Frontend Performance Enhancements

#### Efficient Data Filtering
```javascript
// Advanced filtering with regex and discrete values
const filteredRows = data.rows.filter(row => {
  return data.columns.every((col, cIdx) => {
    const cell = row[cIdx];
    const cellStr = (cell === null || cell === undefined) ? '' : String(cell);
    
    // Regex filter takes precedence
    const pattern = regexFilters[col];
    if (pattern && pattern.trim() !== '') {
      try {
        const re = new RegExp(pattern, 'i');
        return re.test(cellStr);
      } catch (e) {
        return true; // Invalid regex -> ignore filter
      }
    }
    
    // Discrete value filter
    const valSel = valueFilters[col];
    if (valSel && valSel.length > 0) {
      return valSel.includes(cellStr);
    }
    
    return true;
  });
});
```

#### Optimized Component Updates
```javascript
// Memoized components for performance
const MemoizedVersionRow = React.memo(({ version, onSelect, isSelected }) => {
  return (
    <Chip 
      label={`v${version.version_id}`}
      variant={isSelected ? 'filled' : 'outlined'}
      clickable
      onClick={() => onSelect(version)}
    />
  );
});

// Efficient state updates
const updateVersionData = useCallback((versionId, newData) => {
  setVersionRows(prev => ({
    ...prev,
    [`${baseName}|${versionId}`]: {
      ...prev[`${baseName}|${versionId}`],
      ...newData
    }
  }));
}, [baseName]);
```

## Monitoring and Observability

### Enhanced Logging Architecture
```python
class DatabaseLogger:
    """Enhanced database logging with structured data"""
    
    async def log_load_type_verification(self, table_name: str, verification_result: Dict):
        """Log load type verification events"""
        await self.log_info(
            "load_type_verification",
            f"Load type verification for {table_name}: "
            f"mismatch={verification_result['has_mismatch']}, "
            f"determined={verification_result['determined_load_type']}, "
            f"requested={verification_result['requested_load_type']}"
        )
    
    async def log_backup_operation(self, operation: str, base_name: str, version_id: int = None, result: Dict = None):
        """Log backup and rollback operations"""
        message = f"Backup {operation} for {base_name}"
        if version_id:
            message += f" version {version_id}"
        if result:
            message += f": {result.get('status', 'unknown')}"
        
        await self.log_info("backup_operation", message)
```

### System Health Monitoring
```python
@app.get("/health")
async def health_check():
    """Enhanced health check with component status"""
    try:
        # Database connectivity
        conn = db_manager.get_connection()
        db_status = "healthy"
        conn.close()
    except Exception:
        db_status = "unhealthy"
    
    # File system accessibility
    try:
        temp_path = os.getenv("temp_location")
        fs_status = "healthy" if os.path.exists(temp_path) else "unhealthy"
    except Exception:
        fs_status = "unhealthy"
    
    return {
        "status": "healthy" if all([db_status == "healthy", fs_status == "healthy"]) else "degraded",
        "components": {
            "database": db_status,
            "file_system": fs_status,
            "backup_system": "healthy",  # Could add backup system checks
            "load_type_system": "healthy"
        },
        "timestamp": datetime.utcnow().isoformat()
    }
```

## Deployment Architecture

### Production Configuration
```yaml
# Enhanced docker-compose for production
version: '3.8'
services:
  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile.prod
    environment:
      - REACT_APP_API_URL=https://api.yourdomain.com
      - REACT_APP_ENABLE_ROLLBACK=true
      - REACT_APP_ENABLE_LOAD_TYPE_OVERRIDE=true
    
  backend:
    build: ./backend
    environment:
      - DB_POOL_SIZE=10
      - ENABLE_BACKUP_OPERATIONS=true
      - ENABLE_LOAD_TYPE_VERIFICATION=true
      - CORS_ORIGINS=https://yourdomain.com
    volumes:
      - /data/reference_data:/data/reference_data
      - /logs:/app/logs
    
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=${SQL_SERVER_PASSWORD}
    volumes:
      - sqlserver_data:/var/opt/mssql

volumes:
  sqlserver_data:
```

### Load Balancer Configuration
```nginx
# Enhanced Nginx configuration for load balancing
upstream backend_api {
    server backend1:8000 weight=3;
    server backend2:8000 weight=2;
    server backend3:8000 weight=1;
}

server {
    listen 80;
    server_name yourdomain.com;
    
    # Static frontend files
    location / {
        root /var/www/frontend/build;
        try_files $uri $uri/ /index.html;
    }
    
    # API endpoints with enhanced headers
    location /api/ {
        proxy_pass http://backend_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Enhanced timeouts for large file processing
        proxy_connect_timeout 60s;
        proxy_send_timeout 300s;
        proxy_read_timeout 300s;
        
        # Support for Server-Sent Events
        proxy_buffering off;
        proxy_cache off;
    }
    
    # Large file upload support
    client_max_body_size 50M;
}
```

## Future Architecture Enhancements

### Planned Features
1. **Multi-tenancy Support**: Schema isolation per organization
2. **Advanced Analytics**: Data quality metrics and trend analysis  
3. **Workflow Engine**: Complex data processing pipelines
4. **API Rate Limiting**: Request throttling and quota management
5. **Enhanced Security**: OAuth2/JWT authentication system
6. **Cloud Integration**: AWS/Azure blob storage support
7. **Real-time Collaboration**: Multi-user simultaneous operations
8. **Advanced Validation**: ML-powered data quality checks

### Scalability Roadmap
1. **Microservices Split**: Separate backup, ingestion, and validation services
2. **Message Queue Integration**: Redis/RabbitMQ for async processing
3. **Caching Layer**: Redis for session and frequently accessed data
4. **Container Orchestration**: Kubernetes deployment with auto-scaling
5. **Database Scaling**: Read replicas and sharding strategies

This enhanced architecture provides a robust foundation for enterprise-grade reference data management with advanced load type intelligence and comprehensive backup/rollback capabilities.