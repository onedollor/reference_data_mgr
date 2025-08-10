# Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the Reference Data Auto Ingest System in various environments, from development to production. The system consists of a FastAPI backend, React frontend, and SQL Server database.

## System Requirements

### Minimum Requirements

#### Hardware
- **CPU**: 2 cores, 2.4 GHz
- **RAM**: 4 GB (8 GB recommended)
- **Storage**: 20 GB available space
- **Network**: 100 Mbps connection

#### Software
- **Operating System**: Windows 10/11, Windows Server 2019+, Linux (Ubuntu 20.04+)
- **Python**: 3.8+ (3.12+ recommended)
- **Node.js**: 16+ (18+ recommended)
- **Database**: SQL Server 2017+ (Express, Standard, or Enterprise)

### Production Requirements

#### Hardware
- **CPU**: 4+ cores, 3.0+ GHz
- **RAM**: 16 GB (32 GB recommended)
- **Storage**: 100 GB+ SSD storage
- **Network**: 1 Gbps connection

#### Software
- **Web Server**: Nginx or Apache (for production)
- **Process Manager**: systemd, PM2, or similar
- **Monitoring**: Optional but recommended (Prometheus, Grafana)

## Pre-Installation Setup

### 1. Database Preparation

#### SQL Server Installation
```sql
-- Create database
CREATE DATABASE reference_data_db;
GO

-- Create application user with required permissions
USE reference_data_db;
GO

CREATE LOGIN ref_data_user WITH PASSWORD = 'YourSecurePassword123!';
CREATE USER ref_data_user FOR LOGIN ref_data_user;

-- Grant required permissions
GRANT CREATE SCHEMA TO ref_data_user;
GRANT ALTER ON SCHEMA::dbo TO ref_data_user;

-- Create schemas
CREATE SCHEMA [ref];  -- Main data schema
CREATE SCHEMA [bkp];  -- Backup schema
GO

-- Grant schema permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::ref TO ref_data_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::bkp TO ref_data_user;
GRANT CREATE TABLE, ALTER TABLE ON SCHEMA::ref TO ref_data_user;
GRANT CREATE TABLE, ALTER TABLE ON SCHEMA::bkp TO ref_data_user;
GRANT EXECUTE ON SCHEMA::ref TO ref_data_user;
GO
```

#### Database Configuration Verification
```sql
-- Verify user permissions
SELECT 
    p.permission_name,
    p.state_desc,
    s.name as schema_name
FROM sys.database_permissions p
JOIN sys.schemas s ON p.major_id = s.schema_id
WHERE p.grantee_principal_id = USER_ID('ref_data_user');

-- Test connection from application
-- This should be run from your application environment
```

### 2. Directory Structure Setup

#### Windows
```powershell
# Create required directories
New-Item -ItemType Directory -Force -Path "C:\data\reference_data\temp"
New-Item -ItemType Directory -Force -Path "C:\data\reference_data\archive"
New-Item -ItemType Directory -Force -Path "C:\data\reference_data\format"

# Set appropriate permissions
icacls "C:\data\reference_data" /grant "IIS_IUSRS:F" /T
icacls "C:\data\reference_data" /grant "Users:F" /T
```

#### Linux
```bash
# Create required directories
sudo mkdir -p /data/reference_data/{temp,archive,format}

# Create application user
sudo useradd -r -s /bin/false ref_data_app

# Set ownership and permissions
sudo chown -R ref_data_app:ref_data_app /data/reference_data
sudo chmod 755 /data/reference_data
sudo chmod 750 /data/reference_data/{temp,archive,format}
```

## Installation Methods

### Method 1: Development Installation

#### 1. Clone Repository
```bash
git clone <repository-url>
cd reference_data_mgr
```

#### 2. Backend Setup
```bash
# Navigate to backend directory
cd backend

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create environment file
cp .env.example .env
# Edit .env with your configuration
```

#### 3. Frontend Setup
```bash
# Navigate to frontend directory
cd ../frontend

# Install dependencies
npm install

# Optional: Build for production
npm run build
```

#### 4. Configuration
Create or update `.env` file in the root directory:
```bash
# Database Configuration
db_host=localhost
db_name=reference_data_db
db_user=ref_data_user
db_password=YourSecurePassword123!

# Schema Configuration
data_schema=ref
backup_schema=bkp
validation_sp_schema=ref

# File System Configuration
temp_location=C:\data\reference_data\temp
archive_location=C:\data\reference_data\archive
format_location=C:\data\reference_data\format

# Application Configuration
max_upload_size=20971520

# Performance Configuration
DB_POOL_SIZE=5
INGEST_PROGRESS_INTERVAL=5
```

#### 5. Start Development Services
```bash
# Option 1: Use provided script
./start_dev.sh

# Option 2: Start manually
# Terminal 1 - Backend
cd backend && source venv/bin/activate && python start_backend.py

# Terminal 2 - Frontend  
cd frontend && npm start
```

### Method 2: Production Installation

#### 1. System Preparation
```bash
# Update system (Ubuntu/Debian)
sudo apt update && sudo apt upgrade -y

# Install required packages
sudo apt install -y python3 python3-pip python3-venv nodejs npm nginx

# Install SQL Server ODBC driver
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
sudo apt update
sudo apt install -y msodbcsql17 unixodbc-dev
```

#### 2. Application Deployment
```bash
# Create application directory
sudo mkdir -p /opt/reference_data_mgr
sudo chown $USER:$USER /opt/reference_data_mgr

# Deploy application files
cd /opt/reference_data_mgr
# Copy application files here

# Backend setup
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Frontend build
cd ../frontend
npm install
npm run build
```

#### 3. Nginx Configuration
```nginx
# /etc/nginx/sites-available/reference_data_mgr
server {
    listen 80;
    server_name your-domain.com;
    
    # Frontend static files
    location / {
        root /opt/reference_data_mgr/frontend/build;
        try_files $uri $uri/ /index.html;
        
        # Cache static assets
        location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg)$ {
            expires 1y;
            add_header Cache-Control "public, immutable";
        }
    }
    
    # Backend API
    location /api/ {
        proxy_pass http://127.0.0.1:8000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # Server-Sent Events support
        proxy_buffering off;
        proxy_cache off;
        
        # Timeouts for long-running operations
        proxy_read_timeout 300s;
        proxy_connect_timeout 75s;
    }
    
    # File upload size limit
    client_max_body_size 25M;
}

# Enable the site
sudo ln -s /etc/nginx/sites-available/reference_data_mgr /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

#### 4. Systemd Service Configuration
```ini
# /etc/systemd/system/reference-data-backend.service
[Unit]
Description=Reference Data Auto Ingest Backend
After=network.target

[Service]
Type=simple
User=ref_data_app
WorkingDirectory=/opt/reference_data_mgr/backend
Environment=PATH=/opt/reference_data_mgr/backend/venv/bin
ExecStart=/opt/reference_data_mgr/backend/venv/bin/python start_backend.py
Restart=always
RestartSec=5

# Environment variables
Environment=db_host=localhost
Environment=db_name=reference_data_db
Environment=db_user=ref_data_user
Environment=db_password=YourSecurePassword123!

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable reference-data-backend.service
sudo systemctl start reference-data-backend.service

# Check status
sudo systemctl status reference-data-backend.service
```

### Method 3: Docker Deployment

#### 1. Docker Compose Setup
```yaml
# docker-compose.yml
version: '3.8'

services:
  backend:
    build:
      context: ./backend
      dockerfile: Dockerfile
    ports:
      - "8000:8000"
    environment:
      - db_host=sqlserver
      - db_name=reference_data_db
      - db_user=sa
      - db_password=YourStrong@Passw0rd
      - temp_location=/data/temp
      - archive_location=/data/archive
      - format_location=/data/format
    volumes:
      - app_data:/data
    depends_on:
      - sqlserver
    restart: unless-stopped

  frontend:
    build:
      context: ./frontend
      dockerfile: Dockerfile
    ports:
      - "3000:80"
    restart: unless-stopped

  sqlserver:
    image: mcr.microsoft.com/mssql/server:2019-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=YourStrong@Passw0rd
      - MSSQL_PID=Express
    ports:
      - "1433:1433"
    volumes:
      - sqlserver_data:/var/opt/mssql
    restart: unless-stopped

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf
      - ./ssl:/etc/nginx/ssl
    depends_on:
      - frontend
      - backend
    restart: unless-stopped

volumes:
  sqlserver_data:
  app_data:
```

#### 2. Backend Dockerfile
```dockerfile
# backend/Dockerfile
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    gnupg \
    unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directories
RUN mkdir -p /data/{temp,archive,format}

# Expose port
EXPOSE 8000

# Start application
CMD ["python", "start_backend.py"]
```

#### 3. Frontend Dockerfile
```dockerfile
# frontend/Dockerfile
FROM node:18-alpine as build

WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production

COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html

# Copy custom nginx config
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
```

#### 4. Deploy with Docker
```bash
# Build and start services
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f backend

# Scale backend if needed
docker-compose up -d --scale backend=3
```

## Configuration Management

### Environment Variables

#### Core Configuration
```bash
# Database
db_host=localhost                    # Database server hostname
db_name=reference_data_db           # Database name
db_user=ref_data_user              # Database username
db_password=secure_password        # Database password
db_odbc_driver=ODBC Driver 17 for SQL Server  # ODBC driver

# Schemas
data_schema=ref                     # Main data schema
backup_schema=bkp                  # Backup schema
validation_sp_schema=ref           # Validation stored procedure schema

# File System
temp_location=/data/temp           # Temporary file storage
archive_location=/data/archive     # Archive storage
format_location=/data/format       # Format file storage
max_upload_size=20971520          # Max file size (20MB)

# Performance
DB_POOL_SIZE=5                     # Database connection pool size
INGEST_PROGRESS_INTERVAL=5         # Progress update interval (seconds)
DB_MAX_RETRIES=3                   # Database connection retries
DB_RETRY_BACKOFF=0.5              # Retry backoff time

# Features
INGEST_TYPE_INFERENCE=1            # Enable data type inference
INGEST_DATE_THRESHOLD=0.8          # Date detection threshold
INGEST_BATCH_SIZE=990             # Insert batch size
```

#### Advanced Configuration
```bash
# Logging
LOG_LEVEL=INFO                     # Logging level (DEBUG, INFO, WARNING, ERROR)
LOG_FILE_MAX_SIZE=10485760        # Max log file size (10MB)
LOG_FILE_BACKUP_COUNT=5           # Number of log files to keep

# Security
ALLOWED_ORIGINS=http://localhost:3000  # CORS allowed origins
SESSION_TIMEOUT=3600              # Session timeout (seconds)
ENABLE_HTTPS=true                 # Force HTTPS

# Monitoring
ENABLE_METRICS=true               # Enable metrics collection
METRICS_PORT=9090                # Metrics server port
HEALTH_CHECK_INTERVAL=30         # Health check interval
```

### Configuration Validation

#### Startup Configuration Check
```python
# scripts/validate_config.py
import os
import sys
import pyodbc
from pathlib import Path

def validate_configuration():
    """Validate system configuration before startup"""
    errors = []
    
    # Database configuration
    required_db_vars = ['db_host', 'db_name', 'db_user', 'db_password']
    for var in required_db_vars:
        if not os.getenv(var):
            errors.append(f"Missing required environment variable: {var}")
    
    # Test database connection
    try:
        conn_str = build_connection_string()
        conn = pyodbc.connect(conn_str)
        conn.close()
        print("✓ Database connection successful")
    except Exception as e:
        errors.append(f"Database connection failed: {e}")
    
    # File system check
    dirs = ['temp_location', 'archive_location', 'format_location']
    for dir_var in dirs:
        dir_path = os.getenv(dir_var)
        if not dir_path:
            errors.append(f"Missing directory configuration: {dir_var}")
            continue
        
        path = Path(dir_path)
        if not path.exists():
            errors.append(f"Directory does not exist: {dir_path}")
        elif not path.is_dir():
            errors.append(f"Path is not a directory: {dir_path}")
        elif not os.access(dir_path, os.W_OK):
            errors.append(f"Directory is not writable: {dir_path}")
        else:
            print(f"✓ Directory accessible: {dir_path}")
    
    if errors:
        print("Configuration validation failed:")
        for error in errors:
            print(f"  ✗ {error}")
        sys.exit(1)
    else:
        print("✓ All configuration checks passed")

if __name__ == "__main__":
    validate_configuration()
```

## Database Migration & Setup

### Initial Database Setup Script
```sql
-- scripts/setup_database.sql
USE [reference_data_db];
GO

-- Create schemas if they don't exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ref')
    EXEC('CREATE SCHEMA [ref]');
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bkp')  
    EXEC('CREATE SCHEMA [bkp]');
GO

-- Create system log table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'system_log' AND schema_id = SCHEMA_ID('ref'))
BEGIN
    CREATE TABLE [ref].[system_log] (
        [id] bigint IDENTITY(1,1) PRIMARY KEY,
        [timestamp] datetime2 DEFAULT GETDATE(),
        [level] varchar(20) NOT NULL,
        [module] varchar(100) NOT NULL,
        [message] nvarchar(max) NOT NULL,
        [details] nvarchar(max) NULL,
        [correlation_id] varchar(50) NULL
    );
    
    CREATE INDEX IX_system_log_timestamp ON [ref].[system_log] ([timestamp]);
    CREATE INDEX IX_system_log_level ON [ref].[system_log] ([level]);
END
GO

-- Create application metadata table
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'app_metadata' AND schema_id = SCHEMA_ID('ref'))
BEGIN
    CREATE TABLE [ref].[app_metadata] (
        [key] varchar(100) PRIMARY KEY,
        [value] nvarchar(max) NOT NULL,
        [updated_at] datetime2 DEFAULT GETDATE()
    );
    
    -- Insert version information
    INSERT INTO [ref].[app_metadata] ([key], [value]) VALUES ('version', '1.0.0');
    INSERT INTO [ref].[app_metadata] ([key], [value]) VALUES ('last_migration', GETDATE());
END
GO

PRINT 'Database setup completed successfully';
```

### Run Database Setup
```bash
# Using sqlcmd (Windows/Linux)
sqlcmd -S localhost -d reference_data_db -U ref_data_user -P YourSecurePassword123! -i scripts/setup_database.sql

# Using PowerShell (Windows)
Invoke-Sqlcmd -ServerInstance localhost -Database reference_data_db -Username ref_data_user -Password YourSecurePassword123! -InputFile scripts/setup_database.sql

# Verify setup
sqlcmd -S localhost -d reference_data_db -U ref_data_user -P YourSecurePassword123! -Q "SELECT * FROM ref.app_metadata"
```

## SSL/TLS Configuration

### Self-Signed Certificates (Development)
```bash
# Generate self-signed certificate
openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365 \
  -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost"

# For nginx
sudo mkdir -p /etc/nginx/ssl
sudo cp cert.pem /etc/nginx/ssl/
sudo cp key.pem /etc/nginx/ssl/
```

### Production SSL (Let's Encrypt)
```bash
# Install certbot
sudo apt install certbot python3-certbot-nginx

# Obtain certificate
sudo certbot --nginx -d your-domain.com

# Auto-renewal
sudo crontab -e
# Add: 0 12 * * * /usr/bin/certbot renew --quiet
```

### Nginx SSL Configuration
```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;
    
    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    
    # Modern SSL configuration
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512:ECDHE-RSA-AES256-GCM-SHA384;
    ssl_prefer_server_ciphers off;
    
    # HSTS
    add_header Strict-Transport-Security "max-age=63072000" always;
    
    # Rest of configuration...
}

# Redirect HTTP to HTTPS
server {
    listen 80;
    server_name your-domain.com;
    return 301 https://$server_name$request_uri;
}
```

## Monitoring Setup

### Application Health Checks
```python
# backend/health_check.py
import requests
import sys
import time

def check_backend_health():
    """Check backend API health"""
    try:
        response = requests.get('http://localhost:8000/', timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'healthy':
                print("✓ Backend is healthy")
                return True
    except Exception as e:
        print(f"✗ Backend health check failed: {e}")
    return False

def check_database_health():
    """Check database connectivity"""
    try:
        from utils.database import DatabaseManager
        db = DatabaseManager()
        conn = db.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        conn.close()
        if result and result[0] == 1:
            print("✓ Database is accessible")
            return True
    except Exception as e:
        print(f"✗ Database health check failed: {e}")
    return False

def check_filesystem_health():
    """Check file system access"""
    import os
    dirs = ['temp_location', 'archive_location', 'format_location']
    all_healthy = True
    
    for dir_var in dirs:
        dir_path = os.getenv(dir_var)
        if not dir_path or not os.path.exists(dir_path) or not os.access(dir_path, os.W_OK):
            print(f"✗ Directory check failed: {dir_var}")
            all_healthy = False
        else:
            print(f"✓ Directory accessible: {dir_var}")
    
    return all_healthy

if __name__ == "__main__":
    checks = [
        check_backend_health,
        check_database_health, 
        check_filesystem_health
    ]
    
    all_passed = True
    for check in checks:
        if not check():
            all_passed = False
    
    if all_passed:
        print("All health checks passed")
        sys.exit(0)
    else:
        print("One or more health checks failed")
        sys.exit(1)
```

### System Monitoring Script
```bash
#!/bin/bash
# scripts/monitor.sh

# Check service status
check_service_status() {
    echo "=== Service Status ==="
    systemctl status reference-data-backend.service | head -10
    systemctl status nginx | head -10
}

# Check disk usage
check_disk_usage() {
    echo "=== Disk Usage ==="
    df -h /data/reference_data
    echo "Temp files: $(find /data/reference_data/temp -type f | wc -l)"
    echo "Archive files: $(find /data/reference_data/archive -type f | wc -l)"
}

# Check database connectivity
check_database() {
    echo "=== Database Status ==="
    python3 /opt/reference_data_mgr/backend/health_check.py
}

# Check application logs for errors
check_logs() {
    echo "=== Recent Errors ==="
    journalctl -u reference-data-backend.service --since "1 hour ago" | grep -i error | tail -5
}

# Main monitoring function
main() {
    echo "=== System Monitor - $(date) ==="
    check_service_status
    check_disk_usage
    check_database
    check_logs
    echo "=== Monitor Complete ==="
}

main
```

## Performance Optimization

### Database Performance
```sql
-- Create useful indexes
CREATE INDEX IX_system_log_timestamp ON [ref].[system_log] ([timestamp] DESC);
CREATE INDEX IX_system_log_level_timestamp ON [ref].[system_log] ([level], [timestamp] DESC);

-- Configure database for performance
ALTER DATABASE reference_data_db SET RECOVERY SIMPLE;  -- For non-critical data
ALTER DATABASE reference_data_db SET AUTO_UPDATE_STATISTICS_ASYNC ON;
```

### Application Performance
```python
# backend/performance_config.py
import os

class PerformanceConfig:
    # Connection pooling
    DB_POOL_SIZE = int(os.getenv('DB_POOL_SIZE', '10'))
    DB_POOL_MAX_OVERFLOW = int(os.getenv('DB_POOL_MAX_OVERFLOW', '20'))
    
    # File processing
    CSV_CHUNK_SIZE = int(os.getenv('CSV_CHUNK_SIZE', '10000'))
    BATCH_INSERT_SIZE = int(os.getenv('BATCH_INSERT_SIZE', '990'))
    
    # Caching
    ENABLE_RESPONSE_CACHING = os.getenv('ENABLE_RESPONSE_CACHING', 'true').lower() == 'true'
    CACHE_TTL = int(os.getenv('CACHE_TTL', '300'))  # 5 minutes
```

### Nginx Performance Optimization
```nginx
# Add to nginx configuration
worker_processes auto;
worker_connections 1024;

# Gzip compression
gzip on;
gzip_vary on;
gzip_min_length 1024;
gzip_types
    text/plain
    text/css
    application/json
    application/javascript
    text/xml
    application/xml;

# File caching
location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2)$ {
    expires 1y;
    add_header Cache-Control "public, immutable";
    add_header Vary Accept-Encoding;
}

# Connection keep-alive
keepalive_timeout 65;
keepalive_requests 100;
```

## Troubleshooting

### Common Issues

#### 1. Database Connection Issues
```bash
# Check SQL Server status
sudo systemctl status mssql-server

# Test connection
sqlcmd -S localhost -U ref_data_user -P password

# Check firewall
sudo ufw status
sudo ufw allow 1433/tcp  # If needed
```

#### 2. File Permission Issues
```bash
# Check directory permissions
ls -la /data/reference_data/

# Fix permissions
sudo chown -R ref_data_app:ref_data_app /data/reference_data
sudo chmod 755 /data/reference_data
sudo chmod 750 /data/reference_data/{temp,archive,format}
```

#### 3. Service Startup Issues
```bash
# Check service logs
sudo journalctl -u reference-data-backend.service -f

# Check configuration
python3 /opt/reference_data_mgr/backend/scripts/validate_config.py

# Restart service
sudo systemctl restart reference-data-backend.service
```

#### 4. Frontend Issues
```bash
# Check if backend is running
curl http://localhost:8000/

# Check nginx configuration
sudo nginx -t
sudo systemctl reload nginx

# Check frontend build
cd /opt/reference_data_mgr/frontend && npm run build
```

### Log Analysis
```bash
# Backend application logs
tail -f /opt/reference_data_mgr/backend/logs/system.log

# System service logs  
journalctl -u reference-data-backend.service -f

# Nginx logs
tail -f /var/log/nginx/access.log
tail -f /var/log/nginx/error.log

# Database logs (Windows)
# Check SQL Server error log via SSMS or:
# SELECT * FROM sys.dm_exec_requests WHERE status = 'running'
```

## Backup & Recovery

### Database Backup
```sql
-- Create backup job
BACKUP DATABASE reference_data_db 
TO DISK = 'C:\Backups\reference_data_db.bak'
WITH FORMAT, INIT;

-- Automated backup script
CREATE PROCEDURE sp_backup_reference_data
AS
BEGIN
    DECLARE @filename VARCHAR(255);
    SET @filename = 'C:\Backups\reference_data_db_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss') + '.bak';
    
    BACKUP DATABASE reference_data_db 
    TO DISK = @filename
    WITH FORMAT, INIT, COMPRESSION;
END
```

### Application Data Backup
```bash
#!/bin/bash
# scripts/backup.sh

BACKUP_DIR="/backups/reference_data_mgr"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR/$TIMESTAMP"

# Backup application files
tar -czf "$BACKUP_DIR/$TIMESTAMP/application.tar.gz" /opt/reference_data_mgr

# Backup data directories
tar -czf "$BACKUP_DIR/$TIMESTAMP/data_files.tar.gz" /data/reference_data

# Backup configuration
cp /opt/reference_data_mgr/.env "$BACKUP_DIR/$TIMESTAMP/"

# Database backup
sqlcmd -S localhost -U sa -P password -Q "BACKUP DATABASE reference_data_db TO DISK = '$BACKUP_DIR/$TIMESTAMP/database.bak'"

echo "Backup completed: $BACKUP_DIR/$TIMESTAMP"
```

## Security Hardening

### System Security
```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Configure firewall
sudo ufw enable
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow from localhost to any port 8000
sudo ufw deny 8000/tcp  # Block direct access to backend

# Disable unnecessary services
sudo systemctl disable --now bluetooth
sudo systemctl disable --now cups
```

### Application Security
```bash
# Set secure file permissions
sudo chmod 600 /opt/reference_data_mgr/.env
sudo chmod 700 /opt/reference_data_mgr/scripts/

# Create dedicated user
sudo useradd -r -s /bin/false -d /opt/reference_data_mgr ref_data_app
sudo chown -R ref_data_app:ref_data_app /opt/reference_data_mgr
```

## Scaling Considerations

### Load Balancing Setup
```nginx
# nginx load balancer configuration
upstream backend {
    server 127.0.0.1:8000;
    server 127.0.0.1:8001;
    server 127.0.0.1:8002;
}

server {
    listen 80;
    
    location /api/ {
        proxy_pass http://backend;
        # ... other proxy settings
    }
}
```

### Multiple Backend Instances
```bash
# Create multiple service files
sudo cp /etc/systemd/system/reference-data-backend.service /etc/systemd/system/reference-data-backend-1.service
sudo cp /etc/systemd/system/reference-data-backend.service /etc/systemd/system/reference-data-backend-2.service

# Edit each service file to use different ports
# Service 1: Port 8001
# Service 2: Port 8002

# Start all services
sudo systemctl enable --now reference-data-backend.service
sudo systemctl enable --now reference-data-backend-1.service  
sudo systemctl enable --now reference-data-backend-2.service
```

## Maintenance Procedures

### Regular Maintenance Tasks
```bash
#!/bin/bash
# scripts/maintenance.sh

# Clean temporary files older than 7 days
find /data/reference_data/temp -type f -mtime +7 -delete

# Clean old log files
find /opt/reference_data_mgr/backend/logs -name "*.log.*" -mtime +30 -delete

# Update system packages
sudo apt update && sudo apt list --upgradable

# Check disk space
df -h /data/reference_data | awk 'NR==2{printf "Disk usage: %s\n", $5}'

# Database maintenance
sqlcmd -S localhost -U ref_data_user -P password -Q "DELETE FROM ref.system_log WHERE timestamp < DATEADD(day, -90, GETDATE())"
```

### Health Check Automation
```bash
#!/bin/bash
# Add to crontab: */5 * * * * /opt/scripts/health_monitor.sh

HEALTH_CHECK="/opt/reference_data_mgr/backend/health_check.py"
LOG_FILE="/var/log/reference_data_health.log"

if ! python3 "$HEALTH_CHECK" > /dev/null 2>&1; then
    echo "$(date): Health check failed" >> "$LOG_FILE"
    # Send alert (email, Slack, etc.)
    # systemctl restart reference-data-backend.service
fi
```

This comprehensive deployment guide covers all aspects of installing, configuring, and maintaining the Reference Data Auto Ingest System in various environments. Follow the appropriate sections based on your deployment needs and environment requirements.