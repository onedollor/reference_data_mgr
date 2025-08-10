# Troubleshooting Guide

## Overview

This guide provides comprehensive solutions for common issues encountered while using the Reference Data Auto Ingest System. Issues are organized by category with detailed symptoms, root causes, and step-by-step solutions.

## Quick Diagnostics

### System Health Check

Before troubleshooting specific issues, perform these basic health checks:

#### 1. Service Status Check
```bash
# Check if backend service is running
curl -s http://localhost:8000/ | grep "healthy" || echo "Backend not responding"

# Check if frontend is accessible
curl -s http://localhost:3000/ | grep -q "html" && echo "Frontend accessible" || echo "Frontend not accessible"

# Check database connectivity
curl -s http://localhost:8000/db/pool-stats | jq '.active_connections' || echo "Database connection issues"
```

#### 2. Log Review
```bash
# Check recent system logs
curl -s http://localhost:8000/logs?limit=10 | jq '.logs[] | select(.level=="ERROR")'

# Check system service logs (if using systemd)
sudo journalctl -u reference-data-backend.service --since "1 hour ago" --no-pager | tail -20
```

#### 3. Resource Check
```bash
# Check disk space
df -h /data/reference_data/

# Check memory usage
free -h

# Check process status
ps aux | grep -E "(python|node|nginx)" | grep -v grep
```

## File Upload Issues

### Issue: File Upload Fails

#### Symptoms
- Upload button doesn't respond
- Error message: "Upload failed"
- File doesn't appear in upload area
- Browser shows network errors

#### Root Causes & Solutions

##### 1. File Size Too Large
**Symptoms**: Error message mentioning file size limit
```
Solution:
1. Check file size: ls -lh your_file.csv
2. If over 20MB, split file or compress data
3. Contact administrator to increase limit if needed
4. Alternative: Process file in smaller chunks
```

##### 2. Invalid File Type
**Symptoms**: "Only CSV files are supported" error
```
Solution:
1. Verify file extension is .csv (case-insensitive)
2. Check file contents - should be plain text CSV
3. Convert from Excel: Save As > CSV (UTF-8)
4. Ensure file isn't corrupted: head -5 your_file.csv
```

##### 3. Network Connection Issues
**Symptoms**: Timeout errors, incomplete uploads
```
Solution:
1. Check internet connection: ping your-server-address
2. Try upload from different network
3. Disable VPN if applicable
4. Clear browser cache and cookies
5. Try different browser
```

##### 4. Browser Issues
**Symptoms**: Interface not responding, JavaScript errors
```
Solution:
1. Hard refresh: Ctrl+F5 (Windows) or Cmd+Shift+R (Mac)
2. Clear browser cache completely
3. Disable browser extensions temporarily
4. Try private/incognito mode
5. Update browser to latest version
```

##### 5. Backend Service Down
**Symptoms**: Cannot reach upload endpoint
```
Solution:
1. Check backend status: curl http://localhost:8000/
2. Restart backend service:
   sudo systemctl restart reference-data-backend.service
3. Check backend logs for errors
4. Verify environment configuration
```

### Issue: File Upload Hangs

#### Symptoms
- Progress bar stuck at 0% or specific percentage
- No error messages displayed
- Browser tab becomes unresponsive

#### Troubleshooting Steps

1. **Check Network Stability**
```bash
# Monitor network connectivity
ping -c 10 your-server-address

# Check for packet loss
traceroute your-server-address
```

2. **Verify Backend Process**
```bash
# Check if backend process is running
ps aux | grep uvicorn

# Check system resources
top | grep python

# Check backend logs for blocking operations
tail -f /opt/reference_data_mgr/backend/logs/system.log
```

3. **Browser Troubleshooting**
```javascript
// Check browser console for errors (F12 Developer Tools)
// Look for:
// - Network timeout errors
// - JavaScript exceptions
// - CORS errors
```

4. **File-Specific Issues**
```bash
# Check file encoding
file -i your_file.csv

# Check for binary characters
hexdump -C your_file.csv | head -5

# Verify CSV structure
head -10 your_file.csv
```

## Processing and Ingestion Issues

### Issue: Processing Fails During Ingestion

#### Symptoms
- Processing starts but fails partway through
- Error messages in progress stream
- "ERROR!" messages in logs
- Database connection errors

#### Common Causes & Solutions

##### 1. Database Connection Loss
**Symptoms**: "Database connection failed" messages
```
Immediate Solution:
1. Check database service status
2. Verify connection string in .env file
3. Test manual connection:
   sqlcmd -S server -U user -P password -Q "SELECT 1"
4. Restart database service if needed

Long-term Solution:
1. Increase connection timeout settings
2. Implement connection retry logic
3. Monitor database performance
4. Consider connection pooling optimization
```

##### 2. Invalid Data Format
**Symptoms**: Type conversion errors, data validation failures
```
Solution:
1. Review error details in system logs
2. Check problematic rows mentioned in errors
3. Common fixes:
   - Remove non-printable characters: sed 's/[^[:print:]]//g' file.csv
   - Fix encoding: iconv -f ISO-8859-1 -t UTF-8 file.csv
   - Standardize line endings: dos2unix file.csv
4. Re-upload corrected file
```

##### 3. Memory Exhaustion
**Symptoms**: Out of memory errors, process killed
```
Solution:
1. Check available memory: free -h
2. Monitor during processing: watch -n 1 free -h
3. Reduce file size or split into smaller files
4. Restart backend service to clear memory
5. Consider increasing system memory
```

##### 4. Disk Space Issues
**Symptoms**: "No space left on device" errors
```
Solution:
1. Check disk space: df -h
2. Clean temporary files:
   find /data/reference_data/temp -type f -mtime +1 -delete
3. Archive or remove old files
4. Increase available disk space
```

##### 5. SQL Syntax Errors
**Symptoms**: SQL execution errors in logs
```
Solution:
1. Check column names for invalid characters
2. Verify table name doesn't conflict with reserved words
3. Review generated SQL in logs for issues
4. Ensure database user has required permissions
```

### Issue: Processing Hangs or Stalls

#### Symptoms
- Progress stops at specific percentage
- No new log entries
- Server becomes unresponsive

#### Troubleshooting Steps

1. **Identify Hang Location**
```bash
# Check current backend processes
ps aux | grep -E "(python|uvicorn)"

# Check database connections
# SQL Server:
SELECT * FROM sys.dm_exec_requests WHERE status = 'running'
```

2. **Resource Analysis**
```bash
# Check CPU usage
top | head -20

# Check I/O wait
iostat -x 1 5

# Check database locks (SQL Server)
SELECT * FROM sys.dm_tran_locks WHERE resource_type != 'DATABASE'
```

3. **Recovery Actions**
```bash
# Cancel the operation
curl -X POST http://localhost:8000/progress/your_progress_key/cancel

# If unresponsive, restart services
sudo systemctl restart reference-data-backend.service

# Clean up orphaned processes
pkill -f "python.*main.py"
```

## Database Issues

### Issue: Database Connection Failures

#### Symptoms
- "Database connection failed" errors
- Timeout errors during connection attempts
- Authentication failures

#### Troubleshooting Steps

##### 1. Connection String Issues
```bash
# Test environment variables
echo $db_host
echo $db_name  
echo $db_user

# Test basic connectivity
telnet $db_host 1433

# Test SQL Server connection
sqlcmd -S $db_host -U $db_user -P $db_password -Q "SELECT 1"
```

##### 2. Authentication Problems
```sql
-- Check user permissions
USE your_database;
SELECT 
    dp.name AS principal_name,
    dp.type_desc AS principal_type,
    p.permission_name,
    p.state_desc AS permission_state,
    s.name AS schema_name
FROM sys.database_permissions p
JOIN sys.objects o ON p.major_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
WHERE dp.name = 'your_username';
```

##### 3. Network Issues
```bash
# Check port accessibility
nmap -p 1433 your_db_server

# Check firewall rules
sudo ufw status | grep 1433

# Test from application server
nc -zv your_db_server 1433
```

##### 4. Database Server Issues
```sql
-- Check database status
SELECT name, state_desc FROM sys.databases WHERE name = 'your_database';

-- Check server health
SELECT @@VERSION;
SELECT GETDATE();

-- Check active connections
SELECT 
    db_name(dbid) as database_name,
    count(dbid) as connection_count
FROM sys.sysprocesses 
WHERE dbid > 0
GROUP BY dbid, db_name(dbid);
```

### Issue: Database Permission Errors

#### Symptoms
- "Access denied" errors
- "Cannot create table" errors
- "Insufficient permissions" messages

#### Solutions

##### 1. Verify Required Permissions
```sql
-- Grant minimum required permissions
USE your_database;
GRANT CREATE TABLE TO your_app_user;
GRANT ALTER ON SCHEMA::ref TO your_app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::ref TO your_app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::bkp TO your_app_user;
GRANT EXECUTE ON SCHEMA::ref TO your_app_user;
```

##### 2. Check Schema Permissions
```sql
-- Verify schema access
SELECT 
    s.name AS schema_name,
    dp.name AS principal_name,
    p.permission_name,
    p.state_desc
FROM sys.schemas s
JOIN sys.database_permissions p ON s.schema_id = p.major_id
JOIN sys.database_principals dp ON p.grantee_principal_id = dp.principal_id
WHERE dp.name = 'your_app_user';
```

##### 3. Database Role Assignment
```sql
-- Add user to appropriate roles
USE your_database;
ALTER ROLE db_datareader ADD MEMBER your_app_user;
ALTER ROLE db_datawriter ADD MEMBER your_app_user;
ALTER ROLE db_ddladmin ADD MEMBER your_app_user;
```

## Format Detection Issues

### Issue: Auto-Detection Fails or Returns Low Confidence

#### Symptoms
- Confidence score below 50%
- Wrong delimiter detected
- Headers not properly identified
- Sample data looks incorrect

#### Troubleshooting Steps

1. **File Analysis**
```bash
# Check first few lines
head -5 your_file.csv

# Check for consistent delimiters
cut -d',' -f1 your_file.csv | head -10  # For comma-separated
cut -d';' -f1 your_file.csv | head -10  # For semicolon-separated

# Check line endings
file your_file.csv
hexdump -C your_file.csv | head -3
```

2. **Common Format Issues**
```bash
# Mixed delimiters (fix by standardizing)
sed 's/;/,/g' your_file.csv > fixed_file.csv

# Inconsistent quoting (fix with proper quoting)
awk -F, '{for(i=1;i<=NF;i++){if($i~/[,\n]/){gsub(/"/,"\"\"",$i); $i="\""$i"\""}}1}' OFS=, your_file.csv

# Remove byte order mark (BOM)
sed -i '1s/^\xEF\xBB\xBF//' your_file.csv
```

3. **Manual Configuration**
```
If auto-detection fails:
1. Override detected settings manually
2. Use these common combinations:
   - Standard CSV: delimiter=',', qualifier='"'
   - European CSV: delimiter=';', qualifier='"'
   - Pipe-delimited: delimiter='|', qualifier='"'
   - Tab-delimited: delimiter='\t', qualifier='"'
```

### Issue: Trailer Detection Problems

#### Symptoms
- Trailer lines included in data
- Data rows identified as trailers
- Processing fails on trailer validation

#### Solutions

1. **Identify Trailer Pattern**
```bash
# Check last few lines of file
tail -5 your_file.csv

# Look for common trailer patterns
grep -E "(TOTAL|COUNT|END|EOF|SUMMARY)" your_file.csv
```

2. **Configure Trailer Handling**
```
Common trailer patterns:
- "TOTAL" - Lines starting with TOTAL
- "EOF" - End-of-file marker
- "^[0-9]+$" - Lines containing only numbers (record count)
- "SUMMARY.*" - Lines starting with SUMMARY
```

3. **Manual Trailer Removal**
```bash
# Remove last N lines
head -n -1 your_file.csv > file_without_trailer.csv

# Remove lines matching pattern
grep -v "^TOTAL" your_file.csv > cleaned_file.csv

# Remove blank lines at end
sed '/^[[:space:]]*$/d' your_file.csv
```

## Performance Issues

### Issue: Slow Processing Speed

#### Symptoms
- Processing takes much longer than expected
- Progress updates very slowly
- High CPU or memory usage
- Database performance degradation

#### Analysis & Solutions

##### 1. File Size and Complexity
```bash
# Check file characteristics
wc -l your_file.csv          # Count lines
wc -c your_file.csv          # File size in bytes
awk -F',' '{print NF}' your_file.csv | sort -nu | tail -1  # Max columns
```

**Optimization strategies:**
- Split large files into smaller chunks
- Remove unnecessary columns before upload
- Clean data to reduce processing complexity

##### 2. Database Performance
```sql
-- Check database performance
SELECT 
    wait_type,
    wait_time_ms,
    percentage
FROM sys.dm_os_wait_stats
WHERE percentage > 1
ORDER BY percentage DESC;

-- Check index usage
SELECT 
    object_name(i.object_id) AS table_name,
    i.name AS index_name,
    dm_ius.user_seeks,
    dm_ius.user_scans,
    dm_ius.user_lookups
FROM sys.indexes i
LEFT JOIN sys.dm_db_index_usage_stats dm_ius ON i.object_id = dm_ius.object_id 
    AND i.index_id = dm_ius.index_id
WHERE objectproperty(i.object_id, 'IsUserTable') = 1;
```

**Database optimizations:**
- Add indexes to frequently queried columns
- Update database statistics
- Consider increasing database memory allocation

##### 3. System Resource Constraints
```bash
# Monitor system resources during processing
# CPU usage
top -b -n1 | grep "Cpu(s)"

# Memory usage  
free -h

# Disk I/O
iostat -x 1 3

# Network usage
netstat -i
```

**Resource optimizations:**
- Increase available memory
- Use faster storage (SSD)
- Optimize network connectivity
- Consider scaling hardware resources

### Issue: Memory Usage Issues

#### Symptoms
- Out of memory errors
- System becomes unresponsive
- Browser tabs crash
- Backend process killed

#### Solutions

##### 1. Backend Memory Management
```python
# Check memory usage in logs
grep -i "memory\|ram\|oom" /var/log/syslog

# Monitor Python process memory
ps aux | grep python | awk '{print $6}' | head -1  # Memory in KB
```

**Memory optimization:**
- Restart backend service regularly
- Process smaller file chunks
- Implement garbage collection
- Increase system memory

##### 2. Browser Memory Issues
```javascript
// Check browser memory usage (F12 Developer Tools > Memory tab)
// Clear browser cache and restart
// Close unnecessary tabs
// Use a different browser
```

##### 3. Database Memory
```sql
-- Check SQL Server memory usage
SELECT 
    (total_physical_memory_kb/1024) AS Total_OS_Memory_MB,
    (available_physical_memory_kb/1024) AS Available_OS_Memory_MB,
    (total_page_file_kb/1024) AS Total_PageFile_MB,
    (available_page_file_kb/1024) AS Available_PageFile_MB,
    (kernel_paged_pool_kb/1024) AS Kernel_Paged_Pool_MB,
    (kernel_nonpaged_pool_kb/1024) AS Kernel_Nonpaged_Pool_MB,
    (system_cache_kb/1024) AS System_Cache_MB,
    (system_high_memory_signal_state) AS System_High_Memory_Signal_State,
    (system_low_memory_signal_state) AS System_Low_Memory_Signal_State
FROM sys.dm_os_sys_memory;
```

## Frontend Issues

### Issue: User Interface Not Loading

#### Symptoms
- Blank white screen
- "Cannot reach server" messages
- JavaScript errors in browser console
- Styling/layout issues

#### Troubleshooting Steps

##### 1. Browser Console Errors
```javascript
// Open browser developer tools (F12)
// Check Console tab for errors
// Common issues:
// - Failed to load resource
// - CORS errors
// - JavaScript runtime errors
```

##### 2. Network Connectivity
```bash
# Test frontend accessibility
curl -s http://localhost:3000/ | grep -o "<title>.*</title>"

# Check for CORS issues
curl -H "Origin: http://localhost:3000" \
     -H "Access-Control-Request-Method: GET" \
     -H "Access-Control-Request-Headers: X-Requested-With" \
     -X OPTIONS \
     http://localhost:8000/config
```

##### 3. Service Status
```bash
# Check if React dev server is running
ps aux | grep "react-scripts"

# Check frontend build
ls -la frontend/build/

# Check nginx configuration (production)
nginx -t
systemctl status nginx
```

##### 4. Clear Browser State
```
1. Hard refresh: Ctrl+F5 or Cmd+Shift+R
2. Clear browser cache completely
3. Clear cookies for the domain
4. Try private/incognito mode
5. Try different browser entirely
```

### Issue: Real-time Updates Not Working

#### Symptoms
- Progress bar doesn't update
- Logs don't refresh automatically
- Status messages appear delayed
- Manual refresh required to see changes

#### Solutions

##### 1. Server-Sent Events Issues
```javascript
// Check SSE connection in browser console
// Look for EventSource errors
// Verify connection to streaming endpoints
```

##### 2. Network Configuration
```bash
# Check if proxy/firewall blocks SSE
curl -N http://localhost:8000/ingest/test.csv

# Test with different network
# Disable VPN if applicable
```

##### 3. Browser Compatibility
```javascript
// Check EventSource support
if (typeof(EventSource) !== "undefined") {
    console.log("SSE supported");
} else {
    console.log("SSE not supported");
}

// Alternative: Use polling instead of SSE
setInterval(() => {
    fetch('/progress/key').then(response => {
        // Handle response
    });
}, 2000);
```

## System Administration Issues

### Issue: Service Startup Failures

#### Symptoms
- Backend service won't start
- Frontend build fails
- Database connectivity issues on startup
- Environment configuration errors

#### Troubleshooting Steps

##### 1. Check Service Logs
```bash
# System service logs
sudo journalctl -u reference-data-backend.service -f

# Application logs
tail -f /opt/reference_data_mgr/backend/logs/system.log

# Check for startup errors
grep -i error /opt/reference_data_mgr/backend/logs/system.log
```

##### 2. Environment Configuration
```bash
# Verify .env file exists and is readable
ls -la /opt/reference_data_mgr/.env
cat /opt/reference_data_mgr/.env

# Check environment variables
sudo -u ref_data_app env | grep -E "(db_|temp_|archive_)"

# Validate configuration
cd /opt/reference_data_mgr/backend
python -c "from utils.database import DatabaseManager; db = DatabaseManager(); print('Config valid')"
```

##### 3. Dependency Issues
```bash
# Check Python dependencies
cd /opt/reference_data_mgr/backend
source venv/bin/activate
pip check

# Check Node.js dependencies (if applicable)
cd /opt/reference_data_mgr/frontend
npm audit

# Install missing dependencies
pip install -r requirements.txt
npm install
```

##### 4. Permission Issues
```bash
# Check file permissions
ls -la /opt/reference_data_mgr/
ls -la /data/reference_data/

# Fix ownership if needed
sudo chown -R ref_data_app:ref_data_app /opt/reference_data_mgr/
sudo chown -R ref_data_app:ref_data_app /data/reference_data/

# Fix directory permissions
sudo chmod 755 /opt/reference_data_mgr/
sudo chmod 750 /data/reference_data/
```

## Emergency Procedures

### System Recovery

#### Complete System Restart
```bash
# Stop all services
sudo systemctl stop reference-data-backend.service
sudo systemctl stop nginx

# Clear temporary files
sudo rm -rf /data/reference_data/temp/*

# Restart database (if applicable)
sudo systemctl restart mssql-server

# Start services
sudo systemctl start reference-data-backend.service  
sudo systemctl start nginx

# Verify system health
curl http://localhost:8000/
curl http://localhost:3000/
```

#### Data Recovery
```bash
# Restore from backup (if available)
# Check backup directory
ls -la /backups/reference_data_mgr/

# Restore database from backup
sqlcmd -S server -U sa -P password -Q "RESTORE DATABASE reference_data_db FROM DISK = '/backups/database.bak'"

# Restore application files
sudo tar -xzf /backups/application_backup.tar.gz -C /opt/reference_data_mgr/
```

### Escalation Procedures

#### When to Escalate
- Database corruption detected
- Multiple system components failing
- Security breaches suspected
- Data integrity compromised
- System completely unresponsive

#### Information to Gather
```bash
# System information
uname -a
df -h
free -h
ps aux | grep -E "(python|nginx|sql)"

# Recent logs
sudo journalctl --since "1 hour ago" > system_logs.txt
tail -100 /opt/reference_data_mgr/backend/logs/system.log > app_logs.txt

# Configuration snapshot
cp /opt/reference_data_mgr/.env config_snapshot.txt
```

#### Contact Information Template
```
Subject: URGENT - Reference Data System Issue

Environment: [Production/Staging/Development]
Severity: [High/Medium/Low]
Issue Start Time: [YYYY-MM-DD HH:MM]

Symptoms:
- [Describe what users are experiencing]
- [Error messages observed]

Troubleshooting Attempted:
- [List what you've already tried]

Impact:
- [Number of affected users]
- [Business operations affected]

Attached Files:
- system_logs.txt
- app_logs.txt  
- config_snapshot.txt
- screenshots (if applicable)
```

## Preventive Measures

### Regular Maintenance

#### Daily Tasks
```bash
#!/bin/bash
# Daily maintenance script

# Check disk space
df -h /data/reference_data/ | awk 'NR==2{print $5}' | sed 's/%//' | \
  awk '{if($1 > 85) print "WARNING: Disk usage above 85%"}'

# Clean old temporary files
find /data/reference_data/temp -type f -mtime +1 -delete

# Check service status
systemctl is-active reference-data-backend.service || echo "Backend service down"

# Basic health check
curl -s http://localhost:8000/ | grep -q "healthy" || echo "Backend health check failed"
```

#### Weekly Tasks
```bash
#!/bin/bash
# Weekly maintenance script

# Archive old log files
find /opt/reference_data_mgr/backend/logs -name "*.log.*" -mtime +7 -delete

# Database maintenance
sqlcmd -S server -U user -P pass -Q "DELETE FROM ref.system_log WHERE timestamp < DATEADD(day, -30, GETDATE())"

# Check for updates
cd /opt/reference_data_mgr
git fetch
git status

# Performance monitoring
ps aux | grep python | awk '{print $2, $3, $4, $6}' | sort -k3 -nr | head -5
```

#### Monthly Tasks
- Review and analyze system logs
- Update system packages and dependencies
- Review database performance metrics
- Test backup and recovery procedures
- Update documentation and runbooks

This troubleshooting guide provides comprehensive solutions for the most common issues encountered with the Reference Data Auto Ingest System. Use it as a first reference for problem resolution and system maintenance.