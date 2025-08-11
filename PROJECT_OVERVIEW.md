# Reference Data Auto Ingest System - Project Overview

## Executive Summary

The Reference Data Auto Ingest System is a **production-ready, enterprise-grade** data management application that automates CSV reference data ingestion into SQL Server databases. Built with modern FastAPI backend and React frontend, the system delivers advanced features including intelligent load type management, comprehensive backup/rollback capabilities, and professional TD Bank themed user interface.

**Current Status**: ✅ **FULLY IMPLEMENTED, SECURITY HARDENED, AND PRODUCTION READY**

---

## 🏗️ System Architecture

### Multi-Tier Enterprise Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                FRONTEND TIER (React + TD Bank Theme)       │
├─────────────────────────────────────────────────────────────┤
│  • Professional TD Bank Branding  • Load Type Management   │
│  • Advanced Rollback Interface    • Real-time Progress     │
│  • CSV Export Capabilities        • Error Handling & Logs  │
│  • Responsive Material-UI Design  • Configuration Panels   │
└─────────────────────────────────────────────────────────────┘
                                    │
                               HTTP/WebSocket
                                    │
┌─────────────────────────────────────────────────────────────┐
│                   BACKEND TIER (FastAPI)                   │
├─────────────────────────────────────────────────────────────┤
│  • Advanced Load Type Detection   • Backup Version Control │
│  • Rollback & CSV Export APIs     • Real-time Streaming    │
│  • Reference_Data_Cfg Integration • Security Hardening    │
│  • Background Task Processing     • Comprehensive Logging  │
└─────────────────────────────────────────────────────────────┘
                                    │
                              pyodbc (Secured)
                                    │
┌─────────────────────────────────────────────────────────────┐
│              DATABASE TIER (SQL Server + Integration)      │
├─────────────────────────────────────────────────────────────┤
│  Schema: ref (Main + Stage)      │  Schema: bkp (Backup)    │
│  • Data Tables w/ loadtype      │  • Versioned Backups     │
│  • Reference_Data_Cfg table     │  • Point-in-time Recovery│
│  • Post-load Procedures         │  • Export Capabilities   │
│  • Advanced Validation          │  • Metadata Tracking     │
└─────────────────────────────────────────────────────────────┘
```

---

## 🚀 Core Features & Advanced Capabilities

### ✅ **Load Type Management System** (NEW ADVANCED FEATURE)
- **🎯 Intelligent Load Type Determination**: Automatic detection of 'F' (Full) or 'A' (Append) based on existing data patterns
- **🔍 Load Type Verification**: Pre-upload validation with mismatch detection and user warnings
- **⚠️ User Override Dialog**: Professional warning interface for load type conflicts with clear explanations
- **📊 Data Pattern Analysis**: Historical load type tracking with `loadtype` column in all tables
- **🔄 Dynamic Load Mode Handling**: Smart adaptation between user intent and data consistency

### ✅ **Comprehensive Backup and Rollback System** (NEW ENTERPRISE FEATURE)
- **📦 Automatic Backup Versioning**: Every full load creates timestamped backup with incremental version_id
- **⏰ Point-in-time Rollback**: Restore main and stage tables to any previous backup version
- **📤 CSV Export Capabilities**: Export main tables and specific backup versions to CSV format
- **🔍 Advanced Backup Browser**: Professional UI with filtering, search, and data preview
- **🏷️ Metadata Tracking**: Complete audit trail with version history and load tracking
- **✅ Validation & Recovery**: Schema compatibility checks and graceful recovery mechanisms

### ✅ **Professional User Interface** (TD BANK THEMED)
- **🎨 Corporate Branding**: Professional TD Bank color scheme and styling throughout
- **📱 Responsive Design**: Optimized for desktop and mobile with Material-UI components
- **⚡ Real-time Progress**: Live progress tracking with row counts and detailed status updates
- **🚨 Advanced Warning Dialogs**: User-friendly load type conflict resolution with clear explanations
- **🔄 Rollback Management**: Intuitive interface for browsing backups and executing rollbacks
- **📊 Data Preview**: Sample data display and validation feedback

### ✅ **Enhanced Database Integration**
- **🏛️ Reference_Data_Cfg Integration**: Automatic table registration with post-load procedure calls
- **🔗 Three-Schema Architecture**: ref (main/stage), bkp (backup), dbo (configuration)
- **🛡️ Security Hardened**: All parameterized queries prevent SQL injection vulnerabilities
- **⚡ Connection Pooling**: Optimized database connections with retry logic and health monitoring
- **📋 Schema Synchronization**: Automatic table creation and validation with migration support

### ✅ **Advanced CSV Processing**
- **🤖 Smart Format Detection**: Auto-detection of delimiters, qualifiers, headers, and trailer patterns
- **🏷️ Intelligent Trailer Handling**: Pattern-based trailer detection with validation and removal
- **📊 Type Inference Engine**: Configurable data type inference with safe varchar fallback
- **🔍 Sample Data Preview**: Real-time preview of parsed data with format validation
- **📝 Format Persistence**: .fmt file storage for reusable format configurations

---

## 🛡️ Security Excellence

### Critical Security Features (Recently Enhanced)
- ✅ **Complete SQL Injection Prevention**: All database operations use parameterized queries
- ✅ **Input Validation & Sanitization**: Comprehensive validation of all user inputs and file uploads
- ✅ **Safe Error Handling**: Sanitized error messages prevent information leakage
- ✅ **File Upload Security**: Type validation, size limits, and secure path handling
- ✅ **Column Name Sanitization**: Regex-based validation of SQL identifiers

---

## 🔧 Advanced API Architecture

### Core System Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | System health check and version info |
| GET | `/config` | System configuration and delimiter options |
| GET | `/features` | Feature flags and capabilities |

### Advanced Backup & Rollback Endpoints (NEW)
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/backups` | List all backup tables with metadata |
| GET | `/backups/{base_name}/versions` | Get backup version history |
| GET | `/backups/{base_name}/versions/{version_id}` | View specific backup version data |
| POST | `/backups/{base_name}/rollback/{version_id}` | Execute rollback to version |
| GET | `/backups/{base_name}/export-main` | Export main table to CSV |
| GET | `/backups/{base_name}/versions/{version_id}/export` | Export backup version to CSV |

### Load Type Management Endpoints (NEW)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/verify-load-type` | Verify load type compatibility |
| GET | `/reference-data-config` | Get Reference_Data_Cfg records |

### File Processing & Progress Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/detect-format` | Auto-detect CSV format parameters |
| POST | `/upload` | Upload file with format configuration |
| POST | `/ingest/{filename}` | Stream ingestion with load type override |
| GET | `/progress/{key}` | Real-time progress tracking |
| POST | `/progress/{key}/cancel` | Cancel ongoing ingestion |

---

## 📊 Advanced Database Schema

### Main Tables (ref schema) - Enhanced with Load Type Tracking
```sql
CREATE TABLE [ref].[{table_name}] (
    [{user_column_1}] varchar(4000),
    [{user_column_2}] varchar(4000),
    -- ... additional user columns
    [ref_data_loadtime] datetime DEFAULT GETDATE(),
    [loadtype] varchar(255)  -- 'F' for Full, 'A' for Append
)
```

### Backup Tables (bkp schema) - With Version Control
```sql
CREATE TABLE [bkp].[{table_name}_backup] (
    -- Main table structure plus:
    [version_id] int NOT NULL,
    [ref_data_loadtime] datetime,
    [loadtype] varchar(255)
)
```

### Reference Data Configuration (dbo schema) - Enterprise Integration
```sql
CREATE TABLE [dbo].[Reference_Data_Cfg](
    [Id] int IDENTITY(1,1) PRIMARY KEY,
    [TableName] varchar(255) NOT NULL,
    [LastUpdated] datetime DEFAULT GETDATE(),
    [Status] varchar(50),
    [RecordCount] int
)
```

---

## 🎯 Load Type Management Workflow

### Intelligent Load Type Determination
1. **User Selection**: User selects "Full Load" or "Append Load" mode
2. **Data Analysis**: System analyzes existing data for historical load type patterns
3. **Mismatch Detection**: Identifies conflicts between user intent and data consistency
4. **Warning Dialog**: Professional TD Bank themed dialog explains the situation
5. **User Override**: Option to force specific load type with clear consequences
6. **Load Execution**: Proceeds with determined or overridden load type

### Load Type Decision Matrix
| Existing Data | User Mode | System Decision | User Override |
|---------------|-----------|-----------------|---------------|
| None | Full/Append | Use user choice | Not needed |
| Only 'F' | Append | Warning dialog | Can force 'A' |
| Only 'A' | Full | Warning dialog | Can force 'F' |
| Mixed F&A | Either | Use existing pattern | Can override |

---

## 🔄 Backup and Rollback Workflow

### Automated Backup Process
1. **Pre-load Check**: Verify existing data before any modifications
2. **Version Assignment**: Auto-increment version_id for new backup
3. **Schema Validation**: Ensure backup table compatibility
4. **Data Backup**: Copy existing data with metadata preservation
5. **Load Execution**: Proceed with confident data safety

### Rollback Capabilities
1. **Version Browser**: Professional UI showing all backup versions
2. **Data Preview**: Sample data display with filtering and search
3. **Rollback Execution**: One-click restore to any previous version
4. **Validation**: Ensure rollback success with row count verification
5. **CSV Export**: Download main table or backup version data

---

## 🏭 Production Readiness Features

### Enterprise Integration
- **Reference_Data_Cfg Table**: Automatic registration of all processed tables
- **Post-load Procedures**: Custom `usp_reference_data_postLoad` execution
- **Audit Trail**: Complete operational history with user attribution
- **Performance Monitoring**: Connection pool statistics and health checks

### Operational Excellence
- **Health Monitoring**: Comprehensive system health endpoints
- **Error Recovery**: Graceful failure handling with detailed logging
- **Resource Management**: Automatic cleanup and connection management
- **Scalability**: Background processing and concurrent operation support

### User Experience Excellence
- **Professional Branding**: TD Bank corporate theme throughout
- **Intuitive Interface**: Clear navigation and user-friendly workflows
- **Real-time Feedback**: Progress tracking and immediate error notification
- **Help & Guidance**: Contextual warnings and explanatory dialogs

---

## 📈 Performance Characteristics

### Processing Capabilities
- **File Size Support**: Up to 20MB CSV files (configurable)
- **Concurrent Operations**: Multiple users and simultaneous uploads
- **Memory Efficiency**: Streaming processing for large datasets
- **Database Performance**: Connection pooling with configurable pool size

### Advanced Features Performance
- **Load Type Analysis**: Near-instantaneous decision making
- **Backup Creation**: Efficient versioned backup with minimal overhead
- **Rollback Speed**: Fast restoration from any backup version
- **CSV Export**: Streaming export for large datasets

---

## ✨ Conclusion

The Reference Data Auto Ingest System represents a **complete enterprise-grade solution** that successfully delivers:

### Key Achievements:
- **✅ Advanced Load Type Management**: Intelligent detection and user override capabilities
- **🔄 Comprehensive Backup System**: Version-controlled backups with rollback functionality
- **🎨 Professional User Interface**: TD Bank themed with enterprise-quality design
- **🛡️ Security Excellence**: Hardened against vulnerabilities with comprehensive validation
- **🚀 Production Ready**: Immediate deployment capability with operational monitoring
- **🏗️ Enterprise Integration**: Reference_Data_Cfg and post-load procedure support

### Production Status:
The system is **immediately deployable** to production environments and provides advanced capabilities that exceed typical data ingestion tools. With intelligent load type management, comprehensive backup/rollback functionality, professional user interface, and enterprise integration features, the system delivers exceptional value for reference data management operations.

### Enterprise Value:
- **Data Safety**: Never lose data with automatic versioned backups
- **User Confidence**: Clear warnings and override capabilities for edge cases
- **Operational Efficiency**: Streamlined workflows with professional interface
- **Integration Ready**: Direct integration with existing enterprise procedures
- **Audit Compliance**: Complete operational tracking and history

**Status**: ✅ **ENTERPRISE PRODUCTION READY WITH ADVANCED FEATURES**