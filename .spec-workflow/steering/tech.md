# Technology Stack

## Project Type
Enterprise-grade automated data processing system designed as a long-running service with file system monitoring and batch processing capabilities. The system operates as a daemon process for continuous CSV file ingestion into SQL Server databases.

## Core Technologies

### Primary Language(s)
- **Language**: Python 3.12+
- **Runtime/Compiler**: CPython interpreter with asyncio event loop support
- **Language-specific tools**: pip for package management, venv for virtual environments, type hints for static analysis

### Key Dependencies/Libraries
- **pyodbc**: SQL Server database connectivity and connection pooling
- **pandas**: Data processing and CSV parsing for large file handling
- **asyncio**: Asynchronous processing and concurrent file operations
- **logging**: Multi-level logging and audit trail implementation
- **hashlib**: File integrity verification and duplicate detection
- **pathlib**: Modern file system operations and path manipulation
- **python-dotenv**: Environment variable management and configuration

### Application Architecture
The system uses an event-driven architecture with continuous file system monitoring triggering asynchronous processing pipelines. Core design patterns include:

- **Observer Pattern**: File system change detection and monitoring
- **Strategy Pattern**: Different processing modes (fullload, append) and data types (reference, non-reference)
- **Factory Pattern**: Dynamic creation of processing components based on file characteristics
- **Pipeline Pattern**: Sequential data processing stages with error handling

### Data Storage
- **Primary storage**: Microsoft SQL Server with schema-based organization (ref, bkp schemas)
- **Caching**: In-memory file tracking dictionary with database persistence for state recovery
- **Data formats**: CSV input files, JSON configuration, SQL Server native data types

### External Integrations
- **APIs**: Direct Python library integration (no HTTP layer) for optimal performance
- **Protocols**: SQL Server native protocol through pyodbc with TLS encryption
- **Authentication**: SQL Server authentication with secure credential management

## Development Environment

### Build & Development Tools
- **Build System**: Python setuptools with pip-based dependency management
- **Package Management**: pip with requirements.txt for reproducible environments
- **Development workflow**: Direct Python execution with file watching for development
- **Virtual Environment**: venv for isolated dependency management

### Code Quality Tools
- **Static Analysis**: Type hints throughout codebase with mypy validation
- **Formatting**: Python PEP 8 compliance with automatic formatting tools
- **Testing Framework**: unittest and pytest for unit testing, integration test framework
- **Documentation**: Inline docstrings with comprehensive code comments

### Version Control & Collaboration
- **VCS**: Git with feature branch workflow
- **Branching Strategy**: Main branch with feature branches for development
- **Code Review Process**: Pull request-based code review with testing validation

## Deployment & Distribution
- **Target Platform(s)**: Linux and Windows servers with Python 3.12+ support
- **Distribution Method**: Source code deployment with virtual environment setup
- **Installation Requirements**: Python 3.12+, SQL Server connectivity, file system permissions
- **Update Mechanism**: Direct file replacement with graceful service restart

## Technical Requirements & Constraints

### Performance Requirements
- **File Detection Latency**: <15 seconds from file drop to processing initiation
- **Processing Throughput**: >1,000 rows/second for standard CSV files
- **Memory Usage**: <512MB per 100MB file processed through streaming
- **Database Connection Efficiency**: <80% connection pool utilization under normal load

### Compatibility Requirements  
- **Platform Support**: Linux (Ubuntu 20.04+) and Windows Server 2019+ environments
- **Dependency Versions**: Python 3.12+, SQL Server 2017+, ODBC Driver 17+ for SQL Server
- **Standards Compliance**: CSV RFC 4180 with extended delimiter support, SQL ANSI compliance

### Security & Compliance
- **Security Requirements**: TLS-encrypted database connections, secure credential storage, file access control
- **Compliance Standards**: Complete audit trail for regulatory compliance, data lineage tracking
- **Threat Model**: File system access control, SQL injection prevention, connection security

### Scalability & Reliability
- **Expected Load**: 1000+ files processed daily, concurrent processing of 10+ files, 100MB+ individual file sizes
- **Availability Requirements**: 99.9% uptime during business hours, automatic recovery from failures
- **Growth Projections**: Horizontal scaling through multiple monitor instances, vertical scaling for larger files

## Technical Decisions & Rationale

### Decision Log
1. **Python vs Java/C# for Implementation**: Chose Python for excellent data processing ecosystem (pandas, numpy), rapid development, and strong database connectivity options. Trade-off: Slightly lower raw performance for significantly better development productivity.

2. **Direct Integration vs HTTP API Architecture**: Selected direct Python library integration over REST API for lower latency, simpler deployment, better error handling, and more efficient resource utilization. Trade-off: Less flexibility for distributed deployment, but better performance and operational simplicity.

3. **File System Monitoring vs Message Queue**: Implemented file system monitoring instead of message queue (Kafka/RabbitMQ) for simpler deployment, natural atomic operations through file system, better integration with existing workflows, and clearer audit trail. Trade-off: Less scalable for very high volumes, but simpler and more reliable for typical enterprise use cases.

4. **SQL Server vs PostgreSQL**: Chose SQL Server for enterprise integration requirements, existing organizational infrastructure, advanced indexing capabilities, and built-in high availability features. Trade-off: Higher licensing costs but better enterprise feature set.

5. **Async Processing vs Threading**: Implemented asyncio-based async processing for I/O-bound operations, better resource utilization, and cleaner concurrent code. Trade-off: More complex error handling but significantly better performance for file and database operations.

## Known Limitations

- **Single Node Processing**: Current architecture requires single monitor instance per folder structure. Future enhancement will support distributed processing across multiple nodes.

- **CSV Format Dependency**: System optimized for CSV files only. Extension to other formats (JSON, XML, Parquet) will require additional format detection modules.

- **Memory Constraints for Very Large Files**: Files approaching GB sizes may require streaming optimizations beyond current batch processing approach.

- **Limited Dashboard Integration**: Current implementation uses file-based logging. Future versions will include web-based monitoring dashboard for real-time status visibility.