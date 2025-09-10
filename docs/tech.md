# Reference Data Management System - Technical Architecture

## Technology Stack Overview

The Reference Data Management System is built on a modern, scalable technology stack designed for enterprise reliability, performance, and maintainability. The architecture emphasizes Python-based solutions with robust database integration, comprehensive monitoring, and enterprise-grade security.

## Core Technology Decisions

### Programming Language: Python 3.12+
**Rationale**: Python provides excellent support for data processing, database connectivity, and system integration
**Benefits**:
- Rich ecosystem for data processing (pandas, numpy)
- Excellent database connectivity libraries (pyodbc, SQLAlchemy)
- Strong async/await support for concurrent processing
- Mature error handling and logging capabilities
- Extensive testing and monitoring tools

**Considerations**:
- Performance optimization through proper batch processing and async operations
- Memory management for large file processing
- Type safety through type hints and validation

### Database: Microsoft SQL Server
**Rationale**: Enterprise-grade database with strong transaction support and scalability
**Benefits**:
- ACID compliance ensuring data integrity
- Advanced indexing and query optimization
- Built-in backup and recovery capabilities
- Enterprise security and compliance features
- Excellent integration with Python through pyodbc

**Configuration**:
- Connection pooling for efficient resource utilization
- Transaction management with automatic rollback
- Schema-based organization (ref, bkp)
- Optimized indexing for file tracking and audit queries

### File System Architecture
**Rationale**: Structured file system approach for organized processing workflows
**Benefits**:
- Clear separation of processing stages
- Atomic file operations preventing corruption
- Organized audit trail through file movement
- Simple backup and recovery procedures

## System Architecture Principles

### 1. Modularity and Separation of Concerns
**Principle**: Each component has a single, well-defined responsibility
**Implementation**:
- File monitoring separated from data processing
- Format detection isolated from data ingestion
- Database operations abstracted through manager classes
- Utility functions grouped by functionality

**Benefits**:
- Easy testing and debugging
- Independent component evolution
- Clear interfaces and dependencies
- Simplified maintenance and updates

### 2. Asynchronous Processing Model
**Principle**: Non-blocking operations for improved throughput and responsiveness
**Implementation**:
- Async/await pattern for I/O operations
- Background processing for long-running operations
- Progress tracking with cancellation support
- Concurrent file processing capabilities

**Benefits**:
- Improved system responsiveness
- Better resource utilization
- Support for concurrent operations
- Scalable processing architecture

### 3. Defensive Programming
**Principle**: Assume failures will occur and build resilience accordingly
**Implementation**:
- Comprehensive error handling at all levels
- Transaction management with automatic rollback
- Connection retry with exponential backoff
- Graceful degradation under failure conditions

**Benefits**:
- Improved system reliability
- Faster error recovery
- Reduced manual intervention requirements
- Better user experience during failures

### 4. Observability and Monitoring
**Principle**: System behavior must be observable and measurable
**Implementation**:
- Comprehensive logging at multiple levels
- Performance metrics collection
- Health check endpoints
- Audit trail for all operations

**Benefits**:
- Rapid problem identification and resolution
- Performance optimization opportunities
- Compliance and audit support
- Operational visibility and control

## Detailed Technical Architecture

### Core Components Architecture

#### 1. File Monitor Service
**Technology**: Python asyncio with file system monitoring
**Key Libraries**:
- `os` and `pathlib` for file system operations
- `time` and `datetime` for scheduling and timestamps
- `hashlib` for file integrity verification
- `logging` for comprehensive audit trails

**Design Patterns**:
- **Observer Pattern**: File system change detection
- **State Machine**: File processing lifecycle management
- **Strategy Pattern**: Different processing strategies based on file type

**Implementation Details**:
```python
class FileMonitor:
    def __init__(self):
        self.file_tracking = {}  # In-memory state
        self.db_manager = DatabaseManager()
        self.api = ReferenceDataAPI()
    
    async def run(self):
        # Main monitoring loop with 15-second intervals
        while True:
            await self.scan_and_process()
            await asyncio.sleep(15)
```

**Performance Considerations**:
- Efficient file system scanning to minimize I/O overhead
- In-memory tracking with database persistence
- Configurable monitoring intervals
- Resource cleanup for removed files

#### 2. Backend API Layer
**Technology**: Python with direct library integration (no HTTP layer)
**Key Libraries**:
- Direct Python class integration for optimal performance
- `asyncio` for concurrent operations
- `concurrent.futures` for thread pool management
- `typing` for type safety

**Design Patterns**:
- **Facade Pattern**: Simplified interface to complex subsystems
- **Factory Pattern**: Dynamic creation of processing components
- **Command Pattern**: Encapsulation of processing operations

**Implementation Details**:
```python
class ReferenceDataAPI:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.data_ingester = DataIngester()
        self.csv_detector = CSVFormatDetector()
    
    async def process_file_async(self, file_path: str, **kwargs):
        # Orchestrate the complete processing pipeline
        format_info = await self.detect_format(file_path)
        result = await self.data_ingester.ingest_data(...)
        return result
```

#### 3. Data Processing Engine
**Technology**: Python with pandas for data manipulation
**Key Libraries**:
- `pandas` for data processing and analysis
- `numpy` for numerical operations
- `pyodbc` for database connectivity
- `asyncio` for async processing

**Design Patterns**:
- **Pipeline Pattern**: Sequential data processing stages
- **Iterator Pattern**: Memory-efficient large file processing
- **Template Method**: Standardized processing workflow with customization points

**Implementation Details**:
```python
class DataIngester:
    async def ingest_data(self, file_path: str, **kwargs):
        async for progress in self._process_batches(file_path):
            yield progress
    
    async def _process_batches(self, file_path: str):
        # Stream processing with configurable batch sizes
        for batch in self._read_batches(file_path, batch_size=990):
            await self._insert_batch(batch)
            yield f"Processed {batch.index} rows"
```

### Database Architecture

#### Schema Design
**Primary Schema (ref)**:
- All reference data tables
- File tracking and audit tables
- Configuration and metadata tables

**Backup Schema (bkp)**:
- Historical data snapshots
- Backup configurations
- Recovery metadata

**Connection Management**:
```python
class DatabaseManager:
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.pool_size = int(os.getenv("DB_POOL_SIZE", "5"))
        self._pool = []
        self._pool_lock = threading.Lock()
    
    def get_connection(self):
        # Connection with retry logic and health checking
        return self._get_connection_with_retry()
```

#### Transaction Strategy
**ACID Compliance**: All operations wrapped in database transactions
**Isolation Levels**: Read Committed for balance of consistency and performance
**Retry Logic**: Exponential backoff for transient failures
**Deadlock Handling**: Automatic retry with jittered delays

### Security Architecture

#### Authentication and Authorization
**Database Security**:
- SQL Server authentication with encrypted connections
- Least-privilege principle for database user accounts
- Connection string encryption and secure storage
- Regular password rotation procedures

**File System Security**:
- Appropriate file permissions for all processing directories
- Secure temporary file handling
- File integrity verification through hashing
- Access logging for all file operations

#### Data Protection
**Encryption in Transit**:
- TLS encryption for all database connections
- Secure file transfer protocols where applicable

**Encryption at Rest**:
- Database-level encryption for sensitive data
- Secure storage of configuration and credentials

**Audit and Compliance**:
- Comprehensive audit logging for all operations
- Data lineage tracking from source to destination
- Compliance reporting capabilities
- Secure log storage and retention

### Performance Architecture

#### Scalability Design
**Vertical Scaling**:
- Memory-efficient processing for large files
- CPU optimization through batch processing
- I/O optimization through async operations
- Database query optimization

**Horizontal Scaling**:
- Multi-instance deployment capability
- Distributed file processing coordination
- Load balancing across processing nodes
- Shared state management through database

#### Performance Optimization
**Memory Management**:
- Stream processing for large files to minimize memory usage
- Efficient garbage collection through proper resource cleanup
- Memory pooling for frequently used objects
- Configurable memory limits and monitoring

**I/O Optimization**:
- Batch database operations to reduce roundtrips
- Efficient file system operations with minimal scanning
- Asynchronous I/O for concurrent operations
- Connection pooling to reduce connection overhead

**CPU Optimization**:
- Efficient algorithms for format detection and processing
- Minimal CPU overhead during monitoring periods
- Optimized data structures for tracking and processing
- Parallel processing where beneficial

### Monitoring and Observability

#### Logging Architecture
**Multi-Level Logging**:
- DEBUG: Detailed diagnostic information
- INFO: General operational information
- WARNING: Potential issues and recoverable errors
- ERROR: Serious problems requiring attention

**Structured Logging**:
```python
class Logger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler('/logs/app.log')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def log_processing_event(self, file_path: str, event: str, metadata: dict):
        self.logger.info(f"Processing event: {event}", extra={
            'file_path': file_path,
            'event': event,
            'metadata': metadata
        })
```

#### Metrics Collection
**Performance Metrics**:
- Processing throughput (rows per second)
- File detection latency
- Database operation times
- Memory and CPU utilization

**Business Metrics**:
- Processing success rates
- Error categorization and frequency
- Data quality metrics
- System availability

**Health Monitoring**:
- Database connectivity status
- File system accessibility
- System resource availability
- Processing queue status

## Development and Deployment Architecture

### Development Environment
**Version Control**: Git with feature branch workflow
**Code Quality**:
- Type hints throughout codebase
- Comprehensive unit and integration tests
- Code coverage monitoring (target: >80%)
- Automated linting and formatting

**Testing Strategy**:
```python
class TestDataIngester(unittest.TestCase):
    def setUp(self):
        self.test_db = create_test_database()
        self.ingester = DataIngester(self.test_db)
    
    async def test_batch_processing(self):
        # Test batch processing with known data
        test_file = create_test_csv(rows=10000)
        results = []
        async for message in self.ingester.ingest_data(test_file):
            results.append(message)
        self.assertEqual(len(results), expected_progress_updates)
```

### Deployment Architecture
**Environment Management**:
- Environment-specific configuration files
- Secure credential management
- Automated deployment scripts
- Blue-green deployment support

**Infrastructure Requirements**:
- Python 3.12+ runtime environment
- SQL Server database access
- Sufficient storage for file processing and archival
- Network connectivity for monitoring and alerting

**Monitoring Integration**:
- Application performance monitoring (APM)
- Infrastructure monitoring and alerting
- Log aggregation and analysis
- Health check endpoints for load balancers

## Configuration Management

### Environment Configuration
**Database Configuration**:
```bash
# Primary database settings
db_host=sql-server.company.com
db_name=reference_data
db_user=ingest_service
db_password=${SECURE_PASSWORD}
db_odbc_driver=ODBC Driver 17 for SQL Server

# Connection pool settings
DB_POOL_SIZE=5
DB_MAX_RETRIES=3
DB_RETRY_BACKOFF=0.5
```

**Processing Configuration**:
```bash
# File processing settings
MONITOR_INTERVAL=15
STABILITY_CHECKS=6
INGEST_BATCH_SIZE=990

# Performance settings
INGEST_PROGRESS_INTERVAL=5
INGEST_TYPE_INFERENCE=1
INGEST_TYPE_SAMPLE_ROWS=5000
```

### Runtime Configuration
**Dynamic Configuration**:
- Database connection parameters
- Processing batch sizes
- Monitoring intervals
- Logging levels

**Static Configuration**:
- File system paths
- Schema names
- Security settings
- System limits

## Technology Roadmap

### Current Technology Stack (v1.0)
- Python 3.12 with asyncio
- SQL Server with pyodbc
- File system monitoring
- Pandas for data processing

### Near-term Enhancements (v2.0)
- Enhanced monitoring with Prometheus/Grafana
- Containerization with Docker
- CI/CD pipeline automation
- Advanced configuration management

### Long-term Evolution (v3.0+)
- Microservices architecture
- Event-driven processing with message queues
- Machine learning for format detection
- Cloud-native deployment options

## Technical Decision Log

### Decision 1: Python vs Other Languages
**Decision**: Use Python as primary development language
**Rationale**: 
- Excellent data processing ecosystem
- Strong database connectivity options
- Mature async programming support
- Team expertise and community support

**Alternatives Considered**: Java, C#, Node.js
**Trade-offs**: Slightly lower raw performance for significantly better development productivity

### Decision 2: Direct Integration vs HTTP API
**Decision**: Use direct Python library integration instead of HTTP API
**Rationale**:
- Lower latency and overhead
- Simpler deployment and configuration
- Better error handling and debugging
- More efficient resource utilization

**Alternatives Considered**: REST API, gRPC
**Trade-offs**: Less flexibility for distributed deployment, but better performance and simplicity

### Decision 3: File System vs Message Queue
**Decision**: Use file system monitoring instead of message queue
**Rationale**:
- Simpler deployment and maintenance
- Natural atomic operations through file system
- Better integration with existing file-based workflows
- Clear audit trail through file movement

**Alternatives Considered**: Apache Kafka, RabbitMQ
**Trade-offs**: Less scalable for very high volumes, but simpler and more reliable for typical use cases

This technical architecture provides a robust foundation for building and evolving the Reference Data Management System, balancing performance, reliability, and maintainability while supporting future growth and enhancement requirements.