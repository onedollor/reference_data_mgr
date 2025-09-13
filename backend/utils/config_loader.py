import yaml
import os
from pathlib import Path
from typing import Any, Dict, Optional

class ConfigLoader:
    """YAML configuration loader with environment variable support"""
    
    _instance: Optional['ConfigLoader'] = None
    _config: Optional[Dict[str, Any]] = None
    
    def __new__(cls) -> 'ConfigLoader':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_config()
    
    def _load_config(self) -> None:
        """Load configuration from YAML file"""
        config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
        
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r', encoding='utf-8') as file:
            self._config = yaml.safe_load(file)
    
    def get(self, key: str, default: Any = None, section: Optional[str] = None) -> Any:
        """
        Get configuration value with optional environment variable override
        
        Args:
            key: Configuration key
            default: Default value if not found
            section: Configuration section (e.g., 'database', 'logging')
            
        Returns:
            Configuration value, environment variable value, or default
        """
        # Check environment variable first (using original env var names)
        env_mappings = {
            # Logging
            'timezone': 'LOG_TIMEZONE',
            
            # Database
            'host': 'db_host',
            'name': 'db_name', 
            'user': 'db_user',
            'password': 'db_password',
            'odbc_driver': 'db_odbc_driver',
            'pool_size': 'DB_POOL_SIZE',
            'max_retries': 'DB_MAX_RETRIES',
            'retry_backoff': 'DB_RETRY_BACKOFF',
            
            # Schemas
            'data_schema': 'data_schema',
            'backup_schema': 'backup_schema',
            'validation_sp_schema': 'validation_sp_schema',
            'staff_database': 'staff_database',
            
            # File handling
            'temp_location': 'temp_location',
            'archive_location': 'archive_location',
            'format_location': 'format_location',
            'dropoff_path': 'SIMPLIFIED_DROPOFF_PATH',
            'max_upload_size': 'max_upload_size',
            
            # Ingest
            'progress_interval': 'INGEST_PROGRESS_INTERVAL',
            'type_inference': 'INGEST_TYPE_INFERENCE',
            'type_sample_rows': 'INGEST_TYPE_SAMPLE_ROWS',
            'date_threshold': 'INGEST_DATE_THRESHOLD',
            'batch_size': 'INGEST_BATCH_SIZE',
            'slow_progress_demo': 'DEMO_SLOW_PROGRESS',
            'persist_schema': 'INGEST_PERSIST_SCHEMA',
            
            # Debug
            'enabled': 'DEBUG',
            
            # Monitor
            'interval': 'MONITOR_INTERVAL',
            'stability_checks': 'STABILITY_CHECKS',
            'log_file': 'LOG_FILE'
        }
        
        env_var = env_mappings.get(key)
        if env_var:
            env_value = os.getenv(env_var)
            if env_value is not None:
                # Convert string values to appropriate types
                return self._convert_type(env_value, key)
        
        # Get from YAML config
        if section and self._config and section in self._config:
            return self._config[section].get(key, default)
        elif self._config:
            return self._config.get(key, default)
        
        return default
    
    def _convert_type(self, value: str, key: str) -> Any:
        """Convert string environment variable to appropriate type"""
        # Boolean conversions
        if key in ['type_inference', 'slow_progress_demo', 'persist_schema', 'enabled']:
            return value.lower() in ('1', 'true', 'yes', 'on')
        
        # Integer conversions
        if key in ['pool_size', 'max_retries', 'progress_interval', 'type_sample_rows', 'batch_size', 'max_upload_size', 'interval', 'stability_checks']:
            try:
                return int(value)
            except ValueError:
                return value
        
        # Float conversions
        if key in ['retry_backoff', 'date_threshold']:
            try:
                return float(value)
            except ValueError:
                return value
        
        return value
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration section"""
        config = {
            'host': self.get('host', 'localhost', 'database'),
            'name': self.get('name', 'test', 'database'),
            'user': self.get('user', None, 'database'),
            'password': self.get('password', None, 'database'),
            'odbc_driver': self.get('odbc_driver', 'ODBC Driver 17 for SQL Server', 'database'),
            'pool_size': self.get('pool_size', 5, 'database'),
            'max_retries': self.get('max_retries', 3, 'database'),
            'retry_backoff': self.get('retry_backoff', 0.5, 'database'),
        }
        return config
    
    def get_file_config(self) -> Dict[str, Any]:
        """Get file handling configuration section"""
        config = {
            'temp_location': self.get('temp_location', 'C:\\data\\reference_data\\temp', 'file_handling'),
            'archive_location': self.get('archive_location', 'C:\\data\\reference_data\\archive', 'file_handling'),
            'format_location': self.get('format_location', 'C:\\data\\reference_data\\format', 'file_handling'),
            'dropoff_path': self.get('dropoff_path', '/home/lin/repo/reference_data_mgr/data/reference_data/dropoff', 'file_handling'),
            'max_upload_size': self.get('max_upload_size', 20971520, 'file_handling'),
        }
        return config
    
    def get_ingest_config(self) -> Dict[str, Any]:
        """Get ingest configuration section"""
        config = {
            'progress_interval': self.get('progress_interval', 5, 'ingest'),
            'type_inference': self.get('type_inference', False, 'ingest'),
            'type_sample_rows': self.get('type_sample_rows', 5000, 'ingest'),
            'date_threshold': self.get('date_threshold', 0.8, 'ingest'),
            'batch_size': self.get('batch_size', 500, 'ingest'),
            'slow_progress_demo': self.get('slow_progress_demo', False, 'ingest'),
            'persist_schema': self.get('persist_schema', False, 'ingest'),
        }
        return config
    
    def get_monitor_config(self) -> Dict[str, Any]:
        """Get monitor configuration section"""
        config = {
            'interval': self.get('interval', 15, 'monitor'),
            'stability_checks': self.get('stability_checks', 6, 'monitor'),
            'log_file': self.get('log_file', '/home/lin/repo/reference_data_mgr/logs/simplified_file_monitor.log', 'monitor'),
        }
        return config

# Singleton instance
config = ConfigLoader()