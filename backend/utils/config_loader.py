import yaml
import os
from pathlib import Path
from typing import Any, Dict, Optional

# Global environment variable
ENVIRONMENT: Optional[str] = None

def set_environment(env: str) -> None:
    """Set the global environment (dev/sit/prd)"""
    global ENVIRONMENT
    ENVIRONMENT = env.lower() if env else None

def get_environment() -> Optional[str]:
    """Get the current environment"""
    return ENVIRONMENT

class ConfigLoader:
    """YAML configuration loader"""

    _instances: Dict[str, 'ConfigLoader'] = {}

    def __new__(cls, config_file: Optional[str] = None) -> 'ConfigLoader':
        # Use default config path if none provided
        if config_file is None:
            base_path = Path(__file__).parent.parent.parent / "config"
            if ENVIRONMENT:
                # Use environment-specific config if available
                env_config = base_path / f"{ENVIRONMENT}.yaml"
                if env_config.exists():
                    config_file = str(env_config)
                else:
                    config_file = str(base_path / "config.yaml")
            else:
                config_file = str(base_path / "config.yaml")

        # Normalize the path to avoid duplicates
        config_file = str(Path(config_file).resolve())

        # Return existing instance for this config file, or create new one
        if config_file not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[config_file] = instance
            instance._initialized = False

        return cls._instances[config_file]

    def __init__(self, config_file: Optional[str] = None):
        # Only initialize once per instance
        if hasattr(self, '_initialized') and self._initialized:
            return

        self._config: Optional[Dict[str, Any]] = None
        self._config_file: Optional[str] = None
        self._load_config(config_file)
        self._initialized = True

    def _load_config(self, config_file: Optional[str] = None) -> None:
        """Load configuration from YAML file"""
        if config_file:
            config_path = Path(config_file)
            self._config_file = config_file
        else:
            config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
            self._config_file = str(config_path)

        # Normalize the path
        config_path = config_path.resolve()
        self._config_file = str(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r', encoding='utf-8') as file:
            self._config = yaml.safe_load(file)
    
    def get(self, key: str, default: Any = None, section: Optional[str] = None) -> Any:
        """
        Get configuration value from YAML file

        Args:
            key: Configuration key
            default: Default value if not found
            section: Configuration section (e.g., 'database', 'logging')

        Returns:
            Configuration value or default
        """
        # Get from YAML config
        if section and self._config and section in self._config:
            return self._config[section].get(key, default)
        elif self._config:
            return self._config.get(key, default)

        return default
    
    
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
            'processed_location': self.get('processed_location', '/home/lin/repo/reference_data_mgr/data/reference_data/processed', 'file_handling'),
            'error_location': self.get('error_location', '/home/lin/repo/reference_data_mgr/data/reference_data/error', 'file_handling'),
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

def get_config(config_file: Optional[str] = None) -> ConfigLoader:
    """Get ConfigLoader instance with optional config file"""
    return ConfigLoader(config_file)

def get_config_by_env(env: Optional[str] = None) -> ConfigLoader:
    """Get ConfigLoader instance for specific environment"""
    if env:
        config_file = str(Path(__file__).parent.parent.parent / "config" / f"{env.lower()}.yaml")
        return ConfigLoader(config_file)
    return ConfigLoader()