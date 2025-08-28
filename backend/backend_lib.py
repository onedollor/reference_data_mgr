"""
Reference Data Backend Library
Unified API for data ingestion without HTTP server dependency
"""

import os
import sys
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import pandas as pd

# Import utilities directly (now in same directory structure)
from utils.database import DatabaseManager
from utils.ingest import DataIngester
from utils.csv_detector import CSVDetector
from utils.file_handler import FileHandler
from utils.logger import Logger
from utils.progress import ProgressTracker

class ReferenceDataAPI:
    """Unified API for reference data operations without HTTP dependency"""
    
    def __init__(self, logger: Optional[Logger] = None):
        """Initialize the API with optional logger"""
        if logger is None:
            logger = Logger()
        
        self.logger = logger
        self.db_manager = DatabaseManager()
        self.data_ingester = DataIngester(self.db_manager, self.logger)
        self.csv_detector = CSVDetector(self.logger)
        self.file_handler = FileHandler()
        self.progress_tracker = ProgressTracker()
        
    def detect_format(self, file_path: str) -> Dict[str, Any]:
        """Detect CSV format and return analysis"""
        try:
            self.logger.log_info(f"Detecting format for: {file_path}")
            
            # Use the existing CSV detector
            detection_result = self.csv_detector.detect_csv_format(file_path)
            
            return {
                "success": True,
                "file_path": file_path,
                "detected_format": detection_result
            }
            
        except Exception as e:
            self.logger.log_error(f"Format detection failed for {file_path}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "file_path": file_path
            }
    
    def analyze_schema_match(self, file_path: str, headers: List[str]) -> Dict[str, Any]:
        """Analyze schema matching with existing tables"""
        try:
            # Get all existing tables
            tables = self.db_manager.get_all_tables()
            
            # Find matching tables based on column schema
            matching_tables = []
            for table in tables:
                table_columns = self.db_manager.get_table_columns(table["name"], table.get("schema", "ref"))
                table_col_names = [col["name"].lower() for col in table_columns]
                file_col_names = [h.lower() for h in headers]
                
                # Calculate match percentage
                matches = len(set(table_col_names) & set(file_col_names))
                total = len(set(table_col_names) | set(file_col_names))
                match_percentage = matches / total if total > 0 else 0
                
                if match_percentage > 0.7:  # 70% match threshold
                    matching_tables.append({
                        "table_name": table["name"],
                        "schema": table.get("schema", "ref"),
                        "match_percentage": match_percentage,
                        "matching_columns": matches,
                        "total_columns": total
                    })
            
            # Sort by match percentage
            matching_tables.sort(key=lambda x: x["match_percentage"], reverse=True)
            
            return {
                "success": True,
                "matching_tables": matching_tables,
                "file_headers": headers
            }
            
        except Exception as e:
            self.logger.log_error(f"Schema analysis failed: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def extract_table_name_from_file(self, file_path: str) -> str:
        """Extract table name from file path using existing logic"""
        try:
            return self.file_handler.extract_table_name(file_path)
        except Exception as e:
            self.logger.log_error(f"Table name extraction failed for {file_path}: {str(e)}")
            # Fallback to simple extraction
            import re
            file_name = Path(file_path).stem
            # Remove date patterns and clean up
            table_name = re.sub(r'[-_.]\d{8}', '', file_name)
            table_name = re.sub(r'[-_.]\d{4}-\d{2}-\d{2}', '', table_name)
            table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)
            table_name = re.sub(r'_+', '_', table_name)
            return table_name.strip('_').lower()
    
    async def process_file_async(
        self, 
        file_path: str, 
        load_type: str = "fullload",
        table_name: Optional[str] = None,
        target_schema: str = "ref",
        config_reference_data: bool = False
    ) -> Dict[str, Any]:
        """Process a file asynchronously using existing ingestion logic"""
        try:
            # Extract table name if not provided
            if table_name is None:
                table_name = self.extract_table_name_from_file(file_path)
            
            self.logger.log_info(f"Processing file: {file_path}")
            self.logger.log_info(f"Table: {table_name}, Load type: {load_type}, Schema: {target_schema}")
            
            # Create format file path (temporary)
            fmt_file_path = f"{file_path}.fmt"
            
            # Use the existing data ingester
            result = await self.data_ingester.ingest_data(
                file_path=file_path,
                fmt_file_path=fmt_file_path,
                load_mode=load_type,
                filename=Path(file_path).name,
                override_load_type=load_type,
                config_reference_data=config_reference_data,
                target_schema=target_schema
            )
            
            # Clean up temporary format file
            if os.path.exists(fmt_file_path):
                os.remove(fmt_file_path)
            
            return {
                "success": True,
                "result": result,
                "table_name": table_name,
                "load_type": load_type,
                "file_path": file_path
            }
            
        except Exception as e:
            self.logger.log_error(f"File processing failed for {file_path}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "file_path": file_path,
                "table_name": table_name
            }
    
    def process_file_sync(
        self, 
        file_path: str, 
        load_type: str = "fullload",
        table_name: Optional[str] = None,
        target_schema: str = "ref",
        config_reference_data: bool = False
    ) -> Dict[str, Any]:
        """Synchronous wrapper for file processing"""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(
                self.process_file_async(
                    file_path, load_type, table_name, target_schema, config_reference_data
                )
            )
        finally:
            loop.close()
    
    def get_table_info(self, table_name: str, schema: str = "ref") -> Dict[str, Any]:
        """Get information about a specific table"""
        try:
            if self.db_manager.table_exists(table_name, schema):
                columns = self.db_manager.get_table_columns(table_name, schema)
                row_count = self.db_manager.get_table_row_count(table_name, schema)
                
                return {
                    "success": True,
                    "table_name": table_name,
                    "schema": schema,
                    "exists": True,
                    "columns": columns,
                    "row_count": row_count
                }
            else:
                return {
                    "success": True,
                    "table_name": table_name,
                    "schema": schema,
                    "exists": False
                }
                
        except Exception as e:
            self.logger.log_error(f"Failed to get table info for {schema}.{table_name}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name,
                "schema": schema
            }
    
    def get_all_tables(self) -> Dict[str, Any]:
        """Get list of all tables"""
        try:
            tables = self.db_manager.get_all_tables()
            return {
                "success": True,
                "tables": tables
            }
        except Exception as e:
            self.logger.log_error(f"Failed to get all tables: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_progress(self, progress_key: str) -> Dict[str, Any]:
        """Get progress information for a specific key"""
        try:
            progress_info = self.progress_tracker.get_progress(progress_key)
            return {
                "success": True,
                "progress": progress_info
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def cancel_operation(self, progress_key: str) -> Dict[str, Any]:
        """Cancel an ongoing operation"""
        try:
            self.progress_tracker.cancel_progress(progress_key)
            return {
                "success": True,
                "message": f"Operation {progress_key} cancelled"
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_system_logs(self, limit: int = 100) -> Dict[str, Any]:
        """Get system logs"""
        try:
            logs = self.logger.get_recent_logs(limit)
            return {
                "success": True,
                "logs": logs
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    def insert_reference_data_cfg_record(self, table_name: str) -> Dict[str, Any]:
        """Insert a record into Reference_Data_Cfg table"""
        try:
            # Get a connection and call the database method
            connection = self.db_manager.get_connection()
            try:
                self.db_manager.insert_reference_data_cfg_record(connection, table_name)
                connection.commit()
                
                self.logger.log_info(f"Reference data config record inserted for table: {table_name}")
                return {
                    "success": True,
                    "message": f"Reference data config record created for {table_name}"
                }
            finally:
                connection.close()
                
        except Exception as e:
            self.logger.log_error(f"Failed to insert reference data config for {table_name}: {str(e)}")
            return {
                "success": False,
                "error": str(e)
            }

    def health_check(self) -> Dict[str, Any]:
        """Check system health"""
        try:
            # Test database connection
            db_healthy = self.db_manager.test_connection()
            
            return {
                "success": True,
                "status": "healthy" if db_healthy else "unhealthy",
                "database": "connected" if db_healthy else "disconnected",
                "timestamp": datetime.now().isoformat()
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "status": "unhealthy"
            }

# Global API instance for easy importing
api = None

def get_api() -> ReferenceDataAPI:
    """Get or create the global API instance"""
    global api
    if api is None:
        api = ReferenceDataAPI()
    return api

# Convenience functions for direct usage
def detect_format(file_path: str) -> Dict[str, Any]:
    return get_api().detect_format(file_path)

def process_file(file_path: str, load_type: str = "fullload", **kwargs) -> Dict[str, Any]:
    return get_api().process_file_sync(file_path, load_type, **kwargs)

def get_table_info(table_name: str, schema: str = "ref") -> Dict[str, Any]:
    return get_api().get_table_info(table_name, schema)

def health_check() -> Dict[str, Any]:
    return get_api().health_check()

if __name__ == "__main__":
    # Test the API
    api = ReferenceDataAPI()
    print("Backend API initialized successfully")
    
    # Test health check
    health = api.health_check()
    print(f"Health check: {health}")