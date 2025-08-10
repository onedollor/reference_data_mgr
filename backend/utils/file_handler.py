"""
File handling utilities for CSV upload and processing
"""

import os
import json
import shutil
import aiofiles
import traceback
from typing import Optional, Tuple, Dict, Any
from datetime import datetime
from fastapi import UploadFile


class FileHandler:
    """Handles file operations for the Reference Data Auto Ingest System"""
    
    def __init__(self):
        self.temp_location = os.getenv("temp_location", "C:\\data\\reference_data\\temp")
        self.archive_location = os.getenv("archive_location", "C:\\data\\reference_data\\archive")
        self.format_location = os.getenv("format_location", "C:\\data\\reference_data\\format")
        
        # Ensure directories exist
        self._ensure_directories()
    
    def _ensure_directories(self):
        """Ensure all required directories exist"""
        directories = [self.temp_location, self.archive_location, self.format_location]
        
        for directory in directories:
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                print(f"Warning: Could not create directory {directory}: {str(e)}")
    
    async def save_uploaded_file(
        self,
        file: UploadFile,
        header_delimiter: str,
        column_delimiter: str,
        row_delimiter: str,
        text_qualifier: str,
        skip_lines: int = 0,
        trailer_line: Optional[str] = None
    ) -> Tuple[str, str]:
        """
        Save uploaded file to temp location and create corresponding .fmt file
        Returns: (temp_file_path, fmt_file_path)
        """
        try:
            # Generate file paths
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = self._sanitize_filename(file.filename)
            temp_file_path = os.path.join(self.temp_location, f"{timestamp}_{safe_filename}")
            
            # Save the uploaded file
            async with aiofiles.open(temp_file_path, 'wb') as f:
                content = await file.read()
                await f.write(content)
            
            # Create format file
            fmt_file_path = await self._create_format_file(
                safe_filename,
                header_delimiter,
                column_delimiter, 
                row_delimiter,
                text_qualifier,
                skip_lines,
                trailer_line,
                timestamp
            )
            
            return temp_file_path, fmt_file_path
            
        except Exception as e:
            raise Exception(f"Failed to save uploaded file: {str(e)}")
    
    async def _create_format_file(
        self,
        filename: str,
        header_delimiter: str,
        column_delimiter: str,
        row_delimiter: str,
        text_qualifier: str,
        skip_lines: int,
        trailer_line: Optional[str],
        timestamp: str
    ) -> str:
        """Create .fmt file with CSV format parameters"""
        
        fmt_filename = filename.replace('.csv', '.fmt')
        fmt_file_path = os.path.join(self.format_location, f"{timestamp}_{fmt_filename}")
        
        format_config = {
            "file_info": {
                "original_filename": filename,
                "upload_timestamp": datetime.now().isoformat(),
                "format_version": "1.0"
            },
            "csv_format": {
                "header_delimiter": header_delimiter,
                "column_delimiter": column_delimiter,
                "row_delimiter": row_delimiter,
                "text_qualifier": text_qualifier,
                "skip_lines": skip_lines,
                "has_header": True,  # Always true as per PRD requirements
                "has_trailer": bool(trailer_line and trailer_line.strip()),
                "trailer_line": trailer_line
            },
            "processing_options": {
                "encoding": "utf-8",
                "skip_blank_lines": True,
                "strip_whitespace": True
            }
        }
        
        async with aiofiles.open(fmt_file_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(format_config, indent=2))
        
        return fmt_file_path
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to remove potentially dangerous characters"""
        # Remove path components
        filename = os.path.basename(filename)
        
        # Replace unsafe characters
        unsafe_chars = ['<', '>', ':', '"', '/', '\\', '|', '?', '*']
        for char in unsafe_chars:
            filename = filename.replace(char, '_')
        
        return filename
    
    async def read_format_file(self, fmt_file_path: str) -> Dict[str, Any]:
        """Read and parse format configuration file"""
        try:
            async with aiofiles.open(fmt_file_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                return json.loads(content)
        except Exception as e:
            raise Exception(f"Failed to read format file {fmt_file_path}: {str(e)}")
    
    def move_to_archive(self, source_file_path: str, original_filename: str) -> str:
        """Move processed file to archive with timestamp"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = os.path.splitext(original_filename)[0]
            extension = os.path.splitext(original_filename)[1]
            
            archived_filename = f"{base_name}_{timestamp}{extension}"
            archive_path = os.path.join(self.archive_location, archived_filename)
            
            shutil.move(source_file_path, archive_path)
            return archive_path
            
        except Exception as e:
            raise Exception(f"Failed to archive file {source_file_path}: {str(e)}")
    
    def extract_table_base_name(self, filename: str) -> str:
        """
        Extract table base name from filename, removing timestamp patterns:
        - abc.yyyyMMdd.csv -> abc
        - abc_yyyyMMdd.csv -> abc
        - abc.yyyyMMddHHmmss.csv -> abc
        - abc.yyyyMMdd.HHmmss.csv -> abc
        - abc_yyyyMMdd_HHmmss.csv -> abc
        - abc_yyyyMMddHHmmss.csv -> abc
        - abc.csv -> abc
        """
        try:
            import re
            
            # Remove extension
            base_name = os.path.splitext(filename)[0]
            
            # Define timestamp patterns to remove
            timestamp_patterns = [
                r'\.\d{8}\.\d{6}$',      # .yyyyMMdd.HHmmss
                r'_\d{8}_\d{6}$',        # _yyyyMMdd_HHmmss
                r'\.\d{14}$',            # .yyyyMMddHHmmss
                r'_\d{14}$',             # _yyyyMMddHHmmss
                r'\.\d{8}$',             # .yyyyMMdd
                r'_\d{8}$',              # _yyyyMMdd
            ]
            
            # Remove timestamp patterns
            table_base_name = base_name
            for pattern in timestamp_patterns:
                table_base_name = re.sub(pattern, '', table_base_name)
            
            # If no patterns matched, use the first part before any separator
            if table_base_name == base_name:
                # Split by dots and underscores, take first non-empty part
                parts = re.split(r'[._]', base_name)
                table_base_name = parts[0] if parts and parts[0] else base_name
            
            # Sanitize table name (only allow alphanumeric and underscore)
            sanitized_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in table_base_name)
            
            # Ensure it starts with a letter or underscore
            if sanitized_name and not (sanitized_name[0].isalpha() or sanitized_name[0] == '_'):
                sanitized_name = f"t_{sanitized_name}"
            
            return sanitized_name or "unknown_table"
            
        except Exception as e:
            raise Exception(f"Failed to extract table name from {filename}: {str(e)}")
    
    def get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get file information including size, modification time, etc."""
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            stat = os.stat(file_path)
            
            return {
                "file_path": file_path,
                "filename": os.path.basename(file_path),
                "size_bytes": stat.st_size,
                "size_mb": round(stat.st_size / (1024 * 1024), 2),
                "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "created_time": datetime.fromtimestamp(stat.st_ctime).isoformat()
            }
            
        except Exception as e:
            raise Exception(f"Failed to get file info for {file_path}: {str(e)}")
    
    def validate_csv_file(self, file_path: str, max_size_bytes: int = None) -> Dict[str, Any]:
        """Validate CSV file before processing"""
        try:
            if max_size_bytes is None:
                max_size_bytes = int(os.getenv("max_upload_size", "20971520"))
            
            file_info = self.get_file_info(file_path)
            
            validation_result = {
                "valid": True,
                "errors": [],
                "warnings": [],
                "file_info": file_info
            }
            
            # Check file size
            if file_info["size_bytes"] > max_size_bytes:
                validation_result["valid"] = False
                validation_result["errors"].append(
                    f"File size ({file_info['size_mb']} MB) exceeds maximum limit "
                    f"({max_size_bytes / (1024*1024)} MB)"
                )
            
            # Check if file is actually a CSV
            if not file_info["filename"].lower().endswith('.csv'):
                validation_result["valid"] = False
                validation_result["errors"].append("File must have .csv extension")
            
            return validation_result
            
        except Exception as e:
            return {
                "valid": False,
                "errors": [f"Validation failed: {str(e)}"],
                "warnings": [],
                "file_info": None
            }