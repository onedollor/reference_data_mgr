#!/usr/bin/env python3
"""
Test Cases 16-30: Folder structure and file monitoring tests
"""
import os
import asyncio
import shutil
import json
from pathlib import Path
from backend_lib import ReferenceDataAPI

async def test_folder_structure():
    """Test the file monitoring with proper folder structure"""
    
    # Test base paths
    base_path = "/home/lin/repo/reference_data_mgr/backend/dropoff"
    
    test_cases = [
        # Test Case 16-18: Reference data table structure
        {
            "name": "TC16: Reference data fullload",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "employees.csv",
            "content": "id,name,dept\n1,John,IT\n2,Jane,HR\n"
        },
        {
            "name": "TC17: Reference data append", 
            "path": f"{base_path}/reference_data_table/append",
            "file": "products.csv",
            "content": "sku,name,price\nP001,Widget,10.99\nP002,Gadget,25.50\n"
        },
        {
            "name": "TC18: Reference data nested folder",
            "path": f"{base_path}/reference_data_table/fullload/daily",
            "file": "reports.csv", 
            "content": "date,sales,profit\n2025-08-29,1000,200\n2025-08-28,800,150\n"
        },
        
        # Test Case 19-21: Non-reference data structure
        {
            "name": "TC19: Non-reference data fullload",
            "path": f"{base_path}/none_reference_data_table/fullload", 
            "file": "logs.csv",
            "content": "timestamp,event,user\n2025-08-29T10:00:00,login,user1\n2025-08-29T11:00:00,logout,user1\n"
        },
        {
            "name": "TC20: Non-reference data append",
            "path": f"{base_path}/none_reference_data_table/append",
            "file": "metrics.csv",
            "content": "metric,value,unit\ncpu_usage,75,percent\nmemory_usage,8,gb\n"
        },
        {
            "name": "TC21: Multiple delimiters in folder structure",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "mixed_formats.csv",
            "content": "type;format;status\nCSV;semicolon;active\nTSV;tab;pending\n"
        },
        
        # Test Case 22-25: Edge cases
        {
            "name": "TC22: Deep nested folder",
            "path": f"{base_path}/reference_data_table/fullload/2025/08/29",
            "file": "daily_data.csv",
            "content": "hour,visitors,revenue\n10,45,150.00\n11,52,175.50\n"
        },
        {
            "name": "TC23: Invalid folder path",
            "path": f"{base_path}/invalid_data_table/fullload",
            "file": "should_not_process.csv",
            "content": "id,data\n1,test\n"
        },
        {
            "name": "TC24: No load type folder",
            "path": f"{base_path}/reference_data_table",
            "file": "root_level.csv", 
            "content": "id,name\n1,test\n"
        },
        {
            "name": "TC25: Wrong load type",
            "path": f"{base_path}/reference_data_table/invalidload",
            "file": "wrong_type.csv",
            "content": "id,value\n1,data\n"
        }
    ]
    
    print("=== Test Cases 16-30: Folder Structure Tests ===")
    print(f"Base monitoring path: {base_path}")
    
    # Create all test directories and files
    for i, test_case in enumerate(test_cases, 16):
        print(f"\nSetting up {test_case['name']}...")
        
        # Create directory structure
        os.makedirs(test_case['path'], exist_ok=True)
        
        # Write test file
        file_path = os.path.join(test_case['path'], test_case['file'])
        with open(file_path, 'w') as f:
            f.write(test_case['content'])
        
        print(f"  Created: {file_path}")
        
        # For CSV files, also create format file
        if test_case['file'].endswith('.csv'):
            # Detect delimiter from content
            delimiter = ','
            if ';' in test_case['content']:
                delimiter = ';'
            elif '\t' in test_case['content']:
                delimiter = '\t'
                
            format_config = {
                "file_info": {
                    "original_filename": test_case['file'],
                    "format_version": "1.0"
                },
                "csv_format": {
                    "header_delimiter": delimiter,
                    "column_delimiter": delimiter,
                    "row_delimiter": "\\n",
                    "text_qualifier": "\"",
                    "skip_lines": 0,
                    "has_header": True,
                    "has_trailer": False,
                    "trailer_line": None
                }
            }
            
            format_path = file_path.replace('.csv', '.fmt')
            with open(format_path, 'w') as f:
                json.dump(format_config, f, indent=2)
            print(f"  Created: {format_path}")
    
    print(f"\nAll test files created. Folder structure ready for monitoring test.")
    print("You can now run the file monitor to process these files.")
    return len(test_cases)

if __name__ == "__main__":
    count = asyncio.run(test_folder_structure()) 
    print(f"\nFolder structure test setup complete: {count} test cases prepared")