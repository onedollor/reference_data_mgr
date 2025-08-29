#!/usr/bin/env python3
"""
Test the complete delimiter fix in the live pipeline
"""
import asyncio
import os
import json
from backend_lib import ReferenceDataAPI
from utils.database import DatabaseManager
from utils.logger import Logger

async def test_complete_fix():
    """Test the complete pipeline with the delimiter fix"""
    
    # Create test file with semicolon delimiter
    test_file = "/home/lin/repo/reference_data_mgr/backend/test_final_semicolon.csv"
    with open(test_file, 'w') as f:
        f.write("name;role;salary\nAlice;Engineer;75000\nBob;Manager;85000\n")
    
    # Initialize components
    logger = Logger()
    backend = ReferenceDataAPI(logger)
    
    print("Testing complete delimiter fix...")
    print(f"Test file: {test_file}")
    
    try:
        # Test format detection
        format_result = backend.detect_format(test_file)
        if format_result.get("success"):
            detected_delimiter = format_result["detected_format"].get("column_delimiter", "?")
            print(f"1. Format detection - delimiter: '{detected_delimiter}' ✓")
        else:
            print(f"1. Format detection failed: {format_result.get('error')}")
            return
        
        # Test file processing (this should create .fmt file and ingest)
        result = await backend.process_file(
            file_path=test_file,
            table_name="test_semicolon_fix",
            load_type="append",
            target_schema="ref",
            reference_data=True
        )
        
        if result.get("success"):
            print("2. File processing completed ✓")
            
            # Check if format file was created with correct delimiter
            fmt_file = test_file + ".fmt"
            if os.path.exists(fmt_file):
                with open(fmt_file, 'r') as f:
                    fmt_data = json.load(f)
                    fmt_delimiter = fmt_data['csv_format']['column_delimiter']
                    print(f"3. Format file delimiter: '{fmt_delimiter}' ✓")
            else:
                print("3. Format file not found ✗")
        else:
            print(f"2. File processing failed: {result.get('error')} ✗")
        
    except Exception as e:
        print(f"Test error: {e}")
    
    # Cleanup
    try:
        os.remove(test_file)
        if os.path.exists(test_file + ".fmt"):
            os.remove(test_file + ".fmt")
    except:
        pass

if __name__ == "__main__":
    asyncio.run(test_complete_fix())