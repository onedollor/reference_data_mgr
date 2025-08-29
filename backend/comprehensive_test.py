#!/usr/bin/env python3
"""
Comprehensive test suite for file monitoring system
Tests all scenarios including edge cases and error conditions
"""
import os
import json
import time
from pathlib import Path

def create_comprehensive_tests():
    """Create comprehensive test files for monitoring"""
    
    base_path = "/home/lin/repo/reference_data_mgr/data/reference_data/dropoff"
    
    # Comprehensive test scenarios
    test_scenarios = [
        # Test Cases 43-50: Error handling
        {
            "name": "TC43: Invalid CSV format",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "invalid_format.csv",
            "content": "invalid\nformat\nno,proper,structure"
        },
        {
            "name": "TC44: Mixed delimiters in same file", 
            "path": f"{base_path}/reference_data_table/append",
            "file": "mixed_delimiters.csv",
            "content": "id,name;department|location\n1,John;IT|NYC\n2,Jane;HR|LA"
        },
        {
            "name": "TC45: Very long filename",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "extremely_long_filename_with_many_words_and_underscores_that_tests_filename_handling_limits.csv",
            "content": "id,description,notes\n1,Test,Long filename test\n"
        },
        {
            "name": "TC46: Unicode content",
            "path": f"{base_path}/none_reference_data_table/fullload", 
            "file": "unicode_test.csv",
            "content": "id,name,location\n1,José García,São Paulo\n2,李明,北京\n3,محمد علي,القاهرة\n"
        },
        
        # Test Cases 47-52: Performance and scalability
        {
            "name": "TC47: Very large file (10K rows)",
            "path": f"{base_path}/reference_data_table/append",
            "file": "performance_test.csv", 
            "content": "id,value,category\n" + "\n".join([f"{i},Value{i},Cat{i%10}" for i in range(1, 10001)])
        },
        {
            "name": "TC48: Multiple simultaneous files",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "batch_file_1.csv",
            "content": "batch,file,number\n1,first,batch"
        },
        {
            "name": "TC49: Multiple simultaneous files",
            "path": f"{base_path}/reference_data_table/fullload", 
            "file": "batch_file_2.csv",
            "content": "batch;file;number\n2;second;batch"
        },
        {
            "name": "TC50: Multiple simultaneous files",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "batch_file_3.csv",
            "content": "batch\tfile\tnumber\n3\tthird\tbatch"
        },
        
        # Test Cases 51-55: Complex data patterns
        {
            "name": "TC51: Embedded newlines in quoted fields",
            "path": f"{base_path}/reference_data_table/append",
            "file": "multiline_data.csv", 
            "content": 'id,description,notes\n1,"Line 1\nLine 2\nLine 3","Multi-line description"\n2,"Another\nmulti-line","More notes"'
        },
        {
            "name": "TC52: Complex quoting scenarios",
            "path": f"{base_path}/none_reference_data_table/fullload",
            "file": "complex_quotes.csv",
            "content": 'id,text,escaped\n1,"He said ""Hello""","Escaped quotes"\n2,"Text with, comma","Normal text"'
        },
        
        # Test Cases 56-60: Integration scenarios  
        {
            "name": "TC56: Reference data with trailer",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "with_trailer.csv",
            "content": "product,price,category\nWidget,25.99,Electronics\nGadget,45.50,Tech\nTotal Records: 2"
        },
        {
            "name": "TC57: Non-reference data with unusual structure",
            "path": f"{base_path}/none_reference_data_table/append", 
            "file": "log_format.csv",
            "content": "timestamp|level|message|thread\n2025-08-29T10:00:00|INFO|Application started|main\n2025-08-29T10:01:00|DEBUG|Processing request|worker-1"
        },
        {
            "name": "TC58: Production-like data volume",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "production_dataset.csv",
            "content": "customer_id,first_name,last_name,email,registration_date\n" + 
                      "\n".join([f"{1000+i},User{i},Lastname{i},user{i}@example.com,2025-08-{(i%28)+1:02d}" for i in range(1, 2501)])
        },
        {
            "name": "TC59: Mixed reference and non-reference processing",
            "path": f"{base_path}/none_reference_data_table/fullload",
            "file": "event_log.csv", 
            "content": "event_id;timestamp;event_type;user_id;details\n1001;2025-08-29T10:00:00;login;user123;successful\n1002;2025-08-29T10:05:00;logout;user123;session_end"
        },
        {
            "name": "TC60: Final comprehensive validation",
            "path": f"{base_path}/reference_data_table/append",
            "file": "final_validation.csv",
            "content": "validation_id\ttest_type\tresult\tstatus\n1\tdelimiter_detection\tpassed\tvalid\n2\tformat_propagation\tpassed\tvalid\n3\tdata_processing\tpassed\tvalid"
        }
    ]
    
    print("=== Creating Comprehensive Test Suite (Test Cases 43-60) ===")
    
    created_files = []
    for i, test_case in enumerate(test_scenarios, 43):
        print(f"\nSetting up {test_case['name']}...")
        
        # Create directory structure
        os.makedirs(test_case['path'], exist_ok=True)
        
        # Write test file  
        file_path = os.path.join(test_case['path'], test_case['file'])
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(test_case['content'])
        
        created_files.append(file_path)
        print(f"  Created: {file_path}")
        
        # Auto-detect delimiter for format file creation
        content = test_case['content']
        delimiter = ','  # default
        if '\t' in content and content.count('\t') > content.count(',') and content.count('\t') > content.count(';'):
            delimiter = '\t'
        elif ';' in content and content.count(';') > content.count(','):
            delimiter = ';'
        elif '|' in content and content.count('|') > content.count(','):
            delimiter = '|'
            
        # Create format file
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
        print(f"  Created: {format_path} (delimiter: '{delimiter}')")
        
    print(f"\n=== Comprehensive Test Setup Complete ===")
    print(f"Created {len(test_scenarios)} test cases in monitoring folders")
    print(f"Files ready for processing by file monitor")
    
    return created_files

if __name__ == "__main__":
    files = create_comprehensive_tests()
    print(f"\nTest files created: {len(files)}")
    print("\nRun 'python3 file_monitor.py' to process all test cases")