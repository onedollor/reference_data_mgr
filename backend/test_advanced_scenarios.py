#!/usr/bin/env python3
"""
Test Cases 31-60: Advanced scenarios for file monitoring
"""
import os
import asyncio
import json
from pathlib import Path

async def test_advanced_scenarios():
    """Create advanced test scenarios"""
    
    base_path = "/home/lin/repo/reference_data_mgr/backend/dropoff"
    
    advanced_cases = [
        # Test Case 31-35: Different file sizes
        {
            "name": "TC31: Large CSV file",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "large_dataset.csv",
            "content": "id,name,category\n" + "\n".join([f"{i},Item{i},Cat{i%5}" for i in range(1, 1001)])
        },
        {
            "name": "TC32: Empty CSV file", 
            "path": f"{base_path}/reference_data_table/append",
            "file": "empty.csv",
            "content": "id,name,value\n"
        },
        {
            "name": "TC33: Single row CSV",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "single_row.csv", 
            "content": "id,name,status\n1,Test,Active\n"
        },
        
        # Test Case 34-38: Special characters and encodings
        {
            "name": "TC34: Special characters in data",
            "path": f"{base_path}/none_reference_data_table/fullload",
            "file": "special_chars.csv",
            "content": "id,name,description\n1,\"Café & Restaurant\",\"High-end dining, $$$\"\n2,\"José's Store\",\"Groceries & more!\"\n"
        },
        {
            "name": "TC35: Quotes and commas in data",
            "path": f"{base_path}/reference_data_table/append",
            "file": "quoted_data.csv",
            "content": 'id,full_name,address\n1,"Smith, John",\"123 Main St, Apt 4B\"\n2,"Johnson, Jane",\"456 Oak Ave, Suite C\"\n'
        },
        
        # Test Case 36-40: Different delimiters in advanced scenarios
        {
            "name": "TC36: Tab-delimited with quotes",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "tab_quoted.csv",
            "content": 'product\tdescription\tprice\nWidget\t"High quality widget"\t15.99\nGadget\t"Premium gadget with features"\t29.99\n'
        },
        {
            "name": "TC37: Pipe-delimited with special chars",
            "path": f"{base_path}/none_reference_data_table/append",
            "file": "pipe_special.csv",
            "content": 'code|description|notes\nA001|Product A|"Contains special chars: @#$%"\nB002|Product B|"Multi-line\\ndescription"\n'
        },
        
        # Test Case 38-42: File naming patterns
        {
            "name": "TC38: Underscored filename",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "user_accounts_master.csv",
            "content": "user_id,username,email\n1001,jsmith,jsmith@example.com\n1002,mjohnson,mjohnson@example.com\n"
        },
        {
            "name": "TC39: Dated filename",
            "path": f"{base_path}/reference_data_table/append", 
            "file": "sales_20250829.csv",
            "content": "transaction_id,amount,customer\nT001,150.00,CUST001\nT002,75.50,CUST002\n"
        },
        {
            "name": "TC40: Versioned filename",
            "path": f"{base_path}/none_reference_data_table/fullload",
            "file": "config_v2.csv",
            "content": "key,value,version\nmax_connections,100,2\ntimeout,30,2\n"
        }
    ]
    
    print("\n=== Test Cases 31-42: Advanced Scenarios ===")
    
    # Create all advanced test files
    for i, test_case in enumerate(advanced_cases, 31):
        print(f"\nSetting up {test_case['name']}...")
        
        # Create directory structure
        os.makedirs(test_case['path'], exist_ok=True)
        
        # Write test file
        file_path = os.path.join(test_case['path'], test_case['file'])
        with open(file_path, 'w') as f:
            f.write(test_case['content'])
        
        print(f"  Created: {file_path}")
        
        # Create format file for CSV
        if test_case['file'].endswith('.csv'):
            # Detect delimiter
            delimiter = ','
            if '\t' in test_case['content']:
                delimiter = '\t'
            elif '|' in test_case['content'] and ';' not in test_case['content']:
                delimiter = '|'
            elif ';' in test_case['content']:
                delimiter = ';'
                
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
    
    print(f"\nAdvanced scenarios setup complete: {len(advanced_cases)} test cases prepared")
    return len(advanced_cases)

if __name__ == "__main__":
    count = asyncio.run(test_advanced_scenarios())
    print(f"\nAdvanced test setup complete: {count} test cases prepared")