#!/usr/bin/env python3
"""
Final comprehensive validation test
"""
import os
import json
import time
from pathlib import Path

def create_final_validation_tests():
    """Create final validation test files"""
    
    base_path = "/home/lin/repo/reference_data_mgr/data/reference_data/dropoff"
    
    # Final comprehensive validation tests
    final_tests = [
        {
            "name": "Final Validation: All Delimiters Working",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "validation_all_delimiters.csv",
            "content": "test_type,delimiter,status,result\ncomma,comma,pass,3_columns\nsemicolon,semicolon,pass,3_columns\ntab,tab,pass,3_columns\npipe,pipe,pass,3_columns"
        },
        {
            "name": "Final Validation: Semicolon Test",
            "path": f"{base_path}/reference_data_table/append", 
            "file": "final_semicolon_validation.csv",
            "content": "id;name;department;salary\n1001;Alice Johnson;Engineering;75000\n1002;Bob Smith;Marketing;65000\n1003;Carol Davis;HR;60000"
        },
        {
            "name": "Final Validation: Pipe Test",
            "path": f"{base_path}/none_reference_data_table/fullload",
            "file": "final_pipe_validation.csv", 
            "content": "event_id|timestamp|level|message\n1001|2025-08-29T11:00:00|INFO|System started\n1002|2025-08-29T11:01:00|DEBUG|Process initialized\n1003|2025-08-29T11:02:00|WARN|Low memory warning"
        },
        {
            "name": "Final Validation: Tab Test",
            "path": f"{base_path}/reference_data_table/fullload",
            "file": "final_tab_validation.csv",
            "content": "product_id\tname\tcategory\tprice\tupc\nP001\tWidget Pro\tElectronics\t29.99\t123456789012\nP002\tGadget Max\tTech\t49.99\t234567890123\nP003\tDevice Plus\tHardware\t79.99\t345678901234"
        },
        {
            "name": "Final Validation: Unicode and Special Characters",
            "path": f"{base_path}/none_reference_data_table/append",
            "file": "final_unicode_validation.csv",
            "content": "customer_id,name,location,notes\n1,José García,São Paulo,\"Customer from Brazil with ñ and ç\"\n2,李明华,北京,\"Chinese customer 中文测试\"\n3,محمد العلي,الرياض,\"Arabic customer العربية\"\n4,François Müller,Zürich,\"European customer with ü and é\""
        }
    ]
    
    print("=== Creating Final Validation Test Suite ===")
    
    created_files = []
    for i, test in enumerate(final_tests, 1):
        print(f"\nSetting up {test['name']}...")
        
        # Create directory structure
        os.makedirs(test['path'], exist_ok=True)
        
        # Write test file
        file_path = os.path.join(test['path'], test['file'])
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(test['content'])
        
        created_files.append(file_path)
        print(f"  Created: {file_path}")
        
        # Auto-detect delimiter
        content = test['content']
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
                "original_filename": test['file'],
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
    
    print(f"\n=== Final Validation Test Setup Complete ===")
    print(f"Created {len(final_tests)} final validation test cases")
    
    return created_files

if __name__ == "__main__":
    files = create_final_validation_tests()
    print(f"\nFinal validation files created: {len(files)}")
    print("\nReady for final comprehensive testing!")