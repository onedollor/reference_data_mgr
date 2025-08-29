#!/usr/bin/env python3
"""
Direct test of CSV delimiter processing without logger dependencies
"""
import pandas as pd
import json

def test_csv_with_delimiter(file_path, delimiter):
    """Test CSV reading with specific delimiter"""
    print(f"\nTesting {file_path} with delimiter '{delimiter}':")
    
    # Read format file to verify what was detected
    format_file = file_path.replace('.csv', '.fmt')
    try:
        with open(format_file, 'r') as f:
            format_data = json.load(f)
            detected_delimiter = format_data['csv_format']['column_delimiter']
            print(f"  Format file delimiter: '{detected_delimiter}'")
    except FileNotFoundError:
        print(f"  Warning: Format file {format_file} not found")
    
    # Test pandas directly with the delimiter
    pandas_kwargs = {
        'sep': delimiter,
        'dtype': str,
        'keep_default_na': False,
        'na_values': [],
        'encoding': 'utf-8'
    }
    
    try:
        df = pd.read_csv(file_path, **pandas_kwargs)
        print(f"  Pandas result: {len(df)} rows, {len(df.columns)} columns")
        print(f"  Columns: {list(df.columns)}")
        if len(df) > 0:
            print(f"  First row: {df.iloc[0].to_dict()}")
    except Exception as e:
        print(f"  Pandas error: {e}")

# Test the three problematic files
test_files = [
    ('final_semicolon_test.csv', ';'),
    ('final_pipe_test.csv', '|'),
    ('final_tab_test.csv', '\t')
]

for file_name, delimiter in test_files:
    test_csv_with_delimiter(file_name, delimiter)