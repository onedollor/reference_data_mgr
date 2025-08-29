#!/usr/bin/env python3
"""
Direct test of CSV processing pipeline without logger dependencies
"""
import asyncio
import json
from utils.ingest import DataIngester

class MockLogger:
    """Mock logger that doesn't require database"""
    async def log_info(self, category, message):
        print(f"[{category}] {message}")
    
    async def log_error(self, category, message):
        print(f"[ERROR {category}] {message}")

class MockDatabaseManager:
    """Mock database manager"""
    pass

async def test_pipeline():
    """Test the complete pipeline with proper logger"""
    
    # Create mock ingester with proper constructor
    ingester = DataIngester(
        db_manager=MockDatabaseManager(),
        logger=MockLogger()
    )
    
    print("Testing CSV pipeline with different delimiters...")
    
    test_files = [
        'final_semicolon_test.csv',
        'final_pipe_test.csv', 
        'final_tab_test.csv'
    ]
    
    for file_name in test_files:
        print(f"\n=== Testing {file_name} ===")
        
        # Read format file
        format_file = file_name.replace('.csv', '.fmt')
        with open(format_file, 'r') as f:
            format_config = json.load(f)
        
        delimiter = format_config['csv_format']['column_delimiter']
        print(f"Expected delimiter: '{delimiter}'")
        
        try:
            # Call _read_csv_file directly
            df = await ingester._read_csv_file(file_name, format_config['csv_format'])
            print(f"Pipeline result: {len(df)} rows, {len(df.columns)} columns")
            print(f"Columns: {list(df.columns)}")
            if len(df) > 0:
                print(f"First row: {df.iloc[0].to_dict()}")
        except Exception as e:
            print(f"Pipeline error: {e}")

if __name__ == "__main__":
    asyncio.run(test_pipeline())