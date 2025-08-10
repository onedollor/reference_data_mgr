#!/usr/bin/env python3
"""
Test script to verify the schema synchronization fix for varchar truncation
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from utils.database import DatabaseManager
from utils.logger import Logger
import asyncio

async def test_schema_sync():
    """Test the schema synchronization functionality"""
    print("Testing schema synchronization for varchar truncation fix...")
    
    # Initialize components
    db_manager = DatabaseManager()
    logger = Logger()
    
    # Test database connectivity
    print("Testing database connection...")
    connection_result = db_manager.test_connection()
    if connection_result["status"] != "success":
        print(f"ERROR: Database connection failed: {connection_result['error']}")
        return False
    
    print(f"✓ Database connected: {connection_result['server_version']}")
    
    # Get a connection for testing
    connection = db_manager.get_connection()
    
    try:
        # Ensure schemas exist
        db_manager.ensure_schemas_exist(connection)
        
        # Test table name
        test_table = "test_varchar_sync"
        
        # Create a test table with small varchar
        print(f"\nCreating test table '{test_table}' with small varchar(100)...")
        test_columns_small = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'keywords', 'data_type': 'varchar(100)'},
            {'name': 'description', 'data_type': 'varchar(200)'}
        ]
        
        db_manager.create_table(connection, test_table, test_columns_small, add_metadata_columns=False)
        print("✓ Test table created with small varchar sizes")
        
        # Check current table schema
        current_columns = db_manager.get_table_columns(connection, test_table)
        print(f"Current schema:")
        for col in current_columns:
            print(f"  - {col['name']}: {col['data_type']}({col['max_length']})")
        
        # Now test syncing with larger varchar columns
        print(f"\nTesting sync with larger varchar sizes...")
        test_columns_large = [
            {'name': 'id', 'data_type': 'int'},
            {'name': 'keywords', 'data_type': 'varchar(8000)'},  # Widen from 100 to 8000
            {'name': 'description', 'data_type': 'varchar(MAX)'},  # Widen from 200 to MAX
            {'name': 'new_field', 'data_type': 'varchar(500)'}  # New column
        ]
        
        sync_actions = db_manager.sync_table_schema(connection, test_table, test_columns_large)
        print(f"Schema sync completed:")
        print(f"  - Added columns: {len(sync_actions['added'])}")
        for added in sync_actions['added']:
            print(f"    + {added['column']}: {added['data_type']}")
        print(f"  - Widened columns: {len(sync_actions['widened'])}")
        for widened in sync_actions['widened']:
            print(f"    ~ {widened['column']}: {widened['from']} -> {widened['to']}")
        print(f"  - Skipped columns: {len(sync_actions['skipped'])}")
        
        # Verify the updated schema
        updated_columns = db_manager.get_table_columns(connection, test_table)
        print(f"\nUpdated schema:")
        for col in updated_columns:
            max_len_display = col['max_length'] if col['max_length'] != -1 else 'MAX'
            print(f"  - {col['name']}: {col['data_type']}({max_len_display})")
        
        # Test inserting a long value
        print(f"\nTesting insert with long varchar values...")
        cursor = connection.cursor()
        long_keywords = "a" * 1000  # 1000 character string
        long_description = "b" * 5000  # 5000 character string
        
        cursor.execute(f"""
            INSERT INTO [{db_manager.data_schema}].[{test_table}] 
            (id, keywords, description, new_field) 
            VALUES (1, '{long_keywords}', '{long_description}', 'test')
        """)
        connection.commit()
        print("✓ Successfully inserted row with long varchar values")
        
        # Verify the data was inserted correctly
        cursor.execute(f"SELECT id, LEN(keywords), LEN(description), new_field FROM [{db_manager.data_schema}].[{test_table}]")
        result = cursor.fetchone()
        print(f"✓ Inserted data verified: id={result[0]}, keywords_len={result[1]}, description_len={result[2]}, new_field='{result[3]}'")
        
        # Clean up
        cursor.execute(f"DROP TABLE [{db_manager.data_schema}].[{test_table}]")
        print(f"✓ Test table cleaned up")
        
        return True
        
    except Exception as e:
        print(f"ERROR during testing: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        connection.close()

if __name__ == "__main__":
    result = asyncio.run(test_schema_sync())
    if result:
        print("\n🎉 Schema synchronization test PASSED!")
        print("The varchar truncation fix is working correctly.")
    else:
        print("\n❌ Schema synchronization test FAILED!")
        sys.exit(1)