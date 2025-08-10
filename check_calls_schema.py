#!/usr/bin/env python3
"""
Check the existing calls table schema to understand the datetime conversion error
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from utils.database import DatabaseManager

def check_calls_schema():
    """Check the schema of existing calls-related tables"""
    print("Checking existing calls table schemas...")
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
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
        # Check both main and stage tables
        tables_to_check = ['calls', 'calls_stage']
        
        for table_name in tables_to_check:
            print(f"\n--- Checking table: {table_name} ---")
            
            if not db_manager.table_exists(connection, table_name):
                print(f"Table {table_name} does not exist")
                continue
                
            # Get table schema
            columns = db_manager.get_table_columns(connection, table_name)
            print(f"Table {table_name} schema:")
            
            datetime_columns = []
            for col in columns:
                max_len_display = col['max_length'] if col['max_length'] != -1 else 'MAX' if col['max_length'] == -1 else ''
                data_type_display = f"{col['data_type']}" + (f"({max_len_display})" if max_len_display else "")
                print(f"  - {col['name']}: {data_type_display} (nullable: {col['nullable']})")
                
                # Track datetime columns
                if col['data_type'].lower() in ['datetime', 'datetime2', 'date', 'time']:
                    datetime_columns.append(col['name'])
            
            if datetime_columns:
                print(f"  >>> DATETIME COLUMNS FOUND: {datetime_columns}")
            else:
                print(f"  >>> No datetime columns found")
                
        return True
        
    except Exception as e:
        print(f"ERROR during schema check: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        connection.close()

if __name__ == "__main__":
    result = check_calls_schema()
    if result:
        print("\n✓ Schema check completed successfully")
    else:
        print("\n❌ Schema check failed")
        sys.exit(1)