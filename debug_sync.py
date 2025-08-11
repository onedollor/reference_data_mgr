#!/usr/bin/env python3
"""
Debug script to understand why sync_table_schema isn't widening varchar columns
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from utils.database import DatabaseManager
import re

def debug_varchar_matching():
    """Debug the varchar matching logic"""
    print("Debugging varchar column matching logic...")
    
    db_manager = DatabaseManager()
    connection = db_manager.get_connection()
    
    try:
        # Create test table
        test_table = "debug_varchar_sync"
        cursor = connection.cursor()
        
        # Clean up if exists
        cursor.execute(f"DROP TABLE IF EXISTS [{db_manager.data_schema}].[{test_table}]")
        
        # Create table with small varchar
        cursor.execute(f"""
            CREATE TABLE [{db_manager.data_schema}].[{test_table}] (
                id int,
                keywords varchar(100),
                description varchar(200)
            )
        """)
        connection.commit()
        
        # Get existing columns
        existing_cols = {c['name'].lower(): c for c in db_manager.get_table_columns(connection, test_table)}
        print("\nExisting columns:")
        for name, col in existing_cols.items():
            print(f"  {name}: {col}")
        
        # Test new columns
        new_columns = [
            {'name': 'keywords', 'data_type': 'varchar(8000)'},
            {'name': 'description', 'data_type': 'varchar(MAX)'}
        ]
        
        print("\nTesting varchar matching logic for each column:")
        for col in new_columns:
            name = col['name']
            target_type = col['data_type']
            lower = name.lower()
            
            print(f"\n--- Processing column: {name} ---")
            print(f"Target type: {target_type}")
            
            if lower not in existing_cols:
                print("Column not found in existing schema")
                continue
                
            existing = existing_cols[lower]
            existing_type = existing['data_type']
            print(f"Existing type: {existing_type}")
            
            # Debug the regex matching
            import re as _re
            
            # Handle varchar(MAX) for new type
            is_new_varchar_max = target_type.lower() == 'varchar(max)'
            print(f"is_new_varchar_max: {is_new_varchar_max}")
            
            m_new = _re.fullmatch(r'varchar\((\d+)\)', target_type, flags=_re.IGNORECASE) if not is_new_varchar_max else None
            print(f"m_new match: {m_new}")
            
            new_len = int(m_new.group(1)) if m_new else (None if not is_new_varchar_max else 'MAX')
            print(f"new_len: {new_len}")
            
            # Handle varchar(MAX) for existing type  
            is_old_varchar_max = (existing_type or '').lower() == 'varchar(max)'
            print(f"is_old_varchar_max: {is_old_varchar_max}")
            
            m_old = _re.fullmatch(r'varchar\((\d+)\)', existing_type or '', flags=_re.IGNORECASE) if not is_old_varchar_max else None
            print(f"m_old match: {m_old}")
            
            old_len = int(m_old.group(1)) if m_old else (None if not is_old_varchar_max else 'MAX')
            print(f"old_len: {old_len}")
            
            # Determine if widening is needed
            need_widen = False
            if old_len is not None and new_len is not None:
                print(f"Both lengths available: old={old_len}, new={new_len}")
                if new_len == 'MAX' and old_len != 'MAX':
                    print("Should widen to MAX")
                    need_widen = True
                elif old_len != 'MAX' and new_len != 'MAX' and isinstance(new_len, int) and isinstance(old_len, int) and new_len > old_len:
                    print("Should widen numeric length")
                    need_widen = True
                else:
                    print("No widening needed")
            else:
                print("Cannot determine widening (one or both lengths are None)")
            
            print(f"need_widen: {need_widen}")
        
        # Clean up
        cursor.execute(f"DROP TABLE [{db_manager.data_schema}].[{test_table}]")
        connection.commit()
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        connection.close()

if __name__ == "__main__":
    debug_varchar_matching()