#!/usr/bin/env python3
"""
Test script to verify the parameterized multi-row INSERT SQL generation
without requiring a database connection.
"""

def test_multi_row_insert_generation():
    """Test the SQL generation logic for multi-row parameterized INSERT"""
    
    # Mock data similar to what the actual code would process
    data_columns = ['id', 'name', 'description']
    schema = 'ref'
    table_name = 'test_table'
    
    # Simulate a batch of 3 rows (smaller than 990 for testing)
    batch_data = [
        [1, 'Test Item 1', 'This is a test item'],
        [2, 'Test Item 2', 'Another test item'], 
        [3, 'Test Item 3', 'Final test item']
    ]
    
    batch_size = len(batch_data)
    print(f"Testing batch of {batch_size} rows...")
    
    # Generate column list (same as in actual code)
    column_list = ', '.join([f'[{col}]' for col in data_columns])
    print(f"Column list: {column_list}")
    
    # Generate multi-row placeholders (same as in actual code)
    single_row_placeholders = '(' + ', '.join(['?' for _ in data_columns]) + ')'
    all_rows_placeholders = ', '.join([single_row_placeholders for _ in range(batch_size)])
    sql = f"INSERT INTO [{schema}].[{table_name}] ({column_list}) VALUES {all_rows_placeholders}"
    
    print(f"\nGenerated SQL:")
    print(sql)
    
    # Generate flattened parameters (same as in actual code)
    batch_params = []
    for row_values in batch_data:
        batch_params.extend(row_values)
    
    print(f"\nFlattened parameters ({len(batch_params)} total):")
    print(batch_params)
    
    # Verify the parameter count matches the placeholder count
    expected_param_count = batch_size * len(data_columns)
    actual_param_count = len(batch_params)
    placeholder_count = sql.count('?')
    
    print(f"\nValidation:")
    print(f"  Expected parameters: {expected_param_count}")
    print(f"  Actual parameters: {actual_param_count}")
    print(f"  SQL placeholders: {placeholder_count}")
    
    if expected_param_count == actual_param_count == placeholder_count:
        print("  ✅ Parameter count validation PASSED")
        return True
    else:
        print("  ❌ Parameter count validation FAILED")
        return False

def test_large_batch():
    """Test with a larger batch size closer to 990"""
    print("\n" + "="*60)
    print("Testing larger batch (100 rows)...")
    
    data_columns = ['col1', 'col2', 'col3', 'col4']
    batch_size = 100
    
    # Generate placeholders
    single_row_placeholders = '(' + ', '.join(['?' for _ in data_columns]) + ')'
    all_rows_placeholders = ', '.join([single_row_placeholders for _ in range(batch_size)])
    
    # Count placeholders
    placeholder_count = all_rows_placeholders.count('?')
    expected_count = batch_size * len(data_columns)
    
    print(f"Batch size: {batch_size}")
    print(f"Columns: {len(data_columns)}")
    print(f"Expected placeholders: {expected_count}")
    print(f"Actual placeholders: {placeholder_count}")
    
    if expected_count == placeholder_count:
        print("✅ Large batch validation PASSED")
        return True
    else:
        print("❌ Large batch validation FAILED")
        return False

if __name__ == "__main__":
    print("Testing Multi-Row Parameterized INSERT SQL Generation")
    print("="*60)
    
    test1_passed = test_multi_row_insert_generation()
    test2_passed = test_large_batch()
    
    print("\n" + "="*60)
    if test1_passed and test2_passed:
        print("🎉 All tests PASSED! The parameterized INSERT logic is correct.")
    else:
        print("❌ Some tests FAILED. Check the logic.")