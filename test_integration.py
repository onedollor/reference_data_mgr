#!/usr/bin/env python3
"""
Test script to verify backend API integration
"""

import sys
import os
from pathlib import Path

def test_backend_api():
    """Test the backend API library"""
    print("Testing Backend API Integration")
    print("=" * 40)
    
    try:
        from backend_lib import ReferenceDataAPI
        
        # Initialize API
        print("1. Initializing backend API...")
        api = ReferenceDataAPI()
        print("   ✓ Backend API initialized successfully")
        
        # Test health check
        print("2. Testing health check...")
        health = api.health_check()
        if health["success"]:
            print(f"   ✓ Health check passed: {health['status']}")
            print(f"   Database: {health.get('database', 'unknown')}")
        else:
            print(f"   ✗ Health check failed: {health.get('error', 'unknown error')}")
            
        # Test table listing
        print("3. Testing table listing...")
        tables_result = api.get_all_tables()
        if tables_result["success"]:
            tables = tables_result["tables"]
            print(f"   ✓ Found {len(tables)} tables")
            for table in tables[:3]:  # Show first 3 tables
                print(f"     - {table}")
        else:
            print(f"   ✗ Failed to list tables: {tables_result.get('error', 'unknown error')}")
        
        return True
        
    except Exception as e:
        print(f"   ✗ Backend API test failed: {e}")
        return False

def test_file_monitor():
    """Test file monitor initialization"""
    print("\nTesting File Monitor Integration")
    print("=" * 40)
    
    try:
        from file_monitor import FileMonitor
        
        print("1. Initializing file monitor...")
        monitor = FileMonitor()
        print("   ✓ File monitor initialized successfully")
        
        # Test API availability
        if monitor.api is not None:
            print("   ✓ Backend API available in file monitor")
            
            # Test format detection with a sample file
            sample_file_path = "/home/lin/repo/reference_data_mgr/test.csv"
            if os.path.exists(sample_file_path):
                print("2. Testing format detection...")
                delimiter, headers, samples = monitor.detect_csv_format(sample_file_path)
                print(f"   ✓ Detected delimiter: '{delimiter}'")
                print(f"   ✓ Detected headers: {headers}")
                
                print("3. Testing table name extraction...")
                table_name = monitor.extract_table_name(sample_file_path)
                print(f"   ✓ Extracted table name: '{table_name}'")
            else:
                print("2. No sample CSV file found for format testing")
        else:
            print("   ✗ Backend API not available in file monitor")
            return False
        
        return True
        
    except Exception as e:
        print(f"   ✗ File monitor test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run all integration tests"""
    print("Reference Data Management - Integration Tests")
    print("=" * 50)
    
    backend_ok = test_backend_api()
    monitor_ok = test_file_monitor()
    
    print("\nTest Summary")
    print("=" * 20)
    print(f"Backend API: {'✓ PASS' if backend_ok else '✗ FAIL'}")
    print(f"File Monitor: {'✓ PASS' if monitor_ok else '✗ FAIL'}")
    
    if backend_ok and monitor_ok:
        print("\n🎉 All tests passed! The integrated system is ready.")
        print("\nNext steps:")
        print("1. Start the file monitor: ./start_monitor.sh")
        print("2. Drop CSV files in: /home/lin/repo/reference_data_mgr/data/reference_data/dropoff/fullload/")
        print("3. Or use the test utility: ./test_monitor.py")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())