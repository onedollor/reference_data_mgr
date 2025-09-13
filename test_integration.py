#!/usr/bin/env python3
"""
Test script to verify backend API integration
"""

import sys
import os
from pathlib import Path

# Add backend directory to Python path
backend_path = os.path.join(os.path.dirname(__file__), 'backend')
sys.path.insert(0, backend_path)

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

def test_simplified_file_monitor():
    """Test simplified file monitor initialization"""
    print("\nTesting Simplified File Monitor Integration")
    print("=" * 40)

    try:
        from simplified_file_monitor import SimplifiedFileMonitor

        print("1. Initializing simplified file monitor...")
        monitor = SimplifiedFileMonitor()
        print("   ✓ Simplified file monitor initialized successfully")

        # Test database connection
        if monitor.db_manager is not None:
            print("   ✓ Database manager available in file monitor")

            # Test CSV detector
            if monitor.csv_detector is not None:
                print("   ✓ CSV detector initialized")
                
                # Test format detection with a sample file
                sample_file_path = "/home/lin/repo/reference_data_mgr/test.csv"
                if os.path.exists(sample_file_path):
                    print("2. Testing format detection...")
                    format_data = monitor.csv_detector.detect_format(sample_file_path)
                    print(f"   ✓ Detected delimiter: '{format_data.get('delimiter', 'unknown')}'")
                    print(f"   ✓ Detected encoding: '{format_data.get('encoding', 'unknown')}'")
                else:
                    print("2. No sample CSV file found for format testing")
        else:
            print("   ✗ Database manager not available in file monitor")
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
    monitor_ok = test_simplified_file_monitor()

    print("\nTest Summary")
    print("=" * 20)
    print(f"Backend API: {'✓ PASS' if backend_ok else '✗ FAIL'}")
    print(f"Simplified File Monitor: {'✓ PASS' if monitor_ok else '✗ FAIL'}")

    if backend_ok and monitor_ok:
        print("\n🎉 All tests passed! The integrated system is ready.")
        print("\nNext steps:")
        print("1. Start the simplified monitor: ./start_simplified_monitor.sh start")
        print("2. Drop CSV files in: /home/lin/repo/reference_data_mgr/data/reference_data/dropoff/")
        print("3. Check status: ./start_simplified_monitor.sh status")
    else:
        print("\n❌ Some tests failed. Please check the errors above.")
        return 1

    return 0

if __name__ == "__main__":
    sys.exit(main())