#!/usr/bin/env python3
"""
Test script to demonstrate the file monitor functionality
"""

import os
import time
import csv
from datetime import datetime

# Test data
TEST_DATA = [
    ['id', 'name', 'category', 'price'],
    ['1', 'Product A', 'Electronics', '99.99'],
    ['2', 'Product B', 'Books', '19.99'],
    ['3', 'Product C', 'Clothing', '49.99']
]

DROPOFF_PATH = "/home/lin/repo/reference_data_mgr/data/reference_data/dropoff"

def create_test_file(folder_type="fullload"):
    """Create a test CSV file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"test_products_{timestamp}.csv"

    folder_path = os.path.join(DROPOFF_PATH, folder_type)
    file_path = os.path.join(folder_path, filename)

    print(f"Creating test file: {file_path}")

    # Write CSV in chunks to simulate a file being written
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        for i, row in enumerate(TEST_DATA):
            writer.writerow(row)
            print(f"  Written row {i+1}/{len(TEST_DATA)}")
            time.sleep(0.5)  # Simulate slow write

    print(f"Test file created successfully: {filename}")
    return file_path

def main():
    print("File Monitor Test Utility")
    print("=" * 40)

    # Check if dropoff folders exist
    fullload_path = os.path.join(DROPOFF_PATH, "fullload")
    append_path = os.path.join(DROPOFF_PATH, "append")

    if not os.path.exists(fullload_path) or not os.path.exists(append_path):
        print("ERROR: Dropoff folders not found!")
        print(f"Expected: {DROPOFF_PATH}")
        return

    print(f"Dropoff folders found at: {DROPOFF_PATH}")

    while True:
        print("\nOptions:")
        print("1. Create test file in fullload folder")
        print("2. Create test file in append folder")
        print("3. List files in dropoff folders")
        print("4. Exit")

        choice = input("\nEnter choice (1-4): ").strip()

        if choice == '1':
            create_test_file("fullload")
        elif choice == '2':
            create_test_file("append")
        elif choice == '3':
            print("\nCurrent files in dropoff folders:")
            for folder in ['fullload', 'append', 'processed', 'error']:
                folder_path = os.path.join(DROPOFF_PATH, folder)
                if os.path.exists(folder_path):
                    files = os.listdir(folder_path)
                    print(f"  {folder}/: {files if files else '(empty)'}")
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice!")

if __name__ == "__main__":
    main()