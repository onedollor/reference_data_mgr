#!/usr/bin/env python3
"""
Simple test script for the enhanced logging functionality
"""

import asyncio
import sys
import os

# Add backend to path to import utils
sys.path.insert(0, os.path.dirname(__file__))

from utils.logger import Logger

async def test_logging():
    """Test the enhanced logging functionality"""
    logger = Logger()
    
    print("Testing enhanced logging functionality...")
    print(f"Log directory: {logger.log_dir}")
    print(f"System log: {logger.log_file}")
    print(f"Error log: {logger.error_log_file}")
    print(f"Ingest log: {logger.ingest_log_file}")
    
    # Test different types of logging with IP addresses
    await logger.log_info("test_system", "This is a system info message", source_ip="192.168.1.100")
    await logger.log_warning("test_warning", "This is a warning message", source_ip="10.0.0.5")
    await logger.log_error("test_error", "This is an error message", "Sample traceback", source_ip="172.16.0.1")
    await logger.log_info("ingest_test", "This is an ingest-related message that should go to ingest.log", source_ip="203.0.113.42")
    await logger.log_info("upload_test", "This is an upload-related message", source_ip="198.51.100.10")
    await logger.log_error("process_file_error", "This is a process file error", "Error traceback", source_ip="203.0.113.0")
    
    print("\nLog entries created successfully!")
    
    # Test retrieving logs
    print("\n=== System Logs ===")
    system_logs = await logger.get_logs_by_type("system", 5)
    for log in system_logs[-3:]:  # Show last 3
        ip = log.get('source_ip', 'N/A')
        print(f"[{log['timestamp_local']}] {log['level']} - {log['action_step']} (IP: {ip}): {log['message']}")
    
    print("\n=== Error Logs ===")
    error_logs = await logger.get_logs_by_type("error", 5)
    for log in error_logs:
        ip = log.get('source_ip', 'N/A')
        print(f"[{log['timestamp_local']}] {log['level']} - {log['action_step']} (IP: {ip}): {log['message']}")
    
    print("\n=== Ingest Logs ===")
    ingest_logs = await logger.get_logs_by_type("ingest", 5)
    for log in ingest_logs:
        ip = log.get('source_ip', 'N/A')
        print(f"[{log['timestamp_local']}] {log['level']} - {log['action_step']} (IP: {ip}): {log['message']}")
    
    print("\nTest completed!")

if __name__ == "__main__":
    asyncio.run(test_logging())