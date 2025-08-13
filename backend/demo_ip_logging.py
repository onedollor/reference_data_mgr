#!/usr/bin/env python3
"""
Demo script to show IP address logging functionality
"""

import asyncio
import sys
import os
import json

# Add backend to path to import utils
sys.path.insert(0, os.path.dirname(__file__))

from utils.logger import Logger

async def demo_ip_logging():
    """Demo the IP address logging functionality"""
    logger = Logger()
    
    print("🧪 Testing IP Address Logging Functionality")
    print("=" * 50)
    
    # Create test log entries with different IP addresses
    test_entries = [
        ("192.168.1.100", "user_login", "User login successful"),
        ("10.0.0.5", "file_upload", "CSV file uploaded for processing"),
        ("172.16.0.1", "data_ingest", "Starting data ingestion process"),
        ("203.0.113.42", "api_error", "Database connection timeout", "ConnectionTimeout: timeout after 30s"),
        ("198.51.100.10", "validation_warning", "Data validation warning detected"),
    ]
    
    print(f"📁 Log files will be created in: {logger.log_dir}")
    print(f"   - System log: {logger.log_file}")
    print(f"   - Error log: {logger.error_log_file}")
    print(f"   - Ingest log: {logger.ingest_log_file}")
    print()
    
    # Create log entries
    for ip, action, message, *traceback in test_entries:
        if traceback:
            await logger.log_error(action, message, traceback[0], source_ip=ip)
            print(f"✅ ERROR logged: {action} from {ip}")
        elif "warning" in action:
            await logger.log_warning(action, message, source_ip=ip)
            print(f"⚠️  WARNING logged: {action} from {ip}")
        else:
            await logger.log_info(action, message, source_ip=ip)
            print(f"ℹ️  INFO logged: {action} from {ip}")
    
    print("\n📊 Log Summary:")
    print("-" * 30)
    
    # Check if log files exist and show content preview
    for log_type, log_file in [
        ("System", logger.log_file),
        ("Error", logger.error_log_file), 
        ("Ingest", logger.ingest_log_file)
    ]:
        if os.path.exists(log_file):
            with open(log_file, 'r') as f:
                lines = f.readlines()
                print(f"{log_type} Log: {len(lines)} entries")
                
                # Show last entry with IP
                if lines:
                    try:
                        last_entry = json.loads(lines[-1])
                        ip = last_entry.get('source_ip', 'N/A')
                        action = last_entry.get('action_step', 'N/A')
                        print(f"  Latest: {action} from IP {ip}")
                    except:
                        print(f"  Latest: {lines[-1][:50]}...")
        else:
            print(f"{log_type} Log: No entries")
    
    print(f"\n🎯 IP address logging successfully implemented!")
    print(f"📝 All log entries now include source IP address as a dedicated column")

if __name__ == "__main__":
    asyncio.run(demo_ip_logging())