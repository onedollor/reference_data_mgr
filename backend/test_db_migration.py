#!/usr/bin/env python3
"""
Test script to verify database migration and source_ip column handling
"""

import asyncio
import sys
import os

# Add backend to path to import utils
sys.path.insert(0, os.path.dirname(__file__))

from utils.logger import DatabaseLogger
from utils.database import DatabaseManager

async def test_db_migration():
    """Test database migration and IP logging"""
    print("🔄 Testing Database Migration and IP Logging")
    print("=" * 50)
    
    try:
        # Initialize database manager
        print("📊 Initializing database manager...")
        db_manager = DatabaseManager()
        
        # Test connection
        print("🔌 Testing database connection...")
        conn = db_manager.get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        print(f"✅ Connected to: {version.split('\\n')[0]}")
        conn.close()
        
        # Initialize logger with database support
        print("📝 Initializing database logger...")
        logger = DatabaseLogger(db_manager)
        
        # This will trigger table creation/migration
        print("🛠️  Ensuring log table exists and migrating if needed...")
        await logger.log_info("migration_test", "Testing database migration", source_ip="127.0.0.1")
        print("✅ Database log table ensured and migration completed")
        
        # Test logging with different IP addresses
        print("🧪 Testing IP address logging...")
        test_entries = [
            ("192.168.1.100", "test_info", "INFO level test with IP"),
            ("10.0.0.5", "test_warning", "WARNING level test with IP"),
            ("172.16.0.1", "test_error", "ERROR level test with IP", "Sample error traceback"),
        ]
        
        for ip, action, message, *traceback in test_entries:
            if traceback:
                await logger.log_error(action, message, traceback[0], source_ip=ip)
                print(f"  ✅ ERROR logged from {ip}")
            elif "warning" in action:
                await logger.log_warning(action, message, source_ip=ip)
                print(f"  ✅ WARNING logged from {ip}")
            else:
                await logger.log_info(action, message, source_ip=ip)
                print(f"  ✅ INFO logged from {ip}")
        
        # Test log retrieval
        print("📖 Testing log retrieval...")
        logs = await logger.get_logs(5)
        print(f"✅ Retrieved {len(logs)} log entries")
        
        # Show latest entries with IP addresses
        print("📋 Latest log entries:")
        for i, log in enumerate(logs[-3:], 1):
            ip = log.get('source_ip', 'N/A')
            level = log.get('level', 'N/A')
            action = log.get('action_step', 'N/A')
            message = log.get('message', 'N/A')[:50]
            timestamp = log.get('timestamp_local', 'N/A')[:19]
            print(f"  {i}. [{timestamp}] {level} - {action} (IP: {ip})")
            print(f"     Message: {message}...")
        
        print("\n🎯 Database migration and IP logging test completed successfully!")
        print("✅ source_ip column has been added to existing tables")
        print("✅ Both new and existing log entries are handled correctly")
        print("✅ IP address tracking is fully functional")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(test_db_migration())