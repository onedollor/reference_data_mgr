#!/usr/bin/env python3

import pyodbc
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_db_connection():
    try:
        # Get database configuration from .env
        db_host = os.getenv('db_host', 'localhost')
        db_name = os.getenv('db_name', 'test')
        db_user = os.getenv('db_user', 'tester')
        db_password = os.getenv('db_password', '111@abc!')
        
        print(f"Testing connection to:")
        print(f"  Host: {db_host}")
        print(f"  Database: {db_name}")
        print(f"  User: {db_user}")
        print(f"  Password: {'*' * len(db_password)}")
        print()
        
        # Build connection string
        connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_host};"
            f"DATABASE={db_name};"
            f"UID={db_user};"
            f"PWD={db_password};"
            f"TrustServerCertificate=yes;"
        )
        
        print("Attempting connection...")
        connection = pyodbc.connect(connection_string, timeout=10)
        
        # Test query
        cursor = connection.cursor()
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        
        print("✅ Connection successful!")
        print(f"SQL Server Version: {version[:100]}...")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        print()
        
        # Try alternative connection to master database with sa user
        print("Trying alternative connection (sa/master)...")
        try:
            alt_connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_host};"
                f"DATABASE=master;"
                f"UID=sa;"
                f"PWD=121@abc!;"
                f"TrustServerCertificate=yes;"
            )
            
            connection = pyodbc.connect(alt_connection_string, timeout=10)
            cursor = connection.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            
            print("✅ Alternative connection successful!")
            print(f"SQL Server Version: {version[:100]}...")
            print()
            print("💡 Suggestion: Update .env to use 'sa' user and 'master' database")
            
            cursor.close()
            connection.close()
            return True
            
        except Exception as alt_e:
            print(f"❌ Alternative connection also failed: {str(alt_e)}")
            return False

if __name__ == "__main__":
    test_db_connection()