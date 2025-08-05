#!/usr/bin/env python3
"""
CSV数据管理系统 - 前端启动器（无Unicode版本）
"""

import os
import sys
import subprocess
import time
from pathlib import Path

def main():
    print("=== CSV Data Manager - Frontend Launcher ===")
    print(f"Working directory: {Path.cwd()}")
    print()
    
    # 创建环境配置文件
    env_content = """# CSV数据管理系统 - 环境配置
VITE_APP_TITLE=CSV数据管理系统
VITE_APP_VERSION=1.0.0
VITE_API_URL=http://localhost:8000
VITE_API_BASE_PATH=/api

# 功能开关（基于PRD需求）
VITE_ENABLE_CONFIG_MANAGEMENT=true
VITE_ENABLE_FILE_UPLOAD=true
VITE_ENABLE_USER_MANAGEMENT=true
VITE_ENABLE_LOG_VIEWER=true
VITE_ENABLE_DATA_VISUALIZATION=true

# CSV文件格式配置
VITE_CSV_FILE_PATTERN=*.yyyyMMddHHmmss.csv
VITE_MAX_FILE_SIZE=104857600
VITE_ALLOWED_FILE_TYPES=.csv

# UI配置
VITE_PRIMARY_COLOR=#2563eb
VITE_SUCCESS_COLOR=#059669
VITE_WARNING_COLOR=#d97706
VITE_ERROR_COLOR=#dc2626
"""
    
    try:
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)
        print("[OK] Environment config created")
    except Exception as e:
        print(f"[WARNING] Could not create .env file: {e}")
    
    # 检查依赖是否已安装
    if not Path("node_modules").exists():
        print("[INFO] Installing dependencies...")
        try:
            result = subprocess.run(['npm', 'install'], shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print("[OK] Dependencies installed successfully")
            else:
                print(f"[ERROR] Failed to install dependencies: {result.stderr}")
                return
        except Exception as e:
            print(f"[ERROR] Error installing dependencies: {e}")
            return
    else:
        print("[OK] Dependencies already installed")
    
    # 启动开发服务器
    print("[INFO] Starting development server...")
    print("[INFO] Server will be available at http://localhost:5173")
    print("[INFO] Press Ctrl+C to stop the server")
    print()
    
    try:
        # 启动开发服务器
        process = subprocess.Popen(['npm', 'run', 'dev'], shell=True)
        
        print("[OK] Development server started")
        print("[INFO] You can now access the application at http://localhost:5173")
        print("[INFO] The server supports hot reload - changes will be reflected automatically")
        print()
        print("Press Ctrl+C to stop the server...")
        
        # 等待用户中断
        try:
            process.wait()
        except KeyboardInterrupt:
            print("\n[INFO] Stopping server...")
            process.terminate()
            try:
                process.wait(timeout=5)
                print("[OK] Server stopped successfully")
            except subprocess.TimeoutExpired:
                print("[WARNING] Force killing server process")
                process.kill()
                
    except Exception as e:
        print(f"[ERROR] Error starting server: {e}")

if __name__ == "__main__":
    main()