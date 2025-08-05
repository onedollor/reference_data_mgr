#!/usr/bin/env python3
"""
CSV数据管理系统 - 简单前端启动器
完全避免Unicode和复杂shell问题
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
"""
    
    try:
        with open('.env', 'w', encoding='utf-8') as f:
            f.write(env_content)
        print("Environment config created")
    except Exception as e:
        print(f"Warning: Could not create .env file: {e}")
    
    # 检查依赖是否已安装
    if not Path("node_modules").exists():
        print("Installing dependencies...")
        try:
            # 直接使用os.system，更简单但足够
            result = os.system("npm install")
            if result != 0:
                print("Failed to install dependencies")
                return
            print("Dependencies installed successfully")
        except Exception as e:
            print(f"Error installing dependencies: {e}")
            return
    else:
        print("Dependencies already installed")
    
    # 启动开发服务器
    print("Starting development server...")
    print("This will open http://localhost:5173 in your browser")
    print("Press Ctrl+C to stop the server")
    print()
    
    try:
        # 使用os.system启动开发服务器
        os.system("npm run dev")
    except KeyboardInterrupt:
        print("\nServer stopped")
    except Exception as e:
        print(f"Error starting server: {e}")

if __name__ == "__main__":
    main()