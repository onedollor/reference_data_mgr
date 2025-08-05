#!/usr/bin/env python3
"""
CSV数据管理系统 - 前端启动器（简化版）
"""

import os
import sys
import subprocess
import json
from pathlib import Path
import webbrowser
import time

def check_node():
    """检查Node.js环境"""
    try:
        result = subprocess.run(['node', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"Node.js: {result.stdout.strip()}")
            return True
        else:
            print("ERROR: Node.js not found")
            return False
    except FileNotFoundError:
        print("ERROR: Node.js not installed")
        return False

def check_npm():
    """检查npm"""
    try:
        # 在Windows上使用shell=True直接运行npm
        result = subprocess.run('npm --version', capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            print(f"npm: {result.stdout.strip()}")
            return True
        else:
            print("ERROR: npm not found")
            return False
    except Exception as e:
        print(f"ERROR: npm check failed: {e}")
        return False

def install_deps():
    """安装依赖"""
    if Path("node_modules").exists():
        print("Dependencies already installed")
        return True
    
    print("Installing dependencies...")
    try:
        result = subprocess.run('npm install', capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            print("Dependencies installed successfully")
            return True
        else:
            print(f"ERROR: Failed to install dependencies: {result.stderr}")
            return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def create_env():
    """创建环境配置"""
    env_content = """# CSV数据管理系统 - 环境配置
VITE_APP_TITLE=CSV数据管理系统
VITE_APP_VERSION=1.0.0
VITE_API_URL=http://localhost:8000
VITE_API_BASE_PATH=/api
"""
    
    with open('.env', 'w', encoding='utf-8') as f:
        f.write(env_content)
    print("Environment config created")

def start_dev_server():
    """启动开发服务器"""
    print("Starting development server...")
    try:
        # 启动Vite开发服务器
        process = subprocess.Popen(
            'npm run dev',
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
            shell=True
        )
        
        print("Server starting... Please wait...")
        time.sleep(3)
        
        if process.poll() is None:
            print("Development server started successfully!")
            print("Access URL: http://localhost:5173")
            
            # 打开浏览器
            try:
                webbrowser.open("http://localhost:5173")
                print("Browser opened")
            except:
                print("Please open browser manually: http://localhost:5173")
            
            print("\nPress Ctrl+C to stop server")
            
            try:
                # 等待进程结束
                process.wait()
            except KeyboardInterrupt:
                print("\nStopping server...")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                print("Server stopped")
        else:
            print("ERROR: Failed to start server")
            return False
            
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    """主函数"""
    print("=== CSV数据管理系统 - 前端启动器 ===")
    print(f"工作目录: {Path.cwd()}")
    print()
    
    # 检查环境
    print("Checking environment...")
    if not check_node():
        print("Please install Node.js first")
        return
    
    if not check_npm():
        print("Please install npm first")
        return
    
    # 创建配置
    create_env()
    
    # 安装依赖
    if not install_deps():
        return
    
    # 启动服务器
    start_dev_server()

if __name__ == "__main__":
    main()