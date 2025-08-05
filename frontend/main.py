#!/usr/bin/env python3
"""
CSV数据管理系统 - 前端应用启动器
基于PRD.md需求文档实现企业级前端应用
"""

import os
import sys
import subprocess
import json
from pathlib import Path
import webbrowser
import time
import signal
from typing import Dict, Any
import emoji

class FrontendApp:
    """前端应用管理器"""
    
    def __init__(self):
        self.app_name = "CSV数据管理系统"
        self.version = "1.0.0"
        self.frontend_dir = Path(__file__).parent
        self.api_url = "http://localhost:8000"
        self.dev_port = 5173  # Vite默认端口
        self.build_dir = self.frontend_dir / "dist"
        self.process = None
        
    def check_prerequisites(self) -> bool:
        """检查前端运行环境"""
        print("[CHECK] 检查运行环境...")
        
        # 检查Node.js
        try:
            result = subprocess.run(['node', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                node_version = result.stdout.strip()
                print(f"[OK] Node.js: {node_version}")
            else:
                print("[ERROR] Node.js 未安装或版本过低")
                return False
        except FileNotFoundError:
            print("[ERROR] Node.js 未安装")
            return False
        
        # 检查npm
        try:
            result = subprocess.run(['npm', '--version'], capture_output=True, text=True)
            if result.returncode == 0:
                npm_version = result.stdout.strip()
                print(f"[OK] npm: {npm_version}")
            else:
                print("[ERROR] npm 未安装")
                return False
        except FileNotFoundError:
            print("[ERROR] npm 未安装")
            return False
            
        return True
    
    def install_dependencies(self) -> bool:
        """安装Node.js依赖"""
        package_json = self.frontend_dir / "package.json"
        
        if not package_json.exists():
            print(f"{emoji.emojize(':cross_mark:')} package.json 文件不存在")
            return False
            
        node_modules = self.frontend_dir / "node_modules"
        if node_modules.exists():
            print(f"{emoji.emojize(':check_mark_button:')} 依赖已安装")
            return True
            
        print(f"{emoji.emojize(':package:')} 安装依赖包...")
        try:
            result = subprocess.run(
                ['npm', 'install'], 
                cwd=self.frontend_dir,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"{emoji.emojize(':check_mark_button:')} 依赖安装成功")
                return True
            else:
                print(f"{emoji.emojize(':cross_mark:')} 依赖安装失败: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"{emoji.emojize(':cross_mark:')} 安装依赖时出错: {e}")
            return False
    
    def create_env_config(self):
        """创建环境配置文件"""
        env_file = self.frontend_dir / ".env"
        
        env_config = f"""# CSV数据管理系统 - 前端环境配置
VITE_APP_TITLE={self.app_name}
VITE_APP_VERSION={self.version}
VITE_API_URL={self.api_url}
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
        
        with open(env_file, 'w', encoding='utf-8') as f:
            f.write(env_config)
            
        print(f"{emoji.emojize(':check_mark_button:')} 环境配置文件已创建")
    
    def start_development_server(self) -> bool:
        """启动开发服务器"""
        print(f"{emoji.emojize(':rocket:')} 启动开发服务器 (端口: {self.dev_port})...")
        
        try:
            # 启动Vite开发服务器
            self.process = subprocess.Popen(
                ['npm', 'run', 'dev'],
                cwd=self.frontend_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # 等待服务器启动
            print(f"{emoji.emojize(':hourglass_not_done:')} 等待服务器启动...")
            time.sleep(3)
            
            if self.process.poll() is None:
                print(f"{emoji.emojize(':check_mark_button:')} 开发服务器已启动")
                print(f"{emoji.emojize(':globe_with_meridians:')} 访问地址: http://localhost:{self.dev_port}")
                return True
            else:
                stdout, stderr = self.process.communicate()
                print(f"{emoji.emojize(':cross_mark:')} 服务器启动失败:")
                print(f"stdout: {stdout}")
                print(f"stderr: {stderr}")
                return False
                
        except Exception as e:
            print(f"{emoji.emojize(':cross_mark:')} 启动开发服务器时出错: {e}")
            return False
    
    def build_production(self) -> bool:
        """构建生产版本"""
        print(f"{emoji.emojize(':building_construction:')} 构建生产版本...")
        
        try:
            result = subprocess.run(
                ['npm', 'run', 'build'],
                cwd=self.frontend_dir,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"{emoji.emojize(':check_mark_button:')} 构建成功")
                print(f"{emoji.emojize(':file_folder:')} 构建文件位置: {self.build_dir}")
                return True
            else:
                print(f"{emoji.emojize(':cross_mark:')} 构建失败: {result.stderr}")
                return False
                
        except Exception as e:
            print(f"{emoji.emojize(':cross_mark:')} 构建时出错: {e}")
            return False
    
    def serve_production(self) -> bool:
        """启动生产服务器"""
        if not self.build_dir.exists():
            print(f"{emoji.emojize(':cross_mark:')} 生产构建不存在，请先运行构建")
            return False
            
        print(f"{emoji.emojize(':globe_with_meridians:')} 启动生产服务器...")
        
        try:
            # 使用npm serve启动静态文件服务器
            self.process = subprocess.Popen(
                ['npx', 'serve', '-s', 'dist', '-l', str(self.dev_port)],
                cwd=self.frontend_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            time.sleep(2)
            
            if self.process.poll() is None:
                print(f"{emoji.emojize(':check_mark_button:')} 生产服务器已启动")
                print(f"{emoji.emojize(':globe_with_meridians:')} 访问地址: http://localhost:{self.dev_port}")
                return True
            else:
                stdout, stderr = self.process.communicate()
                print(f"{emoji.emojize(':cross_mark:')} 生产服务器启动失败:")
                print(f"stdout: {stdout}")
                print(f"stderr: {stderr}")
                return False
                
        except Exception as e:
            print(f"{emoji.emojize(':cross_mark:')} 启动生产服务器时出错: {e}")
            return False
    
    def open_browser(self):
        """打开浏览器"""
        url = f"http://localhost:{self.dev_port}"
        print(f"{emoji.emojize(':globe_with_meridians:')} 正在打开浏览器: {url}")
        
        try:
            webbrowser.open(url)
        except Exception as e:
            print(f"{emoji.emojize(':warning:')} 无法自动打开浏览器: {e}")
            print(f"请手动访问: {url}")
    
    def stop_server(self):
        """停止服务器"""
        if self.process and self.process.poll() is None:
            print(f"{emoji.emojize(':stop_sign:')} 正在停止服务器...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
                print(f"{emoji.emojize(':check_mark_button:')} 服务器已停止")
            except subprocess.TimeoutExpired:
                print(f"{emoji.emojize(':warning:')} 强制结束服务器进程")
                self.process.kill()
    
    def show_help(self):
        """显示帮助信息"""
        help_text = f"""
{self.app_name} v{self.version} - 前端应用启动器

基于PRD.md需求实现的企业级CSV数据管理系统前端

使用方法:
  python main.py [命令] [选项]

命令:
  dev, serve     启动开发服务器 (默认)
  build          构建生产版本
  prod           启动生产服务器
  install        安装依赖包
  help           显示此帮助信息

选项:
  --port PORT    指定端口号 (默认: {self.dev_port})
  --api-url URL  指定API地址 (默认: {self.api_url})
  --no-browser   不自动打开浏览器

系统功能 (基于PRD需求):
  {emoji.emojize(':check_mark_button:')} 配置管理页面 - 管理CSV导入配置
  {emoji.emojize(':check_mark_button:')} 文件上传页面 - CSV文件上传和导入
  {emoji.emojize(':check_mark_button:')} 用户管理页面 - 用户和用户组管理
  {emoji.emojize(':check_mark_button:')} 日志浏览页面 - 查看操作和验证日志
  {emoji.emojize(':check_mark_button:')} 数据可视化页面 - 导入统计和趋势分析

技术特性:
  • React 18 + TypeScript + Ant Design
  • 企业级UI组件和设计系统
  • 响应式设计，支持多设备
  • 实时数据更新和状态管理
  • 完整的错误处理和用户反馈

示例:
  python main.py                    # 启动开发服务器
  python main.py dev --port 3000    # 在3000端口启动开发服务器
  python main.py build              # 构建生产版本
  python main.py prod               # 启动生产服务器
"""
        print(help_text)
    
    def run(self, args: list):
        """运行应用"""
        command = args[0] if args else 'dev'
        options = args[1:] if len(args) > 1 else []
        
        # 解析选项
        for i, option in enumerate(options):
            if option == '--port' and i + 1 < len(options):
                self.dev_port = int(options[i + 1])
            elif option == '--api-url' and i + 1 < len(options):
                self.api_url = options[i + 1]
        
        no_browser = '--no-browser' in options
        
        print(f"[APP] {self.app_name} v{self.version}")
        print(f"[DIR] 工作目录: {self.frontend_dir}")
        print(f"[API] API地址: {self.api_url}")
        print()
        
        # 注册信号处理器
        def signal_handler(sig, frame):
            print("\n[STOP] 收到中断信号，正在关闭...")
            self.stop_server()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            if command in ['help', '-h', '--help']:
                self.show_help()
                return
            
            # 检查环境
            if not self.check_prerequisites():
                print(f"{emoji.emojize(':cross_mark:')} 环境检查失败，请安装Node.js和npm")
                return
            
            # 创建配置文件
            self.create_env_config()
            
            if command == 'install':
                self.install_dependencies()
                return
            elif command == 'build':
                if self.install_dependencies():
                    self.build_production()
                return
            elif command == 'prod':
                if self.install_dependencies():
                    if not self.build_dir.exists():
                        print(f"{emoji.emojize(':warning:')} 生产构建不存在，正在构建...")
                        if not self.build_production():
                            return
                    
                    if self.serve_production():
                        if not no_browser:
                            time.sleep(1)
                            self.open_browser()
                        
                        print(f"{emoji.emojize(':sparkles:')} 生产服务器运行中...")
                        print("按 Ctrl+C 停止服务器")
                        
                        # 保持服务器运行
                        try:
                            self.process.wait()
                        except KeyboardInterrupt:
                            pass
                return
            else:  # 默认dev命令
                if self.install_dependencies():
                    if self.start_development_server():
                        if not no_browser:
                            time.sleep(1)
                            self.open_browser()
                        
                        print(f"{emoji.emojize(':sparkles:')} 开发服务器运行中...")
                        print(f"{emoji.emojize(':mobile_phone:')} 支持热重载，修改代码后自动刷新")
                        print("按 Ctrl+C 停止服务器")
                        
                        # 保持服务器运行
                        try:
                            self.process.wait()
                        except KeyboardInterrupt:
                            pass
                
        except Exception as e:
            print(f"{emoji.emojize(':cross_mark:')} 运行时出错: {e}")
        finally:
            self.stop_server()


def main():
    """主入口函数"""
    app = FrontendApp()
    args = sys.argv[1:]
    app.run(args)


if __name__ == "__main__":
    main()