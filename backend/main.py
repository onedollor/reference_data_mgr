"""
CSV数据管理系统 - 主应用入口
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from datetime import datetime
import os
from pathlib import Path

# 简化版本的应用，先确保能启动
app = FastAPI(
    title="CSV数据管理系统",
    description="企业级CSV数据导入、验证和管理系统",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 健康检查
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# 基础API路由
@app.get("/api/test")
async def test_endpoint():
    return {"message": "API服务正常运行"}

# 应用启动事件
@app.on_event("startup")
async def startup_event():
    # 创建必要的目录
    directories = ["uploads", "temp", "logs"]
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    print("Directory structure initialized")
    print("CSV Data Manager started successfully")

# 应用关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    print("Application shutdown")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )