"""
配置管理API路由
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_configs():
    return {"message": "Get configs endpoint"}

@router.post("/")
async def create_config():
    return {"message": "Create config endpoint"}