"""
日志系统API路由
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_logs():
    return {"message": "Get logs endpoint"}

@router.get("/export")
async def export_logs():
    return {"message": "Export logs endpoint"}