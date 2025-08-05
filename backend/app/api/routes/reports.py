"""
报表统计API路由
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/dashboard")
async def get_dashboard():
    return {"message": "Dashboard endpoint"}

@router.get("/statistics")
async def get_statistics():
    return {"message": "Statistics endpoint"}