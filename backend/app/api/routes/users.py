"""
用户管理API路由
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/")
async def get_users():
    return {"message": "Get users endpoint"}

@router.post("/")
async def create_user():
    return {"message": "Create user endpoint"}