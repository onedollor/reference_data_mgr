"""
认证相关API路由
"""

from fastapi import APIRouter

router = APIRouter()

@router.post("/login")
async def login():
    return {"message": "Login endpoint"}

@router.post("/logout")
async def logout():
    return {"message": "Logout endpoint"}

@router.get("/profile")
async def get_profile():
    return {"message": "Profile endpoint"}