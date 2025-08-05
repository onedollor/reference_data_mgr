"""
文件处理API路由
"""

from fastapi import APIRouter

router = APIRouter()

@router.post("/upload")
async def upload_file():
    return {"message": "Upload file endpoint"}

@router.post("/import")
async def import_file():
    return {"message": "Import file endpoint"}