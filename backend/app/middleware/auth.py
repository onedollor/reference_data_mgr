"""
认证中间件
"""

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # 这里可以添加认证逻辑
        response = await call_next(request)
        return response