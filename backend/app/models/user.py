"""
用户相关数据模型
"""

from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base
import uuid


class UserGroup(Base):
    """用户组模型"""
    __tablename__ = "sys_user_groups"
    
    group_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    group_name = Column(String(50), nullable=False, unique=True)
    permissions = Column(Text)  # JSON格式权限配置
    description = Column(String(255))
    created_at = Column(DateTime, server_default=func.getdate())
    updated_at = Column(DateTime, server_default=func.getdate(), onupdate=func.getdate())
    is_active = Column(Boolean, default=True)
    
    # 关系
    users = relationship("User", back_populates="user_group")
    
    def __repr__(self):
        return f"<UserGroup(group_name='{self.group_name}')>"


class User(Base):
    """用户模型"""
    __tablename__ = "sys_users"
    
    user_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String(50), nullable=False, unique=True)
    email = Column(String(100), nullable=False, unique=True)
    password_hash = Column(String(255), nullable=False)
    user_group_id = Column(String(36), ForeignKey("sys_user_groups.group_id"))
    full_name = Column(String(100))
    last_login = Column(DateTime)
    created_at = Column(DateTime, server_default=func.getdate())
    updated_at = Column(DateTime, server_default=func.getdate(), onupdate=func.getdate())
    is_active = Column(Boolean, default=True)
    
    # 关系
    user_group = relationship("UserGroup", back_populates="users")
    configs = relationship("SystemConfig", back_populates="created_by_user")
    logs = relationship("SystemLog", back_populates="user")
    validation_results = relationship("ValidationResult", back_populates="created_by_user")
    file_records = relationship("FileProcessingRecord", back_populates="created_by_user")
    
    def __repr__(self):
        return f"<User(username='{self.username}')>"
    
    @property
    def is_admin(self) -> bool:
        """检查是否为管理员"""
        return self.user_group and "admin" in self.user_group.group_name.lower()
    
    def has_permission(self, permission: str) -> bool:
        """检查用户是否有特定权限"""
        if not self.user_group or not self.user_group.permissions:
            return False
        
        try:
            import json
            permissions = json.loads(self.user_group.permissions)
            
            # 管理员拥有所有权限
            if permissions.get("all"):
                return True
            
            # 检查具体权限
            if "." in permission:
                resource, action = permission.split(".", 1)
                resource_perms = permissions.get(resource, [])
                return action in resource_perms
            
            return permission in permissions
            
        except (json.JSONDecodeError, KeyError):
            return False