"""
配置相关数据模型
"""

from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base
import uuid


class SystemConfig(Base):
    """系统配置模型"""
    __tablename__ = "sys_config"
    
    config_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    csv_filename_pattern = Column(String(255), nullable=False)
    validation_rule = Column(String(500))
    validation_type = Column(String(20))
    target_table_name = Column(String(128), nullable=False)
    stage_table_name = Column(String(128), nullable=False)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    created_by = Column(String(36), ForeignKey("sys_users.user_id"), nullable=False)
    created_at = Column(DateTime, server_default=func.getdate())
    updated_at = Column(DateTime, server_default=func.getdate(), onupdate=func.getdate())
    is_active = Column(Boolean, default=True)
    
    # 关系
    created_by_user = relationship("User", back_populates="configs")
    
    def __repr__(self):
        return f"<SystemConfig(pattern='{self.csv_filename_pattern}')>"