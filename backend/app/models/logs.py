"""
日志相关数据模型
"""

from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey, Text, Integer
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.core.database import Base
import uuid


class SystemLog(Base):
    """系统日志模型"""
    __tablename__ = "sys_logs"
    
    log_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    operation_type = Column(String(50), nullable=False)
    user_id = Column(String(36), ForeignKey("sys_users.user_id"))
    timestamp = Column(DateTime, server_default=func.getdate())
    details = Column(Text)  # JSON格式详情
    status = Column(String(20), default='SUCCESS')
    ip_address = Column(String(45))
    user_agent = Column(String(500))
    resource_type = Column(String(50))
    resource_id = Column(String(36))
    
    # 关系
    user = relationship("User", back_populates="logs")
    
    def __repr__(self):
        return f"<SystemLog(operation='{self.operation_type}')>"


class ValidationResult(Base):
    """验证结果模型"""
    __tablename__ = "validation_results"
    
    validation_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    csv_filename = Column(String(255), nullable=False)
    config_id = Column(String(36), ForeignKey("sys_config.config_id"))
    validation_rule = Column(String(500))
    start_time = Column(DateTime, nullable=False, server_default=func.getdate())
    end_time = Column(DateTime)
    status = Column(String(20), default='PENDING')
    total_records = Column(Integer, default=0)
    valid_records = Column(Integer, default=0)
    invalid_records = Column(Integer, default=0)
    error_details = Column(Text)  # JSON格式错误详情
    created_by = Column(String(36), ForeignKey("sys_users.user_id"))
    processing_time_ms = Column(Integer)
    
    # 关系
    created_by_user = relationship("User", back_populates="validation_results")
    
    def __repr__(self):
        return f"<ValidationResult(filename='{self.csv_filename}')>"


class FileProcessingRecord(Base):
    """文件处理记录模型"""
    __tablename__ = "file_processing_records"
    
    record_id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    original_filename = Column(String(255), nullable=False)
    file_size = Column(Integer)
    file_hash = Column(String(64))
    upload_time = Column(DateTime, server_default=func.getdate())
    processing_start_time = Column(DateTime)
    processing_end_time = Column(DateTime)
    status = Column(String(20), default='UPLOADED')
    target_table = Column(String(128))
    stage_table = Column(String(128))
    total_rows = Column(Integer, default=0)
    imported_rows = Column(Integer, default=0)
    error_rows = Column(Integer, default=0)
    validation_id = Column(String(36), ForeignKey("validation_results.validation_id"))
    created_by = Column(String(36), ForeignKey("sys_users.user_id"))
    error_message = Column(Text)
    
    # 关系
    created_by_user = relationship("User", back_populates="file_records")
    
    def __repr__(self):
        return f"<FileProcessingRecord(filename='{self.original_filename}')>"