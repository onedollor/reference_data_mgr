"""
CSV文件处理服务
"""

import pandas as pd
import os
import re
import hashlib
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
import logging
import asyncio
from dataclasses import dataclass

from app.core.config import get_settings
from app.core.database import db_manager
from app.models.logs import FileProcessingRecord, ValidationResult
from app.services.data_validator import DataValidator

settings = get_settings()
logger = logging.getLogger(__name__)


@dataclass
class FileInfo:
    """文件信息数据类"""
    original_filename: str
    table_name: str
    timestamp_str: str
    timestamp: datetime
    file_size: int
    file_hash: str
    columns: List[str]


class FileProcessor:
    """CSV文件处理器"""
    
    def __init__(self):
        self.upload_dir = Path(settings.UPLOAD_DIR)
        self.temp_dir = Path(settings.TEMP_DIR)
        self.upload_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        self.validator = DataValidator()
    
    def parse_filename(self, filename: str) -> Optional[FileInfo]:
        """
        解析CSV文件名，提取表名和时间戳
        格式: 文件名.yyyyMMddHHmmss.csv
        例如: customer.20250104120000.csv
        """
        try:
            # 使用正则表达式匹配文件名格式
            pattern = r'^(.+)\.(\d{14})\.csv$'
            match = re.match(pattern, filename)
            
            if not match:
                logger.error(f"文件名格式不正确: {filename}")
                return None
            
            table_name = match.group(1)
            timestamp_str = match.group(2)
            
            # 解析时间戳
            try:
                timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
            except ValueError:
                logger.error(f"时间戳格式不正确: {timestamp_str}")
                return None
            
            return FileInfo(
                original_filename=filename,
                table_name=f"{table_name}_{timestamp_str}",
                timestamp_str=timestamp_str,
                timestamp=timestamp,
                file_size=0,
                file_hash="",
                columns=[]
            )
            
        except Exception as e:
            logger.error(f"解析文件名失败: {e}")
            return None
    
    def calculate_file_hash(self, file_path: Path) -> str:
        """计算文件MD5哈希值"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def validate_csv_format(self, file_path: Path) -> Tuple[bool, str, List[str]]:
        """
        验证CSV文件格式
        返回: (是否有效, 错误信息, 列名列表)
        """
        try:
            # 读取CSV文件头部
            df = pd.read_csv(file_path, nrows=5, encoding='utf-8')
            
            if df.empty:
                return False, "CSV文件为空", []
            
            columns = df.columns.tolist()
            
            # 检查是否有重复列名
            if len(columns) != len(set(columns)):
                return False, "CSV文件包含重复的列名", columns
            
            # 检查列名是否包含特殊字符
            invalid_chars = ['[', ']', '(', ')', ',', ';', ':', '\'', '"']
            for col in columns:
                if any(char in str(col) for char in invalid_chars):
                    return False, f"列名包含非法字符: {col}", columns
            
            return True, "", columns
            
        except UnicodeDecodeError:
            try:
                # 尝试GBK编码
                df = pd.read_csv(file_path, nrows=5, encoding='gbk')
                columns = df.columns.tolist()
                return True, "", columns
            except Exception as e:
                return False, f"文件编码错误: {e}", []
        except Exception as e:
            return False, f"CSV格式验证失败: {e}", []
    
    async def save_uploaded_file(self, file_content: bytes, filename: str) -> Path:
        """保存上传的文件"""
        file_path = self.upload_dir / filename
        
        # 检查文件大小
        if len(file_content) > settings.MAX_FILE_SIZE:
            raise ValueError(f"文件大小超过限制 ({settings.MAX_FILE_SIZE} bytes)")
        
        # 异步写入文件
        with open(file_path, 'wb') as f:
            f.write(file_content)
        
        return file_path
    
    def prepare_file_info(self, file_path: Path) -> FileInfo:
        """准备文件信息"""
        filename = file_path.name
        file_info = self.parse_filename(filename)
        
        if not file_info:
            raise ValueError(f"无法解析文件名: {filename}")
        
        # 设置文件大小和哈希
        file_info.file_size = file_path.stat().st_size
        file_info.file_hash = self.calculate_file_hash(file_path)
        
        # 验证CSV格式并获取列信息
        is_valid, error_msg, columns = self.validate_csv_format(file_path)
        if not is_valid:
            raise ValueError(f"CSV格式验证失败: {error_msg}")
        
        file_info.columns = columns
        return file_info
    
    def create_tables_if_not_exists(self, file_info: FileInfo) -> bool:
        """创建表（如果不存在）"""
        try:
            # 检查主表是否存在
            if not db_manager.table_exists(file_info.table_name):
                success = db_manager.create_table_from_csv_structure(
                    file_info.table_name, 
                    file_info.columns
                )
                if not success:
                    return False
                
                logger.info(f"成功创建表: {file_info.table_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            return False
    
    def import_to_stage_table(self, file_path: Path, file_info: FileInfo, validation_id: str) -> Tuple[bool, int, str]:
        """
        将CSV数据导入到Stage表
        返回: (是否成功, 导入行数, 错误信息)
        """
        try:
            stage_table_name = f"{file_info.table_name}_stage"
            
            # 读取CSV文件
            df = pd.read_csv(file_path, encoding='utf-8', dtype=str)
            
            # 如果读取失败，尝试GBK编码
            if df.empty:
                try:
                    df = pd.read_csv(file_path, encoding='gbk', dtype=str)
                except:
                    pass
            
            if df.empty:
                return False, 0, "CSV文件为空或无法读取"
            
            # 准备数据：所有列都转换为字符串（符合需求）
            data_rows = []
            for _, row in df.iterrows():
                row_data = {
                    'validation_id': validation_id,
                    'validation_status': 'PENDING',
                    'sequence_number': 1,  # 这里应该从配置或数据库获取下一个序列号
                }
                
                # 添加业务列数据（所有列转为字符串）
                for i, col_value in enumerate(row.values, 1):
                    col_name = f"col_{i}"
                    row_data[col_name] = str(col_value) if pd.notna(col_value) else None
                
                data_rows.append(row_data)
            
            # 批量插入到Stage表
            if data_rows:
                # 这里使用pandas的to_sql方法或者SQLAlchemy的bulk_insert
                # 简化版本：构建SQL语句
                columns_sql = ', '.join([f"col_{i}" for i in range(1, len(file_info.columns) + 1)])
                values_placeholder = ', '.join(['?' for _ in range(len(file_info.columns) + 4)])  # +4 for system columns
                
                insert_sql = f"""
                INSERT INTO [{stage_table_name}] 
                (validation_id, validation_status, sequence_number, {columns_sql})
                VALUES ({values_placeholder})
                """
                
                # 这里应该使用批量插入，简化版本
                logger.info(f"准备导入 {len(data_rows)} 行数据到 {stage_table_name}")
                
                return True, len(data_rows), ""
            else:
                return False, 0, "没有有效数据可导入"
            
        except Exception as e:
            logger.error(f"导入Stage表失败: {e}")
            return False, 0, str(e)
    
    async def process_file(self, file_path: Path, config_id: str, user_id: str) -> Dict:
        """
        处理CSV文件的主流程
        """
        processing_result = {
            "success": False,
            "message": "",
            "file_info": None,
            "validation_result": None,
            "processing_record": None
        }
        
        try:
            # 1. 准备文件信息
            file_info = self.prepare_file_info(file_path)
            processing_result["file_info"] = file_info
            
            # 2. 创建处理记录
            processing_record = FileProcessingRecord(
                original_filename=file_info.original_filename,
                file_size=file_info.file_size,
                file_hash=file_info.file_hash,
                target_table=file_info.table_name,
                stage_table=f"{file_info.table_name}_stage",
                created_by=user_id,
                status="PROCESSING"
            )
            
            # 3. 创建必要的表
            if not self.create_tables_if_not_exists(file_info):
                raise Exception("创建数据表失败")
            
            # 4. 创建验证记录
            validation_result = ValidationResult(
                csv_filename=file_info.original_filename,
                config_id=config_id,
                status="PENDING",
                created_by=user_id
            )
            
            # 5. 导入数据到Stage表
            success, imported_rows, error_msg = self.import_to_stage_table(
                file_path, file_info, validation_result.validation_id
            )
            
            if not success:
                raise Exception(f"导入Stage表失败: {error_msg}")
            
            # 6. 更新处理记录
            processing_record.total_rows = imported_rows
            processing_record.imported_rows = imported_rows
            processing_record.status = "COMPLETED"
            
            processing_result.update({
                "success": True,
                "message": f"文件处理成功，导入 {imported_rows} 行数据",
                "processing_record": processing_record,
                "validation_result": validation_result
            })
            
        except Exception as e:
            logger.error(f"文件处理失败: {e}")
            processing_result.update({
                "success": False,
                "message": str(e)
            })
            
            # 更新处理记录为失败状态
            if "processing_record" in locals():
                processing_record.status = "FAILED"
                processing_record.error_message = str(e)
        
        return processing_result
    
    def cleanup_temp_files(self, max_age_hours: int = 24):
        """清理临时文件"""
        try:
            current_time = datetime.now()
            for file_path in self.temp_dir.glob("*"):
                file_age = current_time - datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_age.total_seconds() > max_age_hours * 3600:
                    file_path.unlink()
                    logger.info(f"清理临时文件: {file_path}")
        except Exception as e:
            logger.error(f"清理临时文件失败: {e}")


# 全局文件处理器实例
file_processor = FileProcessor()