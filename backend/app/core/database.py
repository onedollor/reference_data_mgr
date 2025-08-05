"""
数据库连接和会话管理
"""

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool
from typing import Generator
import logging

from .config import get_settings

settings = get_settings()

# 创建数据库引擎
engine = create_engine(
    settings.DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=settings.DEBUG
)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建基类
Base = declarative_base()

# 元数据对象
metadata = MetaData()

logger = logging.getLogger(__name__)


def get_db() -> Generator[Session, None, None]:
    """
    获取数据库会话
    """
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"数据库会话错误: {e}")
        db.rollback()
        raise
    finally:
        db.close()


def test_connection() -> bool:
    """
    测试数据库连接
    """
    try:
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"数据库连接测试失败: {e}")
        return False


class DatabaseManager:
    """
    数据库管理器
    """
    
    def __init__(self):
        self.engine = engine
        self.SessionLocal = SessionLocal
    
    def create_session(self) -> Session:
        """创建新的数据库会话"""
        return self.SessionLocal()
    
    def execute_raw_sql(self, sql: str, params: dict = None) -> any:
        """执行原始SQL语句"""
        with self.engine.connect() as connection:
            if params:
                result = connection.execute(sql, params)
            else:
                result = connection.execute(sql)
            return result
    
    def create_table_from_csv_structure(self, table_name: str, columns: list) -> bool:
        """
        根据CSV结构动态创建表
        """
        try:
            # 构建CREATE TABLE语句
            column_definitions = []
            for i, col_name in enumerate(columns, 1):
                # 所有列都是NVARCHAR(MAX)类型（符合需求）
                safe_col_name = f"col_{i}"  # 使用安全的列名
                column_definitions.append(f"{safe_col_name} NVARCHAR(MAX)")
            
            # 添加系统列
            system_columns = [
                "id BIGINT IDENTITY(1,1) PRIMARY KEY",
                "sequence_number INT NOT NULL DEFAULT 1",
                "load_timestamp DATETIME2 DEFAULT GETDATE()",
                "created_at DATETIME2 DEFAULT GETDATE()",
                "updated_at DATETIME2 DEFAULT GETDATE()"
            ]
            
            all_columns = system_columns + column_definitions
            
            create_sql = f"""
            CREATE TABLE [{table_name}] (
                {', '.join(all_columns)}
            );
            
            CREATE INDEX IX_{table_name}_Sequence ON [{table_name}](sequence_number);
            CREATE INDEX IX_{table_name}_LoadTime ON [{table_name}](load_timestamp);
            """
            
            # 创建对应的Stage表
            stage_table_name = f"{table_name}_stage"
            stage_columns = [
                "stage_id BIGINT IDENTITY(1,1) PRIMARY KEY",
                "validation_id NVARCHAR(36)",
                "validation_status NVARCHAR(20) DEFAULT 'PENDING'",
                "validation_errors NVARCHAR(MAX)",
                "sequence_number INT NOT NULL DEFAULT 1",
                "load_timestamp DATETIME2 DEFAULT GETDATE()",
                "created_at DATETIME2 DEFAULT GETDATE()"
            ] + column_definitions
            
            stage_sql = f"""
            CREATE TABLE [{stage_table_name}] (
                {', '.join(stage_columns)}
            );
            
            CREATE INDEX IX_{stage_table_name}_ValidationId ON [{stage_table_name}](validation_id);
            CREATE INDEX IX_{stage_table_name}_Status ON [{stage_table_name}](validation_status);
            """
            
            # 执行创建语句
            with self.engine.connect() as connection:
                connection.execute(create_sql)
                connection.execute(stage_sql)
                connection.commit()
            
            logger.info(f"成功创建表: {table_name} 和 {stage_table_name}")
            return True
            
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            return False
    
    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        try:
            with self.engine.connect() as connection:
                result = connection.execute(
                    "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                    (table_name,)
                )
                return result.scalar() > 0
        except Exception as e:
            logger.error(f"检查表存在性失败: {e}")
            return False


# 全局数据库管理器实例
db_manager = DatabaseManager()