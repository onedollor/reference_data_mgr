# 技术架构设计文档
## CSV数据管理系统技术方案

**版本**: 1.0  
**日期**: 2025-08-04  
**架构师**: 程序员小张  

---

## 1. 技术架构概述

### 1.1 架构原则
- **简单可靠**: 基于成熟技术栈，避免过度复杂
- **单点部署**: 符合需求的单节点SQL Server约束
- **模块化设计**: 清晰的分层架构，便于维护
- **扩展性**: 为未来可能的功能扩展预留空间

### 1.2 整体架构
```
┌─────────────────────────────────────────────┐
│                前端层 (React)                  │
├─────────────────────────────────────────────┤
│                API层 (FastAPI)                │
├─────────────────────────────────────────────┤
│              业务逻辑层 (Python)                │
├─────────────────────────────────────────────┤
│            数据层 (SQL Server)                │
└─────────────────────────────────────────────┘
```

---

## 2. 技术栈选择

### 2.1 后端技术栈

#### 核心框架
- **Python 3.11**: 主要开发语言
- **FastAPI**: Web框架，提供高性能API服务
- **SQLAlchemy**: ORM框架，处理数据库操作
- **Alembic**: 数据库迁移工具

#### 数据库
- **Microsoft SQL Server**: 主数据库（符合需求约束）
- **pyodbc**: SQL Server Python驱动

#### 文件处理
- **pandas**: CSV文件读取和数据处理
- **openpyxl**: Excel文件支持（如需扩展）

#### 验证框架
- **pydantic**: 数据验证和序列化
- **自定义验证模块**: 支持Python Class验证

### 2.2 前端技术栈

#### 核心框架
- **React 18**: 前端UI框架
- **TypeScript**: 类型安全的JavaScript
- **Ant Design**: 企业级UI组件库
- **React Router**: 路由管理

#### 状态管理
- **Zustand**: 轻量级状态管理
- **React Query**: 服务端状态管理

#### 构建工具
- **Vite**: 快速构建工具
- **ESLint + Prettier**: 代码规范

### 2.3 部署和运维
- **Docker**: 容器化部署
- **Nginx**: 反向代理和静态文件服务
- **PM2**: Node.js进程管理（如需）

---

## 3. 数据库设计

### 3.1 核心数据表

#### 系统配置表 (sys_config)
```sql
CREATE TABLE sys_config (
    config_id NVARCHAR(36) PRIMARY KEY,
    csv_filename_pattern NVARCHAR(255) NOT NULL,
    validation_rule NVARCHAR(500),
    target_table_name NVARCHAR(128) NOT NULL,
    stage_table_name NVARCHAR(128) NOT NULL,
    start_date DATETIME2,
    end_date DATETIME2,
    created_by NVARCHAR(50) NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    is_active BIT DEFAULT 1
);
```

#### 用户表 (sys_users)
```sql
CREATE TABLE sys_users (
    user_id NVARCHAR(36) PRIMARY KEY,
    username NVARCHAR(50) UNIQUE NOT NULL,
    email NVARCHAR(100) UNIQUE NOT NULL,
    password_hash NVARCHAR(255) NOT NULL,
    user_group_id NVARCHAR(36),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    is_active BIT DEFAULT 1,
    FOREIGN KEY (user_group_id) REFERENCES sys_user_groups(group_id)
);
```

#### 用户组表 (sys_user_groups)
```sql
CREATE TABLE sys_user_groups (
    group_id NVARCHAR(36) PRIMARY KEY,
    group_name NVARCHAR(50) NOT NULL,
    permissions NVARCHAR(MAX), -- JSON格式权限配置
    description NVARCHAR(255),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE()
);
```

#### 系统日志表 (sys_logs)
```sql
CREATE TABLE sys_logs (
    log_id NVARCHAR(36) PRIMARY KEY,
    operation_type NVARCHAR(50) NOT NULL,
    user_id NVARCHAR(36),
    timestamp DATETIME2 DEFAULT GETDATE(),
    details NVARCHAR(MAX), -- JSON格式详情
    status NVARCHAR(20) DEFAULT 'SUCCESS',
    ip_address NVARCHAR(45),
    user_agent NVARCHAR(500)
);
```

#### 验证结果表 (validation_results)
```sql
CREATE TABLE validation_results (
    validation_id NVARCHAR(36) PRIMARY KEY,
    csv_filename NVARCHAR(255) NOT NULL,
    config_id NVARCHAR(36),
    validation_rule NVARCHAR(500),
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2,
    status NVARCHAR(20) DEFAULT 'PENDING',
    total_records INT DEFAULT 0,
    valid_records INT DEFAULT 0,
    invalid_records INT DEFAULT 0,
    error_details NVARCHAR(MAX), -- JSON格式错误详情
    created_by NVARCHAR(50),
    FOREIGN KEY (config_id) REFERENCES sys_config(config_id)
);
```

### 3.2 动态业务表

#### 业务数据表（动态创建）
```sql
-- 示例：customer_20250104120000
CREATE TABLE {table_name} (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,
    sequence_number INT NOT NULL, -- 文件序列号
    load_timestamp DATETIME2 DEFAULT GETDATE(),
    -- 动态列（全部为NVARCHAR类型）
    col_1 NVARCHAR(MAX),
    col_2 NVARCHAR(MAX),
    col_3 NVARCHAR(MAX),
    -- ... 更多列根据CSV文件动态添加
);
```

#### Stage表（动态创建）
```sql
-- 示例：customer_20250104120000_stage
CREATE TABLE {table_name}_stage (
    stage_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    validation_id NVARCHAR(36), -- 关联验证记录
    validation_status NVARCHAR(20) DEFAULT 'PENDING',
    validation_errors NVARCHAR(MAX),
    -- 与目标表相同的业务列结构
    sequence_number INT NOT NULL,
    load_timestamp DATETIME2 DEFAULT GETDATE(),
    col_1 NVARCHAR(MAX),
    col_2 NVARCHAR(MAX),
    col_3 NVARCHAR(MAX)
);
```

---

## 4. API接口设计

### 4.1 API架构模式
- **RESTful API**: 遵循REST设计原则
- **JSON格式**: 统一使用JSON数据交换
- **HTTP状态码**: 标准状态码表示操作结果
- **统一响应格式**: 包含code、message、data字段

### 4.2 核心API端点

#### 认证相关
```
POST /api/auth/login          # 用户登录
POST /api/auth/logout         # 用户登出  
GET  /api/auth/profile        # 获取用户信息
```

#### 配置管理
```
GET    /api/configs           # 获取配置列表
POST   /api/configs           # 创建新配置
GET    /api/configs/{id}      # 获取配置详情
PUT    /api/configs/{id}      # 更新配置
DELETE /api/configs/{id}      # 删除配置
```

#### 文件上传和处理
```
POST /api/files/upload        # 上传CSV文件
POST /api/files/import        # 执行数据导入
GET  /api/files/status/{id}   # 获取处理状态
```

#### 用户管理
```
GET    /api/users             # 获取用户列表
POST   /api/users             # 创建用户
PUT    /api/users/{id}        # 更新用户
DELETE /api/users/{id}        # 删除用户
GET    /api/user-groups       # 获取用户组列表
POST   /api/user-groups       # 创建用户组
```

#### 日志和报表
```
GET /api/logs                 # 获取日志列表
GET /api/logs/export          # 导出日志
GET /api/reports/dashboard    # 获取仪表板数据
GET /api/reports/statistics   # 获取统计数据
```

### 4.3 API响应格式
```json
{
    "code": 200,
    "message": "操作成功",
    "data": {
        // 具体数据内容
    },
    "timestamp": "2025-08-04T12:00:00Z"
}
```

---

## 5. 业务逻辑架构

### 5.1 核心业务模块

#### 文件处理模块 (FileProcessor)
```python
class FileProcessor:
    def parse_csv_filename(self, filename: str) -> dict
    def validate_file_format(self, file_path: str) -> bool
    def extract_table_info(self, filename: str) -> TableInfo
    def create_dynamic_table(self, table_info: TableInfo) -> bool
```

#### 数据验证模块 (DataValidator)
```python
class DataValidator:
    def validate_with_stored_procedure(self, table_name: str, sp_name: str) -> ValidationResult
    def validate_with_python_class(self, table_name: str, validator_class: str) -> ValidationResult
    def log_validation_result(self, result: ValidationResult) -> bool
```

#### 数据导入模块 (DataImporter)
```python
class DataImporter:
    def import_to_stage_table(self, csv_file: str, stage_table: str) -> bool
    def move_validated_data(self, stage_table: str, target_table: str) -> bool
    def update_sequence_number(self, target_table: str) -> int
```

#### 权限管理模块 (PermissionManager)
```python
class PermissionManager:
    def check_user_permission(self, user_id: str, operation: str) -> bool
    def get_user_group_permissions(self, group_id: str) -> list
    def validate_config_access(self, user_id: str, config_id: str) -> bool
```

### 5.2 验证机制实现

#### Python验证类接口
```python
from abc import ABC, abstractmethod

class BaseValidator(ABC):
    @abstractmethod
    def validate(self, data_frame: pd.DataFrame) -> ValidationResult:
        pass
    
    @abstractmethod
    def get_error_details(self) -> list:
        pass

# 示例验证类
class CustomerDataValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        errors = []
        # 验证逻辑
        if df['email'].isnull().any():
            errors.append("邮箱字段不能为空")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            valid_count=len(df) - len(errors),
            total_count=len(df)
        )
```

#### 存储过程验证接口
```sql
-- 存储过程标准接口
CREATE PROCEDURE sp_validate_customer_data
    @stage_table_name NVARCHAR(128),
    @validation_id NVARCHAR(36),
    @result_code INT OUTPUT,
    @error_message NVARCHAR(MAX) OUTPUT
AS
BEGIN
    -- 验证逻辑
    -- 设置返回结果
    SET @result_code = 0; -- 0=成功, 1=失败
    SET @error_message = ''; -- 错误信息
END
```

---

## 6. 前端架构设计

### 6.1 项目结构
```
src/
├── components/          # 通用组件
│   ├── Layout/         # 布局组件
│   ├── Table/          # 表格组件
│   ├── Upload/         # 文件上传组件
│   └── Charts/         # 图表组件
├── pages/              # 页面组件
│   ├── ConfigManagement/
│   ├── FileUpload/
│   ├── UserManagement/
│   ├── LogViewer/
│   └── Dashboard/
├── services/           # API服务
├── stores/             # 状态管理
├── utils/              # 工具函数
└── types/              # TypeScript类型定义
```

### 6.2 状态管理设计
```typescript
// 全局状态结构
interface AppState {
    user: {
        currentUser: User | null;
        permissions: string[];
    };
    configs: {
        list: Config[];
        loading: boolean;
        selectedConfig: Config | null;
    };
    files: {
        uploadProgress: Record<string, number>;
        processingStatus: Record<string, ProcessStatus>;
    };
    logs: {
        list: LogEntry[];
        filters: LogFilters;
    };
}
```

### 6.3 组件设计原则
- **组件复用**: 抽象通用业务组件
- **Props接口**: 清晰的组件接口定义
- **错误边界**: 优雅的错误处理
- **性能优化**: 使用React.memo和useMemo优化

---

## 7. 部署架构

### 7.1 单节点部署方案
```
服务器环境:
├── Nginx (端口80/443)
│   ├── 静态文件服务
│   └── API反向代理
├── Python后端 (端口8000)
│   ├── FastAPI应用
│   └── 后台任务处理
└── SQL Server (端口1433)
    ├── 业务数据库
    └── 系统配置数据库
```

### 7.2 Docker容器化
```dockerfile
# 后端Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

```dockerfile
# 前端Dockerfile  
FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf
EXPOSE 80
```

### 7.3 docker-compose配置
```yaml
version: '3.8'
services:
  backend:
    build: ./backend
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=mssql://sa:password@sqlserver:1433/csvmanager
    depends_on:
      - sqlserver
  
  frontend:
    build: ./frontend
    ports:
      - "80:80"
    depends_on:
      - backend
  
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2022-latest
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=YourStrong@Passw0rd
    ports:
      - "1433:1433"
    volumes:
      - sqlserver_data:/var/opt/mssql

volumes:
  sqlserver_data:
```

---

## 8. 安全设计

### 8.1 认证和授权
- **JWT Token**: 无状态身份认证
- **密码加密**: bcrypt哈希加密
- **权限控制**: 基于角色的访问控制(RBAC)

### 8.2 数据安全
- **SQL注入防护**: 参数化查询
- **输入验证**: 严格的输入数据验证
- **文件上传安全**: 文件类型和大小限制

### 8.3 传输安全
- **HTTPS**: 强制使用SSL/TLS加密
- **CORS配置**: 跨域请求安全配置
- **请求限流**: 防止API滥用

---

## 9. 性能优化

### 9.1 数据库优化
- **索引策略**: 为查询字段创建合适索引
- **分页查询**: 大数据量分页加载
- **连接池**: 数据库连接池管理

### 9.2 文件处理优化
- **分块读取**: 大文件分块处理
- **异步处理**: 后台异步处理长时间任务
- **临时文件管理**: 及时清理临时文件

### 9.3 前端优化
- **代码分割**: 路由级别的代码分割
- **资源压缩**: 静态资源压缩和缓存
- **虚拟滚动**: 大列表虚拟化处理

---

## 10. 监控和日志

### 10.1 应用监控
- **健康检查**: API健康状态监控
- **性能指标**: 响应时间和错误率监控
- **资源监控**: CPU、内存、磁盘使用率

### 10.2 日志管理
- **结构化日志**: JSON格式日志输出
- **日志级别**: DEBUG/INFO/WARN/ERROR分级
- **日志轮转**: 自动日志文件轮转

### 10.3 错误处理
- **全局异常处理**: 统一的错误处理机制
- **错误报告**: 关键错误自动报告
- **用户友好**: 用户友好的错误提示

---

## 11. 开发计划

### 11.1 阶段一：基础架构 (2周)
- 数据库表结构设计和创建
- 基础API框架搭建
- 认证和权限模块开发
- 前端项目初始化和基础组件

### 11.2 阶段二：核心功能 (4周)
- CSV文件上传和解析功能
- 动态表创建和数据导入
- 验证机制实现(SP和Python类)
- 配置管理界面开发

### 11.3 阶段三：高级功能 (3周)
- 用户管理和权限控制
- 日志系统和报表功能
- 数据可视化组件
- 系统测试和优化

### 11.4 阶段四：部署上线 (1周)
- 生产环境部署
- 性能测试和调优
- 用户培训和文档
- 系统验收

---

## 12. 风险评估和缓解

### 12.1 技术风险
- **SQL Server兼容性**: 充分测试不同版本兼容性
- **大文件处理性能**: 实施分块处理和进度反馈
- **并发处理能力**: 使用异步处理和队列机制

### 12.2 数据风险
- **数据丢失**: 实施定期备份策略
- **数据一致性**: 使用事务保证数据一致性
- **验证逻辑错误**: 完善的测试用例覆盖

### 12.3 安全风险
- **身份认证**: 强密码策略和会话管理
- **权限提升**: 严格的权限验证机制
- **数据泄露**: 敏感数据加密存储

---

**技术架构状态**: 已完成  
**下一步**: 开始系统实现和开发