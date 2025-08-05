# CSV数据管理系统

企业级CSV数据导入、验证和管理系统，支持两阶段数据加载和灵活的验证机制。

## 🚀 功能特性

### 核心功能
- **CSV文件导入**: 严格的文件命名格式 `文件名.yyyyMMddHHmmss.csv`
- **两阶段数据加载**: Stage表 → Validation → 正式表
- **灵活验证机制**: 支持存储过程和Python类验证
- **动态表结构**: 根据CSV文件自动创建数据表
- **版本控制**: 支持数据版本管理和序列号追踪
- **用户权限管理**: 基于用户组的权限控制
- **完整日志系统**: 操作日志和验证结果追踪
- **数据可视化**: 导入统计和趋势分析

### 技术特性
- **单节点部署**: 符合SQL Server单节点要求
- **高性能处理**: 支持100MB大文件处理  
- **企业级UI**: 基于Ant Design的专业界面
- **容器化部署**: Docker Compose一键部署
- **RESTful API**: 标准化的API接口

## 🏗️ 系统架构

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

### 技术栈
- **后端**: Python 3.11 + FastAPI + SQLAlchemy
- **前端**: React 18 + TypeScript + Ant Design
- **数据库**: Microsoft SQL Server 2022
- **部署**: Docker + Nginx
- **工具**: pandas, pyodbc, React Query, Zustand

## 📋 项目结构

```
reference_data_mgr/
├── agents/                  # AI协作代理
├── backend/                 # 后端服务
│   ├── app/
│   │   ├── api/            # API路由
│   │   ├── core/           # 核心配置
│   │   ├── models/         # 数据模型
│   │   ├── services/       # 业务服务
│   │   └── middleware/     # 中间件
│   ├── Dockerfile
│   ├── requirements.txt
│   └── main.py
├── frontend/               # 前端应用
│   ├── src/
│   │   ├── components/     # 通用组件
│   │   ├── pages/          # 页面组件
│   │   ├── stores/         # 状态管理
│   │   └── services/       # API服务
│   ├── Dockerfile
│   └── package.json
├── database/               # 数据库脚本
│   └── create_tables.sql
├── docker-compose.yml      # Docker编排
├── PRD.md                 # 产品需求文档
├── Technical_Architecture.md # 技术架构文档
├── UI_Design_Specification.md # UI设计规范
└── CLAUDE.md              # AI协作指南
```

## 🚀 快速开始

### 环境要求
- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ 可用内存
- 10GB+ 可用磁盘空间

### 一键部署
```bash
# 克隆项目
git clone <repository-url>
cd reference_data_mgr

# 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f
```

### 访问系统
- **前端界面**: http://localhost
- **API文档**: http://localhost:8000/api/docs
- **数据库**: localhost:1433 (Windows认证)

### 默认账户
- **用户名**: admin
- **密码**: admin123
- **权限**: 系统管理员

## 📊 核心页面

### 1. 数据统计面板
- 导入数据量趋势图
- 验证成功率统计
- 用户活跃度分析
- 错误类型分布

### 2. 配置管理
- CSV文件名模式配置
- 验证规则设置
- 表映射关系管理
- 配置启用/禁用

### 3. 文件上传
- 拖拽上传CSV文件
- 批量文件处理
- 实时处理进度
- 导入结果反馈

### 4. 用户管理
- 用户账户管理
- 用户组权限配置
- 权限分配控制

### 5. 日志浏览
- 系统操作日志
- 数据验证记录
- 错误详情查看
- 日志导出功能

## 🔧 开发指南

### 后端开发
```bash
cd backend

# 安装依赖
pip install -r requirements.txt

# 启动开发服务器
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 前端开发
```bash
cd frontend

# 使用Python启动器（推荐）
python main.py

# 或传统方式
npm install
npm run dev

# 查看启动器帮助
python main.py help
```

### 数据库初始化
```bash
# 连接SQL Server并执行初始化脚本
sqlcmd -S localhost -U sa -P "YourStrong@Passw0rd123" -i database/create_tables.sql
```

## 📝 配置说明

### 环境变量
```bash
# 后端配置 - Windows认证
DATABASE_URL=mssql+pyodbc://localhost:1433/CSVDataManager?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes
SECRET_KEY=your-secret-key
DEBUG=true
MAX_FILE_SIZE=104857600  # 100MB

# 前端配置
REACT_APP_API_URL=http://localhost:8000
```

### CSV文件格式要求
- **文件命名**: `文件名.yyyyMMddHHmmss.csv`
- **数据格式**: 所有列均为字符串类型
- **编码格式**: UTF-8或GBK
- **最大文件**: 100MB

## 🔍 验证机制

### 存储过程验证
```sql
CREATE PROCEDURE sp_validate_customer_data
    @stage_table_name NVARCHAR(128),
    @validation_id NVARCHAR(36),
    @result_code INT OUTPUT,
    @error_message NVARCHAR(MAX) OUTPUT
AS
BEGIN
    -- 验证逻辑
END
```

### Python类验证
```python
class CustomerDataValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        # 验证逻辑
        return ValidationResult(...)
```

## 📈 监控与运维

### 健康检查
- 前端: http://localhost/health
- 后端: http://localhost:8000/health
- 数据库: SQL Server 连接测试

### 日志管理
- 应用日志: `logs/app.log`
- 访问日志: `logs/nginx/access.log`
- 错误日志: `logs/nginx/error.log`

### 备份策略
- 数据库自动备份: 每日凌晨2点
- 文件数据备份: 每周备份上传文件
- 配置备份: 配置变更时自动备份

## 🛡️ 安全特性

- **身份认证**: JWT Token认证
- **权限控制**: 基于角色的访问控制(RBAC)
- **数据加密**: 密码bcrypt加密
- **SQL注入防护**: 参数化查询
- **文件安全**: 类型和大小限制
- **传输安全**: HTTPS加密传输

## 🐛 故障排除

### 常见问题

1. **数据库连接失败**
   ```bash
   # 检查SQL Server状态
   docker-compose logs sqlserver
   
   # 测试连接
   docker-compose exec backend python -c "from app.core.database import test_connection; print(test_connection())"
   ```

2. **文件上传失败**
   - 检查文件名格式是否正确
   - 确认文件大小不超过100MB
   - 验证文件编码格式

3. **验证失败**
   - 查看验证日志详情
   - 检查验证规则配置
   - 确认数据格式符合要求

### 性能优化
- 数据库索引优化
- 大文件分块处理
- 缓存策略配置
- 连接池参数调优

## 📚 相关文档

- [产品需求文档 (PRD)](./PRD.md)
- [技术架构文档](./Technical_Architecture.md)  
- [UI设计规范](./UI_Design_Specification.md)
- [AI协作指南](./CLAUDE.md)

## 🤝 开发团队

本项目由三个AI角色协作开发：
- **产品经理**: 需求分析和产品规划
- **设计师**: UI/UX设计和用户体验
- **程序员**: 技术架构和系统实现

## 📄 许可证

本项目仅供学习和演示使用。

## 🔄 版本历史

- **v1.0.0** (2025-08-04): 初始版本发布
  - 完整的CSV数据管理功能
  - 企业级UI界面
  - Docker容器化部署
  - 完整的文档体系