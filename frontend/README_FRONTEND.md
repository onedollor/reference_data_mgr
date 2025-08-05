# CSV数据管理系统 - 前端应用

基于PRD.md需求文档实现的企业级CSV数据管理系统前端应用。

## 🚀 快速启动

### 使用Python启动器（推荐）
```bash
# 进入前端目录
cd frontend

# 启动开发服务器
python main.py

# 或使用npm脚本
npm start
```

### 传统Node.js方式
```bash
# 安装依赖
npm install

# 启动开发服务器
npm run dev

# 构建生产版本
npm run build

# 启动生产服务器
npm run serve
```

## 📋 启动器命令

```bash
# 查看帮助
python main.py help

# 启动开发服务器（默认）
python main.py dev

# 自定义端口启动
python main.py dev --port 3000

# 指定API地址
python main.py dev --api-url http://localhost:8080

# 不自动打开浏览器
python main.py dev --no-browser

# 安装依赖
python main.py install

# 构建生产版本
python main.py build

# 启动生产服务器
python main.py prod
```

## 🏗️ 项目结构（基于PRD需求）

```
frontend/
├── main.py                 # Python启动器
├── app.config.json         # 应用配置文件
├── package.json           # Node.js依赖配置
├── vite.config.ts         # Vite构建配置
├── tsconfig.json          # TypeScript配置
├── src/
│   ├── App.tsx            # 主应用组件
│   ├── main.tsx           # 应用入口
│   ├── components/        # 通用组件
│   │   ├── Layout/        # 布局组件
│   │   ├── Upload/        # 文件上传组件
│   │   ├── Table/         # 数据表格组件
│   │   └── Charts/        # 图表组件
│   ├── pages/             # 页面组件
│   │   ├── Dashboard/     # 数据统计页面
│   │   ├── ConfigManagement/ # 配置管理页面
│   │   ├── FileUpload/    # 文件上传页面
│   │   ├── UserManagement/ # 用户管理页面
│   │   ├── LogViewer/     # 日志浏览页面
│   │   └── Login/         # 登录页面
│   ├── services/          # API服务
│   ├── stores/            # 状态管理
│   ├── types/             # TypeScript类型定义
│   ├── utils/             # 工具函数
│   └── styles/            # 样式文件
└── public/                # 静态资源
```

## 🎯 功能特性（基于PRD.md）

### 核心页面
- ✅ **配置管理页面** - 管理CSV导入配置和验证规则
- ✅ **文件上传页面** - CSV文件上传、预览和批量导入
- ✅ **用户管理页面** - 用户账户和用户组权限管理
- ✅ **日志浏览页面** - 系统日志和验证记录查看
- ✅ **数据可视化页面** - 导入统计和趋势分析

### 业务功能
- **CSV文件格式验证**: 严格的文件名格式 `文件名.yyyyMMddHHmmss.csv`
- **两阶段数据加载**: Stage表 → Validation → 正式表流程可视化
- **实时进度显示**: 文件上传和处理进度实时更新
- **权限控制**: 基于用户组的功能访问控制
- **错误处理**: 友好的错误提示和处理建议

### 技术特性
- **响应式设计**: 支持桌面端、平板、手机多设备
- **企业级UI**: Ant Design组件库，专业视觉设计
- **TypeScript**: 类型安全的开发体验
- **状态管理**: Zustand + React Query
- **实时更新**: WebSocket支持（规划中）
- **国际化**: 支持中英文切换（规划中）

## ⚙️ 配置说明

### 环境变量（.env文件）
```bash
# 应用基本信息
VITE_APP_TITLE=CSV数据管理系统
VITE_APP_VERSION=1.0.0
VITE_API_URL=http://localhost:8000

# 功能开关
VITE_ENABLE_CONFIG_MANAGEMENT=true
VITE_ENABLE_FILE_UPLOAD=true
VITE_ENABLE_USER_MANAGEMENT=true
VITE_ENABLE_LOG_VIEWER=true
VITE_ENABLE_DATA_VISUALIZATION=true

# CSV文件配置
VITE_CSV_FILE_PATTERN=*.yyyyMMddHHmmss.csv
VITE_MAX_FILE_SIZE=104857600
VITE_ALLOWED_FILE_TYPES=.csv

# UI主题配置
VITE_PRIMARY_COLOR=#2563eb
VITE_SUCCESS_COLOR=#059669
VITE_WARNING_COLOR=#d97706
VITE_ERROR_COLOR=#dc2626
```

### 应用配置（app.config.json）
- **功能模块配置**: 各页面的路由、权限、描述
- **UI主题配置**: 颜色、布局、组件样式
- **API端点配置**: 后端接口地址映射
- **业务规则配置**: 验证规则、用户角色、数据流程
- **性能配置**: 分页、上传、轮询参数

## 🌐 访问地址

启动成功后可通过以下地址访问：

- **开发环境**: http://localhost:5173
- **生产环境**: http://localhost:5173 (构建后)
- **API文档**: http://localhost:8000/api/docs

## 🔧 开发指南

### 添加新页面
1. 在 `src/pages/` 创建页面组件
2. 在 `src/App.tsx` 添加路由配置
3. 在 `app.config.json` 添加功能配置
4. 更新侧边导航菜单

### 添加新API服务
1. 在 `src/services/` 创建API服务文件
2. 在 `app.config.json` 添加端点配置
3. 使用React Query进行状态管理

### 自定义主题
1. 修改 `app.config.json` 中的UI配置
2. 更新CSS变量或Less变量
3. 重启开发服务器查看效果

## 🐛 故障排除

### 常见问题

1. **Node.js版本问题**
   ```bash
   # 检查Node.js版本（需要18+）
   node --version
   
   # 安装Node.js LTS版本
   # 从 https://nodejs.org 下载安装
   ```

2. **依赖安装失败**
   ```bash
   # 清理缓存
   npm cache clean --force
   
   # 删除node_modules重新安装
   rm -rf node_modules package-lock.json
   npm install
   ```

3. **端口被占用**
   ```bash
   # 使用不同端口启动
   python main.py dev --port 3000
   
   # 或者杀死占用进程
   # Windows: netstat -ano | findstr :5173
   # Linux/Mac: lsof -ti :5173 | xargs kill
   ```

4. **API连接失败**
   - 确保后端服务已启动（http://localhost:8000）
   - 检查防火墙设置
   - 验证API地址配置

### 调试模式
```bash
# 启动时显示详细日志
python main.py dev --verbose

# 检查构建产物
python main.py build --analyze
```

## 📚 相关文档

- [产品需求文档 (PRD)](../PRD.md)
- [技术架构文档](../Technical_Architecture.md)
- [UI设计规范](../UI_Design_Specification.md)
- [后端API文档](../backend/README.md)

---

**版本**: 1.0.0  
**最后更新**: 2025-08-04  
**技术栈**: React + TypeScript + Ant Design + Vite