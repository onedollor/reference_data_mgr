# CLAUDE.md

本文件为Claude Code (claude.ai/code) 在此代码库中工作时提供指导。

## 项目概述

这是一个三角色AI协作系统，包含产品经理、设计师、程序员三个AI角色的协作框架。用于演示企业级参考数据管理系统的开发流程。

## 项目结构

```
reference_data_mgr/
├── agents/
│   ├── __init__.py          # 模块初始化
│   ├── base_agent.py        # 基础Agent类和任务模型
│   ├── product_manager.py   # 产品经理Agent
│   ├── designer.py          # 设计师Agent
│   ├── programmer.py        # 程序员Agent
│   └── coordinator.py       # Agent协调器
├── main.py                  # 主程序演示文件
├── requirements.txt         # Python依赖
└── CLAUDE.md               # 本文档
```

## 技术栈

- **语言**: Python 3.11+
- **核心架构**: 面向对象的Agent协作系统
- **依赖管理**: pip + requirements.txt
- **主要库**: dataclasses, enum, typing, json, uuid, datetime

## 常用开发命令

### 环境设置
```bash
# 安装依赖
pip install -r requirements.txt

# 运行主程序演示
python main.py
```

### 使用方式

#### 1. 基本使用
```python
from agents import AgentCoordinator

# 创建协调器
coordinator = AgentCoordinator()

# 启动项目协作
result = coordinator.start_project("项目描述")

# 执行任务
task_result = coordinator.execute_task(task_id)

# 查看项目状态
status = coordinator.get_project_status()
```

#### 2. 促进协作会议
```python
# 进行需求评审
meeting = coordinator.facilitate_collaboration(
    collaboration_type="需求评审",
    participants=["产品经理", "设计师", "程序员"],
    topic="具体讨论议题"
)
```

## 核心架构

### Agent配置管理

**配置文件位置**: `AGENT_CONFIG.md`
- 所有Agent的角色定义、能力配置、技术栈选择都在此文件中
- 支持动态加载配置，无需修改Python代码
- 包含协作流程、质量标准等完整配置

**配置加载器**: `agents/config_loader.py`
- 自动解析Markdown配置文件
- 提取JSON配置块和能力列表
- 为各Agent提供配置信息

### Agent角色能力

**产品经理Agent (ProductManagerAgent)**:
- 能力从 `AGENT_CONFIG.md` 动态加载
- 包含需求分析、用户故事编写、产品规划等
- 支持自定义处理流程和优先级设置

**设计师Agent (DesignerAgent)**:
- 能力和设计系统从配置文件加载
- 包含UI/UX设计、原型制作、设计规范制定等
- 支持自定义色彩方案、字体系统、组件规范

**程序员Agent (ProgrammerAgent)**:
- 能力和技术栈从配置文件加载
- 包含前后端开发、数据库设计、API开发等
- 支持自定义技术选型和架构设计模式

### 协作流程

1. **项目启动**: 三个角色分析项目需求
2. **任务生成**: 基于分析结果生成协作任务
3. **任务分配**: 将任务分配给对应角色
4. **任务执行**: 各角色执行任务并生成交付物
5. **协作会议**: 定期进行跨角色协作讨论

### 任务管理

- **任务状态**: PENDING → IN_PROGRESS → COMPLETED
- **依赖管理**: 支持任务间依赖关系
- **交付物跟踪**: 每个任务关联具体交付物

## 扩展开发

### 添加新Agent角色
1. 继承 `BaseAgent` 类
2. 实现 `process_request` 和 `generate_deliverable` 方法
3. 在 `AgentCoordinator` 中注册新角色

### 添加新协作类型
在 `AgentCoordinator.facilitate_collaboration` 中添加新的协作类型处理逻辑

## Git工作流程

- 主分支：`main`
- 提交信息使用中文，描述清晰具体的更改内容