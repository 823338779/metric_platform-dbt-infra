# 固定提交服务轻量化实施计划

**Goal:** 将服务收敛为 PostgreSQL 上的固定提交验证、全量构建发布与查询服务。

**Architecture:** 保留单进程 API/worker、持久租约和不可变产物；拆分发布、目录、查询边界，停止 Git 生命周期管理及兼容执行路径。

**Tech Stack:** Python、FastAPI、PostgreSQL、SQLAlchemy Core、dbt、MetricFlow。

**Spec:** `docs/superpowers/specs/2026-10-09-service-slimming-design.md`

## 全局约束

- 不修改 vendor、历史迁移和已有数据库内容。
- 请求只指定固定 SHA 与幂等键，连接配置属于服务。
- 发布原子性、查询版本固定、租约、输出上限继续有效。
- API 与 worker 同进程，不增加新部署组件。

## Review Focus

- 并发同键提交与绑定变更：恢复原身份或冲突，不混用输入。
- 非 main SHA 与 Git ref 移动：读取精确提交，不隐式读最新代码。
- 旧发布带复用绑定：仍可读、仍保护创建者物理对象。
- 发布中途失效/封存失败：旧活动指针与任务状态保持事务一致。
- 已删除入口的导入与配置引用：应用可启动、测试可收集、文档不再引导旧流程。

## Task 1：固定提交与唯一运行模式

- [ ] 增加固定 SHA、幂等、PostgreSQL 必需与旧路由退出测试，运行确认失败。
- [ ] 修改 bindings、请求模型、发布/验证受理、应用装配和管理 publish 命令。
- [ ] 删除 Git 分支事件/生命周期入口及 worker 扫描，保留历史存储结构。
- [ ] 运行契约与 Git fixture 测试。

## Task 2：全量构建与业务边界

- [ ] 将全量构建断言加入发布构建测试，运行确认旧选择性流程失败。
- [ ] 删除构建差异计划及新对象复用路径，保留历史绑定解码。
- [ ] 拆分 CatalogService、QueryService；将 SQL 与原子受理移入存储方法。
- [ ] 将发布完成编排移出 JobStore，验证封存/指针/任务终态原子性。

## Task 3：退出旧路径并验收

- [ ] 删除 SQLite coordinator、通用 CLI API 与内存 YAML adapter 路径及专属测试。
- [ ] 保留仍被执行器使用的命令 runner 与纯函数，更新 fixture/import。
- [ ] 更新 README、配置项和协议 fixture。
- [ ] 运行完整 pytest 与 ruff，修复相关回归并记录跳过条件。

## 决策记录

- 用户已明确授权五项调整及旧接口下线，本计划在当前会话直接执行，不重复请求同一范围授权。
- 历史 branch 数据结构保留只为原记录和外键兼容；新运行不扫描或管理 Git 分支。
