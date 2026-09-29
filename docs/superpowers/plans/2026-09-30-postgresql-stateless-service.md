# PostgreSQL 无状态服务实施计划

> 执行方式：superpowers:subagent-driven-development；在现有工作区实施，不创建 worktree。用户已确认设计并要求实施。

**目标：** PostgreSQL 保存所有接口任务与原生产物，服务实例的工作目录可以丢弃。
**架构：** 六张业务表、短事务队列、带 token 的执行租约；服务还原项目后调用原有 dbt/MetricFlow 入口。启用 SERVICE_DATABASE_URL 时选择持久运行时，旧本地模式保留兼容测试和离线开发。
**技术栈：** Python、FastAPI、psycopg2、PostgreSQL；不修改 vendor。
**设计：** ../specs/2026-09-30-postgresql-stateless-service-design.md

## 共同约束

- 临时 resources 原文仅驻内存，节点失联后 INPUT_LOST，调用方重提。
- 所有公开成功响应必须在数据库提交之后；失效租约不得提交结果。
- 构建外部写入未知时不自动重跑；未确认结束的 attempt 阻止清理。
- PostgreSQL 配置通过环境变量，不提交凭据；迁移由管理命令执行。
- 每个新增字段和代码逻辑块有注释；字符串复用常量；保留现有测试。
- 一份计划全部完成后仅提交本次修改，保留工作区原有修改及 vendor 指针。

## 审查重点

1. 返回 202 后尚未领取就丢失 VOLATILE 输入：输入租约过期可终结。
2. 旧 worker 在恢复后提交：token 和数据库时间阻止迟到提交。
3. 清理与查询同时受理：共同锁定父任务，失联 attempt 继续阻止删除。
4. 产物路径逃逸、损坏、超限：拒绝还原与发布，不读写工作目录之外。
5. PostgreSQL 中断、提交响应丢失：不产生假成功，不盲目重复仓库写入。

## 任务 1：PostgreSQL 任务仓储

文件：新增 storage/postgres.py、storage/jobs.py、storage/migrations/001_runtime.sql 和 tests/test_runtime_storage.py。
接口：Database(dsn).transaction() 返回 RealDictCursor；migrate()/check()/close()。
JobStore 提供受理、查找、认领、心跳、阶段、成功/失败提交、恢复、清理、项目注册；具体签名随实现固定并通知集成者。

- [x] 先写真实 PG 测试，验证并发幂等、领取、过期 token、INPUT_LOST、清理门禁失败。
- [x] 实现带注释的六表 schema 和事务仓储，运行相同测试通过。

## 任务 2：产物与工作目录

文件：新增 storage/artifacts.py、workspace.py、tests/test_runtime_artifacts.py。
接口：ArtifactStore(Database).capture(project_id, directory, *, producer_attempt_id=None, kind='SOURCE') -> str；materialize(set_id, destination)；metadata(set_id)；seal(set_id, cursor) 供完成事务调用。

- [x] 先验证字节摘要一致、非法路径拒绝、不可变集合及容量限制。
- [x] 实现有界逐文件保存、还原、发布与回收；排除凭据和临时文件。

## 任务 3：执行器与租约

文件：新增 worker.py、runtime_execution.py、tests/test_runtime_worker.py。
接口：Worker(runtime).start()/close()；RuntimeExecutor 执行已认领任务，沿用 CommandSpec 和 JobRunner 管理进程树；全部外部命令在持久外部执行标记之后运行。

- [x] 先验证持久任务跨实例执行、心跳丢失取消、只读安全重试与未知写入失败。
- [x] 实现构建、查询、选项、清理和通用 CLI；每次独立还原，提交时核对 token。

## 任务 4：API、管理迁移与文档

文件：新增 runtime.py、admin.py；修改 api.py、settings.py、pyproject.toml、uv.lock、README.md；新增 tests/test_runtime_api.py、tests/test_runtime_migration.py。

- [x] 先验证两个实例读取同一结果、resources 不入库、错误响应保持契约。
- [x] 接入全部接口、健康检查、管理 migrate/import-project/import-legacy 命令。
- [x] 旧 UUID 幂等导入，不重建 schema；导入前校验产物，不修改旧库和文件。
- [x] 文档记录配置、运行、备份迁移、故障处置及兼容边界。

## 任务 5：整体验证与提交

- [x] 运行 README 支持的 pytest 和 ruff 命令，报告已有基线失败。
- [x] 使用真实 PostgreSQL、独立临时目录/进程完成构建查询和故障测试。
- [x] 独立代码审查后修复关键问题并回归。
- [x] 检查根仓库与子仓库状态，确认 vendor 未被本次修改，仅提交本计划相关变更。

## 完成与验证记录

- PostgreSQL 集成测试覆盖幂等、租约失效、临时输入丢失、产物完整性、迁移与清理保护。
- 最终全量测试：255 passed、3 skipped、1 failed。唯一失败为既有 MetricFlow vendor HEAD 与 test_dependencies.py 中固定审计提交不一致；本次未改动 vendor 源码或修改该测试。
- 真实双 HTTP 进程验收通过：独立临时目录、dbt build、停止受理实例后的查询、选项及清理。
- 已有 StarRocks 发布版本导入专用测试库后，从空临时目录还原查询成功：96 行、2 列，原 runId 保留。业务服务尚未切换到 PostgreSQL 模式。
- 本轮新增/修改代码通过 ruff；wheel 构建通过，包内包含迁移 SQL 和管理命令入口。
- 仓储、执行器和集成代码经过子代理交叉审查；已修复项目版本竞争、迁移幂等、失败任务停止保护和清理后文件引用问题。收尾改动由主代理复核并完成回归。
- 实施期间出现外部提交 0021ac8，已包含主体实现；后续表命名空间改动归其他进行中的工作，本轮提交仅包含无状态迁移与恢复相关的收尾修改。
