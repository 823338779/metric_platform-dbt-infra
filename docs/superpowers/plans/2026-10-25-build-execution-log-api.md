# Build Execution Log API Implementation Plan

> 历史执行记录：用户随后选择删除旧 /logs，兼容性要求已由 [移除方案](2026-10-25-remove-stage-log-api.md) 替代。

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 提供前端指标平台可续读的结构化构建日志 API。
**Architecture:** 新路由调用 BuildService，校验构建存在后由 ExecutionLogStore 有界查询；DTO 使用 camelCase。写入增加构建级事务锁，确保本构建日志游标不会越过未提交批次。
**Tech Stack:** FastAPI、Pydantic、SQLAlchemy、PostgreSQL。
**Spec:** docs/superpowers/specs/2026-10-25-build-execution-log-api-design.md

## Constraints and review focus

- 用户明确保留旧 /logs；不改变旧接口返回值。
- 不改 vendor、不部署到现有业务数据库、不创建前端工程。
- JS ID 精度、空页轮询、跨构建游标、并发提交顺序、日志消息作为文本显示。

## Tasks

- [x] 新增 tests/test_execution_log_api.py：真实 PostgreSQL 与 HTTP 测试，验证当前缺少路由而失败。
- [x] 增加 models/builds.py 的 ExecutionLogView / ExecutionLogPage、storage/records.py 的 StoredExecutionLog；storage/execution_logs.py 增加 list(build_id, cursor, limit) 返回快照、游标及 has_more。
- [x] 在 BuildService、api/builds.py、constants.py 接入新路由，复用异常处理。
- [x] 增加同构建并发批次读取回归，验证 RED 后在 append 中增加日志专用事务锁。
- [x] 编写前端接入示例及 README 路由说明。
- [x] 执行新测试、Ruff、完整回归和独立评审，记录既有环境失败。

## 验证结果

- 新增 14 个接口用例，覆盖真实 dbt parse → PostgreSQL → HTTP 链路、空页轮询、跨构建隔离、旧接口兼容、非法游标/页大小、bigint 精度及 UTC 输出。
- 并发回归先复现只读到第二条日志、漏掉迟提交第一条的失败；增加构建级日志事务锁后通过。
- 数据库使用 Asia/Shanghai 时先复现响应偏移未统一的失败；应用层明确转换为 UTC 后通过。
- 完整 pytest 在独立临时 schema 中运行：554 passed、4 skipped、2 failed、23 warnings，100.53 秒；测试结束后删除临时 schema，未修改现有业务库。
- 两个失败与前一轮相同，均在 tests/test_dependencies.py：test_vendored_sources_are_at_audited_commits（vendor/dbt 提交与测试固定值不一致）、test_core_packages_are_installed_from_vendored_sources（当前 editable 安装与测试要求不一致）。未改动用户 vendor 状态或依赖。
- Ruff `check src tests`、`git diff --check` 通过；独立评审无需要修复的问题。
