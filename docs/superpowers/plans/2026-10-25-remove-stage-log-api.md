# Remove Stage Log API Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 按用户最新决定删除旧构建阶段日志接口及重复的日志存储，仅保留 dbt 详细日志接口。
**Architecture:** runtime_job 的 phase/status/error_code 是执行事实，engine_sync_job 继续同步 engine_build 供构建详情读取；移除 engine_change、计数器及生成变化日志的触发器。Database.fact_session/fact_transaction 改用 schema 级事务 advisory lock 保留既有串行约束，避免为删除日志而重写业务事务。
**Tech Stack:** FastAPI、Pydantic、SQLAlchemy、PostgreSQL。
**Spec:** 用户明确要求阶段、状态、错误码属于执行本身，删除旧 /logs；此决定替代前两份日志设计中的旧接口兼容要求。

## Constraints and review focus

- 保留 /builds/{buildId} 状态详情与 /execution-logs，不新增执行查询接口。
- 删除旧 /logs 路由、DTO、应用/存储查询、专用表和生成触发器。
- 保留状态同步、租约、取消、部署并发约束；不修改用户 vendor 工作。
- 提供已有库清理 SQL，声明会删除旧阶段历史；不自动执行真实业务库删除。

## Tasks

- [x] RED：旧接口 404、详情反映执行阶段/失败且不生成伪日志、新库不建阶段日志表。
- [x] 删除旧链路，fact transaction 改为独立事务锁；保留 engine_sync_job。
- [x] 调整原阶段日志测试及使用旧计数器制造约束错误的测试，增加锁提交/回滚释放和迁移保留事实测试。
- [x] 更新接入文档与数据库文档，新增显式清理 SQL。
- [x] 定向测试、完整回归、Ruff、独立评审并记录结果。

## 验证结果

- RED：旧接口仍返回 200、新库仍创建阶段日志表；状态详情测试证明执行事实本来就可读取。清理脚本测试在文件缺失时失败。
- GREEN：48 项定向测试通过，覆盖旧接口 404、新库无旧表、清理脚本幂等且保留执行事实/详细日志、原有状态同步、事实锁提交及回滚释放。
- 独立评审未发现功能问题；已修正文档中的旧计数器锁说明以及集成测试失败诊断里的旧接口引用。
- 完整 pytest：558 passed、4 skipped、2 failed、23 warnings，101.11 秒。测试使用独立临时 schema，结束后已删除。
- 两个失败仍为未修改的 tests/test_dependencies.py 中 test_vendored_sources_are_at_audited_commits（vendor 提交与固定值不符）及 test_core_packages_are_installed_from_vendored_sources（editable 安装与预期不符）。未调整用户依赖或 vendor 状态。
- Ruff `check src tests` 和 `git diff --check` 通过。未对现有业务数据库执行清理 SQL。
