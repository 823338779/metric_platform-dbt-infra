# dbt Execution Logs Implementation Plan

> 历史执行记录：保留旧阶段日志的要求已由用户后续提出的 [移除方案](2026-10-25-remove-stage-log-api.md) 替代。

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 service 发起的 dbt 运行事件持久化到独立 PostgreSQL 表。

**Architecture:** execution 层负责结构化事件与脱敏，runtime 层负责有界后台队列及任务上下文，storage 层负责短事务写库。复用既有执行边界，不修改各业务命令调用。

**Tech Stack:** Python、dbtRunner callbacks、threading/queue、SQLAlchemy、PostgreSQL。

**Spec:** docs/superpowers/specs/2026-10-25-dbt-execution-logs-design.md

## Global Constraints

- 保留 engine_change 和现有输出尾部行为；不改 vendor。
- 不向库写入 DBT_ENV_SECRET_* 明文；所有新增文本字段走统一脱敏。
- 写库错误不影响 dbt 结果，不在回调内执行数据库操作。
- 使用现有工作目录及依赖执行，避免遗漏本地 vendor 修改；不提交或部署用户工作。

## Review Focus

- dbt DEBUG 事件不能受 Python INFO 日志阈值影响。
- dotenv 运行期间新增秘密及节点字段不得泄漏。
- 运行成功、失败、取消后日志均刷新，无后台线程遗留。
- 队列满和数据库断连必须告警，不能无限增长或改变命令结果。
- 现有库需显式升级，升级后 ORM 与初始化 SQL 保持一致。

## Task 1: 结构化采集

- [x] 在 tests/test_execution_logs.py 添加真实事件、动态脱敏和尾部限长独立性测试，运行并确认缺失接收器导致失败。
- [x] 新建 execution/logs.py 的 EngineLog，不依赖数据库；扩展 execution/dbt.py 的可选 log_sink 参数，embedded.py 将 Python 日志与事件统一脱敏分发。
- [x] 运行新测试及 tests/test_embedded_engine.py。

## Task 2: 存储与运行时接入

- [x] 添加 PostgreSQL 持久化、任务隔离及队列故障测试，先验证失败。
- [x] 新建 storage/execution_logs.py、runtime/execution_logs.py，增加 entities.py 映射、schema.sql 建表及 add_execution_log.sql 增量建表。
- [x] 仅在 RuntimeExecutor._dbt 绑定上下文，finally 异步等待日志排空；更新相关测试替身。
- [x] 在独立测试 schema 验证新库及增量脚本，运行相关回归。

## Task 3: 文档和最终验证

- [x] 更新 storage/README.md 说明日志字段、查询 SQL、升级方式和丢失边界。
- [x] 执行 Ruff、完整 pytest；记录失败及环境限制。
- [x] 检查 diff 与最终评审，仅修复本次问题并交付代码及验证结果。

## 执行结果

- 基线：既有 embedded engine 测试 34 passed。
- RED：新增采集测试因缺少 log_sink 失败；队列测试因缺少模块失败；真实 PostgreSQL 测试因缺少 runtime_execution_log 失败。
- GREEN：新增 11 个测试通过，含真实 dbt parse/build、取消末条事件落库、队列满、数据库错误隔离、动态秘密脱敏及增量建表；目标回归 67 passed。
- 最终 Ruff `check src tests` 和 `git diff --check` 通过，独立代码评审无需要修复的问题。
- 完整 pytest 在新建且执行后删除的独立 schema 中运行：540 passed、4 skipped、2 failed、23 warnings，耗时 93.25 秒。
- 两个失败均位于未修改的 tests/test_dependencies.py：test_vendored_sources_are_at_audited_commits（当前 dbt 提交 8e09d2a 与固定值 f7769d8 不一致）；test_core_packages_are_installed_from_vendored_sources（当前依赖 editable 安装，测试要求非 editable）。单独运行该文件可重现；不修改用户 vendor 状态或依赖来掩盖失败。
- 未修改现有业务 schema；现有库升级使用已验证的 add_execution_log.sql。
