# 固定版本 dbt 运行与查询契约 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 Metric Platform 提供可重启恢复的固定 SHA 全量构建、版本化原生目录及 PostgreSQL MetricFlow 类型化查询。

**Architecture:** 在现有通用 CLI 路由旁新增平台专用 API。服务只从配置的 Git 绑定检出固定 SHA，以本地 SQLite 保存任务索引、独立目录保存产物；dbt/MetricFlow 仍负责构建及查询，平台负责发布指针。

**Tech Stack:** Python 3.11–3.14、FastAPI、dbt-core 1.12.5、metricflow 0.213.0、dbt-metricflow 0.15.0、PostgreSQL profile、SQLite 标准库、pytest、ruff。

**Spec:** `../../../../docs/superpowers/specs/2026-09-26-dbt-platform-modules-and-visual-catalog-design.md`

## Global Constraints

- 只修改 `dbt-metricflow-service/`；保持该仓库独立构建，现有 `/v1/dbt/jobs` 与 `/v1/metricflow/jobs` 行为不变。
- 不接受请求提供的 Git remote、profile 内容、schema 名、SQL 或命令行选项；从服务配置解析项目绑定。
- 每次 dbt 输入变化都对固定 SHA 的全部允许定义在独立 schema 全量构建；PostgreSQL 用于本轮端到端验收，StarRocks 仍不可标记为 MetricFlow 可查询。
- 版本化目录必须来自该 run 的 dbt 原生产物；人类可读 CLI 列表输出不是机器契约。
- 不修改 `vendor/` 上游源码或其 gitlink；服务端新增 API、测试及 README 使用中文说明。
- 工作区 `AGENTS.md` 要求每个 Superpowers plan 执行完毕后再形成一个代码 commit；计划编写阶段不创建提交。

## Review Focus

1. 不受信任的 SHA、路径或仓库 URL：请求必须拒绝，不能在本地写出配置范围；Task 1 的测试覆盖。
2. 同一幂等键并发或重启后重复提交：只对应一个运行任务，未知状态不能伪装 READY；Task 2 的测试覆盖。
3. `manifest` 与本次 SHA、schema、profile 不一致：候选失败且不输出可查询目录；Task 3 的测试覆盖。
4. 多指标的兼容维度、`metric_time` 粒度及维度值超限：返回结构化选项或明确错误，不解析 CLI 彩色文本；Task 5 的测试覆盖。
5. 清理时有活动查询或路径被替换成符号链接：不能删除活动/目录外产物；Task 6 的测试覆盖。

---

## File Map

| 文件 | 职责 |
| --- | --- |
| `src/dbt_metricflow_service/platform_models.py` | 平台专用 Pydantic 请求、运行状态、原生目录和查询结果类型 |
| `src/dbt_metricflow_service/platform_bindings.py` | 受控项目 Git、projectSubdir、profileBindingId 绑定与安全 SHA 检出 |
| `src/dbt_metricflow_service/platform_store.py` | SQLite 幂等任务索引及重启恢复 |
| `src/dbt_metricflow_service/platform_runs.py` | 全量构建、产物核验、READY 和清理编排 |
| `src/dbt_metricflow_service/platform_catalog.py` | 固定 run 的原生目录、依赖与物理 relation 读取 |
| `src/dbt_metricflow_service/platform_metricflow.py` | MetricFlow 程序化 API 的单一版本锁定适配层 |
| `src/dbt_metricflow_service/platform_queries.py` | 查询选项、类型化任务、维度值和有界结果 |
| `src/dbt_metricflow_service/api.py` | 新 `/v1/project-runs`、`/v1/query-jobs` 路由和错误映射 |
| `src/dbt_metricflow_service/settings.py`、`pyproject.toml`、`uv.lock`、`README.md` | 受控配置、PostgreSQL adapter 和运行说明 |

### Task 1: 受控绑定与固定 SHA 检出

**Files:** Create `src/dbt_metricflow_service/platform_bindings.py`, `platform_models.py`, `tests/test_platform_bindings.py`; modify `src/dbt_metricflow_service/settings.py`。

**Interfaces:** `ProjectBinding(project_id, remote, project_subdir, profile_binding_id)`；`resolve_revision(binding: ProjectBinding, commit_sha: str, expected_digest: str, work_root: Path) -> Path` 产出只读任务工作目录。服务只读取配置文件 `PLATFORM_BINDINGS_FILE` 中的绑定，`remote` 不来自 HTTP。

- [ ] **Step 1: 写失败测试。** `test_resolve_fixed_sha_and_digest` 用临时 Git 仓库验证 A/B 提交只读取请求 SHA；`test_reject_unsafe_sha_subdir_symlink_and_digest` 验证拒绝越界、符号链接与摘要不符；`test_remote_cannot_be_supplied_by_request` 验证请求 DTO 拒绝未知字段。
- [ ] **Step 2: 验证测试失败。** 运行 `uv run --frozen pytest -q tests/test_platform_bindings.py`，预期因模块或接口不存在而失败。
- [ ] **Step 3: 实现最小安全读取。** Git 调用使用参数数组与固定 remote；摘要算法与平台 `GitSnapshotReader.readDbtRevision` 的 Git 路径、模式、blob ID 规则逐项一致；校验检出目录位于任务根目录。
- [ ] **Step 4: 验证通过。** 重跑本任务测试，并运行 `uv run --frozen ruff check src tests`。

### Task 2: 平台专用持久任务索引

**Files:** Create `src/dbt_metricflow_service/platform_store.py`, `tests/test_platform_store.py`; modify `src/dbt_metricflow_service/settings.py`。

**Interfaces:** `PlatformJobStore(db_path: Path)` 提供 `reserve_run(idempotency_key, request_fingerprint) -> UUID`、`find_run(run_id) -> RunRecord | None`、`find_run_by_key(key) -> RunRecord | None`、`transition_run(run_id, state, artifact_path, error_code) -> RunRecord`；query 对应方法使用同样的幂等原则。SQLite 只存元数据和无凭据路径。

- [ ] **Step 1: 写失败测试。** `test_same_key_returns_same_run_across_reopen`、`test_key_with_different_payload_conflicts`、`test_restart_reconciles_nonterminal_run_without_ready`、`test_concurrent_reserve_creates_one_run`。
- [ ] **Step 2: 验证测试失败。** 运行 `uv run --frozen pytest -q tests/test_platform_store.py`，预期新接口失败。
- [ ] **Step 3: 实现 SQLite 唯一约束与状态流转。** 一个服务实例；任务目录就绪标记在状态 READY 前落盘；重启把未完成任务恢复为可核实状态，不自动重放不确定的写操作。
- [ ] **Step 4: 验证通过。** 重跑本任务测试，并运行 ruff。

### Task 3: 固定 SHA 全量构建与 READY

**Files:** Extend `src/dbt_metricflow_service/platform_models.py`; create `platform_runs.py`, `tests/test_platform_runs.py`; modify `src/dbt_metricflow_service/api.py`。

**Interfaces:** `PlatformRunRequest(projectId, commitSha, projectDigest, profileBindingId, configVersion, idempotencyKey)`；`PlatformRunCoordinator.submit(request) -> RunReceipt`、`get(run_id) -> RunSnapshot`、`get_by_key(key) -> RunSnapshot`。API 为 `POST /v1/project-runs`、`GET /v1/project-runs/{runId}`、`GET /v1/project-runs/by-key/{key}`。

- [ ] **Step 1: 写失败测试。** `test_run_builds_all_models_in_new_schema` 检查 dbt build 没有 select/exclude、每次使用新 schema；`test_ready_requires_sha_digests_tests_relations_and_query_probe` 对缺失或不匹配逐项断言非 READY；`test_profile_binding_mismatch_is_rejected`。
- [ ] **Step 2: 验证测试失败。** 运行 `uv run --frozen pytest -q tests/test_platform_runs.py`，预期新 API 或协调器失败。
- [ ] **Step 3: 实现运行编排。** 只从 Task 1 的工作目录构建，使用任务隔离 `target/` 和 `logs/`，保存 manifest、semantic_manifest、run_results 及无凭据验证摘要；真实 MetricFlow 查询探针只对支持的 adapter 置 `queryCapability=true`。
- [ ] **Step 4: 验证通过。** 重跑测试；现有 `tests/test_api_dbt.py` 与 `tests/test_api_metricflow.py` 仍通过。

### Task 4: 原生目录与物理关系

**Files:** Create `src/dbt_metricflow_service/platform_catalog.py`, `tests/test_platform_catalog.py`; modify `src/dbt_metricflow_service/api.py`。

**Interfaces:** `catalog_for_run(run_id: UUID) -> CatalogSnapshot`；`GET /v1/project-runs/{runId}/catalog` 返回 `resources`、`dependencies` 和三个产物 schema 版本。每个资源含 `resourceId`、`nativeId`、`kind`、`definition`、`sourcePath`、物理 relation/列信息与 `queryable`。

- [ ] **Step 1: 写失败测试。** `test_catalog_maps_metric_dimension_semantic_model_table_and_view` 使用固定 dbt 产物 fixture；`test_catalog_rejects_schema_version_or_missing_relation`；`test_catalog_returns_only_ready_run`。
- [ ] **Step 2: 验证测试失败。** 运行 `uv run --frozen pytest -q tests/test_platform_catalog.py`，预期新函数失败。
- [ ] **Step 3: 实现按产物版本的结构化转换。** 原生 `definition` 不改写业务含义；dbt model 的 table/view 与字段由 catalog 产物或已核验 adapter 元数据提供，不用 YAML 原文猜物化类型。
- [ ] **Step 4: 验证通过。** 重跑本任务测试并检查返回 DTO 与 Task 3 READY 摘要一致。

### Task 5: PostgreSQL MetricFlow 查询与维度值

**Files:** Create `src/dbt_metricflow_service/platform_metricflow.py`, `platform_queries.py`, `tests/test_platform_queries.py`; modify `src/dbt_metricflow_service/platform_models.py`, `api.py`, `pyproject.toml`, `uv.lock`。

**Interfaces:** `query_options(run_id, metrics: tuple[str, ...]) -> QueryOptions(metrics, dimensions, timeDimensions, allowedFilters)`，其中每个维度和粒度都有 MetricFlow 原生 token；`PlatformQueryRequest(runId, idempotencyKey, mode, metrics, groupBy, filters, startTime, endTime, orderBy, limit, datasetResourceId, dimension)`；`submit_query(request) -> QueryReceipt`；`GET /v1/project-runs/{runId}/query-options`、`POST/GET /v1/query-jobs` 及按键读取。模式包括 `QUERY`、`EXPLAIN`、`PREVIEW`、`DIMENSION_VALUES`。

- [ ] **Step 1: 写失败测试。** `test_multi_metric_options_return_common_dimensions_and_time_tokens`、`test_dimension_values_are_bounded_and_typed`、`test_query_rejects_unlisted_filter_operator_and_foreign_run`、`test_starrocks_reports_capability_unavailable`、`test_decimal_result_preserves_precision`。
- [ ] **Step 2: 验证测试失败。** 运行 `uv run --frozen pytest -q tests/test_platform_queries.py`，预期新接口失败。
- [ ] **Step 3: 实现窄适配层。** 只调用锁定版本的已安装 MetricFlow 程序化 API；从其返回值构造结构化选项、原生 group-by token 与结果，不解析 CLI 彩色文本；加入与 dbt-core 1.12.5 兼容的 `dbt-postgres` 并更新锁文件。
- [ ] **Step 4: 验证通过。** 重跑本任务测试、`uv run --frozen ruff check src tests`，再运行现有 MetricFlow API 测试。

### Task 6: 清理与 PostgreSQL 端到端验收

**Files:** Modify `src/dbt_metricflow_service/platform_runs.py`, `api.py`, `README.md`; create `tests/test_platform_cleanup.py`, `tests/integration/test_postgres_platform_flow.py`、测试 dbt 项目和 profile fixture。

**Interfaces:** `cleanup_run(run_id: UUID) -> None`；`POST /v1/project-runs/{runId}:cleanup` 幂等。集成测试通过环境变量指向独立 PostgreSQL 测试库，不使用 Metric Platform 自身数据库。

- [ ] **Step 1: 写失败测试。** `test_cleanup_rejects_running_query`、`test_cleanup_refuses_linked_or_outside_path`、`test_cleanup_retry_is_idempotent`；端到端测试覆盖两次提交各自全量构建、目录、兼容维度、`metric_time` 月粒度、维度值、查询结果和无引用清理。
- [ ] **Step 2: 验证测试失败。** 运行 `uv run --frozen pytest -q tests/test_platform_cleanup.py tests/integration/test_postgres_platform_flow.py`，预期新能力失败；PostgreSQL 测试库不可用时须明确报告环境缺失，不当作通过。
- [ ] **Step 3: 实现清理与测试配置。** 仅清理由 Task 1/2 标识的 run 专属 schema 和服务目录；保留现有通用 CLI 任务产物清理行为；README 写明独立 PostgreSQL 配置和验收命令。
- [ ] **Step 4: 验证通过。** 执行 `uv run --frozen pytest -q`、`uv run --frozen ruff check src tests`，再用真实 PostgreSQL 执行端到端测试；核对 StarRocks 仍返回不可查询能力。

## 计划完成后的提交

检查 `git status --short` 只含本计划文件，按工作区规则在**整份计划执行完成且测试通过后**创建一个 `dbt-metricflow-service` 代码提交，不在各 Task 中分别提交；实施前须遵循用户对提交的明确授权。
