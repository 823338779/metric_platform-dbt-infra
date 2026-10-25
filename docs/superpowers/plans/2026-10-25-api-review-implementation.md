# dbt service API Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** 实施已确认的全接口审查方案，删除无用协议和内部包装，修复三项行为问题。

**Architecture:** 保留 api/application/execution/storage 职责；SDK 收敛到 engines。查询采用按 mode 区分的请求响应，同一次请求只创建一个引擎上下文。数据库 schema 与任务保护不变。

**Tech Stack:** Python、Pydantic、FastAPI、SQLAlchemy、PostgreSQL、vendored dbt/MetricFlow、pytest。

**Spec:** docs/reviews/2026-10-25-dbt-service-api-review.md

## Global Constraints

- 用户已批准直接实施、调整接口、不保留旧协议兼容；无需再次审批计划。
- 保留 14 个 HTTP 操作；保留原生 Manifest、selector、SDK 语义、EXPLAIN 的实际执行 SQL（limit+1）。
- 不增加查询状态预检；统一公开结果大小上限，默认 8 MiB。
- 保留持久幂等、generation/version、租约、封存、历史日志与取消屏障。
- 业务代码保持未提交，不改 vendor/dbt，不改现有未提交设计草案。

## Review Focus

- 重试遇到配置撤回仍恢复旧身份；新请求不能绕过配置验证（任务1）。
- 并发观察 B→C、UNKNOWN、MISSING 不能令旧观察结算成功（任务1）。
- 模式无关字段即使 null/[] 也必须拒绝；非法过滤值在初始化前失败（任务2）。
- 取消时线程仍占用上下文，必须等待真正退出才释放资源（任务3）。
- 产物损坏保持服务错误，不能被误报为用户输入问题；实际响应字节而非内部 DTO 大小决定上限（任务3/4）。

## Task 1: 构建幂等与部署一致性

**Files:** application/builds.py、api/builds.py、admin.py、storage/builds.py、storage/deployments.py、application/deployments.py；tests/test_build_acceptance.py、tests/test_deployment_observation.py。

**Interfaces:** BuildService.submit 为请求入口，新增仅新请求执行的目标校验；BuildStore.accept 在幂等恢复之后、写入之前调用校验。observe 的每次有效观察递增 version，settle 使用 version/head/state 核对观察。部署响应转换在应用层；内部观察返回 StoredDeploymentTarget。

- [x] 写并运行失败用例：同键恢复不调用新配置校验；新键校验失败不产生记录；B→C/UNKNOWN/MISSING 后旧 version 结算不能 DEPLOYED。
- [x] 最小修复受理顺序、观察版本和结算条件；HTTP/CLI 共用用例。
- [x] 运行定向 pytest；若可用，以独立 PostgreSQL 实例验证真实事务与并发。

## Task 2: 公共协议与类型归属

**Files:** models/contracts.py、builds.py、execution.py、queries.py、service.py；迁移并删除 models/artifacts.py、payloads.py；调用方导入。

**Interfaces:** BuildRequest/BuildView 位于 models/builds.py；唯一 Contract/Page/JsonObject 位于 contracts.py；QueryRequest 是 Annotated discriminated union；QUERY/EXPLAIN、PREVIEW、DIMENSION_VALUES 有明确字段集；QueryResult 同样按 mode 区分。

- [x] 写并运行失败用例：混合字段拒绝、IN 标量/空数组/对象拒绝、有限标量、PREVIEW 无时间字段、EXPLAIN 无空数据字段、构建无顶层 configVersion。
- [x] 定义模式联合，SelectedFilter 验证标量/IN 数组；合并重复基础类型，迁移所有导入；删除 valueType/boundaryPolicy。
- [x] 运行模型及受理回归。

## Task 3: 查询生命周期与引擎模块

**Files:** application/queries.py、query_plan.py、execution/query.py、embedded.py；engines/{dbt,metricflow,models,results}.py；迁移 delegates 和 platform 查询模块；tests/test_query_execution.py、test_query_plan.py、test_engine_results.py。

**Interfaces:** QueryExecutor.execute_query(build, public_request) -> DataQueryResult，在同一串行上下文中发现维度、调用纯 compile_query、执行 SDK；QueryService 接收这一个入口。ResolvedDimension 保存公开选项与 token/grain 映射，编译只使用这份索引。引擎请求不含 run_id；引擎维度结果不含 metrics。

- [x] 写并运行失败用例：带维度请求只创建一次上下文/客户端；选项归属、排序和粒度错误；异常直接传播；等待线程结束再处理取消。
- [x] 收拢维度/时间规则，迁移 SDK 与共享序列化，删除异常作为返回值、空 caller/run_id 和重复 DTO；保持结果精度及 EXPLAIN 取数策略。
- [x] QueryService 按实际模式组装响应，统一按 settings.max_result_bytes 检查最终公开 JSON。
- [x] 运行查询回归和全部模块导入检查。

## Task 4: 其余接口与执行瘦身

**Files:** api/{app,system,auth,catalog,deployments}.py、application/catalog.py、execution/{build,dbt,models}.py、settings.py、models/configuration.py；tests/test_api_contracts.py、test_build_execution.py。

**Interfaces:** CatalogService.lineage(build_id, selector) 完整用例；执行器支持复用预读 Manifest。健康/版本显式响应模型；鉴权直接抛 ServiceError，无 caller 返回，构建固定受理身份在边界提供。execute_build 必传 profiles，成功结果必含产物；DbtExecution 直接构造 JobErrorDetail。

- [x] 写并运行失败用例：14接口有效/主要错误、认证码、健康不依赖 CLI PATH、血缘只读一次 Manifest、最终结果大小、构建诊断不生成虚构任务身份。
- [x] 移动系统路由；路由仅调用用例；瘦身构建诊断和可选参数。
- [x] 全部 pytest、Ruff、OpenAPI 和 import smoke 通过。

## Task 5: 集成验收与文档

**Files:** tests/test_postgres_integration.py、tests/test_sdk_integration.py、README.md、docs/typed-data-access.md、docs/api-simplification-migration.md。

- [x] 运行独立 PostgreSQL 与真实 DuckDB/dbt/MetricFlow 集成；不可用时明确记录未完成项，不碰业务库。
- [x] 更新接口迁移、包职责及验证结果；修订常量规则，只集中共享业务协议常量。
- [x] 按 executing-plans 做一次独立最终审查，修复有行为风险的发现并回归。
- [x] 核对 git diff --check、Ruff、全量 pytest、14接口；保持未提交。

## Execution Ledger

Ruling: 在现有 develop checkout 内实施，保留现有虚拟环境及已修改 vendor；不创建额外工作树，不提交，用户可直接查看未提交 diff。

Ruling: 执行清单和进度共用本文，替代依赖 Bash/提交历史的临时脚本；本任务明确要求未提交，避免脚本自动提交或清理工作目录。

Pre-flight: Task 2 的 QueryRequest 联合由 Task 3 执行器消费；Task 3 的 execute_query 接口供 Task 4 app 装配；Task 1 受理方法在 Task 2 仅重命名模型，顺序语义不变。

Task 1: complete — 8 个受理/观察用例 RED→GREEN；独立 PostgreSQL 4 个事务/并发回归通过。Ruling: 旧观察失效时保持意图待协调，不因普通读取竞争把最新意图标记 STALE；否则同 head 的 GET 也可能永久终结部署。

Task 2: complete — 查询契约初始 21 failed / 8 passed，模式联合与过滤边界实现后全部通过；统一 BuildRequest/BuildView/Contract/QueryMode，删除公开重复字段。

Task 3: complete — 同上下文/异常/取消初始 3 failed，修改后通过；选项、时间、精度、截断与最终 UTF-8 响应大小回归通过。

Task 4: complete — 认证码、血缘复用、诊断去包装初始 3 failed，修改后通过；替身测试覆盖 14 操作，真实 SDK 流程也覆盖全部操作。

Task 5: complete — 独立 Docker PostgreSQL 16 容器仅绑定 127.0.0.1 随机端口；每测试使用新 schema 并清理，验收后移除本任务容器。真实 dbt/DuckDB + PostgreSQL 服务元数据验证通过。最终全量 90 passed（包括 7 项 PostgreSQL/SDK 集成用例），2 个第三方弃用警告；Ruff、git diff --check、78 个服务模块导入通过。真实 StarRocks/PostgreSQL 目标仓库 SQL 未在本次运行。

Final review: 一次独立审查发现 1 项 Important、1 项 Minor，均已修复。SOURCE_HEAD_CHANGED 的 STALE 结算现在与 DEPLOYED 一样核对观察版本/head；血缘列表采用与 FastAPI 相同的 Pydantic JSON 序列化计数。新增 C→B/C→UNKNOWN 的服务、Store 和真实 PostgreSQL 交错回归，以及浮点数/中文 HTTP 字节边界回归；修复前 8 failed，修复后相关 40 passed。保留有效当前观察终结失配意图的正例，避免修复把正常 STALE 路径屏蔽。无待处理的审查发现。
