# stdio MCP Server 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**目标**　让通用客户端在目录超限时仍能按固定构建查询指标，并按固定提交受理和追踪构建。

**架构**　FastAPI 在封存 Manifest 中检索并限制每页响应大小。独立 stdio 进程通过公开 HTTP 路由执行七种操作。客户端 Agent 负责候选澄清和相对时间解释。

**技术栈**　Python、FastAPI、Pydantic、MCP Python SDK、HTTPX、pytest、dbt、MetricFlow。依据项目的 Python 版本范围解析并锁定兼容依赖。

**设计依据**　[已批准的设计](../specs/2026-11-18-mcp-server-design.md)

## 全局约束

- 搜索按请求读取当前指定构建的封存产物。所有后续读操作显式传入同一个 build_id。
- 搜索页默认 50 条，允许 1–200 条，同时遵守服务配置的响应字节上限。
- 构建请求要求完整小写 SHA（40 或 64 位）、显式部署策略及稳定幂等键。
- stdout 专用于协议，诊断使用 stderr。环境凭据只用于配置的 HTTP 服务。
- HTTP 直接查询用作测试基准，运行时只执行一次所请求的指标查询。

## 审查重点

| 输入或故障 | 应观察到的行为 | 负责步骤 |
|---|---|---|
| 中文说明和下一页游标共同导致响应超限 | 按实际 UTF-8 字节缩页，翻页没有遗漏或重复 | 1 |
| 显示名相同或引擎查询名称冲突 | 前者保留候选，后者拒绝有歧义的名称映射 | 1、3 |
| 部署切换或复合分组使用单指标选项 | 指标、分组和排序仍指向原构建及正确指标 | 3、5 |
| 构建请求已受理但响应丢失 | 调用结果为受理未知，恢复使用原请求与原键 | 4 |
| 未部署、查询返回零行、上游不可达 | 三种结果可区分，错误中没有凭据 | 2、3 |

## 文件与交付顺序

服务端修改 `src/dbt_metricflow_service/{models/catalog,application/catalog,engines/dbt,api/catalog,constants}.py`。MCP 侧新建同一包下的 `mcp_server.py`（入口及工具注册）和 `mcp_client.py`（HTTP 调用与错误转换）。依赖和命令归入 `pyproject.toml` 与项目锁文件。测试按下列任务归属组织。

### 1．提供固定构建的 HTTP 搜索及精确查找

- [ ] 在 `tests/test_catalog_search.py` 构造封存 Manifest，覆盖关键词匹配 ID、名称、标签、说明、类型过滤及有效节点白名单；断言同标签的两个 ID 均可见。覆盖中文多字节缩页、单条超限、跨构建或筛选条件错用游标及末页空游标。
- [ ] 运行 `uv run pytest tests/test_catalog_search.py -q`，确认失败源于缺少搜索端点。
- [ ] 增加 `GET /builds/{build_id}/catalog/search`。参数采用 keyword 或 resourceId 二选一，及可选 resourceType、limit、cursor。返回 items、nextCursor；摘要包含 resourceId、name、label、description、resourceType。精确 metric 查找另返回 queryNameUnique，依据同构建全部有效指标检查。精确 ID 缺失返回资源不存在。复用 CatalogService.read、BUSINESS_RESOURCE_TYPES、storage/paging.py 游标与 application/results.py 字节检查。仅检查最终候选页，缩页后重新计算游标和大小。
- [ ] 运行 `uv run pytest tests/test_catalog_search.py tests/test_catalog_listing.py -q`，确认超限场景分页可遍历、旧路由用例通过。提交搜索端点及测试。

### 2．打通 stdio、HTTP 和部署读取

- [ ] 在 `tests/test_mcp_transport.py` 用临时 HTTP 服务和真实 stdio 子进程执行初始化、发现、调用。断言生效 ID 来自 activeBuildId；即使 latestAttempt 指向 B，实际生效 A 仍返回 A；空指针为未部署。覆盖 422、含 buildId 的服务错误及连接失败，断言 stdout 可被协议解析，stderr 和工具结果均无测试 token。
- [ ] 运行 `uv run pytest tests/test_mcp_transport.py -q`，确认缺少入口导致失败。
- [ ] 声明可选依赖组 mcp、锁定兼容版本并注册 `dbt-metricflow-mcp`。在 mcp_server.py 建立 `run() -> None`，在 mcp_client.py 建立共享 HTTP 客户端，读取 DBT_METRICFLOW_SERVICE_URL 和可选 DBT_METRICFLOW_SERVICE_TOKEN。实现 get_current_deployment，按统一格式返回结构化结果和等价文本，业务错误设置 isError。
- [ ] 运行 `uv run --extra mcp pytest tests/test_mcp_transport.py -q` 验证启动、部署结果、错误与脱敏。提交入口、依赖和测试。

### 3．完成目录、资源、维度与查询四个工具

- [ ] 在 `tests/test_mcp_queries.py` 用 HTTP 契约替身断言请求路径、build_id、JSON 别名和内容。覆盖选择第二个同标签指标、非指标资源、不可唯一映射、复合分组绑定键、指标排序、非法维度、时间范围与行数限制。成功结果分别覆盖零行和含 null、数值、实际边界、截断标志的行集。
- [ ] 运行 `uv run --extra mcp pytest tests/test_mcp_queries.py -q`，确认四个工具尚未注册。
- [ ] 在 mcp_server.py 注册 search_catalog、get_resource、get_available_dimensions、query_metrics。使用任务 1 的精确查询解析所选 ID；转换复合分组键及指标排序，原样传递维度 optionId。每次调用限定同一 build_id，query_metrics 只发送一个 mode=QUERY 请求。工具说明写明版本固定、选项适用范围及时间口径。
- [ ] 运行 `uv run --extra mcp pytest tests/test_mcp_queries.py tests/test_query_contracts.py -q`，确认工具调用和原有 HTTP 查询契约通过。提交读取工具及测试。

### 4．提交构建并观察独立部署状态

- [ ] 在 `tests/test_mcp_builds.py` 验证缺失 SHA、策略或幂等键的请求不会发出；接受 202 后立即返回。模拟上游受理后断连，断言没有自动重发、报告受理未知，并且显式恢复发出相同请求体。覆盖构建成功、部署失败及缺少独立查询可用性字段。
- [ ] 运行 `uv run --extra mcp pytest tests/test_mcp_builds.py -q`，确认缺少构建工具导致失败。
- [ ] 在 mcp_server.py 注册 submit_build、get_build，复用 BuildRequest 和现有快照字段，在工具入口收紧必填项。说明 ON_SUCCESS 副作用；区分网络结果未知、已知业务失败、实际部署状态。get_build 返回公开构建事实及 queryAvailability=unknown。
- [ ] 运行 `uv run --extra mcp pytest tests/test_mcp_builds.py tests/test_build_acceptance.py -q`。检查 tools/list 恰有设计中的七个工具，提交构建工具及测试。

### 5．跨客户端验收并交付中文使用说明

- [ ] 在 `tests/test_mcp_integration.py` 用同一构建、相同服务大小上限和有已知渠道销售额的测试数据，断言完整目录返回 RESULT_TOO_LARGE，再经搜索、候选澄清、详情、渠道维度和查询得到预期销售额。以独立构造的 HTTP 查询核对构建、SHA、时区、时间边界、列、行和截断字段。验证中途从 A 切换到 B 后后续调用继续携带 A。
- [ ] 运行 `uv run --extra mcp pytest tests/test_mcp_integration.py -q` 并记录数据、大小上限、搜索耗时及结果。有条件时用专用 PostgreSQL 和实际仓库执行构建重试与部署失败验收；替身和未运行项在记录中分别标识。
- [ ] 用 Python MCP 客户端与另一个独立实现（优先 MCP Inspector CLI）分别通过 stdio 启动同一入口，记录实际版本、发现的工具和输入结构、两端相同的生效构建 ID。
- [ ] 更新 `README.md`，给出可选依赖安装、命令、环境变量和客户端无关的配置。新增 `docs/examples/mcp-sales-query.md`，用中文演示如何以构建时区确定上月、澄清候选、选择渠道和解释结果。
- [ ] 运行 `uv run --extra mcp pytest -q` 及 `uv run ruff check src tests`，区分通过、跳过和受环境限制的场景。在 `docs/superpowers/reports/2026-11-18-mcp-validation.md` 逐项记录使用的环境、结果与已知缺口，提交验收材料。
