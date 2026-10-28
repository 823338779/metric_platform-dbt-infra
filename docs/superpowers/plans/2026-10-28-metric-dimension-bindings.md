# Metric Dimension Bindings Implementation Plan

**Goal:** 支持每指标维度绑定，并尽量合并可共同查询的指标。
**Architecture:** 公共契约解析 → 路径签名归组 → 原生编译验证 → 数据库对齐结果。
**Tech Stack:** Pydantic、MetricFlow 0.213.0、dbt adapters、pytest、DuckDB。
**Spec:** `docs/superpowers/specs/2026-10-28-metric-dimension-bindings-design.md`

## Constraints

不改 vendor、不引入依赖、不截断中间结果；旧查询保持兼容；不触碰已有 docs/examples。

## Tasks

- [x] 契约和规划：修改 models/queries.py，新增 application/bound_query_plan.py，新增 engines/models.py 中的 BoundQueryPlan/BoundQueryGroup/BoundGroupBy；写 tests/test_bound_queries.py。`MetricQueryRequest.model_validate` 接受完整绑定，拒绝遗漏及冲突；`compile_bound_query(request, common, per_metric)` 解析选项并将四指标归为两组。先运行测试观察失败，再实现并运行。
- [x] SQL 编译和执行：新增 engines/bound_queries.py，提供分组合并 SQL 函数；MetricFlowClient.query_bound(plan) 原生编译组，处理原生拒绝组合时的贪心拆分；缓存并复用 SQL client。EXPLAIN 与 QUERY 使用同一个 SQL；通过 DuckDB 执行生成 SQL 检查 NULL、单侧键、复合键和最终 LIMIT。先写测试再实现。
- [x] 集成：execution/query.py 检测 bindings，按单指标发现选项并解析，公共项按完整指标集合发现。通过真实 dbt 临时项目验证两个指标同组、其他指标不同路径，执行完整 QUERY/EXPLAIN，并覆盖过滤、时间和截断。SQL client 执行次数测试保证 EXPLAIN 零次取数，QUERY 一次取数；重复显示标签回归保证原始键对齐。
- [x] 文档与回归：README 添加真实 camelCase、全大写 mode 示例和粒度/排序/筛选语义；运行 `.venv/Scripts/python.exe -m pytest -q` 与 `.venv/Scripts/ruff.exe check src tests`，检查最终 diff。

每步验证后更新复选框；在当前会话直接执行用户已批准的设计。

## 验证结果

- 完整 pytest：125 passed，7 skipped，2 个已有依赖弃用警告。
- Ruff：All checks passed；git diff --check 通过。
- 7 个跳过用例需要专用 PostgreSQL 测试连接；本次真实数据查询集成使用 DuckDB，未验证 PostgreSQL/StarRocks 目标实库。
- 独立只读审查发现显示映射键被替换的问题，已用真实失败回归复现并修复；针对修复的复查无新增问题。
