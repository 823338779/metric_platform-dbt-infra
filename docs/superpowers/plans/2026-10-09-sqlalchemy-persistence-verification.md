# SQLAlchemy 持久化迁移验收记录

日期：2026-10-09。分支：`codex/sqlalchemy-persistence`；实施基线：`22cba6a974495bb2af6c2aa722832331ea38186f`。用户选择当前目录新分支、Native 顺序实施；四项任务完成，并完成一次独立审阅及修正。全套测试存在已复现的既有失败，因此保留分支，不进行合并或推送。

## 实施结果

- SQLAlchemy Core Engine 管理连接池、提交和回滚，运行时调用者使用真实 Connection；保留原始业务 SQL、UUID 字符串及可修改字典返回契约。
- 共享事务使用明确的事务内入口；候选、任务、输入和发布写入保持原子性，写后租约失效通过事务外捕获私有异常返回失败。
- Alembic 接管空 schema 和历史版本 1–5，迁移由同一事务与 advisory lock 保护；未知或损坏历史拒绝升级。五份归档 SQL 未修改，服务启动只检查版本。
- wheel 包含迁移资源，原管理命令保持不变：`uv run --frozen dbt-service-admin migrate`。部署顺序见 README。

## 验证证据

所有数据库验证使用本任务创建的独立 Docker PostgreSQL 实例，没有操作生产库。核心 PostgreSQL 测试实际执行，没有用 SQLite 替代。

| 验证 | 结果 |
| --- | --- |
| 改动前完整测试（未启用 PostgreSQL） | 301 通过、1 失败、168 跳过 |
| 改动前 PostgreSQL 存储和发布事务基线 | 25 通过 |
| Connection/异常边界及相关回归 | 79 通过；新行为测试先失败后通过 |
| 共享事务及业务回归 | 70 通过；新事务入口测试先失败后通过 |
| 迁移及受影响存储回归 | 67 通过，扩展检查 29 通过 |
| 审阅修正后的迁移专项，PostgreSQL 16 | 24 通过 |
| 审阅修正后的迁移专项，PostgreSQL 17 | 24 通过 |
| 心跳取消等异常边界 | 7 通过 |
| 最终完整测试，PostgreSQL 16 与 wheel 验证启用 | **502 通过、4 失败、6 跳过**，263.70 秒 |
| 最终修正后重新构建 wheel，离开仓库目录安装运行 migrate/check | 2 通过 |
| `uv run --frozen ruff check src/dbt_metricflow_service tests` | 通过 |
| `git diff --check` | 通过 |

完整测试命令为 `uv run --frozen pytest -q -ra`，设置了 `SERVICE_TEST_DATABASE_URL` 和 `SERVICE_TEST_WHEEL`。wheel 使用 `uv build --wheel --out-dir dist/persistence-verification` 构建，安装验收使用 `uv run --frozen pytest tests/test_migration_package.py -q`。pytest 另报告两项 Starlette/httpx/anyio 弃用警告。

## 四项既有失败

1. `tests/test_dependencies.py::test_vendored_sources_are_at_audited_commits`：MetricFlow 审计期望提交 `05551f73c22786b933969bbccafc47c2a1b51625`，实际为 `9a07d35d6fa7cef009ea574938c1e483c29581ac`；改动前已失败。
2. `tests/test_runtime_execution.py::test_programmatic_tasks_use_restored_input_and_return_json[RUN_CLEANUP-payload1-expected1]`。
3. `tests/test_runtime_execution.py::test_programmatic_tasks_use_restored_input_and_return_json[RUN_CLEANUP-payload2-expected2]`。
4. `tests/test_runtime_execution.py::test_source_only_cleanup_drops_schema_without_semantic_manifest`。

后三项的旧测试夹具缺少 cleanup 的 `parent_run_id`，未改动的 executor 调用 `UUID(None)` 报 TypeError。用 `git archive` 提取基线 `22cba6a` 的源代码和测试，在同一 PostgreSQL 环境重跑原始 `test_runtime_execution.py`，结果为 **3 失败、15 通过**，确认不是本次迁移引入。上述问题不属于持久化框架替换范围，未顺手修改。

## 独立审阅及修正

独立审阅提出两项 P2，无 Critical 或 Minor 项；两项均在一次修正中处理，并观察回归测试从失败到通过。

- 历史结构核验原先忽略禁用的内部外键触发器。现在除 `convalidated` 外，按约束 OID 检查关联触发器启用状态，拒绝接管失去外键执行保护的 schema。
- `quote_all_identifiers=on` 会改变 PostgreSQL catalog 反编译文本。结构读取时在事务内暂时关闭并恢复该显示设置；空库和历史 v5 都有回归覆盖。

## 实施裁决与限制

- 保留很小的共享 `row_dict` 转换函数，确保公开行仍是可修改字典或 None；不增加自制 cursor 门面，否则会泄漏不可修改的 RowMapping 返回值。
- 历史结构契约固定保存为 JSON，由开发期从归档 SQL 提取；避免运行时解析 SQL 或创建影子 schema。PostgreSQL 16、17 已验证，其他主版本仍需部署前验证 catalog 展示兼容性。
- 001 的旧 SQL 按全库约束名查重会漏建其他 schema 的循环外键。仅对本次新建 schema，按目标表 OID 补齐原 SQL 已声明的六条外键；已有历史库缺失外键仍拒绝接管。
- 四项既有失败保留，全套测试不能宣称全绿；没有延期处理的审阅 Minor 项。
- 六项环境门控 E2E 未运行：`integration/test_branch_publication_flow.py`、`integration/test_branch_starrocks_flow.py`、`integration/test_postgres_platform_flow.py`、`integration/test_publication_proxy_flow.py`、`test_runtime_e2e.py`、`test_starrocks_e2e.py`。这些需要额外的 PostgreSQL/真实构建环境、显式平台开关或 StarRocks 测试库；本次未验证完整外部数据仓库流程。

本记录保存了 Native 临时 ledger 中的实施证据和裁决，供清理本任务临时目录后继续审阅。
