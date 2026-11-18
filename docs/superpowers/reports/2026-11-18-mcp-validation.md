# stdio MCP 验证记录

环境为 Windows、Python 3.12.14、本机临时 PostgreSQL 16.9、DuckDB、仓库锁定的 dbt 和 MetricFlow。HTTP 服务与 stdio Server 为独立进程。测试在本地创建源码快照，Git 远端 head 使用固定 SHA 替身；真实构建执行、产物封存、幂等受理、部署调和、SQL 查询与 HTTP 路由均使用项目实现。验收通过程序化工具调用执行，未测试特定聊天模型的自然语言理解效果。

| 场景 | 结果与证据 |
|---|---|
| 跨客户端 | MCP Python SDK 1.30.0 和 MCP Inspector CLI 2.13.0 分别启动同一 stdio 入口，七个工具的名称和输入结构一致，读取到同一生效构建 `04da9f58-f178-4e47-b25d-2edb09b3c6ec`。 |
| 搜索与实际查询 | 测试目录包含 33 个指标，HTTP 响应上限 4096 字节。全量目录触发 RESULT_TOO_LARGE；按“销售额”搜索并翻页，第一页面向客户端的单次测量为 32 ms。选择 revenue 后，2024 年 1 月渠道 new、paid 的结果分别为 2.00、12.34，完整查询响应与同构建直接 HTTP 查询相等。耗时仅描述这次本机样本。 |
| 澄清指标 | 真实 dbt 拒绝两个指标使用完全相同的 label。真实引擎用“销售额”和“双倍销售额”构造两个关键词候选；完全相同标签以及查询名冲突另在封存 Manifest 契约测试中覆盖。 |
| 部署切换 | 在同一环境部署第二个构建，更换快照版本及目标 schema。当前指针返回 B，携带 A 及原有维度选项再次查询得到 A 的原结果。此过程验证构建选择，数据读取沿用服务和底层数仓的快照语义。 |
| 空数据及错误 | 真实服务验证无部署、空行保留列、非法维度保留 buildId。HTTP 连接中断及服务不可达使用契约替身。 |
| 构建与部署 | 真实服务两次受理相同输入和幂等键，返回同一 buildId；成功部署后读取生效指针。通过 HTTP 替身覆盖受理响应丢失、原键恢复及构建成功但部署失败。 |

复现：安装项目依赖和 mcp extra，设置专用测试 PostgreSQL 的 `DBT_SERVICE_TEST_DSN`，执行 `uv run --frozen --extra mcp pytest tests/test_mcp_integration.py -sq`。要同时核对第二客户端，安装 Inspector CLI 2.13.0，将 `MCP_INSPECTOR_ENTRY` 指向该包的 `clients/launcher/build/index.js`，并确保 Node 在 PATH 中。测试在独立临时数据库 schema 和临时 DuckDB 文件内运行。

已知契约缺口：get_build 的 queryAvailability 返回 unknown；失败诊断来自具体维度或指标查询。完整单条摘要超限时，搜索返回服务的 RESULT_TOO_LARGE。

整分支独立审阅后补测了不完整的 202 响应、无正文的鉴权错误、已知构建 ID 保留、仓库 URL 与上游错误的凭据处理。使用专用 PostgreSQL 并启用 Inspector 后，整个测试集 159 项通过，耗时 37.81 秒；两条警告来自 Starlette 测试客户端的依赖弃用提示。Ruff 和 uv 锁文件一致性检查通过。
