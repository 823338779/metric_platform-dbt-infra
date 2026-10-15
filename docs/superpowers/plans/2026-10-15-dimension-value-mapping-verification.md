# 维值映射实施与验证记录

实施分支：`codex/dimension-value-mapping`。修改位于本聊天关联的独立 worktree。

## 已实现

- dbt 正式 `value_mapping` 字段：v1、v2 列维度、derived dimension 的解析、原生 manifest 往返及 partial parse。
- 共享语义接口的字段与引用校验；按源模型和维度身份索引映射，隔离展示模型的计算路径与维度发现。
- QuerySpec 展示规格、显式 `ApplyDimensionValueMappingsNode`、节点复制/比较/visitor 和优化边界；未修改通用 DagNode。
- 原始查询完成计算、排序及 LIMIT 后，SQL converter 在最外层为每个映射维度独立 LEFT JOIN。支持输出别名、CTE、CTAS 和同字典多次使用。
- 原生 query/explain/saved query 接入；DIMENSION_VALUES 保持原值。执行层仅补充文本列类型元数据，不替换单元格。
- dbt-metricflow adapter 构建探针验证类型和 NULL 键，服务调用原生入口。未增加唯一性扫描。
- 服务保存类型化映射配置和源模型身份，沿用原生 SQL、结果和错误协议；README、协议示例及两个 vendor 的功能 changelog 已更新。

独立代码审查发现的三个问题均已通过失败测试复现并修复：无指标过滤的维度发现泄露字典字段、映射节点下游计算丢失 pending 操作、显式绑定缺少来源与当前配置校验。

## 验证结果

| 检查 | 实际结果 |
| --- | --- |
| 共享语义接口完整测试 | 482 passed，2 skipped |
| MetricFlow 功能测试（非 slow，排除 performance/release_tools） | 1189 passed，50 skipped，5 deselected |
| dbt 相关协议、解析及 full/partial parse | 53 passed，3 skipped；其中实际 full/partial parse 4 项 |
| 原生 adapter 映射验证 | 6 passed；DuckDB SQL 合约测试，实际 dbt adapter 另由服务端到端覆盖 |
| 服务回归（版本核对在固定提交后单独执行） | 341 passed，184 skipped，1 deselected |
| 合并前服务完整回归 | 342 passed，184 skipped |
| 最终真实 dbt → DuckDB → 服务端到端（含三个映射维度实际查询） | 2 passed |
| 固定版本及本地源码安装核对 | 4 passed |
| 服务 Ruff | passed |
| MetricFlow mypy | passed：531 + 463 个源文件 |
| dbt 修改文件 mypy、Black、isort | passed |
| dbt-metricflow 新增验证模块及测试的定向 mypy | passed |
| 三仓库 diff 空白检查 | passed |

真实 DuckDB 测试覆盖：404/502 相同标签仍分行、原始过滤/排序/LIMIT、多次 JOIN、两个维度复用字典、未匹配与 NULL 回退、空字符串、空结果文本类型、比率和共享用户的 count_distinct、字典数据更新立即生效、CTE/CTAS、saved query、原始维度值及构建拒绝 NULL 键。

九种原生方言的文本 CAST 和参数保留测试通过。PostgreSQL 与 StarRocks 没有进行真实数据库集成测试；不能将渲染/API 核查当作数据库执行证据。需要服务 PostgreSQL 测试库的用例仍跳过。

## 全量检查的限制

- 本机没有 make，按 Makefile 的 Hatch 命令执行相关套件。
- Windows checkout 的符号链接及长路径需要修复；长快照路径使用仅供测试的 `\\?\` 路径启动器，未修改生产逻辑。
- 完整 semantics 套件：253 passed、2 skipped、3 failed。失败为路径分隔符快照和两个计时精度相关性能测试。
- dbt 完整单测尝试在 354 passed 后遇到 3 个 Windows 换行/路径断言失败而停止。
- MetricFlow 全量 lint 保留 29 个原有中文 docstring 的 D415 问题；未为本功能改写无关文档注释。
- dbt code-quality 的 flake8 遇到现有 ignore 配置解析问题；artifact import hook 在 Windows 路径下未正确豁免 artifacts 目录。
- dbt-metricflow 整包 mypy 受开发环境缺失第三方 stubs/版本差异影响；新增两个文件的定向检查通过。

因此不将本次交付表述为“所有仓库全量测试和 lint 全绿”。详细日志保存在 worktree 的 `.superpowers/sdd/2026-10-15-dimension-value-mapping/`。

## 实施差异

- 计算图通过过滤后的 ManifestObjectLookup 构建，原始完整 manifest 保留用于映射解析；没有复制或改写 manifest。
- CTAS 先优化内部 SELECT，再应用映射并重建原 CTAS，保留目标表。
- 服务使用轻量 NativeValueMapping 类型投影跨越 Pydantic 版本边界；配置解释及有效性校验仍由原生引擎负责。
- 沿用 dbt 现有内容比较和序列化机制识别映射变化，没有增加重复的 partial-parse 逻辑。

## 提交状态

- MetricFlow 协议提交：`14ed1958e62012e6cda203a9913066ae7fe284f2`。
- MetricFlow 实现提交：`12ee501461329a0544e3c7cb3c7b0529c90bc5cc`。服务核对的固定版本已同步。
- dbt 实现提交：`f7769d85e4bd5794a9bb50409347b2d52a3a9119`，基于 `7f78d7b6aa3a88e5efa6dd92753e4983d92aeba4`。用户明确授权本次使用未签名提交；未修改仓库或全局签名配置。
- 根仓库通过 gitlink 和依赖版本核对测试固定两个 vendor 提交。
- 用户已授权合并到本地 `develop`，不包含远程推送。上述新增提交仍需以后随发布流程推送至对应仓库。
