# 原生指标维度通路绑定：实施与验证记录

日期：2026-11-18。分支：`codex/native-metric-bindings`。

## 实施结果

批准计划的 9 项任务已实施并分别审查。全分支审查发现的两项问题已在最后一轮修复：

1. 原生绑定查询按指标原名、输出别名或结构化参数排序时，排序规格移除输出 alias，最终投影仍保留 alias。覆盖 O0/O4/O5、有无共享筛选的 18 个真实执行用例及 3 个解析用例。
2. 服务的绑定请求显式使用原生 `INPUT_ORDER`，保持交错通路分组后的指标列序和混合共享/绑定维度列序；普通请求仍使用原有模式。两个端到端用例同时校验列名、位置值和一次取数。

最后一轮实施按用户要求切换到当前会话顺序完成，没有再次派发代理。已有独立全分支审查作为输入；修复后的代码在当前会话检查并测试，不声称经过第二次独立复审。

原生最终提交：`45849c2e857ed0c910d56eaffe12f0f7f8a55212`。

能力边界：服务只做 optionId 校验和参数转换；MetricFlow 负责按请求指标解析路径、Dataflow 分支规划、结构化 SQL 子树对齐和一次查询。HTTP JSON、旧 groupBy 形式及 available-dimensions 结构保持兼容。派生指标内部输入继承请求级路径；未增加公式输入独立绑定或每指标独立筛选。

## 最终测试结果

| 检查 | 结果 |
| --- | --- |
| 服务全量 pytest | 142 passed、7 skipped、2 个已有依赖警告；退出码 0 |
| 服务 Ruff | 通过；退出码 0 |
| 最后修复的 3 个原生文件 pre-commit | Ruff、Black、mypy 等全部通过；退出码 0 |
| 原生 semantics 全量 | 300 passed、4 failed、2 skipped；退出码 1 |
| 原生 semantic_interfaces 全量 | 482 passed、2 skipped；退出码 0 |
| 原生 metricflow 非 slow 全量 | 1383 passed、9 failed、51 skipped；退出码 1 |
| 原生全量合计 | **2165 passed、13 failed、55 skipped；未全绿** |

Windows 无 make，使用 Makefile 的三个独立 Hatch/pytest 目标，`-n 4`，metricflow 目标带 `-m "not slow"`。不能合并测试根目录，否则 conftest 重复注册参数。使用 `PYTHONUTF8=1` 和临时短路径盘符解决编码/长路径问题。

最后修复前已执行完整原生 lint：主项目有 29 项 D415 和 1 项既有 `Expr.named_selects` mypy 错误；开发前基线诊断集合相同。dbt-metricflow 子项目 lint 通过。最后修复的文件检查通过，不代表完整原生 lint 已全绿。

## 未通过项的分类

此前 12 项失败均在开发前提交 `04a659631ac20456d3af31565a4f03ea9eb71c37` 复现，最终复跑仍出现：

- 10 项 Windows 路径与 Unix 快照/预期路径的分隔符差异，包括 pretty_print 路径测试及 9 项 release_tools 测试。
- 2 项基于 `thread_time` 的零时长性能检查：`test_assert_performance_factor`、`test_model_lookup_performance`。

最终复跑还出现 `tests_metricflow_semantics/toolkit/test_singleton.py::test_create_existing` 的性能阈值失败：要求至少 0.4，完整运行得到 0.38，当前版本单文件复测得到 0.35。单例实现、测试、数据类辅助和性能辅助代码相对开发前版本未改变；但开发前单文件对照 6 项通过、该用例独立对照也通过。因此**没有把这一项计入已复现的基线失败**，也没有降低阈值或反复重试直至通过。该性能差异仍是验证限制，需要在稳定的基准环境继续确认。

保留未全绿的结果，不把功能测试通过表述为所有检查通过；未自动合并或推送。

## 实际验证范围

- DuckDB：真实执行，包含跨事实表、NULL/缺失指标、重复输出键、derived/ratio/cumulative/conversion、共享筛选与时间、两种求值策略、CTE/子查询以及排序/limit。
- PostgreSQL、StarRocks、BigQuery：相关 SQL 规划/渲染检查，不是实库验收。现有服务 StarRocks 配置复用 DuckDB renderer。
- 原生参数字典经过真实 DuckDB 渲染 SQL 执行测试；仓库测试 SqlClient 拒绝绑定参数，未声称新增生产适配器参数执行验收。
- 已有原生 `limit=0` 被 sink 真值判断忽略的问题未修改；普通查询也有同样行为，服务入参要求 limit >= 1。

## 环境事件

dbt-metricflow 的既有 Hatch 初始化 hook `pip install -e ../` 曾错误解析到全局 Python 3.14 的 pip，新增 16 个分发包，并把 more-itertools 从 11.1.0 替换为 10.8.0。根据完整安装日志，先核对全部版本及 editable 路径，再只撤销本次新增包并用 `--no-deps` 恢复 11.1.0。

回滚验证：恰好移除 16 项、无新增项、仅 more-itertools 恢复版本，其他已安装分发包元数据不变；回滚后 pip freeze 成功。无待清理的全局依赖变更。没有修改仓库依赖声明来掩盖该事件。

## 实施中的裁决及代价

以下按发生顺序保留实施决策，方便后续复核。

1. 复用批准计划的隔离工作树；若隔离选择不合适，需要迁移提交，不影响原工作区。
2. 原生代码在子模块单独提交、审查，再在根仓库记录 gitlink；整合时必须同时保留两层历史。
3. 子模块从本地原始克隆获取精确版本；当前工作树的子模块 origin 依赖本地仓库，后续推送需确认真实远端。
4. 前期分任务运行定向回归，最后运行完整套件；跨层问题可能晚发现，最终全分支审查确实补出了两项问题。
5. 为 visitor 闭合修改计划清单外的必要访问器；增加审查范围，避免遗漏导致其他访问器失效。
6. 新 request 字段不进入 repr，避免改变旧快照标识；仅凭 repr 无法区分绑定请求，测试需要独立标识。
7. 完整 QuerySpec 仅以可信内部 pickle 验证往返，具体新记录使用既有 JSON serializer；未来公共 JSON 持久化仍需独立设计，不提供公共 pickle 反序列化接口。
8. 共享筛选先全局解析，各请求指标再完整验证；若分阶段验证有遗漏，可能迟报输入错误，相关解析和真实结果测试已覆盖。
9. 不新增实际不存在的 output_table 参数；接线中途使用明确执行保护，最终已移除；原有签名拒绝行为保留。
10. QUERY 筛选保留全局规格/粒度，只重定位其分支 lookup；若重定位错误会造成分支筛选不同，跨日/月源的真实执行验证通过。
11. 使用合并节点的纯输出实例解析方法，未再建设通用实例推断框架；未来其他节点需要类似能力时可能需重构。
12. 测试使用 UTF-8 和临时短路径盘符；复现 Windows 测试时仍需处理路径环境，未改快照掩盖差异。
13. CTE 读取实例的列关联对齐其已有投影别名；若不一致会影响普通 CTE，完整回归未发现该类功能失败。
14. 只撤销日志证明的全局依赖误变更，并在修改前核对元数据；误判旧状态会影响全局环境，实际前后对比已验证限定变更集合。
15. query_plan.py 直接复用既有 compile_query，没有为满足计划文件清单制造修改；共享筛选/时间语义依赖此接口，服务结果回归通过。
16. 不在本功能中修改既有原生 limit=0 行为；直接原生调用方仍不能依赖 0 得到零行，服务不接受 0。

## 证据位置

当前工作树 `.superpowers/sdd/2026-11-17-native-metric-dimension-bindings/` 保留任务报告、审查、完整命令输出、基线对比和环境回滚证据。由于原生全量门禁尚未全绿，暂不删除这些记录。重点文件：`final-review.md`、`task-9-report.md`、`final-fixes-*-red.log`、`final-fixes-service-full.log`、`final-fixes-full-*.log`、`final-fixes-singleton-*.log`。
