# Native Metric Dimension Bindings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将每指标维度通路绑定下沉到 MetricFlow，以一个原生计划完成不同路径指标的计算、对齐与取数，并删除服务层重复规划。

**Architecture:** 解析器生成每请求指标的源规格及公共输出键，指标求值和 Dataflow 按各自路径构建分支。新增绑定合并节点保存映射，SQL 转换器通过结构化 UNION DISTINCT 和空值安全 LEFT JOIN 组装子树；普通查询保持原有路径。

**Tech Stack:** Python、仓库 vendored MetricFlow 0.213.0、dbt、Pydantic、pytest/Hatch、DuckDB、SQL AST 和原生方言 renderer。

**Spec:** `docs/superpowers/specs/2026-11-17-native-metric-dimension-bindings-design.md`（用户已批准；执行前完整阅读）。

## Global Constraints

- 只支持请求级指标绑定。派生指标的全部内部输入继承该请求指标的路径；不能在公式内部给不同输入另配通路。
- 无绑定请求保持原有解析、Dataflow、展示映射与执行行为。
- 不变更 available-dimensions 的响应结构；optionId 不进入 MetricFlow 内部。
- 不增加每指标独立筛选，不增加值转换、自动业务等价推断或新 entity JOIN 规则，不要求跨分支最优扫描数。
- 绑定模式统一返回原始维度键，包括共享项，延续现有接口语义。
- 不解析 SQL 文本，不使用显示名猜列，不先渲染各组字符串再拼接。
- 原生 limit 使用调用方给定值，服务仍传 `用户 limit + 1` 并负责 resultTruncated。
- QUERY 一次结果取数，EXPLAIN 不执行取数。缺失指标保留 NULL。
- 遵守 `vendor/metricflow/AGENTS.md`：Hatch、完整类型标注、不可变记录、单数类名、最低 Python 版本；不添加依赖。实现时采用 `CombineBoundOutputNode`，为 spec 建议类名的单数形式。
- 原生绑定模式不支持 group-by-metric、saved query、dimension-values/distinct、min/max、apply_group_by=False、output_table；入口明确报错。旧模式保持支持。

## Review Focus

1. 同一个 sales 指标既作为顶层请求又作为 derived 输入、路径不同，必须构建不同上下文；Task 5 固定结果和缓存隔离。
2. 同一源维度映射两个公共键且顺序交错，必须保留两个输出槽位；Tasks 2、3、7 固定列身份与列序。
3. 优化器替换/重排父节点后，绑定不可串到另一指标；Task 4 检查 replacement 与 permutation。
4. UNION 子树经过裁列、别名简化与 CTE 改写后，不得丢键、丢参数或改变去重语义；Tasks 6、7 验证优化前后结果。
5. 共享过滤按完整指标集合解析后切入单指标上下文，不得丢 FilterLocation 或变成公共别名过滤；Tasks 3、8、9 验证。

---

## 执行约定、文件地图与依赖

本计划是一条端到端能力链，不能把 SQL 组装、原生规划和服务迁移当作互不相关项目交付。顺序执行 Tasks 1–9。执行时先按 worktree 技能准备隔离环境，读取当时的 AGENTS.md；本轮计划编写不创建产品分支、不执行实现。

下文 `V/` 精确表示 `vendor/metricflow/`，`S/` 表示 `src/dbt_metricflow_service/`；命令中的路径均写全。原生命令工作目录为 `vendor/metricflow`，服务命令工作目录为仓库根。新增测试文件放入已有测试目录，不新增通用测试框架。

| 单元 | 新增文件 | 现有主要触点 | 责任 |
| --- | --- | --- | --- |
| 输入记录 | V/metricflow_semantics/specs/bound_group_by_parameter.py | engine/metricflow_engine.py | 不可变原生参数与复制 |
| 输出身份与解析规格 | V/metricflow_semantics/specs/bound_output_key_spec.py；bound_query_spec.py（同目录） | specs/query_spec.py、instances.py、spec_set.py、列解析器 | 公共结果键与源路径分离 |
| 绑定解析 | V/metricflow_semantics/query/bound_query_resolver.py | query/query_parser.py | 复用原生 resolver，逐指标校验 |
| Dataflow 对齐 | V/metricflow/dataflow/nodes/combine_bound_output.py | builder、visitor、输出推导、优化器 | 保存父分支映射与边界 |
| 指标求值 | 不新增并行规划器 | metric_evaluation、dataflow/builder | DFS/PassThrough 传播分支上下文 |
| UNION AST | V/metricflow/sql/sql_union_node.py | sql_plan.py、render、optimizer | 结构化去重键集合 |
| SQL 组装 | V/metricflow/plan_conversion/to_sql_plan/bound_output_sql.py | dataflow_to_subquery.py、dataflow_to_cte.py | 从 SqlDataSet 构造对齐子树 |
| 服务适配 | S/application/query_bindings.py | query_plan.py、execution/query.py、engines/metricflow.py、engines/models.py | optionId 到原生参数转换 |

每个任务的“提交”步骤只 stage 该任务明确修改的文件及自动生成快照，先 `git diff --cached --check`，再使用指定 commit message；不得 `git add .`。红灯必须源于目标功能缺失，而不是导入环境故障；环境故障单独解决并记录。新增 visitor 时用 `rg -n 'Visitor名称'` 找齐所有具体实现，验收中不能留下 abstract-method 实例化失败。

### Task 1: 原生不可变参数与旧输入兼容

**Files:** Create `V/metricflow_semantics/specs/bound_group_by_parameter.py`；Modify `V/metricflow/engine/metricflow_engine.py`（MetricFlowQueryRequest.create、with_request_id）；Create `V/tests_metricflow/engine/test_bound_query_request.py`。

**Interfaces:** 提供 frozen `MetricGroupByBinding(metric_name: str, parameter: GroupByQueryParameter)`、`BoundGroupByParameter(name: str, bindings: tuple[MetricGroupByBinding, ...])`；类型别名 `BoundGroupByInput = GroupByQueryParameter | BoundGroupByParameter`。给 request/create 增加可空字段 `bound_group_by: tuple[BoundGroupByInput, ...] | None`（create 接受 Sequence 并转 tuple），参数追加到 create 签名末尾以保护旧 positional 调用。

- [ ] 写失败测试 `test_request_preserves_bound_inputs_when_copied`、`test_old_request_defaults_are_unchanged`，断言如下；复用既有参数类创建 buyer/seller 样例，不实现另一套路径 parser。
  ```python
  assert copied.bound_group_by == request.bound_group_by
  assert old_request.bound_group_by is None
  assert old_request.group_by_names == ["customer__country"]
  ```
- [ ] 在 V 运行 `hatch run dev-env:pytest tests_metricflow/engine/test_bound_query_request.py -q`，确认新增字段/复制失败。
- [ ] 实现参数记录及 request 所有构造/复制入口，保持输入原顺序和不可变元组；此任务只存储，不在构造器访问 manifest。公共参数记录不得包含服务 import。
- [ ] 重跑上述测试和 `hatch run dev-env:pytest tests_metricflow/engine/test_explain.py -q`；预期全部通过，旧请求行为不变。
- [ ] 提交 `feat: add native bound group by request parameters`。

### Task 2: 公共输出键及解析结果的类型闭包

**Files:** Create `V/metricflow_semantics/specs/bound_output_key_spec.py`、`V/metricflow_semantics/specs/bound_query_spec.py`；Modify `V/metricflow_semantics/specs/instance_spec.py`、`spec_set.py`、`spec_set_transforms.py`、`query_spec.py`、`column_assoc.py`、`dunder_column_association_resolver.py`、`V/metricflow_semantics/instances.py`、`V/metricflow/plan_conversion/instance_set_transforms/select_columns.py`、`instance_converters.py`、`V/metricflow/plan_conversion/to_sql_plan/output_column_orderer.py`；Create `V/tests_metricflow_semantics/specs/test_bound_output_key.py`（若 specs 测试目录不存在则创建）。

**Interfaces:** `BoundOutputKeySpec(InstanceSpec)` 增加 `position: int, kind: str, grain: str | None`，kind 仅 categorical/time/entity，element_name 是最终列名；它不是 LinkableInstanceSpec。`BoundOutputKeyInstance` 单独进入 InstanceSet，不冒充 DimensionInstance。`BoundKeySource(output_key: BoundOutputKeySpec, source_spec: LinkableInstanceSpec)`；`BoundMetricSpec(metric_spec: MetricSpec, key_sources: tuple[BoundKeySource, ...], source_query_spec: MetricFlowQuerySpec)`；`BoundQuerySpec(output_keys: tuple[BoundOutputKeySpec, ...], metric_bindings: tuple[BoundMetricSpec, ...])`。MetricFlowQuerySpec 新增 `bound_query_spec: BoundQuerySpec | None = None`；source_query_spec 必须为无嵌套绑定、无最终排序/limit 的已校验原生规格。

- [ ] 写失败测试 `test_bound_key_is_not_source_dimension`、`test_duplicate_source_keeps_two_output_slots`、`test_bound_spec_survives_copy_and_serialization`，断言：
  ```python
  assert country_key != buyer_country_spec
  assert [key.element_name for key in output_keys] == ["country", "country_copy"]
  assert restored.bound_query_spec == original.bound_query_spec
  assert updated_time.bound_query_spec == original.bound_query_spec
  ```
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow_semantics/specs/test_bound_output_key.py -q`，确认新类型缺失的红灯。
- [ ] 实现上述类型、spec/instance visitor 分派、集合合并/筛选/去重、列解析与序列化。原有 linkable_specs 不包含公共键；InputSpecOrder 可保存公共键。公共键列名直接来自其 element_name，来源路径只存在 BoundKeySource；同名同类型不同槽位仍需通过输出名称冲突校验。
- [ ] 覆盖 `OrderBySpec(instance_spec=country_key, descending=True)`、Selector 和输出列排序，确保原生输出机制消费公共键；重跑目标测试及 `hatch run dev-env:pytest tests_metricflow/plan_conversion/instance_converters -q`，预期通过。
- [ ] 提交 `feat: model bound output key identity and query specs`。

### Task 3: 原生绑定解析、校验与共享过滤作用域

**Files:** Create `V/metricflow_semantics/query/bound_query_resolver.py`、`V/tests_metricflow_semantics/query/test_bound_query_parser.py`；Modify `V/metricflow_semantics/query/query_parser.py`（parse_and_validate_query）、`V/metricflow/engine/metricflow_engine.py`（_create_execution_plan）。

**Interfaces:** parser 的旧签名末尾增加 `bound_group_by: tuple[BoundGroupByInput, ...] | None = None`，返回仍为 ParseQueryResult。`BoundQueryResolver.resolve(base_query_spec: MetricFlowQuerySpec, bound_group_by: tuple[BoundGroupByInput, ...], source_query_specs: tuple[MetricFlowQuerySpec, ...]) -> BoundQuerySpec` 仅装配已解析规格；路径解析通过现有 parser/resolver 的普通模式完成，不引入替代语义图。engine 将 bound_group_by 转发到 parser。

- [ ] 写 `test_binding_resolves_each_metric_path` 和参数化非法请求测试，包含缺失/多余/重复绑定、非法路径、时间缺 grain/不同 grain、分类与时间混合、源 alias、名称冲突、两种 group_by 同传、只有共享项的 bound_group_by、未知排序，以及 spec 列出的全部不支持模式。断言例：
  ```python
  assert [item.metric_spec.element_name for item in bound.metric_bindings] == ["sales_amount", "refund_amount"]
  assert bound.metric_bindings[0].key_sources[0].source_spec.dunder_name == "buyer__country"
  assert bound.metric_bindings[1].key_sources[0].source_spec.dunder_name == "seller__country"
  assert parsed.query_spec.order_by_specs[0].instance_spec == bound.output_keys[0]
  ```
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow_semantics/query/test_bound_query_parser.py -q`，观察绑定未解析的失败。
- [ ] 实现解析：先按全指标集合解析共享项/共享过滤，绑定项逐请求指标解析，再组装 source_query_spec；规范化指标名后校验覆盖，重复绑定禁止被 dict 静默覆盖。共享过滤必须先通过全指标作用域验证，再为 source_query_spec 建立一致的单根解析 lookup，保证 query/definition/input FilterLocation 完整。不得将多条 lookup 无条件覆盖合并。源 query 不含公共名、不含最终排序/limit、不再递归进入绑定分支。
- [ ] 对输出名称应用 `[a-z][a-z0-9_]{0,62}`；物理类型不强制转换；共享 entity 允许，绑定 entity 拒绝。重复请求指标按原生规范化去重并稳定保序。名字排序解析为公共键/指标，旧结构化排序对象按可解析的原生字段语义处理，不能把公共键发回语义图选路。不支持组合在 engine 可获得模式信息的入口检查。
- [ ] 写并运行 `test_shared_filter_is_validated_for_all_metrics_then_resolved_per_root`、`test_public_alias_is_not_a_source_where_path`、`test_mixed_order_and_duplicate_source_slots`；重跑 `hatch run dev-env:pytest tests_metricflow_semantics/query/test_query_parser.py tests_metricflow_semantics/query/test_bound_query_parser.py -q`，预期全部通过。
- [ ] 提交 `feat: resolve per metric group by bindings in query parser`。

### Task 4: 绑定合并节点与 Dataflow 输出不变量

**Files:** Create `V/metricflow/dataflow/nodes/combine_bound_output.py`、`V/tests_metricflow/dataflow/test_combine_bound_output.py`；Modify `V/metricflow/dataflow/dataflow_plan_visitor.py`、`V/metricflow/plan_conversion/node_processor.py`、`V/metricflow/dataflow/optimizer/source_scan/source_scan_optimizer.py`、`V/metricflow_semantics/dag/id_prefix.py`，以及通过 visitor 搜索发现的输出推导/节点遍历具体实现。

**Interfaces:** frozen `BoundParentBinding(key_sources: tuple[BoundKeySource, ...], metric_specs: tuple[MetricSpec, ...])`；`CombineBoundOutputNode.create(parent_nodes: Sequence[DataflowPlanNode], output_keys: tuple[BoundOutputKeySpec, ...], parent_bindings: tuple[BoundParentBinding, ...]) -> CombineBoundOutputNode`。绑定元组与父节点按位置一一对应；`reorder_parents(permutation: tuple[int, ...]) -> CombineBoundOutputNode` 同时重排两者；现有 `with_new_parents` 只允许等数量、按原槽位替换，不负责猜重排意图。

- [ ] 写 `test_node_preserves_mapping_when_parents_reordered`、`test_single_parent_is_projection`、`test_different_bindings_are_not_equivalent`、`test_output_instances_exclude_internal_inputs`；断言重排两次恢复原映射，缺键/重复指标来源构造失败，单父节点合法，输出仅公共键和请求指标。
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow/dataflow/test_combine_bound_output.py -q` 确认红灯。
- [ ] 实现节点、显示属性、visitor、输出实例推导、等价性及稳定绑定身份；父分支必须具有源规格和请求指标。保留旧 CombineAggregatedOutputsNode 的断言。检查所有 generic clone/rewriter 的调用约定；需要重排的优化显式调用 reorder_parents。
- [ ] 将新节点作为优化边界：允许递归优化父分支，不跨边界下推谓词、聚合、limit；SQL visitor 在 Task 7 实现前可明确拒绝访问该未接入构建器的节点，不能返回虚假计划。运行目标测试和 `hatch run dev-env:pytest tests_metricflow/dataflow -q`，预期通过。
- [ ] 提交 `feat: add bound output dataflow node and invariants`。

### Task 5: 按请求级指标传递求值上下文并构建分支

**Files:** Modify `V/metricflow/metric_evaluation/metric_query_planner.py`、`dfs_me_planner.py`、`passthrough/passthrough_me_planner.py`、`plan/me_nodes.py`、`plan/me_plan.py`、`V/metricflow/dataflow/builder/dataflow_plan_builder.py`；Create `V/tests_metricflow/metric_evaluation/test_bound_metric_context.py`、`V/tests_metricflow/dataflow/builder/test_bound_query_plan.py`。

**Interfaces:** builder 新增 `_build_bound_query_output_node(query_spec: MetricFlowQuerySpec, option_set: DataflowPlanOptionSet) -> DataflowPlanNode`，消费 Task 2 的 BoundQuerySpec。每个 source_query_spec 复用原生求值规划入口，携带自身 group_by specs、筛选 lookup、time range。可在同规格及相同解析上下文的根之间按原生兼容条件一起求值；最终一个 Dataflow DAG，不执行独立 explain。扩展内部转换 helper 接收同请求共享的 ResultCache，缓存键仍为完整 BuildAnyMetricOutputNodeInput。

- [ ] 写 `test_top_level_sales_and_derived_input_have_distinct_paths`、`test_derived_and_ratio_inherit_root_group_by`、`test_dfs_and_passthrough_preserve_bound_roots`，分别在两种求值策略下断言 sales 的 buyer/seller 两个计算上下文都存在，derived 内部输入保持根路径，错误路径返回原生输入错误。
- [ ] 写 `test_compatible_same_source_metrics_still_share_scan`：以旧原生同路径查询为基线，开启相同优化配置后绑定模式的源扫描数不增加；`test_different_paths_do_not_create_cross_product_grouping` 断言每根只有其请求源规格。
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow/metric_evaluation/test_bound_metric_context.py tests_metricflow/dataflow/builder/test_bound_query_plan.py -q`，确认未构建独立根导致失败。
- [ ] 实现绑定 builder，复用原有 simple/derived/ratio/cumulative/conversion 分支函数。内部 offset、累计窗口、非可加维度的中间规格仍由原生计算；SourceQuerySpec 到求值根的过滤 lookup 不混用。先完成各请求指标，再用 Task 4 节点统一输出，禁止在绑定节点内部重新解释公式。
- [ ] 更新 sink 对公共输出键的投影、排序和输入顺序处理；binding 模式不调用来源维度的输出 value_mapping 绑定。此时测 Dataflow 构建，不要求 SQL 执行。重跑上述测试及 `hatch run dev-env:pytest tests_metricflow/metric_evaluation -q`，预期通过。
- [ ] 提交 `feat: plan bound metric roots with isolated group by contexts`。

### Task 6: 结构化 UNION DISTINCT SQL 节点

**Files:** Create `V/metricflow/sql/sql_union_node.py`、`V/tests_metricflow/sql/test_union_node.py`；Modify `V/metricflow/sql/sql_plan.py`、`sql/render/sql_plan_renderer.py`、`sql/optimizer/table_alias_simplifier.py`、`rewriting_sub_query_reducer.py`、`column_pruning/column_pruner.py`、`column_pruning/node_to_column_alias_maping.py`、`column_pruning/required_column_aliases.py`、`column_pruning/cte_mapping_lookup_builder.py`、`V/metricflow_semantics/dag/id_prefix.py`，以及 SqlPlanNodeVisitor 其他具体实现。

**Interfaces:** `SqlUnionDistinctNode.create(sources: Sequence[SqlSelectStatementNode]) -> SqlUnionDistinctNode`；源至少两个，键投影列序/数量一致，普通 UNION 即去重。不提供本次不需要的 UNION ALL 配置。renderer 的 `visit_union_distinct_node` 返回既有 renderer 的片段类型，参数集合使用既有合并机制。需要 checked_sql_select_node 的位置用外层 SELECT 包装该节点。

- [ ] 写 `test_union_distinct_keeps_null_and_removes_duplicate_keys`、`test_union_preserves_parameters_and_column_order`、`test_union_survives_all_sql_optimization_levels`；用两个小型结构化 SELECT 子树，键集合期望 `{("CN",), ("US",), (None,)}`，验证重复键去重、两个独立绑定参数均保留、每个优化等级结果等价。
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow/sql/test_union_node.py -q`，确认缺失 SQL 节点的红灯。
- [ ] 实现 AST、遍历、copy/with-new-parent、renderer、CTE 与 alias/参数传播；裁列对 UNION 的所有输入保持同一列序。UNION DISTINCT 内去重键集合不可因父层只投影部分字段就提前裁掉参与去重的列。优化器可保守保留 UNION 边界，不能绕过子树遍历或改成文本节点。
- [ ] 通过原有多方言 fixture 生成 DuckDB/Postgres 等快照（`hatch run dev-env:pytest tests_metricflow/sql/test_union_node.py --overwrite-snapshots -q`），检查生成结果后重跑无更新选项的目标测试和 `hatch run dev-env:pytest tests_metricflow/sql -q`。
- [ ] 提交 `feat: add structured union distinct sql plan support`。

### Task 7: SqlDataSet 对齐转换与完整原生执行链

**Files:** Create `V/metricflow/plan_conversion/to_sql_plan/bound_output_sql.py`、`V/tests_metricflow/plan_conversion/test_bound_output_sql.py`；Modify `V/metricflow/plan_conversion/to_sql_plan/dataflow_to_subquery.py`、`dataflow_to_cte.py`、`V/metricflow/engine/metricflow_engine.py`（执行计划与 value_mapping）、`V/metricflow/dataflow/builder/dataflow_plan_builder.py`（最终 sink）。

**Interfaces:** 两条转换 visitor 实现 `visit_combine_bound_output_node(node: CombineBoundOutputNode) -> SqlDataSet`。同一文件内 helper `build_bound_output_sql(node: CombineBoundOutputNode, parents: tuple[SqlDataSet, ...], column_resolver: ColumnAssociationResolver, aliases: tuple[str, ...], key_alias: str) -> SqlDataSet`；alias 由现有 visitor 的单一分配器产生，不由 helper 全局计数。

- [ ] 写 `test_bound_output_two_and_three_branches_align_null_safe`、`test_single_branch_projects_without_join_or_union`、`test_duplicate_source_columns_keep_output_names`。用真实 DuckDB 执行 renderer 输出，覆盖复合键、一个分支为空、所有分支为空，断言输出至多每完整键一行、缺失指标 None、同一 NULL 元组对齐。
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow/plan_conversion/test_bound_output_sql.py -q`，确认绑定 visitor 尚未实现的失败。
- [ ] 按 spec 构造各父投影、UNION DISTINCT 键集合、逐父空值安全 LEFT JOIN、最终投影；每个键为 equality OR both-null，组合键 AND。依据 source_spec 查询实际 associated_column；生成 BoundOutputKeyInstance，不复用第一父维度身份。单父只 SELECT，派生内部指标不输出。
- [ ] CTE 路线使用既有 `_default_handler` 与同一语义 helper；确保父结果只转换一次并通过合法 CTE/子树复用。两种路线和各优化等级必须有效。所有绑定参数由原生 SqlStatement 传到执行客户端；不得沿用服务层参数拒绝。
- [ ] 写 `test_query_and_explain_share_one_plan`、`test_final_limit_is_not_applied_to_branches`、`test_binding_disables_only_output_mapping`：原生 limit=2 返回至多2行，不加1；EXPLAIN 不执行客户端 query；QUERY 结果取数一次；source mapping 配置仍存在且普通请求仍有展示标签。重跑目标测试及现有 `tests_metricflow/engine/test_explain.py`、`tests_metricflow/plan_conversion/test_value_mapping_boundary.py`。
- [ ] 提交 `feat: compose bound output sql subtrees in native engine`。

### Task 8: 原生真实指标语义与服务旧实现对照

**Files:** Create `V/tests_metricflow/engine/test_bound_query.py`；Modify `tests/test_bound_query_integration.py` 增加直接原生调用对照（此时保留旧服务路径）；扩展现有 fixture 使用的模型定义，仅添加该测试需要的数据和指标。

**Interfaces:** 直接通过 `MetricFlowQueryRequest.create(bound_group_by=...)` 调用引擎，使用 Tasks 1–7 的公共入口；不新增产品接口。测试中按列名和完整键归一结果，不依赖随机 alias 或 SQL 文本相等。

- [ ] 写真实查询测试：跨 orders/refunds 两个事实表的公共/独立 entity 路径、derived/ratio、cumulative/conversion、共享筛选/时间、重复显示标签、最终排序/截断。原有 fixture 的买家国家期望 revenue CN=30、US=40、NULL=30；卖家国家 refunds US=1、JP=2、NULL=3、DE=4，缺失值 None。
  ```python
  assert rows["CN"]["revenue"] == 30
  assert rows["CN"]["refunds"] is None
  assert rows[None]["refunds"] == 3
  assert actual_native_rows == actual_legacy_rows
  ```
- [ ] 增加同一源指标同时为顶层请求与 derived 输入的结果断言；累计/转换复用仓库已有有效样例，对照各指标普通单查结果按公共键手工对齐后的期望，避免只检查节点数量。无效继承路径明确失败，不能静默省略分支。
- [ ] 运行 `hatch run dev-env:pytest tests_metricflow/engine/test_bound_query.py -q`；仓库根运行 `.venv/Scripts/python.exe -m pytest tests/test_bound_query_integration.py -q`，新增场景先出现目标语义红灯后，仅修正其归属的生产组件，直至全部通过。
- [ ] 两种求值策略、CTE/subquery 模式分别运行关键 NULL/derived/过滤用例；用 spy 统计真实 SQL client 结果查询次数，不替换计算逻辑。Postgres/StarRocks renderer 可验证即验证；实库无连接时登记未验收，不能阻断 DuckDB 已授权测试或声称全数据库通过。
- [ ] 提交 `test: verify native bound queries against real metric results`。

### Task 9: 服务薄适配、删除旧规划与完整回归

**Files:** Create `S/application/query_bindings.py`；Modify `S/application/query_plan.py`、`S/engines/models.py`、`S/engines/metricflow.py`、`S/execution/query.py`、`tests/test_bound_queries.py`、`tests/test_bound_query_integration.py`、`tests/test_query_execution.py`、`README.md`；Delete `S/application/bound_query_plan.py`、`S/engines/bound_queries.py` 及已被原生替代且无剩余公共契约断言的 `tests/test_bound_query_sql.py`。

**Interfaces:** 在 engines/models.py 定义 `ResolvedBoundPath(token: str, grain: str | None, dimension_type: str)` 与 `ResolvedGroupByBinding(name: str | None, paths: dict[str, ResolvedBoundPath])`，均为服务内部 Pydantic 记录；paths 覆盖请求指标，name=None 表示共享项且路径相同。CompiledQuery 增加 `bound_group_by: list[ResolvedGroupByBinding] | None = None`。`resolve_query_bindings(request: MetricQueryRequest, common: dict[str, ResolvedDimension], per_metric: dict[str, dict[str, ResolvedDimension]]) -> CompiledQuery` 仅解析选项/排序，不归候选组。MetricFlowClient.query 的原签名不变，在 client 内将服务记录转换成原生 bound_group_by。

- [ ] 先把服务测试改成断言原生 request 的绑定映射、混合顺序、共享排序、limit+1，不再断言服务候选组数量或 mf_group_N 字符串。写 `test_executor_calls_one_native_query_for_bound_request`、`test_old_group_by_still_uses_old_native_parameters`、`test_option_from_wrong_build_or_metric_is_rejected`，运行 `.venv/Scripts/python.exe -m pytest tests/test_bound_queries.py tests/test_query_execution.py -q` 确认目标红灯。
- [ ] 实现纯适配：共享筛选仍由 compile_query 的现有逻辑解析；绑定项从 per_metric 取选项，共享项从 common 取；无绑定保持旧流程。`execution/query.py` 只准备选项、调用 resolver 后进入同一个 client.query。原生异常沿用服务业务错误适配，连接/内部错误保持执行失败。
- [ ] 删除 BoundQueryPlan、BoundQueryGroup、BoundGroupBy（旧执行计划类）、compile_bound_query、compile_groups、merge_sql、query_bound、_compile_bound_group 以及 mapping_raw 包装剥离；清理本次造成的未用导入。保留公开 BoundDimension/SelectedDimension，不改 HTTP JSON 或 available-dimensions。用 `rg -n 'compile_bound_query|query_bound|merge_sql|BoundQueryPlan|BoundQueryGroup|_compile_bound_group' src tests` 确认产品与服务测试无旧调用。
- [ ] 将 Task 8 临时双实现对照测试改为固定期望 + 原生/服务结果一致，不保留旧产品实现作测试依赖；更新 README 的内部责任说明、绑定用法、原始键/NULL/过滤语义和新 spec 链接。历史 spec 不改写为当前实现。
- [ ] 运行服务全量 `.venv/Scripts/python.exe -m pytest -q`、`.venv/Scripts/ruff.exe check src tests`；V 下运行 `make lint`、`make test`。新快照只用 Hatch overwrite-snapshots 生成并审阅；无绑定快照不得因绑定功能出现无关变化。若 Windows 无 make/Hatch 或缺外部连接，记录具体命令与阻塞原因，按 Makefile 等效目标可执行部分验证并明确未完成项。
- [ ] 验证 git diff 仅涉及本功能；记录各命令退出码、通过/跳过数、实库范围、优化前后结果证据；提交 `refactor: delegate bound queries to native metricflow`。最后做整分支审查，不能以计划复选框替代测试证据。

## 覆盖自查与交付

| Spec 条款 | 任务 |
| --- | --- |
| 输入兼容、不可变绑定、复制、公共输出身份 | 1–3 |
| 请求级继承、两种求值策略、缓存/优化正确性 | 4–5、8 |
| 新节点、SQL 子树/UNION、CTE/subquery、NULL与参数 | 4、6–8 |
| 公共过滤、时间、原始键展示、排序/limit | 3、5、7–9 |
| 一次原生调用、删除旧服务规划、旧 API 回归 | 8–9 |
| 错误、unsupported 模式、方言与验证限制 | 3、7–9 |

计划已做书面自查：接口名称按上述定义贯穿；公共键独立于源维度；五项 Review Focus 均有对应测试；SQL 优化器 visitor、request/spec 复制与服务删除均有所属任务。本轮仅计划编写，未运行实现测试、未宣称功能完成。执行前由用户审阅计划并选择执行方式。
