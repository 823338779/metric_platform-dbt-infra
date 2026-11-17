# MetricFlow 原生每指标维度通路绑定

状态：待用户审批；本文件描述目标设计，不表示代码已实现。

## 1. 意图与已确认决策

将服务现有的每指标维度通路能力下沉到仓库内 `vendor/metricflow`：在查询解析和指标求值阶段确定各指标的分组路径，在 Dataflow DAG 中表达分支与公共键映射，在 SQL Plan 转换中组装结构化子树，统一渲染和执行。

用户已确认：

- 每个“请求级指标 × 输出维度”最终对应一条明确的通路；共享输入可以展开，不要求调用方重复填写。
- 只支持请求级指标绑定。派生指标的全部内部输入继承该请求指标的路径；不能在公式内部给不同输入另配通路。
- 采用新增绑定合并节点的方案，保持现有 `CombineAggregatedOutputsNode` 的相同分组规格约束。

成功标准：服务只负责 optionId 解析和原生请求适配；直接调用 MetricFlow 的 query/explain 也具备同样能力。一次请求生成一个完整 DataflowPlan、一个最终 SQL Plan；QUERY 一次结果取数，EXPLAIN 不执行取数。指标公式、entity 合法关联和方言渲染仍由原生负责。

## 2. 现状与方案选择

现有服务通过 `application/bound_query_plan.py` 解析绑定并按完整路径归组，通过 `engines/bound_queries.py` 反复 explain 验证组合并拼接 SQL。`MetricFlowClient._compile_bound_group` 还会移除展示映射包装以保留原始关联键。

原生已有基础：

- `MetricFlowQueryRequest` 保存输入；`MetricFlowQuerySpec` 保存解析后的规格。
- `DataflowPlanBuilder._build_query_output_node` 将统一的分组规格传给 MetricEvaluationPlanner。
- 指标分支构建函数接收 `queried_linkable_specs`，能规划具体路径的选源、entity JOIN、聚合及计算。
- `CombineAggregatedOutputsNode` 转换时要求父分支 linkable specs 相同；转换器通过 SqlDataSet 的实例与列映射构造 SqlSelectStatementNode 子树。

### Approaches

| 方案 | 取舍 | 决策 |
| --- | --- | --- |
| 新增绑定合并节点 | 独立表达不同规格对齐，保留旧节点不变量；需补齐 visitor、输出规格及优化器支持 | 用户已选择 |
| 扩展旧合并节点的可选映射 | 少一个节点类型，但旧节点的等价性、校验与优化规则均需重新审视 | 不采用 |
| 将服务的 explain 与 SQL 拼接搬入引擎 | 位置变化，不能形成理解绑定语义的统一计算计划 | 不满足目标 |

以下命名、原生参数形状、SQL 关联策略和首版边界是设计建议，随本 spec 一并审批；不是额外声称用户已经逐项确认。

## 3. 范围与兼容性

- 服务现有 `{optionId, grain}`、`{name, bindings}` 及混合输入保持兼容；选项仍来自同一 build。绑定选项按单指标获取，共享项和 filters 按完整指标集合获取。
- 无绑定请求保持原有解析、Dataflow、展示映射与执行行为。
- 支持原生已经支持的请求级指标类型，包括 simple、derived、ratio、cumulative、conversion；某类指标对路径或时间的既有约束仍生效。继承的是调用方分组要求，内部时间脊、offset 等必要中间粒度仍由原生决定。
- 首版绑定项支持普通分类维度和时间维度；共享项沿用已有 entity 分组能力。原生绑定模式不扩展 group-by-metric、saved query、distinct/dimension-values、min/max、apply_group_by=False、output_table，组合使用时在入口明确报不支持；无绑定时这些能力保持原状。
- 不增加每指标独立筛选，不增加值转换、自动业务等价推断或新 entity JOIN 规则，不要求跨分支最优扫描数。
- 不变更 available-dimensions 的响应结构；optionId 不进入 MetricFlow 内部。

## 4. 输入契约

保留原生 `group_by_names`、`group_by`。新增一个有序参数 `bound_group_by`，表示绑定模式下完整的输出分组列表，其元素为共享原生 GroupByQueryParameter 或新的 BoundGroupByParameter。`bound_group_by` 与旧两种 group_by 入参互斥；同一列表允许共享项和绑定项混用，从而保留交错输入顺序。

概念示例（类名以实现时项目命名约定为准）：

```python
MetricFlowQueryRequest.create(
    metric_names=["sales_amount", "refund_amount"],
    bound_group_by=(
        BoundGroupByParameter(
            name="country",
            bindings=(
                MetricGroupByBinding("sales_amount", DimensionOrEntityParameter(name="buyer__country")),
                MetricGroupByBinding("refund_amount", DimensionOrEntityParameter(name="seller__country")),
            ),
        ),
        TimeDimensionParameter(name="metric_time", grain="month"),
    ),
    order_by_names=["country"],
    limit=101,
)
```

采用不可变有序记录而非可变 dict 存储原生绑定；HTTP bindings 字典在服务适配时转换。请求复制、with_request_id、序列化与测试辅助构造必须保留新增字段。

校验规则：

1. 每个绑定项恰好覆盖去重后的请求指标；拒绝重复、缺失、多余指标。单指标请求也允许绑定。bound_group_by 至少包含一项绑定。
2. 公共 name 为 `[a-z][a-z0-9_]{0,62}`，与所有输出维度及指标名称不冲突。源参数不允许另带与公共 name 竞争的 alias。
3. 同一输出键的源类型一致；时间键必须显式相同粒度。共享时间项在绑定模式下也要求显式粒度，延续服务契约。
4. 路径逐请求级指标解析，时间粒度逐指标验证；不要求不同指标的绑定路径属于全指标共同维度交集。
5. 分类键仅验证语义类型，物理类型兼容性由调用方保证，数据库类型错误按执行错误返回；不暗中 CAST。
6. 原生排序按指标名或最终公共输出名称解析；服务把共享 optionId 排序映射到相应输出名称。公共输出名称不是可用于源查询 WHERE 的维度路径。
7. 相同源规格可映射到不同公共输出键，但输出名称必须唯一；分支可按去重后的源规格聚合，输出键映射仍保留每个槽位。

## 5. 解析结果与公共键身份

绑定模式的解析结果必须同时保留三种信息，不能把公共 name 伪装成语义模型中的真实维度：

- 输出键列表：请求顺序、输出名称、类别、时间粒度、稳定的查询内键身份。
- 每个请求指标的源分组规格列表：已解析的 DimensionSpec / TimeDimensionSpec / 共享 EntitySpec。
- 每个输出键到各请求指标源规格的映射，以及各分支原生筛选解析上下文。

建议增加查询级 BoundOutputKeySpec 及对应输出实例类别。其身份由规范化输出槽位/名称、类型、粒度确定；不以源 entity 路径为身份。来源规格单独记录用于映射与 explain。公共键是结果规格，不参与语义图选路、源选择或指标定义解析。

MetricFlowQuerySpec 的旧 linkable_specs 不能用来冒充所有分支的路径集合。新增绑定规格字段，绑定构建入口显式读取；with_time_range_constraint 等复制方法保留它。InputSpecOrder、列关联解析器、InstanceSet/SpecSet 的转换、Selector、OrderByLimit、输出列排序以及序列化必须识别公共键。精确类层次可由实现者选择，但不得用第一个分支的真实维度规格代表所有来源。

## 6. 指标求值与 Dataflow 构建

解析完成后按请求级指标生成独立求值根，每个根具有自己的完整分组规格。递归展开 derived/ratio 时，内部输入继承该根的分组要求，保留原有定义级过滤、offset、时间脊、非可加维度等语义。

例：请求同时包含 `net_revenue` 和 `sales_amount`，其中 net_revenue 的输入也包含 sales_amount。若两者绑定不同路径，它们必须是不同求值上下文；不能仅按指标名缓存并复用同一分支。

DFS 与 PassThrough 两种求值规划策略都需要支持这种上下文。缓存和分支合并的身份必须包含原生计算上下文中的分组规格、筛选、时间/offset 与优化配置；改变绑定路径不得命中旧分支。路径相同也只有在原生兼容条件满足时才能合并。

首版以逐请求级指标构建正确分支为基线，在同一请求内复用原生缓存与合法的同源优化。不得将不同路径合并成维度笛卡尔组合，不以反复 explain、捕获 InvalidQueryException 做主规划流程。无需保证全局最少分支数，但既有兼容同源分支合并不得仅因启用绑定模式无故失效。

完成请求级指标计算后创建新节点，建议命名 `CombineBoundOutputsNode`：

```text
parent_nodes: 有序的已计算指标分支
output_keys: 有序公共键规格
parent_bindings:
  parent_index → [(output_key, source_spec), ...]
metric_outputs:
  requested_metric_spec → parent_index
```

映射以父分支和实际 Spec 为依据，不依赖 SQL 列名或仅依赖指标字符串。每个父分支必须提供全部公共键对应的源规格；每个请求指标恰有一个输出来源。一个父分支可以包含多个兼容指标；一个父分支也允许新节点存在，此时节点只作键投影和输出规格转换。

各父分支必须已在完整分组粒度上至多一行；在此之前不得有最终 limit。不同分支的实际源键统一映射成公共键后的完整元组，用于最终对齐。此节点不展开公式，不将不同分支明细关联。

节点等价性/哈希、with_new_parents、DAG 格式化、输出实例推导和全部 visitor 必须处理映射。优化器可优化父分支；首版把该节点视为对齐边界，不跨边界下推谓词、聚合或 limit。重排父节点必须同步重写映射，不能仅复制原 parent_index。

## 7. SQL Plan 子树组装

转换器读取各父节点的 SqlDataSet（SQL 子树 + InstanceSet + 关联列），通过 source_spec 获取实际列，通过公共键规格生成输出列关联。不解析 SQL 文本，不使用显示名猜列，不先渲染各组字符串再拼接。

首版选择与现有服务一致的结构化 SQL 策略：

1. 对各分支投影原始分组键，统一公共键列名；保留请求指标列。
2. 多分支：用 UNION DISTINCT 的键子树得到所有键元组；逐分支 LEFT JOIN。
3. 每个键的关联条件为 `left = right OR (left IS NULL AND right IS NULL)`，多个键用 AND。空分支不丢失其他分支的键；指标缺失保留 NULL，不自动补 0。
4. 单分支：只投影及重命名，不生成键集合或 JOIN。
5. 构造合并后的 SqlDataSet，其输出实例只包含公共键与请求指标；派生指标内部输入不得泄漏。

复用已有 SQL AST 的 select、join、表达式节点和方言 renderer。当前源码未发现现成 SQL UNION 节点，因此实施计划必须包含结构化 UNION DISTINCT 节点、visitor/renderer、列序与参数传播、优化遍历和快照测试，不能用 SqlSelectTextNode 或字符串拼接替代。数据流到 subquery 和 CTE 两条转换路线均需支持，CTE 路线复用同一语义转换并负责子树引用；重复引用使用同一转换上下文的 alias/CTE 分配，不合并多个独立 explain 产生的别名空间。

此策略延续服务对 NULL 和跨方言的既有语义，避免直接放宽原有 FULL OUTER JOIN 合并器不变量。将来可在结果等价测试支持下替换实现；首版不承诺扫描次数最优。

排序和 limit 在新节点之后通过原生 sink 构建；原生 limit 使用调用方给定值，服务仍传 `用户 limit + 1` 并负责 resultTruncated。整个请求使用同一个 SQL client 和原生执行链。绑定参数通过结构化 SQL 计划正常传播，不沿用服务的“发现参数即拒绝”逻辑。

## 8. 筛选、时间和展示语义

查询级 WHERE 保持全指标共同可用路径的现有语义，并在每个分支应用。解析时按完整请求指标集合验证共享筛选，构建时向各求值上下文传递可用的筛选规格/查找上下文；不能切到单指标作用域后丢失查询级 FilterLocation，也不能把绑定 country 别名自动改写成各路径过滤。

start/end 时间约束作用于所有分支，继续遵循各指标原生的 metric_time、offset、累计窗口等规则；共享 metric_time 不代表各指标使用同一物理时间字段。

绑定模式统一返回原始维度键，包括共享项，延续现有接口语义。解析/计划构建阶段明确关闭该请求的输出 value_mapping 包装；不删除模型映射配置，不影响可能存在的原生过滤语义，不在 SQL 生成后剥离 mapping_raw。无绑定模式保持现有展示映射。重复显示标签必须不能合并不同原始键。

## 9. Explain、错误与服务迁移

query/explain 共用绑定解析和计划路径。Explain 的 QuerySpec、Dataflow 图必须能显示每个请求指标的源路径及公共键映射，最终 SqlPlan 和返回 SQL 一致。不能仅暴露多个互不关联的子计划。

用户错误在原生查询错误体系中报告，包含可定位的指标、输出键、源路径和原因（缺失绑定、非法路径、不兼容粒度、未知排序或不支持模式）。内部规格不一致、未实现 visitor 和数据库连接错误不能作为拆分信号或伪装成输入非法。

服务迁移后的职责：

1. 同 build 下解析单指标 optionId、共享 optionId、filters，保留现有 API 校验。
2. 将绑定列表按输入顺序转换成原生 bound_group_by；转换共享排序字段；传递过滤、时间和 limit+1。
3. 调用一次原生 query 或 explain，沿用现有错误适配与结果序列化。

在新路径验收通过的同一迁移变更中删除服务自有 BoundQueryPlan/BoundQueryGroup、候选分组、贪心 explain、merge_sql、query_bound 及展示包装剥离代码；保留公开 BoundDimension/SelectedDimension 请求模型和 optionId 解析能力。仅清理因此失去用途的代码与测试。旧设计文档保留为历史，新 README 指向本设计与原生实现。

## 10. 验收与验证证据

| 层次 | 必须验证的行为 |
| --- | --- |
| 原生契约/解析 | 直接调用引擎，无服务依赖；完整覆盖、重复/多余绑定、名称冲突、混合顺序、类型/粒度、非法路径、复制/序列化保留字段、不支持组合明确失败 |
| 求值 | 同一底层指标在顶层和 derived 输入中使用不同路径时不错误复用；derived/ratio 输入继承路径；累计和 conversion 原有约束保留；DFS 与 PassThrough 结果一致 |
| Dataflow | 不同路径形成不同上下文；兼容同源计算仍可复用；新节点包含完整键映射；单分支投影；节点等价性、父节点重排、输出实例与 explain 正确 |
| SQL Plan | subquery/CTE 两条路线；2/3 分支、复合键、NULL、空分支、单侧键；别名唯一；绑定参数保留；指标列与公共键列不泄漏或冲突 |
| 真实结果 | DuckDB 构建 orders/refunds/customer 等真实模型验证多通路查询；与现有服务绑定模式基准结果等价，比较结果及截断，不要求 SQL 文本相同 |
| 回归 | 原有无绑定的 SQL/计划快照及结果；value_mapping 的重复标签保留原始键；公共筛选与时间范围；最终排序、limit；EXPLAIN 零取数、QUERY 一次取数 |
| 方言 | 仓库已有受支持方言渲染测试，重点 PostgreSQL/StarRocks；真实连接不可用时明确记录未验收，不能将渲染成功等同于实库成功 |

原生测试遵守 `vendor/metricflow/AGENTS.md`：使用 hatch，快照通过 overwrite-snapshots 生成，最终运行 make lint / make test；服务运行项目 pytest 与 Ruff。环境不满足时记录具体未运行项和原因，不假定通过。本次只编写设计，尚无实现测试结果。

## 11. 实施边界与留给实现者的决策

主要触点为原生 engine 请求转发、semantics query parser/spec、metric_evaluation 的 DFS/PassThrough、Dataflow builder/新节点/visitor、输出实例及列关联转换、SQL 转换与 renderer 测试，以及服务请求适配。需要更新请求/spec 的所有复制构造点；不能只修改 DataflowPlan 容器。

实现者可以决定具体文件布局、类名、测试 fixture 组织，以及在不改变上述契约的前提下复用现有 SQL AST helper 的方式。公共输出键必须与真实源维度规格区分；合并策略、原始键行为、请求级绑定边界、一次结果取数及旧接口兼容性不是实现时自由改变的选项。

本设计替代 2026-10-28 服务层设计中“不改 vendor”的实现约束，依据为本次用户明确要求下沉到 MetricFlow。文档审批后再进入实施计划，本轮不修改 vendor 或服务产品代码。
