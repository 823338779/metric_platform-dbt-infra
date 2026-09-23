# DataflowPlanBuilder\.build\_plan\(\) 完整流程解析

# `DataflowPlanBuilder.build_plan()` 完整流程解析



本文分析下面这个方法，以及它向下调用的主要规划链路：



```Plain Text
metricflow.dataflow.builder.dataflow_plan_builder.DataflowPlanBuilder.build_plan
```



源码入口：`metricflow/dataflow/builder/dataflow_plan_builder.py:172`。



## 1\. 先看结论



`build_plan()` 的职责不是直接拼 SQL，而是把已经完成语义解析的 `MetricFlowQuerySpec` 转换成一张 `DataflowPlan` 数据流图。



它位于完整查询链路的中间：



```Plain Text
MetricFlowQueryRequest
        ↓
MetricFlowQueryParser
        ↓
MetricFlowQuerySpec
        ↓
DataflowPlanBuilder.build_plan()
        ↓
DataflowPlan
        ↓
DataflowToSqlPlanConverter
        ↓
SqlQueryPlan
        ↓
特定数据库方言 SQL
```



这个方法主要完成四件事：



1. 创建数据流规划选项 `DataflowPlanOptionSet`。

2. 调用 `_build_query_output_node()` 构建指标计算分支。

3. 添加输出裁剪、排序、Limit、别名和结果写入节点。

4. 把节点封装为 `DataflowPlan` 并执行数据流优化。

    

真正复杂的工作下沉在以下方法中：



```Plain Text
_build_query_output_node()
  ├── MetricEvaluationPlanner.build_plan()
  ├── _EvaluationNodeToDataflowNodeConverter
  ├── _build_simple_metric_output_node()
  ├── _build_derived_metric_output_node()
  ├── _build_cumulative_metric_output_node()
  └── _build_conversion_metric_output_node()

简单指标分支继续下沉：

_build_simple_metric_output_node()
  ├── _build_simple_metric_recipe()
  ├── build_aggregated_simple_metric_input()
  │   ├── _find_source_node_recipe()
  │   ├── _build_pre_aggregation_plan()
  │   └── AggregateSimpleMetricInputsNode
  └── ComputeMetricsNode
```



### 1\.1 最核心的逻辑：两次图转换、三次关键决策



虽然最终代码创建了很多种 `DataflowPlanNode`，但主算法可以压缩成“两次图转换”：



```Plain Text
MetricFlowQuerySpec
        │
        │ 第一次转换：展开指标定义和依赖
        ▼
MetricEvaluationPlan
        │
        │ 第二次转换：把每个指标计算节点翻译成数据操作分支
        ▼
DataflowPlanNode DAG
        │
        │ 补充输出节点并执行优化
        ▼
DataflowPlan
```



第一次转换回答：



> 一个指标依赖哪些简单指标、派生指标、累计指标或转化指标，应该按什么顺序计算？
> 
> 



第二次转换回答：



> 为了算出这些指标，应该从哪个源节点读取数据，经过哪些 Join、过滤、时间处理、聚合和指标计算节点？
> 
> 



第二次转换中最重要的三次决策是：



1. **按指标类型选择算法**：Visitor 把 simple、derived、cumulative、conversion 和 top\-level

节点分派给不同 builder。

2. **选择源节点与实体 Join 路径**：`_find_source_node_recipe()` 枚举可用源节点，判断哪些

linkable specs 可以本地取得、哪些需要 Join，并选择 Join 数量最少的合法方案。

3. **决定过滤和时间操作的位置**：`SimpleMetricRecipe` 与 `PredicatePushdownState` 决定时间范围、

where filter、Time Spine Join 应该在聚合前、聚合后还是 offset 后执行。



因此，阅读下层调用时应重点跟踪三类中间对象：



|中间对象|它携带的决策|
|---|---|
|`MetricEvaluationPlan`|指标依赖图和计算顺序|
|`SimpleMetricRecipe`|单个简单指标的过滤、offset、累计和 Time Spine 要求|
|`SourceNodeRecipe`|起始源节点、必须保留的本地字段以及实体 Join 方案|



### 1\.2 完整主调用链



下面是从 `build_plan()` 到最终计划的主干。特殊指标只在“指标类型专用分支”处展开，随后重新汇入

顶层指标输出节点：



```Plain Text
build_plan(query_spec)
│
├── 创建 DataflowPlanOptionSet
│
├── _build_query_output_node(query_spec)
│   ├── WhereFilterSpecFactory：把已解析的 where 转成规划期 FilterSpec
│   ├── MetricEvaluationPlanner.build_plan：构建指标依赖图
│   └── DFS 遍历 MetricEvaluationPlan
│       └── _EvaluationNodeToDataflowNodeConverter
│           ├── SimpleMetricsQueryNode
│           │   └── _build_simple_metric_output_node
│           │       ├── _build_simple_metric_recipe
│           │       ├── build_aggregated_simple_metric_input
│           │       │   ├── _find_source_node_recipe
│           │       │   ├── JoinOverTimeRange / JoinToTimeSpine（按需）
│           │       │   ├── _build_pre_aggregation_plan
│           │       │   ├── AggregateSimpleMetricInputsNode
│           │       │   └── 聚合后 Time Spine / 延迟过滤（按需）
│           │       └── ComputeMetricsNode
│           ├── DerivedMetricsQueryNode
│           │   └── 合并依赖分支 → ComputeMetricsNode → nested offset（按需）
│           ├── CumulativeMetricQueryNode
│           │   └── 扩展时间范围 → JoinOverTimeRange → 聚合 → 窗口重聚合（按需）
│           ├── ConversionMetricQueryNode
│           │   └── base/conversion 两条事件分支 → JoinConversionEventsNode → 聚合计算
│           └── TopLevelQueryNode
│               └── CombineAggregatedOutputsNode（多指标时）
│
├── SelectorNode（output_selection_specs 非空时）
├── build_sink_node
│   ├── OrderByLimitNode（按需）
│   ├── AliasSpecsNode（按需）
│   └── WriteToResultDataTableNode / WriteToResultTableNode
│
├── DataflowPlan(sink_nodes=[sink_node])
└── _optimize_plan → 优化后的 DataflowPlan
```



### 1\.3 方法边界：无指标查询不走这里



`MetricFlowEngine._create_execution_plan()` 会先根据 `query_spec.metric_specs` 分流：



```Plain Text
有指标    → DataflowPlanBuilder.build_plan()
无指标    → DataflowPlanBuilder.build_plan_for_distinct_values()
```



所以本文的主体是指标查询。无指标的维度值查询不会构建 `MetricEvaluationPlan`，而是直接寻找能够

提供 linkable specs 的源节点方案，再用 `_build_pre_aggregation_plan()` 完成 Join、过滤、去重或

min/max 等处理。



因此，理解 `build_plan()` 的关键不是只看它本身的四十行代码，而是理解它如何协调三层计划：



|层次|主要对象|解决的问题|
|---|---|---|
|查询规格|`MetricFlowQuerySpec`|用户最终想查询什么|
|指标评估计划|`MetricEvaluationPlan`|指标之间应该按照什么依赖顺序计算|
|数据流计划|`DataflowPlan`|数据应该怎样读取、关联、过滤、聚合和输出|



## 2\. `DataflowPlan` 的基本心智模型



`DataflowPlan` 是一张 DAG，也就是有向无环图。



每个 `DataflowPlanNode` 表示一个数据处理操作，例如：



- `ReadSqlSourceNode`：读取语义模型对应的 SQL 数据源。

- `JoinOnEntitiesNode`：按照实体关联不同语义模型。

- `WhereFilterNode`：应用 Where 条件。

- `ConstrainTimeRangeNode`：应用时间范围约束。

- `AggregateSimpleMetricInputsNode`：对 measure / simple metric input 进行聚合。

- `ComputeMetricsNode`：从聚合结果计算指标。

- `CombineAggregatedOutputsNode`：对齐并合并多个指标分支。

- `OrderByLimitNode`：排序和限制行数。

- `AliasSpecsNode`：设置最终输出别名。

- `WriteToResultDataTableNode`：返回内存结果。

- `WriteToResultTableNode`：写入指定 SQL 表。

    

节点中的 `parent_node` 表示它的上游输入。例如：



```Python
aggregate_node = AggregateSimpleMetricInputsNode.create(
    parent_node=filtered_node,
)
```



对应的数据流方向是：



```Plain Text
filtered_node
      ↓
aggregate_node
```



最终的 `DataflowPlan` 从唯一的 Sink 节点向上引用所有父节点。当前实现要求计划只能有一个 Sink：



```Python
assert len(sink_nodes) == 1
```



## 3\. 方法签名和参数



```Python
def build_plan(
    self,
    query_spec: MetricFlowQuerySpec,
    output_sql_table: Optional[SqlTable] = None,
    output_selection_specs: Optional[InstanceSpecSet] = None,
    optimizations: FrozenSet[DataflowPlanOptimization] = frozenset(),
    me_plan_override: Optional[MetricEvaluationPlan] = None,
) -> DataflowPlan:
```



### 3\.1 `query_spec`



`MetricFlowQuerySpec` 是查询解析阶段的产物。它已经不再是用户输入的字符串，而是结构化 Spec 集合。



主要字段包括：



|字段|含义|
|---|---|
|`metric_specs`|要计算的指标|
|`dimension_specs`|查询的普通维度|
|`time_dimension_specs`|查询的时间维度及粒度|
|`entity_specs`|查询的实体|
|`group_by_metric_specs`|作为分组项使用的指标|
|`order_by_specs`|排序规则|
|`time_range_constraint`|查询时间范围|
|`filter_intersection`|查询级 Where 条件|
|`filter_spec_resolution_lookup`|Where 字段解析结果|
|`limit`|最大输出行数|
|`input_spec_order`|用户输入字段的原始顺序|



`query_spec.linkable_specs` 会把维度、时间维度、实体和 group\-by metric 统一包装成 `LinkableSpecSet`。



### 3\.2 `output_sql_table`



控制结果最终写向哪里：



- `None`：构建 `WriteToResultDataTableNode`，查询结果返回给调用方。

- 指定 `SqlTable`：构建 `WriteToResultTableNode`，生成写表计划。

    

### 3\.3 `output_selection_specs`



用于在指标分支构建完成后，再裁剪一次输出字段。



当前引擎在处理 `DIMENSION_VALUES` 查询时会使用它，只保留用户需要的维度或时间维度，避免把计算过程中的辅助字段也暴露出去。



### 3\.4 `optimizations`



数据流优化开关集合。当前定义了：



- `SOURCE_SCAN`

- `PASSTHROUGH_METRIC_EVALUATION`

    

两者介入阶段不同：



- `PASSTHROUGH_METRIC_EVALUATION` 在构建指标评估计划时生效。

- `SOURCE_SCAN` 在完整 `DataflowPlan` 构建后，由 `_optimize_plan()` 执行。

    

### 3\.5 `me_plan_override`



测试辅助参数。正常流程会自行构建 `MetricEvaluationPlan`；传入该参数后，可以跳过指标评估规划，直接测试从指标评估计划到数据流计划的转换。



## 4\. `build_plan()` 主流程逐行分析



方法主体可以简化成：



```Python
option_set = DataflowPlanOptionSet(...)

metrics_output_node = self._build_query_output_node(...)

if output_selection_specs is not None:
    metrics_output_node = SelectorNode.create(...)

sink_node = self.build_sink_node(...)

plan = DataflowPlan(sink_nodes=[sink_node], plan_id=...)

return self._optimize_plan(plan, option_set)
```



### 4\.1 创建 `DataflowPlanOptionSet`



```Python
option_set = DataflowPlanOptionSet(
    optimizations=frozenset(optimizations),
    output_group_by_metric_instances=False,
)
```



`DataflowPlanOptionSet` 是内部规划选项，包含：



```Plain Text
optimizations
output_group_by_metric_instances
```



普通指标查询把 `output_group_by_metric_instances` 设置为 `False`。这个标志主要用于“按指标结果分组”这一特殊场景，普通顶层查询不需要额外输出 group\-by metric instance。



### 4\.2 构建指标输出节点



```Python
metrics_output_node = self._build_query_output_node(
    query_spec=query_spec,
    option_set=option_set,
    me_plan_override=me_plan_override,
)
```



这是整个流程的核心。



它返回的是“所有请求指标已经计算完成”的数据流顶层节点，但还没有添加：



- 最终排序

- Limit

- 输出别名

- 写结果节点

    

### 4\.3 可选的输出字段裁剪



```Python
if output_selection_specs is not None:
    metrics_output_node = SelectorNode.create(
        parent_node=metrics_output_node,
        include_specs=output_selection_specs,
    )
```



这里添加的 `SelectorNode` 类似 SQL 中只选择指定列：



```SQL
SELECT wanted_column_1, wanted_column_2
FROM (...)
```



它不是查询最早阶段的列裁剪，而是指标输出已经构建好后的额外输出限制。



### 4\.4 构建 Sink 分支



```Python
sink_node = DataflowPlanBuilder.build_sink_node(
    parent_node=metrics_output_node,
    desired_output_metric_specs=query_spec.metric_specs,
    desired_output_group_by_item_specs=(
        query_spec.dimension_specs
        + query_spec.time_dimension_specs
        + query_spec.entity_specs
    ),
    order_by_specs=query_spec.order_by_specs,
    output_sql_table=output_sql_table,
    limit=query_spec.limit,
)
```



这里开始处理“结果如何输出”，而不是“指标如何计算”。



需要注意，`group_by_metric_specs` 没有直接放进 `desired_output_group_by_item_specs`；这一类特殊实例由指标规划分支中的 `output_group_by_metric_instances` 机制处理。



### 4\.5 封装成 `DataflowPlan`



```Python
plan_id = DagId.from_id_prefix(
    StaticIdPrefix.DATAFLOW_PLAN_PREFIX
)

plan = DataflowPlan(
    sink_nodes=[sink_node],
    plan_id=plan_id,
)
```



到这里，前面创建的节点分支才被正式包装成完整的数据流 DAG。



因为 Sink 节点递归引用所有父节点，所以只需要把 Sink 放入 `sink_nodes`，整张上游图就都包含在计划中了。



### 4\.6 优化数据流计划



```Python
optimized_plan = self._optimize_plan(
    plan=plan,
    option_set=option_set,
)
```



`_optimize_plan()` 根据 `option_set.optimizations` 从 `DataflowPlanOptimizerFactory` 取得优化器，并依次执行：



```Python
plan = optimizer.optimize(plan)
```



当前 `SOURCE_SCAN` 会得到 `SourceScanOptimizer`。



`PASSTHROUGH_METRIC_EVALUATION` 不会在这里得到一个优化器，因为它已经在 `_build_query_output_node()` 选择指标评估规划器时生效。



如果某个数据流优化器抛出异常，当前实现会记录异常并继续返回已有计划，而不是直接让整个查询失败。



## 5\. `_build_query_output_node()`：从 QuerySpec 到指标数据流



这个方法是 `build_plan()` 中最关键的一层。它包含两个明显阶段：



```Plain Text
阶段 A：构建 MetricEvaluationPlan
阶段 B：把 MetricEvaluationPlan 转换成 DataflowPlanNode DAG
```



### 5\.1 检查顶层指标 Spec



方法首先要求 `query_spec.metric_specs` 中不能预先带有：



- `where_filter_specs`

- `offset_to_grain`

- `offset_window`

    

这些修饰信息应该由指标评估规划器根据指标定义和依赖关系生成，而不是直接出现在顶层查询指标中。



### 5\.2 构建过滤条件工厂



```Python
filter_spec_factory = WhereFilterSpecFactory(
    column_association_resolver=...,
    spec_resolution_lookup=...,
    custom_grain_names=...,
)
```



它把解析阶段产生的过滤条件转换为规划阶段可以理解的 `WhereFilterSpec`，其中包含：



- 过滤表达式引用了哪些 Spec。

- 这些 Spec 对应哪些逻辑列。

- 是否包含实体路径。

- 是否包含自定义时间粒度。

- 过滤条件所在的指标层级。

    

### 5\.3 将查询级过滤条件附着到指标请求



```Python
query_level_filter_specs = (
    filter_spec_factory.create_from_where_filter_intersection(...)
)
```



然后重新构造顶层 `MetricSpec`：



```Python
metric_specs = tuple(
    MetricSpec.create(
        element_name=metric_spec.element_name,
        where_filter_specs=query_level_filter_specs,
    )
    for metric_spec in query_spec.metric_specs
)
```



这里还没有创建 `WhereFilterNode`，只是把查询过滤条件作为指标计算要求向下传递。实际放置位置需要结合指标依赖、时间偏移和 Time Spine 决定。



### 5\.4 初始化谓词下推状态



```Python
predicate_pushdown_state = PredicatePushdownState.create(
    time_range_constraint=query_spec.time_range_constraint,
    where_filter_specs=(),
    pushdown_enabled_types=frozenset({
        PredicateInputType.TIME_RANGE_CONSTRAINT
    }),
)
```



这里明确只允许时间范围约束参与这套下推机制。



普通 Where 条件通过 `WhereFilterSpec` 和指标 recipe 单独确定位置。



### 5\.5 去掉 group\-by 别名



```Python
group_by_item_specs_without_aliases = (
    query_spec.linkable_specs.without_aliases
)
```



指标依赖规划使用没有别名的标准 Spec。



原因是：



```Plain Text
别名属于最终输出表示
实体路径、维度满足性和指标依赖属于计算语义
```



别名不应该影响源节点选择和 Join 路径判断。最终别名由 Sink 阶段重新添加。



### 5\.6 构建 `MetricEvaluationPlan`



如果没有传入 `me_plan_override`，根据优化配置选择规划器：



```Plain Text
默认：DepthFirstSearchMetricEvaluationPlanner
优化：PassThroughMetricEvaluationPlanner
```



然后调用：



```Python
me_planner.build_plan(
    metric_specs=metric_specs,
    group_by_item_specs=group_by_item_specs_without_aliases.as_tuple,
    predicate_pushdown_state=predicate_pushdown_state,
    filter_spec_factory=filter_spec_factory,
)
```



这一层解决的是“指标依赖”，不是表 Join。



例如：



```Plain Text
profit_rate = profit / revenue
profit = revenue - cost
```



可能形成：



```Plain Text
TopLevelQueryNode
        ↓
DerivedMetricsQueryNode(profit_rate)
        ├── DerivedMetricsQueryNode(profit)
        │       ├── SimpleMetricsQueryNode(revenue)
        │       └── SimpleMetricsQueryNode(cost)
        └── SimpleMetricsQueryNode(revenue)
```



它主要决定：



- 指标依赖哪些子指标。

- 子指标继承哪些过滤条件。

- 哪个输入指标有 offset。

- 每一层使用哪些 group\-by Spec。

- 节点属于 simple、derived、cumulative 还是 conversion。

    

## 6\. 从指标评估计划转换为数据流计划



### 6\.1 找到顶层查询节点



```Python
top_level_query_node = me_plan_override.node_with_label(
    TopLevelQueryLabel.get_instance()
)
```



它是整个指标依赖图的最终出口。



### 6\.2 建立转换结果映射



```Python
query_node_to_dataflow_node = {}
```



这个字典保存：



```Plain Text
MetricQueryNode → 已经生成的 DataflowPlanNode
```



父指标在生成数据流时，可以直接取得子指标已经生成好的数据流分支。



### 6\.3 DFS 自底向上转换



代码通过 `MetricFlowPathfinder.find_paths_dfs()` 遍历指标评估图。



转换原则是：



```Plain Text
先转换当前指标依赖的 source query nodes
再使用这些输入分支构建当前指标的数据流节点
```



对于每个 `current_query_node`：



1. 检查是否已经转换，避免重复生成。

2. 从 `MetricEvaluationPlan.source_nodes()` 取得依赖节点。

3. 从映射中取得依赖节点对应的数据流分支。

4. 调用 Visitor，根据指标节点类型生成数据流节点。

5. 把转换结果放回映射。

    

伪代码如下：



```Python
for current_query_node in dfs(metric_evaluation_plan):
    if current_query_node already converted:
        continue

    input_dataflow_nodes = [
        converted[source_query_node]
        for source_query_node in sources(current_query_node)
    ]

    output_dataflow_node = convert_by_metric_type(
        current_query_node,
        input_dataflow_nodes,
    )

    converted[current_query_node] = output_dataflow_node
```



如果当前节点的某个输入还没有转换，代码会抛出 `MetricFlowInternalError`。这代表指标图遍历或依赖方向存在内部错误。



### 6\.4 Visitor 的作用



真正的类型分派由内部类完成：



```Plain Text
_EvaluationNodeToDataflowNodeConverter
```



它把不同的 `MetricQueryNode` 映射为不同数据流分支：



|MetricQueryNode|构建逻辑|
|---|---|
|`SimpleMetricsQueryNode`|构建简单指标读取、Join、过滤、聚合和计算|
|`DerivedMetricsQueryNode`|合并依赖指标并计算表达式|
|`CumulativeMetricQueryNode`|构建累计时间窗口逻辑|
|`ConversionMetricQueryNode`|构建基础事件与转化事件匹配逻辑|
|`TopLevelQueryNode`|汇总最终请求的一个或多个指标分支|



## 7\. 简单指标的完整数据流



简单指标是其他指标类型的基础，入口为：



```Plain Text
_build_simple_metric_output_node()
```



总体结构：



```Plain Text
Simple Metric
    ↓
构建 SimpleMetricRecipe
    ↓
查找 SourceNodeRecipe
    ↓
读取源表 / 实体 Join / 过滤 / 时间约束
    ↓
AggregateSimpleMetricInputsNode
    ↓
ComputeMetricsNode
```



### 7\.1 构建 `SimpleMetricRecipe`



`_build_simple_metric_recipe()` 汇总计算一个简单指标所需的信息，包括：



- 指标对应的 `SimpleMetricInput`。

- 用户查询的分组项。

- 指标定义上的过滤条件。

- 顶层查询向下传递的过滤条件。

- 派生指标输入上的时间 offset。

- 累计窗口信息。

- Time Spine Join 应该发生在聚合前还是聚合后。

- 哪些过滤条件要延迟到 Time Spine Join 之后重新应用。

    

它不是数据流节点，而是一份构建数据流的中间 recipe。



### 7\.2 计算全部必需的 Linkable Specs



用户最终输出的维度不一定是计算过程中需要的全部维度。



`__get_required_linkable_specs()` 会合并：



```Plain Text
用户查询的 group-by specs
+ Where 条件引用的 specs
+ 非可加维度计算需要的 specs
+ 自定义粒度对应的基础时间粒度
```



其中一些是辅助 Spec，只参与中间计算，不应该出现在最终结果中。



例如用户查询：



```SQL
metric: revenue
group by: country
where: customer_tier = 'VIP'
```



即使 `customer_tier` 不在最终 SELECT 中，它仍然必须出现在 Join 和过滤阶段。



### 7\.3 查找 `SourceNodeRecipe`



入口：



```Plain Text
_find_source_node_recipe()
```



它的任务不是立即创建 Join，而是寻找一套能够满足指标输入和所有 Linkable Specs 的源节点方案。



`SourceNodeRecipe` 包含：



```Plain Text
source_node
required_local_linkable_specs
join_linkable_instances_recipes
all_linkable_specs_required_for_source_nodes
```



可以理解成：



```Plain Text
从哪张语义模型开始
当前源表必须保留哪些字段
还需要关联哪些语义模型
每个关联应该使用哪个实体和条件
```



### 7\.4 源节点和 Join 方案如何选择



`_find_source_node_recipe_non_cached()` 的主要过程是：



1. 将自定义时间粒度暂时替换成基础粒度。

2. 从源节点集合中找出包含所需 measure / simple metric input 的左侧候选节点。

3. 收集可能提供维度、实体和时间维度的右侧候选节点。

4. 尽可能把时间范围条件下推到候选源节点。

5. 删除不能提供所需 Linkable Specs 的无关节点。

6. 如果存在多跳实体路径，预先补充多跳 Join 节点。

7. 如果查询包含 group\-by metric，递归调用 `_build_query_output_node()` 构建其数据源。

8. 使用 `NodeEvaluatorForLinkableInstances` 评估每个左侧候选能否满足全部 Linkable Specs。

9. 排除存在 `unjoinable_linkable_specs` 的方案。

10. 在合法方案中选择 Join 数量最少的方案。

11. 返回 `SourceNodeRecipe`。

    

普通指标查询默认使用：



```Plain Text
LEFT OUTER JOIN
```



无指标的 distinct values 查询通常使用：



```Plain Text
FULL OUTER JOIN
```



### 7\.5 实体 Join 在哪里变成数据流节点



`SourceNodeRecipe` 中的 Join 方案最终传给：



```Plain Text
_build_pre_aggregation_plan()
```



如果存在 `join_targets`：



```Python
output_node = JoinOnEntitiesNode.create(
    left_node=output_node,
    join_targets=join_targets,
)
```



因此跨表查询的调用链是：



```Plain Text
build_plan()
  → _build_query_output_node()
    → visit_simple_metrics_query_node()
      → _build_simple_metric_output_node()
        → build_aggregated_simple_metric_input()
          → _find_source_node_recipe()
            → NodeEvaluatorForLinkableInstances
          → _build_pre_aggregation_plan()
            → JoinOnEntitiesNode
```



实体 Join 的关系和条件来自语义模型中的实体定义：



```Plain Text
相同 entity name
+ 合法的 entity type 方向
+ 两侧 entity expr / column association
+ 必要的分区维度条件
+ SCD validity 条件（如果存在）
```



MetricFlow 不会依赖数据库外键自动猜测业务关系。



### 7\.6 聚合前处理顺序



`_build_pre_aggregation_plan()` 按以下顺序包装节点：



```Plain Text
source_node
    ↓
JoinOnEntitiesNode                  可选
    ↓
JoinToCustomGranularityNode        可选
    ↓
SelectorNode                       保留过滤所需辅助列
    ↓
WhereFilterNode                    可选
    ↓
ConstrainTimeRangeNode             可选
    ↓
SemiAdditiveJoinNode               可选
    ↓
SelectorNode                       只保留聚合所需列
```



第一个 `SelectorNode` 需要临时保留过滤、时间约束和半可加计算使用的辅助列。



最后一个 `SelectorNode` 在约束都执行完后，再删除辅助列，只保留聚合真正需要的 Spec。



### 7\.7 聚合输入



聚合前分支完成后创建：



```Python
AggregateSimpleMetricInputsNode.create(
    parent_node=unaggregated_simple_metric_input_node,
    null_fill_value_mapping=...,
)
```



它负责：



- 按查询的 Linkable Specs 分组。

- 对 measure / simple metric input 执行 `SUM`、`COUNT`、`MAX` 等聚合。

- 处理 `fill_nulls_with` 所需的空值填充信息。

    

### 7\.8 计算最终简单指标



聚合结果还不是最终指标，随后创建：



```Python
ComputeMetricsNode.create(
    parent_node=aggregated_node,
    computed_metric_specs=[metric_spec],
    ...,
)
```



例如一个简单指标可能只是把内部聚合列：



```Plain Text
__bookings
```



转换成面向用户的指标实例：



```Plain Text
bookings
```



它还会处理指标级空值语义等逻辑。



## 8\. 派生指标的数据流



入口：



```Plain Text
_build_derived_metric_output_node()
```



假设：



```Plain Text
profit_margin = profit / revenue
```



DFS 会先构建：



```Plain Text
profit 数据流分支
revenue 数据流分支
```



然后派生指标节点取得这些输入：



```Python
input_node = (
    input_dataflow_plan_nodes[0]
    if len(input_dataflow_plan_nodes) == 1
    else CombineAggregatedOutputsNode.create(
        input_dataflow_plan_nodes
    )
)
```



最后：



```Python
ComputeMetricsNode.create(
    parent_node=input_node,
    computed_metric_specs=[metric_spec],
    passthrough_metric_specs=...,
)
```



形成：



```Plain Text
profit branch ─────┐
                   ├── CombineAggregatedOutputsNode
revenue branch ────┘
                                ↓
                     ComputeMetricsNode
                                ↓
                         profit_margin
```



如果派生指标输入包含时间 offset，还可能在计算后增加 Time Spine offset Join。



## 9\. 累计指标的数据流



入口：



```Plain Text
_build_cumulative_metric_output_node()
```



累计指标需要额外处理：



- 累计窗口 `window`

- `grain_to_date`

- 查询时间范围扩展

- `JoinOverTimeRangeNode`

- 可能的二次窗口重聚合

    

典型结构：



```Plain Text
ReadSqlSourceNode
        ↓
JoinOverTimeRangeNode
        ↓
聚合简单指标输入
        ↓
ComputeMetricsNode（基础简单指标）
        ↓
ComputeMetricsNode（累计指标）
        ↓
WindowReaggregationNode            必要时
```



为什么要扩展时间范围：



假设查询 9 月 10 日的七日累计值，只读取 9 月 10 日的数据无法计算结果，还需要读取之前六天的数据。因此源扫描范围可能先被扩大，累计计算完成后再收窄到用户请求范围。



如果查询粒度比指标最小计算粒度更粗，代码会添加 `WindowReaggregationNode`，将中间结果重新压缩到用户查询粒度。



## 10\. 转化指标的数据流



入口：



```Plain Text
_build_conversion_metric_output_node()
```



转化指标需要两类事件：



- base / opportunity event

- conversion event

    

大致数据流是：



```Plain Text
base event source ────────────┐
                              ├── JoinConversionEventsNode
conversion event source ──────┘
                                          ↓
                             聚合 base 与 conversion
                                          ↓
                              CombineAggregatedOutputsNode
                                          ↓
                                 ComputeMetricsNode
```



`JoinConversionEventsNode` 会携带：



- 用于匹配的实体 Spec。

- base 和 conversion 的时间维度。

- 转化时间窗口。

- 唯一事件标识。

- constant properties。

    

之后分别聚合机会数和成功转化数，再计算最终转化指标。



## 11\. 顶层查询节点如何汇总指标



Visitor 处理 `TopLevelQueryNode` 时：



- 没有输入：属于内部错误。

- 一个输入：直接返回该数据流分支。

- 多个输入：使用 `CombineAggregatedOutputsNode` 合并。

    

```Python
if input_count == 1:
    return input_nodes[0]
else:
    return CombineAggregatedOutputsNode.create(input_nodes)
```



因此查询多个指标时，每个指标可能先形成独立分支，再按照相同 group\-by 粒度合并。



## 12\. `CombineAggregatedOutputsNode` 的含义



这个节点不是简单的 `UNION ALL`。



它用于把已经聚合到相同或兼容 Linkable Specs 的多个指标结果横向对齐，使一行中能够同时出现多个指标列。



概念上类似：



```Plain Text
revenue_by_country ────┐
                       ├── 按 country 对齐
orders_by_country ─────┘
```



结果：



|country|revenue|orders|
|---|---|---|
|CN|1000|20|
|US|1500|18|



它的具体 SQL Join 类型和空值处理由后续 Dataflow\-to\-SQL 转换器决定。



## 13\. 简单指标缓存



`_build_query_output_node()` 内部创建：



```Python
simple_metric_node_cache = ResultCache()
```



缓存键包含：



- 计算指标 Spec。

- 透传指标 Spec。

- group\-by Spec。

- 谓词下推状态。

- 优化配置。

    

如果多个派生指标依赖相同的简单指标，并且过滤条件和分组粒度也相同，可以复用同一个数据流分支。



这既减少构建时间，也让后续 SQL 转换器有机会识别公共分支并使用 CTE。



## 14\. `build_sink_node()` 完整流程



指标计算完成后，`build_plan()` 调用 `build_sink_node()` 添加输出阶段节点。



顺序如下：



```Plain Text
metrics_output_node
        ↓
OrderByLimitNode            可选
        ↓
AliasSpecsNode              可选
        ↓
WriteToResultDataTableNode
或 WriteToResultTableNode
```



### 14\.1 排序和 Limit



只要存在 `order_by_specs` 或 `limit`，就创建：



```Python
OrderByLimitNode.create(
    order_by_specs=list(order_by_specs),
    limit=limit,
    parent_node=sink_node,
)
```



排序和 Limit 被放在别名节点之前，因为内部计算始终使用规范化 Spec；输出别名只负责最终展示。



### 14\.2 输出别名



如果任意输出 Spec 设置了 `alias`：



```Python
sink_node = DataflowPlanBuilder._add_alias_node(...)
```



`_add_alias_node()` 会创建从无别名 Spec 到有别名 Spec 的映射：



```Plain Text
input spec:  revenue
output spec: revenue AS total_revenue
```



如果用户同时查询原名和别名，还会额外添加 identity 映射，保证原名列不会被别名转换吞掉。



### 14\.3 结果节点



```Plain Text
output_sql_table is None
    → WriteToResultDataTableNode

output_sql_table is not None
    → WriteToResultTableNode
```



这两个节点是 Dataflow Plan 的最终 Sink。



## 15\. 一个跨表简单指标示例



假设查询：



```Plain Text
metric: order_revenue
group by: customer__country
time range: 2026-09-01 ~ 2026-09-30
where: customer__tier = 'VIP'
order by: order_revenue desc
limit: 10
```



语义模型关系：



```Plain Text
orders.customer   = foreign, expr=customer_id
customers.customer = primary, expr=id
```



完整规划思路如下：



```Plain Text
1. QueryParser 已将输入转换为 QuerySpec
   metric_specs       = order_revenue
   dimension_specs    = customer__country
   filter              = customer__tier = 'VIP'
   time constraint     = September 2026

2. _build_query_output_node
   构建 order_revenue 的 MetricEvaluationPlan

3. simple metric visitor
   进入 _build_simple_metric_output_node

4. _build_simple_metric_recipe
   合并指标定义过滤、查询过滤和时间信息

5. required linkable specs
   customer__country
   customer__tier          中间过滤字段
   metric_time             时间约束需要

6. _find_source_node_recipe
   选择 orders 作为左侧指标源
   找到 customers 可以提供 country 和 tier
   识别 customer foreign → primary 是合法 Join
   生成 JoinDescription

7. _build_pre_aggregation_plan
   ReadSqlSourceNode(orders)
       ↓
   JoinOnEntitiesNode(customers on customer)
       ↓
   SelectorNode
       ↓
   WhereFilterNode(customer__tier = 'VIP')
       ↓
   ConstrainTimeRangeNode(September 2026)
       ↓
   SelectorNode

8. AggregateSimpleMetricInputsNode
   group by customer__country
   aggregate revenue measure

9. ComputeMetricsNode
   输出 order_revenue

10. build_sink_node
   OrderByLimitNode(order_revenue desc, limit 10)
       ↓
   WriteToResultDataTableNode

11. _optimize_plan
   运行 SourceScanOptimizer（如果启用）
```



对应的 Dataflow Plan 结构大致是：



```Plain Text
WriteToResultDataTableNode
└── OrderByLimitNode
    └── ComputeMetricsNode(order_revenue)
        └── AggregateSimpleMetricInputsNode
            └── SelectorNode
                └── ConstrainTimeRangeNode
                    └── WhereFilterNode
                        └── SelectorNode
                            └── JoinOnEntitiesNode
                                ├── ReadSqlSourceNode(orders)
                                └── ReadSqlSourceNode(customers)
```



## 16\. 一个派生指标示例



假设查询：



```Plain Text
metric: average_order_value
group by: metric_time__month
```



定义：



```Plain Text
average_order_value = revenue / order_count
```



指标评估计划可能是：



```Plain Text
TopLevelQueryNode
└── DerivedMetricsQueryNode(average_order_value)
    ├── SimpleMetricsQueryNode(revenue)
    └── SimpleMetricsQueryNode(order_count)
```



转换后的数据流可能是：



```Plain Text
WriteToResultDataTableNode
└── ComputeMetricsNode(average_order_value)
    └── CombineAggregatedOutputsNode
        ├── ComputeMetricsNode(revenue)
        │   └── AggregateSimpleMetricInputsNode
        │       └── ReadSqlSourceNode(...)
        └── ComputeMetricsNode(order_count)
            └── AggregateSimpleMetricInputsNode
                └── ReadSqlSourceNode(...)
```



如果两个简单指标具备相同来源和规划条件，公共分支可能被缓存、优化，并在 SQL 转换阶段提取为 CTE。



## 17\. Dataflow Plan 到 SQL 的边界



`build_plan()` 完成时还没有生成具体数据库 SQL。



它只表达逻辑操作：



```Plain Text
读取哪个源
按哪个实体 Join
应用哪些过滤
按哪些字段聚合
计算哪些指标
怎样组合指标结果
怎样排序和输出
```



之后引擎调用 `DataflowToExecutionPlanConverter`，内部再使用 `DataflowToSqlPlanConverter`：



```Plain Text
DataflowPlanNode
        ↓
DataflowNodeToSqlSubqueryVisitor / DataflowNodeToSqlCteVisitor
        ↓
SqlQueryPlan
        ↓
SqlPlanRenderer
        ↓
BigQuery / Postgres / Snowflake / DuckDB 等方言 SQL
```



所以不要把下面两件事混在一起：



```Plain Text
DataflowPlanBuilder：决定“要执行哪些逻辑操作”
SQL Converter：决定“这些操作怎样表达成 SQL AST”
SQL Renderer：决定“SQL AST 怎样渲染成数据库方言”
```



## 18\. `build_plan()` 不负责什么



`build_plan()` 不负责：



- 解析用户传入的指标和维度字符串。

- 判断用户输入名称是否存在。

- 直接读取数据库元数据推断 Join。

- 直接生成 SQL 字符串。

- 执行 SQL。

- 获取查询结果。

    

这些职责分别属于：



|职责|模块|
|---|---|
|查询解析和校验|`MetricFlowQueryParser`|
|语义模型和实体关系|`SemanticManifestLookup` / semantic graph|
|指标依赖规划|`MetricEvaluationPlanner`|
|数据流规划|`DataflowPlanBuilder`|
|SQL Plan 转换|`DataflowToSqlPlanConverter`|
|方言 SQL 渲染|`SqlPlanRenderer`|
|SQL 执行|`SqlClient` / execution plan|



## 19\. 常见异常分别说明什么



### 顶层 MetricSpec 带修饰信息



```Plain Text
The metric specs in the query spec should not contain any metric modifiers
```



说明传给 `_build_query_output_node()` 的顶层指标没有经过预期的规范化，或者调用方错误地把子指标修饰信息放到了顶层。



### 找不到 SourceNodeRecipe



```Plain Text
Unable to join all items in request
```



一般表示：



- 没有源节点提供目标 measure。

- 请求的维度无法从指标源 Join 到。

- 实体名称不一致。

- 实体类型方向不合法。

- 多跳路径超过能力限制。

- 过滤字段需要的维度无法满足。

    

### 依赖节点还没有被转换



```Plain Text
A dataflow node has not been generated for given metric evaluation node
```



这是内部图遍历或指标依赖图错误，不属于普通用户配置问题。



### 没有生成 TopLevelQueryNode 对应节点



```Plain Text
A dataflow plan node was not created for the top level query node
```



说明指标评估计划到数据流计划的转换没有完整覆盖顶层节点。



## 20\. 推荐调试断点



阅读或调试一条真实查询时，可以按下面顺序设置断点。



### 断点 1：`build_plan()` 入口



观察：



```Plain Text
query_spec.metric_specs
query_spec.linkable_specs
query_spec.filter_intersection
query_spec.time_range_constraint
query_spec.order_by_specs
query_spec.limit
```



目标：确认查询解析阶段交付了什么。



### 断点 2：`_build_query_output_node()` 的 `me_planner.build_plan()` 之后



观察格式化后的 `MetricEvaluationPlan`。



目标：确认指标依赖是否正确展开，尤其是派生指标、累计指标和 offset 输入。



### 断点 3：Visitor 的各个 `visit_*` 方法



观察：



```Plain Text
current MetricQueryNode 类型
input_dataflow_plan_nodes
query_properties.group_by_item_specs
predicate_pushdown_state
```



目标：确认指标评估节点如何变成数据流分支。



### 断点 4：`_find_source_node_recipe_non_cached()`



观察：



```Plain Text
candidate_nodes_for_left_side_of_join
candidate_nodes_for_right_side_of_join
linkable_specs_to_satisfy
evaluation.local_linkable_specs
evaluation.join_recipes
evaluation.unjoinable_linkable_specs
```



目标：定位为什么选择某张表、为什么需要某个 Join，或者为什么无法满足维度。



### 断点 5：`_build_pre_aggregation_plan()`



观察每包装一个节点后的 `output_node.structure_text()`。



目标：确认 Join、过滤、时间约束和 Selector 的实际顺序。



### 断点 6：`build_plan()` 返回前



观察：



```Python
optimized_plan.structure_text()
```



目标：查看最终数据流计划，而不是直接从最终 SQL 反推。



## 21\. 核心方法速查



|方法|作用|
|---|---|
|`build_plan()`|组织指标分支、Sink 和优化，返回完整 DataflowPlan|
|`_build_query_output_node()`|将 QuerySpec 转换为指标数据流顶层节点|
|`MetricEvaluationPlanner.build_plan()`|构建指标依赖计划|
|`_EvaluationNodeToDataflowNodeConverter`|将不同类型的指标评估节点翻译成数据流节点|
|`_build_simple_metric_output_node()`|构建简单指标输出分支|
|`_build_simple_metric_recipe()`|汇总简单指标的过滤、offset 和 Time Spine 需求|
|`_find_source_node_recipe()`|选择源节点和实体 Join 方案|
|`_build_pre_aggregation_plan()`|构建 Join、过滤、时间约束和聚合前列裁剪|
|`build_aggregated_simple_metric_input()`|构建简单指标输入的完整聚合分支|
|`build_computed_metrics_node()`|从聚合输入创建最终指标实例|
|`_build_derived_metric_output_node()`|合并依赖指标并计算派生指标|
|`_build_cumulative_metric_output_node()`|构建累计窗口指标|
|`_build_conversion_metric_output_node()`|构建转化事件匹配和转化指标|
|`build_sink_node()`|添加排序、Limit、别名和结果写入节点|
|`_optimize_plan()`|执行数据流级优化|



## 22\. 最终总结



`DataflowPlanBuilder.build_plan()` 可以用下面一句话概括：



> 它接收“用户要查询什么”的 `MetricFlowQuerySpec`，先规划“指标依赖怎样计算”，再把每个指标依赖节点翻译成“数据怎样读取、关联、过滤、聚合和组合”的数据流 DAG，最后补充排序、Limit、别名和结果写入节点，并返回优化后的 `DataflowPlan`。
> 
> 



最核心的分层是：



```Plain Text
QuerySpec
解决：查什么

MetricEvaluationPlan
解决：指标按什么依赖顺序计算

DataflowPlan
解决：数据经过哪些逻辑操作

SqlQueryPlan / Renderer
解决：怎样生成具体数据库 SQL
```



阅读源码时建议始终沿着下面这条主线：



```Plain Text
build_plan
  → _build_query_output_node
    → MetricEvaluationPlanner
    → MetricQueryNode Visitor
      → 指标类型专用 builder
        → _find_source_node_recipe
        → _build_pre_aggregation_plan
        → AggregateSimpleMetricInputsNode
        → ComputeMetricsNode
  → build_sink_node
  → _optimize_plan
```



