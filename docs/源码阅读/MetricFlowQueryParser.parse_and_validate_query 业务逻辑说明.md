# MetricFlowQueryParser\.parse\_and\_validate\_query 业务逻辑说明

本文解释

`metricflow_semantics.query.query_parser.MetricFlowQueryParser.parse_and_validate_query()`

如何把调用方传入的一组查询参数转换为 MetricFlow 内部可执行的查询规格。



源码位置：

`[metricflow_semantics/query/query_parser.py](metricflow_semantics/query/query_parser.py)`



## 1\. 方法的职责



这个方法位于“外部查询参数”和“内部查询规格”之间，承担四项职责：



1. 检查参数组合是否合法。

2. 把字符串或参数对象统一转换成 resolver input。

3. 调用 `MetricFlowQueryResolver`，结合 semantic manifest 完成语义解析和校验。

4. 处理时间范围，并返回 `ParseQueryResult`。

    

它不会生成 SQL，也不会构建 dataflow plan。成功产出的 `MetricFlowQuerySpec`

会交给后续规划和 SQL 生成流程使用。



可以把整个过程概括为：



```Plain Text
flowchart TD
    A[调用方输入查询参数] --> B[检查互斥参数与特殊参数]
    B --> C[字符串或对象转换为 ResolverInput]
    C --> D[组装 ResolverInputForQuery]
    D --> E[MetricFlowQueryResolver.resolve_query]
    E --> F{是否存在解析或校验错误}
    F -- 是 --> G[抛出 InvalidQueryException]
    F -- 否 --> H[取得 MetricFlowQuerySpec]
    H --> I{是否传入时间边界}
    I -- 是 --> J[补齐缺失边界并按查询时间粒度扩展]
    I -- 否 --> K[保持无显式时间范围]
    J --> L[返回 ParseQueryResult]
    K --> L
```



## 2\. 输入参数



### 2\.1 指标



|参数|含义|
|---|---|
|`metric_names`|字符串形式的指标，例如 `bookings` 或 `Metric('bookings')`|
|`metrics`|已结构化的 `MetricQueryParameter` 对象|



两者最多只能传一个。字符串形式会依次尝试以下命名方案：



- `MetricNamingScheme`：普通指标名，例如 `bookings`。

- `ObjectBuilderNamingScheme`：对象构造语法，例如 `Metric('bookings')`。

    

匹配成功后生成 `ResolverInputForMetric`；如果所有命名方案都不匹配，则记录

`StringInputParsingIssue`。



### 2\.2 分组项



|参数|含义|
|---|---|
|`group_by_names`|字符串形式的维度、实体或时间维度|
|`group_by`|已结构化的 `GroupByQueryParameter` 元组|



两者最多只能传一个。字符串形式支持：



- `ObjectBuilderNamingScheme`，例如 `Dimension('country')`、

`TimeDimension('metric_time').grain('month')`。

- `DunderNamingScheme`，例如 `listing__country`、`metric_time__month`。

    

匹配成功后生成 `ResolverInputForGroupByItem`；无法识别的字符串同样记录为

`StringInputParsingIssue`。



### 2\.3 过滤条件



|参数|含义|
|---|---|
|`where_constraints`|`WhereFilter` 对象列表|
|`where_constraint_strs`|过滤表达式字符串列表|



两者最多只能传一个。两类输入最终都会转换成 `PydanticWhereFilter`，再合并为一个

`PydanticWhereFilterIntersection`。这里的 intersection 表示多个过滤条件同时成立，

即逻辑上的 AND。



这个阶段只统一数据结构。过滤表达式中引用的维度、实体和时间维度是否存在、是否能从

指标对应的语义模型路径访问，由 resolver 在后续阶段解析。



### 2\.4 排序



|参数|含义|
|---|---|
|`order_by_names`|字符串形式的排序项|
|`order_by`|已结构化的 `OrderByQueryParameter` 对象|



两者最多只能传一个。字符串形式支持两种降序表达方式：



- 前缀 `-`，例如 `-bookings`。

- object builder 中的 `descending` 参数。

    

排序项会同时尝试按“指标”和“分组项”理解，形成一组候选输入。resolver 随后要求排序项

在本次查询已经选择的指标或分组项中恰好匹配一个，否则查询无效。



### 2\.5 其他控制参数



|参数|默认值|业务含义|
|---|---|---|
|`limit`|`None`|返回行数上限；负数无效|
|`time_constraint_start`|`None`|查询时间范围起点|
|`time_constraint_end`|`None`|查询时间范围终点|
|`min_max_only`|`False`|只计算单个分组项的最小值和最大值|
|`apply_group_by`|`True`|是否在结果上应用 GROUP BY；只有无指标查询才允许为 `False`|



`min_max_only=True` 有严格限制：



- 不能查询指标。

- 必须且只能查询一个分组项。

- 不能指定排序。

- 不能指定 limit。

    

其中“不能和指标一起使用”在 parser 入口直接检查，其余组合由 resolver 校验。



## 3\. 详细处理流程



### 3\.1 检查参数组合



方法首先执行两类前置检查：



1. `min_max_only=True` 时如果传入指标，立即抛出 `InvalidQueryException`。

2. 对四组等价输入调用 `assert_at_most_one_arg_set()`：

`metric_names/metrics`、`group_by_names/group_by`、

`order_by_names/order_by`、`where_constraints/where_constraint_strs`。



第二类检查针对调用方编程错误：同一种信息不能同时使用字符串接口和对象接口传入，违反时

触发 `AssertionError`。



随后，指标、分组项和排序项中的 `None` 被规范化为空元组，简化后续循环和组装逻辑。



### 3\.2 把外部输入转换为 resolver input



parser 不直接判断某个名称最终对应哪个语义对象。它先把输入转换为带有匹配模式的中间对象：



|外部输入|中间对象|
|---|---|
|指标|`ResolverInputForMetric`|
|分组项|`ResolverInputForGroupByItem`|
|排序项|`ResolverInputForOrderByItem`|
|limit|`ResolverInputForLimit`|
|where|`ResolverInputForQueryLevelWhereFilterIntersection`|
|min/max 模式|`ResolverInputForMinMaxOnly`|
|是否分组|`ResolverInputForApplyGroupBy`|



字符串只要符合某种语法，就会生成相应的匹配模式 `SpecPattern`。例如普通指标名会变成

`MetricSpecPattern`，dunder 分组名会变成包含实体路径、元素名和时间粒度信息的

`EntityLinkPattern`。名称是否真的存在，要到 resolver 与 semantic manifest 匹配时才确定。



语法都无法识别的指标或分组字符串不会马上抛异常，而是转换为 `InvalidStringInput` 并附带

解析 issue。这样可以和 resolver 发现的其他错误一起生成统一的错误消息。



### 3\.3 组装整条查询



所有中间输入会汇总为一个 `ResolverInputForQuery`：



```Plain Text
ResolverInputForQuery
├── metric_inputs
├── group_by_item_inputs
├── order_by_item_inputs
├── filter_input
├── limit_input
├── min_max_only
└── apply_group_by
```



这个对象是 parser 与 resolver 之间的边界。parser 负责识别外部语法，resolver 负责把

`SpecPattern` 解析为确定的 `Spec`。



### 3\.4 resolver 完成语义解析和校验



`MetricFlowQueryResolver.resolve_query()` 的处理顺序如下：



1. **解析指标**：在 manifest 的可用指标中匹配每个指标模式。必须恰好匹配一个；否则产生

`InvalidMetricIssue`，并提供可能的名称建议。

2. **基础查询检查**：查询至少包含一个指标或一个分组项；校验 limit、`min_max_only` 和

`apply_group_by` 的组合。

3. **第一次提前结束**：基础输入已经有问题时立即返回，避免基于错误指标继续推导，产生误导性错误。

4. **构建 resolution DAG**：根据查询指标和过滤条件建立分组项解析图。这个图描述复杂指标、

简单指标及其可访问维度之间的语义路径。

5. **解析分组项**：在 resolution DAG 允许的范围内，将每个分组项模式解析为确定的

dimension、entity 或 time dimension spec。

6. **解析排序**：排序项必须匹配本次查询中的一个指标 spec 或分组 spec。

7. **解析过滤条件**：解析查询级过滤器以及指标定义内部过滤器引用的字段，构建

`FilterSpecResolutionLookUp`。

8. **第二次提前结束**：分组、排序或过滤解析出现问题时停止，不执行依赖完整解析结果的查询级校验。

9. **查询级校验**：检查需要时间维度的累计/偏移指标、重复指标以及重复输出列名。

10. **生成查询规格**：构造 `MetricFlowQuerySpec`，并收集查询实际涉及的 semantic model。

    

解析成功后的 `MetricFlowQuerySpec` 包含：



- 指标 specs；

- dimension、entity、time dimension specs；

- 排序 specs；

- limit；

- where filter intersection 及过滤字段解析结果；

- `min_max_only` 和 `apply_group_by`；

- 原始指标和分组项的输入顺序。

    

### 3\.5 汇总并抛出错误



resolver 返回后，parser 将两类 issue 合并：



- parser 识别字符串语法时发现的 issue；

- resolver 在名称匹配、语义路径和业务规则校验中发现的 issue。

    

如果合并结果包含错误，`_raise_exception_if_there_are_errors()` 会生成按输入分组的错误消息，

包括错误描述、原始查询输入和问题所在的 resolution path，并抛出

`InvalidQueryException`。



因此，调用方拿到正常返回值时，可以认为查询输入已经通过了本阶段的语义校验。



### 3\.6 处理时间范围



时间范围在 resolver 成功之后处理。



- 起止时间都未提供：`MetricFlowQuerySpec.time_range_constraint` 保持为 `None`。

- 只提供起点：终点补为 `2040-12-31`。

- 只提供终点：起点补为 `2000-01-01`。

- 两端都提供：直接构造初始 `TimeRangeConstraint`。

    

构造后，范围会按照查询使用的 `metric_time` 最小粒度向外扩展到完整周期。例如查询按月分组：



```Plain Text
输入时间范围：2020-01-15 ～ 2020-02-15
实际时间范围：2020-01-01 ～ 2020-02-29
```



粒度的选择规则是：



1. 优先从查询中的 `metric_time` specs 选择最小粒度。

2. 如果查询没有显式选择 `metric_time`，通过 resolution DAG 推导可用的最小

`metric_time` 粒度。



这种扩展保证按周、月、季度或年聚合时，边界不会截断首尾时间桶。



## 4\. 返回值



方法返回不可变的 `ParseQueryResult`：



```Python
@dataclass(frozen=True)
class ParseQueryResult:
    query_spec: MetricFlowQuerySpec
    queried_semantic_models: Tuple[SemanticModelReference, ...]
```



- `query_spec`：后续构建 dataflow plan 所需的完整查询规格。

- `queried_semantic_models`：为完成该查询实际需要访问的 semantic model，来源包括分组项、

过滤字段以及指标最终依赖的简单指标模型。虚拟模型和 manifest 中不存在的引用不会出现在结果中。



## 5\. 示例



下面的示例查询 `bookings` 指标，按月度指标时间和国家分组，并按指标倒序取前 100 行：



```Python
result = query_parser.parse_and_validate_query(
    metric_names=("bookings",),
    group_by_names=("metric_time__month", "country"),
    where_constraint_strs=("{{ Dimension('country') }} = 'US'",),
    order_by_names=("-bookings",),
    limit=100,
    time_constraint_start=datetime.datetime(2020, 1, 15),
    time_constraint_end=datetime.datetime(2020, 3, 10),
)
```



业务上会发生以下转换：



1. `bookings` 被识别为指标模式。

2. `metric_time__month` 和 `country` 被识别为分组项模式。

3. 过滤字符串被包装为查询级 where filter intersection。

4. `-bookings` 被识别为对查询内 `bookings` 指标降序排序。

5. resolver 验证指标、分组和过滤字段能通过 semantic manifest 关联起来。

6. 时间范围按月扩展为 `2020-01-01` 至 `2020-03-31`。

7. 返回查询规格和实际涉及的 semantic model 列表。

    

## 6\. 关键设计点



### parser 与 resolver 分工



parser 处理输入形态和语法，resolver 处理语义。比如字符串 `bookings` 符合普通指标名语法，

并不等于 manifest 中一定存在这个指标；存在性由 resolver 验证。



### issue 先收集、后统一抛出



多数输入错误不会在发现的第一刻抛出，而是关联到对应 resolver input。最终错误信息可以同时说明

“哪个输入有问题”和“解析路径上的哪个位置失败”。



### resolution DAG 是语义可达性的依据



分组字段和过滤字段是否有效，不只取决于字段名是否存在，还取决于它能否从当前指标及其依赖模型

通过合法实体路径访问。resolution DAG 为这种判断提供上下文。



### 时间范围属于解析后的规格修正



时间边界需要依赖已经解析出的时间维度粒度，因此在 resolver 成功后才扩展，并通过

`query_spec.with_time_range_constraint()` 写回新的不可变查询规格。



## 7\. 常见失败场景



|场景|结果|
|---|---|
|同时传 `metric_names` 和 `metrics`|`AssertionError`|
|`min_max_only=True` 且传入指标|`InvalidQueryException`|
|查询既无指标也无分组项|`InvalidQueryException`|
|指标名称不存在或不能唯一匹配|`InvalidQueryException`|
|分组项对当前指标不可达|`InvalidQueryException`|
|排序项不在查询选择项中|`InvalidQueryException`|
|limit 为负数|`InvalidQueryException`|
|`apply_group_by=False` 且查询包含指标|`InvalidQueryException`|
|累计或时间偏移指标缺少所需时间维度|`InvalidQueryException`|
|指标重复或输出列名重复|`InvalidQueryException`|
|where 表达式无法解析或引用无效字段|`InvalidQueryException`|



## 8\. 一句话总结



`parse_and_validate_query()` 是 MetricFlow 查询入口的适配与编排层：它把多种外部参数统一成

resolver input，借助 semantic manifest 将其解析为确定的内部 specs，汇总业务校验错误，最后按

时间粒度修正范围并返回后续查询规划所需的 `MetricFlowQuerySpec`。

