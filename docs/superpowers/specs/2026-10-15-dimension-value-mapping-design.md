# 指标查询的维值展示映射

日期：2026-10-15

状态：业务行为已确认；按用户要求修订为 dbt / MetricFlow 原生协议扩展，修订版待评审，尚未实施产品代码。

## 1. 目标与已确认的约束

用户在 dbt 工程中配置业务维度的展示映射，映射数据来自另一个 semantic model。配置随构建发布，查询使用选定 buildId 的配置。

本功能由本仓库维护的 dbt、metricflow_semantic_interfaces 和 MetricFlow 源码共同实现。dbt 负责声明及产物传递，语义接口负责类型与引用校验，MetricFlow 负责维度发现和最终 SQL 映射；服务层只接入原生能力。直接使用修改后的 MetricFlow query/explain 也必须生效，不依赖本服务的 SQL 包装。

映射表负责保证原始值唯一，每个原始值最多对应一条映射记录。服务直接依赖这一数据约束，不在构建或查询阶段额外检查重复键。

指标查询先按照原始维度值完成筛选、分组、指标计算，再在 SQL 最外层关联映射模型，将输出的原始维度列替换为显示值列。数据库直接完成映射，不在 Python 内存中替换结果字符串，也不将映射写成硬编码 CASE 枚举。

例如映射数据为 `502 → 失败`、`404 → 失败`、`200 → 成功`。原始查询结果及最终结果为：

| 原始状态码 | 原始请求数 | 最终显示值 | 最终请求数 |
| --- | ---: | --- | ---: |
| 502 | 10 | 失败 | 10 |
| 404 | 20 | 失败 | 20 |
| 200 | 100 | 成功 | 100 |

502 和 404 的结果保持为两行。不得按显示值重新分组、去重或聚合；比率、去重计数、累计指标等同样保持原始计算结果。

## 2. 本期范围与设计默认值

本期覆盖 categorical 业务维度的单列等值映射，允许一次查询中的多个维度分别配置映射。每个业务维度最多配置一个映射，每个原始值最多对应一条映射记录，多个原始值可以对应同一个显示值。

以下为本 spec 提出的默认行为，供评审确认：

- 映射目标是专用于展示的 semantic model；目标的原始值维度、显示值维度均为 categorical，显示值为文本。
- 使用 LEFT JOIN。没有匹配记录或匹配记录的显示值为 NULL 时，回退为原始值的文本表示；原始 NULL 保持 NULL，空字符串是有效显示值。
- 映射后的结果列名和列顺序不变，列类型变为文本，不额外对外返回原始值列。
- 筛选和排序继续使用原始值。查询响应是展示结果，调用方不得按相同显示名称自动合并行。
- `DIMENSION_VALUES` 本期只返回原始值，不额外增加显示标签字段。
- 不支持时间粒度映射、复合键、范围匹配、递归映射、查询请求覆盖映射配置或本服务的映射管理接口。

不新增数据库连接配置。映射模型使用本次构建的执行连接和已解析物理关系；已有表可通过 dbt source 和包装 model 接入。

## 3. dbt 工程配置

### 3.1 映射模型

以下示例中 `dim_status_code` 是 dbt model 名称，不是直接填写的数据库表名。实际关系由 ref、物化配置、alias、schema 和构建执行环境共同解析。

```yaml
semantic_models:
  - name: status_code_mapping
    model: ref('dim_status_code')

    entities:
      - name: status_code_mapping_key
        type: primary
        expr: code

    dimensions:
      - name: raw_value
        type: categorical
        expr: code

      - name: display_value
        type: categorical
        expr: name
```

映射模型可以不定义指标或度量。`primary` 表达模型意图，实际原始值唯一性由映射表保证。

如果字典表已经存在，可声明 source，并用 dbt view 包装：

```sql
-- models/dim_status_code.sql
{{ config(materialized='view') }}

SELECT code, name
FROM {{ source('dictionary', 'status_code') }}
```

包装 model 可在自身 SQL 中筛选字典类别，确保同一个映射模型内原始值唯一；本期不增加映射配置中的任意 WHERE 表达式。

### 3.2 业务维度引用

以下片段放在业务 semantic model 的 dimensions 下：

```yaml
dimensions:
  - name: status_code
    type: categorical
    expr: status_code
    value_mapping:
      semantic_model: status_code_mapping
      value_dimension: raw_value
      display_dimension: display_value
```

`value_mapping` 是本次在 dbt 维度协议中新增的正式可选字段，与 expr、type 同级，并贯通到 MetricFlow 的语义接口。它属于本仓库 fork 的协议扩展，不表示上游未修改的 dbt / MetricFlow 已支持。

dbt v2 inline 格式在列的 dimension 对象中使用同一个字段；derived dimension 同样保留该字段。三种声明最终生成相同的 Dimension.value_mapping：

```yaml
models:
  - name: requests
    semantic_model:
      enabled: true
    columns:
      - name: status_code
        dimension:
          type: categorical
          value_mapping:
            semantic_model: status_code_mapping
            value_dimension: raw_value
            display_dimension: display_value
```

配置不再放入 config.meta；meta 中其他用户元数据保持原行为。旧产物没有该字段时按无映射读取。

| 字段 | 含义 |
| --- | --- |
| semantic_model | 同一构建中唯一的映射 semantic model 名称 |
| value_dimension | 该模型中提供原始匹配值的维度名称 |
| display_dimension | 该模型中提供显示文本的维度名称 |

字段引用的是语义维度，不是物理列。MetricFlow 必须尊重维度的 expr；未设置 expr 时使用维度名称。映射维度表达式在映射模型的物理关系作用域内解析，不对表达式做字符串列名替换。

## 4. 构建校验与封存

dbt 在 v1、v2 inline 和 derived dimension 的解析过程中读取正式字段，写入 manifest.json，并经 metricflow_semantic_interfaces 的类型化对象无损写入 semantic_manifest.json。完整解析、partial parsing、缓存往返及 state:modified 必须识别映射的新增、修改和删除。

语义接口校验跨模型引用，MetricFlow 解析实际物理关系。服务从原生产物投影展示配置到 published_catalog，与两个原生 manifest 一起封存；它不是配置解释或 SQL 生成的唯一实现。封存信息至少包含：

- 业务维度资源身份及所属 semantic model。
- 映射 semantic model、原始值维度、显示值维度的资源身份。
- 通过现有模型绑定解析的物理关系及两个维度的投影表达式。

同时记录展示映射的资源依赖以供追溯；该依赖不得转换成 MetricFlow 的计算实体关系，也不能破坏 dbt semantic model 对唯一物理来源的现有解析。两个原生 manifest 与 published_catalog 中的配置必须一致。

原生协议及构建校验必须拒绝以下配置：

1. 配置字段缺失、类型错误或存在未知字段。
2. 引用的 semantic model 不存在或解析不唯一，目标维度不存在或属于其他模型。
3. 业务维度或目标维度不是本期支持的 categorical 类型。
4. 目标不能解析到本构建中已验证的唯一物理表或视图，或目标表达式无法执行。
5. 映射目标继续声明 value_mapping，或作为指标的计算来源使用。
6. 原始匹配键为 NULL，或显示值不符合文本类型约定。

结构、引用和语义类型错误由 dbt 解析及共享语义校验在不连接数据库时识别；物理表达式、空键及数据库类型由 dbt-metricflow 的 adapter 校验入口在构建探测阶段验证。服务调用这些原生入口，不复制一份同类规则。

原始值列与业务维度必须具备兼容的数据库类型，JOIN 不通过统一转成字符串来掩盖类型差异。若源表键的类型不同，应由 dbt 包装 model 显式规范类型。

唯一性约定适用于实际参与 JOIN 的 value_dimension 值。如果配置 expr，映射模型的提供方也须保证表达式结果唯一。服务不新增唯一性扫描或要求额外的 unique 测试。

无映射的已封存目录以缺省空映射读取，维持现有查询行为。实现需验证新字段的序列化和旧产物读取；不引入映射持久化表或历史数据迁移。

## 5. 可用维度与维度取值

`POST /query/available-dimensions` 继续返回原业务维度，例如 status_code，其 optionId、过滤操作和原始值语义保持不变。不会把“成功”“失败”作为维度定义返回。

被引用的映射 semantic model 被视为展示专用模型。它的内部维度和经过它到达的展示专用路径不进入公开的可用业务维度，也不能作为业务 groupBy 或 filters 的独立选项。资源目录仍保留映射模型及其字段，以便查看配置和依赖。

展示模型隔离必须在 MetricFlow 自身实现：

- 不为展示映射建立业务模型到映射模型的计算实体边。
- MetricFlow 保留完整 manifest 供映射解析，但构建指标计算用的语义图时排除展示专用模型，阻止它们成为计算 JOIN 的终点或中间路径；原生可用维度入口使用同一边界。
- 不按 raw_value、display_value 等名称全局过滤，以免误伤其他业务模型的同名维度。
- MetricFlow 按输出实例的实际来源定位映射。服务当前按维度名称猜测 resourceId 的逻辑也应改为使用引擎返回的 semantic model 身份；无法唯一确定来源时返回明确错误，不能静默套用或忽略映射。

服务不得自行裁剪或改写 semantic manifest 来实现隔离，也不维护独立的展示维度黑名单。

`DIMENSION_VALUES` 继续对原始维度取值、去重、筛选，返回 `502`、`404`、`200` 等原始值，并保持原始类型。该模式不应用展示替换，以保证筛选请求可使用原始值。

## 6. SQL 生成与执行

### 6.1 数据流

```text
dbt 维度正式字段 value_mapping
    → manifest.json / semantic_manifest.json
    → MetricFlow 语义引用校验和来源解析
    → 按原始维度完成指标 SQL 生成与优化
    → MetricFlow 最终输出转换追加 N 个展示 JOIN
    → 原生渲染器及执行计划
    → engine.query() 执行 / engine.explain() 返回同一最终 SQL
    → 服务按现有协议序列化结果
```

只对最终输出中出现的已配置业务维度添加映射。如果某个维度仅出现在 WHERE 中而未输出，不为它追加展示 JOIN。

一次查询支持任意数量的已选映射维度，受现有查询维度数量上限约束。若最终输出有 N 个需要映射的维度，则在同一最外层 SELECT 中追加 N 个独立的 LEFT JOIN，不限制为单次关联。

内层保留原有筛选、聚合、指标依赖计算及基于原始值的排序和 `limit + 1` 取数策略。外层不新增按显示值执行的 WHERE、GROUP BY、DISTINCT、HAVING 或聚合。

### 6.2 SQL 形态

下例同时映射状态码和渠道两个维度，分别关联各自原始值唯一的映射模型。表名和文本 CAST 由当前 adapter 的方言渲染。

```sql
SELECT
    COALESCE(m0.display_value, CAST(q.request__status_code AS VARCHAR))
        AS request__status_code,
    COALESCE(m1.display_value, CAST(q.request__channel_code AS VARCHAR))
        AS request__channel_code,
    q.request_count
FROM (
    -- MetricFlow 原始查询：筛选、聚合、原始值排序和 limit + 1
) q
LEFT JOIN (
    SELECT
        code AS raw_value,
        name AS display_value
    FROM analytics.dim_status_code
) m0
    ON q.request__status_code = m0.raw_value
LEFT JOIN (
    SELECT
        channel_code AS raw_value,
        channel_name AS display_value
    FROM analytics.dim_channel
) m1
    ON q.request__channel_code = m1.raw_value
ORDER BY q.request__status_code, q.request__channel_code;
```

该 SQL 是结构示意，不是可直接执行的完整示例。多个维度使用独立且无冲突的表别名，依次添加 LEFT JOIN。输出显式枚举列，映射列替换对应投影，指标和未映射维度保持原来的列顺序和别名。

多次关联遵循以下规则：

- 每个 JOIN 的左侧匹配条件都引用 q 中对应的原始维度列，不使用其他映射 JOIN 的显示值作为关联键。
- 按最终输出维度的顺序生成确定且无冲突的别名，如 m0、m1。每个维度的输出投影只引用自己对应的映射别名。
- 两个业务维度即使引用同一个映射 semantic model，也分别生成 JOIN 和别名。例如 source_status、target_status 都引用 status_code_mapping 时，分别以 q.source_status、q.target_status 关联，不能仅按目标模型合并 JOIN。
- 每个维度独立处理未匹配或显示值为空的回退，不影响其他维度的映射结果。
- 所有映射均在同一最外层完成；不为每个映射维度重新计算指标，也不逐层嵌套指标查询。

即使内层已排序，外层也必须显式恢复请求的排序；不能依赖子查询顺序。外层排序引用 q 的原始结果列，而不是显示值别名。若请求没有排序，维持现有的无顺序保证语义，不增加默认排序规则。

映射转换以结构化 SQL 计划、输出列及语义绑定为依据，不对 SQL 文本做全局查找替换。在 MetricFlow 内部的 SQL 优化完成之后、方言渲染及执行任务创建之前增加专用最终输出转换。转换后不再运行会将展示 JOIN 下推到聚合前的优化。

SQL 表达式和方言支持在 MetricFlow 原生节点与 renderer 中实现；服务不读取 explain 再自行拼接或包装 SQL。正常 query 和 explain 共用转换后的执行计划，最终 SQL、参数、输出列信息保持一致。数据表及输出关系两类原生结果终点都须经过相同的映射边界。

### 6.3 查询模式

| 模式 | 行为 |
| --- | --- |
| QUERY | 编译原始指标 SQL，添加外层映射后执行 |
| EXPLAIN | 返回与 QUERY 相同转换路径生成的最终 SQL，不执行指标查询 |
| DIMENSION_VALUES | 返回原始值，不添加映射替换 |
| PREVIEW | 维持物理数据集预览行为，不应用业务维度映射 |

无映射时保持现有执行路径。MetricFlow 根据原生 query_type 对普通指标查询应用映射，对 DIMENSION_VALUES 跳过映射；不新增供 HTTP 请求覆盖配置的参数。服务继续调用 engine.query / engine.explain，不另起一条绕过引擎执行计划的 SQL 执行路径。

## 7. 映射键唯一性与数据变化

配置随构建固定，不等于映射表内容随构建快照化。本服务现有构建不提供历史物理数据快照，映射内容同样在每次查询时从实际表或视图读取。更新字典内容可以影响后续查询的显示结果。

映射表在构建时及后续数据更新中持续保证原始值唯一，服务不承担重复键检测或修复职责。查询按第 6 节直接投影映射模型的原始值与显示值，再执行 LEFT JOIN。

不为映射追加 GROUP BY、DISTINCT、匹配条数、显示值聚合或内部诊断列。在原始值唯一的前提下，每条原始结果最多匹配一条映射记录；LEFT JOIN 保留未匹配行，因此结果行数和既有截断语义保持不变。显示值允许重复，不要求唯一。

这一性质适用于连续的多个映射 JOIN：每次关联都最多匹配一条记录并保留未匹配行，因此 N 次关联后仍保持原始结果行数和指标值。

## 8. 返回协议与失败行为

QueryRequest 不新增映射配置字段，配置只来自选定构建。QueryResult 的字段结构保持不变，columns 必须反映实际返回类型，不能仍把已替换的状态码声明为整数。

dbt Dimension、共享语义 Dimension 协议及 Pydantic 实现均新增可选映射字段；MetricFlow 内部维护已解析的映射身份和投影信息。封存目录只投影该原生配置，服务的查询请求不新增 valueMappings 或任意 JOIN SQL。

旧 manifest、semantic_manifest 和缓存缺少该字段时默认无映射。对 fork 的协议增量进行 schema、序列化和版本兼容测试，不声称未修改的上游消费者支持该扩展；发布时固定 dbt 与 MetricFlow 的配套源码提交。

配置错误在构建阶段报告具体的模型、维度和配置字段。查询时来源解析失败或匹配类型错误导致明确失败，不静默回退成未映射查询。错误沿用统一 error 协议，并使用可区分的映射错误码；不得暴露连接凭据。

不存在映射记录是正常数据情况，按第 2 节回退。数据库连接、权限及执行错误沿用既有运行时错误处理。

## 9. 预计影响的代码边界

此处界定职责，不作为实施步骤或实施计划。

| 位置 | 预期职责变化 |
| --- | --- |
| vendor/dbt/core/dbt/contracts/graph/unparsed.py、artifacts/resources、parser/schema_yaml_readers.py | 正式字段、v1/v2/derived 解析及原生 manifest 输出 |
| vendor/dbt/core/dbt/contracts/graph/semantic_manifest.py、nodes.py、parser/partial.py | semantic manifest 往返、变更识别与缓存正确性 |
| vendor/metricflow/metricflow_semantic_interfaces | 映射协议、Pydantic 类型及共享引用校验 |
| vendor/metricflow/metricflow_semantics | 完整映射 lookup、真实来源解析及计算语义图隔离 |
| vendor/metricflow/metricflow/engine、execution、plan_conversion | 查询模式、最终输出映射及原生执行计划接入 |
| vendor/metricflow/metricflow/sql、metricflow_semantics/sql | 显示投影表达式、JOIN、排序、引用和方言渲染 |
| vendor/metricflow/dbt-metricflow | 复用 dbt adapter 的构建期映射数据校验 |
| src/dbt_metricflow_service/models、platform/catalog.py、platform/sealed_catalog.py | 读取正式字段并封存原生配置、来源和展示依赖 |
| src/dbt_metricflow_service/platform/dimensions.py、platform/metricflow.py 及构建入口 | 接入原生维度发现、query/explain 和数据验证，不实现第二套映射 |
| 原生结果与服务序列化边界 | 保持实际输出类型和既有截断语义 |
| README.md 与相关测试 | 说明配置、模式差异及验证行为 |

不重构无关查询流程，不增加通用 SQL 改写框架，不新增第三方依赖作为默认方案。

## 10. 验收标准

### 配置与发布

- 真实 dbt parse/build 能接受 v1、v2 inline 和 derived dimension 的正式 value_mapping 字段，两个原生 manifest 无损保留，协议及 schema 测试通过。
- partial parsing、缓存往返及 state:modified 能识别映射配置的新增、修改和删除，无旧映射残留。
- 支持已有 source 经包装 model/view 接入；物理关系遵循构建环境和 alias。
- 缺失引用、错误维度归属、空键和不支持的类型在构建时失败。
- 相同维度名出现在不同模型、不同关联路径时，不发生映射串用。
- 新构建修改配置后，旧 buildId 仍读取旧的封存配置；旧目录未配置映射时继续可读。

### 计算和 SQL

- `502 → 失败`、`404 → 失败`、`200 → 成功` 返回三行，指标分别与原始查询一致。
- `WHERE status_code = 502` 仍只选中 502；WHERE、GROUP BY、指标计算不引用显示值。
- 比率和去重计数不会因相同标签被求和或重新计算。
- 未匹配、原始 NULL、显示 NULL、空字符串分别满足约定。
- 多维度映射、维度 expr、列顺序、列类型及别名冲突有覆盖。
- 两个及三个以上输出维度同时映射时，分别生成对应数量的独立 LEFT JOIN；全部 ON 条件使用各自的原始结果列，行数和指标值保持不变。
- 两个业务维度引用同一映射模型时，使用独立别名及各自的原始值关联，显示结果不串用。
- 混合已映射维度、未配置映射维度及仅用于筛选的维度时，仅为需要映射的输出维度生成 JOIN；其中一个映射未命中不影响其他映射。
- 原始值排序和指标排序均正确；行数上限、额外探测行及 resultTruncated 保持既有语义。
- 仅用于筛选的维度不追加映射 JOIN；未配置映射的查询行为不变。
- 原生 engine.explain 生成的映射 SQL 与 engine.query 实际执行 SQL 采用同一执行计划转换，不依赖服务。
- 直接关联原始值唯一的映射数据，不生成重复键检查、映射聚合、去重或内部诊断列；不同原始值使用相同显示值时仍保留独立结果行。

### 维度发现与执行证据

- 可用维度仍包含原业务维度，不包含展示专用模型的内部维度或路径，不误伤业务模型同名字段。
- DIMENSION_VALUES 返回原始值及原始类型，可直接用于后续筛选；PREVIEW 行为不变。
- 至少以真实 DuckDB 执行测试证明外层 JOIN、行数保持及类型行为；PostgreSQL、StarRocks 分别验证方言生成。
- PostgreSQL、StarRocks 的真实数据库执行结果与仅 SQL 渲染的验证必须在交付说明中区分，未运行的集成测试不得宣称通过。
- 所有展示映射发生在数据库 SQL 中；应用层按既有流程序列化结果。
- 在不导入 dbt_metricflow_service 的测试中，原生 dbt 协议往返、MetricFlow 维度发现和多 JOIN 查询完整通过。
