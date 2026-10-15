# Dimension Value Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按固定构建中的维度配置，在指标 SQL 最外层生成多个独立 LEFT JOIN，用映射 semantic model 的显示值替换输出维度值，同时保持原始计算语义。

**Architecture:** dbt 维度的 config.meta.value_mapping 经校验后封存为类型化引用和物理投影。计算用的 MetricFlow 语义视图排除展示专用模型，维度绑定保留真实来源；SQL 转换使用 explain 的输出语义与结构化计划，对每个映射输出维度追加独立 JOIN。数据库执行最终 SQL，应用层只序列化结果。

**Tech Stack:** Python 3.11–3.14、Pydantic、dbt-core 1.12.5、dbt-metricflow 0.15.0、MetricFlow 0.213.0、现有 dbt adapters、pytest、Ruff；依赖以 uv.lock 为准。

**Spec:** [已确认的设计](../specs/2026-10-15-dimension-value-mapping-design.md)。实施前必须读完 spec；本计划不更改已确认的行为。

状态：待评审；只完成计划，不开始产品实现。推荐在当前会话按顺序执行，任务间共享的数据类型较多，不适合并行修改。

## Global Constraints

- 映射表负责保证原始值唯一，每个原始值最多对应一条映射记录。服务直接依赖这一数据约束，不在构建或查询阶段额外检查重复键。
- 配置只来自选定 buildId；不增加请求覆盖配置、配置 CRUD、数据库连接或持久化表。
- 每个业务维度最多配置一个映射，每个原始值最多对应一条映射记录，多个原始值可以对应同一个显示值。
- 使用 LEFT JOIN。没有匹配记录或匹配记录的显示值为 NULL 时，回退为原始值的文本表示；原始 NULL 保持 NULL，空字符串是有效显示值。
- 映射后的结果列名和列顺序不变，列类型变为文本，不额外对外返回原始值列。
- 筛选和排序继续使用原始值。不按显示值分组、去重或再次计算指标。
- N 个映射输出维度生成 N 个独立 JOIN；同一个映射模型可以重复关联，使用不同别名。
- DIMENSION_VALUES 返回原始值，PREVIEW 保持现有行为；QUERY 和 EXPLAIN 使用同一 SQL 转换。
- 不为映射追加 GROUP BY、DISTINCT、匹配条数、显示值聚合或内部诊断列。
- 不修改 vendor，不做全局 SQL 字符串替换，不增加第三方依赖或通用 SQL 改写框架。
- 自有 Python 函数完整标注参数和返回值；固定载荷用具体类型；新常量遵循 constants.py 和 test_string_constants.py 的现有规则。
- 测试使用临时 DuckDB 或独立测试 PostgreSQL。渲染测试与真实数据库执行证据必须分别报告。

## Review Focus

1. 多模型的同名维度及同 token 的来源歧义：按实际 semantic model 归属绑定，不能取第一个候选。由任务 3 的 `test_dimension_source_disambiguation` 覆盖。
2. 两个维度复用同一个映射模型：两个独立 JOIN，不能按目标模型去重。由任务 5 的 `test_same_mapping_model_has_independent_joins` 覆盖。
3. 查询返回零行或映射列全是 NULL：列仍声明为文本，未映射列和数值精度保持原样。由任务 6 的 `test_mapped_column_type_with_empty_or_null_results` 覆盖。
4. 维度 expr、带引用符的物理标识符、大小写或保留字：使用真实投影和 adapter 引用规则，不能按列名替换字符串。由任务 5 的 `test_mapping_projection_respects_expr_and_quoting` 覆盖。
5. 带 CTE、参数、多个排序项及 limit + 1 的计划：内层计算不变，外层恢复原始列排序，绑定参数不丢失。由任务 5 的 `test_mapping_preserves_cte_parameters_order_and_limit` 覆盖。

## 当前代码证据与实现决策

- `application/query_plan.py::compile_query` 已将 optionId 转换为 token；`QueryService.submit` 拥有固定构建的 PublishedCatalog，适合传递封存映射。
- `platform/dimensions.py::map_available_dimensions` 当前只按维度名寻找资源，存在同名歧义。上游 `Dimension.semantic_model_reference` 已提供来源，服务尚未保留它。
- `platform/metricflow.py::execute_programmatic` 的 QUERY 直接调用 engine.query，EXPLAIN 调用 engine.explain。映射分支必须改为 explain → 转换 → SqlClient.query，不能再调用 engine.query。
- `MetricFlowExplainResult.convert_to_execution_plan_result.convert_to_sql_plan_result` 提供 `instance_set` 和 `sql_plan`；`SqlPlan.render_node`、`SqlSelectStatementNode`、`SqlJoinDescription` 和 `SqlPlanRenderer` 可直接复用。
- `CLIConfiguration.sql_client` 是公开访问入口；StarRocks 使用已有 StarRocksSqlClient。不要读取或修改 engine 的私有客户端字段。
- `platform/build.py::execute_publication` 当前在写 published_catalog 之前探测查询能力。任务 6 将探测移至目录生成之后、产物 capture 之前，以便同一次受控探测完成映射验证。
- `BaseAdapter.get_column_schema_from_query(sql)` 能从投影查询获得列类型；使用 adapter 提供的列类型判断，不从一行样本推断类型。
- 展示专用模型的识别来源是封存的映射目标集合，不另加用户配置标志。为保证包括中间路径在内的隔离，给计算引擎构建一个排除这些模型的语义 manifest 副本；完整封存文件和资源目录不变。

## 文件职责与任务顺序

下列均为仓库根目录相对路径；现有文件只修改所列职责，不重构相邻功能。

| 文件 | 职责 | 任务 |
| --- | --- | --- |
| src/dbt_metricflow_service/models/native.py | 严格的映射配置及开放 meta 投影 | 1 |
| src/dbt_metricflow_service/models/artifacts.py | 可选封存映射类型 | 1 |
| src/dbt_metricflow_service/platform/models.py | 来源身份、引擎映射载荷 | 1、3、4 |
| src/dbt_metricflow_service/platform/value_mapping_catalog.py（新建） | 配置引用解析、展示目标集合、映射领域错误 | 2 |
| src/dbt_metricflow_service/platform/sealed_catalog.py | 封存配置和展示依赖 | 2 |
| src/dbt_metricflow_service/platform/metricflow.py | 计算语义视图、客户端选择、执行和探测接入 | 3、6 |
| src/dbt_metricflow_service/platform/queries.py、platform/dimensions.py | 维度来源传递与绑定；结果列类型 | 3、6 |
| src/dbt_metricflow_service/application/queries.py | 将目录维度绑定错误转为现有用例错误 | 3 |
| src/dbt_metricflow_service/application/query_plan.py | 只为 QUERY/EXPLAIN 的输出维度传递映射 | 4 |
| src/dbt_metricflow_service/platform/value_mapping_sql.py（新建） | 结构化外层 JOIN 和方言渲染 | 5 |
| src/dbt_metricflow_service/platform/value_mapping_validation.py（新建） | 构建时投影、空键及类型校验 | 6 |
| src/dbt_metricflow_service/platform/build.py、runtime/executor.py、runtime/service.py、execution/metricflow.py | 构建顺序和映射错误转换 | 6 |
| src/dbt_metricflow_service/constants.py | 新增错误码与共享字段常量 | 1–6 按需 |
| tests/dimension_mapping_helpers.py（新建） | 本功能的临时工程及目录 fixture | 1、2、7 |
| tests/test_dimension_value_mapping_*.py（下文指定） | 各职责的回归测试 | 1–6 |
| tests/integration/test_dimension_value_mapping.py（新建）、README.md | 真实工具链验收与配置文档 | 7 |

依赖顺序：1 → 2 → 3 → 4 → 5 → 6 → 7。开始实现前按 using-git-worktrees 技能检查当前工作区，选择合适隔离目录；不要因本计划修改或重置已有用户改动。

---

### Task 1: 配置契约与真实 dbt 解析样例

**Files:** 修改 models/native.py、models/artifacts.py、platform/models.py；新建 tests/dimension_mapping_helpers.py、tests/test_dimension_value_mapping_contracts.py。

**Interfaces:**

- native.py 新增 `ValueMappingConfig(BaseModel)`：必填且非空的 `semantic_model: str`、`value_dimension: str`、`display_dimension: str`，保持 YAML snake_case，extra=forbid。
- 新增 `NativeDimensionMeta(NativeProjection)`，字段 `value_mapping: ValueMappingConfig | None = None`；`NativeConfig.meta` 采用此开放投影并默认空。其他 meta 键继续无损保留。
- artifacts.py 新增 `DimensionValueMapping(Contract)`：`semantic_model_id: str`、`value_dimension_id: str`、`display_dimension_id: str`、`relation: RelationName`、`value_expression: str`、`display_expression: str`。`DimensionAttributes.value_mapping: DimensionValueMapping | None = None`。
- platform/models.py 新增 `QueryValueMapping(Contract)`：`dimension: str`（原生 token）、`source_dimension_id: str`、`mapping: DimensionValueMapping`；`PlatformQueryRequest.value_mappings: list[QueryValueMapping]` 默认空。
- helper：`make_mapping_project(tmp_path: Path) -> tuple[Path, Path]` 返回 project、profiles。复制现有 DuckDB 工程并在临时目录追加 spec 的映射模型和配置；不修改共享 fixture，不调用已过时的 call_worker。

- [ ] **Step 1: 添加失败测试。** `test_mapping_config_is_strict_and_other_meta_survives` 验证三个必填键、未知映射键、错误类型与 unrelated meta；`test_old_catalog_defaults_to_no_mapping` 验证旧目录读取；`test_dbt_parse_preserves_value_mapping` 使用 dbtRunner 解析 helper 工程。

```python
assert node.config.meta.value_mapping.semantic_model == "status_code_mapping"
assert node.native_details()["config"]["meta"]["owner"] == "analytics"
assert legacy_dimension.attributes.value_mapping is None
assert parsed_mapping == {
    "semantic_model": "status_code_mapping",
    "value_dimension": "raw_value",
    "display_dimension": "display_value",
}
```

- [ ] **Step 2: 运行失败测试。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_contracts.py`；预期新类型或字段缺失导致失败。真实解析通过 `dbtRunner.invoke`，不启动引擎 CLI 子进程。
- [ ] **Step 3: 实现上述类型与 helper。** 用当前工具链确认传统 semantic_models YAML 和 config.meta 被保留；只有成功解析的样例才能进入后续任务。旧 published_catalog 的 schemaVersion 保持 1，可选字段缺省读取。
- [ ] **Step 4: 验证契约。** 重跑 Step 2，并运行 `uv run --frozen pytest -q tests/test_native_contracts.py tests/test_build_contract.py tests/test_json_value_boundaries.py`；预期全部通过。只调整因新增内部默认字段受到影响的精确断言。
- [ ] **Step 5: 提交。** 只暂存本任务文件，提交 `feat: define dimension value mapping contracts`。

### Task 2: 解析映射引用并随构建封存

**Files:** 新建 platform/value_mapping_catalog.py、tests/test_dimension_value_mapping_catalog.py；修改 platform/sealed_catalog.py、constants.py、tests/dimension_mapping_helpers.py、tests/test_publication_catalog.py。

**Interfaces:**

- `ValueMappingError(ValueError)`：`__init__(self, code: str, reason: str, *, dimension_resource_id: str | None = None) -> None`。保存这三个安全字段，str(error) 仅由这些字段生成；不保存驱动异常文本或 SQL。
- `resolve_value_mappings(native_catalog: NativeCatalog, relation_bindings: list[RelationBinding]) -> dict[str, DimensionValueMapping]`，键为业务维度资源 ID；纯函数，不连接数据库。
- `mapping_model_names(catalog: PublishedCatalog) -> frozenset[str]`，由封存目标 ID 查模型名。
- `read_published_catalog(target: Path) -> PublishedCatalog` 读取目标目录中的封存文件；已有文件缺少可选映射字段是兼容情况，文件缺失或损坏仍是错误。
- 新增错误码 `INVALID_VALUE_MAPPING`；展示依赖关系 kind 使用 `DIMENSION_VALUE_MAPPING`。

- [ ] **Step 1: 添加失败测试。** 用现有 catalog fixture 模式构造两个业务模型和一个展示模型，覆盖成功封存、物理 alias、expr 缺省、同名维度、每种无效配置及旧产物往返。

```python
assert resolved[source_id].semantic_model_id == "semantic_model.sample.status_code_mapping"
assert resolved[source_id].relation.identifier == "actual_status_dictionary"
assert resolved[source_id].value_expression == "code"
assert resolved[source_id].display_expression == "name"
assert restored.resources == published.resources
assert error.code == "INVALID_VALUE_MAPPING"
```

- [ ] **Step 2: 运行失败测试。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_catalog.py`；预期新解析入口缺失或封存字段为空。
- [ ] **Step 3: 实现解析和封存。** 在 `build_published_catalog` 取得已验证关系后解析配置，将结果写入对应 DimensionAttributes。校验三个维度均为 categorical、目标及字段归属唯一、物理关系已绑定、目标不继续声明映射、目标不出现在直接或派生指标的 dataset_ids 中。引用链和目标模型均留在目录；增加目标模型到业务维度的展示依赖，不添加计算 entity。
- [ ] **Step 4: 完成兼容与语义校验。** 检查原生 manifest 和 semantic_manifest 的维度配置一致性，保留其他 meta。实体声明或存在同名维度不能替代实际资源身份。不要扫描重复键，也不产生 unique 测试。
- [ ] **Step 5: 验证并提交。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_catalog.py tests/test_publication_catalog.py tests/test_catalog_queries.py` 全部通过；提交 `feat: seal semantic dimension mapping references`。

### Task 3: 隔离展示模型并保留维度真实来源

**Files:** 修改 platform/metricflow.py、platform/queries.py、platform/dimensions.py、platform/models.py、application/queries.py；新建 tests/test_dimension_value_mapping_dimensions.py；更新 tests/test_metricflow_operations.py 中对应替身。

**Interfaces:**

- `NativeDimension.semantic_model_name: str | None = None`，来自 `Dimension.semantic_model_reference.semantic_model_name`，不从 token 猜测。
- `DimensionBinding` 保持现有 token、grain_tokens，并增加默认空的 `resource_id: str | None = None`，保持现有两参数构造兼容。
- `_sql_client(configuration: CLIConfiguration) -> SqlClient`：StarRocks 返回已有 StarRocksSqlClient，其余返回 configuration.sql_client；engine 和最终 SQL 执行共用选择规则。
- `_engine(configuration: CLIConfiguration, *, excluded_semantic_models: frozenset[str] = frozenset()) -> MetricFlowEngine`：无排除项时保持现有路径，有排除项时用新的 PydanticSemanticManifest 实例及 SemanticManifestLookup 创建计算引擎，不修改原对象。
- 保持 `available_dimensions(engine, metrics)`、`map_available_dimensions(build_id, metric_ids, catalog, native)` 的公开签名；新增来源歧义错误码 `AMBIGUOUS_DIMENSION_SOURCE`。

- [ ] **Step 1: 添加失败测试。** `test_dimension_source_disambiguation` 覆盖两模型同名、同 token 不同来源、多跳路径及 metric_time 无来源；`test_mapping_models_are_absent_from_compute_graph` 覆盖映射目标直接维度和通过它的中间路径。

```python
assert by_token["order__status_code"].resource_id == orders_dimension_id
assert by_token["return__status_code"].resource_id == returns_dimension_id
assert error.code == "AMBIGUOUS_DIMENSION_SOURCE"
assert "status_code_mapping" not in compute_model_names
assert "status_code_mapping" in original_model_names
assert business_raw_value_dimension_id in available_resource_ids
```

- [ ] **Step 2: 运行失败测试。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_dimensions.py`；预期来源丢失或展示模型仍可达。
- [ ] **Step 3: 接入计算视图。** `execute_available_dimensions` 读取当前工程恢复的 published_catalog，取目标集合并建立计算引擎；QUERY/EXPLAIN/DIMENSION_VALUES 必须使用同一视图（执行接入由任务 6 完成）。副本保留 metrics、project_configuration、saved_queries 等字段，只移除展示专用 semantic_models。
- [ ] **Step 4: 精确绑定原生维度。** `available_dimensions` 保留来源；`map_available_dimensions` 按来源和维度名定位资源，并在 token 去重前检查来源冲突。无来源的 metric_time 保持原逻辑；与映射有关而不能唯一判定的业务来源报错。`QueryService._available_dimensions` 将目录绑定阶段的 ValueMappingError 转为 ServiceError，测试错误码与 HTTP 422，避免发生在运行时边界之外的错误变成 500。无映射的旧测试替身允许原有唯一候选路径，不能破坏已有普通查询。
- [ ] **Step 5: 验证并提交。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_dimensions.py tests/test_query_plan.py tests/test_platform_queries.py tests/test_metricflow_operations.py` 全部通过；提交 `feat: isolate display mapping models from query dimensions`。

### Task 4: 将多个映射输出维度传递到引擎请求

**Files:** 修改 application/query_plan.py；扩展 tests/test_query_plan.py、tests/test_native_contracts.py、tests/test_synchronous_queries.py。

**Interfaces:** 消费任务 1 的 QueryValueMapping 和任务 3 的 DimensionBinding.resource_id；`compile_query` 签名保持不变，返回 PlatformQueryRequest.value_mappings。

- [ ] **Step 1: 添加失败测试。** 参数化 QUERY/EXPLAIN；选中状态码、渠道和另一个复用状态字典的维度。另设仅用于筛选的映射维度、无映射分组和 DIMENSION_VALUES/PREVIEW。

```python
assert [item.dimension for item in plan.value_mappings] == [
    "request__status_code", "request__channel_code", "request__target_status",
]
assert plan.value_mappings[0].mapping == plan.value_mappings[2].mapping
assert plan.filters[0].value == 502
assert plan.order_by == ["request__status_code", "-request_count"]
assert dimension_values_plan.value_mappings == []
assert preview_plan.value_mappings == []
```

- [ ] **Step 2: 运行失败测试。** `uv run --frozen pytest -q tests/test_query_plan.py -k mapping`；预期映射列表为空。
- [ ] **Step 3: 实现最小转换。** 仅在 QUERY/EXPLAIN 处理 group_by 中具有封存配置的已解析资源，按选择顺序生成 QueryValueMapping。filters、order_by、metric 名称、时间和 limit 保持原有转换。来源缺失或资源不一致时沿现有方式抛出 ServiceError，不默默丢掉配置；不按映射模型去重。
- [ ] **Step 4: 验证接口边界。** `uv run --frozen pytest -q tests/test_query_plan.py tests/test_native_contracts.py tests/test_build_contract.py tests/test_synchronous_queries.py`；全部通过。公开 QueryRequest 继续拒绝调用方传入 valueMappings 或任意 JOIN SQL。
- [ ] **Step 5: 提交。** 提交 `feat: carry selected dimension mappings into engine requests`。

### Task 5: 基于结构化计划生成多个最外层 JOIN

**Files:** 新建 platform/value_mapping_sql.py、tests/test_dimension_value_mapping_sql.py；需要的新错误码放 constants.py。

**Interfaces:**

- `MappedSql` 为 frozen dataclass：`sql: str`、`bind_parameters: SqlBindParameterSet`、`display_columns: tuple[str, ...]`。
- `compile_mapped_sql(explained: MetricFlowExplainResult, mappings: list[QueryValueMapping], *, sql_client: SqlClient, adapter: BaseAdapter) -> MappedSql`；无数据库 I/O，不修改 explained。
- `mapping_projection(mapping: DimensionValueMapping, *, adapter: BaseAdapter) -> SqlSelectStatementNode`：投影 sealed expr 为 raw_value/display_value，供本任务和任务 6 复用。
- 使用任务 2 的 ValueMappingError，不能处理的结构报告 `VALUE_MAPPING_SQL_UNSUPPORTED`，不能退回全局字符串替换。

- [ ] **Step 1: 添加失败测试。** 在临时 DuckDB 创建原始聚合结果与两个映射关系，构造带 InstanceSet 的真实 SqlPlan；SQL 生成后直接执行。首个断言为按原始状态码升序返回 `[("成功", 100), ("失败", 20), ("失败", 10)]`，证明相同标签没有合并。
- [ ] **Step 2: 增加 Review Focus 中的四组测试。** 用参数化测试覆盖 0、1、2、3 个映射；同目标复用；expr 与物理引用；CTE、绑定参数、混合排序和 limit。

```python
assert len(outer.join_descs) == 3
assert len({join.right_source_alias for join in outer.join_descs}) == 3
assert mapped.bind_parameters == original_bind_parameters
assert mapped.display_columns == ("request__status_code", "request__channel_code")
assert explained.convert_to_execution_plan_result.convert_to_sql_plan_result.sql_plan is original_plan
assert explained.sql_statement.sql == original_sql
assert mapped_rows == [("成功", 100), ("失败", 20), ("失败", 10)]
```

这些断言分属三映射、双映射和单状态码用例。测试通过记录 renderer 收到的 SqlPlan 取得 outer，调用前保存 original_plan 和 original_sql；执行断言使用数据库结果。不要以全文 COUNT/GROUP BY 搜索禁止重复键检测，因为内层指标本身可以合法聚合。

- [ ] **Step 3: 运行失败测试。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_sql.py`；预期转换入口缺失。
- [ ] **Step 4: 实现语义列定位与 JOIN。** 从 convert_to_sql_plan_result 的 instance_set 将维度 spec 和实际 associated_column 对齐，并核对 defined_from 与 source_dimension_id。按最终 SELECT 列顺序生成外层投影，不假设 token 等于 SQL 输出别名。保留原始 render_node 为左侧子查询；每个映射创建独立映射投影和 LEFT JOIN，ON 只引用左侧原始列。
- [ ] **Step 5: 实现表达式、方言与排序。** 用 adapter.Relation 和 adapter.quote 引用物理关系和列；映射 expr 作为已封存的 SQL 表达式在单一映射来源内求值。原生表达式节点缺少通用文本 CAST 时，使用仓库已采用的 sqlglot 构造该标量表达式，再交给 SqlStringExpression；不解析或替换整个内层 SQL。使用当前 SqlPlanRenderer 渲染新 SqlPlan，保留返回的绑定参数。根据 query_spec 的排序 spec 和 instance_set 解析实际输出列，在外层按 q 的原始列恢复原有顺序及方向；保留内层 limit + 1 和 CTE 作用域，不运行可能把展示 JOIN 下推的后续优化。
- [ ] **Step 6: 验证并提交。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_sql.py` 全部通过。DuckDB 做真实执行；PostgreSQL renderer、StarRocksSqlClient 的现有 renderer 配合各自 adapter 引用规则做生成验证，包含文本 CAST 和保留字。提交 `feat: render outer joins for dimension display mappings`。

### Task 6: 执行最终 SQL、构建验证和错误边界

**Files:** 新建 platform/value_mapping_validation.py、tests/test_dimension_value_mapping_execution.py、tests/test_dimension_value_mapping_validation.py；修改 platform/metricflow.py、platform/queries.py、platform/build.py、runtime/executor.py、runtime/service.py、execution/metricflow.py；扩展 tests/test_metricflow_operations.py、tests/test_publication_build.py、tests/test_realtime_execution.py。

**Interfaces:**

- `validate_mapping_data(catalog: PublishedCatalog, *, adapter: BaseAdapter, sql_client: SqlClient) -> None`，在构建受控引擎线程中运行；消费任务 5 的 mapping_projection。
- `serialize_rows(table: MetricFlowDataTable, *, limit: int, text_columns: frozenset[str] = frozenset()) -> DataQueryResult`；text_columns 只固定结果类型元数据，绝不修改单元格内容。
- `probe_query_capability(project: Path, profiles: Path) -> ProbeResult` 和 `execute_programmatic(project, profiles, request)` 保持签名；读取封存目录并消费前序任务。
- 新增 `INVALID_VALUE_MAPPING_DATA` 错误码。ValueMappingError 在运行时转为现有 ServiceError；构建时转为 ExecutionError 和安全 JobErrorDetail，保留模型/维度信息。
- `MetricFlowResult.error_message: str | None = None` 只传递上述安全错误说明；不把驱动异常加入这个字段，成功结果仍为 None。

- [ ] **Step 1: 添加失败测试。** 映射 QUERY 的替身 engine.query 必须设置为立即失败；断言只有 sql_client.query 执行最终 SQL。EXPLAIN 使用同一编译器但不得执行最终数据 SQL，DIMENSION_VALUES 和 PREVIEW 不应用映射。

```python
assert executed_sql == explained_result.sql
assert result.rows == [["成功", "100"], ["失败", "20"], ["失败", "10"]]
assert engine_query_calls == []
assert result.columns[0].type == "str"
assert truncated_result.truncated is True
```

结果替身中的指标使用 Decimal，故序列化为字符串。最后一个截断断言使用请求 limit=2 的独立测试；无截断用例断言 `result.truncated is False`。平台层始终使用 DataQueryResult.truncated，HTTP 层才是 resultTruncated。

- [ ] **Step 2: 添加类型和构建验证测试。** `test_mapped_column_type_with_empty_or_null_results` 覆盖零行、全 NULL、Decimal 指标；`test_mapping_validation_checks_null_keys_and_types_only` 覆盖空键、非文本显示值、不兼容的源/映射键类型、无效 expr、空字典和 nullable 显示列。重复键不在失败测试范围内，不要求任何去重、计数或诊断列。
- [ ] **Step 3: 运行失败测试。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_execution.py tests/test_dimension_value_mapping_validation.py`；预期仍调用 engine.query 或构建验证入口缺失。
- [ ] **Step 4: 接入 QUERY/EXPLAIN。** 计算引擎使用任务 3 的语义视图；非空映射列表执行 explain → compile_mapped_sql。EXPLAIN 返回生成的 SQL；QUERY 调用同一 SqlClient.query(sql, bind_parameters)，再 serialize_rows。未映射请求保留原路径。文本列空结果也显式标为 str，其余列和 _json_cell 的 Decimal 行为不变。
- [ ] **Step 5: 实现构建数据验证。** 使用 adapter.get_column_schema_from_query 读取加上 LIMIT 0 的原始业务维度投影和映射投影，检查真实类型，包括 expr、空表及全部显示 NULL。允许数值家族之间、文本家族之间及相同其他标量类型的等值比较，拒绝数值与文本、日期与数值等跨家族匹配；不把 JOIN 键统一 CAST 为文本。显示列必须是 adapter 判定的文本类型。用有界 `WHERE raw_value IS NULL LIMIT 1` 检查空键。数据库权限、连接失败沿用既有运行时处理，不伪装成配置错误。
- [ ] **Step 6: 调整构建顺序。** 在 execute_publication 完成物理验证并写入目录后调用 probe_query_capability，再设置验证标志和 capture。probe 先调用 validate_mapping_data，再执行既有代表性指标探测；无映射立即跳过新增验证。任何失败都不封存成功产物。复用 RuntimeExecutor._metricflow 的串行、租约和超时边界，不另开无保护的后台连接。
- [ ] **Step 7: 接通错误与验证。** 只捕获已识别的 ValueMappingError；runtime/service.py 返回区分错误码的结构化响应，execution/metricflow.py 和 RuntimeExecutor 通过 error_code/error_message 保留构建错误码及安全模型/维度说明。execute_publication 对目录生成阶段的 ValueMappingError 同样转换为 ExecutionError，不依赖引擎探测捕获。运行新测试及 `uv run --frozen pytest -q tests/test_metricflow_operations.py tests/test_publication_build.py tests/test_realtime_execution.py tests/test_synchronous_queries.py tests/test_execution_configuration_errors.py`；全部通过后提交 `feat: execute and validate sealed dimension mappings`。

### Task 7: 真实工具链回归与使用文档

**Files:** 新建 tests/integration/test_dimension_value_mapping.py；扩展 tests/dimension_mapping_helpers.py、README.md、tests/test_publication_available_dimensions.py、tests/test_publication_queries.py。

**Interfaces:** 仅消费前六任务及现有 dbtRunner、QueryService、Runtime。DuckDB 验收调用真实 dbt parse/build/docs 和 MetricFlow，不依赖服务 PostgreSQL；固定 buildId 的应用测试继续复用独立 PostgreSQL fixture 或已有 mock store。

- [ ] **Step 1: 添加真实工程测试。** 在 helper 的临时数据库建 source 字典表，通过包装 view 和 semantic model 读取；事实模型产生 502、404、200，另有渠道和复用状态字典的 target_status。用真实 dbtRunner 构建并写 published_catalog，再经 execute_available_dimensions/execute_programmatic 执行全部查询模式。
- [ ] **Step 2: 添加业务结果断言。** 验证多 JOIN、原始值筛选、原始值排序、指标排序、未命中、空值、相同标签、比率和 count_distinct。特意让 502、404 共享同一个用户，确认映射后不会把两个分组的去重计数重新合并。检查先筛选 502 再输出“失败”只得到该组；DIMENSION_VALUES 仍是数值类型的三个原始值。
- [ ] **Step 3: 添加构建与来源回归。** 配置变更后生成两个封存目录，证明固定 buildId 的映射引用保持独立；只更新 source 字典名称时后续查询读到新显示文本。测试展示模型内部维度不可用、同名业务字段仍可用、旧无映射目录正常查询，以及无效映射阻止成功 capture。
- [ ] **Step 4: 运行红绿循环。** `uv run --frozen pytest -q tests/integration/test_dimension_value_mapping.py`；先记录失败的具体未覆盖路径，仅补实现该路径所需最小改动，最终真实 DuckDB 验收全部通过。不能以增加 mock 代替真实 SQL 执行。
- [ ] **Step 5: 更新 README。** 加入 spec 的 YAML、已有 source 包装方式、N 维度对应 N 次 JOIN、原始值唯一由数据方保证、未命中回退、四种模式差异、显示文本列不能作为原始筛选值、配置固定但字典内容实时读取。记录准确验证命令及实际支持证据。
- [ ] **Step 6: 完成最终验证。** 执行下表，不重复运行已通过的检查，除非后续修改、失败或未解决问题要求重跑。提交 `test: verify dimension mappings through dbt and MetricFlow`，提交前使用 verification-before-completion 技能并检查最终 diff。

## 验证命令与通过条件

| 检查 | 命令 | 通过条件 |
| --- | --- | --- |
| 本功能单测 | `uv run --frozen pytest -q tests/test_dimension_value_mapping_contracts.py tests/test_dimension_value_mapping_catalog.py tests/test_dimension_value_mapping_dimensions.py tests/test_dimension_value_mapping_sql.py tests/test_dimension_value_mapping_execution.py tests/test_dimension_value_mapping_validation.py` | 全部通过，无数据库测试被替换为渲染断言 |
| DuckDB 真实链路 | `uv run --frozen pytest -q tests/integration/test_dimension_value_mapping.py` | dbt 和 MetricFlow 真实执行通过 |
| 全量回归 | `uv run --frozen pytest -q` | 使用 SERVICE_TEST_DATABASE_URL 指向独立测试库；全部可运行测试通过，列明环境缺失造成的 skip |
| 静态检查 | `uv run --frozen ruff check src tests` | 无错误 |
| 差异检查 | `git diff --check` | 无空白错误，改动仅覆盖本功能 |
| 既有 PostgreSQL 集成 | `uv run --frozen pytest -q tests/integration/test_publication_proxy_flow.py` | 已设置 PLATFORM_TEST_POSTGRES=1、PLATFORM_TEST_PGHOST/PGPORT/PGUSER/PGPASSWORD/PGDATABASE 和独立服务测试库；无环境时明确说明未验证 |

StarRocks 本期至少完成生成 SQL、adapter 引用和类型元数据接口的测试；只有实际连接 StarRocks 执行本功能后才能报告真实集成通过。不得把已有 DuckDB renderer 的使用当作 StarRocks 执行证明。

## Spec 覆盖与计划自检

| Spec 章节 | 实施任务 |
| --- | --- |
| 1–3：目标、范围、配置 | 1、4、5、7 |
| 4：校验与封存 | 1、2、6 |
| 5：可用维度与原始取值 | 3、4、6、7 |
| 6：多 JOIN、投影、排序、模式 | 4、5、6、7 |
| 7：唯一性前提与实时数据 | 2、5、7；明确不增加重复键检测 |
| 8：返回类型和失败行为 | 1、2、6 |
| 9–10：代码边界和验收 | 文件职责表及各任务验证 |

实施前评审本计划，并选择当前会话顺序实施或子代理逐项实施与评审；本次只交付计划。
