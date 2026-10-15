# Native dbt / MetricFlow Dimension Value Mapping Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修改 dbt、共享语义接口和 MetricFlow 源码，使维值映射成为正式维度协议，并由原生执行计划生成多个最外层 LEFT JOIN。

**Architecture:** dbt 将 Dimension.value_mapping 从 YAML 解析到两个原生 manifest，共享语义接口负责类型和引用校验。MetricFlow 在自己的 lookup、计算图和最终 SQL 转换中解析映射、隔离展示模型，并在指标 SQL 优化后追加 N 个 JOIN。服务继续调用 engine.query / engine.explain，只承担固定构建、原生配置封存和接口适配。

**Tech Stack:** 当前锁定的 dbt-core 1.12.5、dbt-metricflow 0.15.0、MetricFlow 0.213.0 及仓库内 metricflow_semantic_interfaces / metricflow_semantics；dbt dataclass、MSI Pydantic shim、MetricFlow SQL AST；Hatch、pytest、现有 adapters。

**Spec:** [修订后的设计](../specs/2026-10-15-dimension-value-mapping-design.md)。业务行为保持已确认版本；实现边界按用户最新要求改为修改 dbt / MetricFlow。

状态：修订版待评审；尚未修改产品代码。原先由服务包装 explain SQL 的计划已被本版本替代，不按旧计划创建服务 SQL 转换模块。

## Global Constraints

- 修改 vendor/dbt 和 vendor/metricflow 源码实现功能，不仅在服务层实现；这是本仓库 fork 的协议扩展，不声称上游已支持。
- value_mapping 是维度正式字段，与 type、expr 同级；不再使用 config.meta.value_mapping。三个键仍是 semantic_model、value_dimension、display_dimension。
- 映射表负责保证原始值唯一，每个原始值最多对应一条映射记录。服务直接依赖这一数据约束，不在构建或查询阶段额外检查重复键。
- N 个映射输出维度产生 N 个独立 LEFT JOIN；同一个映射模型被两个业务维度使用时也分别关联。
- 筛选、分组、排序和指标计算使用原始值。映射仅替换最终 SELECT 输出，不合并相同标签的结果行。
- 使用 LEFT JOIN。没有匹配记录或匹配记录的显示值为 NULL 时，回退为原始值的文本表示；原始 NULL 保持 NULL，空字符串是有效显示值。
- 映射后的结果列名和列顺序不变，列类型变为文本，不额外对外返回原始值列。
- DIMENSION_VALUES 返回原始值；PREVIEW 保持原行为；原生 query 和 explain 共用最终执行计划转换。
- 不新增重复键扫描、映射 GROUP BY/DISTINCT、计数或诊断列；不在内存中替换显示值。
- 服务不新增 valueMappings 请求参数，不另建计算 manifest 副本、映射黑名单或 SQL 执行旁路。
- 不增加第三方依赖、配置 CRUD 或持久化表，不重构无关代码。新增 vendor 代码兼容其最低 Python 3.10，服务遵循现有 Python 版本及完整类型标注要求。

## Review Focus

1. v1、v2 inline、derived dimension 和 partial parsing 的字段丢失：任务 2 验证新增、修改、删除及两类 manifest 往返。
2. 同名维度、多来源和经过展示模型的中间路径：任务 3 按实际来源绑定，在原生计算图隔离展示模型。
3. 同一字典复用多次、同标签和单个映射未命中：任务 4、5 验证独立 JOIN、独立回退和行数不变。
4. CTE、绑定参数、原始列排序、limit + 1 和 CTAS 终点：任务 4、5 验证转换位置和实际执行 SQL 一致。
5. 零行、全 NULL、带引号标识符及 expr：任务 4–6 验证方言、真实表达式、类型元数据和 Decimal 精度。

## 仓库规则与执行准备

- 实施前读取 vendor/dbt/AGENTS.md、vendor/metricflow/AGENTS.md，以及存在时的 vendor/metricflow/git_ignored/AGENTS.md；dbt 解析修改参考 docs/arch/3_Parsing.md、3.1_Partial_Parsing.md、3.3_Semantic_Models.md。
- dbt artifact 类型从 dbt.artifacts.resources 导入，不在 artifacts 模块之外直接导入 v1 路径；dbt dataclass 与 MSI Pydantic 必须满足同一个 Protocol。
- MetricFlow 遵循其类型注解、日志、不可变类型和 visitor 风格；不新引入 getattr/isinstance 分派，使用现有访问器和显式类型。
- 检查根仓库与两个 submodule 状态并创建合适的隔离工作区，不覆盖已有更改；检查 detached HEAD，按需创建 codex/ 前缀功能分支。
- submodule 分别提交，根仓库最后固定 gitlink。dbt 要求 GPG 签名提交：先核实签名能力；不可用时保留工作并报告，不能静默绕过或代替用户生成、上传密钥。
- MSI 契约先实现，dbt 测试环境通过 Hatch 本地开发依赖使用修改后的 MetricFlow 包；检查实际导入对象含新字段，不能用已发布旧包验证新协议。
- 根项目已有 uv 本地源码依赖。服务验收前执行 `uv sync --frozen --reinstall-package dbt-core --reinstall-package metricflow --reinstall-package dbt-metricflow`，不直接修改 site-packages。

## 已核实的扩展点与文件职责

| 扩展点 | 当前代码与本次职责 |
| --- | --- |
| dbt YAML 合约 | core/dbt/contracts/graph/unparsed.py：UnparsedDimensionBase、UnparsedDimensionV2、UnparsedDerivedDimensionV2 |
| dbt artifact | core/dbt/artifacts/resources/v1/semantic_model.py 的 Dimension；components.py 的 ColumnDimension |
| dbt 解析 | core/dbt/parser/schema_yaml_readers.py：_get_dimensions、_parse_v2_column_dimensions、_parse_v2_derived_dimensions |
| dbt 产物 | core/dbt/contracts/graph/semantic_manifest.py：semantic_model.to_dict() → PydanticSemanticModel.parse_obj |
| 共享协议 | metricflow_semantic_interfaces/protocols/dimension.py、implementations/elements/dimension.py、validations/semantic_manifest_validator.py |
| 来源与计算图 | metricflow_semantics/model/semantic_manifest_lookup.py、semantic_graph；DimensionInstance.defined_from |
| 物理投影 | metricflow/dataset/convert_semantic_model.py：SemanticModelToDataSetConverter.create_sql_source_data_set |
| 最终 SQL 边界 | metricflow/execution/dataflow_to_execution.py：_convert_to_sql_plan → _render_sql → 查询任务 |
| 优化边界 | metricflow/plan_conversion/to_sql_plan/dataflow_to_sql.py：优化完成后返回 ConvertToSqlPlanResult |
| query/explain 共用路径 | metricflow/engine/metricflow_engine.py：_create_execution_plan |

下文 Files 路径相对各任务标明的仓库目录。新增类型和函数是计划实施的接口，不表示当前源码已有。

---

### Task 1: 共享维度协议与引用校验

**Repository:** vendor/metricflow。

**Files:** 修改 metricflow_semantic_interfaces/protocols/dimension.py、implementations/elements/dimension.py、validations/semantic_manifest_validator.py；新建 validations/dimension_value_mapping.py、tests_metricflow_semantic_interfaces/validations/test_dimension_value_mapping.py、tests_metricflow_semantic_interfaces/implementations/test_dimension_value_mapping.py。

**Interfaces:**

- DimensionValueMapping Protocol：semantic_model、value_dimension、display_dimension 三个 str 属性。
- PydanticDimensionValueMapping(HashableBaseModel)：三个必填非空字符串，extra=forbid，使用现有 msi_pydantic_shim。
- Dimension.value_mapping 为 Optional[DimensionValueMapping]；PydanticDimension 对应实现字段默认 None。
- DimensionValueMappingRule 实现现有 SemanticManifestValidationRule，注册到 DEFAULT_RULES。

- [ ] **Step 1: 写失败测试。** 覆盖缺键、未知键、旧 manifest 缺字段、JSON 往返；模型不存在、目标维度归属错误、非 categorical、递归映射、目标作为指标计算来源均报错。映射模型不含 metrics/measures 且没有业务 entity JOIN 路径时仍合法。

```python
assert dimension.value_mapping.semantic_model == "status_code_mapping"
assert dimension.value_mapping.value_dimension == "raw_value"
assert restored.value_mapping == dimension.value_mapping
assert legacy_dimension.value_mapping is None
assert validation_result.has_blocking_issues is True
```

- [ ] **Step 2: 运行红测试。** `hatch run dev-env:pytest tests_metricflow_semantic_interfaces/validations/test_dimension_value_mapping.py tests_metricflow_semantic_interfaces/implementations/test_dimension_value_mapping.py`，确认失败来自新能力缺失。
- [ ] **Step 3: 实现类型和规则。** 按模型与维度身份解析引用，错误包含源模型、维度和配置字段；沿用 ValidationIssue，不连接数据库或检查重复键。
- [ ] **Step 4: 验证并提交。** 重跑新测试及 `hatch run dev-env:pytest tests_metricflow_semantic_interfaces/validations/test_dimension_const.py`；提交 `feat: define native dimension value mapping protocol`。

### Task 2: dbt 正式字段、解析、产物和增量解析

**Repository:** vendor/dbt，环境使用任务 1 的本地语义接口。

**Files:** 修改 core/dbt/contracts/graph/unparsed.py、core/dbt/artifacts/resources/v1/components.py、semantic_model.py、core/dbt/artifacts/resources/__init__.py、core/dbt/parser/schema_yaml_readers.py、core/dbt/contracts/graph/semantic_manifest.py；按回归证据修改 nodes.py、parser/partial.py。扩展 tests/unit/test_semantic_layer_nodes_satisfy_protocols.py、tests/unit/parser/test_v2_column_semantic_parsing.py、tests/unit/contracts/graph/test_semantic_manifest.py；新建 tests/functional/semantic_models/test_dimension_value_mapping.py。

**Interfaces:**

- dbt dataclass DimensionValueMapping 定义在 artifacts/resources/v1/components.py，继承 dbtClassMixin，从 artifacts.resources 导出，满足任务 1 的协议；同名 MSI 类型通过 import alias 区分。
- UnparsedDimensionBase、ColumnDimension、artifact Dimension 增加 value_mapping: Optional[DimensionValueMapping] = None。
- 解析函数签名保持不变，显式传递该字段；v2 列别名和 derived expr 规则不变。

- [ ] **Step 1: 写失败测试。** 三种 YAML 格式都用正式字段；两个 manifest 配置相同，dataclass schema 与 Protocol 匹配，旧产物和缓存缺字段可读。

```python
assert manifest_dimension["value_mapping"] == semantic_dimension["value_mapping"]
assert semantic_dimension["value_mapping"]["display_dimension"] == "display_value"
assert changed_node.same_contents(old_node) is False
assert reparsed_dimension.value_mapping is None  # partial parse 删除后
```

- [ ] **Step 2: 运行红测试。** 在 vendor/dbt/core 执行 `hatch run python3 -m pytest ../tests/unit/test_semantic_layer_nodes_satisfy_protocols.py ../tests/unit/parser/test_v2_column_semantic_parsing.py ../tests/unit/contracts/graph/test_semantic_manifest.py`。
- [ ] **Step 3: 贯通字段。** Unparsed → ColumnDimension/Dimension → manifest → MSI Pydantic；三个键严格校验，不藏回 meta。保留派生 expr 和列重命名表达式，缺字段默认 None。
- [ ] **Step 4: 验证增量变化。** 独立 functional test class 分别测试新增、修改、删除映射以及目标改名/删除；第二次 parse 使用真实 partial_parse.msgpack。same_dimensions 的 dataclass 比较若已覆盖字段，不做无关重写。引用校验使用当前完整 manifest，不依赖解析顺序。
- [ ] **Step 5: 验证并提交。** 重跑单测，在独立 PostgreSQL 上运行 `hatch run python3 -m pytest ../tests/functional/semantic_models/test_dimension_value_mapping.py`；验证新增字段的本地生成 schema，不擅改上游 schema URL 宣称官方兼容。按 GPG 签名规则提交 `feat: parse dimension value mappings into semantic manifests`。

### Task 3: MetricFlow 原生 lookup 与展示模型隔离

**Repository:** vendor/metricflow。

**Files:** 新建 metricflow_semantics/model/semantics/value_mapping_lookup.py、tests_metricflow_semantics/model/test_value_mapping_lookup.py、tests_metricflow/engine/test_value_mapping_dimensions.py；修改 metricflow_semantics/model/semantic_manifest_lookup.py、semantic_graph/builder/graph_builder.py、metricflow_semantics/errors/error_classes.py、metricflow/engine/metricflow_engine.py、metricflow/engine/models.py。

**Interfaces:**

- ResolvedValueMapping frozen dataclass：source_dimension: SemanticModelElementReference、target_model: SemanticModelReference、value_dimension/display_dimension: DimensionReference。
- `ValueMappingLookup(semantic_manifest: SemanticManifest)`；`get(source: SemanticModelElementReference) -> Optional[ResolvedValueMapping]`；mapping_model_references: frozenset[SemanticModelReference]。
- SemanticManifestLookup.value_mapping_lookup 暴露新 lookup；完整 manifest/model lookup 保留展示模型供投影使用。
- engine 的 Dimension 表示保留 semantic_model_reference，并传递正式 value_mapping 属性。
- DimensionValueMappingError 继承现有 UnableToSatisfyQueryError，沿用 `(error_name: str, context: Optional[Dict[str, str]] = None)`；error_name 区分来源不明确、键类型不兼容、显示类型无效和空键。context 只包含安全的模型、维度和字段身份；任务 6、7 复用此类型，连接和权限异常不转换为此类型。

- [ ] **Step 1: 写失败测试。** 两业务模型含同名 status_code 且使用不同字典；同一字典复用；展示模型 entity 碰巧可关联；普通业务模型也含 raw_value。

```python
assert lookup.get(orders_status).target_model != lookup.get(returns_status).target_model
assert mapping_model in full_manifest_model_names
assert mapping_model not in metric_compute_source_names
assert business_raw_value in available_dimensions
assert mapping_raw_value not in available_dimensions
```

- [ ] **Step 2: 运行红测试。** `hatch run dev-env:pytest tests_metricflow_semantics/model/test_value_mapping_lookup.py tests_metricflow/engine/test_value_mapping_dimensions.py`。
- [ ] **Step 3: 实现隔离和来源绑定。** 目标集合限制计算语义图及 engine 计算 source datasets；完整 lookup 仍能解析映射物理模型。原生可用维度不暴露展示字段，计算图不能通过展示模型绕行。依据 defined_from 和真实 source reference 定位配置，多来源无法唯一判断时报错，不取首个同名候选。
- [ ] **Step 4: 验证并提交。** 重跑新测试及 `hatch run dev-env:pytest tests_metricflow/engine/test_errors.py tests_metricflow/engine/test_explain.py`；无映射图和发现行为不变；提交 `feat: resolve display mappings in MetricFlow semantics`。

### Task 4: 原生最终 SQL 转换及方言表达式

**Repository:** vendor/metricflow。

**Files:** 新建 metricflow/plan_conversion/to_sql_plan/dimension_value_mapping.py、tests_metricflow/plan_conversion/test_dimension_value_mapping.py；修改 metricflow/plan_conversion/convert_to_sql_plan.py、metricflow_semantics/sql/sql_exprs.py、metricflow/sql/render/expr_renderer.py 及需要实现新 visitor 的原生表达式访问器/方言 renderer；扩展 tests_metricflow/sql/test_sql_plan_render.py、test_engine_specific_rendering.py。

**Interfaces:**

- OutputValueMapping frozen dataclass：source_column_name: str、mapping_select: SqlSelectStatementNode、value_column_name: str、display_column_name: str。
- `apply_dimension_value_mappings(result: ConvertToSqlPlanResult, mappings: tuple[OutputValueMapping, ...], *, order_bys: tuple[SqlOrderByDescription, ...]) -> ConvertToSqlPlanResult`，纯转换且不修改输入。
- ConvertToSqlPlanResult.display_columns: tuple[str, ...] = ()，标识最终文本列；instance_set 用于原始输出语义，转换后不再参与下游指标计算。
- 原生 `SqlCastToStringExpression.create(arg: SqlExpressionNode) -> SqlCastToStringExpression` 及 visitor/render 支持，不用服务 SQL 包装替代。

- [ ] **Step 1: 写失败测试。** 原生计划覆盖 0/1/2/3 个映射、同目标复用、expr、physical alias、带引号标识符；DuckDB 执行映射 SQL，包含 Review Focus 的 CTE、参数和排序。

```python
assert len(outer.join_descs) == 3
assert len({join.right_source_alias for join in outer.join_descs}) == 3
assert actual_rows == [("成功", 100), ("失败", 20), ("失败", 10)]
assert converted.display_columns == ("request__status_code",)
assert original_result.sql_plan is original_plan
```

三 JOIN 与单状态码断言分属不同测试；outer 从转换后的原生计划取得，不新增仅供测试的生产接口。

- [ ] **Step 2: 运行红测试。** `hatch run dev-env:pytest tests_metricflow/plan_conversion/test_dimension_value_mapping.py`。
- [ ] **Step 3: 复用源模型投影。** 用 SemanticModelToDataSetConverter.create_sql_source_data_set 解析目标，按 dimension instance 取列并投影为匹配值/显示值。保留 expr、node_relation 和引用规则，不能假设维度名等于物理列。
- [ ] **Step 4: 构建外层 SELECT。** 以原始 SELECT 为 q，按最终输出列顺序生成不同别名。每个 ON 只用 q 的对应原始值；SELECT 使用原生 COALESCE/文本 CAST，其他列顺序和别名不变。同目标不去重，无映射直接返回原结果。
- [ ] **Step 5: 保持优化边界。** 内层 CTE、筛选、排序和 limit 不变；外层显式按原始列恢复顺序；绑定参数由 renderer 传递。根为 SqlCreateTableAsNode 时转换 parent_node 的 SELECT 并保留原 sql_table，不能把 CTAS 放进 FROM。转换后不运行会下推展示 JOIN 的优化。
- [ ] **Step 6: 方言、快照和提交。** 测试 DuckDB/PostgreSQL 及已有方言的文本 CAST 和引用规则；运行新测试及 `hatch run dev-env:pytest tests_metricflow/sql/test_sql_plan_render.py tests_metricflow/sql/test_engine_specific_rendering.py`。快照使用 --overwrite-snapshots 生成并审阅，不手改；提交 `feat: add final SQL projection for dimension value mappings`。

### Task 5: 接入原生 query / explain / 执行任务

**Repository:** vendor/metricflow。

**Files:** 修改 metricflow/engine/metricflow_engine.py、metricflow/execution/dataflow_to_execution.py、execution_plan.py；新建 tests_metricflow/engine/test_dimension_value_mapping.py；扩展 tests_metricflow/plan_conversion/test_dataflow_to_execution.py。

**Interfaces:**

- DataflowToExecutionPlanConverter 增加内部 lookup 依赖与 apply_value_mappings: bool = False，engine 根据现有 MetricFlowQueryType 设置，不增加公开 HTTP 覆盖参数。
- _convert_to_sql_plan 在原转换及优化完成后，按任务 3 的 lookup、输出 instance_set 和关联列建立任务 4 的 OutputValueMapping；_render_sql 及查询任务消费转换结果。
- SelectSqlQueryToDataTableTask.create 增加 display_columns: tuple[str, ...] = ()，只保持文本类型元数据，不修改单元格。

- [ ] **Step 1: 写失败测试。** 直接构造 MetricFlowEngine 调用 query/explain，禁止导入服务。普通指标查询多 JOIN，DIMENSION_VALUES 跳过，只有 filter 使用映射维度时不加 JOIN。

```python
assert query_result.sql == explain_result.sql_statement.sql
assert query_result.result_df.rows == (("成功", 100), ("失败", 20), ("失败", 10))
assert raw_dimension_values == (200, 404, 502)
assert empty_mapped_table.column_descriptions[0].column_type is str
```

- [ ] **Step 2: 运行红测试。** `hatch run dev-env:pytest tests_metricflow/engine/test_dimension_value_mapping.py tests_metricflow/plan_conversion/test_dataflow_to_execution.py`。
- [ ] **Step 3: 接入共用执行计划。** 保持 query/explain 共用 _create_execution_plan，在创建任务前完成映射；数据表和写结果表两个终点都覆盖。不能先执行原 SQL 再补映射，不能两条路径各自改写。
- [ ] **Step 4: 保持类型与取数语义。** 零行/全 NULL 使用 display_columns 修正 ColumnDescription 为 str，不改写值；Decimal 和未映射列保持原状。测试混合排序、无排序、limit=2 的额外一行、参数过滤；客户端的原有参数支持边界不变。
- [ ] **Step 5: 验证并提交。** 新测试及既有 engine 测试通过，无映射 SQL 快照不应无故变化；提交 `feat: execute native dimension mapping plans`。

### Task 6: dbt-metricflow 原生构建期数据校验

**Repository:** vendor/metricflow/dbt-metricflow；提交属于 vendor/metricflow。

**Files:** 新建 dbt_metricflow/cli/dbt_connectors/value_mapping_validation.py、tests_dbt_metricflow/test_value_mapping_validation.py；复用 cli_configuration.py、adapter_backed_client.py，仅为公开验证入口做必要接入。

**Interfaces:** `validate_value_mapping_data(semantic_manifest_lookup: SemanticManifestLookup, adapter: BaseAdapter) -> None`；复用任务 3 的引用及原生投影，不重新解释 JSON。使用任务 3 的 DimensionValueMappingError，含安全模型/维度身份，不包裹连接故障为配置错误。

- [ ] **Step 1: 写失败测试。** 临时 DuckDB 覆盖空字典、NULL 原始键、兼容与不兼容键类型、非文本显示列、全 NULL 文本显示列、expr 和不存在的列。
- [ ] **Step 2: 运行红测试。** `hatch run dev-env:pytest tests_dbt_metricflow/test_value_mapping_validation.py`。
- [ ] **Step 3: 实现 adapter 验证。** 原生投影加 LIMIT 0，用 get_column_schema_from_query 读取真实类型；数值家族、文本家族和相同其他标量类型可匹配，禁止统一字符串 CAST 掩盖类型差异。显示列必须为文本；用 WHERE raw_value IS NULL LIMIT 1 检查空键。空字典、显示 NULL 合法，不新增 COUNT/DISTINCT/GROUP BY/unique 检查。
- [ ] **Step 4: 验证并提交。** 分别验证 PostgreSQL、DuckDB、服务使用的 StarRocks adapter 类型接口，区分替身与实测；保留连接/权限故障分类。测试通过后提交 `feat: validate dimension mapping data through dbt adapters`。

### Task 7: 服务适配原生协议和固定构建

**Repository:** 根仓库。

**Files:** 修改 src/dbt_metricflow_service/models/native.py、models/artifacts.py、platform/models.py、platform/catalog.py、platform/sealed_catalog.py、platform/dimensions.py、platform/queries.py、platform/metricflow.py、platform/build.py；按错误接入需要修改 application/queries.py、runtime/service.py、runtime/executor.py、execution/metricflow.py、constants.py。新建 tests/test_dimension_value_mapping_contracts.py、tests/test_dimension_value_mapping_catalog.py，扩展 tests/test_query_plan.py、test_metricflow_operations.py、test_publication_build.py、test_publication_catalog.py。

**Interfaces:**

- NativeNode 从正式 value_mapping 读取 MSI 原生类型，Published DimensionAttributes 增加可选配置投影，旧目录缺字段按无映射读取。
- NativeDimension 保留原生 semantic_model_name，map_available_dimensions 按实际归属匹配 resourceId。
- QueryRequest/PlatformQueryRequest 不增加映射列表；engine.query/explain 继续作为指标入口。
- probe_query_capability 调用任务 6 的原生验证入口；不增加服务 value_mapping_sql.py、配置解析器或副本 manifest。

- [ ] **Step 1: 写失败测试。** 正式字段封存不丢失、旧目录可读、同名维度不串用、原生显示结果原样序列化；请求不含 valueMappings，服务不直接执行改写 SQL。

```python
assert published_dimension.attributes.value_mapping.semantic_model == "status_code_mapping"
assert legacy_dimension.attributes.value_mapping is None
assert engine_query_calls == [expected_native_request]
assert service_result.rows == native_serialized_rows
assert "valueMappings" not in public_request_schema["properties"]
```

- [ ] **Step 2: 运行红测试。** `uv run --frozen pytest -q tests/test_dimension_value_mapping_contracts.py tests/test_dimension_value_mapping_catalog.py tests/test_metricflow_operations.py`。
- [ ] **Step 3: 实现封存与接入。** 两个原生 manifest 配置一致，使用既有固定构建和物理关系绑定；只在目录记录展示依赖，不污染物理 depends_on 造成多来源。构建探测调用原生验证，失败不 capture 成功产物；引擎不依赖 published_catalog 识别映射。
- [ ] **Step 4: 保持错误和类型边界。** 将已识别原生映射错误转为现有 error 协议，保留安全字段；数据库故障沿用原分类。直接复用原生文本列类型，零行正确、Decimal 序列化不变，不在服务修复显示值。
- [ ] **Step 5: 验证并提交。** 重跑新测试及 `uv run --frozen pytest -q tests/test_query_plan.py tests/test_publication_build.py tests/test_publication_catalog.py tests/test_synchronous_queries.py`。提交时固定依赖的已提交 vendor gitlink，确保 checkout 后可重现。

### Task 8: 跨仓库端到端、文档与发布引用

**Repositories:** 根仓库及两个 vendor 仓库。

**Files:** 新建根 tests/integration/test_dimension_value_mapping.py、tests/dimension_mapping_helpers.py；修改根 README.md、vendor/dbt/docs/arch/3.3_Semantic_Models.md、vendor/metricflow/README.md；按 vendor 各自规则新增功能 changelog；根仓库固定 gitlink。

**Interfaces:** 不增加生产接口；组合任务 1–7 的正式 YAML 字段、原生 query/explain 和现有服务入口验证完整链路。

- [ ] **Step 1: 建立真实工程。** 临时 source 字典表、包装 view、映射 semantic model、事实模型，贯通 dbt parse/build/docs → semantic_manifest → 原生 query/explain → 服务。
- [ ] **Step 2: 验证结果。** 状态、渠道、target_status 三维度，其中两个复用字典；覆盖 502/404 同为“失败”但保留两行、原始过滤、混合排序、未匹配、NULL、空字符串、比率和共享用户的 count_distinct。
- [ ] **Step 3: 验证生命周期。** partial parse 修改/删除不残留，两个 buildId 使用各自配置，更新字典文本后读取新数据，旧无映射工程不变。原生测试不得导入服务包，证明能力位于引擎内部。
- [ ] **Step 4: 运行真实链路。** `uv run --frozen pytest -q tests/integration/test_dimension_value_mapping.py`；先记录缺失路径的失败，补最小实现后达到真实 DuckDB 全部通过。PostgreSQL 用独立测试库，StarRocks 区分生成和真实执行证据。
- [ ] **Step 5: 更新文档与 changelog。** 使用正式字段及 v1/v2 示例，说明 fork、模式差异、唯一性、多 JOIN、配套版本。移除根 README 中与本次修改 vendor 授权冲突的旧描述。dbt 用 changie 记录功能，不手改生成的 CHANGELOG.md。
- [ ] **Step 6: 完成下表检查。** make lint 可能格式化代码，审阅并撤回本次产生的无关格式变更；快照用工具生成，只报告实际通过项。
- [ ] **Step 7: 固定交付。** 先提交 vendor（dbt 遵守签名），再提交根服务、文档和 gitlink；未请求发布不推送。使用 verification-before-completion 技能检查最终差异及提交。

## 验证命令

| 目录 | 命令 | 通过证据 |
| --- | --- | --- |
| vendor/dbt/core | `hatch run unit-tests` | dbt 单测含协议和序列化 |
| vendor/dbt/core | `hatch run python3 -m pytest ../tests/functional/semantic_models/test_dimension_value_mapping.py` | 独立 PostgreSQL 上 full/partial parse |
| vendor/dbt/core | `hatch run code-quality` | dbt 风格、类型检查 |
| vendor/metricflow | `hatch run dev-env:pytest tests_metricflow_semantic_interfaces/validations/test_dimension_value_mapping.py tests_metricflow_semantic_interfaces/implementations/test_dimension_value_mapping.py tests_metricflow_semantics/model/test_value_mapping_lookup.py tests_metricflow/engine/test_value_mapping_dimensions.py tests_metricflow/plan_conversion/test_dimension_value_mapping.py tests_metricflow/engine/test_dimension_value_mapping.py` | 原生协议、图隔离及查询语义 |
| vendor/metricflow/dbt-metricflow | `hatch run dev-env:pytest tests_dbt_metricflow/test_value_mapping_validation.py` | 原生 adapter 校验 |
| vendor/metricflow | `make lint`、`make test` | 仓库规定的最终全量验证 |
| 根仓库 | `uv run --frozen pytest -q tests/integration/test_dimension_value_mapping.py` | DuckDB 全链路 |
| 根仓库 | `uv run --frozen pytest -q`、`uv run --frozen ruff check src tests` | 服务回归；数据库用独立 SERVICE_TEST_DATABASE_URL |
| 三个仓库 | `git diff --check` | 无空白错误和无关改动 |

缺少 Hatch/make、依赖、数据库或签名能力时准确列明未执行项及原因；不能用服务单测代替原生测试，也不能降低要求后宣称完成。

## 覆盖检查与顺序

| Spec 要求 | 任务 |
| --- | --- |
| 正式协议、旧产物兼容、固定构建 | 1、2、7、8 |
| 引用错误、空键和类型 | 1、3、6、7 |
| 原生维度发现、实际来源与图隔离 | 3、7 |
| 多 JOIN、expr、排序及方言 | 4、5 |
| query/explain 同一 SQL、CTAS、类型和查询模式 | 5、7、8 |
| 字典保证唯一、无重复检测、实时内容 | 4、6、8 |
| 不依赖服务即可使用 | 1–6 的原生测试、8 |

按 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 顺序执行；跨仓库协议先稳定再接入，不并行修改同一接口。修订版评审后再实施，本次仅修订 spec 和计划。
