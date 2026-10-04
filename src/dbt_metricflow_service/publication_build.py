"""候选构建的受控物理映射。"""

import asyncio
import hashlib
import json
from pathlib import Path
from uuid import UUID

import sqlglot
from sqlglot import exp

from .platform_namespace import TABLE_NAME_PATTERN, prepare_versioned_project, validate_schema_name
from .publication_models import BindingMode, BuildMode, RelationBinding
from .publication_template import validate_templates

UTF8 = "utf-8"
MANIFEST_FILE = "manifest.json"
MACROS_DIRECTORY = "macros"
ALIAS_FILE = "generate_alias_name.sql"
SCHEMA_FILE = "generate_schema_name.sql"
MODEL = "model"
EPHEMERAL = "ephemeral"
OPERATION = "operation"
HOOK_KEYS = ("pre-hook", "post-hook", "pre_hook", "post_hook")
SAFE_MATERIALIZATIONS = frozenset({"table", "view", EPHEMERAL})
STATE_FILE = "publication_state.json"
EVIDENCE_FILE = "publication_evidence.json"
TARGET = "target"
SEMANTIC_FILE = "semantic_manifest.json"
CATALOG_FILE = "catalog.json"
RESULTS_FILE = "run_results.json"
DBT = "dbt"
PARSE = "parse"
DEPS = "deps"
BUILD = "build"
TEST = "test"
COMPILE = "compile"
SOURCE = "source"
FRESHNESS = "freshness"
FRESHNESS_THRESHOLDS = ("warn_after", "error_after")
COUNT = "count"
PERIOD = "period"
SOURCES_FILE = "sources.json"
STORE_FAILURES = "store_failures"
STORE_FAILURES_AS = "store_failures_as"
DOCS = "docs"
GENERATE = "generate"
NO_PARTIAL_PARSE = "--no-partial-parse"
SELECT = "--select"
TEST_SELECTOR = "resource_type:test"
BUILDING = "BUILDING"
EXECUTION = "EXECUTION"
PASSED = "PASSED"
NOT_REQUIRED = "NOT_REQUIRED"
ROOT_CONFIGS = ("dbt_project.yml", "packages.yml", "dependencies.yml", "package-lock.yml")
PHYSICAL_FIELDS = ("raw_code", "config", "unrendered_config", "depends_on", "contract", "constraints", "resource_type")
SOURCE_FIELDS = ("database", "schema", "identifier", "quoting", "freshness", "loaded_at_field",
                 "loaded_at_query", "config")
MAPPING_TEMPLATE = """{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- set bindings = BINDINGS -%}
    {%- if node.unique_id in bindings -%}
        {{ return(bindings[node.unique_id]['identifier']) }}
    {%- else -%}
        {{ return('PREFIX' ~ (custom_alias_name | trim if custom_alias_name else node.name)) }}
    {%- endif -%}
{%- endmacro %}
"""
# dbt 测试默认配置 dbt_test__audit，即使 store_failures=false 也会传入此值。
# 测试统一解析到受控 schema；validate_bound_manifest 会在执行前拒绝写入失败结果表。
SCHEMA_TEMPLATE = """{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- set bindings = BINDINGS -%}
    {%- if node.unique_id in bindings -%}
        {{ return(bindings[node.unique_id]['schema']) }}
    {%- elif node.resource_type == 'test' -%}
        {{ return(target.schema) }}
    {%- elif custom_schema_name is not none -%}
        {{ exceptions.raise_compiler_error('Custom schema is not supported for publication') }}
    {%- else -%}
        {{ return(target.schema) }}
    {%- endif -%}
{%- endmacro %}
"""


def apply_relation_bindings(project: Path, run_id: UUID, schema: str, bindings: list) -> str:
    # 宏只能写入任务副本；复用映射由已发布记录生成，不能来自客户端。
    prefix = prepare_versioned_project(project, run_id, schema)
    mapping = {}
    for raw in bindings:
        binding = RelationBinding.model_validate(raw)
        validate_schema_name(binding.relation.schema_name)
        if not TABLE_NAME_PATTERN.fullmatch(binding.relation.identifier):
            raise ValueError("复用物理标识符无效")
        mapping[binding.native_id] = {"schema": binding.relation.schema_name, "identifier": binding.relation.identifier}
    macros = project / MACROS_DIRECTORY
    if (macros / SCHEMA_FILE).exists():
        raise ValueError("项目已有受控 schema 宏")
    encoded = json.dumps(mapping, sort_keys=True)
    alias_macro = MAPPING_TEMPLATE.replace("BINDINGS", encoded).replace("PREFIX", prefix)
    (macros / ALIAS_FILE).write_text(alias_macro, encoding=UTF8)
    (macros / SCHEMA_FILE).write_text(SCHEMA_TEMPLATE.replace("BINDINGS", encoded), encoding=UTF8)
    return prefix


def validate_bound_manifest(target: Path, schema: str, prefix: str, bindings: list) -> None:
    # 最终解析必须逐节点吻合实际映射；复用节点不能落入候选的新前缀。
    manifest = json.loads((target / MANIFEST_FILE).read_text(encoding=UTF8))
    reused = {item.native_id: item for item in map(RelationBinding.model_validate, bindings)}
    observed = set()
    for native_id, node in manifest["nodes"].items():
        validate_execution_policy(node)
        config = node.get("config") or {}
        if node.get("resource_type") != MODEL:
            continue
        if config.get("materialized") == EPHEMERAL:
            continue
        actual = (node.get("database"), node.get("schema"), node.get("alias"))
        if native_id in reused:
            relation = reused[native_id].relation
            if actual != (relation.database, relation.schema_name, relation.identifier):
                raise ValueError("复用关系与最终 manifest 不一致")
        elif (actual[1] != schema or not isinstance(actual[2], str) or not actual[2].startswith(prefix)
              or not TABLE_NAME_PATTERN.fullmatch(actual[2])):
            raise ValueError("新建关系不属于候选")
        relation_parts = [part.strip('`" ') for part in node.get("relation_name", "").split(".")]
        if relation_parts[-2:] != list(actual[1:]) or actual in observed:
            raise ValueError("物理关系重复或与节点标识不一致")
        observed.add(actual)
    if not reused.keys() <= manifest["nodes"].keys():
        raise ValueError("复用绑定包含不属于当前模型的节点")


def validate_execution_policy(node: dict) -> None:
    # 草稿与发布共用定义边界；此检查不连接数据库、不执行 hook。
    config = node.get("config") or {}
    if node.get("resource_type") == OPERATION or any(config.get(key) for key in HOOK_KEYS):
        raise ValueError("发布构建禁止写入钩子")
    if config.get("sql_header"):
        raise ValueError("发布构建禁止执行 SQL header")
    if node.get("resource_type") not in (MODEL, TEST):
        raise ValueError("发布暂不支持 seed、snapshot 或自定义执行节点")
    if node.get("resource_type") == TEST and (
            config.get(STORE_FAILURES) or config.get(STORE_FAILURES_AS) not in (None, EPHEMERAL)):
        raise ValueError("发布测试禁止写入失败结果表")
    if node.get("resource_type") == MODEL and config.get("materialized") not in SAFE_MATERIALIZATIONS:
        raise ValueError("发布暂不支持此物化策略")


def validate_readonly_sql(sql: str, dialect: str) -> None:
    # 编译后的模型只能是单条取数语句，阻止宏输出多语句或带写入的 CTE。
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except sqlglot.errors.ParseError as error:
        raise ValueError("发布 SQL 无法证明为只读查询") from error
    if (len(statements) != 1 or not isinstance(statements[0], exp.Query)
            or any(isinstance(node, (exp.DDL, exp.DML, exp.Command, exp.Into)) for node in statements[0].walk())):
        raise ValueError("发布 SQL 必须为单条只读查询")


def requires_source_freshness(node: dict) -> bool:
    """只对有效阈值要求 freshness 证明，兼容 dbt 为未配置阈值生成的空对象。"""
    freshness = node.get(FRESHNESS) or {}
    # count=0 是合法阈值，因此不能使用布尔判断；count/period 都存在才构成可执行规则。
    return any(threshold.get(COUNT) is not None and threshold.get(PERIOD) is not None
               for key in FRESHNESS_THRESHOLDS if (threshold := freshness.get(key)))


async def execute_publication(executor, job, runner, attempt: Path, project: Path):
    """逻辑解析、选择性构建和完整验证使用同一个受租约控制的命令执行器。"""
    from .models import CommandSpec
    from .platform_catalog import catalog_from_artifacts
    from .publication_catalog import write_publication_catalog
    from .publication_plan import plan_publication, validate_publication_evidence
    from .runtime_execution import ExecutionResult, build_programmatic_command
    from .storage.publications import PublicationStore

    target = project / TARGET
    request = job["request_json"]
    settings = executor.settings
    base = build_programmatic_command(project, settings.profiles_dir, job["schema_name"],
                                      job["profile_binding_id"], attempt / "input.json", attempt / "output.json")
    common = ("--project-dir", str(project), "--profiles-dir", str(settings.profiles_dir),
              "--target", job["profile_binding_id"], "--target-path", str(target))

    async def command(*args):
        # deps 不支持 target-path，沿用既有执行器的命令参数边界。
        options = common[:-2] if args[0] == DEPS else common
        spec = CommandSpec((DBT, *args, *options), project, base.environment, True)
        from .branch_validation_summary import failure_summary
        from .runtime_execution import ExecutionError

        try:
            await executor._command(job, runner, spec, BUILDING)
        except ExecutionError as error:
            # 摘要与当前租约失败事务一起保存；不复制 stderr、SQL 或数据库异常。
            summary = failure_summary(target, args[0], settings.max_artifact_file_bytes)
            raise ExecutionError(error.code, {"validationSummary": summary}, stopped=error.stopped) from error

    # 依赖安装后先做无版本前缀的逻辑解析，保存比较输入而非整个易变 manifest。
    await asyncio.to_thread(validate_templates, project)
    if any((project / name).exists() for name in ROOT_CONFIGS[1:3]):
        await command(DEPS)
        await asyncio.to_thread(validate_templates, project)
    await command(PARSE, NO_PARTIAL_PARSE)
    logical = json.loads((target / MANIFEST_FILE).read_text(encoding=UTF8))
    context = {"configVersion": job["config_version"], "toolchainVersion": job["toolchain_version"],
               "binding": request["binding"], "projectConfig": {
                   name: hashlib.sha256((project / name).read_bytes()).hexdigest()
                   for name in ROOT_CONFIGS if (project / name).exists()}}
    current = {
        "nodes": {key: {field: node.get(field) for field in PHYSICAL_FIELDS}
                  for key, node in logical["nodes"].items() if node.get("resource_type") == MODEL},
        "macros": {key: node.get("macro_sql") for key, node in logical.get("macros", {}).items()},
        "sources": {key: {field: node.get(field) for field in SOURCE_FIELDS}
                    for key, node in logical.get("sources", {}).items()}, "context": context,
    }
    # 环境变量的实际值不持久化；使用它们时保守全构建，避免变化未进入比较摘要。
    if any("env_var" in path.read_text(encoding=UTF8) for path in project.rglob("*")
           if path.is_file() and path.suffix in (".sql", ".yml", ".yaml") and TARGET not in path.parts):
        context["environmentObservation"] = job["job_id"]
    releases = PublicationStore(executor.jobs.db)
    release = await asyncio.to_thread(releases.get_release, job["project_id"], request["releaseId"])
    baseline = None
    if release["baseline_release_id"]:
        prior = await asyncio.to_thread(releases.get_release, job["project_id"], release["baseline_release_id"])
        # 物理复用只能沿本分支固定基线，禁止跨分支引用 creator run。
        if prior["branch_id"] != release["branch_id"]:
            raise ValueError("构建基线不属于当前分支")
        # 较早全构建版本可能没有逻辑比较状态，显式回退全构建。
        try:
            raw = await asyncio.to_thread(
                executor.artifacts.read_file, prior["artifact_set_id"], TARGET + "/" + STATE_FILE)
            baseline = json.loads(raw)
        except ValueError:
            baseline = None
    plan = plan_publication(current, baseline, context)
    reused = [binding.model_copy(update={"mode": BindingMode.REUSED}) for binding in plan.relation_bindings
              if binding.native_id in plan.reuse_native_ids]
    if {item.native_id for item in reused} != set(plan.reuse_native_ids):
        raise ValueError("基线缺少完整复用绑定")
    prefix = await asyncio.to_thread(apply_relation_bindings, project, UUID(job["job_id"]), job["schema_name"], reused)
    await command(PARSE, NO_PARTIAL_PARSE)
    await asyncio.to_thread(validate_bound_manifest, target, job["schema_name"], prefix, reused)

    await command(COMPILE)
    compiled = json.loads((target / MANIFEST_FILE).read_text(encoding=UTF8))
    dialect = compiled["metadata"]["adapter_type"]
    for node in compiled["nodes"].values():
        if node.get("resource_type") in (MODEL, TEST):
            validate_readonly_sql(node.get("compiled_code", ""), dialect)
    # 新版 dbt 把 freshness/loaded_at_query 放入 config；保留旧产物顶层字段兼容。
    configured_sources = {key: {**node, **node.get("config", {})}
                          for key, node in compiled.get("sources", {}).items()}
    fresh_sources = {key for key, node in configured_sources.items() if requires_source_freshness(node)}
    for node in configured_sources.values():
        if node.get("loaded_at_query"):
            validate_readonly_sql(node["loaded_at_query"], dialect)
    if fresh_sources:
        await command(SOURCE, FRESHNESS)
        freshness = json.loads((target / SOURCES_FILE).read_text(encoding=UTF8)).get("results", [])
        passed = {row["unique_id"] for row in freshness if row.get("status") == "pass"}
        if not fresh_sources <= passed:
            raise ValueError("source freshness 证明不完整或未通过")

    # 所有测试每次执行；只有被选中的物理模型可进入写入选择器。
    if plan.build_mode == BuildMode.SEMANTIC_ONLY:
        await command(TEST)
    elif plan.build_mode == BuildMode.SELECTIVE_BUILD:
        # unique_id 不包含模型目录；必须使用 dbt 解析出的完整 fqn 选择嵌套模型。
        selectors = ["fqn:" + ".".join(logical["nodes"][node]["fqn"]) for node in plan.selected_native_ids]
        await command(BUILD, SELECT, *selectors, TEST_SELECTOR)
    else:
        await command(BUILD)
    result_path = target / RESULTS_FILE
    raw_results = result_path.read_bytes()
    if len(raw_results) > settings.max_artifact_file_bytes:
        raise ValueError("构建步骤证明超限")
    await command(DOCS, GENERATE)
    result_path.write_bytes(raw_results)
    probe = await executor._programmatic(job, runner, project, attempt, {"mode": "PROBE"})
    manifest = json.loads((target / MANIFEST_FILE).read_text(encoding=UTF8))
    physical_catalog = json.loads((target / CATALOG_FILE).read_text(encoding=UTF8))
    results = json.loads(raw_results)
    records = results.get("results")
    if not isinstance(records, list) or any(row.get("status") not in ("success", "pass") for row in records):
        raise ValueError("构建或测试证明失败")
    completed = {row["unique_id"] for row in records}
    tests = {key for key, node in manifest["nodes"].items() if node.get("resource_type") == TEST}
    if not (tests | set(plan.selected_native_ids)) <= completed:
        raise ValueError("模型或必需测试执行不完整")
    await asyncio.to_thread(validate_bound_manifest, target, job["schema_name"], prefix, reused)
    physical_ids = set(plan.selected_native_ids + plan.reuse_native_ids)
    if not physical_ids <= physical_catalog.get("nodes", {}).keys():
        raise ValueError("物理对象缺失，不能复用发布")
    empty = not manifest["nodes"] and not manifest.get("sources") and not manifest.get("metrics")
    if probe.get("queryCapability") is not True and not empty:
        raise ValueError("缺少查询证明")
    evidence = validate_publication_evidence(plan, {
        "build": NOT_REQUIRED if plan.build_mode == BuildMode.SEMANTIC_ONLY else PASSED,
        "tests": PASSED, "semanticValidation": PASSED, "relationVerification": PASSED,
        "queryProbe": PASSED, "coveredNativeIds": sorted(physical_ids),
    })
    native = await asyncio.to_thread(catalog_from_artifacts, target, query_capability=not empty)
    native["ephemeralDependencies"] = {
        key: node.get("depends_on", {}).get("nodes", []) for key, node in manifest["nodes"].items()
        if node.get("resource_type") == MODEL and node.get("config", {}).get("materialized") == EPHEMERAL}
    catalog = await asyncio.to_thread(write_publication_catalog, target, project_id=job["project_id"],
                                      release_id=UUID(request["releaseId"]), run_id=UUID(job["job_id"]),
                                      native_catalog=native, reused_bindings=reused)
    current["relationBindings"] = catalog["relationBindings"]
    (target / STATE_FILE).write_text(json.dumps(current, sort_keys=True), encoding=UTF8)
    (target / EVIDENCE_FILE).write_text(json.dumps(evidence, sort_keys=True), encoding=UTF8)
    validation = {"allTestsPassed": True, "representativeQueryPassed": True, "relationsVerified": True,
                  "queryCapability": not empty, "publicationValidated": True, "buildMode": plan.build_mode,
                  "schemaName": job["schema_name"], "toolchainVersion": job["toolchain_version"],
                  "evidence": evidence, "plan": plan.model_dump(mode="json", by_alias=True)}
    output = await asyncio.to_thread(
        executor.artifacts.capture, job["project_id"], project, producer_attempt_id=job["attempt_id"],
        kind=EXECUTION, metadata={**executor._metadata(job), "validation_json": validation, "catalog_json": catalog},
    )
    return ExecutionResult(validation, output)
