"""候选构建的受控物理映射。"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Final, cast
from uuid import UUID

import sqlglot
from sqlglot import exp

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.native import NativeManifest, NativeNode, NativePhysicalCatalog, NativeResults
from dbt_metricflow_service.storage.records import ArtifactValidation, JobErrorDetail, LeasedJob

from ..platform.namespace import TABLE_NAME_PATTERN, prepare_fixed_schema_project
from .template import validate_templates

if TYPE_CHECKING:
    from dbt_metricflow_service.runtime.executor import ExecutionResult, RuntimeExecutor


CONTROLLED_SCHEMA_MACRO_EXISTS_MESSAGE: Final[str] = '项目已有受控 schema 宏'
RELATION_OUTSIDE_CANDIDATE_MESSAGE: Final[str] = '新建关系不属于候选'
RELATION_IDENTITY_MISMATCH_MESSAGE: Final[str] = '物理关系重复或与节点标识不一致'
WRITE_HOOKS_FORBIDDEN_MESSAGE: Final[str] = '发布构建禁止写入钩子'
SQL_HEADER_FORBIDDEN_MESSAGE: Final[str] = '发布构建禁止执行 SQL header'
EXECUTION_NODE_UNSUPPORTED_MESSAGE: Final[str] = '发布暂不支持 seed、snapshot 或自定义执行节点'
TEST_FAILURE_STORAGE_FORBIDDEN_MESSAGE: Final[str] = '发布测试禁止写入失败结果表'
MATERIALIZATION_UNSUPPORTED_MESSAGE: Final[str] = '发布暂不支持此物化策略'
READONLY_SQL_UNPROVEN_MESSAGE: Final[str] = '发布 SQL 无法证明为只读查询'
SINGLE_READONLY_QUERY_REQUIRED_MESSAGE: Final[str] = '发布 SQL 必须为单条只读查询'
SOURCE_FRESHNESS_PROOF_INCOMPLETE_MESSAGE: Final[str] = 'source freshness 证明不完整或未通过'
BUILD_PROOF_LIMIT_MESSAGE: Final[str] = '构建步骤证明超限'
BUILD_PROOF_FAILED_MESSAGE: Final[str] = '构建或测试证明失败'
REQUIRED_EXECUTION_INCOMPLETE_MESSAGE: Final[str] = '模型或必需测试执行不完整'
PHYSICAL_OBJECTS_MISSING_MESSAGE: Final[str] = '物理对象缺失，不能发布'
QUERY_PROOF_MISSING_MESSAGE: Final[str] = '缺少查询证明'

UTF8 = strings.UTF8
MANIFEST_FILE = strings.MANIFEST_FILE
MACROS_DIRECTORY = strings.MACROS_DIRECTORY
SCHEMA_FILE = "generate_schema_name.sql"
MODEL = strings.DBT_MODEL_RESOURCE
EPHEMERAL = strings.DBT_EPHEMERAL_MATERIALIZATION
OPERATION = strings.DBT_OPERATION_RESOURCE
HOOK_KEYS = (
    strings.DBT_PRE_HOOK_FIELD,
    strings.DBT_POST_HOOK_FIELD,
    strings.DBT_PRE_HOOK_ATTRIBUTE,
    strings.DBT_POST_HOOK_ATTRIBUTE,
)
SAFE_MATERIALIZATIONS = frozenset({strings.DBT_TABLE_MATERIALIZATION, strings.DBT_VIEW_MATERIALIZATION, EPHEMERAL})
TARGET = strings.TARGET_DIRECTORY
CATALOG_FILE = strings.CATALOG_FILE
RESULTS_FILE = strings.RUN_RESULTS_FILE
DBT = strings.DBT_EXECUTABLE
PARSE = strings.DBT_PARSE_COMMAND
DEPS = strings.DBT_DEPS_COMMAND
BUILD = strings.DBT_BUILD_COMMAND
TEST = strings.DBT_TEST_RESOURCE
COMPILE = strings.DBT_COMPILE_COMMAND
SOURCE = strings.DBT_SOURCE_COMMAND
FRESHNESS = strings.DBT_FRESHNESS_COMMAND
FRESHNESS_THRESHOLDS = (strings.DBT_WARN_AFTER_FIELD, strings.DBT_ERROR_AFTER_FIELD)
COUNT = strings.DBT_COUNT_FIELD
PERIOD = strings.DBT_PERIOD_FIELD
SOURCES_FILE = strings.SOURCES_FILE
STORE_FAILURES = strings.DBT_STORE_FAILURES_FIELD
STORE_FAILURES_AS = strings.DBT_STORE_FAILURES_AS_FIELD
DOCS = strings.DBT_DOCS_COMMAND
GENERATE = strings.DBT_GENERATE_COMMAND
NO_PARTIAL_PARSE = strings.DBT_NO_PARTIAL_PARSE_OPTION
BUILDING = strings.BUILDING
COMMAND_PHASES = {
    DEPS: strings.FETCHING_SOURCE,
    PARSE: strings.PARSING,
    COMPILE: strings.COMPILING,
    BUILD: BUILDING,
    SOURCE: strings.TESTING,
    DOCS: strings.PACKAGING,
}
EXECUTION = strings.EXECUTION
ROOT_CONFIGS = (strings.DBT_PROJECT_FILE, strings.PACKAGES_FILE, strings.DEPENDENCIES_FILE, strings.PACKAGE_LOCK_FILE)
# dbt 测试默认配置 dbt_test__audit，即使 store_failures=false 也会传入此值。
# 测试统一解析到受控 schema；validate_bound_manifest 会在执行前拒绝写入失败结果表。
SCHEMA_TEMPLATE = """{% macro generate_schema_name(custom_schema_name=none, node=none) -%}
    {%- if node.resource_type == 'test' -%}
        {{ return(target.schema) }}
    {%- elif custom_schema_name is not none -%}
        {{ exceptions.raise_compiler_error('Custom schema is not supported for publication') }}
    {%- else -%}
        {{ return(target.schema) }}
    {%- endif -%}
{%- endmacro %}
"""


def prepare_publication_project(project: Path, schema: str) -> None:
    """物理模型使用指定 schema 和原表名，只修改任务副本。"""
    prepare_fixed_schema_project(project, schema)
    path = project / MACROS_DIRECTORY / SCHEMA_FILE
    if path.exists():
        raise ValueError(CONTROLLED_SCHEMA_MACRO_EXISTS_MESSAGE)
    path.write_text(SCHEMA_TEMPLATE, encoding=UTF8)


def validate_bound_manifest(target: Path, schema: str) -> None:
    # 最终解析的所有物理模型必须属于指定 schema。
    manifest = NativeManifest.model_validate_json((target / MANIFEST_FILE).read_bytes())
    observed = set()
    for node in manifest.nodes.values():
        validate_execution_policy(node)
        config = node.config
        if node.resource_type != MODEL:
            continue
        if config.materialized == EPHEMERAL:
            continue
        actual = (node.database, node.schema_name, node.alias)
        if (
            actual[1] != schema
            or not isinstance(actual[2], str)
            or not TABLE_NAME_PATTERN.fullmatch(actual[2])
        ):
            raise ValueError(RELATION_OUTSIDE_CANDIDATE_MESSAGE)
        relation_parts = [part.strip('`" ') for part in (node.relation_name or "").split(".")]
        if relation_parts[-2:] != list(actual[1:]) or actual in observed:
            raise ValueError(RELATION_IDENTITY_MISMATCH_MESSAGE)
        observed.add(actual)


def validate_execution_policy(node: NativeNode) -> None:
    # 草稿与发布共用定义边界；此检查不连接数据库、不执行 hook。
    config = node.config
    if node.resource_type == OPERATION or any(
        (config.pre_hook, config.post_hook, config.pre_hook_legacy, config.post_hook_legacy)
    ):
        raise ValueError(WRITE_HOOKS_FORBIDDEN_MESSAGE)
    if config.sql_header:
        raise ValueError(SQL_HEADER_FORBIDDEN_MESSAGE)
    if node.resource_type not in (MODEL, TEST):
        raise ValueError(EXECUTION_NODE_UNSUPPORTED_MESSAGE)
    if node.resource_type == TEST and (
        config.store_failures or config.store_failures_as not in (None, EPHEMERAL)
    ):
        raise ValueError(TEST_FAILURE_STORAGE_FORBIDDEN_MESSAGE)
    if node.resource_type == MODEL and config.materialized not in SAFE_MATERIALIZATIONS:
        raise ValueError(MATERIALIZATION_UNSUPPORTED_MESSAGE)


def validate_readonly_sql(sql: str, dialect: str) -> None:
    # 编译后的模型只能是单条取数语句，阻止宏输出多语句或带写入的 CTE。
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except sqlglot.errors.ParseError as error:
        raise ValueError(READONLY_SQL_UNPROVEN_MESSAGE) from error
    if (
        len(statements) != 1
        or not isinstance(statements[0], exp.Query)
        or any(isinstance(node, (exp.DDL, exp.DML, exp.Command, exp.Into)) for node in statements[0].walk())
    ):
        raise ValueError(SINGLE_READONLY_QUERY_REQUIRED_MESSAGE)


def requires_source_freshness(node: NativeNode) -> bool:
    """只对有效阈值要求 freshness 证明，兼容 dbt 为未配置阈值生成的空对象。"""
    freshness = (
        node.config.freshness if strings.DBT_FRESHNESS_COMMAND in node.config.model_fields_set else node.freshness
    )
    if freshness is None:
        return False
    # count=0 is a valid configured threshold.
    return any(
        threshold is not None and threshold.count is not None and threshold.period is not None
        for threshold in (freshness.warn_after, freshness.error_after)
    )


async def execute_publication(
    executor: RuntimeExecutor, job: LeasedJob, attempt: Path, project: Path, *, profiles: Path | None = None,
) -> ExecutionResult:
    """全量构建和完整验证使用同一个受租约控制的命令执行器。"""
    from ..execution.models import CommandSpec
    from ..platform.catalog import catalog_from_artifacts
    from ..runtime.executor import ExecutionResult, build_engine_environment
    from .sealed_catalog import write_publication_catalog

    target = project / TARGET
    request = job.request_json
    profiles = profiles or executor.settings.profiles_dir
    settings = executor.settings
    environment = build_engine_environment(
        project,
        profiles,
        cast(str, job.schema_name),
        cast(str, job.profile_binding_id),
    )
    common = (
        strings.DBT_PROJECT_DIR_OPTION,
        str(project),
        strings.DBT_PROFILES_DIR_OPTION,
        str(profiles),
        strings.DBT_TARGET_OPTION,
        cast(str, job.profile_binding_id),
        strings.DBT_TARGET_PATH_OPTION,
        str(target),
    )

    async def command(*args: str) -> None:
        # deps 不支持 target-path，沿用既有执行器的命令参数边界。
        options = common[:-2] if args[0] == DEPS else common
        spec = CommandSpec((DBT, *args, *options), project, environment, args[0] == BUILD)
        from ..runtime.executor import ExecutionError
        from .validation_summary import failure_summary

        try:
            await executor._dbt(job, spec, COMMAND_PHASES[args[0]])
        except ExecutionError as error:
            # 摘要与当前租约失败事务一起保存；不复制 stderr、SQL 或数据库异常。
            summary = failure_summary(target, args[0], settings.max_artifact_file_bytes)
            raise ExecutionError(
                error.code, JobErrorDetail(validation_summary=summary), stopped=error.stopped,
            ) from error

    # 验证源码和已安装依赖后，为全部模型应用本次构建的受控前缀。
    await asyncio.to_thread(validate_templates, project)
    if any((project / name).exists() for name in ROOT_CONFIGS[1:3]):
        await command(DEPS)
        await asyncio.to_thread(validate_templates, project)
    await asyncio.to_thread(
        prepare_publication_project, project, cast(str, job.schema_name)
    )
    await command(PARSE, NO_PARTIAL_PARSE)
    await asyncio.to_thread(validate_bound_manifest, target, cast(str, job.schema_name))

    if request.execution:
        from .execution_profile import validate_execution_manifest
        await asyncio.to_thread(validate_execution_manifest, target,
                                request.execution)
    await command(COMPILE)
    compiled = NativeManifest.model_validate_json((target / MANIFEST_FILE).read_bytes())
    dialect = compiled.metadata.adapter_type
    for node in compiled.nodes.values():
        if node.resource_type in (MODEL, TEST):
            validate_readonly_sql(node.compiled_code, dialect)
    # 新版 dbt 把 freshness/loaded_at_query 放入 config；保留旧产物顶层字段兼容。
    fresh_sources = {key for key, node in compiled.sources.items() if requires_source_freshness(node)}
    for node in compiled.sources.values():
        loaded_at_query = (
            node.config.loaded_at_query
            if strings.DBT_LOADED_AT_QUERY_FIELD in node.config.model_fields_set
            else node.loaded_at_query
        )
        if loaded_at_query:
            validate_readonly_sql(loaded_at_query, dialect)
    if fresh_sources:
        await command(SOURCE, FRESHNESS)
        freshness = NativeResults.model_validate_json((target / SOURCES_FILE).read_bytes()).results
        passed = {row.unique_id for row in freshness if row.status == strings.DBT_PASS_STATUS}
        if not fresh_sources <= passed:
            raise ValueError(SOURCE_FRESHNESS_PROOF_INCOMPLETE_MESSAGE)

    if request.execution:
        from .execution_profile import validate_execution_manifest
        await asyncio.to_thread(validate_execution_manifest, target,
                                request.execution)

    # 每次执行全部模型和测试，并在封存前核对完整覆盖。
    await command(BUILD)
    result_path = target / RESULTS_FILE
    raw_results = result_path.read_bytes()
    if len(raw_results) > settings.max_artifact_file_bytes:
        raise ValueError(BUILD_PROOF_LIMIT_MESSAGE)
    await command(DOCS, GENERATE)
    probe = (await executor.probe_query_capability(job, project, profiles=profiles)
             if request.execution else await executor.probe_query_capability(job, project))
    manifest = NativeManifest.model_validate_json((target / MANIFEST_FILE).read_bytes())
    physical_catalog = NativePhysicalCatalog.model_validate_json((target / CATALOG_FILE).read_bytes())
    records = NativeResults.model_validate_json(raw_results).results
    if any(row.status not in (strings.DBT_SUCCESS_STATUS, strings.DBT_PASS_STATUS) for row in records):
        raise ValueError(BUILD_PROOF_FAILED_MESSAGE)
    completed = {row.unique_id for row in records}
    tests = {key for key, node in manifest.nodes.items() if node.resource_type == TEST}
    physical_ids = {
        key for key, node in manifest.nodes.items()
        if node.resource_type == MODEL and node.config.materialized != EPHEMERAL
    }
    if not (tests | physical_ids) <= completed:
        raise ValueError(REQUIRED_EXECUTION_INCOMPLETE_MESSAGE)
    await asyncio.to_thread(validate_bound_manifest, target, cast(str, job.schema_name))
    if not physical_ids <= physical_catalog.nodes.keys():
        raise ValueError(PHYSICAL_OBJECTS_MISSING_MESSAGE)
    empty = not manifest.nodes and not manifest.sources and not manifest.metrics
    if probe.query_capability is not True and not empty:
        raise ValueError(QUERY_PROOF_MISSING_MESSAGE)
    native = await asyncio.to_thread(catalog_from_artifacts, target, query_capability=not empty)
    native.ephemeral_dependencies = {
        key: node.depends_on.nodes
        for key, node in manifest.nodes.items()
        if node.resource_type == MODEL and node.config.materialized == EPHEMERAL
    }
    await asyncio.to_thread(
        write_publication_catalog,
        target,
        project_id=job.project_id,
        release_id=UUID(request.build_id),
        run_id=UUID(job.job_id),
        native_catalog=native,
    )
    metadata = executor._metadata(job)
    metadata.validation_json = ArtifactValidation(
        all_tests_passed=True, representative_query_passed=True, relations_verified=True,
    )
    output = await asyncio.to_thread(
        executor.artifacts.capture,
        job.project_id,
        project,
        producer_attempt_id=job.attempt_id,
        kind=EXECUTION,
        metadata=metadata,
    )
    return ExecutionResult(output_set_id=output)
