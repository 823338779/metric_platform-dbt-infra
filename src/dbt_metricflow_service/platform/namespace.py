"""为平台构建校验固定 schema，保留项目的物理表名。"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Final

import yaml

from dbt_metricflow_service import constants as strings

from ..models.native import NativeManifest, NativeProject

INVALID_FIXED_SCHEMA_MESSAGE: Final[str] = '固定 schema 名称无效'
CONTROLLED_ALIAS_MACRO_EXISTS_MESSAGE: Final[str] = '项目已定义受控物理表别名宏'
RUN_HOOKS_FORBIDDEN_MESSAGE: Final[str] = '固定 schema 构建不允许运行钩子'
MODEL_HOOKS_FORBIDDEN_MESSAGE: Final[str] = '固定 schema 构建不允许模型钩子'
RELATION_OUTSIDE_SCHEMA_MESSAGE: Final[str] = 'dbt 物理关系不属于指定 schema'

SCHEMA_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
TABLE_NAME_PATTERN = re.compile(strings.IDENTIFIER_PATTERN)
MAX_SCHEMA_NAME_LENGTH = 256
MAX_TABLE_NAME_LENGTH = 1024
ALIAS_MACRO_PATTERN = re.compile(r"\{%\s*macro\s+generate_alias_name\s*\(", re.IGNORECASE)
MACROS_DIRECTORY = strings.MACROS_DIRECTORY
TARGET_DIRECTORY = strings.TARGET_DIRECTORY


def validate_schema_name(schema: str) -> str:
    """只允许安全的 StarRocks database 标识符。"""

    if not isinstance(schema, str) or len(schema) > MAX_SCHEMA_NAME_LENGTH or not SCHEMA_NAME_PATTERN.fullmatch(schema):
        raise ValueError(INVALID_FIXED_SCHEMA_MESSAGE)
    return schema


def prepare_fixed_schema_project(project: Path, schema: str) -> None:
    """仅修改已核对 Git 摘要的任务副本，不改受控仓库。"""

    validate_schema_name(schema)
    macros = project / MACROS_DIRECTORY
    if macros.exists():
        for path in macros.rglob(strings.SQL_GLOB):
            if ALIAS_MACRO_PATTERN.search(path.read_text(encoding=strings.UTF8)):
                raise ValueError(CONTROLLED_ALIAS_MACRO_EXISTS_MESSAGE)
    # 检查所有实际宏目录；受控 schema 宏在非默认配置下也必须被 dbt 加载。
    project_file = project / strings.DBT_PROJECT_FILE
    configuration = (NativeProject.model_validate(yaml.safe_load(project_file.read_text(encoding=strings.UTF8)))
                     if project_file.is_file() else NativeProject())
    macro_paths = configuration.macro_paths
    for directory in macro_paths:
        for path in (project / directory).rglob(strings.SQL_GLOB):
            if ALIAS_MACRO_PATTERN.search(path.read_text(encoding=strings.UTF8)):
                raise ValueError(CONTROLLED_ALIAS_MACRO_EXISTS_MESSAGE)
    if MACROS_DIRECTORY not in macro_paths:
        configuration.macro_paths = [*macro_paths, MACROS_DIRECTORY]
    # 执行命令固定写入 target；任务副本同步配置，确保封存和历史读取找到同一批产物。
    if project_file.is_file():
        configuration.target_path = TARGET_DIRECTORY
        project_file.write_text(yaml.safe_dump(configuration.native_details(), sort_keys=False), encoding=strings.UTF8)
    macros.mkdir(exist_ok=True)


def validate_fixed_schema_manifest(target: Path, schema: str) -> None:
    """在任何模型写入之前确认全部可写关系属于指定 schema。"""

    manifest = NativeManifest.model_validate_json((target / strings.MANIFEST_FILE).read_bytes())
    names: set[str] = set()
    for node in manifest.nodes.values():
        if node.resource_type == strings.DBT_OPERATION_RESOURCE:
            raise ValueError(RUN_HOOKS_FORBIDDEN_MESSAGE)
        if node.resource_type not in {
            strings.DBT_MODEL_RESOURCE,
            strings.DBT_SEED_RESOURCE,
            strings.DBT_SNAPSHOT_RESOURCE,
        }:
            continue
        config = node.config
        if any((config.pre_hook, config.post_hook, config.pre_hook_legacy, config.post_hook_legacy)):
            raise ValueError(MODEL_HOOKS_FORBIDDEN_MESSAGE)
        if config.materialized == strings.DBT_EPHEMERAL_MATERIALIZATION:
            continue
        alias = node.alias
        relation = node.relation_name
        relation_parts = [part.strip('`" ') for part in relation.split(".")] if isinstance(relation, str) else []
        if (node.schema_name != schema or not isinstance(alias, str)
                or len(alias) > MAX_TABLE_NAME_LENGTH
                or not TABLE_NAME_PATTERN.fullmatch(alias)
                or len(relation_parts) < 2 or relation_parts[-2:] != [schema, alias]
                or alias in names):
            raise ValueError(RELATION_OUTSIDE_SCHEMA_MESSAGE)
        names.add(alias)
