"""为平台构建校验固定 schema，保留项目的物理表名。"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

SCHEMA_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
TABLE_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
MAX_SCHEMA_NAME_LENGTH = 256
MAX_TABLE_NAME_LENGTH = 1024
ALIAS_MACRO_PATTERN = re.compile(r"\{%\s*macro\s+generate_alias_name\s*\(", re.IGNORECASE)
MACROS_DIRECTORY = "macros"
TARGET_PATH = "target-path"
TARGET_DIRECTORY = "target"


def validate_schema_name(schema: str) -> str:
    """只允许安全的 StarRocks database 标识符。"""

    if not isinstance(schema, str) or len(schema) > MAX_SCHEMA_NAME_LENGTH or not SCHEMA_NAME_PATTERN.fullmatch(schema):
        raise ValueError("固定 schema 名称无效")
    return schema


def prepare_fixed_schema_project(project: Path, schema: str) -> None:
    """仅修改已核对 Git 摘要的任务副本，不改受控仓库。"""

    validate_schema_name(schema)
    macros = project / MACROS_DIRECTORY
    if macros.exists():
        for path in macros.rglob("*.sql"):
            if ALIAS_MACRO_PATTERN.search(path.read_text(encoding="utf-8")):
                raise ValueError("项目已定义受控物理表别名宏")
    # 检查所有实际宏目录；受控 schema 宏在非默认配置下也必须被 dbt 加载。
    project_file = project / "dbt_project.yml"
    configuration = yaml.safe_load(project_file.read_text(encoding="utf-8")) if project_file.is_file() else {}
    macro_paths = configuration.get("macro-paths", [MACROS_DIRECTORY])
    for directory in macro_paths:
        for path in (project / directory).rglob("*.sql"):
            if ALIAS_MACRO_PATTERN.search(path.read_text(encoding="utf-8")):
                raise ValueError("项目已定义受控物理表别名宏")
    if MACROS_DIRECTORY not in macro_paths:
        configuration["macro-paths"] = [*macro_paths, MACROS_DIRECTORY]
    # 执行命令固定写入 target；任务副本同步配置，确保封存和历史读取找到同一批产物。
    if project_file.is_file():
        configuration[TARGET_PATH] = TARGET_DIRECTORY
        project_file.write_text(yaml.safe_dump(configuration, sort_keys=False), encoding="utf-8")
    macros.mkdir(exist_ok=True)


def validate_fixed_schema_manifest(target: Path, schema: str) -> None:
    """在任何模型写入之前确认全部可写关系属于指定 schema。"""

    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    nodes = manifest.get("nodes")
    if not isinstance(nodes, dict):
        raise ValueError("dbt 解析产物无效")
    names: set[str] = set()
    for node in nodes.values():
        if not isinstance(node, dict):
            continue
        if node.get("resource_type") == "operation":
            raise ValueError("固定 schema 构建不允许运行钩子")
        if node.get("resource_type") not in {"model", "seed", "snapshot"}:
            continue
        config = node.get("config") or {}
        if any(config.get(key) for key in ("pre-hook", "post-hook", "pre_hook", "post_hook")):
            raise ValueError("固定 schema 构建不允许模型钩子")
        if config.get("materialized") == "ephemeral":
            continue
        alias = node.get("alias")
        relation = node.get("relation_name")
        relation_parts = [part.strip('`" ') for part in relation.split(".")] if isinstance(relation, str) else []
        if (node.get("schema") != schema or not isinstance(alias, str)
                or len(alias) > MAX_TABLE_NAME_LENGTH
                or not TABLE_NAME_PATTERN.fullmatch(alias)
                or len(relation_parts) < 2 or relation_parts[-2:] != [schema, alias]
                or alias in names):
            raise ValueError("dbt 物理关系不属于指定 schema")
        names.add(alias)
