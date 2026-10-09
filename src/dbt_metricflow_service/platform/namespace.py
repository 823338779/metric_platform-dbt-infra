"""为平台固定 schema 内的每次 dbt 构建隔离物理对象。"""

from __future__ import annotations

import json
import re
from pathlib import Path
from uuid import UUID

SCHEMA_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
TABLE_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
MAX_SCHEMA_NAME_LENGTH = 256
MAX_TABLE_NAME_LENGTH = 1024
ALIAS_MACRO_PATTERN = re.compile(r"\{%\s*macro\s+generate_alias_name\s*\(", re.IGNORECASE)
ALIAS_MACRO_FILE = "generate_alias_name.sql"
MACROS_DIRECTORY = "macros"


def validate_schema_name(schema: str) -> str:
    """只允许安全的 StarRocks database 标识符。"""

    if not isinstance(schema, str) or len(schema) > MAX_SCHEMA_NAME_LENGTH or not SCHEMA_NAME_PATTERN.fullmatch(schema):
        raise ValueError("固定 schema 名称无效")
    return schema


def run_prefix(run_id: UUID) -> str:
    """同一 run 重试时始终生成相同的模型表前缀。"""

    return f"rv_{run_id.hex}_"


def prepare_versioned_project(project: Path, run_id: UUID, schema: str) -> str:
    """仅修改已核对 Git 摘要的任务副本，不改受控仓库。"""

    validate_schema_name(schema)
    prefix = run_prefix(run_id)
    macros = project / MACROS_DIRECTORY
    if macros.exists():
        for path in macros.rglob("*.sql"):
            if ALIAS_MACRO_PATTERN.search(path.read_text(encoding="utf-8")):
                raise ValueError("项目已定义受控物理表别名宏")
    macros.mkdir(exist_ok=True)
    (macros / ALIAS_MACRO_FILE).write_text(
        "{% macro generate_alias_name(custom_alias_name=none, node=none) -%}\n"
        "    {%- if custom_alias_name is none -%}\n"
        f"        {{{{ return('{prefix}' ~ node.name) }}}}\n"
        "    {%- else -%}\n"
        f"        {{{{ return('{prefix}' ~ (custom_alias_name | trim)) }}}}\n"
        "    {%- endif -%}\n"
        "{%- endmacro %}\n",
        encoding="utf-8",
    )
    return prefix


def validate_versioned_manifest(target: Path, schema: str, prefix: str) -> None:
    """在任何模型写入之前确认全部可写关系属于本 run。"""

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
                or not alias.startswith(prefix) or len(alias) > MAX_TABLE_NAME_LENGTH
                or not TABLE_NAME_PATTERN.fullmatch(alias)
                or len(relation_parts) < 2 or relation_parts[-2:] != [schema, alias]
                or alias in names):
            raise ValueError("dbt 物理关系不属于当前运行任务")
        names.add(alias)
