"""按构建快照生成独立 profile，核验写入目标，不保存环境配置。"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import yaml

from ..models.execution import SNAPSHOT_TARGET, ExecutionSnapshot
from ..settings import Settings

UTF8 = "utf-8"
STARROCKS, POSTGRES, DUCKDB = "starrocks", "postgres", "duckdb"
PROFILES_FILE, PROJECT_FILE, MANIFEST_FILE = "profiles.yml", "dbt_project.yml", "manifest.json"
TARGET_UNVERIFIED = "EXECUTION_TARGET_UNVERIFIED"
CREDENTIAL_UNAVAILABLE = "CREDENTIAL_UNAVAILABLE"
SECRET_PATTERN = re.compile(r"DBT_ENV_SECRET_[A-Z0-9_]+\Z")
PROFILE_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


class ExecutionConfigurationError(ValueError):
    """可信运行配置不可用，只携带固定错误码，不暴露 secret 或配置内容。"""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        # 边界仅使用本模块定义的错误码生成公开响应。
        self.code = code


def validate_execution_target(snapshot: ExecutionSnapshot, settings: Settings) -> None:
    """可信部署清单归一物理实例，防止别名或伪造实例身份绕过目标冲突。"""
    target = snapshot.connection
    allowed = settings.execution_instances.get(target.instance_id)
    if not isinstance(allowed, dict) or allowed.get("type") != target.type:
        raise ExecutionConfigurationError(TARGET_UNVERIFIED)
    if target.type == DUCKDB:
        path = Path(target.path or "")
        if (not path.is_absolute() or str(path.resolve()) != str(Path(allowed.get("path", "")).resolve())
                or snapshot.catalog != path.stem or target.host is not None or target.port is not None):
            raise ExecutionConfigurationError(TARGET_UNVERIFIED)
    elif (not target.host or target.host.lower().rstrip(".") not in
          [host.lower().rstrip(".") for host in allowed.get("hosts", [])]
          or target.port != allowed.get("port") or target.path is not None):
        raise ExecutionConfigurationError(TARGET_UNVERIFIED)


def _secret_template(reference: object) -> str:
    if not isinstance(reference, str) or not SECRET_PATTERN.fullmatch(reference) or reference not in os.environ:
        raise ExecutionConfigurationError(CREDENTIAL_UNAVAILABLE)
    return "{{ env_var('" + reference + "') }}"


@contextmanager
def materialized_profile(
    snapshot: ExecutionSnapshot, root: Path, settings: Settings, project: Path | None = None,
) -> Iterator[Path]:
    """secret 通过 dbt 受保护环境引用使用，不将明文写入临时 YAML 或产物。"""
    validate_execution_target(snapshot, settings)
    connection = snapshot.connection
    output = {"type": connection.type, "schema": snapshot.schema_name, "threads": snapshot.threads}
    if connection.type == DUCKDB:
        output["path"] = connection.path
    else:
        credential = settings.execution_credentials.get(snapshot.credential_ref)
        if not isinstance(credential, dict):
            raise ExecutionConfigurationError(CREDENTIAL_UNAVAILABLE)
        user = _secret_template(credential.get("userEnv"))
        password = _secret_template(credential.get("passwordEnv"))
        output.update(host=connection.host, port=connection.port, password=password)
        if connection.type == STARROCKS:
            # schema 承载 StarRocks database；target.database 必须留空，避免 source 被误当外部 catalog。
            output.update(username=user, catalog=snapshot.catalog, database=None)
        else:
            output.update(user=user, dbname=snapshot.catalog)
    profile_name = SNAPSHOT_TARGET
    if project is not None:
        configuration = yaml.safe_load((project / PROJECT_FILE).read_text(encoding=UTF8))
        profile_name = configuration.get("profile", configuration.get("name"))
    if not isinstance(profile_name, str) or not PROFILE_PATTERN.fullmatch(profile_name):
        raise ValueError("invalid dbt profile name")
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="execution-profile-", dir=root) as name:
        directory = Path(name)
        configuration = {profile_name: {"target": SNAPSHOT_TARGET, "outputs": {SNAPSHOT_TARGET: output}}}
        (directory / PROFILES_FILE).write_text(yaml.safe_dump(configuration), encoding=UTF8)
        yield directory


def validate_execution_manifest(target: Path, snapshot: ExecutionSnapshot) -> None:
    """必须核验适配器 database 含义及显式 relation catalog，不能仅比较 schema。"""
    from .build import validate_bound_manifest

    validate_bound_manifest(target, snapshot.schema_name)
    manifest = json.loads((target / MANIFEST_FILE).read_text(encoding=UTF8))
    starrocks = snapshot.connection.type == STARROCKS
    # StarRocks 内置宏将节点 database 置空；物化宏另外读取 config.catalog/database。
    databases = (None, snapshot.schema_name, snapshot.catalog) if starrocks else (snapshot.catalog,)
    for node in manifest["nodes"].values():
        if node.get("resource_type") != "model" or node.get("config", {}).get("materialized") == "ephemeral":
            continue
        parts = [part.strip('`" ') for part in node.get("relation_name", "").split(".")]
        config = node.get("config", {})
        if (node.get("database") not in databases or len(parts) not in (2, 3)
                or starrocks and config.get("catalog", snapshot.catalog) != snapshot.catalog
                or starrocks and config.get("database") not in (None, snapshot.schema_name)
                or len(parts) == 3 and parts[0] != snapshot.catalog):
            raise ValueError("model relation escapes execution catalog")
