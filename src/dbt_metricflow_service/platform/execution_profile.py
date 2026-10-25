"""按构建快照生成独立 profile，核验写入目标，不保存环境配置。"""

from __future__ import annotations

import os
import re
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final

import yaml

from dbt_metricflow_service import constants as strings

from ..models.configuration import DbtProjectProfile, ExecutionProfile, ExecutionProfileOutput
from ..models.execution import SNAPSHOT_TARGET, ExecutionSnapshot
from ..settings import Settings

INVALID_DBT_PROFILE_NAME_MESSAGE: Final[str] = 'invalid dbt profile name'

UTF8 = strings.UTF8
STARROCKS, POSTGRES, DUCKDB = strings.STARROCKS_ADAPTER, strings.POSTGRES_ADAPTER, strings.DUCKDB_ADAPTER
PROFILES_FILE, PROJECT_FILE = strings.DBT_PROFILES_FILE, strings.DBT_PROJECT_FILE
TARGET_UNVERIFIED = strings.EXECUTION_TARGET_UNVERIFIED
CREDENTIAL_UNAVAILABLE = strings.CREDENTIAL_UNAVAILABLE
SECRET_PATTERN = re.compile(r"DBT_ENV_SECRET_[A-Z0-9_]+\Z")
PROFILE_PATTERN = re.compile(strings.IDENTIFIER_PATTERN)


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
    if allowed is None or allowed.type != target.type:
        raise ExecutionConfigurationError(TARGET_UNVERIFIED)
    if target.type == DUCKDB:
        path = Path(target.path or "")
        if (not path.is_absolute() or str(path.resolve()) != str(Path((allowed.path or "")).resolve())
                or snapshot.catalog != path.stem or target.host is not None or target.port is not None):
            raise ExecutionConfigurationError(TARGET_UNVERIFIED)
    elif (not target.host or target.host.lower().rstrip(".") not in
          [host.lower().rstrip(".") for host in allowed.hosts]
          or target.port != allowed.port or target.path is not None):
        raise ExecutionConfigurationError(TARGET_UNVERIFIED)


def _secret_template(reference: str) -> str:
    if not isinstance(reference, str) or not SECRET_PATTERN.fullmatch(reference) or reference not in os.environ:
        raise ExecutionConfigurationError(CREDENTIAL_UNAVAILABLE)
    return strings.JINJA_ENV_VAR_OPEN + reference + "') }}"


@contextmanager
def materialized_profile(
    snapshot: ExecutionSnapshot, root: Path, settings: Settings, project: Path | None = None,
) -> Iterator[Path]:
    """secret 通过 dbt 受保护环境引用使用，不将明文写入临时 YAML 或产物。"""
    validate_execution_target(snapshot, settings)
    connection = snapshot.connection
    if connection.type == DUCKDB:
        output = ExecutionProfileOutput(
            type=connection.type, schema_name=snapshot.schema_name, threads=snapshot.threads, path=connection.path,
        )
    else:
        credential = settings.execution_credentials.get(snapshot.credential_ref)
        if credential is None:
            raise ExecutionConfigurationError(CREDENTIAL_UNAVAILABLE)
        user = _secret_template(credential.user_env)
        password = _secret_template(credential.password_env)
        if connection.type == STARROCKS:
            # schema 承载 StarRocks database；target.database 必须留空，避免 source 被误当外部 catalog。
            output = ExecutionProfileOutput(
                type=connection.type, schema_name=snapshot.schema_name, threads=snapshot.threads,
                host=connection.host, port=connection.port, password=password,
                username=user, catalog=snapshot.catalog, database=None,
            )
        else:
            output = ExecutionProfileOutput(
                type=connection.type, schema_name=snapshot.schema_name, threads=snapshot.threads,
                host=connection.host, port=connection.port, password=password, user=user, dbname=snapshot.catalog,
            )
    profile_name = SNAPSHOT_TARGET
    if project is not None:
        configuration = DbtProjectProfile.model_validate(
            yaml.safe_load((project / PROJECT_FILE).read_text(encoding=UTF8)),
        )
        profile_name = (
            configuration.profile if strings.DBT_PROFILE_FIELD in configuration.model_fields_set else configuration.name
        )
    if not isinstance(profile_name, str) or not PROFILE_PATTERN.fullmatch(profile_name):
        raise ValueError(INVALID_DBT_PROFILE_NAME_MESSAGE)
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=strings.PROFILE_TEMP_PREFIX, dir=root) as name:
        directory = Path(name)
        profile = ExecutionProfile(target=SNAPSHOT_TARGET, outputs={SNAPSHOT_TARGET: output})
        configuration = {profile_name: profile.model_dump(by_alias=True, exclude_unset=True)}
        (directory / PROFILES_FILE).write_text(yaml.safe_dump(configuration), encoding=UTF8)
        yield directory
