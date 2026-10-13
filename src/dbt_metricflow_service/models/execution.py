"""调用方完整提供的不可变执行快照；不注册环境或源码绑定。"""

from __future__ import annotations

import hashlib
import json
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator

from .builds import SHA_PATTERN, BuildView, Contract, DeploymentPolicy, Origin, validate_branch

IDENTIFIER_PATTERN = r"^[a-z][a-z0-9_]{0,62}$"
SNAPSHOT_TARGET = "execution"
JSON_MODE = "json"


class ConnectionTarget(Contract):
    """非敏感连接信息；实例身份必须由运行基础设施核验。"""

    type: Literal["starrocks", "postgres", "duckdb"] = Field(description="已支持的 dbt 适配器")
    instance_id: str = Field(min_length=1, max_length=128, description="规范化物理实例身份")
    host: str | None = Field(default=None, max_length=253, description="网络数据库主机")
    port: int | None = Field(default=None, ge=1, le=65535, description="网络数据库端口")
    path: str | None = Field(default=None, max_length=2048, description="受控 DuckDB 文件绝对路径")


class ExecutionSnapshot(Contract):
    environment_id: UUID = Field(description="外部环境稳定身份，与源码无关")
    config_version: int = Field(ge=1, description="外部环境配置版本，不是 worker 运行版本")
    connection: ConnectionTarget = Field(description="完整非敏感物理连接目标")
    credential_ref: str = Field(min_length=1, max_length=128, description="运行基础设施允许的 secret 引用")
    catalog: str = Field(pattern=IDENTIFIER_PATTERN, description="目标 catalog 或适配器数据库")
    schema_name: str = Field(alias="schema", pattern=IDENTIFIER_PATTERN, description="物理写入 schema")
    threads: int = Field(default=1, ge=1, le=32, description="单次 dbt 并行度")
    business_timezone: str = Field(default="UTC", description="该构建查询的业务时区")

    @field_validator("business_timezone")
    @classmethod
    def valid_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError("businessTimezone must be an IANA timezone") from error
        return value


class SnapshotBuildRequest(Contract):
    repository: str = Field(min_length=1, max_length=2048, description="本次源码仓库地址")
    branch_name: str = Field(description="本次明确指定的 Git 短分支名")
    commit_sha: str | None = Field(default=None, pattern=SHA_PATTERN, description="可选固定提交")
    execution: ExecutionSnapshot = Field(description="受理时固定的完整执行配置")
    deployment_policy: DeploymentPolicy = Field(default=DeploymentPolicy.NONE, description="成功部署意图")
    idempotency_key: str = Field(min_length=1, max_length=256, description="一次提交的稳定请求键")
    origin: Origin | None = Field(default=None, description="调用方追溯标签")

    _branch = field_validator("branch_name")(validate_branch)

    @field_validator("repository")
    @classmethod
    def valid_repository(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (parsed.scheme not in {"http", "https", "ssh"} or not parsed.hostname
                or parsed.password or parsed.query or parsed.fragment
                or any(char.isspace() for char in value)
                or parsed.scheme in {"http", "https"} and parsed.username):
            raise ValueError("repository must be a remote URL without embedded credentials")
        return value


class SnapshotBuildView(BuildView):
    execution: ExecutionSnapshot = Field(description="受理时固定的完整执行快照")


def physical_target_key(snapshot: ExecutionSnapshot) -> str:
    """地址别名在入口核验为相同 instanceId，凭据不参与物理隔离。"""
    parts = [snapshot.connection.type, snapshot.connection.instance_id, snapshot.catalog, snapshot.schema_name]
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()
