"""调用方完整提供的不可变执行快照；不注册环境或源码绑定。"""

from __future__ import annotations

import hashlib
import json
from typing import Final, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator

from dbt_metricflow_service import constants as strings

from .contracts import Contract

INVALID_BUSINESS_TIMEZONE_MESSAGE: Final[str] = 'businessTimezone must be an IANA timezone'

IDENTIFIER_PATTERN = r"^[a-z][a-z0-9_]{0,62}$"
SNAPSHOT_TARGET = "execution"
JSON_MODE = strings.JSON_MODE


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
    schema_name: str = Field(alias=strings.DBT_SCHEMA_FIELD, pattern=IDENTIFIER_PATTERN, description="物理写入 schema")
    threads: int = Field(default=1, ge=1, le=32, description="单次 dbt 并行度")
    business_timezone: str = Field(default=strings.UTC_TIMEZONE, description="该构建查询的业务时区")

    @field_validator(strings.BUSINESS_TIMEZONE_FIELD)
    @classmethod
    def valid_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ValueError(INVALID_BUSINESS_TIMEZONE_MESSAGE) from error
        return value


def physical_target_key(snapshot: ExecutionSnapshot) -> str:
    """地址别名在入口核验为相同 instanceId，凭据不参与物理隔离。"""
    parts = [snapshot.connection.type, snapshot.connection.instance_id, snapshot.catalog, snapshot.schema_name]
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()
