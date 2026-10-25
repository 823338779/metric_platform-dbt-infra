"""Service health, version and error response contracts."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from dbt_metricflow_service import constants as strings

from .contracts import Contract


class ErrorView(Contract):
    code: str = Field(description="稳定错误码")
    message: str = Field(description="不含凭据或本地路径的诊断")
    retryable: bool = Field(default=False, description="能否以同请求重试")
    phase: str | None = Field(default=None, description="失败执行阶段")
    build_id: UUID | None = Field(default=None, description="已有构建身份")


class HealthView(Contract):
    status: Literal["ok", "ready"]


class VersionsView(Contract):
    service: str
    dbt_core: str = Field(alias=strings.DBT_CORE_PACKAGE)
    dbt_starrocks: str = Field(alias=strings.DBT_STARROCKS_PACKAGE)
    dbt_duckdb: str = Field(alias=strings.DBT_DUCKDB_PACKAGE)
    dbt_metricflow: str = Field(alias=strings.DBT_METRICFLOW_PACKAGE)
    metricflow: str


class ErrorResponse(Contract):
    error: ErrorView
