"""Service health, version and error response contracts."""

from typing import Literal

from pydantic import Field

from dbt_metricflow_service import constants as strings

from .builds import Contract, ErrorView


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


class CredentialError(Contract):
    code: Literal["mutations_disabled", "service_credential_required"]
