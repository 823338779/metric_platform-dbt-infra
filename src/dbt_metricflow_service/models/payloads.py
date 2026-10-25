"""Validated internal payload contracts and explicit open JSON boundaries."""

from typing import TypeAlias

from pydantic import JsonValue

from dbt_metricflow_service import constants as strings

from .artifacts import Contract

JsonObject: TypeAlias = dict[str, JsonValue]


class DimensionOption(Contract):
    option_id: str
    name: str | None = None
    semantic_model_name: str | None = None
    display_name: str
    dimension_type: str
    value_type: str
    granularities: list[str]
    operators: list[str]


class AvailableDimensions(Contract):
    build_id: str
    metrics: list[str]
    dimensions: list[DimensionOption]


class ValidationCheck(Contract):
    name: str
    status: str
    message: str | None


class ValidationSummary(Contract):
    phase: str
    checks: list[ValidationCheck]
    truncated: bool


class ResultColumn(Contract):
    name: str
    type: str


class NormalizedTimeRange(Contract):
    start_time: str | None
    end_time: str | None


class QueryTimeMetadata(Contract):
    business_timezone: str
    normalized_time_range: NormalizedTimeRange
    boundary_policy: str = strings.QUERY_TIME_BOUNDARY_POLICY
