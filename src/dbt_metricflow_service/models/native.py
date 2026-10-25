"""Typed projections of dbt artifacts; vendor extension fields remain lossless."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, JsonValue, StrictBool

from dbt_metricflow_service import constants as strings


class NativeProjection(BaseModel):
    # Vendor-owned objects are open; accessed fields are explicit and validated.
    model_config = ConfigDict(extra=strings.EXTRA_ALLOW, populate_by_name=True)
    __pydantic_extra__: dict[str, JsonValue] = Field(init=False)

    def native_details(self) -> dict[str, JsonValue]:
        return self.model_dump(mode=strings.JSON_MODE, by_alias=True, exclude_unset=True)


class NativeMetadata(NativeProjection):
    dbt_schema_version: str | None = None
    adapter_type: str | None = None


class NativeDependencies(NativeProjection):
    nodes: list[str] = Field(default_factory=list)


class FreshnessThreshold(NativeProjection):
    count: int | None = None
    period: str | None = None


class NativeFreshness(NativeProjection):
    warn_after: FreshnessThreshold | None = None
    error_after: FreshnessThreshold | None = None


class NativeConfig(NativeProjection):
    model_config = ConfigDict(extra=strings.EXTRA_ALLOW, populate_by_name=False, validate_by_name=False)
    materialized: str | None = None
    pre_hook: JsonValue = Field(default=None, alias=strings.DBT_PRE_HOOK_FIELD)
    post_hook: JsonValue = Field(default=None, alias=strings.DBT_POST_HOOK_FIELD)
    # dbt artifacts may contain both spelling variants.
    pre_hook_legacy: JsonValue = Field(default=None, alias=strings.DBT_PRE_HOOK_ATTRIBUTE)
    post_hook_legacy: JsonValue = Field(default=None, alias=strings.DBT_POST_HOOK_ATTRIBUTE)
    sql_header: str | None = None
    store_failures: StrictBool | None = None
    store_failures_as: str | None = None
    database: str | None = None
    catalog: str | None = None
    freshness: NativeFreshness | None = None
    loaded_at_query: str | None = None


class NativeReference(NativeProjection):
    name: str


class NativeAggregation(NativeProjection):
    semantic_model: str | None = None


class NativeCumulative(NativeProjection):
    metric: str | NativeReference | None = None


class NativeConversion(NativeProjection):
    base_metric: str | NativeReference | None = None
    conversion_metric: str | NativeReference | None = None
    base_measure: str | NativeReference | None = None
    conversion_measure: str | NativeReference | None = None


class NativeTypeParams(NativeProjection):
    metrics: list[str | NativeReference] | None = None
    numerator: str | NativeReference | None = None
    denominator: str | NativeReference | None = None
    cumulative_type_params: NativeCumulative | None = None
    conversion_type_params: NativeConversion | None = None
    metric_aggregation_params: NativeAggregation | None = None
    input_measures: list[str | NativeReference] = Field(default_factory=list)
    measure: str | NativeReference | None = None
    expr: str | None = None
    window: JsonValue = None
    time_granularity: str | None = None


class NativeDefaults(NativeProjection):
    agg_time_dimension: str | None = None


class NativeRelation(NativeProjection):
    database: str | None = None
    schema_name: str | None = None
    alias: str | None = None


class NativeValueMapping(NativeProjection):
    """Lossless catalog projection; configuration validation belongs to native dbt/MetricFlow."""

    semantic_model: str
    value_dimension: str
    display_dimension: str


class NativeColumn(NativeProjection):
    name: str | None = None
    type: str | None = None
    index: int | None = None
    data_type: str | None = None
    description: str | None = None
    comment: str | None = None


class NativeNode(NativeProjection):
    name: str | None = None
    description: str | None = None
    label: str | None = None
    original_file_path: str | None = None
    resource_type: str | None = None
    config: NativeConfig = Field(default_factory=NativeConfig)
    database: str | None = None
    schema_name: str | None = Field(default=None, alias=strings.DBT_SCHEMA_FIELD)
    alias: str | None = None
    identifier: str | None = None
    relation_name: str | None = None
    columns: dict[str, NativeColumn] = Field(default_factory=dict)
    compiled_code: str = ""
    depends_on: NativeDependencies = Field(default_factory=NativeDependencies)
    dimensions: list[NativeNode] = Field(default_factory=list)
    entities: list[NativeReference] = Field(default_factory=list)
    measures: list[NativeReference] = Field(default_factory=list)
    defaults: NativeDefaults | None = None
    type: str | None = None
    expr: str | None = None
    type_params: NativeTypeParams | None = None
    filter: dict[str, JsonValue] | None = None
    time_granularity: str | None = None
    node_relation: NativeRelation | None = None
    value_mapping: NativeValueMapping | None = None
    freshness: NativeFreshness | None = None
    loaded_at_query: str | None = None


class NativeManifest(NativeProjection):
    metadata: NativeMetadata = Field(default_factory=NativeMetadata)
    nodes: dict[str, NativeNode]
    sources: dict[str, NativeNode] = Field(default_factory=dict)
    semantic_models: dict[str, NativeNode] = Field(default_factory=dict)
    metrics: dict[str, NativeNode] = Field(default_factory=dict)


class SemanticManifest(NativeProjection):
    semantic_models: list[NativeNode]
    metrics: list[NativeNode]


class NativeProject(NativeProjection):
    name: str | None = None
    profile: str | None = None
    model_paths: list[str] = Field(
        default_factory=lambda: [strings.MODELS_DIRECTORY], alias=strings.DBT_MODEL_PATHS_FIELD
    )
    macro_paths: list[str] = Field(
        default_factory=lambda: [strings.MACROS_DIRECTORY], alias=strings.DBT_MACRO_PATHS_FIELD
    )
    test_paths: list[str] = Field(default_factory=lambda: [strings.TESTS_DIRECTORY], alias=strings.DBT_TEST_PATHS_FIELD)
    analysis_paths: list[str] = Field(
        default_factory=lambda: [strings.ANALYSES_DIRECTORY], alias=strings.DBT_ANALYSIS_PATHS_FIELD
    )
    seed_paths: list[str] = Field(default_factory=lambda: [strings.SEEDS_DIRECTORY], alias=strings.DBT_SEED_PATHS_FIELD)
    snapshot_paths: list[str] = Field(
        default_factory=lambda: [strings.SNAPSHOTS_DIRECTORY], alias=strings.DBT_SNAPSHOT_PATHS_FIELD
    )
    docs_paths: list[str] = Field(
        default_factory=lambda: [strings.MODELS_DIRECTORY], alias=strings.DBT_DOCS_PATHS_FIELD
    )
    asset_paths: list[str] = Field(
        default_factory=lambda: [strings.ASSETS_DIRECTORY], alias=strings.DBT_ASSET_PATHS_FIELD
    )
    target_path: str | None = Field(default=None, alias=strings.DBT_TARGET_PATH_FIELD)
    packages_install_path: str = Field(
        default=strings.DBT_PACKAGES_DIRECTORY, alias=strings.DBT_PACKAGES_INSTALL_PATH_FIELD
    )
    log_path: str = Field(default=strings.LOGS_DIRECTORY, alias=strings.DBT_LOG_PATH_FIELD)

    def resource_paths(self) -> list[str]:
        return [*self.model_paths, *self.macro_paths, *self.test_paths, *self.analysis_paths,
                *self.seed_paths, *self.snapshot_paths, *self.docs_paths, *self.asset_paths]


class NativePackage(NativeProjection):
    local: str | None = None


class NativePackages(NativeProjection):
    packages: list[NativePackage] = Field(default_factory=list)
