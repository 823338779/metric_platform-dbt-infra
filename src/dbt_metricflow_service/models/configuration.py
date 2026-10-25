"""Validated service configuration and dbt profile boundaries."""

from typing import Annotated, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, TypeAdapter

from dbt_metricflow_service import constants as strings


class ConfigurationModel(BaseModel):
    model_config = ConfigDict(extra=strings.EXTRA_FORBID, strict=True, populate_by_name=True, hide_input_in_errors=True)


class ExecutionInstance(ConfigurationModel):
    type: str
    hosts: list[str] = Field(default_factory=list)
    port: int | None = None
    path: str | None = None


class ExecutionCredential(ConfigurationModel):
    user_env: str = Field(alias=strings.CREDENTIAL_USER_ENV_ALIAS)
    password_env: str = Field(alias=strings.CREDENTIAL_PASSWORD_ENV_ALIAS)


EXECUTION_INSTANCES_ADAPTER = TypeAdapter(dict[str, ExecutionInstance])
EXECUTION_CREDENTIALS_ADAPTER = TypeAdapter(dict[str, ExecutionCredential])

NonemptyName: TypeAlias = Annotated[str, Field(min_length=1)]


class DbtProfileOutput(BaseModel):
    # Adapter-specific options remain an open dbt extension boundary.
    model_config = ConfigDict(extra=strings.EXTRA_ALLOW, strict=True, hide_input_in_errors=True)
    type: NonemptyName


class DbtProfile(BaseModel):
    model_config = ConfigDict(extra=strings.EXTRA_ALLOW, strict=True, hide_input_in_errors=True)
    target: NonemptyName
    outputs: Annotated[dict[NonemptyName, DbtProfileOutput], Field(min_length=1)]


DbtProfiles: TypeAlias = Annotated[dict[NonemptyName, DbtProfile], Field(min_length=1)]
DBT_PROFILES_ADAPTER = TypeAdapter(DbtProfiles)
ConfigurationScalar: TypeAlias = StrictStr | StrictInt | None


class ServiceConfiguration(ConfigurationModel):
    SERVICE_HOST: ConfigurationScalar = None
    SERVICE_PORT: ConfigurationScalar = None
    DBT_PROFILES_DIR: ConfigurationScalar = None
    MAX_OUTPUT_BYTES: ConfigurationScalar = None
    SERVICE_DATABASE_URL: ConfigurationScalar = None
    SERVICE_TEMP_ROOT: ConfigurationScalar = None
    WORKER_CONCURRENCY: ConfigurationScalar = None
    JOB_LEASE_SECONDS: ConfigurationScalar = None
    JOB_HEARTBEAT_SECONDS: ConfigurationScalar = None
    SERVICE_CONFIG_VERSION: ConfigurationScalar = None
    SERVICE_TOOLCHAIN_VERSION: ConfigurationScalar = None
    MAX_RESULT_BYTES: ConfigurationScalar = None
    MAX_ARTIFACT_FILE_BYTES: ConfigurationScalar = None
    MAX_ARTIFACT_BYTES: ConfigurationScalar = None
    SERVICE_TOKEN: ConfigurationScalar = None
    DBT_PROFILES: DbtProfiles | None = None
    EXECUTION_INSTANCES: dict[str, ExecutionInstance] = Field(default_factory=dict)
    EXECUTION_CREDENTIALS: dict[str, ExecutionCredential] = Field(default_factory=dict, repr=False)


class ExecutionProfileOutput(ConfigurationModel):
    type: str
    schema_name: str = Field(alias=strings.DBT_SCHEMA_FIELD)
    threads: int
    path: str | None = None
    host: str | None = None
    port: int | None = None
    password: str | None = None
    username: str | None = None
    user: str | None = None
    catalog: str | None = None
    database: str | None = None
    dbname: str | None = None


class ExecutionProfile(ConfigurationModel):
    target: str
    outputs: dict[str, ExecutionProfileOutput]


class DbtProjectProfile(BaseModel):
    # dbt owns the rest of dbt_project.yml; only profile selection is read here.
    model_config = ConfigDict(extra=strings.EXTRA_IGNORE, strict=True, hide_input_in_errors=True)
    name: str | None = None
    profile: str | None = None
