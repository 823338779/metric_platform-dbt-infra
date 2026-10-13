"""Configuration boundaries validate fixed fields while preserving adapter extensions."""

import json

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.models.configuration import (
    DBT_PROFILES_ADAPTER,
    ExecutionCredential,
    ExecutionInstance,
)
from dbt_metricflow_service.settings import Settings


def test_environment_infrastructure_is_parsed_into_records(monkeypatch):
    monkeypatch.setenv("EXECUTION_INSTANCES", json.dumps({
        "warehouse": {"type": "postgres", "hosts": ["warehouse.local"], "port": 5432},
    }))
    monkeypatch.setenv("EXECUTION_CREDENTIALS", json.dumps({
        "warehouse": {"userEnv": "DBT_ENV_SECRET_USER", "passwordEnv": "DBT_ENV_SECRET_PASSWORD"},
    }))

    settings = Settings.from_environment()

    instance = settings.execution_instances["warehouse"]
    credential = settings.execution_credentials["warehouse"]
    assert isinstance(instance, ExecutionInstance)
    assert instance.hosts == ["warehouse.local"]
    assert isinstance(credential, ExecutionCredential)
    assert credential.user_env == "DBT_ENV_SECRET_USER"
    with pytest.raises(TypeError):
        instance["hosts"]


@pytest.mark.parametrize("instance", [
    {"type": "postgres", "hosts": "warehouse.local", "port": 5432},
    {"type": "postgres", "hosts": ["warehouse.local"], "port": "5432"},
    {"type": "postgres", "hosts": ["warehouse.local"], "unknown": True},
])
def test_invalid_infrastructure_is_rejected_at_environment_boundary(monkeypatch, instance):
    monkeypatch.setenv("EXECUTION_INSTANCES", json.dumps({"warehouse": instance}))
    with pytest.raises(ValidationError):
        Settings.from_environment()


def test_profile_extension_fields_round_trip_without_becoming_fixed_field_access():
    payload = {"project": {"target": "dev", "outputs": {"dev": {
        "type": "duckdb", "path": "data.duckdb", "extensions": ["httpfs"],
    }}, "custom_profile_option": True}}
    profiles = DBT_PROFILES_ADAPTER.validate_python(payload)
    assert profiles["project"].outputs["dev"].type == "duckdb"
    assert DBT_PROFILES_ADAPTER.dump_python(profiles) == payload
