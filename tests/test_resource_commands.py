from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest

from dbt_metricflow_service import commands
from dbt_metricflow_service.models import DbtJobRequest, MetricFlowJobRequest
from dbt_metricflow_service.resource_commands import (
    build_dbt_command,
    build_metricflow_command,
)

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("raw", [{}, {"a.yml": " \n"}])
def test_blank_resources_return_original_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    raw: dict[str, str],
) -> None:
    profiles = tmp_path / "profiles"
    request = DbtJobRequest(project="sales", command="parse", resources=raw)
    base = commands.build_dbt_command(request, tmp_path, profiles)
    calls = 0

    def stub(*args: object) -> object:
        nonlocal calls
        calls += 1
        return base

    monkeypatch.setattr("dbt_metricflow_service.resource_commands.base_commands.build_dbt_command", stub)
    actual = build_dbt_command(request, tmp_path, profiles)
    assert actual is base
    assert calls == 1


def test_dbt_resource_command_keeps_yaml_only_in_stdin(tmp_path: Path) -> None:
    raw = "version: 2\n# resource-only-marker\n"
    profiles = tmp_path / "profiles"
    request = DbtJobRequest(project="sales", command="parse", resources={"a.yml": raw})
    spec = build_dbt_command(request, tmp_path, profiles)
    assert spec.argv == (sys.executable, "-m", "dbt_metricflow_service.resource_worker")
    assert spec.stdin_data is not None
    assert json.loads(spec.stdin_data)["request"]["resources"] == {"a.yml": raw}
    assert raw not in " ".join(spec.argv)
    assert raw not in " ".join(spec.environment.values())
    assert spec.cwd == tmp_path
    assert spec.write_operation is True
    assert spec.use_job_artifacts is True


def test_metricflow_resource_command_keeps_read_only_lock_semantics(tmp_path: Path) -> None:
    profiles = tmp_path / "profiles"
    request = MetricFlowJobRequest(
        project="sales",
        command="list_metrics",
        resources={"a.yml": "version: 2\n"},
    )
    spec = build_metricflow_command(request, tmp_path, profiles)
    assert spec.argv == (sys.executable, "-m", "dbt_metricflow_service.resource_worker")
    assert spec.stdin_data is not None
    assert json.loads(spec.stdin_data)["kind"] == "metricflow"
    assert spec.write_operation is False
    assert spec.use_job_artifacts is True
