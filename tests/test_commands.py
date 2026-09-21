from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from dbt_metricflow_service.commands import build_dbt_command, build_metricflow_command
from dbt_metricflow_service.models import (
    DbtCommand,
    DbtJobRequest,
    MetricFlowCommand,
    MetricFlowJobRequest,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Provide a concrete cwd for command construction."""
    path = tmp_path / "sales"
    path.mkdir()
    return path


@pytest.fixture
def profiles_dir(tmp_path: Path) -> Path:
    """Provide a concrete profiles directory for dbt and MetricFlow."""
    path = tmp_path / "profiles"
    path.mkdir()
    return path


def test_build_dbt_command_uses_profile_without_shell(
    project_dir: Path, profiles_dir: Path
) -> None:
    """A build request must become literal argv elements, never shell text."""
    request = DbtJobRequest(
        project="sales", command=DbtCommand.BUILD, target="prod", select=["tag:daily"]
    )

    spec = build_dbt_command(request, project_dir, profiles_dir)

    assert spec.argv == (
        "dbt",
        "build",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--target",
        "prod",
        "--select",
        "tag:daily",
    )
    assert spec.cwd == project_dir
    assert spec.environment["DBT_PROFILES_DIR"] == str(profiles_dir)
    assert spec.environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] == "false"
    assert spec.write_operation is True


@pytest.mark.parametrize("command", list(DbtCommand))
def test_every_allowed_dbt_command_is_constructed(
    command: DbtCommand, project_dir: Path, profiles_dir: Path
) -> None:
    """Every enum member must map to the same literal dbt subcommand."""
    request = DbtJobRequest(project="sales", command=command)

    spec = build_dbt_command(request, project_dir, profiles_dir)

    assert spec.argv[:2] == ("dbt", command.value)
    assert spec.write_operation is True


def test_build_dbt_command_serializes_all_structured_options(
    project_dir: Path, profiles_dir: Path
) -> None:
    """Selectors and vars must retain argument boundaries and deterministic JSON."""
    request = DbtJobRequest(
        project="sales",
        command=DbtCommand.RUN,
        exclude=["tag:slow", "model_b"],
        variables={"region": "east", "days": 7},
        full_refresh=True,
    )

    spec = build_dbt_command(request, project_dir, profiles_dir)

    assert spec.argv[-6:] == (
        "--exclude",
        "tag:slow",
        "model_b",
        "--vars",
        '{"days":7,"region":"east"}',
        "--full-refresh",
    )


def test_unknown_dbt_command_is_rejected_by_request_model() -> None:
    """Raw or future CLI names must not bypass the command allowlist."""
    with pytest.raises(ValidationError):
        DbtJobRequest.model_validate({"project": "sales", "command": "run-operation"})


def test_build_metricflow_list_metrics(project_dir: Path, profiles_dir: Path) -> None:
    """Metric discovery must request all dimensions without accepting paths."""
    request = MetricFlowJobRequest(project="sales", command=MetricFlowCommand.LIST_METRICS)

    spec = build_metricflow_command(request, project_dir, profiles_dir)

    assert spec.argv == ("mf", "list", "metrics", "--show-all-dimensions")
    assert spec.cwd == project_dir
    assert spec.write_operation is False


def test_build_metricflow_list_dimensions_requires_metrics(
    project_dir: Path, profiles_dir: Path
) -> None:
    """Dimension discovery without a metric set must fail before spawning mf."""
    request = MetricFlowJobRequest(project="sales", command=MetricFlowCommand.LIST_DIMENSIONS)

    with pytest.raises(ValueError, match="metrics"):
        build_metricflow_command(request, project_dir, profiles_dir)


def test_build_metricflow_explain_serializes_query_options(
    project_dir: Path, profiles_dir: Path
) -> None:
    """Explain must compile SQL using the complete structured query request."""
    request = MetricFlowJobRequest(
        project="sales",
        command=MetricFlowCommand.EXPLAIN,
        metrics=["revenue", "orders"],
        group_by=["metric_time__month"],
        where=["{{ Dimension('order__status') }} = 'paid'"],
        order_by=["-revenue"],
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 2, 1, tzinfo=UTC),
        limit=10,
    )

    spec = build_metricflow_command(request, project_dir, profiles_dir)

    assert spec.argv == (
        "mf",
        "query",
        "--explain",
        "--quiet",
        "--metrics",
        "revenue,orders",
        "--group-by",
        "metric_time__month",
        "--where",
        "{{ Dimension('order__status') }} = 'paid'",
        "--order",
        "-revenue",
        "--start-time",
        "2026-01-01T00:00:00+00:00",
        "--end-time",
        "2026-02-01T00:00:00+00:00",
        "--limit",
        "10",
    )


def test_build_metricflow_query_requires_metrics(project_dir: Path, profiles_dir: Path) -> None:
    """A metric query without metrics or a saved query has no defined result."""
    request = MetricFlowJobRequest(project="sales", command=MetricFlowCommand.QUERY)

    with pytest.raises(ValueError, match="metrics"):
        build_metricflow_command(request, project_dir, profiles_dir)
