from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from dbt_metricflow_service.models import (
    CommandSpec,
    DbtJobRequest,
    MetricFlowCommand,
    MetricFlowJobRequest,
)

logger = logging.getLogger(__name__)

DBT_EXECUTABLE = "dbt"
METRICFLOW_EXECUTABLE = "mf"
DBT_PROJECT_DIR_OPTION = "--project-dir"
DBT_PROFILES_DIR_OPTION = "--profiles-dir"
METRICFLOW_BASE_ARGUMENTS = {
    MetricFlowCommand.LIST_METRICS: (METRICFLOW_EXECUTABLE, "list", "metrics", "--show-all-dimensions"),
    MetricFlowCommand.LIST_DIMENSIONS: (METRICFLOW_EXECUTABLE, "list", "dimensions"),
    MetricFlowCommand.EXPLAIN: (METRICFLOW_EXECUTABLE, "query", "--explain", "--quiet"),
    MetricFlowCommand.QUERY: (METRICFLOW_EXECUTABLE, "query", "--quiet"),
}


def _command_environment(profiles_dir: Path) -> dict[str, str]:
    """Return a child environment with dbt profiles and telemetry fixed."""
    environment = dict(os.environ)
    environment["DBT_PROFILES_DIR"] = str(profiles_dir)
    environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    environment["PYTHONUTF8"] = "1"
    return environment


def build_dbt_command(
    request: DbtJobRequest, project_dir: Path, profiles_dir: Path
) -> CommandSpec:
    """Build a literal dbt argv tuple from a validated request."""
    arguments = [
        DBT_EXECUTABLE,
        request.command.value,
        DBT_PROJECT_DIR_OPTION,
        str(project_dir),
        DBT_PROFILES_DIR_OPTION,
        str(profiles_dir),
    ]

    # Add each structured option as an independent argv element.
    if request.target is not None:
        arguments.extend(("--target", request.target))
    if request.select:
        arguments.append("--select")
        arguments.extend(request.select)
    if request.exclude:
        arguments.append("--exclude")
        arguments.extend(request.exclude)
    if request.variables:
        variables = json.dumps(request.variables, sort_keys=True, separators=(",", ":"))
        arguments.extend(("--vars", variables))
    if request.full_refresh:
        arguments.append("--full-refresh")

    return CommandSpec(
        argv=tuple(arguments),
        cwd=project_dir,
        environment=_command_environment(profiles_dir),
        write_operation=True,
    )


def build_metricflow_command(
    request: MetricFlowJobRequest, project_dir: Path, profiles_dir: Path
) -> CommandSpec:
    """Build a literal MetricFlow argv tuple from a validated request."""
    arguments = list(METRICFLOW_BASE_ARGUMENTS[request.command])

    if request.command is MetricFlowCommand.LIST_METRICS:
        return _metricflow_spec(arguments, project_dir, profiles_dir)
    if request.command is MetricFlowCommand.LIST_DIMENSIONS:
        if not request.metrics:
            raise ValueError("metrics are required for list_dimensions")
        arguments.extend(("--metrics", ",".join(request.metrics)))
        return _metricflow_spec(arguments, project_dir, profiles_dir)
    if not request.metrics:
        raise ValueError("metrics are required for MetricFlow queries")

    # Query and explain share the same public CLI options.
    arguments.extend(("--metrics", ",".join(request.metrics)))
    if request.group_by:
        arguments.extend(("--group-by", ",".join(request.group_by)))
    for where_constraint in request.where:
        arguments.extend(("--where", where_constraint))
    if request.order_by:
        arguments.extend(("--order", ",".join(request.order_by)))
    if request.start_time is not None:
        arguments.extend(("--start-time", request.start_time.isoformat()))
    if request.end_time is not None:
        arguments.extend(("--end-time", request.end_time.isoformat()))
    if request.limit is not None:
        arguments.extend(("--limit", str(request.limit)))
    return _metricflow_spec(arguments, project_dir, profiles_dir)


def _metricflow_spec(
    arguments: list[str], project_dir: Path, profiles_dir: Path
) -> CommandSpec:
    """Create the common read-only MetricFlow command specification."""
    return CommandSpec(
        argv=tuple(arguments),
        cwd=project_dir,
        environment=_command_environment(profiles_dir),
        write_operation=False,
    )
