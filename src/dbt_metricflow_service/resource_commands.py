from __future__ import annotations

import dataclasses
import logging
import sys
from pathlib import Path
from typing import Literal

from dbt_metricflow_service import commands as base_commands
from dbt_metricflow_service.models import (
    CommandSpec,
    DbtJobRequest,
    MetricFlowJobRequest,
)
from dbt_metricflow_service.resource_protocol import WorkerRequest

logger = logging.getLogger(__name__)
WORKER_MODULE = "dbt_metricflow_service.resource_worker"


def build_dbt_command(
    request: DbtJobRequest,
    project_dir: Path,
    profiles_dir: Path,
) -> CommandSpec:
    """Delegate normal commands and adapt resource commands to the fixed worker."""
    base = base_commands.build_dbt_command(request, project_dir, profiles_dir)
    return _adapt_command("dbt", request, base)


def build_metricflow_command(
    request: MetricFlowJobRequest,
    project_dir: Path,
    profiles_dir: Path,
) -> CommandSpec:
    """Delegate normal commands and adapt resource commands to the fixed worker."""
    base = base_commands.build_metricflow_command(request, project_dir, profiles_dir)
    return _adapt_command("metricflow", request, base)


def _adapt_command(
    kind: Literal["dbt", "metricflow"],
    request: DbtJobRequest | MetricFlowJobRequest,
    base: CommandSpec,
) -> CommandSpec:
    if not request.resources:
        return base
    envelope = WorkerRequest(kind=kind, request=request)
    environment = dict(base.environment)
    environment["DBT_PROJECT_DIR"] = str(base.cwd)
    return dataclasses.replace(
        base,
        argv=(sys.executable, "-m", WORKER_MODULE),
        environment=environment,
        stdin_data=envelope.model_dump_json().encode("utf-8"),
        use_job_artifacts=True,
    )
