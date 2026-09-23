from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pydantic import ValidationError

from dbt_metricflow_service.models import DbtJobRequest, MetricFlowJobRequest
from dbt_metricflow_service.resource_adapter import (
    IncompatibleRuntimeError,
    ResourceAdapterError,
    execute_dbt,
    execute_metricflow,
)
from dbt_metricflow_service.resource_protocol import WorkerRequest
from dbt_metricflow_service.resources import MAX_WORKER_INPUT_BYTES

logger = logging.getLogger(__name__)


def _environment_path(name: str) -> Path:
    value = os.environ.get(name)
    if value is None:
        raise ValueError(name)
    path = Path(value)
    if not path.is_absolute() or not path.is_dir():
        raise ValueError(name)
    return path.resolve()


def main() -> int:
    """Validate one bounded stdin envelope and execute its fixed backend."""
    payload = sys.stdin.buffer.read(MAX_WORKER_INPUT_BYTES + 1)
    if len(payload) > MAX_WORKER_INPUT_BYTES:
        print("invalid_worker_request", file=sys.stderr)
        return 2
    try:
        envelope = WorkerRequest.model_validate_json(payload)
        project_dir = _environment_path("DBT_PROJECT_DIR")
        profiles_dir = _environment_path("DBT_PROFILES_DIR")
        artifact_dir = _environment_path("JOB_ARTIFACT_DIR")
    except (ValidationError, ValueError):
        print("invalid_worker_request", file=sys.stderr)
        return 2
    try:
        if envelope.kind == "dbt" and isinstance(envelope.request, DbtJobRequest):
            return execute_dbt(envelope.request, project_dir, profiles_dir, artifact_dir)
        if envelope.kind == "metricflow" and isinstance(envelope.request, MetricFlowJobRequest):
            return execute_metricflow(envelope.request, project_dir, profiles_dir, artifact_dir)
        print("invalid_worker_request", file=sys.stderr)
        return 2
    except (IncompatibleRuntimeError, ResourceAdapterError) as error:
        print(str(error), file=sys.stderr)
        return 1
    except Exception as error:
        logger.info("Resource worker failed with %s", type(error).__name__)
        print("resource_execution_error", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
