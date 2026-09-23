from __future__ import annotations

import logging

import uvicorn

from dbt_metricflow_service.api import create_app
from dbt_metricflow_service.jobs import JobRunner
from dbt_metricflow_service.projects import ProjectRegistry
from dbt_metricflow_service.settings import Settings

logger = logging.getLogger(__name__)

SERVER_HOST = "0.0.0.0"
SERVER_PORT = 8000


def run() -> None:
    """Build runtime dependencies from environment and start the HTTP server."""
    settings = Settings.from_environment()
    app = create_app(
        settings,
        ProjectRegistry(settings.projects_root),
        JobRunner(
            settings.command_timeout_seconds,
            settings.max_output_bytes,
            job_artifacts_root=settings.job_artifacts_root,
        ),
    )
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)


if __name__ == "__main__":
    run()
