from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger(__name__)
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
PROJECT_FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "dbt_project"
WORKER_MODULE = "dbt_metricflow_service.resource_worker"
ResourceProject = tuple[Path, Path, Path]


def make_resource_project(tmp_path: Path) -> ResourceProject:
    project = tmp_path / "project"
    profiles = tmp_path / "profiles"
    artifacts = tmp_path / "artifacts"
    shutil.copytree(PROJECT_FIXTURE, project)
    profiles.mkdir()
    artifacts.mkdir()
    database_path = json.dumps(str(tmp_path / "warehouse.duckdb"))
    profile = (
        "wrapper_fixture:\n  target: test\n  outputs:\n    test:\n"
        f"      type: duckdb\n      path: {database_path}\n      schema: main\n"
    )
    (profiles / "profiles.yml").write_text(profile, encoding="utf-8")
    return project, profiles, artifacts


def call_worker(
    project: ResourceProject,
    request: dict[str, object],
    kind: str = "dbt",
) -> subprocess.CompletedProcess[str]:
    project_dir, profiles_dir, artifact_dir = project
    environment = dict(os.environ)
    environment.update(
        {
            "DBT_PROJECT_DIR": str(project_dir),
            "DBT_PROFILES_DIR": str(profiles_dir),
            "JOB_ARTIFACT_DIR": str(artifact_dir),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "PYTHONUTF8": "1",
            "PYTHONPATH": str(REPOSITORY_ROOT / "src"),
        }
    )
    return subprocess.run(
        [sys.executable, "-m", WORKER_MODULE],
        input=json.dumps({"kind": kind, "request": request}),
        text=True,
        encoding="utf-8",
        capture_output=True,
        cwd=project_dir,
        env=environment,
        timeout=90,
        check=False,
    )
