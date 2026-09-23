from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path

import pytest
from resource_helpers import call_worker, make_resource_project

logger = logging.getLogger(__name__)


def _renamed_metric_yaml(project_dir: Path, metric_name: str) -> str:
    raw = (project_dir / "models" / "orders.yml").read_text(encoding="utf-8")
    changed = raw.replace(
        "    metrics:\n      - name: revenue",
        f"    metrics:\n      - name: {metric_name}",
    )
    assert changed != raw
    return changed


def test_metricflow_uses_request_manifest_instead_of_stale_project_target(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    stale = project[0] / "target"
    stale.mkdir()
    (stale / "semantic_manifest.json").write_text("invalid old artifact", encoding="utf-8")
    result = call_worker(
        project,
        {
            "project": "sales",
            "command": "list_metrics",
            "resources": {"orders.yml": _renamed_metric_yaml(project[0], "request_revenue")},
        },
        kind="metricflow",
    )
    assert result.returncode == 0, result.stderr
    assert "request_revenue" in result.stdout
    assert "• revenue:" not in result.stdout
    assert (stale / "semantic_manifest.json").read_text(encoding="utf-8") == "invalid old artifact"


@pytest.mark.parametrize("disabled_by", ["environment", "project_dotenv"])
def test_metricflow_forces_required_json_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    disabled_by: str,
) -> None:
    project = make_resource_project(tmp_path)
    if disabled_by == "environment":
        monkeypatch.setenv("DBT_WRITE_JSON", "false")
    else:
        (project[0] / ".env").write_text("DBT_WRITE_JSON=false\n", encoding="utf-8")
    result = call_worker(
        project,
        {
            "project": "sales",
            "command": "list_metrics",
            "resources": {"orders.yml": _renamed_metric_yaml(project[0], "forced_json_revenue")},
        },
        kind="metricflow",
    )

    assert result.returncode == 0, result.stderr
    assert "forced_json_revenue" in result.stdout


def test_metricflow_list_dimensions_uses_request_metric(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    result = call_worker(
        project,
        {
            "project": "sales",
            "command": "list_dimensions",
            "metrics": ["request_revenue"],
            "resources": {"orders.yml": _renamed_metric_yaml(project[0], "request_revenue")},
        },
        kind="metricflow",
    )
    assert result.returncode == 0, result.stderr
    assert "ordered_at" in result.stdout


def test_metricflow_explain_uses_request_metric(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    result = call_worker(
        project,
        {
            "project": "sales",
            "command": "explain",
            "metrics": ["request_revenue"],
            "resources": {"orders.yml": _renamed_metric_yaml(project[0], "request_revenue")},
        },
        kind="metricflow",
    )
    assert result.returncode == 0, result.stderr
    assert "SUM(revenue)" in result.stdout


def test_metricflow_query_executes_against_duckdb(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    environment = dict(os.environ)
    environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    subprocess.run(
        [
            "dbt", "build", "--project-dir", str(project[0]),
            "--profiles-dir", str(project[1]), "--no-partial-parse",
        ],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=environment,
        timeout=60,
    )
    result = call_worker(
        project,
        {
            "project": "sales",
            "command": "query",
            "metrics": ["request_revenue"],
            "resources": {"orders.yml": _renamed_metric_yaml(project[0], "request_revenue")},
        },
        kind="metricflow",
    )
    assert result.returncode == 0, result.stderr
    assert "10" in result.stdout
