from __future__ import annotations

import json
import logging
from io import StringIO
from pathlib import Path

import pytest
from resource_helpers import call_worker, make_resource_project

from dbt_metricflow_service.resource_adapter import _BoundedTextCapture

logger = logging.getLogger(__name__)


def test_memory_yaml_precedes_invalid_disk_yaml(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    source = project[0] / "models" / "orders.yml"
    original = source.read_text(encoding="utf-8")
    source.write_text("version: [broken\n", encoding="utf-8")
    marker = "MEMORY_RESOURCE_COMMENT_57bd"
    raw = original + f"\n# {marker}\n"

    result = call_worker(
        project,
        {"project": "sales", "command": "parse", "resources": {"orders.yml": raw}},
    )

    assert result.returncode == 0, result.stderr
    assert (project[2] / "semantic_manifest.json").is_file()
    assert source.read_text(encoding="utf-8") == "version: [broken\n"
    assert not list(project[2].rglob("*.msgpack"))
    assert not list(project[2].rglob("*.yml"))
    for path in project[2].rglob("*"):
        if path.is_file():
            assert marker.encode() not in path.read_bytes()


def test_invalid_memory_yaml_does_not_fall_back(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    result = call_worker(
        project,
        {"project": "sales", "command": "parse", "resources": {"orders.yml": "version: [broken\n"}},
    )
    assert result.returncode != 0
    assert "resource_parse_error" in result.stderr
    assert "file=orders.yml" in result.stderr
    assert "line=2" in result.stderr
    assert not (project[2] / "semantic_manifest.json").exists()


def test_new_memory_yaml_is_added_to_first_model_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = make_resource_project(tmp_path)
    monkeypatch.setenv("PYTHONHASHSEED", "0")
    (project[0] / "functions").mkdir()
    project_config = project[0] / "dbt_project.yml"
    project_config.write_text(
        project_config.read_text(encoding="utf-8") + "\nfunction-paths: [functions]\n",
        encoding="utf-8",
    )
    raw = (
        "version: 2\nsources:\n  - name: virtual_source\n"
        "    schema: main\n    tables:\n      - name: virtual_table\n"
    )
    result = call_worker(
        project,
        {"project": "sales", "command": "parse", "resources": {"virtual.yml": raw}},
    )
    assert result.returncode == 0, result.stderr
    manifest = json.loads((project[2] / "manifest.json").read_text(encoding="utf-8"))
    source = manifest["sources"]["source.wrapper_fixture.virtual_source.virtual_table"]
    assert source["original_file_path"].replace("\\", "/") == "models/virtual.yml"
    assert not (project[0] / "models" / "virtual.yml").exists()


def test_virtual_memory_yaml_respects_first_model_path_ignore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = make_resource_project(tmp_path)
    monkeypatch.setenv("PYTHONHASHSEED", "0")
    (project[0] / "functions").mkdir()
    project_config = project[0] / "dbt_project.yml"
    project_config.write_text(
        project_config.read_text(encoding="utf-8") + "\nfunction-paths: [functions]\n",
        encoding="utf-8",
    )
    (project[0] / ".dbtignore").write_text("models/virtual.yml\n", encoding="utf-8")

    result = call_worker(
        project,
        {"project": "sales", "command": "parse", "resources": {"virtual.yml": "version: 2\n"}},
    )

    assert result.returncode != 0
    assert "resource_file_ignored" in result.stderr


def test_virtual_memory_yaml_requires_a_model_path(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    project_config = project[0] / "dbt_project.yml"
    project_config.write_text(
        project_config.read_text(encoding="utf-8").replace(
            "model-paths: [models]",
            "model-paths: []",
        ),
        encoding="utf-8",
    )

    result = call_worker(
        project,
        {"project": "sales", "command": "parse", "resources": {"virtual.yml": "version: 2\n"}},
    )

    assert result.returncode != 0
    assert "resource_model_path_missing" in result.stderr


def test_failed_run_preserves_sanitized_execution_diagnostics(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    (project[0] / "models" / "orders.sql").write_text(
        "select * from missing_resource_table_71c9",
        encoding="utf-8",
    )
    raw = (project[0] / "models" / "orders.yml").read_text(encoding="utf-8")

    result = call_worker(
        project,
        {"project": "sales", "command": "run", "resources": {"orders.yml": raw}},
    )

    assert result.returncode != 0
    assert "resource_execution_error" in result.stderr
    assert "missing_resource_table_71c9" in result.stdout


def test_bounded_capture_retains_only_configured_tail() -> None:
    capture = _BoundedTextCapture(8)
    capture.write("prefix-0123456789")
    target = StringIO()

    capture.replay(target)

    assert capture.truncated is True
    assert target.getvalue().endswith("23456789")
    assert len(target.getvalue().encode("utf-8")) > 8
    assert len(capture.retained_bytes) == 8


def test_ambiguous_disk_basename_is_rejected(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    duplicate = project[0] / "models" / "nested" / "orders.yml"
    duplicate.parent.mkdir()
    duplicate.write_text("version: 2\n", encoding="utf-8")
    result = call_worker(
        project,
        {"project": "sales", "command": "parse", "resources": {"orders.yml": "version: 2\n"}},
    )
    assert result.returncode != 0
    assert "resource_name_ambiguous" in result.stderr


def test_worker_rejects_untrusted_paths_and_oversize_input(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
    invalid = call_worker(
        project,
        {
            "project": "sales",
            "command": "parse",
            "resources": {"a.yml": "{}"},
            "artifact_dir": str(tmp_path / "elsewhere"),
        },
    )
    assert invalid.returncode == 2
    assert invalid.stderr.strip() == "invalid_worker_request"
