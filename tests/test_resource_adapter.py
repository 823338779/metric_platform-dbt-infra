from __future__ import annotations

import json
import logging
from pathlib import Path

from resource_helpers import call_worker, make_resource_project

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
    assert not (project[2] / "semantic_manifest.json").exists()


def test_new_memory_yaml_is_added_to_first_model_path(tmp_path: Path) -> None:
    project = make_resource_project(tmp_path)
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
    assert "source.wrapper_fixture.virtual_source.virtual_table" in manifest["sources"]
    assert not (project[0] / "models" / "virtual.yml").exists()


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
