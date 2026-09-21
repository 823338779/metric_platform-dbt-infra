from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pytest

from dbt_metricflow_service.projects import (
    InvalidManifestError,
    InvalidProjectError,
    ManifestNotFoundError,
    ProjectNotFoundError,
    ProjectRegistry,
)

logger = logging.getLogger(__name__)


@pytest.fixture
def projects_root(tmp_path: Path) -> Path:
    """Provide an isolated root that mirrors the production mount."""
    root = tmp_path / "projects"
    root.mkdir()
    return root


@pytest.fixture
def registry(projects_root: Path) -> ProjectRegistry:
    """Resolve projects only beneath the isolated root."""
    return ProjectRegistry(projects_root)


def test_resolve_returns_project_containing_dbt_project_file(
    registry: ProjectRegistry, projects_root: Path
) -> None:
    """A valid project ID must resolve to its canonical project directory."""
    project_dir = projects_root / "sales"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: sales\n", encoding="utf-8")

    assert registry.resolve("sales") == project_dir.resolve()


@pytest.mark.parametrize("project", ["../secret", "a/b", "a\\b", "C:\\secret", ".", ".."])
def test_resolve_rejects_unsafe_project_names(registry: ProjectRegistry, project: str) -> None:
    """Path syntax in a project ID must never reach filesystem resolution."""
    with pytest.raises(InvalidProjectError):
        registry.resolve(project)


def test_resolve_rejects_symlink_escape(
    registry: ProjectRegistry, projects_root: Path, tmp_path: Path
) -> None:
    """A symlink must not escape the configured project root."""
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "dbt_project.yml").write_text("name: outside\n", encoding="utf-8")
    link = projects_root / "escape"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"Symlink creation is unavailable: {error}")

    with pytest.raises(InvalidProjectError):
        registry.resolve("escape")


def test_resolve_rejects_missing_project(registry: ProjectRegistry) -> None:
    """A valid-looking ID with no directory must be reported as missing."""
    with pytest.raises(ProjectNotFoundError):
        registry.resolve("missing")


def test_resolve_requires_dbt_project_file(registry: ProjectRegistry, projects_root: Path) -> None:
    """An arbitrary directory must not be accepted as a dbt project."""
    (projects_root / "empty").mkdir()

    with pytest.raises(ProjectNotFoundError):
        registry.resolve("empty")


def test_adapter_type_reads_manifest_metadata(registry: ProjectRegistry, tmp_path: Path) -> None:
    """MetricFlow routing must use the adapter recorded by dbt parse."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(
        json.dumps({"metadata": {"adapter_type": "starrocks"}}), encoding="utf-8"
    )

    assert registry.adapter_type(tmp_path) == "starrocks"


def test_adapter_type_rejects_missing_manifest(registry: ProjectRegistry, tmp_path: Path) -> None:
    """MetricFlow cannot run before dbt generated its manifest."""
    with pytest.raises(ManifestNotFoundError):
        registry.adapter_type(tmp_path)


@pytest.mark.parametrize("contents", ["not-json", "{}", '{"metadata": null}'])
def test_adapter_type_rejects_invalid_manifest(
    registry: ProjectRegistry, tmp_path: Path, contents: str
) -> None:
    """Malformed or incomplete dbt metadata must be a stable domain error."""
    target = tmp_path / "target"
    target.mkdir()
    (target / "manifest.json").write_text(contents, encoding="utf-8")

    with pytest.raises(InvalidManifestError):
        registry.adapter_type(tmp_path)


def test_resolve_handles_case_normalized_root(registry: ProjectRegistry, projects_root: Path) -> None:
    """Canonical containment must remain valid on case-insensitive filesystems."""
    project_dir = projects_root / "CaseProject"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").write_text("name: case_project\n", encoding="utf-8")

    expected = Path(os.path.realpath(project_dir))
    assert registry.resolve("CaseProject") == expected
