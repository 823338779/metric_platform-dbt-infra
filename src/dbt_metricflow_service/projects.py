from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
PROJECT_FILE_NAME = "dbt_project.yml"
MANIFEST_RELATIVE_PATH = Path("target/manifest.json")


class ProjectError(ValueError):
    """Base class for stable project lookup failures."""


class InvalidProjectError(ProjectError):
    """The supplied project ID is unsafe or escapes the configured root."""


class ProjectNotFoundError(ProjectError):
    """The supplied project ID does not name a dbt project."""


class ManifestNotFoundError(ProjectError):
    """dbt has not generated target/manifest.json for the project."""


class InvalidManifestError(ProjectError):
    """The generated manifest lacks readable adapter metadata."""


class ProjectRegistry:
    """Resolve project IDs within one configured filesystem root."""

    def __init__(self, projects_root: Path) -> None:
        self._projects_root = projects_root.resolve()

    def resolve(self, project: str) -> Path:
        """Return the canonical dbt project directory without allowing escape."""
        if not PROJECT_NAME_PATTERN.fullmatch(project) or project in {".", ".."}:
            raise InvalidProjectError(project)

        # Resolve symlinks before checking containment so an in-root link cannot escape.
        try:
            candidate = (self._projects_root / project).resolve(strict=True)
        except FileNotFoundError as error:
            raise ProjectNotFoundError(project) from error
        try:
            candidate.relative_to(self._projects_root)
        except ValueError as error:
            raise InvalidProjectError(project) from error
        if not (candidate / PROJECT_FILE_NAME).is_file():
            raise ProjectNotFoundError(project)
        return candidate

    def adapter_type(self, project_dir: Path) -> str:
        """Read the adapter type recorded by dbt in target/manifest.json."""
        manifest_path = project_dir / MANIFEST_RELATIVE_PATH
        try:
            document = json.loads(manifest_path.read_text(encoding="utf-8"))
            adapter_type = document["metadata"]["adapter_type"]
            if type(adapter_type) is not str or not adapter_type:
                raise TypeError("adapter_type must be a non-empty string")
            return adapter_type
        except FileNotFoundError as error:
            raise ManifestNotFoundError(manifest_path) from error
        except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError) as error:
            raise InvalidManifestError(manifest_path) from error
