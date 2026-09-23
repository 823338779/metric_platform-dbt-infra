from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from uuid import UUID

logger = logging.getLogger(__name__)
FINISHED_MARKER = ".finished"


def _is_link(path: Path) -> bool:
    return path.is_symlink() or (os.name == "nt" and os.path.isjunction(path))


def _validated_directory(root: Path, directory: Path) -> tuple[Path, Path]:
    if _is_link(root) or _is_link(directory):
        raise ValueError("artifact paths cannot be links")
    resolved_root = root.resolve(strict=True)
    resolved_directory = directory.resolve(strict=True)
    try:
        identifier = UUID(directory.name)
    except ValueError as error:
        raise ValueError("artifact directory name must be a UUID") from error
    if str(identifier) != directory.name or resolved_directory.parent != resolved_root:
        raise ValueError("artifact directory is outside the configured root")
    return resolved_root, resolved_directory


def create_job_directory(root: Path, job_id: UUID) -> Path:
    """Create one direct UUID child after rejecting linked roots."""
    if root.exists() and _is_link(root):
        raise ValueError("artifact root cannot be a link")
    root.mkdir(parents=True, exist_ok=True)
    resolved_root = root.resolve(strict=True)
    directory = resolved_root / str(job_id)
    directory.mkdir(exist_ok=False)
    _validated_directory(resolved_root, directory)
    return directory


def mark_job_finished(root: Path, directory: Path) -> None:
    """Mark a reaped worker directory as safe for startup recovery."""
    _, resolved_directory = _validated_directory(root, directory)
    (resolved_directory / FINISHED_MARKER).touch(exist_ok=False)


def remove_job_directory(root: Path, directory: Path) -> None:
    """Remove only a direct, non-linked UUID child of the configured root."""
    _, resolved_directory = _validated_directory(root, directory)
    shutil.rmtree(resolved_directory)


def recover_finished_directories(root: Path) -> None:
    """Delete only service-owned directories carrying the completed marker."""
    if root.exists() and _is_link(root):
        raise ValueError("artifact root cannot be a link")
    root.mkdir(parents=True, exist_ok=True)
    for directory in root.iterdir():
        if not directory.is_dir() or _is_link(directory):
            logger.warning("Leaving unrecognized artifact entry %s", directory.name)
            continue
        try:
            _validated_directory(root, directory)
        except ValueError:
            logger.warning("Leaving unrecognized artifact directory %s", directory.name)
            continue
        marker = directory / FINISHED_MARKER
        if marker.is_file() and not _is_link(marker):
            remove_job_directory(root, directory)
        else:
            logger.warning("Leaving unfinished artifact directory %s", directory.name)
