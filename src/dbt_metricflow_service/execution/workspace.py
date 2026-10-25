from __future__ import annotations

import errno
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Final
from uuid import UUID

from dbt_metricflow_service.execution.artifacts import _is_link
from dbt_metricflow_service.storage.artifacts import ArtifactStore

LINKED_WORKSPACE_ROOT_FORBIDDEN: Final[str] = "workspace root cannot contain links"
LINKED_JOB_DIRECTORY_FORBIDDEN: Final[str] = "workspace job directory cannot be linked"
WORKSPACE_PATH_CHANGED: Final[str] = "workspace path changed during execution"


@contextmanager
def attempt_workspace(temp_root: Path, job_id: str, attempt_id: str) -> Iterator[Path]:
    """创建安全的 attempt 目录，调用方须在退出前等待执行线程停止。"""
    # UUID 规范化防止调用方将路径片段作为任务标识传入。
    job_name, attempt_name = str(UUID(str(job_id))), str(UUID(str(attempt_id)))
    root = Path(temp_root)
    if any(_is_link(path) for path in (root, *root.parents)):
        raise ValueError(LINKED_WORKSPACE_ROOT_FORBIDDEN)
    root.mkdir(parents=True, exist_ok=True)
    root = root.resolve(strict=True)
    parent = root / job_name
    if _is_link(parent):
        raise ValueError(LINKED_JOB_DIRECTORY_FORBIDDEN)
    parent.mkdir(exist_ok=True)
    directory = parent / attempt_name
    directory.mkdir(exist_ok=False)
    try:
        yield directory
    finally:
        # 清理前再次核对边界；绝不递归删除被替换为链接的目录。
        if (_is_link(parent) or _is_link(directory)
                or directory.resolve().parent != root / job_name):
            raise ValueError(WORKSPACE_PATH_CHANGED)
        shutil.rmtree(directory)
        # 仅删除已空的任务目录；并发 attempt 仍在使用时保留父目录。
        try:
            parent.rmdir()
        except OSError as error:
            if error.errno not in {errno.ENOTEMPTY, errno.EEXIST, errno.ENOENT}:
                raise


@contextmanager
def materialized_workspace(
    store: ArtifactStore, set_id: str, temp_root: Path, job_id: str, attempt_id: str
) -> Iterator[Path]:
    """在受控 attempt 目录中还原一个已封存的项目版本。"""
    with attempt_workspace(temp_root, job_id, attempt_id) as directory:
        store.materialize(set_id, directory)
        yield directory
