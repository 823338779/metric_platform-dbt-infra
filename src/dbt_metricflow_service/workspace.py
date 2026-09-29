from __future__ import annotations

import shutil
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

from dbt_metricflow_service.job_artifacts import _is_link
from dbt_metricflow_service.storage.artifacts import ArtifactStore


@contextmanager
def attempt_workspace(temp_root: Path, job_id: str, attempt_id: str):
    """创建安全的 attempt 目录，调用方须在退出前确认本地子进程停止。"""
    # UUID 规范化防止调用方将路径片段作为任务标识传入。
    job_name, attempt_name = str(UUID(str(job_id))), str(UUID(str(attempt_id)))
    root = Path(temp_root)
    if any(_is_link(path) for path in (root, *root.parents)):
        raise ValueError("workspace root cannot contain links")
    root.mkdir(parents=True, exist_ok=True)
    root = root.resolve(strict=True)
    parent = root / job_name
    if _is_link(parent):
        raise ValueError("workspace job directory cannot be linked")
    parent.mkdir(exist_ok=True)
    directory = parent / attempt_name
    directory.mkdir(exist_ok=False)
    try:
        yield directory
    finally:
        # 清理前再次核对边界；绝不递归删除被替换为链接的目录。
        if (_is_link(parent) or _is_link(directory)
                or directory.resolve().parent != root / job_name):
            raise ValueError("workspace path changed during execution")
        shutil.rmtree(directory)


@contextmanager
def materialized_workspace(store: ArtifactStore, set_id: str, temp_root: Path, job_id: str, attempt_id: str):
    """在受控 attempt 目录中还原一个已封存的项目版本。"""
    with attempt_workspace(temp_root, job_id, attempt_id) as directory:
        store.materialize(set_id, directory)
        yield directory
