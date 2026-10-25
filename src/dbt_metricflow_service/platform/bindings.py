from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import yaml

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.models.contracts import JsonObject

from ..models.native import NativePackages, NativeProject

GIT_READ_FAILED_MESSAGE: Final[str] = 'Git 固定版本读取失败'
GIT_OUTPUT_LIMIT_MESSAGE: Final[str] = 'Git 输出超限'
INVALID_PROJECT_PATH_MESSAGE: Final[str] = 'dbt 项目路径无效'
INVALID_REVISION_DIGEST_MESSAGE: Final[str] = '固定版本或项目摘要无效'
INVALID_REVISION_MESSAGE: Final[str] = '固定版本无效'
INVALID_PROJECT_BINDING_MESSAGE: Final[str] = '平台项目绑定无效'
INVALID_FETCHED_REVISION_MESSAGE: Final[str] = 'Git 版本无效'
REQUESTED_COMMIT_MISMATCH_MESSAGE: Final[str] = 'Git did not return the requested commit'
INVALID_PROJECT_INPUT_MESSAGE: Final[str] = 'dbt 项目输入无效'
PROJECT_FILE_MISSING_MESSAGE: Final[str] = 'dbt_project.yml 缺失'
PROJECT_DIGEST_MISMATCH_MESSAGE: Final[str] = 'dbt 项目摘要不匹配'

PROJECT_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
SHA_PATTERN = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
ROOT_INPUTS = frozenset(
    {
        strings.DBT_PROJECT_FILE,
        strings.PACKAGES_FILE,
        strings.PACKAGE_LOCK_FILE,
        strings.DEPENDENCIES_FILE,
        strings.SELECTORS_FILE,
    }
)
INPUT_DIRECTORIES = frozenset(
    {
        strings.MODELS_DIRECTORY,
        strings.MACROS_DIRECTORY,
        strings.TESTS_DIRECTORY,
        strings.ANALYSES_DIRECTORY,
        strings.SEEDS_DIRECTORY,
        strings.SNAPSHOTS_DIRECTORY,
    }
)
GIT_OPTIONS = (
    strings.GIT_CONFIG_OPTION,
    strings.GIT_DISABLE_EXT_PROTOCOL,
    strings.GIT_CONFIG_OPTION,
    strings.GIT_DISABLE_HOOKS,
    strings.GIT_CONFIG_OPTION,
    strings.GIT_DISABLE_FSMONITOR,
)
MAX_TREE_BYTES = 8 * 1024 * 1024
GIT_FETCH = strings.GIT_FETCH_COMMAND
GIT_NO_TAGS = strings.GIT_NO_TAGS_OPTION
GIT_SEPARATOR = "--"


@dataclass(frozen=True, slots=True)
class ProjectBinding:
    """服务配置拥有的 Git 和 profile 绑定。"""

    project_id: str
    remote: str
    project_subdir: str
    profile_binding_id: str
    schema_name: str | None = None


def binding_digest(binding: JsonObject) -> str:
    return hashlib.sha256(json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _git(directory: Path, *args: str, limit: int = MAX_TREE_BYTES) -> bytes:
    command = [strings.GIT_EXECUTABLE, *GIT_OPTIONS, strings.GIT_DIRECTORY_OPTION, str(directory), *args]
    environment = {**os.environ, strings.GIT_TERMINAL_PROMPT: strings.GIT_PROMPT_DISABLED}
    try:
        process = subprocess.run(command, capture_output=True, timeout=60, env=environment, check=True)
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError(GIT_READ_FAILED_MESSAGE) from error
    if len(process.stdout) > limit:
        raise ValueError(GIT_OUTPUT_LIMIT_MESSAGE)
    return process.stdout


def _prefix(value: str) -> str:
    if not value or value.isspace() or value.startswith("/") or value.endswith("/") or "\\" in value or ":" in value:
        raise ValueError(INVALID_PROJECT_PATH_MESSAGE)
    if value == ".":
        return ""
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError(INVALID_PROJECT_PATH_MESSAGE)
    return f"{value}/"


def _included(path: str) -> bool:
    return path in ROOT_INPUTS or path.split("/", 1)[0] in INPUT_DIRECTORIES and "/" in path


def resolve_revision(binding: ProjectBinding, commit_sha: str, expected_digest: str, work_root: Path) -> Path:
    """从受控 remote 读取固定 Git 树，核对摘要后安全写入任务目录。"""
    if not SHA_PATTERN.fullmatch(commit_sha) or not DIGEST_PATTERN.fullmatch(expected_digest):
        raise ValueError(INVALID_REVISION_DIGEST_MESSAGE)
    return _resolve_revision(binding, commit_sha, expected_digest, work_root)[0]


def resolve_commit(binding: ProjectBinding, commit_sha: str, work_root: Path) -> tuple[Path, str]:
    """读取受控 remote 的精确提交并计算源码摘要。"""
    if not SHA_PATTERN.fullmatch(commit_sha):
        raise ValueError(INVALID_REVISION_MESSAGE)
    return _resolve_revision(binding, commit_sha, None, work_root)


def _resolve_revision(
    binding: ProjectBinding, commit_sha: str, expected_digest: str | None, work_root: Path
) -> tuple[Path, str]:
    prefix = _prefix(binding.project_subdir)
    if not binding.remote or not binding.project_id or not binding.profile_binding_id:
        raise ValueError(INVALID_PROJECT_BINDING_MESSAGE)
    root = work_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=strings.GIT_TEMP_PREFIX, dir=root) as cache_name:
        cache = Path(cache_name)
        _git(cache, strings.GIT_INIT_COMMAND, strings.GIT_BARE_OPTION)
        _git(cache, GIT_FETCH, GIT_NO_TAGS, GIT_SEPARATOR, binding.remote, commit_sha)
        fetched = (
            _git(cache, strings.GIT_REV_PARSE_COMMAND, strings.GIT_VERIFY_OPTION, strings.GIT_FETCH_HEAD)
            .decode()
            .strip()
        )
        if not SHA_PATTERN.fullmatch(fetched):
            raise ValueError(INVALID_FETCHED_REVISION_MESSAGE)
        if fetched != commit_sha:
            raise ValueError(REQUESTED_COMMIT_MISMATCH_MESSAGE)
        raw = _git(
            cache,
            strings.GIT_LS_TREE_COMMAND,
            strings.GIT_RECURSIVE_OPTION,
            strings.GIT_NULL_SEPARATOR_OPTION,
            commit_sha,
            "--",
            prefix[:-1] if prefix else ".",
        )
        entries: dict[str, tuple[str, str]] = {}
        for item in raw.split(b"\0"):
            if not item:
                continue
            try:
                metadata, raw_path = item.decode(strings.UTF8).split("\t", 1)
                mode, kind, blob = metadata.split(" ")
            except ValueError as error:
                raise ValueError(INVALID_PROJECT_INPUT_MESSAGE) from error
            if (
                kind != strings.GIT_BLOB_TYPE
                or mode not in {strings.GIT_FILE_MODE, strings.GIT_EXECUTABLE_FILE_MODE}
                or not SHA_PATTERN.fullmatch(blob)
            ):
                raise ValueError(INVALID_PROJECT_INPUT_MESSAGE)
            if prefix and not raw_path.startswith(prefix):
                continue
            relative = raw_path[len(prefix):]
            if any(part in {"", ".", ".."} for part in relative.split("/")):
                raise ValueError(INVALID_PROJECT_INPUT_MESSAGE)
            entries[relative] = (mode, blob)
        if strings.DBT_PROJECT_FILE not in entries:
            raise ValueError(PROJECT_FILE_MISSING_MESSAGE)
        project_yaml = NativeProject.model_validate(
            yaml.safe_load(
                _git(
                    cache,
                    strings.GIT_CAT_FILE_COMMAND,
                    strings.GIT_PRETTY_PRINT_OPTION,
                    entries[strings.DBT_PROJECT_FILE][1],
                )
            )
        )
        folders = [_prefix(value) for value in project_yaml.resource_paths()]
        # 单次 build 不安装依赖；工程中已提交的安装目录必须随固定源码还原。
        folders.append(_prefix(project_yaml.packages_install_path))
        # 本地依赖同属固定 Git 工程，允许受控根内路径，不能静默漏掉非模型目录。
        for filename in (strings.PACKAGES_FILE, strings.DEPENDENCIES_FILE):
            if filename not in entries:
                continue
            dependencies = NativePackages.model_validate(
                yaml.safe_load(
                    _git(cache, strings.GIT_CAT_FILE_COMMAND, strings.GIT_PRETTY_PRINT_OPTION, entries[filename][1])
                )
                or {}
            )
            for package in dependencies.packages:
                if package.local is not None:
                    folders.append(_prefix(package.local))
        entries = {path: item for path, item in entries.items()
                   if path in ROOT_INPUTS or any(path.startswith(folder) for folder in folders)}
        digest = hashlib.sha256()
        for relative, (mode, blob) in sorted(entries.items()):
            digest.update(relative.encode(strings.UTF8) + b"\0" + f"{mode} {blob}".encode(strings.UTF8) + b"\0")
        if expected_digest is not None and digest.hexdigest() != expected_digest:
            raise ValueError(PROJECT_DIGEST_MISMATCH_MESSAGE)
        # Git 允许跨平台不安全的路径；在任何落盘之前执行 artifact 相同边界检查。
        from ..storage.artifacts import _check_paths

        _check_paths(list(entries))
        destination = Path(tempfile.mkdtemp(prefix=strings.RUN_TEMP_PREFIX, dir=root))
        try:
            for relative, (mode, blob) in entries.items():
                target = destination.joinpath(*relative.split("/"))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(_git(cache, strings.GIT_CAT_FILE_COMMAND, strings.GIT_PRETTY_PRINT_OPTION, blob))
                if mode == strings.GIT_EXECUTABLE_FILE_MODE:
                    target.chmod(0o755)
            return destination, digest.hexdigest()
        except Exception:
            shutil.rmtree(destination)
            raise
