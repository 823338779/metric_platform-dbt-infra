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

import yaml

from dbt_metricflow_service.platform.namespace import validate_schema_name

SHA_PATTERN = re.compile(r"[0-9a-f]{40}(?:[0-9a-f]{24})?\Z")
DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
ROOT_INPUTS = frozenset({"dbt_project.yml", "packages.yml", "package-lock.yml", "dependencies.yml", "selectors.yml"})
INPUT_DIRECTORIES = frozenset({"models", "macros", "tests", "analyses", "seeds", "snapshots"})
RESOURCE_PATHS = {
    "model-paths": "models", "macro-paths": "macros", "test-paths": "tests",
    "analysis-paths": "analyses", "seed-paths": "seeds", "snapshot-paths": "snapshots",
}
GIT_OPTIONS = ("-c", "protocol.ext.allow=never", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false")
MAX_TREE_BYTES = 8 * 1024 * 1024
GIT_INIT = "init"
GIT_BARE = "--bare"
GIT_FETCH = "fetch"
GIT_MAIN = "refs/heads/main"
GIT_HEAD = "FETCH_HEAD"
GIT_REV_PARSE = "rev-parse"
GIT_VERIFY = "--verify"
GIT_NO_TAGS = "--no-tags"
GIT_SEPARATOR = "--"
GIT_TREE = "ls-tree"
GIT_RECURSIVE = "-r"
GIT_ZERO = "-z"
GIT_BLOB = "blob"
GIT_MODES = frozenset({"100644", "100755"})
GIT_CACHE_PREFIX = "observe-"
UTF8 = "utf-8"
ROOT_PATH = "."
HEADS_PREFIX = "refs/heads/"
REF_FORBIDDEN = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]|\.\.|@\{")
REF_SEPARATOR = "/"
REF_DOT = "."
REF_LOCK = ".lock"






@dataclass(frozen=True, slots=True)
class ProjectBinding:
    """服务配置拥有的 Git 和 profile 绑定。"""

    project_id: str
    remote: str
    project_subdir: str
    profile_binding_id: str
    schema_name: str | None = None


def load_bindings(path: Path) -> dict[str, ProjectBinding]:
    """从服务配置读取项目绑定，HTTP 请求无权指定 remote。"""

    records = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError("平台项目绑定配置无效")
    bindings = {item["projectId"]: ProjectBinding(
        item["projectId"], item["remote"], item["projectSubdir"], item["profileBindingId"],
        validate_schema_name(item["schemaName"]) if "schemaName" in item else None,
    ) for item in records}
    if len(bindings) != len(records):
        raise ValueError("平台项目绑定重复")
    return bindings


def _git(directory: Path, *args: str, limit: int = MAX_TREE_BYTES) -> bytes:
    command = ["git", *GIT_OPTIONS, "-C", str(directory), *args]
    environment = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    try:
        process = subprocess.run(command, capture_output=True, timeout=60, env=environment, check=True)
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("Git 固定版本读取失败") from error
    if len(process.stdout) > limit:
        raise ValueError("Git 输出超限")
    return process.stdout


def _prefix(value: str) -> str:
    if not value or value.isspace() or value.startswith("/") or value.endswith("/") or "\\" in value or ":" in value:
        raise ValueError("dbt 项目路径无效")
    if value == ".":
        return ""
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ValueError("dbt 项目路径无效")
    return f"{value}/"


def _included(path: str) -> bool:
    return path in ROOT_INPUTS or path.split("/", 1)[0] in INPUT_DIRECTORIES and "/" in path


def resolve_revision(binding: ProjectBinding, commit_sha: str, expected_digest: str, work_root: Path) -> Path:
    """从受控 remote 读取固定 Git 树，核对摘要后安全写入任务目录。"""
    if not SHA_PATTERN.fullmatch(commit_sha) or not DIGEST_PATTERN.fullmatch(expected_digest):
        raise ValueError("固定版本或项目摘要无效")
    return _resolve_revision(binding, commit_sha, expected_digest, work_root)[0]


def resolve_commit(binding: ProjectBinding, commit_sha: str, work_root: Path) -> tuple[Path, str]:
    """读取受控 remote 的精确提交并计算源码摘要。"""
    if not SHA_PATTERN.fullmatch(commit_sha):
        raise ValueError("固定版本无效")
    return _resolve_revision(binding, commit_sha, None, work_root)


def _resolve_revision(binding, commit_sha, expected_digest, work_root):
    prefix = _prefix(binding.project_subdir)
    if not binding.remote or not binding.project_id or not binding.profile_binding_id:
        raise ValueError("平台项目绑定无效")
    root = work_root.resolve()
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="git-", dir=root) as cache_name:
        cache = Path(cache_name)
        _git(cache, "init", "--bare")
        _git(cache, GIT_FETCH, GIT_NO_TAGS, GIT_SEPARATOR, binding.remote, commit_sha)
        fetched = _git(cache, "rev-parse", "--verify", "FETCH_HEAD").decode().strip()
        if not SHA_PATTERN.fullmatch(fetched):
            raise ValueError("Git 版本无效")
        if fetched != commit_sha:
            raise ValueError("Git did not return the requested commit")
        raw = _git(cache, "ls-tree", "-r", "-z", commit_sha, "--", prefix[:-1] if prefix else ".")
        entries: dict[str, tuple[str, str]] = {}
        for item in raw.split(b"\0"):
            if not item:
                continue
            try:
                metadata, raw_path = item.decode("utf-8").split("\t", 1)
                mode, kind, blob = metadata.split(" ")
            except ValueError as error:
                raise ValueError("dbt 项目输入无效") from error
            if kind != "blob" or mode not in {"100644", "100755"} or not SHA_PATTERN.fullmatch(blob):
                raise ValueError("dbt 项目输入无效")
            if prefix and not raw_path.startswith(prefix):
                continue
            relative = raw_path[len(prefix):]
            if relative == "dbt_project.yml" or _included(relative):
                if any(part in {"", ".", ".."} for part in relative.split("/")):
                    raise ValueError("dbt 项目输入无效")
                entries[relative] = (mode, blob)
        if "dbt_project.yml" not in entries:
            raise ValueError("dbt_project.yml 缺失")
        project_yaml = yaml.safe_load(_git(cache, "cat-file", "-p", entries["dbt_project.yml"][1]))
        if not isinstance(project_yaml, dict):
            raise ValueError("dbt_project.yml 无效")
        for key, folder in RESOURCE_PATHS.items():
            if key in project_yaml and project_yaml[key] != [folder]:
                raise ValueError("dbt 资源目录配置无效")
        digest = hashlib.sha256()
        for relative, (mode, blob) in sorted(entries.items()):
            digest.update(relative.encode("utf-8") + b"\0" + f"{mode} {blob}".encode("utf-8") + b"\0")
        if expected_digest is not None and digest.hexdigest() != expected_digest:
            raise ValueError("dbt 项目摘要不匹配")
        # Git 允许跨平台不安全的路径；在任何落盘之前执行 artifact 相同边界检查。
        from ..storage.artifacts import _check_paths

        _check_paths(list(entries))
        destination = Path(tempfile.mkdtemp(prefix="run-", dir=root))
        try:
            for relative, (mode, blob) in entries.items():
                target = destination.joinpath(*relative.split("/"))
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(_git(cache, "cat-file", "-p", blob))
                if mode == "100755":
                    target.chmod(0o755)
            return destination, digest.hexdigest()
        except Exception:
            shutil.rmtree(destination)
            raise
