from __future__ import annotations

import gzip
import hashlib
import io
import mimetypes
import stat
from pathlib import Path, PurePosixPath, PureWindowsPath
from uuid import uuid4

import psycopg2
import yaml
from psycopg2.extras import Json

from dbt_metricflow_service.job_artifacts import _is_link
from dbt_metricflow_service.storage.postgres import Database

# 快照边界限定数据库占用，以及还原时单文件解压的最大内存。
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_SET_BYTES = 256 * 1024 * 1024
SOURCE = "SOURCE"
EXECUTION = "EXECUTION"
VALIDATION_INPUT = "VALIDATION_INPUT"
VALIDATION_INPUT_FILE = "changes.json"
MAX_VALIDATION_INPUT_BYTES = 8 * 1024 * 1024
STAGING = "STAGING"
SEALED = "SEALED"
RAW = "raw"
GZIP = "gzip"
UTF8 = "utf-8"
DEFAULT_MEDIA_TYPE = "application/octet-stream"
PROJECT_FILE = "dbt_project.yml"
TARGET_DIRECTORY = "target"
PACKAGE_DIRECTORY = "dbt_packages"
ROOT_FILES = frozenset({PROJECT_FILE, "packages.yml", "dependencies.yml", "package-lock.yml", "selectors.yml"})
RESOURCE_PATHS = {
    "model-paths": ["models"], "macro-paths": ["macros"], "test-paths": ["tests"],
    "analysis-paths": ["analyses"], "seed-paths": ["seeds"], "snapshot-paths": ["snapshots"],
    "docs-paths": [], "asset-paths": [],
}
EXCLUDED_PARTS = frozenset({
    ".git", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache", "logs", "profiles.yml",
    "partial_parse.msgpack", "request.json", "project-path.json", ".env",
})
TARGET_FILES = frozenset({
    "manifest.json", "semantic_manifest.json", "run_results.json", "catalog.json",
    "published_catalog.json", "publication_evidence.json", "publication_state.json",
    "sources.json",
})
TARGET_SUBDIRECTORIES = frozenset({"compiled", "run"})
SQL_INSERT_SET = """
INSERT INTO runtime_artifact_set
 (set_id,project_id,producer_attempt_id,kind,state,source_set_id,source_commit_sha,project_digest,
  config_version,toolchain_version,format_version,content_digest,file_count,raw_bytes,
  validation_json,catalog_json,metadata)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
SQL_INSERT_FILE = """
INSERT INTO runtime_artifact_file
 (set_id,relative_path,content,codec,raw_sha256,raw_size,stored_size,media_type,executable)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
SQL_SET = "SELECT * FROM runtime_artifact_set WHERE set_id=%s"
SQL_FILES = "SELECT * FROM runtime_artifact_file WHERE set_id=%s ORDER BY relative_path"
SQL_FILE = "SELECT * FROM runtime_artifact_file WHERE set_id=%s AND relative_path=%s"
SQL_DIGEST = "UPDATE runtime_artifact_set SET content_digest=%s WHERE set_id=%s"
FOR_UPDATE = " FOR UPDATE"
FOR_SHARE = " FOR SHARE"
SQL_SEAL = "UPDATE runtime_artifact_set SET state=%s,sealed_at=clock_timestamp() WHERE set_id=%s"
SQL_GC_LOCK = "SELECT set_id FROM runtime_artifact_set WHERE set_id=%s FOR UPDATE"
SQL_GC_ELIGIBLE = """
SELECT NOT EXISTS(SELECT 1 FROM runtime_job WHERE input_set_id=%s OR output_set_id=%s)
 AND NOT EXISTS(SELECT 1 FROM runtime_project WHERE source_set_id=%s OR current_output_set_id=%s)
 AND NOT EXISTS(SELECT 1 FROM runtime_artifact_set WHERE source_set_id=%s)
 AND NOT EXISTS(SELECT 1 FROM runtime_release WHERE artifact_set_id=%s)
 AND NOT EXISTS(SELECT 1 FROM runtime_branch WHERE base_input_set_id=%s)
 AND NOT EXISTS(SELECT 1 FROM runtime_attempt a JOIN runtime_artifact_set s
   ON a.attempt_id=s.producer_attempt_id WHERE s.set_id=%s
   AND a.state IN ('EXECUTING','EXPIRED_UNCONFIRMED')) AS eligible
"""
SQL_DELETING = "UPDATE runtime_artifact_set SET state='DELETING' WHERE set_id=%s"
SQL_DELETE_FILES = "DELETE FROM runtime_artifact_file WHERE set_id=%s"
SQL_DELETE_SET = "DELETE FROM runtime_artifact_set WHERE set_id=%s"


def _relative(value: str) -> PurePosixPath:
    """所有平台使用同一套严格相对路径，拒绝规范化会隐藏的穿越。"""
    if (not value or "\\" in value or ":" in value or "\x00" in value
            or PureWindowsPath(value).drive or value.startswith("/")):
        raise ValueError("artifact path must be a relative POSIX path")
    if any(part in {"", ".", ".."} or part.endswith((" ", ".")) for part in value.split("/")):
        raise ValueError("artifact path contains unsafe segments")
    if any(PureWindowsPath(part).is_reserved() for part in value.split("/")):
        raise ValueError("artifact path contains a reserved device name")
    return PurePosixPath(value)


def _check_paths(paths: list[str]) -> None:
    # 大小写折叠与父文件冲突同时检查，防止跨平台还原覆盖其他文件。
    seen: set[str] = set()
    spelling: dict[str, str] = {}
    for value in sorted(paths):
        path = _relative(value)
        for component in (path, *path.parents):
            name = component.as_posix()
            previous = spelling.setdefault(name.casefold(), name)
            if previous != name:
                raise ValueError("artifact paths have a case collision")
        folded = path.as_posix().casefold()
        if folded in seen:
            raise ValueError("artifact paths have a case collision")
        seen.add(folded)
    for value in seen:
        if any(parent.as_posix() in seen for parent in PurePosixPath(value).parents):
            raise ValueError("artifact file conflicts with a parent directory")


def _digest(files: list[dict]) -> str:
    digest = hashlib.sha256()
    for item in sorted(files, key=lambda item: item["relative_path"]):
        for value in (item["relative_path"], item["raw_sha256"], str(item["raw_size"])):
            digest.update(value.encode(UTF8))
            digest.update(b"\0")
    return digest.hexdigest()


def _files(directory: Path, kind: str, max_file_bytes: int) -> list[tuple[str, Path]]:
    # 项目目录作为唯一允许根；配置不能引入根目录以外的输入。
    root = directory.resolve(strict=True)
    if _is_link(directory) or not root.is_dir():
        raise ValueError("artifact source must be a regular directory")
    config_path = root / PROJECT_FILE
    if _is_link(config_path):
        raise ValueError("dbt project configuration cannot be linked")
    if config_path.stat().st_size > max_file_bytes:
        raise ValueError("artifact file size limit exceeded")
    config = yaml.safe_load(config_path.read_bytes())
    if not isinstance(config, dict):
        raise ValueError("dbt project configuration must be a mapping")
    inputs = set(ROOT_FILES)
    for key, defaults in RESOURCE_PATHS.items():
        paths = config.get(key, defaults)
        if not isinstance(paths, list) or any(not isinstance(path, str) for path in paths):
            raise ValueError("dbt resource paths must be a list of relative paths")
        inputs.update(_relative(path).as_posix() for path in paths)
    target = _relative(config.get("target-path", TARGET_DIRECTORY)).as_posix()
    packages = _relative(config.get("packages-install-path", PACKAGE_DIRECTORY)).as_posix()
    log_path = _relative(config.get("log-path", "logs")).as_posix()
    inputs.add(packages)
    if kind == EXECUTION:
        inputs.update(f"{target}/{name}" for name in TARGET_FILES | TARGET_SUBDIRECTORIES)
    result: dict[str, Path] = {}

    def visit(path: Path, ancestry: frozenset[Path]) -> None:
        relative = path.relative_to(root).as_posix()
        parts = _relative(relative).parts
        if any(part.casefold() in EXCLUDED_PARTS for part in parts):
            return
        if relative == log_path or relative.startswith(log_path + "/"):
            return
        if kind == SOURCE and (relative == target or relative.startswith(target + "/")):
            return
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root):
            raise ValueError("artifact symlink escapes the allowed input root")
        resolved_relative = resolved.relative_to(root).as_posix()
        if (any(part.casefold() in EXCLUDED_PARTS for part in resolved.relative_to(root).parts)
                or resolved_relative == log_path or resolved_relative.startswith(log_path + "/")
                or kind == SOURCE and (resolved_relative == target or resolved_relative.startswith(target + "/"))):
            raise ValueError("artifact symlink refers to an excluded file")
        if resolved in ancestry:
            raise ValueError("artifact directory contains a symlink cycle")
        if path.is_dir():
            for child in sorted(path.iterdir()):
                visit(child, ancestry | {resolved})
        elif path.is_file() and stat.S_ISREG(path.stat().st_mode):
            result[relative] = path
        else:
            raise ValueError("artifact input must be an ordinary file")

    for relative in sorted(inputs):
        candidate = root.joinpath(*_relative(relative).parts)
        if candidate.exists() or candidate.is_symlink():
            visit(candidate, frozenset({root}))
    _check_paths(list(result))
    return sorted(result.items())


def _decode(row: dict, max_file_bytes: int) -> bytes:
    # 限制解压输出，且逐文件核对长度和原始摘要，拒绝损坏与解压炸弹。
    size = row["raw_size"]
    if size < 0 or size > max_file_bytes or row["stored_size"] > max_file_bytes:
        raise ValueError("artifact file size limit exceeded")
    content = bytes(row["content"])
    if len(content) != row["stored_size"]:
        raise ValueError("artifact stored size mismatch")
    if row["codec"] == GZIP:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as stream:
            content = stream.read(max_file_bytes + 1)
    elif row["codec"] != RAW:
        raise ValueError("artifact codec is unsupported")
    if len(content) != size or hashlib.sha256(content).hexdigest() != row["raw_sha256"]:
        raise ValueError("artifact checksum or size mismatch")
    return content


class ArtifactStore:
    def __init__(self, database: Database, *, max_file_bytes: int = MAX_FILE_BYTES,
                 max_set_bytes: int = MAX_SET_BYTES):
        # 数据库为唯一持久事实来源，本地路径不会保存为集合定位符。
        self.database = database
        # 同时约束导入与还原；配置仅能收紧格式的硬上限。
        self.max_file_bytes = min(max_file_bytes, MAX_FILE_BYTES)
        self.max_set_bytes = min(max_set_bytes, MAX_SET_BYTES)
        if self.max_file_bytes <= 0 or self.max_set_bytes <= 0:
            raise ValueError("artifact limits must be positive")

    def capture(self, project_id: str, directory: Path, *, producer_attempt_id: str | None = None,
                kind: str = SOURCE, metadata: dict | None = None) -> str:
        if kind not in {SOURCE, EXECUTION}:
            raise ValueError("artifact kind is unsupported")
        metadata = dict(metadata or {})
        files: list[dict] = []
        total = 0
        # 先完成路径和容量检查，再以单个事务保存集合，失败不会留下半份快照。
        for relative, path in _files(Path(directory), kind, self.max_file_bytes):
            size = path.stat().st_size
            total += size
            if size > self.max_file_bytes or total > self.max_set_bytes:
                raise ValueError("artifact size limit exceeded")
            files.append({"relative_path": relative, "path": path, "raw_size": size,
                          "raw_sha256": None, "executable": bool(path.stat().st_mode & stat.S_IXUSR)})
        set_id = str(uuid4())
        with self.database.transaction() as cursor:
            cursor.execute(SQL_INSERT_SET, (
                set_id, project_id, producer_attempt_id, kind, STAGING, metadata.get("source_set_id"),
                metadata.get("source_commit_sha"), metadata.get("project_digest"),
                metadata.get("config_version", "1"), metadata.get("toolchain_version", "1"),
                metadata.get("format_version", "1"), "", len(files), total,
                Json(metadata.get("validation_json", {})), Json(metadata.get("catalog_json", {})), Json(metadata),
            ))
            for item in files:
                # 有界读取可发现检查与读取之间增长的文件，不无限制读入内存。
                with item["path"].open("rb") as stream:
                    content = stream.read(self.max_file_bytes + 1)
                if len(content) != item["raw_size"]:
                    raise ValueError("artifact file changed during capture")
                item["raw_sha256"] = hashlib.sha256(content).hexdigest()
                cursor.execute(SQL_INSERT_FILE, (
                    set_id, item["relative_path"], psycopg2.Binary(content), RAW, item["raw_sha256"],
                    len(content), len(content), mimetypes.guess_type(item["relative_path"])[0] or DEFAULT_MEDIA_TYPE,
                    item["executable"],
                ))
            cursor.execute(SQL_DIGEST, (_digest(files), set_id))
            if producer_attempt_id is None:
                self.seal(set_id, cursor)
        return set_id

    def capture_validation_input(self, project_id: str, payload: bytes, cursor, *, version: int = 1) -> str:
        """与 job 受理共用事务；专用输入不放宽普通项目快照的文件白名单。"""
        if len(payload) > min(MAX_VALIDATION_INPUT_BYTES, self.max_file_bytes, self.max_set_bytes):
            raise ValueError("validation input exceeds byte limit")
        # artifact 即使被其他内部调用者提交，也必须符合公开草稿协议。
        from dbt_metricflow_service.draft_validation_models import BranchDraftValidationRequest, DraftValidationRequest

        # 受理端显式选择协议版本；旧接口不因载荷包含新字段而自动升级。
        model = BranchDraftValidationRequest if version == 2 else DraftValidationRequest
        model.model_validate_json(payload)
        set_id = str(uuid4())
        item = {"relative_path": VALIDATION_INPUT_FILE, "raw_sha256": hashlib.sha256(payload).hexdigest(),
                "raw_size": len(payload)}
        cursor.execute(SQL_INSERT_SET, (
            set_id, project_id, None, VALIDATION_INPUT, STAGING, None, None, None,
            "1", "1", "1", _digest([item]), 1, len(payload), Json({}), Json({}), Json({}),
        ))
        cursor.execute(SQL_INSERT_FILE, (
            set_id, VALIDATION_INPUT_FILE, psycopg2.Binary(payload), RAW, item["raw_sha256"],
            len(payload), len(payload), DEFAULT_MEDIA_TYPE, False,
        ))
        self.seal(set_id, cursor)
        return set_id

    def metadata(self, set_id: str) -> dict:
        with self.database.transaction() as cursor:
            cursor.execute(SQL_SET, (set_id,))
            row = cursor.fetchone()
            if row is None:
                raise ValueError("artifact set does not exist")
            return {**row["metadata"], **dict(row)}

    def seal(self, set_id: str, cursor) -> None:
        # 调用方负责租约校验；同一事务锁定集合并核对完整内容后才能发布。
        cursor.execute(SQL_SET + FOR_UPDATE, (set_id,))
        row = cursor.fetchone()
        if row is None or row["state"] not in {STAGING, SEALED}:
            raise ValueError("artifact set cannot be sealed")
        cursor.execute(SQL_FILES, (set_id,))
        files = cursor.fetchall()
        self._validate(row, files)
        if row["state"] == STAGING:
            cursor.execute(SQL_SEAL, (SEALED, set_id))

    def _validate(self, metadata: dict, files: list[dict]) -> None:
        _check_paths([row["relative_path"] for row in files])
        total = sum(row["raw_size"] for row in files)
        if total > self.max_set_bytes or total != metadata["raw_bytes"] or len(files) != metadata["file_count"]:
            raise ValueError("artifact set size or file count mismatch")
        if _digest(files) != metadata["content_digest"]:
            raise ValueError("artifact set digest mismatch")
        for row in files:
            _decode(row, self.max_file_bytes)

    def read_file(self, set_id: str, relative_path: str) -> bytes:
        # 仅读取指定文件即可进行 API 前置校验，不必还原整个项目目录。
        relative_path = _relative(relative_path).as_posix()
        with self.database.transaction() as cursor:
            cursor.execute(SQL_SET + FOR_SHARE, (set_id,))
            metadata = cursor.fetchone()
            if metadata is None or metadata["state"] != SEALED:
                raise ValueError("only SEALED artifact files can be read")
            cursor.execute(SQL_FILE, (set_id, relative_path))
            row = cursor.fetchone()
            if row is None:
                raise ValueError("artifact file does not exist")
            return _decode(row, self.max_file_bytes)

    def materialize(self, set_id: str, destination: Path) -> None:
        # 目标只能是空的普通目录，所有上级路径也不得借链接跳转。
        destination = Path(destination)
        if any(_is_link(path) for path in (destination, *destination.parents)):
            raise ValueError("artifact destination cannot contain links")
        if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
            raise ValueError("artifact destination must be an empty directory")
        with self.database.transaction() as cursor:
            cursor.execute(SQL_SET + FOR_SHARE, (set_id,))
            metadata = cursor.fetchone()
            if metadata is None or metadata["state"] != SEALED:
                raise ValueError("only SEALED artifact sets can be materialized")
            cursor.execute(SQL_FILES, (set_id,))
            files = cursor.fetchall()
            self._validate(metadata, files)
            destination.mkdir(parents=True, exist_ok=True)
            for row in files:
                target = destination.joinpath(*_relative(row["relative_path"]).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("xb") as stream:
                    stream.write(_decode(row, self.max_file_bytes))
                if row["executable"]:
                    target.chmod(target.stat().st_mode | stat.S_IXUSR)

    def delete_unreferenced(self, set_id: str) -> bool:
        # 引用外键与集合行锁共同阻止 GC 删除正在发布或仍被任务引用的版本。
        try:
            with self.database.transaction() as cursor:
                cursor.execute(SQL_GC_LOCK, (set_id,))
                if cursor.fetchone() is None:
                    return False
                cursor.execute(SQL_GC_ELIGIBLE, (set_id,) * 8)
                if not cursor.fetchone()["eligible"]:
                    return False
                cursor.execute(SQL_DELETING, (set_id,))
                cursor.execute(SQL_DELETE_FILES, (set_id,))
                cursor.execute(SQL_DELETE_SET, (set_id,))
                return True
        except psycopg2.errors.ForeignKeyViolation:
            return False
