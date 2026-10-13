from __future__ import annotations

import gzip
import hashlib
import io
import mimetypes
import stat
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Final
from uuid import uuid4

import yaml
from pydantic import Field
from sqlalchemy import delete, exists, func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dbt_metricflow_service import constants as strings
from dbt_metricflow_service.execution.artifacts import _is_link
from dbt_metricflow_service.models.native import NativePackages, NativeProject
from dbt_metricflow_service.storage.entities import (
    ArtifactFile,
    ArtifactSet,
    RuntimeAttempt,
    RuntimeJob,
    RuntimeProject,
)
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.records import (
    ArtifactDigestEntry,
    ArtifactMetadata,
    CaptureFile,
    StoredArtifactFile,
    StoredArtifactSet,
)
from dbt_metricflow_service.storage.rows import entity_record

ARTIFACT_CASE_COLLISION_MESSAGE: Final = "artifact paths have a case collision"
ARTIFACT_CHANGED_MESSAGE: Final = "artifact file changed during capture"
ARTIFACT_CHECKSUM_MESSAGE: Final = "artifact checksum or size mismatch"
ARTIFACT_EXCLUDED_LINK_MESSAGE: Final = "artifact symlink refers to an excluded file"
ARTIFACT_FILE_LIMIT_MESSAGE: Final = "artifact file size limit exceeded"
ARTIFACT_LINK_CYCLE_MESSAGE: Final = "artifact directory contains a symlink cycle"
ARTIFACT_LINK_ESCAPE_MESSAGE: Final = "artifact symlink escapes the allowed input root"
ARTIFACT_PARENT_CONFLICT_MESSAGE: Final = "artifact file conflicts with a parent directory"
ARTIFACT_SET_DIGEST_MESSAGE: Final = "artifact set digest mismatch"
ARTIFACT_SET_LIMIT_MESSAGE: Final = "artifact size limit exceeded"
ARTIFACT_SET_SIZE_MESSAGE: Final = "artifact set size or file count mismatch"
ARTIFACT_STORED_SIZE_MESSAGE: Final = "artifact stored size mismatch"
INVALID_ARTIFACT_INPUT_MESSAGE: Final = "artifact input must be an ordinary file"
INVALID_ARTIFACT_LIMITS_MESSAGE: Final = "artifact limits must be positive"
INVALID_ARTIFACT_SOURCE_MESSAGE: Final = "artifact source must be a regular directory"
INVALID_DEPENDENCY_MESSAGE: Final = "invalid dependency declaration"
LINKED_ARTIFACT_DESTINATION_MESSAGE: Final = "artifact destination cannot contain links"
LINKED_PROJECT_CONFIG_MESSAGE: Final = "dbt project configuration cannot be linked"
MISSING_ARTIFACT_FILE_MESSAGE: Final = "artifact file does not exist"
NONEMPTY_ARTIFACT_DESTINATION_MESSAGE: Final = "artifact destination must be an empty directory"
RELATIVE_ARTIFACT_PATH_MESSAGE: Final = "artifact path must be a relative POSIX path"
RESERVED_ARTIFACT_PATH_MESSAGE: Final = "artifact path contains a reserved device name"
UNSAFE_ARTIFACT_PATH_MESSAGE: Final = "artifact path contains unsafe segments"
UNSEALABLE_ARTIFACT_SET_MESSAGE: Final = "artifact set cannot be sealed"
UNSEALED_ARTIFACT_FILE_MESSAGE: Final = "only SEALED artifact files can be read"
UNSEALED_ARTIFACT_SET_MESSAGE: Final = "only SEALED artifact sets can be materialized"
UNSUPPORTED_ARTIFACT_CODEC_MESSAGE: Final = "artifact codec is unsupported"
UNSUPPORTED_ARTIFACT_KIND_MESSAGE: Final = "artifact kind is unsupported"

# 快照边界限定数据库占用，以及还原时单文件解压的最大内存。
MAX_FILE_BYTES = 64 * 1024 * 1024
MAX_SET_BYTES = 256 * 1024 * 1024
SOURCE = strings.SOURCE
EXECUTION = strings.EXECUTION
STAGING = strings.STAGING
SEALED = strings.SEALED
RAW = strings.RAW_CODEC
GZIP = strings.GZIP_CODEC
UTF8 = strings.UTF8
DEFAULT_MEDIA_TYPE = strings.BINARY_MEDIA_TYPE
PROJECT_FILE = strings.DBT_PROJECT_FILE
TARGET_DIRECTORY = strings.TARGET_DIRECTORY
ROOT_FILES = frozenset(
    {PROJECT_FILE, strings.PACKAGES_FILE, strings.DEPENDENCIES_FILE, strings.PACKAGE_LOCK_FILE, strings.SELECTORS_FILE}
)
EXCLUDED_PARTS = frozenset(
    {
        strings.GIT_DIRECTORY,
        strings.VENV_DIRECTORY,
        strings.PYTHON_CACHE_DIRECTORY,
        strings.PYTEST_CACHE_DIRECTORY,
        strings.RUFF_CACHE_DIRECTORY,
        strings.LOGS_DIRECTORY,
        strings.DBT_PROFILES_FILE,
        strings.PARTIAL_PARSE_FILE,
        strings.REQUEST_FILE,
        strings.PROJECT_PATH_FILE,
        strings.ENV_FILE,
    }
)
TARGET_FILES = frozenset({
    strings.MANIFEST_FILE, strings.SEMANTIC_MANIFEST_FILE, strings.CATALOG_FILE, strings.PUBLISHED_CATALOG_FILE,
})
DELETING = strings.DELETING
LIVE_ATTEMPT_STATES = (strings.EXECUTING, strings.EXPIRED_UNCONFIRMED)


class ArtifactProject(NativeProject):
    """dbt projection with the artifact capture defaults kept explicit."""

    docs_paths: list[str] = Field(default_factory=list, alias=strings.DBT_DOCS_PATHS_FIELD)
    asset_paths: list[str] = Field(default_factory=list, alias=strings.DBT_ASSET_PATHS_FIELD)
    target_path: str = Field(default=TARGET_DIRECTORY, alias=strings.DBT_TARGET_PATH_FIELD)


def _relative(value: str) -> PurePosixPath:
    """所有平台使用同一套严格相对路径，拒绝规范化会隐藏的穿越。"""
    if (not value or "\\" in value or ":" in value or "\x00" in value
            or PureWindowsPath(value).drive or value.startswith("/")):
        raise ValueError(RELATIVE_ARTIFACT_PATH_MESSAGE)
    if any(part in {"", ".", ".."} or part.endswith((" ", ".")) for part in value.split("/")):
        raise ValueError(UNSAFE_ARTIFACT_PATH_MESSAGE)
    if any(PureWindowsPath(part).is_reserved() for part in value.split("/")):
        raise ValueError(RESERVED_ARTIFACT_PATH_MESSAGE)
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
                raise ValueError(ARTIFACT_CASE_COLLISION_MESSAGE)
        folded = path.as_posix().casefold()
        if folded in seen:
            raise ValueError(ARTIFACT_CASE_COLLISION_MESSAGE)
        seen.add(folded)
    for value in seen:
        if any(parent.as_posix() in seen for parent in PurePosixPath(value).parents):
            raise ValueError(ARTIFACT_PARENT_CONFLICT_MESSAGE)


def _digest(files: list[ArtifactDigestEntry]) -> str:
    digest = hashlib.sha256()
    for item in sorted(files, key=lambda item: item.relative_path):
        for value in (item.relative_path, item.raw_sha256, str(item.raw_size)):
            digest.update(value.encode(UTF8))
            digest.update(b"\0")
    return digest.hexdigest()


def _files(directory: Path, kind: str, max_file_bytes: int) -> list[tuple[str, Path]]:
    # 项目目录作为唯一允许根；配置不能引入根目录以外的输入。
    root = directory.resolve(strict=True)
    if _is_link(directory) or not root.is_dir():
        raise ValueError(INVALID_ARTIFACT_SOURCE_MESSAGE)
    config_path = root / PROJECT_FILE
    if _is_link(config_path):
        raise ValueError(LINKED_PROJECT_CONFIG_MESSAGE)
    if config_path.stat().st_size > max_file_bytes:
        raise ValueError(ARTIFACT_FILE_LIMIT_MESSAGE)
    # dbt project/dependency YAML is an external extensible protocol, not a service record.
    config = ArtifactProject.model_validate(yaml.safe_load(config_path.read_bytes()))
    inputs = set(ROOT_FILES)
    # 与 Git 读取一致，把工程内本地依赖也纳入可重启源码快照。
    for filename in (strings.PACKAGES_FILE, strings.DEPENDENCIES_FILE):
        dependency_file = root / filename
        if dependency_file.is_file():
            if dependency_file.stat().st_size > max_file_bytes or _is_link(dependency_file):
                raise ValueError(INVALID_DEPENDENCY_MESSAGE)
            dependencies = NativePackages.model_validate(yaml.safe_load(dependency_file.read_bytes()) or {})
            for package in dependencies.packages:
                if package.local is not None:
                    inputs.add(_relative(package.local).as_posix())
    inputs.update(_relative(path).as_posix() for path in config.resource_paths())
    target = _relative(config.target_path).as_posix()
    packages = _relative(config.packages_install_path).as_posix()
    log_path = _relative(config.log_path).as_posix()
    inputs.add(packages)
    if kind == EXECUTION:
        inputs.update(f"{target}/{name}" for name in TARGET_FILES)
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
            raise ValueError(ARTIFACT_LINK_ESCAPE_MESSAGE)
        resolved_relative = resolved.relative_to(root).as_posix()
        if (any(part.casefold() in EXCLUDED_PARTS for part in resolved.relative_to(root).parts)
                or resolved_relative == log_path or resolved_relative.startswith(log_path + "/")
                or kind == SOURCE and (resolved_relative == target or resolved_relative.startswith(target + "/"))):
            raise ValueError(ARTIFACT_EXCLUDED_LINK_MESSAGE)
        if resolved in ancestry:
            raise ValueError(ARTIFACT_LINK_CYCLE_MESSAGE)
        if path.is_dir():
            for child in sorted(path.iterdir()):
                visit(child, ancestry | {resolved})
        elif path.is_file() and stat.S_ISREG(path.stat().st_mode):
            result[relative] = path
        else:
            raise ValueError(INVALID_ARTIFACT_INPUT_MESSAGE)

    for relative in sorted(inputs):
        candidate = root.joinpath(*_relative(relative).parts)
        if candidate.exists() or candidate.is_symlink():
            visit(candidate, frozenset({root}))
    _check_paths(list(result))
    return sorted(result.items())


def _decode(row: StoredArtifactFile, max_file_bytes: int) -> bytes:
    # 限制解压输出，且逐文件核对长度和原始摘要，拒绝损坏与解压炸弹。
    size = row.raw_size
    if size < 0 or size > max_file_bytes or row.stored_size > max_file_bytes:
        raise ValueError(ARTIFACT_FILE_LIMIT_MESSAGE)
    content = bytes(row.content)
    if len(content) != row.stored_size:
        raise ValueError(ARTIFACT_STORED_SIZE_MESSAGE)
    if row.codec == GZIP:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as stream:
            content = stream.read(max_file_bytes + 1)
    elif row.codec != RAW:
        raise ValueError(UNSUPPORTED_ARTIFACT_CODEC_MESSAGE)
    if len(content) != size or hashlib.sha256(content).hexdigest() != row.raw_sha256:
        raise ValueError(ARTIFACT_CHECKSUM_MESSAGE)
    return content


class ArtifactStore:
    def __init__(self, database: Database, *, max_file_bytes: int = MAX_FILE_BYTES,
                 max_set_bytes: int = MAX_SET_BYTES) -> None:
        # 数据库为唯一持久事实来源，本地路径不会保存为集合定位符。
        self.database = database
        # 同时约束导入与还原；配置仅能收紧格式的硬上限。
        self.max_file_bytes = min(max_file_bytes, MAX_FILE_BYTES)
        self.max_set_bytes = min(max_set_bytes, MAX_SET_BYTES)
        if self.max_file_bytes <= 0 or self.max_set_bytes <= 0:
            raise ValueError(INVALID_ARTIFACT_LIMITS_MESSAGE)

    def capture(self, project_id: str, directory: Path, *, producer_attempt_id: str | None = None,
                kind: str = SOURCE, metadata: ArtifactMetadata | None = None) -> str:
        if kind not in {SOURCE, EXECUTION}:
            raise ValueError(UNSUPPORTED_ARTIFACT_KIND_MESSAGE)
        metadata = metadata or ArtifactMetadata()
        files: list[CaptureFile] = []
        total = 0
        # 先完成路径和容量检查，再以单个事务保存集合，失败不会留下半份快照。
        for relative, path in _files(Path(directory), kind, self.max_file_bytes):
            size = path.stat().st_size
            total += size
            if size > self.max_file_bytes or total > self.max_set_bytes:
                raise ValueError(ARTIFACT_SET_LIMIT_MESSAGE)
            files.append(CaptureFile(
                relative_path=relative, path=path, raw_size=size, raw_sha256="",
                executable=bool(path.stat().st_mode & stat.S_IXUSR),
            ))
        set_id = str(uuid4())
        with self.database.session() as session:
            artifact_set = ArtifactSet(
                set_id=set_id,
                project_id=project_id,
                producer_attempt_id=producer_attempt_id,
                kind=kind,
                state=STAGING,
                source_set_id=metadata.source_set_id,
                source_commit_sha=metadata.source_commit_sha,
                project_digest=metadata.project_digest,
                config_version=metadata.config_version,
                toolchain_version=metadata.toolchain_version,
                format_version=metadata.format_version,
                content_digest="",
                file_count=len(files),
                raw_bytes=total,
                validation_json=metadata.validation_json.model_dump(
                    mode=strings.JSON_MODE, by_alias=True, exclude_unset=True
                ),
            )
            session.add(artifact_set)
            # 先写父集合，随后逐文件 flush，保持外键顺序与有界内存。
            session.flush()
            for item in files:
                # 有界读取可发现检查与读取之间增长的文件，不无限制读入内存。
                with item.path.open(strings.BINARY_READ_MODE) as stream:
                    content = stream.read(self.max_file_bytes + 1)
                if len(content) != item.raw_size:
                    raise ValueError(ARTIFACT_CHANGED_MESSAGE)
                item.raw_sha256 = hashlib.sha256(content).hexdigest()
                session.add(ArtifactFile(
                    set_id=set_id, relative_path=item.relative_path, content=content, codec=RAW,
                    raw_sha256=item.raw_sha256, raw_size=len(content), stored_size=len(content),
                    media_type=mimetypes.guess_type(item.relative_path)[0] or DEFAULT_MEDIA_TYPE,
                    executable=item.executable,
                ))
                session.flush()
            artifact_set.content_digest = _digest(files)
            session.flush()
            if producer_attempt_id is None:
                self.seal(set_id, session)
        return set_id

    def seal(self, set_id: str, session: Session) -> None:
        # 调用方负责租约校验；同一事务锁定集合并核对完整内容后才能发布。
        entity = session.get(ArtifactSet, set_id, with_for_update=True, populate_existing=True)
        row = entity_record(entity, StoredArtifactSet)
        if row is None or row.state not in {STAGING, SEALED}:
            raise ValueError(UNSEALABLE_ARTIFACT_SET_MESSAGE)
        files = [entity_record(file, StoredArtifactFile) for file in session.scalars(
            select(ArtifactFile).where(ArtifactFile.set_id == set_id).order_by(ArtifactFile.relative_path))]
        self._validate(row, files)
        if row.state == STAGING:
            session.execute(update(ArtifactSet).where(ArtifactSet.set_id == set_id)
                               .values(state=SEALED, sealed_at=func.clock_timestamp()))

    def _validate(self, metadata: StoredArtifactSet, files: list[StoredArtifactFile]) -> None:
        _check_paths([row.relative_path for row in files])
        total = sum(row.raw_size for row in files)
        if total > self.max_set_bytes or total != metadata.raw_bytes or len(files) != metadata.file_count:
            raise ValueError(ARTIFACT_SET_SIZE_MESSAGE)
        if _digest(files) != metadata.content_digest:
            raise ValueError(ARTIFACT_SET_DIGEST_MESSAGE)
        for row in files:
            _decode(row, self.max_file_bytes)

    def read_file(self, set_id: str, relative_path: str) -> bytes:
        # 仅读取指定文件即可进行 API 前置校验，不必还原整个项目目录。
        relative_path = _relative(relative_path).as_posix()
        with self.database.session() as session:
            metadata = entity_record(
                session.get(ArtifactSet, set_id, with_for_update={strings.SQLALCHEMY_READ_LOCK_OPTION: True}),
                StoredArtifactSet,
            )
            if metadata is None or metadata.state != SEALED:
                raise ValueError(UNSEALED_ARTIFACT_FILE_MESSAGE)
            row = entity_record(session.get(ArtifactFile, (set_id, relative_path)), StoredArtifactFile)
            if row is None:
                raise ValueError(MISSING_ARTIFACT_FILE_MESSAGE)
            return _decode(row, self.max_file_bytes)

    def materialize(self, set_id: str, destination: Path) -> None:
        # 目标只能是空的普通目录，所有上级路径也不得借链接跳转。
        destination = Path(destination)
        if any(_is_link(path) for path in (destination, *destination.parents)):
            raise ValueError(LINKED_ARTIFACT_DESTINATION_MESSAGE)
        if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
            raise ValueError(NONEMPTY_ARTIFACT_DESTINATION_MESSAGE)
        with self.database.session() as session:
            metadata = entity_record(
                session.get(ArtifactSet, set_id, with_for_update={strings.SQLALCHEMY_READ_LOCK_OPTION: True}),
                StoredArtifactSet,
            )
            if metadata is None or metadata.state != SEALED:
                raise ValueError(UNSEALED_ARTIFACT_SET_MESSAGE)
            files = [entity_record(file, StoredArtifactFile) for file in session.scalars(
                select(ArtifactFile).where(ArtifactFile.set_id == set_id).order_by(ArtifactFile.relative_path))]
            self._validate(metadata, files)
            destination.mkdir(parents=True, exist_ok=True)
            for row in files:
                target = destination.joinpath(*_relative(row.relative_path).parts)
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open(strings.EXCLUSIVE_BINARY_WRITE_MODE) as stream:
                    stream.write(_decode(row, self.max_file_bytes))
                if row.executable:
                    target.chmod(target.stat().st_mode | stat.S_IXUSR)

    def delete_unreferenced(self, set_id: str) -> bool:
        # 引用外键与集合行锁共同阻止 GC 删除正在发布或仍被任务引用的版本。
        try:
            with self.database.session() as session:
                if session.get(ArtifactSet, set_id, with_for_update=True) is None:
                    return False
                referenced = or_(
                    exists().where(or_(RuntimeJob.input_set_id == set_id, RuntimeJob.output_set_id == set_id)),
                    exists().where(RuntimeProject.source_set_id == set_id),
                    exists().where(ArtifactSet.source_set_id == set_id),
                    exists().where(RuntimeAttempt.attempt_id == ArtifactSet.producer_attempt_id,
                                   ArtifactSet.set_id == set_id, RuntimeAttempt.state.in_(LIVE_ATTEMPT_STATES)),
                )
                if session.scalar(select(referenced)):
                    return False
                session.execute(update(ArtifactSet).where(ArtifactSet.set_id == set_id).values(state=DELETING))
                session.execute(delete(ArtifactFile).where(ArtifactFile.set_id == set_id))
                session.execute(delete(ArtifactSet).where(ArtifactSet.set_id == set_id))
                return True
        except IntegrityError as error:
            if error.orig.pgcode == strings.POSTGRES_FOREIGN_KEY_VIOLATION:
                return False
            raise
