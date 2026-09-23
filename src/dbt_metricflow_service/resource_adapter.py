from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path

from dbt.artifacts.resources.base import FileHash
from dbt.cli.main import dbtRunner
from dbt.config import Project
from dbt.contracts.files import AnySourceFile, FilePath, ParseFileType, SchemaSourceFile
from dbt.parser import read_files
from dbt.parser.manifest import ManifestLoader
from dbt.parser.schemas import yaml_from_file
from pathspec import PathSpec

from dbt_metricflow_service.commands import build_dbt_command
from dbt_metricflow_service.models import DbtJobRequest

logger = logging.getLogger(__name__)
EXPECTED_DBT_CORE_VERSION = "1.12.5"


class IncompatibleRuntimeError(RuntimeError):
    """The installed dbt runtime does not match the adapter's audited version."""


class ResourceAdapterError(RuntimeError):
    """A stable, source-free resource failure suitable for worker stderr."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _memory_source(path: FilePath, project_name: str, contents: str) -> SchemaSourceFile:
    source = SchemaSourceFile(
        path=path,
        checksum=FileHash.from_contents(read_files.normalize_file_contents(contents)),
        parse_file_type=ParseFileType.Schema,
        project_name=project_name,
        contents=contents,
    )
    parsed = yaml_from_file(source_file=source, validate=True)
    if parsed:
        read_files.validate_yaml(source.path.original_file_path, parsed)
        source.dfy = parsed
    return source


def _resource_paths(
    project: Project,
    paths: list[str],
    ignore_spec: PathSpec | None,
    resources: dict[str, str],
) -> dict[str, FilePath]:
    project_root = Path(project.project_root)
    visible: dict[str, list[FilePath]] = {}
    for extension in (".yml", ".yaml"):
        for path in read_files.filesystem_search(project, paths, extension, ignore_spec):
            visible.setdefault(Path(path.relative_path).name.casefold(), []).append(path)

    selected: dict[str, FilePath] = {}
    if not paths:
        raise ResourceAdapterError("resource_model_path_missing")
    for name in resources:
        matches = visible.get(name.casefold(), [])
        if len(matches) > 1:
            raise ResourceAdapterError("resource_name_ambiguous")
        if matches:
            selected[name.casefold()] = matches[0]
            continue

        # An ignored physical file cannot be silently treated as a new virtual definition.
        physical_matches = [
            candidate
            for model_path in paths
            for candidate in (project_root / model_path).rglob("*")
            if candidate.is_file() and candidate.name.casefold() == name.casefold()
        ]
        if physical_matches:
            raise ResourceAdapterError("resource_file_ignored")
        first_model_path = paths[0].rstrip("/\\")
        virtual_relative = f"{first_model_path}/{name}".replace("\\", "/")
        if ignore_spec is not None and ignore_spec.match_file(virtual_relative):
            raise ResourceAdapterError("resource_file_ignored")
        selected[name.casefold()] = FilePath(
            searched_path=paths[0],
            relative_path=name,
            modification_time=0.0,
            project_root=str(project_root),
        )
    return selected


@contextmanager
def _install_resource_hooks(project_dir: Path, resources: dict[str, str]) -> Iterator[None]:
    original_get_source_files = read_files.get_source_files
    original_write_partial_parse = ManifestLoader.write_manifest_for_partial_parse
    selected_paths: dict[str, FilePath] | None = None

    def resource_get_source_files(
        project: Project,
        paths: list[str],
        extension: str,
        parse_file_type: ParseFileType,
        saved_files: Mapping[str, AnySourceFile],
        ignore_spec: PathSpec | None,
    ) -> list[AnySourceFile]:
        nonlocal selected_paths
        is_root_schema = (
            parse_file_type == ParseFileType.Schema
            and Path(project.project_root).resolve() == project_dir.resolve()
        )
        if not is_root_schema:
            return original_get_source_files(project, paths, extension, parse_file_type, saved_files, ignore_spec)
        if selected_paths is None:
            selected_paths = _resource_paths(project, paths, ignore_spec, resources)
        files: list[AnySourceFile] = []
        physical_paths = read_files.filesystem_search(project, paths, extension, ignore_spec)
        for path in physical_paths:
            resource_name = Path(path.relative_path).name.casefold()
            raw = next((value for name, value in resources.items() if name.casefold() == resource_name), None)
            if raw is not None:
                files.append(_memory_source(path, project.project_name, raw))
            else:
                source = read_files.load_source_file(path, parse_file_type, project.project_name, saved_files)
                if source is not None:
                    files.append(source)
        existing_names = {Path(path.relative_path).name.casefold() for path in physical_paths}
        for name, raw in resources.items():
            if name.casefold() not in existing_names and name.endswith(extension):
                files.append(_memory_source(selected_paths[name.casefold()], project.project_name, raw))
        return files

    def skip_partial_parse_cache(_loader: ManifestLoader) -> None:
        return None

    read_files.get_source_files = resource_get_source_files  # type: ignore[assignment]
    ManifestLoader.write_manifest_for_partial_parse = skip_partial_parse_cache
    try:
        yield
    finally:
        read_files.get_source_files = original_get_source_files
        ManifestLoader.write_manifest_for_partial_parse = original_write_partial_parse


def execute_dbt(
    request: DbtJobRequest,
    project_dir: Path,
    profiles_dir: Path,
    artifact_dir: Path,
) -> int:
    """Invoke dbt with root schema YAML overlaid from request memory."""
    if version("dbt-core") != EXPECTED_DBT_CORE_VERSION:
        raise IncompatibleRuntimeError("incompatible_runtime")
    base_request = request.model_copy(update={"resources": {}})
    base = build_dbt_command(base_request, project_dir, profiles_dir)
    arguments = [
        *base.argv[1:],
        "--target-path",
        str(artifact_dir),
        "--log-path",
        str(artifact_dir / "logs"),
        "--log-level-file",
        "none",
        "--no-partial-parse",
        "--no-use-v2-parser",
    ]
    try:
        with _install_resource_hooks(project_dir, request.resources):
            result = dbtRunner().invoke(arguments)
    except ResourceAdapterError:
        raise
    except Exception as error:
        logger.info("Resource parsing failed with %s", type(error).__name__)
        raise ResourceAdapterError("resource_parse_error") from None
    if not result.success:
        if isinstance(result.exception, ResourceAdapterError):
            raise result.exception
        logger.info("Resource dbt invocation failed with %s", type(result.exception).__name__)
        raise ResourceAdapterError("resource_parse_error")
    return 0
