from __future__ import annotations

import logging
import os
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from importlib.metadata import version
from pathlib import Path

from dbt.adapters.factory import get_adapter_by_type
from dbt.artifacts.resources.base import FileHash
from dbt.cli.main import dbtRunner
from dbt.config import Project
from dbt.contracts.files import AnySourceFile, FilePath, ParseFileType, SchemaSourceFile
from dbt.parser import read_files
from dbt.parser.manifest import ManifestLoader
from dbt.parser.schemas import yaml_from_file
from dbt_metricflow.cli.cli_configuration import CLIConfiguration
from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtArtifacts
from dbt_metricflow.cli.main import cli as mf_cli
from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest
from pathspec import PathSpec

from dbt_metricflow_service.adapter_support import METRICFLOW_SUPPORTED_ADAPTERS
from dbt_metricflow_service.commands import build_dbt_command, build_metricflow_command
from dbt_metricflow_service.models import DbtJobRequest, MetricFlowJobRequest

logger = logging.getLogger(__name__)
EXPECTED_DBT_CORE_VERSION = "1.12.5"


class IncompatibleRuntimeError(RuntimeError):
    """The installed dbt runtime does not match the adapter's audited version."""


class ResourceAdapterError(RuntimeError):
    """A stable, source-free resource failure suitable for worker stderr."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class TaskCLIConfiguration(CLIConfiguration):
    """MetricFlow configuration bound to one task's semantic artifact."""

    def __init__(self, artifact_dir: Path) -> None:
        super().__init__()
        self._artifact_dir = artifact_dir

    @property
    def dbt_artifacts(self) -> dbtArtifacts:
        if self._dbt_artifacts is None:
            metadata = self.dbt_project_metadata
            manifest_path = self._artifact_dir / "semantic_manifest.json"
            semantic_manifest = parse_manifest_from_dbt_generated_manifest(
                manifest_json_string=manifest_path.read_text(encoding="utf-8")
            )
            self._dbt_artifacts = dbtArtifacts(
                profile=metadata.profile,
                project=metadata.project,
                adapter=get_adapter_by_type(metadata.profile.credentials.type),
                semantic_manifest=semantic_manifest,
            )
        return self._dbt_artifacts


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


@contextmanager
def _locked_metricflow_environment(artifact_dir: Path) -> Iterator[None]:
    values = {
        "DBT_TARGET_PATH": str(artifact_dir),
        "DBT_LOG_PATH": str(artifact_dir / "logs"),
        "DBT_LOG_LEVEL_FILE": "none",
        "DBT_ENGINE_USE_V2_PARSER": "false",
        "DBT_PARTIAL_PARSE": "false",
    }
    previous = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def execute_metricflow(
    request: MetricFlowJobRequest,
    project_dir: Path,
    profiles_dir: Path,
    artifact_dir: Path,
) -> int:
    """Parse request resources, then run the installed MetricFlow CLI on that manifest."""
    parse_request = DbtJobRequest(
        project=request.project,
        command="parse",
        resources=request.resources,
    )
    execute_dbt(parse_request, project_dir, profiles_dir, artifact_dir)
    configuration = TaskCLIConfiguration(artifact_dir)
    with _locked_metricflow_environment(artifact_dir):
        configuration.setup(
            dbt_profiles_path=profiles_dir,
            dbt_project_path=project_dir,
            configure_file_logging=False,
        )
        adapter_type = configuration.dbt_project_metadata.profile.credentials.type
        if adapter_type not in METRICFLOW_SUPPORTED_ADAPTERS:
            raise ResourceAdapterError("metricflow_adapter_not_supported")
        arguments = build_metricflow_command(
            request.model_copy(update={"resources": {}}),
            project_dir,
            profiles_dir,
        ).argv[1:]
        try:
            mf_cli.main(args=list(arguments), obj=configuration, standalone_mode=False)
        except SystemExit as error:
            if error.code is None:
                return 0
            return error.code if isinstance(error.code, int) else 1
        finally:
            if configuration._sql_client is not None:
                configuration._sql_client.close()
    return 0
