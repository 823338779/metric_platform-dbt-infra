"""构建只调用 dbt build，捕获返回 Manifest，不执行外部验收。"""

import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml

from dbt_metricflow_service.platform.build import execute_build
from dbt_metricflow_service.runtime.executor import ExecutionError, RuntimeExecutor
from dbt_metricflow_service.storage.records import JobRequest
from tests.data_helpers import leased_job


@pytest.fixture
def publication_executor(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: sample\nversion: '1.0'\ntarget-path: custom-target\n")
    (project / "models").mkdir()
    (project / "models/orders.sql").write_text("{{ config(schema='custom', pre_hook='select 1') }} select 1")
    (project / "macros").mkdir()
    macro = "{% macro generate_schema_name(custom_schema_name, node) %}{{ target.schema }}{% endmacro %}"
    (project / "macros/generate_schema_name.sql").write_text(macro)
    (project / "packages.yml").write_text("packages: []")
    job = leased_job(project_id="sample", request_json=JobRequest(build_id=str(uuid4())),
                     schema_name="candidate", profile_binding_id="test", toolchain_version="1")
    calls, captured = [], {}
    manifest = {"nodes": {}, "memory_build": True}

    async def dbt(_job, spec, phase, *, manifest_sink=None):
        calls.append((spec, phase))
        assert spec.argv[1] == "build", "build must not run additional commands"
        assert spec.write_operation
        manifest_sink(SimpleNamespace(writable_manifest=lambda: SimpleNamespace(to_dict=lambda **_: manifest)))

    def capture(_project_id, _project, **kwargs):
        captured.update(kwargs)
        return "sealed-output"

    executor = SimpleNamespace(settings=SimpleNamespace(profiles_dir=tmp_path, max_artifact_file_bytes=1024 * 1024),
        _dbt=dbt, _metadata=RuntimeExecutor._metadata, artifacts=SimpleNamespace(capture=capture))
    return executor, job, project, calls, captured


async def test_only_build_persists_memory_manifest_without_other_artifacts(publication_executor, monkeypatch):
    executor, job, project, calls, captured = publication_executor
    original_open = Path.open

    def guarded_open(path, mode="r", *args, **kwargs):
        if path.name in ("manifest.json", "run_results.json", "catalog.json", "sources.json") and "r" in mode:
            raise AssertionError("build must not read artifact proof files")
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    result = await execute_build(executor, job, project)
    assert result.output_set_id == "sealed-output"
    assert len(calls) == 1
    assert calls[0][1] == "BUILDING"
    assert json.loads(captured["manifest_json"])["memory_build"] is True
    assert "validation_json" not in captured["metadata"].model_dump()
    assert yaml.safe_load((project / "dbt_project.yml").read_text())["target-path"] == "target"
    assert not (project / "target").exists()


async def test_build_preserves_project_macros_and_model_configuration(publication_executor):
    executor, job, project, _calls, _captured = publication_executor
    files = {path: path.read_bytes() for directory in ("models", "macros") for path in (project / directory).iterdir()}
    await execute_build(executor, job, project)
    assert all(path.read_bytes() == content for path, content in files.items())
    assert set((project / "macros").iterdir()) == {project / "macros/generate_schema_name.sql"}


async def test_missing_memory_manifest_cannot_publish(publication_executor):
    executor, job, project, _calls, captured = publication_executor
    async def without_manifest(*args, **kwargs):
        pass
    executor._dbt = without_manifest
    with pytest.raises(ValueError, match="manifest is unavailable"):
        await execute_build(executor, job, project)
    assert not captured


async def test_dbt_failure_propagates_without_artifact_capture(publication_executor):
    executor, job, project, _calls, captured = publication_executor
    failure = ExecutionError("COMMAND_FAILED", stopped=True)
    async def fail(*args, **kwargs):
        raise failure
    executor._dbt = fail
    with pytest.raises(ExecutionError) as error:
        await execute_build(executor, job, project)
    assert error.value is failure
    assert not captured
