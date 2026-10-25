"""Strict records at storage boundaries and preserved artifact/JSON encoding."""

import base64
import hashlib
import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, select, text

from dbt_metricflow_service.models.execution import ExecutionSnapshot, SnapshotBuildRequest
from dbt_metricflow_service.storage.artifacts import ArtifactStore, _decode, _digest
from dbt_metricflow_service.storage.builds import BuildStore
from dbt_metricflow_service.storage.entities import RuntimeJob, RuntimeProject
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.paging import read_cursor, write_cursor
from dbt_metricflow_service.storage.records import (
    ArtifactMetadata,
    ArtifactValidation,
    JobErrorDetail,
    JobRequest,
    StoredArtifactFile,
    StoredProject,
)
from dbt_metricflow_service.storage.rows import entity_record, row_record
from tests.schema_helpers import isolated_database


def execution():
    return ExecutionSnapshot(
        environment_id=uuid4(), config_version=1,
        connection={"type": "postgres", "instanceId": "warehouse", "host": "localhost", "port": 5432},
        credential_ref="warehouse", catalog="analytics", schema="preview",
    )


def test_record_converts_nested_json_and_rejects_unknown_fields():
    snapshot = execution()
    entity = RuntimeProject(
        project_id="example", binding_config=snapshot.model_dump(mode="json", by_alias=True),
        config_version="1", source_set_id=None, busy_job_id=None, revision=1,
    )
    record = entity_record(entity, StoredProject)
    assert record.binding_config.connection.instance_id == "warehouse"
    assert record.binding_config.schema_name == "preview"
    entity.binding_config = {}
    assert record.binding_config.schema_name == "preview"
    with pytest.raises(TypeError):
        record["project_id"]
    with pytest.raises(ValidationError, match="extra_forbidden"):
        StoredProject.model_validate({**record.model_dump(), "unknown": True})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        JobRequest.model_validate({"execution": {**snapshot.model_dump(by_alias=True), "unknown": True}})
    with pytest.raises(ValidationError, match="extra_forbidden"):
        ArtifactMetadata.model_validate({"validation_json": {"allTestsPassed": True, "unknown": True}})
    for non_boolean in (1, "true"):
        with pytest.raises(ValidationError):
            ArtifactValidation.model_validate({"allTestsPassed": non_boolean})


def test_sql_projection_returns_a_declared_record():
    engine = create_engine("sqlite://")
    try:
        with engine.connect() as connection:
            record = row_record(connection.execute(text(
                "SELECT 'diagnostic' AS message, 'RuntimeError' AS type"
            )), JobErrorDetail)
            assert record.message == "diagnostic"
            assert record.type == "RuntimeError"
            assert row_record(connection.execute(text(
                "SELECT 'diagnostic' AS message, 'RuntimeError' AS type WHERE 0"
            )), JobErrorDetail) is None
            with pytest.raises(ValidationError, match="extra_forbidden"):
                row_record(connection.execute(text(
                    "SELECT 'diagnostic' AS message, 'RuntimeError' AS type, 1 AS unknown"
                )), JobErrorDetail)
    finally:
        engine.dispose()


def test_typed_json_keeps_existing_field_names_and_empty_requests():
    assert JobRequest().model_dump(mode="json", by_alias=True, exclude_unset=True) == {}
    request = JobRequest(build_id="build", project_digest="digest", execution=execution())
    body = request.model_dump(mode="json", by_alias=True, exclude_unset=True)
    assert body["buildId"] == "build"
    assert body["projectDigest"] == "digest"
    assert body["execution"]["schema"] == "preview"
    detail = JobErrorDetail(external_outcome_unknown=True, submitted_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert detail.model_dump(mode="json", by_alias=True, exclude_unset=True) == {
        "externalOutcomeUnknown": True, "submitted_at": "2026-01-01T00:00:00Z",
    }
    validation = ArtifactValidation(all_tests_passed=True, representative_query_passed=True, relations_verified=True)
    assert validation.model_dump(mode="json", by_alias=True, exclude_unset=True) == {
        "allTestsPassed": True, "representativeQueryPassed": True, "relationsVerified": True,
    }


def test_artifact_record_preserves_checksum_and_corruption_detection():
    content = b"select 1"
    digest = hashlib.sha256(content).hexdigest()
    record = StoredArtifactFile(
        set_id=str(uuid4()), relative_path="models/test.sql", content=content, codec="raw",
        raw_sha256=digest, raw_size=len(content), stored_size=len(content), media_type="text/plain", executable=False,
    )
    assert _decode(record, 1024) == content
    assert _digest([record]) == hashlib.sha256(f"models/test.sql\0{digest}\0{len(content)}\0".encode()).hexdigest()
    with pytest.raises(ValueError, match="checksum"):
        _decode(record.model_copy(update={"content": b"select 2"}), 1024)


def test_cursor_validation_rejects_extra_fields_and_wrong_types():
    cursor = write_cursor(["scope"], "last")
    assert read_cursor(["scope"], cursor, 50) == "last"
    token = json.loads(base64.urlsafe_b64decode(cursor))
    for change in ({"unknown": 1}, {"version": True}, {"after": 1}):
        invalid = base64.urlsafe_b64encode(json.dumps({**token, **change}).encode()).decode()
        with pytest.raises(ValueError, match="invalid cursor"):
            read_cursor(["scope"], invalid, 50)


def test_postgres_build_job_and_artifact_records(tmp_path):
    with isolated_database() as db:
        db.initialize()
        jobs = JobStore(db)
        builds = BuildStore(db)
        request = SnapshotBuildRequest(
            repository="https://example.com/project.git", branch_name="main", execution=execution(),
            idempotency_key=str(uuid4()),
        )
        build = builds.accept_snapshot(request, "test", "strict-records", 600, "1")
        assert build.request_json.repository == request.repository
        assert build.config_snapshot.schema_name == "preview"
        with db.session() as session:
            persisted = session.scalar(select(RuntimeJob.request_json).where(RuntimeJob.job_id == build.run_id))
        assert persisted == {
            **request.model_dump(mode="json", by_alias=True), "buildId": build.build_id,
            "binding": request.execution.model_dump(mode="json", by_alias=True),
        }
        job = jobs.claim(str(uuid4()), toolchain_version="strict-records", job_id=build.run_id)
        assert job.request_json.build_id == build.build_id
        assert job.request_json.execution.connection.host == "localhost"
        assert jobs.heartbeat(job.job_id, job.lease_token)
        assert builds.get(build.build_id).build_status == "RUNNING"
        assert jobs.fail(job.job_id, job.lease_token, "TEST_FINISHED", JobErrorDetail(message="done"))
        assert jobs.get(job.job_id).error_detail.message == "done"
        (tmp_path / "dbt_project.yml").write_text("name: example\nversion: '1.0'\n", encoding="utf-8")
        artifacts = ArtifactStore(db)
        identifier = artifacts.capture(job.project_id, tmp_path, metadata=ArtifactMetadata())
        assert artifacts.read_file(identifier, "dbt_project.yml").startswith(b"name: example")
