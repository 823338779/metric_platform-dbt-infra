"""Validated record factories for tests that replace persistence boundaries."""

from datetime import UTC, datetime
from uuid import uuid4

from dbt_metricflow_service.storage.records import JobRequest, LeasedJob, StoredBuild
from tests.test_build_acceptance import request


def leased_job(**changes):
    now = datetime.now(UTC)
    values = {
        "physical_target_key": None,
        "target_claimed": False,
        "idempotency_scope": None,
        "idempotency_key": None,
        "attempt_no": 1,
        "available_at": now,
        "deadline_at": now,
        "max_attempts": 3,
        "retry_policy": "PREPARATION_ONLY",
        "created_at": now,
        "started_at": None,
        "finished_at": None,
        "job_id": str(uuid4()),
        "kind": "BUILD_RUN",
        "project_id": "fixture",
        "parent_run_id": None,
        "request_json": JobRequest(),
        "request_fingerprint": "fixture",
        "status": "RUNNING",
        "phase": "PREPARING",
        "input_set_id": None,
        "output_set_id": None,
        "config_version": "1",
        "toolchain_version": "test",
        "profile_binding_id": "fixture",
        "schema_name": "fixture",
        "run_lifecycle": "ACTIVE",
        "current_attempt_id": None,
        "error_code": None,
        "error_detail": None,
        "attempt_id": str(uuid4()),
        "lease_token": str(uuid4()),
    }
    return LeasedJob.model_validate(values | changes)


def stored_build(**changes):
    now = datetime.now(UTC)
    body = request()
    values = {
        "build_id": str(uuid4()),
        "run_id": str(uuid4()),
        "repository": body.repository,
        "branch_name": body.branch_name,
        "environment": "test",
        "environment_id": str(body.execution.environment_id),
        "physical_target_key": "fixture",
        "config_version": "1",
        "toolchain_version": "test",
        "caller": "test",
        "idempotency_key": body.idempotency_key,
        "request_digest": "fixture",
        "request_json": body,
        "config_snapshot": body.execution,
        "requested_commit_sha": None,
        "commit_sha": None,
        "build_status": "SUCCEEDED",
        "phase": "DONE",
        "cancel_requested": False,
        "output_set_id": "output",
        "catalog_digest": None,
        "error_code": None,
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "finished_at": now,
        "run_lifecycle": "ACTIVE",
    }
    return StoredBuild.model_validate(values | changes)
