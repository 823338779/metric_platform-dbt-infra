"""发布封存事务故障不能暴露半份目录。"""

import json
from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import CleanupBlocked, JobStore
from dbt_metricflow_service.storage.publications import SQL_ATTACH_RUN, PublicationStore
from tests.test_publication_storage import store as store

BUILD = "BUILD_RUN"
VERSION = "publication-transaction"
FLAGS = {"allTestsPassed": True, "representativeQueryPassed": True, "relationsVerified": True}


def prepared(store, tmp_path, project=None):
    jobs = JobStore(store.db)
    project = project or "publication-" + uuid4().hex
    jobs.register_project(project)
    toolchain = VERSION + uuid4().hex
    request = {"commitSha": "a" * 40, "projectDigest": "b" * 64, "configVersion": "1",
               "toolchainVersion": toolchain}
    release = store.create_candidate(project, request, uuid4().hex)
    job = jobs.reserve(BUILD, project, {**request, "releaseId": release["release_id"]}, toolchain_version=toolchain)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(SQL_ATTACH_RUN, (job["job_id"], release["release_id"]))
    job = jobs.claim(str(uuid4()), toolchain_version=toolchain)
    directory = tmp_path / uuid4().hex
    target = directory / "target"
    target.mkdir(parents=True)
    (directory / "dbt_project.yml").write_text("name: publication\nversion: '1.0'\n")
    for name in ("manifest.json", "semantic_manifest.json", "catalog.json", "run_results.json"):
        (target / name).write_text("{}")
    catalog = {"schemaVersion": 1, "projectId": project, "releaseId": release["release_id"],
               "resources": [], "relations": [], "relationBindings": []}
    (target / "published_catalog.json").write_text(json.dumps(catalog))
    artifacts = ArtifactStore(store.db)
    output = artifacts.capture(project, directory, kind="EXECUTION", producer_attempt_id=job["attempt_id"],
                               metadata={"source_commit_sha": request["commitSha"],
                                         "project_digest": request["projectDigest"],
                                         "config_version": request["configVersion"],
                                         "toolchain_version": toolchain,
                                         "validation_json": {**FLAGS, "publicationValidated": True},
                                         "catalog_json": catalog})
    return jobs, job, release, output, artifacts


def test_finish_publishes_and_cleanup_is_blocked(store, tmp_path):
    jobs, job, release, output, _ = prepared(store, tmp_path)
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    row = store.get_release(job["project_id"], release["release_id"])
    assert row["state"] == "PUBLISHED"
    assert row["artifact_set_id"] == output
    assert store.get_publication(job["project_id"])["activePublication"]["release_id"] == release["release_id"]
    with pytest.raises(CleanupBlocked):
        jobs.reserve_cleanup(job["job_id"])


def test_seal_failure_preserves_old_publication(store, tmp_path):
    jobs, job, release, output, _ = prepared(store, tmp_path)
    jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    jobs, candidate, _, next_output, artifacts = prepared(store, tmp_path, job["project_id"])

    def broken_seal(set_id, connection):
        artifacts.seal(set_id, connection)
        raise ValueError("injected after seal")

    with pytest.raises(ValueError, match="injected"):
        jobs.finish(candidate["job_id"], candidate["lease_token"], output_set_id=next_output, seal=broken_seal)
    assert artifacts.metadata(next_output)["state"] == "STAGING"
    assert store.get_publication(job["project_id"])["activePublication"]["release_id"] == release["release_id"]


def test_older_candidate_cannot_replace_latest_input(store, tmp_path):
    jobs, job, release, output, _ = prepared(store, tmp_path)
    store.create_candidate(job["project_id"], {"commitSha": "b" * 40}, uuid4().hex)
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_release(job["project_id"], release["release_id"])["state"] == "SUPERSEDED"
    assert store.get_publication(job["project_id"])["activePublication"] is None


def test_failed_job_marks_candidate_failed_without_changing_pointer(store, tmp_path):
    jobs, job, release, _, _ = prepared(store, tmp_path)
    assert jobs.fail(job["job_id"], job["lease_token"], "INVALID_CATALOG")
    assert store.get_release(job["project_id"], release["release_id"])["state"] == "FAILED"
    assert store.get_publication(job["project_id"])["activePublication"] is None


def test_expired_worker_cannot_publish(store, tmp_path):
    jobs, job, release, output, _ = prepared(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql("UPDATE runtime_attempt SET lease_expires_at=clock_timestamp()-interval '1 second' "
                       "WHERE attempt_id=%s", (job["attempt_id"],))
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output) is False
    assert store.get_release(job["project_id"], release["release_id"])["state"] != "PUBLISHED"


def test_wrong_source_proof_cannot_publish(store, tmp_path):
    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    with store.db.transaction() as connection:
        connection.exec_driver_sql(
            "UPDATE runtime_artifact_set SET source_commit_sha=%s WHERE set_id=%s", ("c" * 40, output)
        )
    with pytest.raises(ValueError, match="输入"):
        jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    assert artifacts.metadata(output)["state"] == "STAGING"
    assert store.get_publication(job["project_id"])["activePublication"] is None


def test_failure_after_pointer_update_rolls_back_entire_publication(store, tmp_path, monkeypatch):
    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    original = PublicationStore.publish_in_transaction

    def fail_after_pointer(self, connection, **kwargs):
        original(self, connection, **kwargs)
        raise ValueError("injected after pointer")

    monkeypatch.setattr(PublicationStore, "publish_in_transaction", fail_after_pointer)
    with pytest.raises(ValueError, match="injected"):
        jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    assert store.get_publication(job["project_id"])["activePublication"] is None
    assert store.get_release(job["project_id"], release["release_id"])["state"] == "PREPARING"
    assert artifacts.metadata(output)["state"] == "STAGING"
    assert jobs.result(job["job_id"]) is None


def test_committed_publication_cannot_be_failed_after_response_loss(store, tmp_path):
    jobs, job, release, output, _ = prepared(store, tmp_path)
    assert jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    assert jobs.fail(job["job_id"], job["lease_token"], "RESPONSE_LOST") is False
    assert store.get_release(job["project_id"], release["release_id"])["state"] == "PUBLISHED"
