from __future__ import annotations

import gzip
import hashlib
import os
from uuid import uuid4

import psycopg2
import pytest

from dbt_metricflow_service.runtime.workspace import materialized_workspace
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.postgres import Database

# 所有集成测试使用独立项目，允许不同仓储测试并行执行。
DATABASE_ENV = "SERVICE_TEST_DATABASE_URL"
PROJECT_FILE = "dbt_project.yml"
PROJECT_TEXT = "name: artifacts\nversion: '1.0'\nmodel-paths: [custom_models]\n"
MODEL_PATH = "custom_models/orders.sql"
MODEL_BYTES = b"select 1 as id\r\n"
TARGET_PATH = "target/semantic_manifest.json"
MANIFEST_BYTES = b'{ "semantic_models": [] }\n'
SQL_PROJECT = "INSERT INTO runtime_project(project_id) VALUES (%s)"
SQL_MUTATE = "UPDATE runtime_artifact_file SET content = %s WHERE set_id = %s"
SQL_STATE = "UPDATE runtime_artifact_set SET state = 'STAGING' WHERE set_id = %s"
SQL_JOB = """INSERT INTO runtime_job(job_id,kind,project_id,request_fingerprint,config_version,
toolchain_version,deadline_at) VALUES (%s,'DBT_COMMAND',%s,'test','1','1',clock_timestamp()+interval '1 hour')"""
SQL_ATTEMPT = """INSERT INTO runtime_attempt(attempt_id,job_id,attempt_no,worker_id,lease_token,lease_expires_at)
VALUES (%s,%s,1,%s,%s,clock_timestamp()+interval '1 hour')"""


@pytest.fixture
def store():
    dsn = os.environ.get(DATABASE_ENV)
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL is required for PostgreSQL artifact tests")
    database = Database(dsn)
    database.migrate()
    yield ArtifactStore(database)
    database.close()


@pytest.fixture
def project(store, tmp_path):
    project_id = str(uuid4())
    with store.database.transaction() as cursor:
        cursor.execute(SQL_PROJECT, (project_id,))
    source = tmp_path / "input"
    source.mkdir()
    (source / PROJECT_FILE).write_text(PROJECT_TEXT, encoding="utf-8")
    (source / MODEL_PATH).parent.mkdir()
    (source / MODEL_PATH).write_bytes(MODEL_BYTES)
    (source / TARGET_PATH).parent.mkdir()
    (source / TARGET_PATH).write_bytes(MANIFEST_BYTES)
    return project_id, source


def test_source_roundtrip_preserves_raw_bytes_and_configured_paths(store, project, tmp_path):
    project_id, source = project
    set_id = store.capture(project_id, source)
    destination = tmp_path / "restored"
    store.materialize(set_id, destination)
    assert (destination / MODEL_PATH).read_bytes() == MODEL_BYTES
    assert not (destination / TARGET_PATH).exists()
    assert store.metadata(set_id)["file_count"] == 2


def test_execution_keeps_dependencies_and_excludes_local_state(store, project, tmp_path):
    project_id, source = project
    included = "dbt_packages/example/macros/test.sql"
    forbidden = ["profiles.yml", "target/partial_parse.msgpack", "logs/dbt.log", "request.json",
                 "project-path.json", "dbt_packages/example/profiles.yml", ".git/config"]
    for relative in [included, *forbidden]:
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(MODEL_BYTES)
    set_id = store.capture(project_id, source, kind="EXECUTION")
    destination = tmp_path / "restored"
    store.materialize(set_id, destination)
    assert (destination / included).read_bytes() == MODEL_BYTES
    assert (destination / TARGET_PATH).read_bytes() == MANIFEST_BYTES
    assert all(not (destination / path).exists() for path in forbidden)


def test_sealed_files_cannot_be_overwritten(store, project):
    set_id = store.capture(*project)
    with pytest.raises(psycopg2.Error), store.database.transaction() as cursor:
        cursor.execute(SQL_MUTATE, (b"corrupt", set_id))


@pytest.mark.parametrize("unsafe", ["../outside", "/absolute", "C:/absolute", "models/../outside"])
def test_source_rejects_escaping_configured_paths(store, project, unsafe):
    project_id, source = project
    (source / PROJECT_FILE).write_text(f"name: unsafe\nmodel-paths: ['{unsafe}']\n", encoding="utf-8")
    with pytest.raises(ValueError):
        store.capture(project_id, source)


def test_capture_rejects_single_file_over_limit(store, project):
    project_id, source = project
    with (source / MODEL_PATH).open("wb") as stream:
        stream.truncate(64 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="size|limit|大小|超限"):
        store.capture(project_id, source)


def test_capture_rejects_external_symlink(store, project, tmp_path):
    project_id, source = project
    external = tmp_path / "outside.sql"
    external.write_bytes(MODEL_BYTES)
    link = source / "custom_models/link.sql"
    try:
        link.symlink_to(external)
    except OSError:
        pytest.skip("symbolic links unavailable")
    with pytest.raises(ValueError):
        store.capture(project_id, source)


def test_workspace_cleans_only_its_attempt(store, project, tmp_path):
    set_id = store.capture(*project)
    root = tmp_path / "workspaces"
    job_id, attempt_id = str(uuid4()), str(uuid4())
    with materialized_workspace(store, set_id, root, job_id, attempt_id) as directory:
        assert directory == root / job_id / attempt_id
        assert (directory / MODEL_PATH).read_bytes() == MODEL_BYTES
    assert not directory.exists()


def test_metadata_has_stable_digest_across_local_roots(store, project, tmp_path):
    project_id, source = project
    first = store.capture(project_id, source)
    destination = tmp_path / "other-root"
    store.materialize(first, destination)
    second = store.capture(project_id, destination)
    assert store.metadata(first)["content_digest"] == store.metadata(second)["content_digest"]
    with store.database.transaction() as cursor:
        cursor.execute("SELECT raw_sha256 FROM runtime_artifact_file WHERE set_id=%s AND relative_path=%s",
                       (second, MODEL_PATH))
        assert cursor.fetchone()["raw_sha256"] == hashlib.sha256(MODEL_BYTES).hexdigest()


@pytest.fixture
def attempt(store, project):
    job_id, attempt_id = str(uuid4()), str(uuid4())
    with store.database.transaction() as cursor:
        cursor.execute(SQL_JOB, (job_id, project[0]))
        cursor.execute(SQL_ATTEMPT, (attempt_id, job_id, str(uuid4()), str(uuid4())))
    return attempt_id


def test_worker_capture_is_invisible_until_transaction_seals(store, project, attempt, tmp_path):
    set_id = store.capture(*project, kind="EXECUTION", producer_attempt_id=attempt)
    with pytest.raises(ValueError, match="SEALED"):
        store.materialize(set_id, tmp_path / "before")
    with store.database.transaction() as cursor:
        store.seal(set_id, cursor)
    store.materialize(set_id, tmp_path / "after")
    assert (tmp_path / "after" / TARGET_PATH).read_bytes() == MANIFEST_BYTES


def test_gc_preserves_referenced_and_active_worker_artifacts(store, project, attempt):
    source = store.capture(*project)
    with store.database.transaction() as cursor:
        cursor.execute("UPDATE runtime_project SET source_set_id=%s WHERE project_id=%s", (source, project[0]))
    assert not store.delete_unreferenced(source)
    staged = store.capture(*project, producer_attempt_id=attempt)
    assert not store.delete_unreferenced(staged)
    orphan = store.capture(*project)
    assert store.delete_unreferenced(orphan)
    with pytest.raises(ValueError):
        store.metadata(orphan)


def test_read_file_is_bounded_and_requires_sealed(store, project, attempt):
    sealed = store.capture(*project, kind="EXECUTION")
    assert store.read_file(sealed, TARGET_PATH) == MANIFEST_BYTES
    with pytest.raises(ValueError):
        store.read_file(sealed, "../outside")
    staged = store.capture(*project, kind="EXECUTION", producer_attempt_id=attempt)
    with pytest.raises(ValueError):
        store.read_file(staged, TARGET_PATH)


def test_configurable_limits_reject_set_before_publication(store, project):
    limited = ArtifactStore(store.database, max_file_bytes=128, max_set_bytes=32)
    with pytest.raises(ValueError, match="size|limit"):
        limited.capture(*project)


def test_source_includes_resolved_dependencies(store, project):
    dependency = project[1] / "dbt_packages/example/macros/test.sql"
    dependency.parent.mkdir(parents=True)
    dependency.write_bytes(MODEL_BYTES)
    set_id = store.capture(*project)
    assert store.read_file(set_id, dependency.relative_to(project[1]).as_posix()) == MODEL_BYTES


@pytest.mark.parametrize("names", [["../outside.sql"], ["/outside.sql"], ["A.sql", "a.sql"],
                                   ["models", "models/x.sql"], ["C:/outside.sql"], ["models/CON"],
                                   ["Models/a.sql", "models/b.sql"]])
def test_restoration_rejects_unsafe_database_paths_before_writes(store, project, tmp_path, names):
    # 直接构造历史/损坏数据库内容，检验还原端不信任入库端曾经做过验证。
    set_id = str(uuid4())
    digest = hashlib.sha256(b"x").hexdigest()
    manifest = b"".join(name.encode() + b"\0" + digest.encode() + b"\0" + b"1\0" for name in sorted(names))
    with store.database.transaction() as cursor:
        cursor.execute(
            """INSERT INTO runtime_artifact_set(set_id,project_id,kind,state,file_count,raw_bytes,content_digest)
            VALUES (%s,%s,'SOURCE','STAGING',%s,%s,%s)""",
            (set_id, project[0], len(names), len(names), hashlib.sha256(manifest).hexdigest()),
        )
        for name in names:
            cursor.execute("""INSERT INTO runtime_artifact_file
                (set_id,relative_path,content,codec,raw_sha256,raw_size,stored_size)
                VALUES (%s,%s,%s,'raw',%s,%s,%s)""",
                           (set_id, name, b"x", digest, 1, 1))
        cursor.execute("UPDATE runtime_artifact_set SET state='SEALED' WHERE set_id=%s", (set_id,))
    destination = tmp_path / "bad"
    with pytest.raises(ValueError):
        store.materialize(set_id, destination)
    assert not destination.exists()


def test_internal_package_link_is_materialized_as_plain_files(store, project):
    package = project[1] / "dbt_packages/example"
    package.parent.mkdir()
    try:
        package.symlink_to(project[1] / "custom_models", target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links unavailable")
    set_id = store.capture(*project, kind="EXECUTION")
    destination = project[1].parent / "restored"
    store.materialize(set_id, destination)
    assert (destination / "dbt_packages/example/orders.sql").read_bytes() == MODEL_BYTES
    assert not (destination / "dbt_packages/example").is_symlink()


def test_destination_link_is_rejected_without_writing_outside(store, project, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    linked = tmp_path / "linked"
    try:
        linked.symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links unavailable")
    set_id = store.capture(*project)
    with pytest.raises(ValueError):
        store.materialize(set_id, linked / "child")
    assert not (outside / "child").exists()


def test_source_link_cannot_smuggle_excluded_credentials(store, project):
    secret = project[1] / "profiles.yml"
    secret.write_bytes(b"private profile")
    link = project[1] / "custom_models/innocent.yml"
    try:
        link.symlink_to(secret)
    except OSError:
        pytest.skip("symbolic links unavailable")
    with pytest.raises(ValueError):
        store.capture(*project)


@pytest.mark.parametrize("codec,content", [("raw", b"corrupt"), ("gzip", gzip.compress(b"x" * 256))])
def test_corrupt_bytes_and_decompression_over_limit_are_rejected(store, project, tmp_path, codec, content):
    set_id = str(uuid4())
    declared_digest = hashlib.sha256(b"expected").hexdigest()
    manifest_digest = hashlib.sha256(
        b"file.sql\0" + declared_digest.encode() + b"\0" + b"8\0"
    ).hexdigest()
    with store.database.transaction() as cursor:
        cursor.execute("""INSERT INTO runtime_artifact_set
            (set_id,project_id,kind,state,file_count,raw_bytes,content_digest)
            VALUES (%s,%s,'SOURCE','STAGING',1,8,%s)""", (set_id, project[0], manifest_digest))
        cursor.execute("""INSERT INTO runtime_artifact_file
            (set_id,relative_path,content,codec,raw_sha256,raw_size,stored_size)
            VALUES (%s,'file.sql',%s,%s,%s,8,%s)""", (set_id, content, codec, declared_digest, len(content)))
        cursor.execute("UPDATE runtime_artifact_set SET state='SEALED' WHERE set_id=%s", (set_id,))
    limited = ArtifactStore(store.database, max_file_bytes=128)
    with pytest.raises(ValueError, match="checksum|size"):
        limited.materialize(set_id, tmp_path / "corrupt")
    assert not (tmp_path / "corrupt").exists()
