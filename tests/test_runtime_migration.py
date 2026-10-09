import hashlib
import json
import os
import sqlite3
from uuid import uuid4

import pytest

from dbt_metricflow_service.admin import import_legacy, import_project, reconcile_attempt, register_bindings
from dbt_metricflow_service.platform.catalog import CATALOG_SCHEMA, MANIFEST_SCHEMA
from dbt_metricflow_service.settings import Settings
from dbt_metricflow_service.storage.artifacts import ArtifactStore
from dbt_metricflow_service.storage.jobs import JobStore
from dbt_metricflow_service.storage.postgres import Database

# 旧库文件由测试直接构造，避免使用会修改历史运行状态的旧仓储构造函数。
DATABASE_ENV = "SERVICE_TEST_DATABASE_URL"
ENCODING = "utf-8"
LEGACY_TABLES = ("platform_runs", "platform_queries")
ARTIFACT_DIGESTS = {"manifest.json": "manifestDigest", "semantic_manifest.json": "semanticManifestDigest",
                    "run_results.json": "runResultsDigest", "catalog.json": "catalogDigest"}


@pytest.fixture
def database(tmp_path):
    dsn = os.getenv(DATABASE_ENV)
    if not dsn:
        pytest.skip("SERVICE_TEST_DATABASE_URL required for migration integration tests")
    db = Database(dsn)
    db.migrate()
    settings = Settings(tmp_path, tmp_path, 60, 1024, database_url=dsn, toolchain_version="migration-test")
    yield db, settings
    db.close()


@pytest.fixture
def legacy(tmp_path):
    run_id, query_id = str(uuid4()), str(uuid4())
    project_id = "migration_" + uuid4().hex
    schema = "run_" + run_id.replace("-", "")
    directory = tmp_path / run_id
    project = directory / "source"
    target = project / "target"
    target.mkdir(parents=True)
    (project / "dbt_project.yml").write_text("name: sample\nversion: '1.0'\n", encoding=ENCODING)
    artifacts = {
        "manifest.json": {"metadata": {"adapter_type": "postgres", "dbt_schema_version": MANIFEST_SCHEMA},
                          "nodes": {"model.sample.a": {"resource_type": "model", "schema": schema,
                                    "relation_name": f'"db"."{schema}"."a"', "config": {"materialized": "table"}}}},
        "semantic_manifest.json": {"semantic_models": [], "metrics": []},
        "run_results.json": {"results": [{"unique_id": "model.sample.a", "status": "success"}]},
        "catalog.json": {"metadata": {"dbt_schema_version": CATALOG_SCHEMA},
                         "nodes": {"model.sample.a": {"columns": {}}}},
    }
    validation = {"schemaName": schema, "allTestsPassed": True, "representativeQueryPassed": True,
                  "queryCapability": True, "relationsVerified": True}
    for name, artifact in artifacts.items():
        content = json.dumps(artifact).encode()
        (target / name).write_bytes(content)
        validation[ARTIFACT_DIGESTS[name]] = hashlib.sha256(content).hexdigest()
    request = {"projectId": project_id, "commitSha": "a" * 40, "projectDigest": "b" * 64,
               "profileBindingId": "postgres", "configVersion": "1", "idempotencyKey": str(uuid4())}
    for name, value in {"request.json": request, "validation.json": validation,
                        "project-path.json": {"path": str(project)}}.items():
        (directory / name).write_text(json.dumps(value), encoding=ENCODING)
    (directory / "READY").write_text("ready", encoding=ENCODING)
    query_dir = tmp_path / query_id
    query_dir.mkdir()
    query_request = {"runId": run_id, "idempotencyKey": str(uuid4()), "mode": "QUERY", "metrics": ["revenue"]}
    result = {"columns": [{"name": "revenue", "type": "Decimal"}], "rows": [["1234567890123.000000001"]]}
    (query_dir / "request.json").write_text(json.dumps(query_request), encoding=ENCODING)
    (query_dir / "result.json").write_text(json.dumps(result), encoding=ENCODING)
    (query_dir / "READY").write_text("ready", encoding=ENCODING)
    sqlite_path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(sqlite_path) as connection:
        for table in LEGACY_TABLES:
            connection.execute(f"CREATE TABLE {table}(id TEXT,idempotency_key TEXT,fingerprint TEXT,state TEXT,"
                               "artifact_path TEXT,error_code TEXT,parent_run_id TEXT)")
        connection.execute("INSERT INTO platform_runs VALUES (?,?,?,'READY',?,NULL,NULL)",
                           (run_id, request["idempotencyKey"], "run-fingerprint", str(directory)))
        connection.execute("INSERT INTO platform_queries VALUES (?,?,?,'READY',?,NULL,?)",
                           (query_id, query_request["idempotencyKey"], "query-fingerprint", str(query_dir), run_id))
    return sqlite_path, run_id, query_id, project_id, target, result


def test_import_legacy_preserves_uuid_bytes_and_decimal_and_is_idempotent(database, legacy, tmp_path):
    db, settings = database
    sqlite_path, run_id, query_id, _, target, expected = legacy
    before = sqlite_path.read_bytes()
    assert import_legacy(db, settings, sqlite_path) == [run_id, query_id]
    first = JobStore(db).get(run_id)
    assert first["status"] == "SUCCEEDED"
    assert JobStore(db).result(run_id)["payload_json"]["schemaName"] == "run_" + run_id.replace("-", "")
    assert JobStore(db).result(query_id)["payload_json"] == expected
    assert import_legacy(db, settings, sqlite_path) == [run_id, query_id]
    assert JobStore(db).get(run_id)["output_set_id"] == first["output_set_id"]
    assert ArtifactStore(db).read_file(first["output_set_id"], "target/manifest.json") == (
        target / "manifest.json"
    ).read_bytes()
    assert sqlite_path.read_bytes() == before


def test_missing_ready_artifact_refuses_import_without_success_record(database, legacy):
    db, settings = database
    sqlite_path, run_id, _, _, target, _ = legacy
    (target / "catalog.json").unlink()
    with pytest.raises((ValueError, FileNotFoundError)):
        import_legacy(db, settings, sqlite_path)
    assert JobStore(db).get(run_id) is None


def test_reimport_changed_query_result_is_refused(database, legacy):
    db, settings = database
    sqlite_path, _, query_id, _, _, _ = legacy
    import_legacy(db, settings, sqlite_path)
    (sqlite_path.parent / query_id / "result.json").write_text('{"rows": [["changed"]]}', encoding=ENCODING)
    with pytest.raises(ValueError, match="different|changed|mismatch|不同|变化"):
        import_legacy(db, settings, sqlite_path)


def test_import_project_records_source_and_existing_execution(database, legacy):
    db, settings = database
    _, _, _, project_id, target, _ = legacy
    result = import_project(db, settings, project_id, target.parent)
    row = JobStore(db).project(project_id)
    assert row["source_set_id"] == result["sourceSetId"]
    assert row["current_output_set_id"] == result["outputSetId"]
    assert ArtifactStore(db).metadata(result["outputSetId"])["source_set_id"] == result["sourceSetId"]


def test_register_bindings_rejects_remote_credentials_without_persisting(database, tmp_path):
    db, settings = database
    project_id = "binding_" + uuid4().hex
    path = tmp_path / "bindings.json"
    path.write_text(json.dumps([{"projectId": project_id, "remote": "https://user:private@example.test/repo",
                                 "projectSubdir": ".", "profileBindingId": "postgres"}]), encoding=ENCODING)
    with pytest.raises(ValueError):
        register_bindings(db, settings, path)
    assert JobStore(db).project(project_id) is None


def test_reconciliation_requires_explicit_external_stop_confirmation(database):
    db, _ = database
    with pytest.raises(ValueError):
        reconcile_attempt(db, str(uuid4()), confirm_external_stopped=False)


def test_historic_run_import_does_not_replace_current_project_configuration(database, legacy):
    db, settings = database
    sqlite_path, _, _, project_id, _, _ = legacy
    JobStore(db).register_project(project_id, config_version="current-config")
    import_legacy(db, settings, sqlite_path)
    assert JobStore(db).project(project_id)["config_version"] == "current-config"


def test_failed_project_import_does_not_replace_configuration(database, legacy, tmp_path):
    db, settings = database
    project_id = legacy[3]
    JobStore(db).register_project(project_id, config_version="current-config")
    with pytest.raises(FileNotFoundError):
        import_project(db, settings, project_id, tmp_path / "missing")
    assert JobStore(db).project(project_id)["config_version"] == "current-config"


def test_register_valid_bindings_and_confirm_stopped_release_write_guard(database, tmp_path):
    db, settings = database
    project_id = "binding_" + uuid4().hex
    path = tmp_path / "bindings.json"
    binding = {"projectId": project_id, "remote": "https://example.test/repo.git",
               "projectSubdir": ".", "profileBindingId": "postgres"}
    path.write_text(json.dumps([binding]), encoding=ENCODING)
    assert register_bindings(db, settings, path) == [project_id]
    jobs = JobStore(db)
    assert jobs.project(project_id)["binding_config"] == binding
    jobs.reserve("DBT_COMMAND", project_id, {}, write=True, toolchain_version=project_id)
    claim = jobs.claim(uuid4(), toolchain_version=project_id)
    with db.transaction() as cursor:
        cursor.execute("UPDATE runtime_attempt SET state='EXPIRED_UNCONFIRMED' WHERE attempt_id=%s",
                       (claim["current_attempt_id"],))
        cursor.execute("UPDATE runtime_job SET status='FAILED' WHERE job_id=%s", (claim["job_id"],))
    assert reconcile_attempt(db, claim["current_attempt_id"], confirm_external_stopped=True)
    assert jobs.project(project_id)["busy_job_id"] is None


def test_bindings_support_explicit_config_and_readonly_policy(database, tmp_path):
    db, settings = database
    project_id = "binding_" + uuid4().hex
    path = tmp_path / "bindings-policy.json"
    path.write_text(json.dumps([{"projectId": project_id, "remote": "https://example.test/repo.git",
                                 "projectSubdir": ".", "profileBindingId": "postgres",
                                 "configVersion": "v2", "queryRetrySafe": True}]), encoding=ENCODING)
    register_bindings(db, settings, path)
    row = JobStore(db).project(project_id)
    assert row["config_version"] == "v2"
    assert row["binding_config"]["queryRetrySafe"] is True


def test_imported_idempotency_key_reuses_existing_run(database, legacy):
    db, settings = database
    sqlite_path, run_id, _, project_id, _, _ = legacy
    import_legacy(db, settings, sqlite_path)
    jobs = JobStore(db)
    old = jobs.get(run_id)
    request = {**old["request_json"], "binding": {"remote": "https://example.test/repo.git"}}
    assert jobs.reserve("BUILD_RUN", project_id, request, idempotency_scope="BUILD_RUN",
                        idempotency_key=old["idempotency_key"], config_version=old["config_version"],
                        toolchain_version=settings.toolchain_version)["job_id"] == run_id


def test_failed_legacy_build_keeps_source_and_unknown_execution_guard(database, legacy):
    from dbt_metricflow_service.storage.jobs import CleanupBlocked
    db, settings = database
    sqlite_path, run_id, _, _, _, _ = legacy
    with sqlite3.connect(sqlite_path) as connection:
        connection.execute("DELETE FROM platform_queries")
        connection.execute("UPDATE platform_runs SET state='FAILED',error_code='INTERRUPTED'")
    import_legacy(db, settings, sqlite_path)
    jobs = JobStore(db)
    row = jobs.get(run_id)
    assert row["input_set_id"] is not None
    with pytest.raises(CleanupBlocked):
        jobs.reserve_cleanup(run_id)


def test_migrate_cli_uses_environment_without_exposing_database_url(database, monkeypatch, capsys):
    from dbt_metricflow_service.admin import main

    _, settings = database
    monkeypatch.setenv("SERVICE_DATABASE_URL", settings.database_url)
    main(["migrate"])
    output = capsys.readouterr().out
    assert json.loads(output) == {"migrated": True}
    assert settings.database_url not in output
