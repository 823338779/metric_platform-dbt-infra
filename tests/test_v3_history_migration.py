"""历史身份映射可重复执行，旧封存字节不能被转换覆盖。"""

from dbt_metricflow_service.runtime.completion import complete_job
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared


def test_migration_is_stable_and_preserves_bytes(store, tmp_path):
    from dbt_metricflow_service.storage.migration import migrate_history

    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    before = artifacts.read_file(output, "target/published_catalog.json")
    mapping = {job["project_id"]: "https://git.example.com/legacy.git"}
    with store.db.transaction() as connection:
        first = migrate_history(connection, mapping, dry_run=False)
    with store.db.transaction() as connection:
        second = migrate_history(connection, mapping, dry_run=False)
    assert first["mapping"][release["release_id"]] == second["mapping"][release["release_id"]]
    assert artifacts.read_file(output, "target/published_catalog.json") == before


def test_source_conflict_is_reported_and_dry_run_does_not_write(store, tmp_path):
    from dbt_metricflow_service.storage.migration import migrate_history

    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    assert complete_job(jobs, job["job_id"], job["lease_token"], output_set_id=output)
    mapping = {job["project_id"]: "https://git.example.com/first.git"}
    with store.db.transaction() as connection:
        dry = migrate_history(connection, mapping, dry_run=True)
        identifier = dry["mapping"][release["release_id"]]
        assert connection.exec_driver_sql("SELECT 1 FROM engine_build WHERE build_id=%s", (identifier,)).first() is None
        migrated = migrate_history(connection, mapping)
    with store.db.transaction() as connection:
        conflict = migrate_history(connection, {job["project_id"]: "https://git.example.com/different.git"})
        assert release["release_id"] in conflict["conflicts"]
        assert (
            connection.exec_driver_sql(
                "SELECT repository FROM engine_build WHERE build_id=%s", (identifier,)
            ).scalar_one()
            == mapping[job["project_id"]]
        )
        assert migrate_history(connection, {})["mapping"][release["release_id"]] == identifier
    assert migrated["conflicts"] == []
