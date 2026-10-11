"""独立 PostgreSQL 验证候选幂等和权威指针。"""

import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from dbt_metricflow_service.storage.jobs import JobStore, StoreConflict
from dbt_metricflow_service.storage.postgres import Database
from dbt_metricflow_service.storage.publications import PublicationStore

DSN_ENV = "SERVICE_TEST_DATABASE_URL"
PROJECT_PREFIX = "publication-test-"


@pytest.fixture
def store():
    dsn = os.getenv(DSN_ENV)
    if not dsn:
        pytest.skip("需要独立 SERVICE_TEST_DATABASE_URL")
    db = Database(dsn)
    db.initialize()
    db.check()
    yield PublicationStore(db)
    db.close()


def test_candidate_sequence_and_idempotency(store):
    project = PROJECT_PREFIX + uuid4().hex
    JobStore(store.db).register_project(project)
    request = {"commitSha": "a" * 40, "configVersion": "1"}
    key = uuid4().hex
    with ThreadPoolExecutor(max_workers=4) as pool:
        rows = list(pool.map(lambda _: store.create_candidate(project, request, key), range(4)))
    assert len({row["release_id"] for row in rows}) == 1
    assert rows[0]["sequence"] == 1
    assert store.get_publication(project)["activePublication"] is None
    with pytest.raises(StoreConflict):
        store.create_candidate(project, {**request, "commitSha": "b" * 40}, key)
    second = store.create_candidate(project, request, uuid4().hex)
    assert second["sequence"] == 2
    with pytest.raises(KeyError):
        store.get_release(PROJECT_PREFIX + uuid4().hex, rows[0]["release_id"])


def test_initialization_keeps_default_output_separate(store):
    project = PROJECT_PREFIX + uuid4().hex
    JobStore(store.db).register_project(project)
    store.db.initialize()
    row = JobStore(store.db).project(project)
    assert row["current_output_set_id"] is None
    assert row["active_published_release_id"] is None
    assert row["publication_sequence"] == 0
