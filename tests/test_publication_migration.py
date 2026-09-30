"""已有服务发布的旧公开身份映射必须幂等且限定项目。"""

from uuid import uuid4

import pytest

from dbt_metricflow_service.publication_migration import import_publication
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared


def test_import_existing_publication_identity_is_idempotent(store, tmp_path):
    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
    old_id = str(uuid4())
    document = {"projectId": job["project_id"], "legacyReleaseId": old_id,
                "runId": job["job_id"], "queries": []}
    assert import_publication(store.db, artifacts, document, dry_run=True)["mapped"] is False
    assert import_publication(store.db, artifacts, document, dry_run=False)["mapped"] is True
    assert import_publication(store.db, artifacts, document, dry_run=False)["mapped"] is True
    assert store.get_release(job["project_id"], old_id)["release_id"] == release["release_id"]
    with pytest.raises((ValueError, KeyError)):
        import_publication(store.db, artifacts, {**document, "projectId": "missing"}, dry_run=False)
