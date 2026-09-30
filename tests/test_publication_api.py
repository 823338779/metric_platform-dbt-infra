"""v2 仅读取服务已封存的目录，旧版本不允许启动新读取。"""

from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbt_metricflow_service.publication_api import create_publication_router
from tests.test_publication_storage import store as store
from tests.test_publication_transaction import prepared


def test_active_catalog_and_historical_release_access(store, tmp_path):
    jobs, job, release, output, artifacts = prepared(store, tmp_path)
    runtime = SimpleNamespace(db=store.db, jobs=jobs, artifacts=artifacts)
    app = FastAPI()
    app.include_router(create_publication_router(runtime))
    with TestClient(app) as client:
        base = "/v2/projects/" + job["project_id"]
        assert client.get(base + "/publication").json()["activePublication"] is None
        jobs.finish(job["job_id"], job["lease_token"], output_set_id=output)
        publication = client.get(base + "/publication").json()["activePublication"]
        assert publication["releaseId"] == release["release_id"]
        catalog_path = base + "/releases/" + release["release_id"] + "/catalog"
        assert client.get(catalog_path).json()["resources"] == []
        assert client.get(catalog_path + "?page=0").status_code == 422
        assert client.get(catalog_path.replace(job["project_id"], "wrong-project")).status_code == 404
        next_jobs, next_job, _, next_output, _ = prepared(store, tmp_path, job["project_id"])
        next_jobs.finish(next_job["job_id"], next_job["lease_token"], output_set_id=next_output)
        assert client.get(catalog_path).status_code == 410
        assert client.get(base + "/releases/" + release["release_id"]).status_code == 200
